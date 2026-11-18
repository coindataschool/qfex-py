import asyncio
import hashlib
import hmac
import json
import logging
import random
import secrets
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Protocol, Tuple

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from .config import QFEXConfig
from .models import BalanceState, PositionState, BBO, Side, SymbolRefdata
from .strategy import TraderStrategy


class QFEXTradeError(Exception):
    """
    Raised by _send_order_and_wait / _send_cancel_and_wait /
    set_user_leverage when QFEX rejects the command via the top-level
    {"err": {...}} envelope (see _handle_trade_error) rather than a
    normal terminal order_response.

    self.err holds the raw err dict — self.err["error_code"] and
    self.err["message"] are the fields you most likely want;
    self.err["incoming_message"] echoes back what QFEX received.
    """

    def __init__(self, err: Dict[str, Any]):
        self.err = err
        super().__init__(err.get("message") or err.get("error_code") or str(err))


class _TimeBoundedSet:
    """
    A dict of key -> time.monotonic() timestamp, capped in both size and
    retention age. Used for small "remember this happened recently"
    trackers (a timed-out client_order_id, a resolved stop order_id)
    where unbounded growth over a long-running session would be a real
    problem, but a plain dict with no eviction would otherwise be the
    simplest thing to reach for.

    `retention_s`: entries older than this are treated as expired by
    get()/pop() (an expired entry still counts toward `max_size` until
    naturally evicted or explicitly removed — this class doesn't
    proactively sweep on a timer, only on access/insert).
    `max_size`: hard cap; insert() evicts the oldest entry first if
    exceeded, regardless of whether it's expired by age yet.
    """

    def __init__(self, retention_s: float, max_size: int):
        self._retention_s = retention_s
        self._max_size = max_size
        self._data: Dict[str, float] = {}

    def insert(self, key: str) -> None:
        """Records `key` as seen now, evicting the oldest entry if over max_size."""
        self._data[key] = time.monotonic()
        if len(self._data) > self._max_size:
            oldest_key = min(self._data, key=self._data.get)
            self._data.pop(oldest_key, None)

    def peek(self, key: str) -> Optional[float]:
        """Returns seconds since `key` was inserted, or None if absent/expired."""
        inserted_at = self._data.get(key)
        if inserted_at is None:
            return None
        elapsed = time.monotonic() - inserted_at
        return elapsed if elapsed <= self._retention_s else None

    def pop(self, key: str) -> Optional[float]:
        """Like peek(), but also removes `key` so a second call returns None."""
        elapsed = self.peek(key)
        self._data.pop(key, None)
        return elapsed


class _OrderEntryHost(Protocol):
    """
    Structural type for what QFEXTraderClient's order-entry methods
    (send_gtc_alo, send_ioc_limit, etc., in trader_client.py) need from
    self. Static-typing only, no runtime effect. Kept as a Protocol
    rather than inlined onto _QFEXBaseClient's own type so the
    order-entry methods' signatures stay decoupled from everything else
    _QFEXBaseClient defines. Keep in sync with the real signatures below.
    """

    cfg: QFEXConfig

    async def _send_order_and_wait(
        self,
        *,
        symbol: str,
        side: Side,
        quantity: Decimal,
        order_type: str,
        time_in_force: Optional[str],
        price: Optional[Decimal],
        terminal_statuses: Tuple[str, ...],
        client_order_id: Optional[str] = None,
        reduce_only: Optional[bool] = None,
    ) -> Dict[str, Any]: ...

    async def _send_cancel_and_wait(
        self, order_id: str, symbol: str
    ) -> Dict[str, Any]: ...

    async def _send_stop_cancel_and_wait(
        self, stop_order_id: str
    ) -> Dict[str, Any]: ...


# Terminal statuses for a pending cancel_order() call — fixed and
# universal, unlike new-order terminal statuses which vary per order
# type. (docs.qfex.com/websocket/channels/trade/cancel_order)
_CANCEL_TERMINAL_STATUSES = (
    "CANCELLED",
    "CANCELLED_STP",
    "NO_SUCH_ORDER",
    "CANNOT_MODIFY_NO_SUCH_ORDER",
    "REJECTED",
)

# Statuses for which quantity/quantity_remaining reflect a REAL, known
# order state on QFEX's book — worth diffing for the fill-delta
# bookkeeping in _handle_order_response's step 1. ACK (freshly resting),
# FILLED (partial or complete), and CANCELLED/CANCELLED_STP (a resting
# order pulled, possibly after a partial fill) all mean an order
# genuinely existed and this response describes its real quantity
# fields.
#
# Every reject-family status (REJECTED, NO_SUCH_ORDER, INVALID_QUANTITY,
# INVALID_PRICE, FAILED_MARGIN_CHECK, and the rest of trader_client.py's
# _ALO_REJECT_STATUSES/_STOP_REJECT_STATUSES) means QFEX has no real
# order to report on — either the placement never landed, or (as below)
# a later action targeted an order that's already gone — and its
# quantity/quantity_remaining fields come back zeroed/templated rather
# than describing anything real. Diffing that against a previously
# cached remaining produces a PHANTOM fill.
#
# Confirmed live (2026-09-04, GOLD-USD): QFEX ACKed a resting BID, then
# auto-cancelled it itself moments later (order_origin=PORT, not a
# bracket-sibling cancel) — nothing cleared the strategy's local
# bid_order_id, so a later tick tried to cancel the (already-gone)
# order. QFEX correctly replied NO_SUCH_ORDER, but with
# quantity_remaining=0.0, client_order_id='', and type=LIMIT (a generic
# "no such order" template, not the order's real ALO shape). The old
# code diffed that 0.0 against the real remaining cached from the ACK
# and read it as a fresh 0.01 fill — a phantom fill whose status
# (NO_SUCH_ORDER) then counted as a genuine failure toward
# PerpetualMarketMakingStrategy's order_error_streak circuit breaker,
# tripping it on a false signal even though the position itself had
# already been closed cleanly two BBO ticks earlier. Two of that
# incident's three "failures" were the same root bug from the
# INVALID_QUANTITY race documented on _STOP_REJECT_STATUSES in
# trader_client.py — this fixes the general case, not just this one
# incident's specific status code.
_FILL_TRACKABLE_STATUSES = frozenset({"ACK", "FILLED", "CANCELLED", "CANCELLED_STP"})

# Default retention/cap for the small "remember this happened recently"
# _TimeBoundedSet trackers below (_recently_timed_out_oids,
# _resolved_stop_order_ids, _outstanding_client_oids) — all three are a
# pure memory-safety bound on an incidental tracking dict, not a value
# with independent tuning significance, so they share one pair of
# defaults rather than three copies of the same two numbers. Each still
# gets its own named constant (not used directly) so a call site stays
# self-documenting about which tracker it's sizing, and any future
# divergence is a one-line change rather than a search-and-replace.
_DEFAULT_TRACKING_SET_RETENTION_S = 120.0
_DEFAULT_TRACKING_SET_MAX_SIZE = 200

# How long (seconds) a timed-out client_order_id is remembered before a
# late response for it is no longer worth flagging, and a hard cap on
# how many such entries are kept (oldest dropped first). See
# _recently_timed_out_oids / _handle_order_response.
_TIMED_OUT_OID_RETENTION_S = _DEFAULT_TRACKING_SET_RETENTION_S
_MAX_TIMED_OUT_OID_MEMORY = _DEFAULT_TRACKING_SET_MAX_SIZE

# Same retention/cap pattern, for _resolved_stop_order_ids /
# _handle_trade_error's late-error suppression.
_RESOLVED_STOP_ORDER_RETENTION_S = _DEFAULT_TRACKING_SET_RETENTION_S
_MAX_RESOLVED_STOP_ORDER_MEMORY = _DEFAULT_TRACKING_SET_MAX_SIZE

# Margin added to cfg.order_update_timeout_s to derive the backstop for
# is_order_outstanding() moved to QFEXConfig.order_outstanding_backstop_
# margin_s — see that field's comment in config.py for the full
# reasoning (the 2026-07-27 CL-USD incident that motivated it) and
# is_order_outstanding() for how it's combined with
# cfg.order_update_timeout_s.

# Retention/cap for the _TimeBoundedSet backing _outstanding_client_oids
# — purely a memory-bound safety net for entries that never get an
# explicit terminal answer at all. is_order_outstanding() clamps its
# computed backstop to never exceed this value (see that method), so
# retention doesn't need to be derived from order_update_timeout_s or
# order_outstanding_backstop_margin_s to stay safe even if either is set
# unusually large — the clamp is the actual guarantee, not a specific
# numeric relationship between these constants and those config fields.
_ORDER_OUTSTANDING_RETENTION_S = _DEFAULT_TRACKING_SET_RETENTION_S
_MAX_OUTSTANDING_OID_MEMORY = _DEFAULT_TRACKING_SET_MAX_SIZE

# Mark-price and underlier-price deviation thresholds/streak-required
# counts used to be fixed module constants here; they're now
# self.cfg.mark_price_deviation_threshold /
# mark_price_deviation_streak_required / underlier_price_deviation_
# threshold / underlier_price_deviation_streak_required — see
# QFEXConfig (config.py) for the current defaults and the reasoning for
# why these moved to per-instance config (correctness depends on the
# specific symbol's mark_price/underlier_price behavior, not on
# anything intrinsic to this client — confirmed live across two
# separate incidents, 2026-07-26 CL-USD and 2026-07-27/28 CL-USD and
# GOLD-USD, that motivated first setting then re-tuning these values).

# Confirmed live (2026-07-27, CL-USD): BBO can go one-sided (bid_px or
# ask_px missing — see _parse_bbo) for an extended period — that
# incident lasted the full ~48s run — while mark_price ticks normally.
# A one-sided/empty book has no midpoint to deviate from, so this is a
# presence/absence check, not a magnitude one — no threshold field on
# QFEXConfig, just the streak count
# (self.cfg.one_sided_book_streak_required). has_two_sided_book()
# (strategy.py) does the same presence check but is stateless; this
# streak, tracked here at the base_client layer (same reasoning as
# mark_price/underlier — detection shouldn't wait on the strategy
# layer), adds memory across ticks. Per-instance config like the
# mark_price/underlier streak counts, for the same reason: how fast to
# react is a judgment call a user may want to tune, even though the
# missing-side condition itself is unambiguous regardless of symbol.
#
# Default set lower (5) than mark_price/underlier's default (10)
# deliberately: a missing side is unambiguous, not a noisy magnitude
# that could legitimately hover near a threshold, so a shorter
# confirmation window costs less and reacting slowly to something this
# severe costs more — a strategy holding a position may not even be
# able to tell its book is broken without this check.


# Key names redacted anywhere they appear, at any nesting depth, by
# _redact_for_log — not just in the one outbound auth-message shape this
# was originally scoped to. Case-sensitive exact match on the dict key,
# not a substring search. secret_key/public_key are QFEX API credentials;
# nonce/unix_ts/signature are the derived, signed auth payload described
# in _build_auth_message.
_REDACTED_LOG_KEYS = frozenset(
    {"secret_key", "nonce", "unix_ts", "signature"}
)


def _redact_for_log(msg: Any) -> Any:
    """
    Returns a copy of `msg` safe to log: any dict key in
    _REDACTED_LOG_KEYS, at any nesting depth (including inside lists),
    has its value replaced with "<redacted>". Defense in depth — no code
    path in this client should ever place a secret_key or a signed auth
    payload into a message routed through a logged send/receive path in
    the first place — but this makes that a structural guarantee rather
    than something that depends on every call site, and every future
    message shape QFEX might send, getting it right by convention.

    public_key is deliberately NOT redacted — it already appears in
    plaintext in the connection URL (_trade_url) and is useful for
    confirming which account a log line belongs to.
    """
    if isinstance(msg, dict):
        return {
            key: ("<redacted>" if key in _REDACTED_LOG_KEYS else _redact_for_log(value))
            for key, value in msg.items()
        }
    if isinstance(msg, list):
        return [_redact_for_log(item) for item in msg]
    return msg


class _QFEXBaseClient:
    """
    Shared connection/reconnect/auth/state machinery for QFEX websocket
    clients (MDS + Trade). Holds no order-placement methods of its own —
    QFEXTraderClient (trader_client.py) adds those, and is responsible
    for _handle_order_response()'s per-order terminal-status semantics,
    since "terminal" means different things for an IOC order vs. a
    resting ALO order.
    """

    def __init__(self, cfg: QFEXConfig, strategy: TraderStrategy, log_name: str):
        # log_name: logger name suffix (e.g. "qfex.taker", "qfex.maker") so
        # logs from different client types are distinguishable.
        self.cfg = cfg
        self.strategy = strategy
        self.log = logging.getLogger(log_name)
        if not self.log.hasHandlers():
            # Add a NullHandler so we don't complain if the user doesn't configure logging
            self.log.addHandler(logging.NullHandler())

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []

        # Set once the trade session has authenticated successfully (a
        # verified auth ack, not just "auth message sent") and its setup
        # messages (cancel_on_disconnect, private-channel subscribe) have
        # been sent for the current connection. See _trade_session().
        # Cleared on disconnect/reconnect so callers don't proceed against a
        # dead or re-authenticating session during the reconnect gap.
        self._trade_ready = asyncio.Event()

        # Public state snapshots (safe to read; updated on event loop thread)
        self.bbo_by_symbol: Dict[str, BBO] = {}
        # Mark price per symbol, from the dedicated MDS "mark_price"
        # channel — not derived from bbo_by_symbol, and what
        # STOP_LOSS/TAKE_PROFIT orders actually trigger against.
        self.mark_price_by_symbol: Dict[str, Decimal] = {}
        # Underlier (index) price per symbol, from the dedicated MDS
        # "underlier" channel — a different feed from mark_price (which is
        # itself derived partly from underlier — see QFEX's mark price
        # formula at docs.qfex.com/qfex/definitions). Subscribed
        # unconditionally alongside bbo/trade/mark_price (see
        # _mds_subscribe) — not every strategy needs it, but the
        # subscription itself is cheap and this avoids a second
        # configuration knob at the client layer for something only the
        # strategy layer actually consumes.
        self.underlier_price_by_symbol: Dict[str, Decimal] = {}
        # Consecutive BBO ticks (per symbol) where mark_price has
        # deviated from BBO midpoint by more than _MARK_PRICE_DEVIATION_
        # THRESHOLD. Reset to 0 the moment a tick's deviation drops back
        # under the threshold — this counts a SUSTAINED streak, not a
        # cumulative total. Updated in _handle_mds_message's bbo branch,
        # on every BBO tick regardless of trading_enabled or reentrancy
        # guards, so detection isn't slowed by whatever the strategy layer
        # happens to be doing. See is_mark_price_broken().
        self._mark_price_deviation_streak: Dict[str, int] = {}
        # Same purpose as _mark_price_deviation_streak above, tracked
        # independently for underlier_price — see
        # self.cfg.underlier_price_deviation_threshold/
        # underlier_price_deviation_streak_required and
        # is_underlier_price_broken().
        self._underlier_price_deviation_streak: Dict[str, int] = {}
        # Same purpose again, tracked independently for one-sided/empty
        # BBO — see self.cfg.one_sided_book_streak_required and
        # is_one_sided_book_broken(). A presence/absence streak, not a
        # magnitude deviation like the two above.
        self._one_sided_book_streak: Dict[str, int] = {}
        # Trading rules (tick_size, lot_size, price/quantity bounds) per
        # symbol, from QFEX's REST /refdata endpoint. Empty until
        # fetch_refdata() is called (NOT fetched automatically on connect
        # — see that method's docstring for why this is opt-in). Unlike
        # bbo_by_symbol/mark_price_by_symbol, this is not kept live/fresh
        # by any background loop — it's a one-shot snapshot, since tick
        # size and friends don't change intraday the way price does.
        self.refdata_by_symbol: Dict[str, SymbolRefdata] = {}
        self.balance: BalanceState = BalanceState()
        self.positions: Dict[str, PositionState] = {}

        # Set (per symbol) the first time a position_response for that
        # symbol has actually been received and applied to self.positions.
        # Exists so callers can distinguish "we confirmed this symbol is
        # flat" from "we haven't heard from the exchange about this symbol
        # yet" — get_position() alone can't make that distinction, since it
        # returns the same all-zero default PositionState in both cases.
        # See wait_position_known().
        self._position_seen: Dict[str, asyncio.Event] = {}

        # Sequence tracking for MDS feeds
        self._mds_seq_last: Dict[Tuple[str, str], int] = {}  # (channel, symbol) -> seq

        # Trade WS order tracking
        # QFEX OrderStatus docs: FILLED can be partial, so we compute deltas using quantity_remaining.
        self._order_last_remaining: Dict[str, Decimal] = {}  # order_id -> last quantity_remaining
        self._client_oid_to_fut: Dict[str, asyncio.Future] = {}

        # Per-in-flight-order terminal-status set, keyed by client_order_id.
        # "Terminal" means different things for different order flavors —
        # IOC/FOK/MARKET orders resolve on fill-complete or reject, never on
        # ACK (ACK is just a waypoint before the real outcome arrives in the
        # same round trip); GTC/ALO orders resolve on ACK itself (accepted,
        # resting on the book) or reject, and must NOT wait for a fill,
        # since a resting order can sit unfilled indefinitely.
        # _send_order_and_wait() registers the correct set here at send
        # time; _handle_order_response() looks it up per response instead
        # of guessing or hardcoding one rule for the whole class. This is
        # what lets ONE class (e.g. a future combined maker+taker client)
        # correctly juggle both order flavors on the same connection — each
        # in-flight order carries its own rule, rather than the class
        # carring one rule for everything it sends.
        self._client_oid_terminal_statuses: Dict[str, Tuple[str, ...]] = {}

        # client_order_id -> send time, for every order awaiting a
        # terminal order_response — inserted in _send_order_and_wait()
        # at enqueue time, removed in _handle_order_response() or
        # _handle_trade_error() once a terminal answer arrives.
        # Deliberately NOT removed on a local order_update_timeout_s
        # timeout (unlike _client_oid_to_fut, which is) — a local
        # timeout means this client gave up waiting, not that QFEX gave
        # up processing. See is_order_outstanding(), used by callers
        # like PerpetualMarketMakingStrategy._place_stop_orders to avoid
        # retrying a leg whose previous attempt may still land: confirmed
        # live (2026-07-27) that a local timeout alone is too short a
        # signal, since a "timed out" order's real ACK can arrive several
        # seconds later, and retrying then risks a genuine duplicate.
        #
        # A _TimeBoundedSet, not a plain dict, so a response that's
        # genuinely lost forever doesn't grow this unboundedly. See
        # is_order_outstanding() and QFEXConfig.order_outstanding_
        # backstop_margin_s's comments for how retention_s
        # (_ORDER_OUTSTANDING_RETENTION_S) relates to the backstop it's
        # clamped against — no longer a fixed relationship, since the
        # margin is per-instance configurable now.
        #
        # Distinct from _recently_timed_out_oids below: that exists to
        # log/flag a late response after the fact; this exists so a
        # caller can proactively ask "should I avoid a duplicate now."
        self._outstanding_client_oids = _TimeBoundedSet(
            retention_s=_ORDER_OUTSTANDING_RETENTION_S,
            max_size=_MAX_OUTSTANDING_OID_MEMORY,
        )

        # client_order_id -> recently timed out on, so
        # _handle_order_response can recognize a late-arriving response
        # for an order already given up on (possible duplicate if the
        # strategy retried).
        self._recently_timed_out_oids = _TimeBoundedSet(
            retention_s=_TIMED_OUT_OID_RETENTION_S,
            max_size=_MAX_TIMED_OUT_OID_MEMORY,
        )

        # Background tasks cancelling "orphan" resting orders — see
        # _schedule_orphan_cancel. Held here only so the tasks aren't
        # garbage-collected mid-flight (asyncio keeps weak refs only).
        self._orphan_cancel_tasks: set = set()

        # BBO conflation (added 2026-10-14). The MDS reader used to
        # `await strategy.on_bbo(b)` inline, so while a tick was waiting on
        # slow order responses (1-3 s each during the 2026-10-14 US500 data
        # spike) no new market data was read and the strategy then acted on
        # stale prices. Now the reader only stores the newest BBO per symbol
        # and a per-symbol dispatcher task feeds the strategy, always with
        # the LATEST snapshot (intermediate ticks that arrive while a tick is
        # running are skipped, not queued).
        self._pending_bbo: Dict[str, Any] = {}
        self._bbo_dispatch_tasks: Dict[str, asyncio.Task] = {}

        # Trade-session counter (added 2026-10-17). cancel_on_disconnect
        # makes QFEX cancel the account's open orders when a trade
        # connection drops, but no cancel notice arrives on the NEW
        # connection, so the strategy keeps tracking orders that may no
        # longer exist (seen live on GOLD 2026-10-17 08:30, while holding a
        # position). From the second session on, the strategy's
        # on_trade_reconnected() hook is called so it can re-verify.
        self._trade_session_count = 0

        # order_id -> recently resolved via a stop order's "phantom
        # FILLED" (see _handle_order_response, point 1b), so
        # _handle_trade_error can log a late error for it quietly instead
        # of as a fresh ERROR.
        self._resolved_stop_order_ids = _TimeBoundedSet(
            retention_s=_RESOLVED_STOP_ORDER_RETENTION_S,
            max_size=_MAX_RESOLVED_STOP_ORDER_MEMORY,
        )

        # order_id -> Future, for cancel_order() calls in flight. Separate
        # from _client_oid_to_fut/_client_oid_terminal_statuses (which are
        # keyed by client_order_id) because a cancel is addressed by
        # order_id — a different namespace on QFEX's side that must not be
        # conflated with client_order_id.
        self._cancel_oid_to_fut: Dict[str, asyncio.Future] = {}

        # set_user_leverage tracking. The exchange's ack_response for this
        # call carries no correlation ID (no symbol, no request id) — unlike
        # add_order/cancel_order responses, which carry client_order_id /
        # order_id. That makes it structurally impossible to match a given
        # ack_response back to a specific set_user_leverage() call if more
        # than one is in flight at once. Rather than guess-match (first ack
        # resolves whichever call happened to be waiting), this serializes:
        # only one set_user_leverage call may be in flight at a time, and a
        # second call while one is pending raises immediately. See
        # set_user_leverage() docstring for the reasoning and the doc-stated
        # constraint (leverage can't be changed with open orders/position)
        # that makes serialization a reasonable real-world fit anyway.
        self._leverage_lock = asyncio.Lock()
        self._leverage_fut: Optional[asyncio.Future] = None

        # Outbound trade messages go through a single writer task
        self._trade_out_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(
            maxsize=cfg.outbound_queue_max
        )

        # Websocket handles
        self._mds_ws: Optional[ClientConnection] = None
        self._trade_ws: Optional[ClientConnection] = None

    # ---------- Public API ----------

    async def __aenter__(self) -> "_QFEXBaseClient":
        self.log.info(
            "starting %s; is_prod=%s symbols=%s",
            type(self).__name__,
            self.cfg.is_prod,
            self.cfg.symbol_list,
        )
        # Logs the actual live values this running process loaded for
        # every price-feed circuit breaker knob — confirmed live
        # (2026-07-28/29, CL-USD) that "did my config edit actually take
        # effect" is a real, hard-to-diagnose question otherwise: editing
        # config.py on disk does nothing to a process that was already
        # running before the edit was saved (a stale/leftover process,
        # not the freshly-launched one the person believes is active),
        # and there was previously no way to tell the two apart from the
        # log alone — an observed circuit-breaker trip had to be
        # reverse-engineered against an assumed threshold instead of
        # checked against the actual one. One line, all five knobs
        # together, so this is answerable by grepping the start of any
        # log rather than re-deriving it from a deviation calculation
        # after the fact.
        self.log.info(
            "circuit breaker config: mark_price_deviation_threshold=%s "
            "mark_price_deviation_streak_required=%s "
            "underlier_price_deviation_threshold=%s "
            "underlier_price_deviation_streak_required=%s "
            "one_sided_book_streak_required=%s",
            self.cfg.mark_price_deviation_threshold,
            self.cfg.mark_price_deviation_streak_required,
            self.cfg.underlier_price_deviation_threshold,
            self.cfg.underlier_price_deviation_streak_required,
            self.cfg.one_sided_book_streak_required,
        )
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._run_mds_loop(), name="mds_loop"),
            asyncio.create_task(self._run_trade_loop(), name="trade_loop"),
        ]
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.log.info("stopping %s", type(self).__name__)
        self.stop()
        for t in self._tasks:
            t.cancel()
        for t in list(self._bbo_dispatch_tasks.values()):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []
        await self._safe_close(self._mds_ws)
        await self._safe_close(self._trade_ws)

    def stop(self) -> None:
        self._stop.set()

    async def wait_stopped(self) -> None:
        """
        Waits until stop() has been called. Useful when you're managing the
        `async with client:` context yourself (e.g. to run setup logic like
        set_user_leverage() after connecting but before blocking) rather
        than using run(), which does `async with self: await
        self.wait_stopped()` internally.
        """
        await self._stop.wait()

    def is_trade_connected(self) -> bool:
        """True while a trade session is authenticated and set up."""
        return self._trade_ready.is_set()

    def _purge_stale_trade_queue(self) -> int:
        """Drop messages queued during a disconnect. Added 2026-10-19: on
        Oct 18 a 11-minute outage left 138 MARKET closes in the queue and all
        of them went out in one burst on reconnect. Their callers have long
        since timed out, cancel_on_disconnect already removed the resting
        orders, and the strategy re-checks everything on reconnect, so none
        of them is still wanted."""
        dropped: Dict[str, int] = {}
        while True:
            try:
                msg = self._trade_out_q.get_nowait()
            except asyncio.QueueEmpty:
                break
            t = str(msg.get("type"))
            dropped[t] = dropped.get(t, 0) + 1
        n = sum(dropped.values())
        if n:
            self.log.warning(
                "trade reconnect: dropped %d stale queued message(s) from the "
                "previous session instead of sending them late: %s",
                n, dropped,
            )
        return n

    async def wait_trade_ready(self, timeout: Optional[float] = None) -> None:
        """
        Waits until the trade session has authenticated and its setup
        messages (cancel_on_disconnect, private-channel subscribe) have
        been sent. Call once after entering the client, before sending
        any trade commands.

        Raises asyncio.TimeoutError if `timeout` elapses first. A hard
        auth failure looks the same as "still not ready" from here (it
        surfaces via _run_trade_loop's retry/backoff instead) — check
        logs for "trade auth failed" if this times out.
        """
        if timeout is None:
            await self._trade_ready.wait()
        else:
            await asyncio.wait_for(self._trade_ready.wait(), timeout=timeout)

    def get_position(self, symbol: str) -> PositionState:
        return self.positions.get(symbol, PositionState(symbol=symbol))

    async def wait_position_known(
        self, symbol: str, timeout: Optional[float] = None
    ) -> bool:
        """
        Waits until at least one position_response for `symbol` has been
        received. Exists because get_position() can't otherwise
        distinguish "confirmed flat" from "unknown, defaulting to zero" —
        both look identical. Matters most before set_user_leverage(),
        which QFEX rejects if position or open_orders is non-zero.

        Returns True once seen (within `timeout`, or immediately if
        already seen). Returns False — does not raise — if `timeout`
        elapses first.
        """
        event = self._position_seen.setdefault(symbol, asyncio.Event())
        if timeout is None:
            await event.wait()
            return True
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def is_order_outstanding(self, client_order_id: str) -> bool:
        """
        True if `client_order_id` was sent via _send_order_and_wait()
        and hasn't yet received a terminal answer from QFEX — whether
        that arrived promptly or late, after this client's own
        order_update_timeout_s gave up waiting locally.

        Exists so a caller retrying a placement (e.g.
        PerpetualMarketMakingStrategy._place_stop_orders after a
        STOP_LOSS/TAKE_PROFIT timeout) can check "did the previous
        attempt actually resolve" instead of "has enough time passed" —
        confirmed live (2026-07-27, CL-USD) that a fixed cooldown isn't
        a safe substitute: QFEX can take longer than
        order_update_timeout_s to respond, and retrying before the
        original resolves risks a genuine duplicate order.

        Returns False (safe to retry) once EITHER a terminal response
        arrives for this client_order_id OR the backstop
        (self.cfg.order_update_timeout_s +
        self.cfg.order_outstanding_backstop_margin_s) elapses since it
        was sent — whichever first, so a response that's genuinely lost
        doesn't block retries forever. Also False for a client_order_id
        with no record at all (never sent, or already cleaned up).

        The backstop is derived from self.cfg, not a fixed value,
        specifically so it scales with whatever order_update_timeout_s
        this instance is configured with — see order_outstanding_
        backstop_margin_s's comment in config.py for why a fixed
        backstop wasn't safe. Defensively clamped to never exceed
        _ORDER_OUTSTANDING_RETENTION_S: past that point
        _TimeBoundedSet.peek() would already report the entry as expired
        (returning None, treated as "not outstanding" via the `elapsed
        is None` branch below) regardless of this method's own
        comparison, so an unclamped backstop larger than retention would
        never actually apply — clamping makes that ceiling explicit
        rather than an accidental side effect of a different constant.
        """
        elapsed = self._outstanding_client_oids.peek(client_order_id)
        if elapsed is None:
            return False
        backstop = min(
            self.cfg.order_update_timeout_s + self.cfg.order_outstanding_backstop_margin_s,
            _ORDER_OUTSTANDING_RETENTION_S,
        )
        return elapsed < backstop

    def get_margin_usage(self) -> Dict[str, Decimal]:
        """
        Simple margin view derived from balances stream (pulsed every second).
        """
        return {
            "order_margin": self.balance.order_margin,
            "position_margin": self.balance.position_margin,
            "available_balance": self.balance.available_balance,
        }

    def is_mark_price_broken(self, symbol: str) -> bool:
        """
        True if mark_price_by_symbol[symbol] has deviated from BBO
        midpoint by more than self.cfg.mark_price_deviation_threshold
        for self.cfg.mark_price_deviation_streak_required consecutive
        ticks — see _update_mark_price_deviation_streak and
        QFEXConfig's comments for these fields.

        Doesn't distinguish why (frozen feed, stuck exchange-side value,
        etc.) — only that mark price and the live book have disagreed
        long enough that mark price shouldn't be trusted for STOP_LOSS/
        TAKE_PROFIT triggers. Confirmed live (2026-07-26, CL-USD) this
        happens while the feed itself keeps ticking — a value check, not
        a liveness/staleness check, since the incident showed the feed
        alive and updating while the value stayed wrong.
        """
        return (
            self._mark_price_deviation_streak.get(symbol, 0)
            >= self.cfg.mark_price_deviation_streak_required
        )

    def is_underlier_price_broken(self, symbol: str) -> bool:
        """
        Same check as is_mark_price_broken(), independently tracked
        against self.cfg.underlier_price_deviation_threshold/
        underlier_price_deviation_streak_required — see
        _update_underlier_price_deviation_streak.

        mark_price and underlier_price are separate MDS feeds serving
        different purposes: mark_price is what stop triggers evaluate
        against, underlier_price feeds PerpetualMarketMakingStrategy's
        quoting reference price. A break in one is a distinct failure
        from a break in the other, so this is a separate signal —
        callers that care about both must check both explicitly.
        """
        return (
            self._underlier_price_deviation_streak.get(symbol, 0)
            >= self.cfg.underlier_price_deviation_streak_required
        )

    def is_one_sided_book_broken(self, symbol: str) -> bool:
        """
        True if bbo_by_symbol[symbol] has been one-sided or fully empty
        (bid_px or ask_px missing) for
        self.cfg.one_sided_book_streak_required consecutive ticks — see
        _update_one_sided_book_streak.

        Confirmed live (2026-07-27, CL-USD): BBO can go one-sided for an
        extended period (missing bid) while mark_price ticks correctly —
        this catches that specifically. Not redundant with
        is_mark_price_broken/is_underlier_price_broken: those compare a
        feed's value against BBO midpoint, which needs a two-sided book
        to even compute — there's nothing for them to check during
        exactly the condition this detects. Callers wanting full
        coverage must check all three explicitly.
        """
        return (
            self._one_sided_book_streak.get(symbol, 0)
            >= self.cfg.one_sided_book_streak_required
        )

    async def fetch_refdata(
        self, symbol: str, timeout: Optional[float] = None
    ) -> SymbolRefdata:
        """
        Fetches trading rules (tick_size, lot_size, price/quantity bounds)
        for `symbol` from QFEX's REST /refdata endpoint
        (GET https://api.{domain}/refdata?ticker={symbol}, no auth
        required), stores the result in self.refdata_by_symbol[symbol],
        and returns it. Needed because QFEX rejects add_order calls whose
        price isn't a multiple of the symbol's tick_size — see
        PerpetualMarketMakingStrategy for where this gets applied.

        Not fetched automatically on connect — call this explicitly during
        startup if your strategy computes prices that need rounding.

        `timeout` defaults to self.cfg.refdata_fetch_timeout_s if not
        given — pass an explicit value here to override just this one
        call without changing the instance's general REST timeout.

        A plain REST call (via stdlib urllib in a background thread), not
        a websocket message like the rest of this client.

        UAT REST host (api.qfex.io) is inferred by analogy with the
        confirmed mds./trade. hosts, not independently verified — see
        _rest_url().

        Raises asyncio.TimeoutError on timeout, ValueError if `symbol`
        isn't in the response, or the underlying urllib error for
        connection/HTTP failures.
        """
        if timeout is None:
            timeout = self.cfg.refdata_fetch_timeout_s

        import urllib.error
        import urllib.request
        from urllib.parse import urlencode

        url = f"{self._rest_url()}/refdata?{urlencode({'ticker': symbol})}"

        def _do_request() -> Dict[str, Any]:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))

        loop = asyncio.get_running_loop()
        body = await asyncio.wait_for(
            loop.run_in_executor(None, _do_request), timeout=timeout
        )

        entries = body.get("data") or []
        match = next((e for e in entries if e.get("symbol") == symbol), None)
        if match is None:
            raise ValueError(
                f"QFEX /refdata response for ticker={symbol!r} did not "
                f"include a matching symbol entry (got {len(entries)} "
                f"entries) — check the symbol is correct"
            )

        refdata = SymbolRefdata(
            symbol=symbol,
            tick_size=Decimal(str(match["tick_size"])),
            lot_size=Decimal(str(match["lot_size"])),
            min_price=Decimal(str(match["min_price"])),
            max_price=Decimal(str(match["max_price"])),
            min_quantity=Decimal(str(match["min_quantity"])),
            max_quantity=Decimal(str(match["max_quantity"])),
        )
        self.refdata_by_symbol[symbol] = refdata
        self.log.info(
            "refdata fetched: %s tick_size=%s lot_size=%s",
            symbol,
            refdata.tick_size,
            refdata.lot_size,
        )
        return refdata

    async def set_user_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """
        Sets the leverage for `symbol`. `leverage` must be an integer —
        QFEX rejects a float despite its docs showing one in the example
        payload.

        Fails exchange-side if you have any open orders or a non-zero
        position on that symbol — call this before placing anything, not
        mid-strategy. Not checked locally.

        Serialized: only one call may be in flight at a time (the
        exchange's ack carries no correlation ID, so concurrent calls
        can't be matched to responses). A second call while one is
        pending raises RuntimeError immediately.

        Returns the raw response dict. Raises asyncio.TimeoutError on
        timeout, or QFEXTradeError if QFEX rejects the call via the
        {"err": {...}} envelope (e.g. leverage can't change with open
        orders/position).
        """
        if self._leverage_lock.locked():
            raise RuntimeError(
                "a set_user_leverage call is already in flight; wait for it "
                "to resolve before calling again (see docstring — the "
                "exchange's ack_response has no correlation ID, so this "
                "can't be safely parallelized)"
            )

        async with self._leverage_lock:
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._leverage_fut = fut

            msg = {
                "type": "set_user_leverage",
                "params": {"symbol": symbol, "leverage": leverage},
            }

            await self._enqueue_trade(msg)
            try:
                result = await asyncio.wait_for(
                    fut, timeout=self.cfg.order_update_timeout_s
                )
                if "error" in result:
                    raise QFEXTradeError(result["error"])
                return result
            finally:
                self._leverage_fut = None

    # ---------- Order placement scaffolding (used by QFEXTraderClient's order-entry methods) ----------

    async def _send_order_and_wait(
        self,
        *,
        symbol: str,
        side: Side,
        quantity: Decimal,
        order_type: str,
        time_in_force: Optional[str],
        price: Optional[Decimal],
        terminal_statuses: Tuple[str, ...],
        client_order_id: Optional[str] = None,
        reduce_only: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Shared send-and-wait scaffolding for every order-placement method
        (send_gtc_alo, send_ioc_limit, send_market, etc.) — each differs
        only in what payload fields to send and which statuses count as
        terminal for its wait.

        `time_in_force`/`price` are Optional since not every order type
        sends them (e.g. MARKET sends neither); pass None to omit the
        field entirely. `reduce_only`, if given, tells QFEX this order may
        only shrink or close an existing position — never open a new one
        or flip its sign — a real, exchange-enforced guarantee, not
        something this client validates locally. `terminal_statuses` is
        registered per-order in self._client_oid_terminal_statuses and
        consulted by _handle_order_response — this per-order registration
        is what lets one class correctly place both resting and immediate
        order types on the same connection.

        Raises ValueError on a client_order_id collision or unknown
        symbol, asyncio.TimeoutError on timeout, or QFEXTradeError if
        QFEX rejects the order via the {"err": {...}} envelope.
        """
        if symbol not in self.cfg.symbol_list:
            raise ValueError(f"symbol {symbol} not in configured symbol_list")

        client_oid = client_order_id or secrets.token_hex(16)
        if client_oid in self._client_oid_to_fut:
            raise ValueError("client_order_id already in use")

        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._client_oid_to_fut[client_oid] = fut
        self._client_oid_terminal_statuses[client_oid] = terminal_statuses
        # Tracked from send time, not from the eventual outcome — see
        # is_order_outstanding() / _outstanding_client_oids' docstring in
        # __init__. Cleared in _handle_order_response (normal path) or
        # _handle_trade_error (err-envelope rejection path) once a
        # terminal answer actually arrives, NOT in this method's own
        # finally block below — a local wait_for timeout must NOT clear
        # this, since QFEX may still resolve the order after we've given
        # up waiting locally.
        self._outstanding_client_oids.insert(client_oid)

        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side.value,
            "order_type": order_type,
            "quantity": float(quantity),
            "take_profit": 0,
            "stop_loss": 0,
            "client_order_id": client_oid,
        }
        if time_in_force is not None:
            params["order_time_in_force"] = time_in_force
        if price is not None:
            params["price"] = float(price)
        if reduce_only is not None:
            params["reduce_only"] = reduce_only

        msg = {"type": "add_order", "params": params}

        await self._enqueue_trade(msg)
        try:
            result = await asyncio.wait_for(
                fut, timeout=self.cfg.order_update_timeout_s
            )
            if "error" in result:
                raise QFEXTradeError(result["error"])
            return result
        except asyncio.TimeoutError:
            # Record the abandonment: QFEX may still be processing this
            # request and respond after we've given up waiting — this is
            # what lets _handle_order_response recognize a late response
            # and log it, rather than silently absorbing it.
            self._recently_timed_out_oids.insert(client_oid)
            raise
        finally:
            self._client_oid_to_fut.pop(client_oid, None)
            # Deliberately NOT popping _client_oid_terminal_statuses here
            # (unlike _client_oid_to_fut above). Confirmed live
            # (2026-07-27, CL-USD): a late order_response can still
            # arrive after this timeout, and _handle_order_response needs
            # the per-order-type terminal-status set (ACK vs. FILLED vs.
            # reject-family — see trader_client.py's _ALO_TERMINAL_
            # STATUSES/_STOP_TERMINAL_STATUSES/etc.) to recognize that
            # late response as terminal and clear is_order_outstanding()
            # for it. Popping it here used to mean a late ACK had no way
            # to be recognized as terminal — only the universal
            # FILLED-with-zero-remaining rule still applied, so an
            # ACK'd-but-not-filled late response silently failed to
            # resolve is_order_outstanding. Now cleared in
            # _handle_order_response instead, at the same moment
            # _outstanding_client_oids is cleared.

    async def _send_cancel_and_wait(
        self, order_id: str, symbol: str
    ) -> Dict[str, Any]:
        """
        Shared send-and-wait scaffolding for cancel_order(). Separate from
        _send_order_and_wait() because a cancel is addressed by order_id
        (not client_order_id) and its response shape/terminal-status rule
        is fixed and universal — there's no per-order-type variation to
        parameterize here the way there is for placing new orders, so this
        doesn't need the same generality.

        Raises ValueError if `order_id` already has a cancel in flight.
        Raises asyncio.TimeoutError if no terminal response arrives within
        cfg.order_update_timeout_s. Raises QFEXTradeError if QFEX rejects
        the cancel via the top-level {"err": {...}} envelope — see
        _handle_trade_error / QFEXTradeError.
        """
        msg = {
            "type": "cancel_order",
            "params": {
                "order_id": order_id,
                "symbol": symbol,
                "cancel_order_id_type": "order_id",
            },
        }
        return await self._send_cancel_msg_and_wait(order_id, "order_id", msg)

    async def _send_stop_cancel_and_wait(self, stop_order_id: str) -> Dict[str, Any]:
        """
        Shared send-and-wait scaffolding for cancel_stop_order(). Separate
        from _send_cancel_and_wait() because QFEX tracks stop orders
        (TAKE_PROFIT/STOP_LOSS) in a different registry from regular
        orders — cancel_stop_order sends {"type": "cancel_stop_order",
        "params": {"stop_order_id": ...}}, a different message type and
        shape from cancel_order's. The response still arrives as a normal
        order_response envelope, so this reuses self._cancel_oid_to_fut
        and _handle_order_response's existing resolution logic.

        Raises ValueError if `stop_order_id` already has a cancel in
        flight, asyncio.TimeoutError if no terminal response arrives
        within cfg.order_update_timeout_s, or QFEXTradeError if QFEX
        rejects the cancel via the {"err": {...}} envelope.
        """
        msg = {
            "type": "cancel_stop_order",
            "params": {"stop_order_id": stop_order_id},
        }
        return await self._send_cancel_msg_and_wait(stop_order_id, "stop_order_id", msg)

    async def _send_cancel_msg_and_wait(
        self, key: str, key_label: str, msg: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Shared body behind _send_cancel_and_wait/_send_stop_cancel_and_
        wait — both track a pending cancel Future in
        self._cancel_oid_to_fut keyed by `key` (an order_id or
        stop_order_id — same dict, since both cancel kinds resolve
        through _handle_order_response's step 3, which doesn't
        distinguish which registry `key` came from), enqueue `msg`, wait
        for order_update_timeout_s, and raise QFEXTradeError on an
        {"error": ...}-wrapped result. `key_label` ("order_id" or
        "stop_order_id") is only for the ValueError message below, so it
        still names which kind of id collided. The two callers differ
        only in `key`'s meaning and `msg`'s shape — see their own
        docstrings.

        Raises ValueError if `key` already has a cancel in flight.
        """
        if key in self._cancel_oid_to_fut:
            raise ValueError(f"cancel already in flight for {key_label} {key}")

        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._cancel_oid_to_fut[key] = fut

        await self._enqueue_trade(msg)
        try:
            result = await asyncio.wait_for(
                fut, timeout=self.cfg.order_update_timeout_s
            )
            if "error" in result:
                raise QFEXTradeError(result["error"])
            return result
        finally:
            self._cancel_oid_to_fut.pop(key, None)

    def _schedule_orphan_cancel(self, order_id: str, symbol: str, client_oid: str) -> None:
        """
        Fire-and-forget cancel of an orphaned resting order (see step 0 of
        _handle_order_response). Must NOT be awaited inline there: the
        cancel's own response is processed by the same receive loop that
        is currently running _handle_order_response, so awaiting it
        would deadlock until the cancel times out.
        """
        self.log.warning(
            "late ACK for ALO client_order_id=%s — the caller already gave "
            "up on it, so nothing tracks this resting order. Cancelling "
            "order_id=%s so it cannot fill unmanaged.",
            client_oid,
            order_id,
        )
        task = asyncio.create_task(
            self._cancel_orphan(order_id, symbol), name=f"orphan_cancel_{order_id[:8]}"
        )
        self._orphan_cancel_tasks.add(task)
        task.add_done_callback(self._orphan_cancel_tasks.discard)

    async def _cancel_orphan(self, order_id: str, symbol: str, attempts: int = 3) -> None:
        for attempt in range(1, attempts + 1):
            try:
                result = await self._send_cancel_and_wait(order_id, symbol)
                status = (result.get("final_order_response") or {}).get("status")
                self.log.warning(
                    "orphan ALO order_id=%s cancel attempt %d/%d: status=%s",
                    order_id, attempt, attempts, status,
                )
                return
            except QFEXTradeError as e:
                # NO_SUCH_ORDER etc.: the order is already gone (filled or
                # cancelled) — nothing left to cancel.
                self.log.warning(
                    "orphan ALO order_id=%s cancel rejected (%s) — treating "
                    "as already gone", order_id, e,
                )
                return
            except Exception as e:  # timeout, cancel already in flight, ...
                self.log.error(
                    "orphan ALO order_id=%s cancel attempt %d/%d failed: %r",
                    order_id, attempt, attempts, e,
                )
                await asyncio.sleep(1.0)
        self.log.critical(
            "could NOT confirm cancel of orphan ALO order_id=%s after %d "
            "attempts — check the QFEX UI and cancel it manually",
            order_id, attempts,
        )

    # ---------- Run loop ----------

    async def run(self) -> None:
        """
        Starts MDS + Trade connections and runs until stop() is called.
        """
        async with self:
            await self.wait_stopped()

    # ---------- Internals ----------

    def _domain(self) -> str:
        return "qfex.com" if self.cfg.is_prod else "qfex.io"

    def _mds_url(self) -> str:
        return f"wss://mds.{self._domain()}"

    def _trade_url(self) -> str:
        return f"wss://trade.{self._domain()}?api_key={self.cfg.public_key}"

    def _rest_url(self) -> str:
        """
        Base URL for QFEX's REST API (currently only used by
        fetch_refdata()). UNVERIFIED for the UAT (is_prod=False) case —
        QFEX's docs confirm the PROD REST host as api.qfex.com (see
        docs.qfex.com/api-reference/rest/market-data/refdata, OpenAPI
        `servers` block), but do not state a UAT REST host anywhere this
        codebase has found. api.qfex.io is inferred purely by following
        the SAME domain-swap pattern already confirmed correct for the
        mds./trade. websocket hosts above (_mds_url/_trade_url) — it has
        NOT been independently confirmed against a live UAT connection
        the way those were. If fetch_refdata() fails against UAT
        specifically (connection error, 404), this guess is the first
        thing to check.
        """
        return f"https://api.{self._domain()}"

    def _build_auth_message(self) -> Dict[str, Any]:
        """
        HMAC auth: signature = HMAC_SHA256(secret, f"{nonce}:{unix_ts}")
        """
        nonce = secrets.token_hex(16)
        unix_ts = int(time.time())
        payload = f"{nonce}:{unix_ts}".encode("utf-8")
        signature = hmac.new(
            self.cfg.secret_key.encode("utf-8"), payload, hashlib.sha256
        ).hexdigest()
        return {
            "type": "auth",
            "params": {
                "hmac": {
                    "public_key": self.cfg.public_key,
                    "nonce": nonce,
                    "unix_ts": unix_ts,
                    "signature": signature,
                }
            },
        }

    async def _enqueue_trade(self, msg: Dict[str, Any]) -> None:
        # If we're stopping, refuse new orders
        if self._stop.is_set():
            raise RuntimeError("client is stopping; cannot send")
        try:
            self._trade_out_q.put_nowait(msg)
            self.log.debug(
                "trade enqueued (qsize=%d): %s",
                self._trade_out_q.qsize(),
                _redact_for_log(msg),
            )
        except asyncio.QueueFull:
            raise RuntimeError("trade outbound queue full (backpressure)")

    async def _run_mds_loop(self) -> None:
        await self._run_reconnect_loop(self._mds_session, "mds")

    async def _run_trade_loop(self) -> None:
        await self._run_reconnect_loop(self._trade_session, "trade")

    async def _run_reconnect_loop(self, session: Any, label: str) -> None:
        """
        Shared body behind _run_mds_loop/_run_trade_loop — both are the
        same reconnect-with-backoff wrapper around a session coroutine
        (_mds_session/_trade_session), differing only in which session
        to run and what to call it in the reconnect log line. `session`
        is a zero-arg async callable; `label` is "mds" or "trade".
        """
        delay = self.cfg.reconnect_base_delay_s
        while not self._stop.is_set():
            try:
                await session()
                delay = self.cfg.reconnect_base_delay_s
            except asyncio.CancelledError:
                return
            except Exception:
                self.log.exception("%s loop error; reconnecting", label)
                await asyncio.sleep(self._backoff(delay))
                delay = min(delay * 2, self.cfg.reconnect_max_delay_s)

    def _backoff(self, base_delay: float) -> float:
        # exponential backoff with jitter
        return base_delay * (0.7 + random.random() * 0.6)

    async def _mds_session(self) -> None:
        url = self._mds_url()
        self.log.info("connecting mds: %s", url)
        async with websockets.connect(
            url,
            open_timeout=self.cfg.connect_timeout_s,
            ping_interval=self.cfg.heartbeat_interval_s,
            ping_timeout=self.cfg.heartbeat_timeout_s,
            close_timeout=self.cfg.close_timeout_s,
            max_queue=self.cfg.mds_max_queue,
        ) as ws:
            self._mds_ws = ws
            await self._mds_subscribe(ws)
            async for raw in ws:
                await self._handle_mds_message(raw)

    async def _mds_subscribe(self, ws: ClientConnection) -> None:
        # Subscribe to BBO, public trades, mark price, and underlier for
        # configured symbols. mark_price and underlier are separate
        # dedicated channels (docs.qfex.com/websocket/mds), not derivable
        # from bbo — see mark_price_by_symbol/underlier_price_by_symbol's
        # docstrings in __init__. Sent as one message per channel (the
        # original shape this code used) rather than consolidated into
        # one multi-channel message — preserved as-is since it's not
        # confirmed whether QFEX's API accepts multiple channels in a
        # single "channels" list; a loop over channels reproduces the
        # exact same four messages, just without repeating the dict
        # literal four times.
        for channel in ("bbo", "trade", "mark_price", "underlier"):
            await ws.send(json.dumps({
                "type": "subscribe",
                "channels": [channel],
                "symbols": self.cfg.symbol_list,
            }))
        self.log.info(
            "mds subscribed: bbo+trade+mark_price+underlier symbols=%s",
            self.cfg.symbol_list,
        )

    def _on_trade_session_ready(self) -> None:
        self._trade_session_count += 1
        if self._trade_session_count < 2:
            return
        hook = getattr(self.strategy, "on_trade_reconnected", None)
        if hook is None:
            return
        self.log.warning(
            "trade connection re-established (session %d) — asking the "
            "strategy to re-verify its resting orders (cancel_on_disconnect "
            "may have removed them)",
            self._trade_session_count,
        )
        task = asyncio.create_task(self._run_reconnect_hook(hook), name="trade_reconnect_hook")
        self._orphan_cancel_tasks.add(task)
        task.add_done_callback(self._orphan_cancel_tasks.discard)

    async def _run_reconnect_hook(self, hook: Any) -> None:
        try:
            await hook()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("strategy.on_trade_reconnected failed")

    def _dispatch_bbo(self, b: Any) -> None:
        """Hand a BBO to the strategy without blocking the MDS reader."""
        self._pending_bbo[b.symbol] = b
        task = self._bbo_dispatch_tasks.get(b.symbol)
        if task is None or task.done():
            self._bbo_dispatch_tasks[b.symbol] = asyncio.create_task(
                self._bbo_dispatch_loop(b.symbol), name=f"bbo_dispatch_{b.symbol}"
            )

    async def _bbo_dispatch_loop(self, symbol: str) -> None:
        try:
            while True:
                b = self._pending_bbo.pop(symbol, None)
                if b is None:
                    return
                try:
                    await self.strategy.on_bbo(b)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.log.exception("strategy.on_bbo failed for %s", symbol)
        finally:
            if self._bbo_dispatch_tasks.get(symbol) is asyncio.current_task():
                self._bbo_dispatch_tasks.pop(symbol, None)

    async def _handle_mds_message(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        msg = json.loads(raw)
        mtype = msg.get("type")
        if mtype == "bbo":
            b = self._parse_bbo(msg)
            self._sequence_check("bbo", b.symbol, b.sequence)
            self.bbo_by_symbol[b.symbol] = b
            self._update_mark_price_deviation_streak(b)
            self._update_underlier_price_deviation_streak(b)
            self._update_one_sided_book_streak(b)
            self._dispatch_bbo(b)
        elif mtype == "trade":
            # public trades stream
            sym = msg.get("symbol")
            seq = msg.get("sequence")
            if isinstance(sym, str) and isinstance(seq, int):
                self._sequence_check("trade", sym, seq)
            await self.strategy.on_trade(msg)
        elif mtype == "mark_price":
            # docs.qfex.com/websocket/mds — no "sequence" field on this
            # message type (unlike bbo/trade), so there's nothing to pass
            # to _sequence_check here; that's a real difference in what
            # QFEX sends, not an oversight. No strategy callback fires for
            # this today (see mark_price_by_symbol's docstring in __init__)
            # — this just keeps the snapshot current for whichever code
            # reads self.mark_price_by_symbol directly (e.g. a PMM
            # strategy's tick loop, or a future stop-order proximity
            # check). Add an on_mark_price callback later if something
            # needs to react the instant a new mark price arrives, rather
            # than reading the latest value on its own schedule.
            symbol = msg.get("symbol")
            price = self._parse_mark_price(msg)
            if isinstance(symbol, str) and price is not None:
                self.mark_price_by_symbol[symbol] = price
        elif mtype == "underlier":
            # Same shape/no-sequence/no-callback situation as mark_price
            # above — see that branch's comment, and
            # underlier_price_by_symbol's docstring in __init__. Reuses
            # _parse_mark_price since the payload shape (just a "price"
            # field to extract) is identical between the two message
            # types; QFEX's underlier message additionally carries a
            # "source" field (internal/external) this client doesn't
            # currently use.
            symbol = msg.get("symbol")
            price = self._parse_mark_price(msg)
            if isinstance(symbol, str) and price is not None:
                self.underlier_price_by_symbol[symbol] = price
        else:
            return

    def _sequence_check(self, channel: str, symbol: str, seq: Optional[int]) -> None:
        if seq is None:
            return
        key = (channel, symbol)
        last = self._mds_seq_last.get(key)
        if last is not None and seq <= last:
            self.log.warning(
                "mds out-of-order: channel=%s symbol=%s last=%s now=%s",
                channel,
                symbol,
                last,
                seq,
            )
        self._mds_seq_last[key] = seq

    def _parse_bbo(self, msg: Dict[str, Any]) -> BBO:
        symbol = msg["symbol"]
        bid = msg.get("bid") or []
        ask = msg.get("ask") or []
        bid_px, bid_qty = (None, None)
        ask_px, ask_qty = (None, None)
        if bid and isinstance(bid, list) and len(bid[0]) >= 2:
            bid_px = Decimal(str(bid[0][0]))
            bid_qty = Decimal(str(bid[0][1]))
        if ask and isinstance(ask, list) and len(ask[0]) >= 2:
            ask_px = Decimal(str(ask[0][0]))
            ask_qty = Decimal(str(ask[0][1]))
        return BBO(
            symbol=symbol,
            bid_px=bid_px,
            bid_qty=bid_qty,
            ask_px=ask_px,
            ask_qty=ask_qty,
            sequence=msg.get("sequence"),
            ts=msg.get("time"),
        )

    def _parse_mark_price(self, msg: Dict[str, Any]) -> Optional[Decimal]:
        """
        Extracts the price from a mark_price MDS message
        ({"type": "mark_price", "symbol": ..., "price": "437.45", "time":
        ...}, per docs.qfex.com/websocket/mds). Returns None if "price" is
        missing rather than raising — mirrors this codebase's general
        preference (see _parse_bbo above, and _update_balance/_update_
        position) for defensively defaulting/skipping on an unexpected
        shape over crashing the MDS read loop on one bad message.
        """
        price = msg.get("price")
        if price is None:
            return None
        return Decimal(str(price))

    def _update_mark_price_deviation_streak(self, bbo: BBO) -> None:
        """
        Called on every BBO tick (from _handle_mds_message, unconditionally
        — not gated by trading_enabled or any strategy-level guard, so
        detection isn't slowed by whatever the strategy is doing). Updates
        self._mark_price_deviation_streak[bbo.symbol] via
        _update_price_deviation_streak — see that method for the shared
        logic, and is_mark_price_broken() for how this streak is
        consumed.
        """
        self._update_price_deviation_streak(
            bbo,
            price_by_symbol=self.mark_price_by_symbol,
            threshold=self.cfg.mark_price_deviation_threshold,
            streak_by_symbol=self._mark_price_deviation_streak,
        )

    def _update_underlier_price_deviation_streak(self, bbo: BBO) -> None:
        """
        Same as _update_mark_price_deviation_streak above, for
        underlier_price_by_symbol /
        self.cfg.underlier_price_deviation_threshold /
        _underlier_price_deviation_streak instead. See
        _update_price_deviation_streak for the shared logic.
        """
        self._update_price_deviation_streak(
            bbo,
            price_by_symbol=self.underlier_price_by_symbol,
            threshold=self.cfg.underlier_price_deviation_threshold,
            streak_by_symbol=self._underlier_price_deviation_streak,
        )

    def _update_price_deviation_streak(
        self,
        bbo: BBO,
        *,
        price_by_symbol: Dict[str, Decimal],
        threshold: Decimal,
        streak_by_symbol: Dict[str, int],
    ) -> None:
        """
        Shared body behind _update_mark_price_deviation_streak and
        _update_underlier_price_deviation_streak — both compare a
        single reference price feed (mark_price or underlier_price)
        against this tick's BBO midpoint and maintain a consecutive-
        ticks-over-threshold streak, differing only in which price dict,
        threshold, and streak dict to use. Kept as one parameterized
        method (rather than two independent copies) since the two
        callers have stayed byte-for-byte identical apart from those
        three values through every change made to either check so far —
        if a real behavioral difference between the two is ever needed,
        split them back out rather than adding a branch here.

        Increments streak_by_symbol[bbo.symbol] if the referenced price
        deviates from this tick's BBO midpoint by more than `threshold`;
        resets to 0 otherwise. No-ops (leaves the streak untouched) if
        bbo is one-sided (missing bid or ask — can't compute a midpoint)
        or if no price has been seen yet for this symbol in
        `price_by_symbol` — an absence of data is not evidence the feed
        is broken, just that there's nothing to compare yet.
        """
        if bbo.bid_px is None or bbo.ask_px is None:
            return
        price = price_by_symbol.get(bbo.symbol)
        if price is None:
            return

        mid = (bbo.bid_px + bbo.ask_px) / 2
        if mid == 0:
            return
        deviation = abs(price - mid) / mid

        if deviation > threshold:
            streak_by_symbol[bbo.symbol] = streak_by_symbol.get(bbo.symbol, 0) + 1
        else:
            streak_by_symbol[bbo.symbol] = 0

    def _update_one_sided_book_streak(self, bbo: BBO) -> None:
        """
        Called on every BBO tick (from _handle_mds_message, unconditionally
        — same reasoning as the mark_price/underlier streak updates: this
        detection must not be gated by whatever the strategy layer is
        doing). Increments self._one_sided_book_streak[bbo.symbol] if
        `bbo` is missing either side (bid_px or ask_px is None —
        includes the fully-empty-book case, where both are None); resets
        to 0 the moment a genuinely two-sided tick arrives.

        Unlike _update_price_deviation_streak above, this is NOT a
        magnitude/deviation check — there's no meaningful "how
        one-sided" measurement, just present-or-not, and no reference
        price to look up — so it doesn't share that helper; forcing it
        into the same shape would mean passing in unused/dummy
        parameters just to fit. See self.cfg.one_sided_book_streak_
        required and is_one_sided_book_broken() for how this streak is
        consumed.
        """
        if bbo.bid_px is None or bbo.ask_px is None:
            self._one_sided_book_streak[bbo.symbol] = (
                self._one_sided_book_streak.get(bbo.symbol, 0) + 1
            )
        else:
            self._one_sided_book_streak[bbo.symbol] = 0

    async def _trade_session(self) -> None:
        url = self._trade_url()
        self.log.info("connecting trade: %s", url)
        async with websockets.connect(
            url,
            open_timeout=self.cfg.connect_timeout_s,
            ping_interval=self.cfg.heartbeat_interval_s,
            ping_timeout=self.cfg.heartbeat_timeout_s,
            close_timeout=self.cfg.close_timeout_s,
            max_queue=self.cfg.trade_max_queue,
        ) as ws:
            self._trade_ws = ws
            # Authenticate within 1 minute of connecting.
            #
            # Sent directly via ws.send(), NOT via _enqueue_trade()/
            # _trade_writer() — those log every message at DEBUG via
            # _redact_for_log(), which redacts auth payloads defensively,
            # but the stronger guarantee is never routing auth through a
            # logged path at all. Don't "simplify" this to go through the
            # normal enqueue path for consistency — keep it a direct,
            # unlogged send; _redact_for_log() is a safety net, not
            # permission to route auth through logging.
            await ws.send(json.dumps(self._build_auth_message()))

            # Wait for and verify the auth ack before doing anything else.
            # QFEX's docs (docs.qfex.com/websocket/channels/trade/
            # authenticate) document success as {"type": "auth", "result":
            # "success"} — confirmed against live prod that the real
            # server actually sends {"authenticated": true} instead; the
            # docs are wrong/stale here. This checks what the server
            # actually sends, not what the docs claim.
            #
            # Distinct shape from order_response/ack_response/balance_
            # response/position_response, so read directly here rather
            # than through _handle_trade_message (which doesn't know this
            # shape and would just fall through all its branches).
            #
            # Auth is verified BEFORE cancel_on_disconnect/subscribe are
            # sent and before _trade_ready is set — previously this sent
            # those unconditionally and set _trade_ready regardless of
            # auth outcome, so a bad API key let callers proceed to
            # set_user_leverage()/place orders only to fail or hang
            # individually instead of failing fast here.
            auth_ack_raw = await asyncio.wait_for(
                ws.recv(), timeout=self.cfg.connect_timeout_s
            )
            if isinstance(auth_ack_raw, bytes):
                auth_ack_raw = auth_ack_raw.decode("utf-8")
            auth_ack = json.loads(auth_ack_raw)
            if auth_ack.get("authenticated") is not True:
                # Anything other than {"authenticated": true} is a
                # failure — no partial-success semantics assumed. Raising
                # lets _run_trade_loop's existing except/backoff/retry
                # handle it like any other trade-session error.
                #
                # The raw auth_ack body is deliberately NOT logged: this
                # reaches self.log.exception() in _run_trade_loop, and
                # unlike order/cancel responses (documented, known not to
                # echo credentials), QFEX's auth-failure shape isn't
                # confirmed here. _redact_for_log only strips
                # params.hmac.{nonce,unix_ts,signature} from our OUTBOUND
                # auth message — a different shape from this inbound
                # response — so it would be a silent no-op if applied
                # here. If the failure response ever echoes back what was
                # sent, logging it raw risks leaking the signed nonce/
                # signature. Only the boolean outcome is logged; capture
                # the raw response manually, out of band, if you need it
                # to diagnose a specific failure.
                raise RuntimeError(
                    "trade auth failed: server did not return "
                    "authenticated=true (response body withheld from logs "
                    "as a precaution — see comment above)"
                )
            self.log.info("trade auth succeeded")

            # Enable cancel-on-disconnect each reconnect
            await ws.send(
                json.dumps(
                    {
                        "type": "cancel_on_disconnect",
                        "params": {"cancel_on_disconnect": True},
                    }
                )
            )
            # Subscribe to private channels we need
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "params": {
                            "channels": ["order_responses", "balances", "positions"]
                        },
                    }
                )
            )
            # Now that auth is confirmed and setup messages are sent,
            # callers waiting on wait_trade_ready() can safely start sending
            # their own trade commands (add_order, set_user_leverage, etc.)
            if self._trade_session_count >= 1:
                self._purge_stale_trade_queue()
            self._trade_ready.set()
            # Start a single writer that flushes outbound messages
            writer = asyncio.create_task(self._trade_writer(ws), name="trade_writer")
            self._on_trade_session_ready()
            try:
                async for raw in ws:
                    await self._handle_trade_message(raw)
            finally:
                # Session ending (reconnect or shutdown) — clear readiness
                # so a caller can't proceed against a dead connection during
                # the reconnect gap. It will be set again once the next
                # session re-authenticates successfully.
                self._trade_ready.clear()
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)

    async def _trade_writer(self, ws: ClientConnection) -> None:
        while True:
            msg = await self._trade_out_q.get()
            try:
                await ws.send(json.dumps(msg))
                self.log.debug("trade sent: %s", _redact_for_log(msg))
            except ConnectionClosed:
                # put it back so it can be retried after reconnect (best-effort)
                try:
                    self._trade_out_q.put_nowait(msg)
                except asyncio.QueueFull:
                    self.log.error(
                        "dropping outbound trade msg due to full queue after disconnect: %s",
                        _redact_for_log(msg),
                    )
                raise

    async def _handle_trade_message(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        msg = json.loads(raw)
        self.log.debug("trade received: %s", _redact_for_log(msg))
        # Errors envelope
        if "err" in msg:
            await self._handle_trade_error(msg["err"])
            return

        if "balance_response" in msg:
            self._update_balance(msg["balance_response"])
            return

        if "position_response" in msg:
            self._update_position(msg["position_response"])
            return

        if "order_response" in msg:
            await self._handle_order_response(msg["order_response"])
            return

        if "ack" in msg:
            # set_user_leverage's actual response shape, confirmed live:
            #   {"ack": {"incoming_message": {"type": "set_user_leverage",
            #                                  "params": {...}}}}
            # Does NOT match docs.qfex.com's documented example
            # ({"ack_response": true}) — third confirmed case of QFEX's
            # docs not matching real behavior this session (see the
            # auth-ack and leverage-int fixes). Trusting reality here too.
            #
            # This envelope only confirms receipt, not that the leverage
            # change succeeded — no captured example yet of a REJECTED
            # set_user_leverage on the wire. If you hit that case, capture
            # the payload via this DEBUG logging and extend this branch to
            # distinguish success from failure.
            #
            # Matched on incoming_message.type, not just "ack" presence,
            # since other commands can be acked this way too — only
            # resolve the leverage future for an ack actually about it.
            incoming = msg["ack"].get("incoming_message", {})
            incoming_type = incoming.get("type")
            if incoming_type == "set_user_leverage":
                if self._leverage_fut is not None and not self._leverage_fut.done():
                    self._leverage_fut.set_result(msg)
                else:
                    self.log.warning(
                        "received set_user_leverage ack with no pending "
                        "call: %s",
                        msg,
                    )
                return
            if incoming_type == "cancel_on_disconnect":
                # Sent once per session right after auth, no future
                # waiting on it — just confirms QFEX received the
                # setting. DEBUG, not WARNING: expected every connect,
                # not an unaccounted-for shape. `msg` was already logged
                # (redacted) above; this just confirms which branch
                # handled it.
                self.log.debug("cancel_on_disconnect acked")
                return
            if incoming_type == "cancel_stop_order":
                # Receipt confirmation only — the real result arrives
                # separately as a normal order_response, which is what
                # actually resolves the pending cancel future. Same
                # already-logged-once reasoning as cancel_on_disconnect.
                self.log.debug("cancel_stop_order acked")
                return
            # An ack for some other message type we don't specifically
            # handle yet — fall through to the catch-all below so it's at
            # least visible, rather than silently matching this branch and
            # doing nothing.

        if "subscribed" in msg:
            # Confirmation for one of the three private-channel subscribes
            # sent in _trade_session right after auth (order_responses,
            # balances, positions). Nothing to do with these beyond noting
            # the subscription went through — logged at DEBUG since this is
            # expected exactly once per channel per connect, not a shape
            # this client has failed to account for.
            self.log.debug("subscribed: %s", msg["subscribed"])
            return

        # Nothing above matched. This is a real gap worth surfacing loudly:
        # every branch above returns once it handles a known shape, so a
        # message that doesn't match any of them was previously dropped
        # with zero trace. Log at WARNING (not DEBUG) so it's visible even
        # without --log-level DEBUG. `msg` itself is not repeated here —
        # it was already logged, redacted, at the top of this method; if
        # you need the payload, correlate against that DEBUG-level "trade
        # received" line rather than re-logging the raw message a second
        # time here, since this is exactly the branch most likely to see
        # a shape this codebase hasn't accounted for and therefore hasn't
        # confirmed is safe to log unredacted.
        self.log.warning(
            "trade message did not match any known shape (incoming_type=%s)",
            msg.get("type"),
        )

    def _update_balance(self, br: Dict[str, Any]) -> None:
        self.balance.deposit = Decimal(str(br.get("deposit", 0)))
        self.balance.realised_pnl = Decimal(str(br.get("realised_pnl", 0)))
        self.balance.unrealised_pnl = Decimal(str(br.get("unrealised_pnl", 0)))
        self.balance.net_funding = Decimal(str(br.get("net_funding", 0)))
        self.balance.order_margin = Decimal(str(br.get("order_margin", 0)))
        self.balance.position_margin = Decimal(str(br.get("position_margin", 0)))
        self.balance.available_balance = Decimal(str(br.get("available_balance", 0)))
        # Added 2026-11-13: let the strategy record account P&L snapshots (see
        # PerpetualMarketMakingStrategy.on_balance). Optional hook, never fatal.
        hook = getattr(self.strategy, "on_balance", None)
        if hook is not None:
            try:
                hook(br)
            except Exception:
                self.log.exception("strategy.on_balance failed")

    def _update_position(self, pr: Dict[str, Any]) -> None:
        sym = pr["symbol"]
        ps = self.positions.get(sym, PositionState(symbol=sym))
        ps.position = Decimal(str(pr.get("position", 0)))
        ps.margin_alloc = Decimal(str(pr.get("margin_alloc", 0)))
        ps.realised_pnl = Decimal(str(pr.get("realised_pnl", 0)))
        ps.unrealised_pnl = Decimal(str(pr.get("unrealised_pnl", 0)))
        ps.net_funding = Decimal(str(pr.get("net_funding", 0)))
        ps.open_orders = Decimal(str(pr.get("open_orders", 0)))
        ps.open_quantity = Decimal(str(pr.get("open_quantity", 0)))
        ps.leverage = Decimal(str(pr.get("leverage", 0)))
        ps.initial_margin = Decimal(str(pr.get("initial_margin", 0)))
        ps.maintenance_margin = Decimal(str(pr.get("maintenance_margin", 0)))
        ps.average_price = Decimal(str(pr.get("average_price", 0)))
        self.positions[sym] = ps
        self._position_seen.setdefault(sym, asyncio.Event()).set()

    async def _handle_trade_error(self, err: Dict[str, Any]) -> None:
        """
        Handles the top-level {"err": {...}} envelope QFEX sends when a
        trade command is rejected outside the normal order_response flow.
        Resolves the pending Future for the rejected command (if any)
        with {"error": err}, so the caller fails immediately instead of
        waiting out order_update_timeout_s.

        Correlates by err.incoming_message.type:
          - add_order         -> client_order_id -> _client_oid_to_fut
          - cancel_order      -> order_id -> _cancel_oid_to_fut
          - cancel_stop_order -> stop_order_id -> _cancel_oid_to_fut
          - set_user_leverage -> no correlation ID; resolves whatever's
            pending in self._leverage_fut

        Logs at ERROR if nothing matches — except a cancel_stop_order
        error for an order already in self._resolved_stop_order_ids,
        which logs at DEBUG since the strategy already knows that order
        resolved.
        """
        code = err.get("error_code")

        incoming = err.get("incoming_message")
        incoming_type = incoming.get("type") if isinstance(incoming, dict) else None
        params = incoming.get("params", {}) if isinstance(incoming, dict) else {}

        already_resolved_elapsed: Optional[float] = None
        already_resolved_stop_order_id: Optional[str] = None
        if incoming_type == "cancel_stop_order":
            candidate = params.get("stop_order_id")
            elapsed = self._resolved_stop_order_ids.peek(candidate) if candidate else None
            if elapsed is not None:
                already_resolved_stop_order_id = candidate
                already_resolved_elapsed = elapsed

        if already_resolved_stop_order_id is not None:
            self.log.debug(
                "trade error for cancel_stop_order(stop_order_id=%s) — "
                "already known resolved via phantom-FILLED %.3fs earlier; "
                "not logging as ERROR since the strategy has already "
                "handled this order's resolution. err=%s",
                already_resolved_stop_order_id,
                already_resolved_elapsed,
                err,
            )
        else:
            self.log.error("trade error: %s", err)

        resolved = False
        if incoming_type == "add_order":
            client_oid = params.get("client_order_id")
            fut = self._client_oid_to_fut.get(client_oid) if client_oid else None
            if fut and not fut.done():
                fut.set_result({"error": err})
                resolved = True
            # An err-envelope rejection is just as terminal an answer as
            # a normal order_response — clear outstanding tracking
            # regardless of whether a live future was still around to
            # receive it (a LATE err-envelope rejection, arriving after
            # this client's local timeout already popped `fut`, still
            # needs to clear this — see is_order_outstanding).
            if client_oid:
                self._outstanding_client_oids.pop(client_oid)
        elif incoming_type == "cancel_order":
            order_id = params.get("order_id")
            fut = self._cancel_oid_to_fut.get(order_id) if order_id else None
            if fut and not fut.done():
                fut.set_result({"error": err})
                resolved = True
        elif incoming_type == "cancel_stop_order":
            stop_order_id = params.get("stop_order_id")
            fut = self._cancel_oid_to_fut.get(stop_order_id) if stop_order_id else None
            if fut and not fut.done():
                fut.set_result({"error": err})
                resolved = True
        elif incoming_type == "set_user_leverage":
            if self._leverage_fut is not None and not self._leverage_fut.done():
                self._leverage_fut.set_result({"error": err})
                resolved = True

        if not resolved:
            self.log.debug(
                "trade error did not match any pending future "
                "(incoming_message.type=%s) — nothing to resolve, error "
                "already logged above",
                incoming_type,
            )

        # If we rate-limit, slow down a bit — unchanged from before this
        # fix, still applies regardless of whether a future was resolved.
        if code == "RateLimited":
            await asyncio.sleep(0.25 + random.random() * 0.5)

    async def _handle_order_response(self, orsp: Dict[str, Any]) -> None:
        """
        Final implementation, not meant to be overridden. Handles, for
        every order type QFEXTraderClient can place, uniformly:

        0. Detecting a late response for a client_order_id already timed
           out on (see _recently_timed_out_oids).
        1. Fill-delta bookkeeping + strategy.on_fill dispatch.
        1b. Detecting a stop order's "phantom FILLED" (status FILLED but
            no trade_id — the order triggered and QFEX spawned a separate
            order to execute the trade, rather than filling this one) and
            firing strategy.on_stop_order_resolved.
        2. Resolving a pending new-order Future, using the terminal-status
           set registered per-client_order_id at send time — this is what
           lets one class place both resting and immediate order types on
           the same connection without their rules interfering.
        3. Resolving a pending cancel Future, keyed by order_id.
        """
        order_id = orsp.get("order_id")
        client_oid = orsp.get("client_order_id") or ""
        symbol = orsp.get("symbol")
        status = orsp.get("status")
        qty = Decimal(str(orsp.get("quantity", 0)))
        remaining = Decimal(str(orsp.get("quantity_remaining", qty)))
        px = Decimal(str(orsp.get("price", 0)))

        # 0. Late response for a client_order_id we already gave up
        # waiting on — doesn't change how the response is handled below,
        # just adds visibility that a timed-out request may still have
        # gone through, possibly as a duplicate if the strategy retried.
        elapsed = self._recently_timed_out_oids.pop(client_oid) if client_oid else None
        if elapsed is not None:
            self.log.error(
                "late response for client_order_id=%s, %.1fs after "
                "this client gave up waiting on it "
                "(order_update_timeout_s=%s) — check for a duplicate "
                "order if the strategy retried. order_id=%s status=%s "
                "resp=%s",
                client_oid,
                elapsed,
                self.cfg.order_update_timeout_s,
                order_id,
                status,
                orsp,
            )
            # A late ACK for a resting ALO order is an ORPHAN: the caller
            # (e.g. the strategy's _place_quote) already gave up waiting,
            # so nothing is tracking this order_id and nothing will ever
            # cancel or reprice it. Confirmed live (2026-09-28, GOLD-USD):
            # two bids ACKed 7-8s late sat on the book for 26 minutes,
            # then both filled while the strategy was already long,
            # tripling the position to 3x order_amount. Cancel it now.
            if (
                status == "ACK"
                and orsp.get("type") == "ALO"
                and isinstance(order_id, str)
                and order_id
                and isinstance(symbol, str)
            ):
                self._schedule_orphan_cancel(order_id, symbol, client_oid)

        # 1. Detect fill delta for this order_id — only for a status that
        # reflects a REAL order state (see _FILL_TRACKABLE_STATUSES). A
        # reject-family status (NO_SUCH_ORDER, REJECTED, INVALID_*, etc.)
        # means QFEX has no real order to describe, and its quantity
        # fields come back zeroed/templated rather than meaningful — so
        # this is skipped entirely for those rather than diffed, which
        # previously produced phantom fills (see that constant's
        # docstring for the live incident). _order_last_remaining is
        # also left untouched in that case, so a genuine future response
        # for this order_id still compares against the last REAL
        # remaining seen, not a poisoned zero from a templated reject.
        if isinstance(order_id, str) and order_id and status in _FILL_TRACKABLE_STATUSES:
            last_rem = self._order_last_remaining.get(order_id, qty)
            # filled = qty - remaining, so delta fill = last_remaining - remaining
            filled_qty_delta = last_rem - remaining
            if filled_qty_delta > 0:
                fill_payload = {
                    "order_response": orsp,
                    "filled_qty_delta": filled_qty_delta,
                    "filled_notional_delta": filled_qty_delta * px,
                    "symbol": symbol,
                    "client_order_id": client_oid,
                    "order_id": order_id,
                }
                await self.strategy.on_fill(fill_payload)
            self._order_last_remaining[order_id] = remaining

        # 1b/1c below are independent of fill-delta tracking above — they
        # key off `status` directly, not off the trackable-status gate,
        # since both describe order LIFECYCLE events (a stop order
        # triggering, or being auto-cancelled) rather than a fill
        # quantity that needs the real order state to diff correctly.
        if isinstance(order_id, str) and order_id:
            # 1b. "Phantom FILLED": when a STOP_LOSS/TAKE_PROFIT order
            # triggers, QFEX spawns a SEPARATE order to execute the trade
            # rather than filling this one — but still sends an
            # order_response for the original order_id claiming FILLED,
            # with quantity_remaining unchanged and trade_id missing.
            # That order_id becomes uncancellable afterward, so this
            # fires on_stop_order_resolved so the strategy stops tracking
            # it rather than retrying a cancel against a dead id forever.
            if status == "FILLED" and orsp.get("trade_id") is None:
                self._resolved_stop_order_ids.insert(order_id)
                await self.strategy.on_stop_order_resolved(order_id, orsp)

            # 1c. Externally-cancelled stop order: confirmed live that
            # when one leg of a STOP_LOSS/TAKE_PROFIT bracket triggers,
            # QFEX auto-cancels the OTHER leg — a normal CANCELLED
            # order_response this client never asked for (order_id NOT
            # in _cancel_oid_to_fut, which step 3 checks for cancels this
            # client initiated). Without this, the strategy's tracking
            # for that leg never clears, so a later cleanup cancel
            # targets an order QFEX already discarded — confirmed live
            # that gets a repeated ServerError until the retry cap gives
            # up. Reuses on_stop_order_resolved rather than a separate
            # callback, since strategies would handle both identically.
            elif (
                status == "CANCELLED"
                and orsp.get("order_origin") == "STOP_ORDER"
                and order_id not in self._cancel_oid_to_fut
            ):
                self._resolved_stop_order_ids.insert(order_id)
                await self.strategy.on_stop_order_resolved(order_id, orsp)

        # 2. Resolve a pending new-order future, using its registered
        # per-order terminal-status set. FILLED-with-zero-remaining is
        # always terminal regardless of registered set, since a fully
        # filled order is unambiguously done no matter how it was
        # submitted.
        #
        # is_terminal is computed regardless of whether `fut` is still
        # live, to also cover a LATE response arriving after
        # _send_order_and_wait's wait_for already gave up locally — see
        # that method's finally block for why _client_oid_terminal_
        # statuses survives past a local timeout specifically to make
        # this possible.
        terminal_statuses = self._client_oid_terminal_statuses.get(client_oid, ())
        is_terminal = status in terminal_statuses
        if status == "FILLED" and remaining == 0:
            is_terminal = True

        fut = self._client_oid_to_fut.get(client_oid)
        if fut and not fut.done() and is_terminal:
            fut.set_result({"final_order_response": orsp})

        if is_terminal and client_oid:
            # Clear both pieces of per-order tracking the instant a
            # terminal answer is recognized (via terminal_statuses or
            # the FILLED rule) — this is what lets is_order_outstanding()
            # flip to False right away rather than only at the backstop.
            self._outstanding_client_oids.pop(client_oid)
            self._client_oid_terminal_statuses.pop(client_oid, None)

        # 3. Resolve a pending cancel_order() future, keyed by order_id.
        if isinstance(order_id, str) and order_id in self._cancel_oid_to_fut:
            cancel_fut = self._cancel_oid_to_fut[order_id]
            if not cancel_fut.done() and status in _CANCEL_TERMINAL_STATUSES:
                cancel_fut.set_result({"final_order_response": orsp})

    async def _safe_close(self, ws: Optional[ClientConnection]) -> None:
        if ws is None:
            return
        try:
            await ws.close()
        except Exception:
            return
