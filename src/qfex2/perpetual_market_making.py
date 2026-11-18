"""
PerpetualMarketMakingStrategy

A port of Hummingbot's perpetual_market_making strategy onto qfex2.
Single symbol, single order level (one resting bid, one resting ask) —
simpler than Hummingbot's version, which supports multiple levels,
inventory skew, and an external price source; none of that is ported.

State machine, same shape as Hummingbot's tick():
    position == 0   -> QUOTING: rest a bid and ask around BBO midpoint,
                        refresh if the market moves past a tolerance band.
    position != 0   -> POSITION-MANAGING: cancel resting quotes (no fresh
                        quoting while holding a position), manage the exit
                        via a resting stop-loss and take-profit order.

Driven by on_bbo, not a clock — each BBO tick is this strategy's tick().
guard_reentrant() (strategy.py) prevents overlapping ticks.

Differences from Hummingbot:

1. Exits are native QFEX STOP_LOSS/TAKE_PROFIT orders, not Hummingbot's
   manual poll-and-chase. QFEX's stop orders trigger server-side against
   mark price and convert to an immediate MARKET order — guaranteed to
   fire (modulo book liquidity), unlike Hummingbot's limit order which
   can sit unfilled with no escalation.

2. Both legs are Stop MARKET orders — QFEX has no native "stop limit"
   order type. A reduce_only-ALO-based take-profit is a deferred
   alternative, not built here.

3. Renew-if-stale is ported, but purely time-based: re-place at a fresh
   price after enough time has passed, not Hummingbot's local
   breach-detection-and-chase (which QFEX's native trigger replaces).

4. No order_levels, inventory skew, external price source, order
   optimization, or budget/margin pre-check — QFEX's own
   FAILED_MARGIN_CHECK rejection is relied on instead. Minimal viable
   port, not feature-complete.

5. Quoting reference price is a configurable blend of underlier price
   and BBO midpoint (see _compute_mid), not pure BBO midpoint and not
   mark price. Mark price is still what stop orders trigger against.

6. Circuit breakers (mark_price / underlier_price / one-sided book /
   order-error streak) PAUSE new quoting instead of shutting the process
   down: resting quotes are cancelled, no new ones are placed, and
   quoting resumes on its own once the trigger has stayed clear for
   PerpetualMarketMakingConfig.breaker_resume_healthy_ticks consecutive
   ticks (order-error breaker: once its cooldown has elapsed). A position
   that is already open keeps being managed the whole time (BBO-
   triggered close + native stop orders). The original one-shot
   cancel-everything / force-close / client.stop() behavior is still
   available via breaker_hard_stop=True — see _tick.

7. Spread gate: a separate, always-soft pause on the live BBO spread —
   quoting pauses when the spread stays above quote_pause_spread for
   quote_pause_spread_bad_ticks consecutive ticks and the gate clears
   once it drops below quote_resume_spread (then the usual
   breaker_resume_healthy_ticks countdown applies). Needs no underlier
   feed, so it works at mid_price_underlier_weight == 0, where the
   underlier breaker is skipped. Added after the 2026-09-18 16:31 GOLD-USD
   incident (thin book, ask jumped 4392 -> 4440, BBO mid ~0.6% above the
   underlier; three bids placed off that mid were filled within seconds
   for ~140 bps). Unlike the breakers above it never triggers the
   breaker_hard_stop shutdown — see _update_spread_gate.
"""

from __future__ import annotations

import asyncio
import secrets
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Tuple

from .base_client import QFEXTradeError
from .trader_client import QFEXTraderClient
from .models import BBO, Side
from .strategy import TraderStrategy
from .trigger_logger import TriggerLogger

# _MAX_STOP_CANCEL_FAILURES / _MARK_PRICE_BREAK_MARKET_CLOSE_MAX_ATTEMPTS
# used to be fixed module constants here; they're now
# self.client.cfg.max_stop_cancel_failures /
# mark_price_break_market_close_max_attempts — see QFEXConfig
# (config.py) for defaults and the reasoning for living there: both are
# strategy-agnostic retry policy, not something specific to this
# strategy's design, even though this strategy is their only consumer
# today.

# Per-reason pieces for _handle_price_feed_broken's CRITICAL log line —
# keyed by the `reason` string each is_*_broken() check reports (see
# _tick). Each entry is (is_deviation, value_getter, streak_required_
# getter):
#   is_deviation: True for the two magnitude checks (mark_price,
#     underlier_price), which share "has deviated from BBO midpoint"
#     phrasing; False for one_sided_book, worded differently since
#     there's no midpoint to have deviated from.
#   value_getter: given the strategy instance, returns the broken feed's
#     current reading to report — None for one_sided_book (no such
#     value).
#   streak_required_getter: given the strategy instance, returns the
#     confirmation-window count to cite in the log — all three read
#     this from strat.client.cfg (see QFEXConfig), since all three are
#     per-instance configurable, not fixed. Wrapped in a lambda uniformly
#     across all three entries even though a plain attribute lookup
#     would do, so the table's third slot always has one shape
#     (a callable) rather than mixing callables with bare values.
# A new circuit-breaker cause later means adding one entry here, not a
# branch in _log_price_feed_broken to keep in sync with the others.
# (Also reused by _log_quoting_paused for the pause-mode log line.)
_PRICE_FEED_BROKEN_REASONS: Dict[str, tuple] = {
    "mark_price": (
        True,
        lambda strat: strat.client.mark_price_by_symbol.get(strat.symbol),
        lambda strat: strat.client.cfg.mark_price_deviation_streak_required,
    ),
    "underlier_price": (
        True,
        lambda strat: strat.client.underlier_price_by_symbol.get(strat.symbol),
        lambda strat: strat.client.cfg.underlier_price_deviation_streak_required,
    ),
    "one_sided_book": (
        False,
        None,
        lambda strat: strat.client.cfg.one_sided_book_streak_required,
    ),
}


@dataclass(frozen=True)
class PerpetualMarketMakingConfig:
    """
    Tunable parameters for PerpetualMarketMakingStrategy. A frozen
    dataclass (same pattern as QFEXConfig in config.py) rather than loose
    constructor args, since there are enough of these that keyword-only
    construction with named fields is meaningfully more readable at the
    call site than a long positional/keyword arg list on the strategy's
    own __init__.

    Spread/price fields are fractions, not percentages — 0.001 means
    0.1%, matching how Hummingbot's start.py divides its config map's
    percentage inputs by Decimal('100') before constructing the strategy
    (see perpetual_market_making/start.py) rather than working in raw
    percentage points internally. Keeping qfex2's config in the same
    post-conversion units avoids a second implicit unit change at a
    different layer.
    """

    order_amount: Decimal
    # Quoting (only used while position == 0)
    bid_spread: Decimal  # fraction below mid, e.g. Decimal("0.001") = 0.1%
    ask_spread: Decimal  # fraction above mid
    order_refresh_tolerance: Decimal = Decimal("0.0002")  # fraction; see _quotes_are_stale
    price_ceiling: Optional[Decimal] = None  # None = no ceiling (Hummingbot default -1)
    price_floor: Optional[Decimal] = None  # None = no floor

    # Reference ("mid") price for quoting is a weighted blend of underlier
    # price and BBO midpoint:
    #   mid = mid_price_underlier_weight * underlier_price
    #       + (1 - mid_price_underlier_weight) * (bbo.bid_px + bbo.ask_px) / 2
    # rather than pure BBO midpoint. Default (0.8) weights underlier
    # heavily — underlier is a slower-moving index/reference price, less
    # susceptible to momentary BBO noise/thin-book spikes than raw BBO
    # midpoint alone.
    #
    # Only ONE weight is a field; the BBO weight is derived as
    # `1 - mid_price_underlier_weight` (see mid_price_bbo_weight property
    # below), not a second independent field. Two independently-settable
    # weights that are supposed to sum to 1 is a representable-but-
    # invalid state (nothing stops underlier_weight=0.8, bbo_weight=0.8)
    # — collapsing to one field makes that state unrepresentable rather
    # than validated after the fact. This gives up the ability to shrink
    # mid toward zero or amplify it beyond the blended price, which was
    # possible before this change — deliberately, in favor of "weights
    # always sum to 1" being structurally guaranteed.
    mid_price_underlier_weight: Decimal = Decimal("0.8")

    # Position exit (only used while position != 0)
    stop_loss_spread: Decimal = Decimal("0.01")  # fraction beyond entry, adverse direction
    take_profit_spread: Decimal = Decimal("0.01")  # fraction beyond entry, favorable direction

    # Consecutive BBO ticks the stop-loss trigger must stay crossed
    # before _maybe_bbo_triggered_close actually fires the close — a
    # confirmation window, same shape as the circuit breakers'
    # *_streak_required fields, but scoped to this one mechanism and
    # exposed via --bbo-stop-loss-confirm-streak in perpetual_maker.py
    # (unlike QFEXConfig's circuit-breaker fields, which are deliberately
    # code-only) — this one's a trading parameter under active
    # iteration, not an infrequently-tuned reliability knob, so it
    # belongs with the other CLI-exposed strategy parameters here.
    # Default 1 means "fire on the very first crossing tick" — today's
    # original behavior, unchanged unless explicitly configured higher.
    #
    # Deliberately does NOT apply to take-profit: raised at the user's
    # request (2026-08-02) specifically to make stop-loss less reactive
    # to a brief BBO wick that might reverse before a real move is
    # confirmed — asymmetric because a slow take-profit only costs some
    # unrealized gain (bounded, gradual), while a slow stop-loss means
    # staying in a position actively moving against you for longer (cost
    # compounds the longer confirmation takes) — the two failure modes
    # aren't symmetric, so there's no case made yet for slowing down
    # take-profit the same way.
    bbo_stop_loss_confirm_streak: int = 1

    # Added 2026-10-19 (GOLD, Sun 08:38 UTC): QFEX's GOLD book emptied to
    # bid 4084 / ask 4784 while mark stayed at 4161. The BBO stop-loss saw
    # the 4084 bid and kept trying to MARKET-sell into the hole. When the
    # spread is wider than this, the book is treated as broken and the BBO
    # stop-loss only fires if mark_price agrees the stop is hit; otherwise
    # the exchange-side (mark-triggered) STOP_LOSS is left to do the job.
    # 0.005 = 50 bps; normal GOLD/US500 spreads are 1-5 bps.
    bbo_close_max_spread: Decimal = Decimal("0.005")

    # Circuit-breaker RESPONSE (what to do once a breaker in QFEXConfig has
    # detected a problem — the detection thresholds/streaks themselves
    # stay on QFEXConfig, code-only, as before). These three live here
    # rather than on QFEXConfig because they describe how THIS strategy
    # reacts (pause quoting vs. shut down), not how a feed is judged
    # broken; and defaults here mean perpetual_maker.py needs no change.
    #
    # breaker_hard_stop=False (default): a tripped breaker only PAUSES new
    # quoting — resting quotes are cancelled, no new ones are placed, an
    # open position keeps being managed, and quoting resumes by itself
    # (see _update_pause_state). True restores the original one-shot
    # behavior: cancel everything, force-close any position, client.stop(),
    # manual restart required (_handle_price_feed_broken /
    # _handle_order_error_streak_broken / _execute_emergency_shutdown).
    breaker_hard_stop: bool = False
    # Consecutive evaluated ticks with NO breaker condition active before
    # a paused strategy resumes quoting. The price-feed breakers' streaks
    # reset to 0 on the very first healthy tick, so without this a feed
    # hovering around its threshold would flap pause/resume every other
    # tick; 20 ticks is ~10s at the ~0.5s BBO cadence seen in the logs.
    breaker_resume_healthy_ticks: int = 20
    # Order-error breaker only: how long quoting stays paused after
    # order_error_streak_required consecutive failures (any further
    # failure while the streak is still >= required re-arms the timer).
    # Needed because that streak is reset only by an ACK/FILLED response
    # — and a paused, flat strategy places nothing, so no ACK could ever
    # arrive to clear it. On resume the streak is reset to 0, so a
    # still-broken exchange re-trips after order_error_streak_required
    # fresh failures rather than being forgiven silently.
    order_error_pause_cooldown_s: float = 30.0

    # Spread gate (see module docstring item 7 and _update_spread_gate).
    # Fractions of BBO mid, like every other spread field here — 0.002 is
    # 20 bps. Calibrated on GOLD-USD logs 2026-09-15..18: normal spread
    # median ~4-5 bps, 99.9th percentile ~7.5 bps, versus 35-120 bps
    # during the 16:31 incident — so 20 bps sits ~4x above normal. THE
    # RIGHT VALUE IS SYMBOL-SPECIFIC: on a symbol whose ordinary spread is
    # anywhere near this, the gate would pause constantly — check that
    # symbol's spread distribution before running it. None disables the
    # gate entirely.
    quote_pause_spread: Optional[Decimal] = Decimal("0.002")
    # Consecutive two-sided ticks above quote_pause_spread before pausing.
    # Default 1 = pause on the first blown-out tick. Chosen from the four
    # days of GOLD-USD logs (2026-09-15..18): requiring 2 ticks would
    # have let the first bad tick of an episode through, and a quote
    # placed off a blown-out mid is exactly what got filled at 16:31 —
    # while 1 tick only adds ~5 extra ~10-second pauses across all four
    # days (~17 min total paused out of 96h, vs ~16 min at 2 ticks).
    # Raise it if a symbol's book flickers wide for a single tick often.
    quote_pause_spread_bad_ticks: int = 1
    # The gate clears the first tick the spread is back below this
    # (hysteresis: strictly below quote_pause_spread, so a spread hovering
    # around the pause threshold can't flap the gate), after which
    # breaker_resume_healthy_ticks must still elapse before quoting resumes.
    quote_resume_spread: Decimal = Decimal("0.0012")

    def __post_init__(self) -> None:
        if (
            self.quote_pause_spread is not None
            and self.quote_resume_spread >= self.quote_pause_spread
        ):
            raise ValueError(
                "quote_resume_spread (%s) must be below quote_pause_spread "
                "(%s), otherwise the spread gate has no hysteresis"
                % (self.quote_resume_spread, self.quote_pause_spread)
            )

    @property
    def mid_price_bbo_weight(self) -> Decimal:
        """
        Derived, not stored — always `1 - mid_price_underlier_weight`, so
        the two weights sum to exactly 1 by construction. See
        mid_price_underlier_weight's comment above for why this isn't a
        second independent field.
        """
        return Decimal("1") - self.mid_price_underlier_weight


class PerpetualMarketMakingStrategy(TraderStrategy):
    """
    See module docstring for the full design. Single symbol per instance
    — construct one of these per symbol you want to quote.
    """

    def __init__(
        self,
        client: QFEXTraderClient,
        symbol: str,
        cfg: PerpetualMarketMakingConfig,
        trigger_log_file: Optional[str] = None,
        taker_ledger_file: Optional[str] = None,
        pnl_ledger_file: Optional[str] = None,
    ) -> None:
        super().__init__(client)
        self.symbol = symbol
        self.cfg = cfg
        # Added 2026-10-31: one JSON line per TAKER fill (every non-ALO fill = the bot's
        # exits), read by bot_supervisor.py for the weekly taker-volume cap / pace.
        # None = don't record (tests, ad-hoc runs).
        self.taker_ledger_file = taker_ledger_file
        # Added 2026-11-13: account realised P&L + fees, one JSON line whenever either
        # changes; the supervisor uses it to judge recent cost per $10k.
        self.pnl_ledger_file = pnl_ledger_file
        self._pnl_last: Optional[Tuple[float, float]] = None

        # Appends one JSONL record per successfully-ACKed STOP_LOSS/
        # TAKE_PROFIT placement — trigger_price, order_id, side, and
        # which leg it is are all known with certainty here, unlike
        # trying to recover them after the fact from QFEX's
        # historic-orders/trade endpoints (both collapse either leg into
        # an indistinguishable STOP_MARKET execution once triggered —
        # confirmed by inspecting real historic-orders responses). See
        # _place_one_stop_leg for the write call.
        #
        # trigger_log_file is caller-supplied (see perpetual_maker.py,
        # where it's derived from --log-file's directory as
        # "triggers_<run_started_at>.jsonl" — no separate CLI flag) —
        # not hardcoded here, matching how this file has no opinion on
        # log paths generally. Required (not Optional in practice): the
        # only current caller (perpetual_maker.py) always supplies one,
        # so a None here means a direct/programmatic construction forgot
        # it — worth failing loudly rather than silently writing to a
        # hardcoded default path.
        if trigger_log_file is None:
            raise ValueError(
                "trigger_log_file is required — pass trigger_log_file= "
                "when constructing PerpetualMarketMakingStrategy (see "
                "perpetual_maker.py for how it's normally derived from "
                "--log-file)"
            )
        self.trigger_logger = TriggerLogger(out_path=trigger_log_file)

        # --- Quoting-mode state (position == 0) ---
        self.bid_order_id: Optional[str] = None
        self.ask_order_id: Optional[str] = None
        self.bid_price: Optional[Decimal] = None
        self.ask_price: Optional[Decimal] = None

        # --- Position-managing-mode state (position != 0) ---
        self.stop_loss_order_id: Optional[str] = None
        self.stop_loss_trigger_price: Optional[Decimal] = None
        self.take_profit_order_id: Optional[str] = None
        self.take_profit_trigger_price: Optional[Decimal] = None
        # exit_qty / average_price the CURRENTLY RESTING stop orders were
        # sized/triggered against — compared against the live position
        # each tick so a partial fill (which changes exit_qty and usually
        # average_price together) triggers an immediate renewal. See
        # _maybe_renew_stop_orders.
        self._stop_orders_qty: Optional[Decimal] = None
        self._stop_orders_avg_price: Optional[Decimal] = None
        # Added 2026-10-27: what each resting leg was actually placed for, keyed by
        # its order-id attribute name -> (qty, average_price). _stop_orders_qty above
        # is per PAIR and gets overwritten when only one leg is (re)placed, which hid
        # a stale-sized leg (US500 2026-10-26 20:51: STOP_LOSS sized 0.014 after the
        # first partial fill stayed at 0.014 when the position grew to 0.07, because
        # the TAKE_PROFIT leg was placed later for 0.07).
        self._stop_leg_placed_for: Dict[str, Tuple[Decimal, Decimal]] = {}
        # Consecutive cancel_stop_order failure count, keyed by
        # stop_order_id — see _cancel_stop_orders_if_any /
        # _note_stop_cancel_failure. Only ever holds entries for orders
        # CURRENTLY being retried; cleared (popped) on either a successful
        # cancel or a forced give-up past
        # self.client.cfg.max_stop_cancel_failures, so this never
        # accumulates entries for orders no longer relevant.
        self._stop_cancel_failures: Dict[str, int] = {}
        # Monotonic timestamp of the most recent send_stop_loss/
        # send_take_profit ATTEMPT for this leg — set on every attempt
        # (success, reject, or timeout). Used only for logging/
        # diagnostics now; the actual retry gate is
        # client.is_order_outstanding() below, not this timestamp. See
        # _place_stop_orders. None until the first attempt for that leg.
        self._stop_loss_last_attempt_at: Optional[float] = None
        self._take_profit_last_attempt_at: Optional[float] = None
        # client_order_id used for the most recent attempt at this leg —
        # explicitly generated here (rather than left to
        # _send_order_and_wait's auto-generation) so _place_stop_orders
        # can ask client.is_order_outstanding(this_id) on a later tick
        # before sending a new one. None until the first attempt; cleared
        # alongside stop_loss_order_id/take_profit_order_id once the leg
        # is confirmed resting or its retry history is no longer relevant
        # (see _cancel_stop_orders_if_any).
        self._stop_loss_last_client_oid: Optional[str] = None
        self._take_profit_last_client_oid: Optional[str] = None

        # trigger_price this leg last skipped placing because mark_price
        # was already on the wrong side of it (see
        # _mark_price_makes_trigger_unplaceable / _place_one_stop_leg).
        # None means: not currently suppressing this leg. Keyed by the
        # trigger_price itself, not just a bool, so a genuinely NEW
        # trigger_price (a new position, or a renewal after a partial
        # fill changes average_price) is never silently suppressed by a
        # stale reason that applied to a different price. Confirmed live
        # (2026-08-02, CL-USD): before this existed, an unplaceable
        # trigger (mark_price already past it, so QFEX correctly rejects
        # every attempt as INVALID_PRICE) was retried every tick with a
        # fresh client_order_id regardless — 480 identical rejections
        # over ~80 seconds in that incident, all for a price BBO itself
        # never actually reached (only mark_price had drifted past it) —
        # see that method's docstring for why this suppresses the retry
        # rather than force-closing the position.
        self._stop_loss_suppressed_trigger: Optional[Decimal] = None
        self._take_profit_suppressed_trigger: Optional[Decimal] = None

        # Consecutive BBO ticks the stop-loss trigger has been seen
        # crossed, toward self.cfg.bbo_stop_loss_confirm_streak — see
        # _maybe_bbo_triggered_close. Reset to 0 the instant a tick shows
        # it NOT crossed (this is a sustained-streak counter, not a
        # cumulative total — same shape as the circuit breakers'
        # deviation streaks in base_client.py). Also reset whenever
        # _manage_position computes a trigger_price different from the
        # one this streak was counting against, so a stale streak from a
        # previous position/trigger can never carry over — see
        # _maybe_bbo_triggered_close.
        self._stop_loss_confirm_streak = 0
        self._stop_loss_confirm_streak_trigger: Optional[Decimal] = None
        # throttles the 'skipping BBO close' warnings (broken book / trade
        # connection down) to one per BBO_CLOSE_SKIP_LOG_S
        self._bbo_close_skip_logged_at: float = 0.0

        # HARD-STOP MODE ONLY (cfg.breaker_hard_stop=True): set once
        # _handle_price_feed_broken / _handle_order_error_streak_broken has
        # run — makes that breaker a one-shot: even after guard_reentrant()
        # releases (e.g. between awaits inside the breaker sequence
        # itself), a later on_bbo must never start a second bailout, and
        # _tick must never resume normal quoting/position-management after
        # this fires. There is deliberately no path that clears this —
        # recovery is a manual restart, not something this strategy
        # attempts itself. Shared by both trigger causes (price-feed,
        # order-error) — whichever fires first trips this once, for good.
        # In the default pause mode this is never set; see _pause_reason
        # below for that mode's state.
        self._price_feed_circuit_breaker_tripped = False

        # --- Pause-mode state (cfg.breaker_hard_stop=False, the default) ---
        # Which cause put quoting on hold ("mark_price",
        # "underlier_price", "one_sided_book", "order_error_streak" or
        # "wide_spread"), or None when quoting is allowed. Reasons are re-derived from live
        # client state every tick (_active_breaker_reason), so this only
        # remembers that a pause is in progress, for logging and for the
        # resume countdown — see _update_pause_state.
        self._pause_reason: Optional[str] = None
        # Consecutive evaluated ticks with no breaker condition active
        # while paused, toward cfg.breaker_resume_healthy_ticks.
        self._pause_healthy_ticks = 0
        # time.monotonic() deadline before which the order-error breaker
        # counts as still active — set/re-armed by on_fill when the
        # failure streak reaches order_error_streak_required (see
        # cfg.order_error_pause_cooldown_s for why this is time-based
        # rather than clearing on the first healthy response).
        self._order_error_paused_until: Optional[float] = None
        # Spread-gate state — see _update_spread_gate. Unlike the breaker
        # causes above (re-derived from client state every tick), the gate
        # is stateful because it has hysteresis: pauses above
        # quote_pause_spread, clears only below quote_resume_spread.
        self._spread_gate_active = False
        self._spread_bad_ticks = 0
        # Most recent two-sided BBO spread as a fraction of mid, for
        # logging only.
        self._last_spread: Optional[Decimal] = None

        # Set the first time underlier_price_by_symbol[symbol] is seen
        # non-None (checked in _quote, not cleared afterward) — gates
        # quoting specifically, not the whole strategy: stop-loss/
        # take-profit management (_manage_position) doesn't use underlier
        # at all, so a position you're already holding stays fully
        # protected regardless of whether underlier has arrived yet. Only
        # _quote() checks this. A one-way "seen at least once" flag, same
        # pattern as trading_enabled — not a live "is currently present"
        # check, since a momentary gap in the underlier feed should be
        # caught by the deviation/staleness breaker (is_underlier_price_
        # broken), not by silently pausing quoting again here.
        self.underlier_price_confirmed = asyncio.Event()

        # Non-price circuit breaker state — see
        # self.client.cfg.order_error_streak_required's docstring in
        # config.py (on QFEXConfig, alongside the other three circuit
        # breaker thresholds — NOT on PerpetualMarketMakingConfig/
        # self.cfg, matching how mark_price/underlier_price/one_sided_
        # book's thresholds are all accessed via self.client.cfg too).
        # Tracks CONSECUTIVE genuine-failure terminal order responses
        # seen via on_fill, regardless of which specific order/action
        # they belong to — deliberately not scoped to one
        # client_order_id, since the 2026-08-13/14 incident this is
        # built from involved a RETRY LOOP generating a fresh
        # client_order_id each attempt, all rejected the same way; a
        # per-client_order_id counter would never accumulate past 1 for
        # any single id and would miss the pattern entirely. Reset to 0
        # by ACK/FILLED; CANCELLED is neutral (neither resets nor
        # increments) — see on_fill's status-handling comment for why,
        # including the 2026-08-20 false-positive this was added to
        # avoid repeating. Also reset to 0 by _update_pause_state when a
        # pause caused by this breaker ends.
        self._order_error_streak = 0

        # Monotonic time of the last _trim_excess_position MARKET order —
        # a short cooldown so a lagging position update can't trigger a
        # second trim before the first one is reflected.
        self._last_trim_at: float = 0.0

        # Exit guard (added 2026-10-13). After this strategy closes a
        # position with a MARKET order (or learns it is already closed),
        # the exchange's position_response arrives a few ms later. Until
        # then get_position() still shows the old size, and the next BBO
        # tick would fire a SECOND close (seen live on US500-USD, 6 of 11
        # trips, ~6 ms apart; only reduce_only stopped it flipping the
        # position) and re-place stop orders for a position that no longer
        # exists. While armed, position management is skipped as long as
        # the position still equals the pre-close value; it disarms as soon
        # as the position changes, or after EXIT_GUARD_S as a fallback.
        self._exit_guard_pos: Optional[Decimal] = None
        self._exit_guard_until: float = 0.0

        # Drain mode (added 2026-10-14). Set by drain_until_flat() when the
        # runner receives SIGUSR1 (sent by examples/bot_supervisor.py at a
        # scheduled switch). While draining, a FLAT strategy cancels its
        # quotes and places no new ones; a strategy holding a position keeps
        # managing the exit exactly as usual. The process then exits once
        # flat with no open orders, so a switch never leaves a position
        # without its exchange-side stop.
        self.draining: bool = False

        # Background quote cancel (added 2026-10-14): when a position opens,
        # cancelling the leftover quotes no longer blocks exit management —
        # see _cancel_quotes_in_background.
        self._bg_quote_cancel: Optional[asyncio.Task] = None

    # ---------- Callback entry point ----------

    async def on_bbo(self, bbo: BBO) -> None:
        self.log_bbo(bbo)

        if not self.trading_enabled.is_set():
            return
        if not self.has_two_sided_book(bbo):
            # _tick only ever runs for two-sided ticks, and base_client
            # resets the one-sided streak to 0 on the first two-sided
            # tick — so the is_one_sided_book_broken() check could never
            # be True from inside _tick (it was unreachable there).
            # One-sided ticks are the only place it CAN be True, so it
            # is evaluated here instead.
            await self._on_one_sided_tick()
            return

        async with self.guard_reentrant() as acquired:
            if not acquired:
                # A previous tick's order calls are still in flight —
                # skip this one rather than racing it.
                return
            await self._tick(bbo)

    async def on_trade(self, trade_msg: Dict[str, Any]) -> None:
        return

    def _record_taker_fill(self, fill: Dict[str, Any]) -> None:
        """Append a taker fill to the weekly ledger (see taker_ledger_file). Only real
        fills of non-ALO orders count (market closes and triggered stops); a status other
        than FILLED (e.g. the CANCELLED response of a take-profit QFEX resized) is skipped.
        Never raises: a ledger problem must not affect trading. Added 2026-10-31."""
        if not self.taker_ledger_file:
            return
        try:
            orsp = fill["order_response"]
            otype = orsp.get("type")
            if orsp.get("status") != "FILLED" or otype in (None, "ALO"):
                return
            qty = float(fill["filled_qty_delta"])
            px = float(orsp.get("price") or 0.0)
            if qty <= 0 or px <= 0:
                return
            rec = {
                "t": round(time.time(), 3),
                "symbol": orsp.get("symbol"),
                "type": otype,
                "side": orsp.get("side"),
                "qty": qty,
                "px": px,
                "notional": round(qty * px, 4),
                "order_id": orsp.get("order_id"),
            }
            with open(self.taker_ledger_file, "a") as fh:
                fh.write(json.dumps(rec) + "\n")
        except Exception:
            self.client.log.exception("taker ledger write failed (trading unaffected)")

    def on_balance(self, br: Dict[str, Any]) -> None:
        """Append an account P&L snapshot (realised P&L, fees, funding) to
        pnl_ledger_file when realised P&L or fees changed. Never raises. Added 2026-11-13."""
        if not self.pnl_ledger_file:
            return
        try:
            realised = float(br.get("realised_pnl") or 0.0)
            fees = float(br.get("fees") or 0.0)
            if self._pnl_last == (realised, fees):
                return
            self._pnl_last = (realised, fees)
            rec = {"t": round(time.time(), 3), "realised": realised, "fees": fees,
                   "funding": float(br.get("net_funding") or 0.0)}
            with open(self.pnl_ledger_file, "a") as fh:
                fh.write(json.dumps(rec) + "\n")
        except Exception:
            self.client.log.exception("pnl ledger write failed (trading unaffected)")

    async def on_fill(self, fill: Dict[str, Any]) -> None:
        self.log_fill(fill)
        self._record_taker_fill(fill)
        # No bookkeeping beyond logging here deliberately — order_id ->
        # None transitions on a filled quote/stop/take-profit are handled
        # by _tick() re-deriving live state from self.client.positions and
        # self.client.get_position() each call, not by reacting to this
        # specific fill event. See _tick()'s docstring for why re-deriving
        # from live state each tick is preferred over maintaining
        # fill-driven state transitions in parallel.

        # Non-price circuit breaker — see
        # self.client.cfg.order_error_streak_required in config.py for
        # the incident this is built from and why it's tracked
        # separately from the price-feed breakers.
        #
        # Status handling, three-way (not the two-way ACK/FILLED-vs-
        # everything-else split this had originally):
        #   - ACK, FILLED: genuine success, resets the streak to 0.
        #   - CANCELLED: NEUTRAL — does NOT increment or reset. Confirmed
        #     live (2026-08-20, CL-USD) that CANCELLED is a completely
        #     normal, expected outcome of this codebase's own retry
        #     pattern: is_order_outstanding-gated retries routinely
        #     produce a late, delayed CANCELLED confirmation for an
        #     attempt that was already superseded by a successful retry
        #     (see the "late response... Ns after this client gave up
        #     waiting" ERROR lines that accompany these — the delay is
        #     the retry timing, not a QFEX malfunction). Treating
        #     CANCELLED as a failure previously tripped this breaker on
        #     an otherwise-healthy retry sequence where fresh ACKs had
        #     already landed — a false positive, not a real incident.
        #     Not treated as a success either, since a cancel succeeding
        #     says nothing about whether PLACEMENTS are failing — only
        #     skipping it (leaving the streak exactly where it was)
        #     avoids masking a genuine concurrent placement-failure
        #     streak that a stray CANCELLED happened to land inside.
        #   - everything else (INVALID_QUANTITY, REJECTED, ServerError-
        #     surfaced statuses, any other terminal status QFEX
        #     returns): genuine failure, increments the streak — this is
        #     the real signal the breaker exists to catch (see the
        #     2026-08-13/14 and 2026-08-20/22 incidents, both real
        #     INVALID_QUANTITY/ServerError rejections of legitimate
        #     requests).
        if self._price_feed_circuit_breaker_tripped:
            return
        status = fill["order_response"].get("status")
        if status in ("ACK", "FILLED"):
            self._order_error_streak = 0
        elif status == "CANCELLED":
            pass  # neutral — see status-handling comment above
        else:
            self._order_error_streak += 1
            self.client.log.warning(
                "on_fill saw failure status=%s (client_order_id=%s) "
                "— order_error_streak now %d/%d",
                status,
                fill["order_response"].get("client_order_id"),
                self._order_error_streak,
                self.client.cfg.order_error_streak_required,
            )
            if self._order_error_streak >= self.client.cfg.order_error_streak_required:
                if self.cfg.breaker_hard_stop:
                    await self._handle_order_error_streak_broken()
                else:
                    # Pause mode: arm (or re-arm, if failures keep
                    # coming) the cooldown. The next tick sees it via
                    # _active_breaker_reason, cancels any resting quotes
                    # and stops placing new ones — see _update_pause_state
                    # for how it ends.
                    self._order_error_paused_until = (
                        time.monotonic() + self.cfg.order_error_pause_cooldown_s
                    )

    async def on_trade_reconnected(self) -> None:
        """
        Called by the client after a trade connection is re-established
        (added 2026-10-17). cancel_on_disconnect may have removed the
        orders this strategy is tracking, and QFEX sends no cancel notice
        on the new connection. Re-verify each tracked order by cancelling
        it: CANCELLED, NO_SUCH_ORDER or a rejection all mean it is gone
        now, so local tracking is cleared and the normal tick logic
        re-places exchange stops (if a position is open) or fresh quotes
        (if flat). Only a timeout leaves an order tracked (its state is
        unknown; re-placing could duplicate it) — the bot's own BBO
        stop-loss/take-profit keep protecting the position meanwhile.
        Waits for any running tick to finish first.
        """
        async with self._reentrant_lock:
            await self._reverify_orders_after_reconnect()

    async def _reverify_orders_after_reconnect(self) -> None:
        log = self.client.log
        if self.bid_order_id is not None or self.ask_order_id is not None:
            await self._cancel_quotes_if_any()     # NO_SUCH_ORDER clears tracking too
        cleared = False
        for label, id_attr, price_attr in (
            ("STOP_LOSS", "stop_loss_order_id", "stop_loss_trigger_price"),
            ("TAKE_PROFIT", "take_profit_order_id", "take_profit_trigger_price"),
        ):
            order_id = getattr(self, id_attr)
            if order_id is None:
                continue
            try:
                result = await self.client.cancel_stop_order(order_id)
                outcome = result["final_order_response"].get("status")
            except asyncio.TimeoutError:
                log.error(
                    "%s order_id=%s: re-check after reconnect timed out — state "
                    "unknown, leaving it tracked (not re-placing, to avoid a "
                    "duplicate); the bot's own BBO exit still protects the "
                    "position. Verify on the QFEX website.",
                    label, order_id,
                )
                continue
            except QFEXTradeError as e:
                outcome = "rejected:%s" % e.err.get("error_code")
            except ValueError:
                # a cancel for this id is already in flight — it will resolve on its own
                continue
            log.warning(
                "%s order_id=%s re-checked after reconnect (%s) — it is gone "
                "now; will re-place it on the next tick if a position is open",
                label, order_id, outcome,
            )
            setattr(self, id_attr, None)
            setattr(self, price_attr, None)
            self._stop_cancel_failures.pop(order_id, None)
            cleared = True
        if cleared:
            # force a fresh placement for the current position
            self._stop_orders_qty = None
            self._stop_orders_avg_price = None
            self._stop_loss_last_client_oid = None
            self._take_profit_last_client_oid = None
            self._stop_loss_suppressed_trigger = None
            self._take_profit_suppressed_trigger = None

    async def on_stop_order_resolved(self, order_id: str, orsp: Dict[str, Any]) -> None:
        """
        See TraderStrategy.on_stop_order_resolved (strategy.py). Fires
        when QFEX has resolved a stop order without this strategy asking
        — either it triggered and QFEX spawned a separate order to
        execute the trade (phantom FILLED), or QFEX auto-cancelled it as
        the other leg of a bracket whose sibling just triggered
        (externally cancelled). Either way `order_id` is no longer
        cancellable, so this clears local tracking (stop_loss_order_id or
        take_profit_order_id, whichever matches) immediately, so
        _cancel_stop_orders_if_any never wastes attempts on a dead id. If
        `order_id` matches neither, it's not one this instance is
        tracking — ignored at DEBUG.
        """
        if order_id == self.stop_loss_order_id:
            label, order_attr, price_attr = "STOP_LOSS", "stop_loss_order_id", "stop_loss_trigger_price"
        elif order_id == self.take_profit_order_id:
            label, order_attr, price_attr = "TAKE_PROFIT", "take_profit_order_id", "take_profit_trigger_price"
        else:
            self.client.log.debug(
                "on_stop_order_resolved for order_id=%s, which this "
                "strategy instance is not currently tracking — ignoring",
                order_id,
            )
            return

        self.client.log.warning(
            "%s order_id=%s resolved externally (status=%s, not a cancel "
            "this strategy requested) — clearing local tracking",
            label,
            order_id,
            orsp.get("status"),
        )
        setattr(self, order_attr, None)
        setattr(self, price_attr, None)

    # ---------- Tick ----------

    async def _tick(self, bbo: BBO) -> None:
        """
        One strategy iteration. Branches on live position state, re-read
        from self.client fresh EVERY tick rather than tracked locally —
        deliberately mirrors Hummingbot's own approach (its tick() also
        re-derives active_positions from the connector each call, not from
        strategy-local counters) since the client's positions channel is
        the actual source of truth and can change for reasons this
        strategy didn't itself cause (a stop order triggering between
        ticks, a manual intervention, a liquidation).

        Checks the circuit breakers FIRST, before any of the above — see
        _active_breaker_reason. Four independent causes share one
        response: mark_price protects stop-loss/take-profit trigger
        reliability; underlier_price protects quoting reference accuracy;
        one-sided book catches a BBO failure the other two can't, since
        both compare a feed's value against BBO midpoint, which doesn't
        exist when the book is one-sided or empty (confirmed live
        2026-07-27, CL-USD: a missing bid for a full ~48s run, undetected
        by either other check since there was no midpoint to compare
        against; that cause is evaluated in on_bbo/_on_one_sided_tick, not
        here, since a one-sided tick never reaches _tick); and the
        non-price order-error streak (see on_fill).

        Response, DEFAULT (cfg.breaker_hard_stop=False): PAUSE NEW QUOTES
        ONLY. While any cause is active (and for
        cfg.breaker_resume_healthy_ticks healthy ticks afterwards — see
        _update_pause_state) a flat strategy cancels its resting quotes
        and places no new ones. Nothing else changes: a position that is
        already open goes through _manage_position exactly as usual, so
        the BBO-triggered close and the native stop orders stay live, and
        leftover stop orders are still cleaned up when flat. Quoting
        resumes by itself; no restart needed.

        Response, cfg.breaker_hard_stop=True: the original one-shot
        behavior — _handle_price_feed_broken cancels everything,
        force-closes any position and stops the client; this method never
        falls through to normal quoting/position logic again for the rest
        of the process's life.

        underlier_price's check is skipped entirely when
        self.cfg.mid_price_underlier_weight is exactly 0 — confirmed
        live (2026-08-02, CL-USD) that with weight=0, underlier_price
        has zero influence on _compute_mid (multiplied by zero) and zero
        relevance to STOP_LOSS/TAKE_PROFIT triggers (that's mark_price's
        job), yet a divergence in a feed the strategy isn't using at all
        was still forcing a stop — the check was correct on its own
        terms but answering a question that no longer mattered once the
        weight was set to zero. Exact-zero only, not a tolerance/epsilon
        around zero: a deliberately-chosen small nonzero weight (e.g.
        0.01) still means underlier genuinely influences quoting, however
        slightly, so it still deserves the check.

        Spread gate: on top of the breakers, quoting also pauses while
        the BBO spread is blown out (see _update_spread_gate). It is
        updated on EVERY two-sided tick — including while holding a
        position, so it is already tripped by the time the strategy goes
        flat — and it is soft-only: it never feeds the hard-stop path
        below, in either mode it just pauses quoting.
        """
        if self._price_feed_circuit_breaker_tripped:
            return  # hard-stop mode only: one-shot, never resumes
        self._update_spread_gate(bbo)
        reason = self._active_breaker_reason()
        if reason is not None and self.cfg.breaker_hard_stop:
            await self._handle_price_feed_broken(reason)
            return
        if reason is None and self._spread_gate_active:
            reason = "wide_spread"
        paused = self._update_pause_state(reason)

        position = self.client.get_position(self.symbol)
        if position.position == 0:
            self._exit_guard_pos = None
            await self._await_bg_quote_cancel()
            # Flat: not managing anything — if a stop/take-profit order is
            # still resting from a position that has since closed (e.g.
            # the OTHER side just filled fully and closed us out), cancel
            # the leftover leg. QFEX does not do this for you.
            await self._cancel_stop_orders_if_any()
            if paused or self.draining:
                # Circuit breaker active (or cooling down): pull any
                # resting quotes and do not place new ones. Runs every
                # paused tick; a no-op once nothing is resting.
                await self._cancel_quotes_if_any()
                return
            await self._quote(bbo)
        else:
            # Holding a position: Hummingbot does not quote fresh maker
            # orders while a position is open — mirrored here. Cancel any
            # resting quotes first, then manage the exit. Deliberately
            # NOT gated by `paused`: exit management must keep running.
            # Changed 2026-10-14: the cancels no longer block exit
            # management (they took 2.8 s + 3.1 s during the 19:30 data
            # spike, before any stop logic ran). A leftover quote that fills
            # meanwhile is harmless: the opposite side only shrinks the
            # position, and the same side can add at most up to
            # order_amount (stop orders are renewed on size changes).
            self._cancel_quotes_in_background()
            if self._exit_guard_active(position.position):
                return
            await self._trim_excess_position(position.position)
            await self._manage_position(bbo, position.position, position.average_price)

    def _cancel_quotes_in_background(self) -> None:
        if self.bid_order_id is None and self.ask_order_id is None:
            return
        if self._bg_quote_cancel is not None and not self._bg_quote_cancel.done():
            return
        self._bg_quote_cancel = asyncio.create_task(self._cancel_quotes_now())

    async def _await_bg_quote_cancel(self) -> None:
        bg = self._bg_quote_cancel
        if bg is not None and not bg.done():
            try:
                await asyncio.shield(bg)
            except Exception:
                pass

    async def drain_cancel_quotes(self) -> None:
        """Cancel resting quotes from outside the BBO callback (used while
        draining, in case no BBO tick arrives to do it)."""
        async with self.guard_reentrant() as acquired:
            if acquired:
                await self._cancel_quotes_if_any()

    EXIT_GUARD_S = 3.0

    BBO_CLOSE_SKIP_LOG_S = 10.0

    def _book_looks_broken(self, bbo: BBO) -> bool:
        """True when the BBO spread is wider than cfg.bbo_close_max_spread
        (relative to the bid). Added 2026-10-19."""
        if bbo.bid_px is None or bbo.ask_px is None or bbo.bid_px <= 0:
            return True
        return (bbo.ask_px - bbo.bid_px) / bbo.bid_px > self.cfg.bbo_close_max_spread

    def _log_bbo_close_skip(self, fmt: str, *args: Any) -> None:
        now = time.monotonic()
        if now - self._bbo_close_skip_logged_at >= self.BBO_CLOSE_SKIP_LOG_S:
            self._bbo_close_skip_logged_at = now
            self.client.log.warning(fmt, *args)

    def _arm_exit_guard(self, position_qty: Decimal) -> None:
        self._exit_guard_pos = position_qty
        self._exit_guard_until = time.monotonic() + self.EXIT_GUARD_S

    def _exit_guard_active(self, position_qty: Decimal) -> bool:
        """True while we are waiting for the exchange to confirm a close."""
        if self._exit_guard_pos is None:
            return False
        if position_qty != self._exit_guard_pos or time.monotonic() >= self._exit_guard_until:
            self._exit_guard_pos = None
            return False
        return True

    async def _trim_excess_position(self, position_qty: Decimal) -> None:
        """
        Safety net: this strategy never intends to hold more than
        cfg.order_amount (one quote fills at a time, and quotes are
        cancelled while a position is open). A position larger than that
        can only come from an order this strategy lost track of — e.g. an
        orphaned quote whose ACK arrived after its placement timed out
        (confirmed live 2026-09-28, GOLD-USD: position reached 3x
        order_amount). Trims the excess with a reduce_only MARKET order;
        the normal exit logic then manages the remaining order_amount
        (its stop orders renew automatically since the size changed).

        reduce_only means this can never flip or grow the position. A 5s
        cooldown stops a lagging position update from firing a second
        trim before the first is reflected.
        """
        excess = abs(position_qty) - self.cfg.order_amount
        if excess <= 0:
            return
        now = time.monotonic()
        if now - self._last_trim_at < 5.0:
            return
        self._last_trim_at = now
        exit_side = Side.SELL if position_qty > 0 else Side.BUY
        self.client.log.critical(
            "POSITION %s on %s exceeds order_amount %s by %s — an order this "
            "strategy was not tracking must have filled. Trimming the excess "
            "with a reduce_only MARKET %s.",
            position_qty, self.symbol, self.cfg.order_amount, excess, exit_side,
        )
        try:
            result = await self.client.send_market(
                symbol=self.symbol, side=exit_side, quantity=excess, reduce_only=True,
            )
            self.client.log.critical(
                "excess-position trim for %s: status=%s",
                self.symbol, result["final_order_response"].get("status"),
            )
        except Exception:
            self.client.log.exception(
                "excess-position trim for %s failed — will retry after the "
                "cooldown if the position is still too large", self.symbol,
            )

    async def _on_one_sided_tick(self) -> None:
        """
        Called from on_bbo for a one-sided/empty BBO tick (which never
        reaches _tick). Only acts if a breaker cause is active — in
        practice one_sided_book once its streak has built up (see
        is_one_sided_book_broken), or any other cause that is still
        active. Does NOT count toward the resume countdown: a tick with
        no usable book is not evidence that things are healthy again.
        Pause mode: cancels resting quotes (no book to quote around).
        Hard-stop mode: same emergency response as _tick.
        """
        if self._price_feed_circuit_breaker_tripped:
            return
        reason = self._active_breaker_reason()
        if reason is None:
            return
        async with self.guard_reentrant() as acquired:
            if not acquired:
                return
            if self.cfg.breaker_hard_stop:
                await self._handle_price_feed_broken(reason)
                return
            if self._update_pause_state(reason):
                await self._cancel_quotes_if_any()

    # ---------- Circuit-breaker pause state ----------

    def _update_spread_gate(self, bbo: BBO) -> None:
        """
        Updates the spread gate from this two-sided tick. No-op when
        cfg.quote_pause_spread is None (gate disabled).

          spread = (ask - bid) / mid, a fraction, same units as the
          config fields.

          - gate off: counts consecutive ticks with spread >
            quote_pause_spread; at quote_pause_spread_bad_ticks the gate
            turns on (_tick then reports "wide_spread" to
            _update_pause_state).
          - gate on: turns off the first tick spread < quote_resume_spread
            (a spread between the two thresholds keeps it on). From that
            tick, _update_pause_state's usual healthy-tick countdown
            (cfg.breaker_resume_healthy_ticks) still has to run before
            quoting resumes.

        Deliberately independent of every other feed: it reads only the
        BBO the strategy is already quoting off, so it works at
        mid_price_underlier_weight == 0 (underlier breaker skipped) and
        needs no threshold tuned against mark_price/underlier basis. The
        2026-09-18 16:31 incident is exactly the case it exists for: the
        book thinned, best ask jumped 4392 -> 4402 -> 4440 while the bid
        sat at 4386, the spread went from ~5 bps to 35-120 bps, and the
        blended mid moved ~0.6% away from where the market had actually
        been trading.
        """
        threshold = self.cfg.quote_pause_spread
        if threshold is None:
            return
        assert bbo.bid_px is not None and bbo.ask_px is not None
        mid = (bbo.bid_px + bbo.ask_px) / 2
        if mid <= 0:
            return
        spread = (bbo.ask_px - bbo.bid_px) / mid
        self._last_spread = spread

        if not self._spread_gate_active:
            self._spread_bad_ticks = self._spread_bad_ticks + 1 if spread > threshold else 0
            if self._spread_bad_ticks >= self.cfg.quote_pause_spread_bad_ticks:
                self._spread_gate_active = True
        elif spread < self.cfg.quote_resume_spread:
            self._spread_gate_active = False
            self._spread_bad_ticks = 0

    def _active_breaker_reason(self) -> Optional[str]:
        """
        The first breaker cause currently active, or None if all clear.
        Re-derived from live client state on every call — nothing here is
        latched. Checked in a fixed order (mark_price, underlier_price,
        one_sided_book, order_error_streak); when several are active at
        once only the first is reported, which affects logging only, not
        behavior (all causes get the same response).

        underlier_price is skipped when mid_price_underlier_weight == 0 —
        see _tick's docstring. order_error_streak is time-based, not
        state-derived: active until the cooldown armed by on_fill has
        elapsed (see PerpetualMarketMakingConfig.order_error_pause_
        cooldown_s for why).
        """
        if self.client.is_mark_price_broken(self.symbol):
            return "mark_price"
        if (
            self.cfg.mid_price_underlier_weight != 0
            and self.client.is_underlier_price_broken(self.symbol)
        ):
            return "underlier_price"
        if self.client.is_one_sided_book_broken(self.symbol):
            return "one_sided_book"
        if (
            self._order_error_paused_until is not None
            and time.monotonic() < self._order_error_paused_until
        ):
            return "order_error_streak"
        return None

    def _update_pause_state(self, reason: Optional[str]) -> bool:
        """
        Advances the pause/resume state machine by one evaluated tick and
        returns True if quoting must stay paused this tick.

        - reason is not None (a cause is active): pause, and reset the
          healthy-tick countdown to 0. Logs when a pause starts, and
          again if the cause changes while already paused.
        - reason is None and not paused: nothing to do, returns False.
        - reason is None and paused: counts one healthy tick; once
          cfg.breaker_resume_healthy_ticks consecutive healthy ticks have
          accumulated, resumes (returns False) and clears the order-error
          streak/cooldown so a still-broken exchange has to fail again
          from zero to re-trip. Until then stays paused (returns True).
          The countdown is what stops a feed hovering right at its
          threshold from flapping pause/resume every other tick: the
          price-feed streaks themselves reset on the first healthy tick.
        """
        if reason is not None:
            self._pause_healthy_ticks = 0
            if reason != self._pause_reason:
                previous = self._pause_reason
                self._pause_reason = reason
                self._log_quoting_paused(reason, previous)
            return True

        if self._pause_reason is None:
            return False

        self._pause_healthy_ticks += 1
        if self._pause_healthy_ticks < self.cfg.breaker_resume_healthy_ticks:
            return True

        self.client.log.warning(
            "circuit breaker cleared for %s (was: %s) — %d consecutive "
            "healthy ticks, RESUMING quoting",
            self.symbol,
            self._pause_reason,
            self._pause_healthy_ticks,
        )
        self._pause_reason = None
        self._pause_healthy_ticks = 0
        self._order_error_streak = 0
        self._order_error_paused_until = None
        return False

    def _log_quoting_paused(self, reason: str, previous: Optional[str]) -> None:
        """
        Logs that quoting is paused for `reason`. `previous` is the cause
        that was already pausing quoting when this one took over, or None
        when this is a fresh pause. Deviation causes reuse
        _PRICE_FEED_BROKEN_REASONS for the feed's current reading and
        streak length; order_error_streak has no feed to report, so it is
        worded separately. Unknown reasons raise KeyError on purpose —
        same "surface a bug loudly" stance as _log_price_feed_broken.
        """
        if previous is None:
            header = "CIRCUIT BREAKER for %s — PAUSING new quotes"
        else:
            header = "CIRCUIT BREAKER for %s — quoting stays PAUSED (cause changed from " + previous + ")"

        tail = (
            " (resting quotes cancelled; any open position keeps being "
            "managed). Auto-resumes after %d consecutive healthy ticks."
        )
        resume_ticks = self.cfg.breaker_resume_healthy_ticks

        if reason == "order_error_streak":
            self.client.log.warning(
                header + ": %d consecutive order-failure responses"
                " (order_error_streak_required=%d); paused for at least %.0fs"
                + tail,
                self.symbol,
                self._order_error_streak,
                self.client.cfg.order_error_streak_required,
                self.cfg.order_error_pause_cooldown_s,
                resume_ticks,
            )
            return

        if reason == "wide_spread":
            # Not a feed-vs-BBO deviation, so not in _PRICE_FEED_BROKEN_
            # REASONS: the spread gate is quoting-only and never routes
            # through the hard-stop handlers that table exists for.
            bps = lambda x: float(x) * 10000 if x is not None else float("nan")
            self.client.log.warning(
                header + ": BBO spread=%.1f bps > %.1f bps for %d+ consecutive "
                "ticks (gate clears once spread < %.1f bps)" + tail,
                self.symbol,
                bps(self._last_spread),
                bps(self.cfg.quote_pause_spread),
                self.cfg.quote_pause_spread_bad_ticks,
                bps(self.cfg.quote_resume_spread),
                resume_ticks,
            )
            return

        is_deviation, value_getter, streak_required_getter = _PRICE_FEED_BROKEN_REASONS[reason]
        streak_required = streak_required_getter(self)
        current_bbo = self.client.bbo_by_symbol.get(self.symbol)
        bid = getattr(current_bbo, "bid_px", None)
        ask = getattr(current_bbo, "ask_px", None)

        if is_deviation:
            self.client.log.warning(
                header + ": %s=%s has deviated from BBO midpoint (bid=%s "
                "ask=%s) for %d+ consecutive ticks" + tail,
                self.symbol,
                reason,
                value_getter(self),
                bid,
                ask,
                streak_required,
                resume_ticks,
            )
        else:
            self.client.log.warning(
                header + ": BBO has been one-sided or empty (bid=%s ask=%s)"
                " for %d+ consecutive ticks" + tail,
                self.symbol,
                bid,
                ask,
                streak_required,
                resume_ticks,
            )

    # ---------- Hard-stop mode (cfg.breaker_hard_stop=True) ----------

    def _log_price_feed_broken(self, reason: str) -> None:
        """
        HARD-STOP MODE ONLY (see _handle_price_feed_broken). Pause mode
        logs through _log_quoting_paused instead.

        Logs the CRITICAL "circuit breaker tripped" line for `reason`,
        looked up in _PRICE_FEED_BROKEN_REASONS. Split out from
        _handle_price_feed_broken so that method's flow (trip flag ->
        log -> cancel -> close -> stop) stays readable as one sequence,
        without the per-reason message construction interrupting it.

        Raises KeyError if `reason` isn't a key in
        _PRICE_FEED_BROKEN_REASONS — deliberately not defensive here:
        an unrecognized reason means a caller (only _tick today) is
        passing a string that doesn't match any registered is_*_broken
        check, which is a bug worth surfacing loudly rather than
        silently logging a garbled or generic message.
        """
        is_deviation, value_getter, streak_required_getter = _PRICE_FEED_BROKEN_REASONS[reason]
        streak_required = streak_required_getter(self)
        current_bbo = self.client.bbo_by_symbol.get(self.symbol)
        bid = getattr(current_bbo, "bid_px", None)
        ask = getattr(current_bbo, "ask_px", None)

        if is_deviation:
            value = value_getter(self)
            self.client.log.critical(
                "PRICE FEED CIRCUIT BREAKER TRIPPED for %s: %s=%s has "
                "deviated from BBO midpoint (bid=%s ask=%s) for %d+ "
                "consecutive ticks — cancelling all orders, "
                "force-closing any open position, and stopping. This "
                "will NOT auto-recover; investigate and restart manually.",
                self.symbol,
                reason,
                value,
                bid,
                ask,
                streak_required,
            )
        else:
            # one_sided_book (or any future non-deviation cause): no
            # midpoint to have deviated from, which is exactly the
            # condition being reported — different phrasing from the
            # deviation case above rather than a generic template with
            # "N/A" plugged in for the missing value.
            self.client.log.critical(
                "PRICE FEED CIRCUIT BREAKER TRIPPED for %s: BBO has been "
                "one-sided or empty (bid=%s ask=%s) for %d+ consecutive "
                "ticks — cancelling all orders, force-closing any open "
                "position, and stopping. This will NOT auto-recover; "
                "investigate and restart manually.",
                self.symbol,
                bid,
                ask,
                streak_required,
            )

    async def _handle_price_feed_broken(self, reason: str) -> None:
        """
        HARD-STOP MODE ONLY (cfg.breaker_hard_stop=True) — the default
        pause mode never calls this; see _update_pause_state.

        Circuit breaker: fires once, the first time any of
        self.client.is_mark_price_broken(self.symbol),
        is_underlier_price_broken(self.symbol), or
        is_one_sided_book_broken(self.symbol) returns True (see
        base_client.py). `reason` is "mark_price", "underlier_price", or
        "one_sided_book" — for logging/diagnosis only, since each cause
        points an investigation differently (mark_price: stop-order
        trigger reliability; underlier_price: quoting reference has been
        drifting; one_sided_book: the BBO feed is unusable) even though
        the response is identical either way. See
        _PRICE_FEED_BROKEN_REASONS for how each reason's message is
        constructed.

        Confirmed live (2026-07-26, CL-USD) that a stuck mark price makes
        every STOP_LOSS/TAKE_PROFIT on the wrong side of it fail as
        INVALID_PRICE indefinitely — this strategy can't protect a
        position under that condition, so it stops trying and gets out.
        Same shutdown sequence for the other two causes: a broken
        quoting reference or an unusable BBO feed are just as
        disqualifying, even without mark_price's specific failure
        signature.

        Logs this reason's specific CRITICAL message, then hands off to
        _execute_emergency_shutdown for the actual cancel/close/stop
        sequence — see that method's docstring for what happens in each
        step and how failures there are handled. See also
        _handle_order_error_streak_broken, a sibling for a different
        (non-price) failure signature that shares the same shutdown
        sequence but not this method's price-specific logging.
        """
        self._price_feed_circuit_breaker_tripped = True
        self._log_price_feed_broken(reason)
        await self._execute_emergency_shutdown()

    async def _handle_order_error_streak_broken(self) -> None:
        """
        HARD-STOP MODE ONLY (cfg.breaker_hard_stop=True) — in the default
        pause mode on_fill arms _order_error_paused_until instead.

        Sibling to _handle_price_feed_broken, for a fundamentally
        different failure signature: this fires when QFEX itself is
        repeatedly rejecting/erroring on order-management requests
        (INVALID_QUANTITY, ServerError, etc.) — see
        self.client.cfg.order_error_streak_required in config.py (on
        QFEXConfig, alongside the other three circuit breaker
        thresholds) for the live incident this is built from.
        Deliberately NOT folded into _handle_price_feed_broken/
        _log_price_feed_broken: those are specifically about a broken
        PRICE FEED (their log message says so explicitly, and
        _PRICE_FEED_BROKEN_REASONS's entries are all price-value/
        deviation shaped), whereas this can fire while the price feed
        looks completely normal — reusing that machinery under a
        mismatched name/message would be actively misleading about what
        actually happened when read back later.

        Same emergency response either way, though (cancel everything,
        force-close, hard stop) — that part IS shared, via
        _execute_emergency_shutdown.

        Also sets _price_feed_circuit_breaker_tripped (despite the name)
        so this remains a one-shot breaker sharing the same "already
        tripped, ignore everything else" gate as the price-feed causes —
        on_bbo/on_fill both check this single flag; a second, separately-
        named flag here would mean the two breakers could never see each
        other's trip state and could double-fire concurrently.
        """
        self._price_feed_circuit_breaker_tripped = True
        self.client.log.critical(
            "NON-PRICE CIRCUIT BREAKER TRIPPED for %s: %d consecutive "
            "order-failure responses (see on_fill's warning logs "
            "immediately above for each one) — cancelling all orders, "
            "force-closing any open position, and stopping. This will "
            "NOT auto-recover; investigate and restart manually.",
            self.symbol,
            self._order_error_streak,
        )
        await self._execute_emergency_shutdown()

    async def _execute_emergency_shutdown(self) -> None:
        """
        HARD-STOP MODE ONLY — not reached in the default pause mode.

        Shared shutdown sequence behind both _handle_price_feed_broken
        and _handle_order_error_streak_broken — cancel everything
        resting, force-close any open position, hard stop. Callers are
        responsible for setting _price_feed_circuit_breaker_tripped and
        logging their own reason-specific CRITICAL message before
        calling this; this method itself is reason-agnostic.

        Sequence, and what happens if a step fails:

        1. Cancel all resting orders (quotes if flat, stop orders if in a
           position). A cancel failure here is logged but does NOT block
           the next step — an uncancelled resting order is a secondary
           concern next to actually closing the position.
        2. If a position is open, force-close it with a reduce_only
           MARKET order — the one action that actually protects capital,
           since a MARKET order has no price for QFEX to validate against
           a broken price feed, regardless of which feed broke. Retried
           up to self.client.cfg.mark_price_break_market_close_max_
           attempts times if it fails; if it still fails after that,
           this logs a CRITICAL failure and proceeds to step 3 anyway —
           continuing to trade blind while unable to close would be
           strictly worse than stopping blind.
        3. Hard stop: self.client.stop(). Runs unconditionally, however
           steps 1-2 went. There is no resume path — this is a one-shot
           breaker; recovery is a manual restart after investigating why
           this fired, not something this strategy attempts itself.
        """
        # Step 1: cancel everything resting, best-effort.
        try:
            await self._cancel_quotes_if_any()
        except Exception:
            self.client.log.exception(
                "circuit breaker: failed to cancel resting quotes for %s "
                "— continuing to position close regardless",
                self.symbol,
            )
        try:
            await self._cancel_stop_orders_if_any()
        except Exception:
            self.client.log.exception(
                "circuit breaker: failed to cancel resting stop orders "
                "for %s — continuing to position close regardless",
                self.symbol,
            )

        # Step 2: force-close via reduce_only MARKET order, if a position
        # is actually open. Re-read position fresh right before each
        # attempt, not just once before the loop — confirmed live
        # (2026-08-02, CL-USD): a STOP_LOSS/TAKE_PROFIT can trigger and
        # flatten the position in the same narrow window this circuit
        # breaker is reacting to (both watch the same broken price feed,
        # so both can fire close together), meaning the position this
        # loop is trying to close may already be gone by attempt 1, or
        # between attempts. Retrying against a stale exit_qty/exit_side
        # from before that happened sends a MARKET order that no longer
        # corresponds to any real position — that incident saw two
        # further attempts each time out with no response at all,
        # rather than a clean rejection, which re-checking before each
        # attempt avoids by simply not sending them once flat.
        max_attempts = self.client.cfg.mark_price_break_market_close_max_attempts
        closed = False
        for attempt in range(1, max_attempts + 1):
            position = self.client.get_position(self.symbol)
            if position.position == 0:
                self.client.log.critical(
                    "circuit breaker: %s position confirmed flat before "
                    "market-close attempt %d — nothing left to close",
                    self.symbol,
                    attempt,
                )
                closed = True
                break
            exit_side = Side.SELL if position.position > 0 else Side.BUY
            exit_qty = abs(position.position)
            try:
                result = await self.client.send_market(
                    symbol=self.symbol,
                    side=exit_side,
                    quantity=exit_qty,
                    reduce_only=True,
                )
                orsp = result["final_order_response"]
                self.client.log.critical(
                    "circuit breaker: market close attempt %d/%d for "
                    "%s: status=%s order_id=%s",
                    attempt,
                    max_attempts,
                    self.symbol,
                    orsp.get("status"),
                    orsp.get("order_id"),
                )
                if orsp.get("status") == "FILLED":
                    closed = True
                    break
                if orsp.get("status") == "INVALID_QUANTITY":
                    # Re-check immediately rather than waiting for the
                    # top of the next loop iteration — if this rejection
                    # is because the position already went flat (the
                    # scenario this whole change targets), this confirms
                    # it now instead of retrying once more first.
                    if self.client.get_position(self.symbol).position == 0:
                        self.client.log.critical(
                            "circuit breaker: %s market-close got "
                            "INVALID_QUANTITY and position is now "
                            "confirmed flat — treating as already closed, "
                            "not a failure",
                            self.symbol,
                        )
                        closed = True
                        break
            except Exception:
                self.client.log.exception(
                    "circuit breaker: market close attempt %d/%d for "
                    "%s raised an exception",
                    attempt,
                    max_attempts,
                    self.symbol,
                )
        if not closed:
            self.client.log.critical(
                "circuit breaker: FAILED TO CONFIRM position close for "
                "%s after %d attempts — a position may still be open "
                "and UNPROTECTED. Verify and close manually on QFEX "
                "immediately.",
                self.symbol,
                max_attempts,
            )

        # Step 3: hard stop, unconditionally.
        self.client.log.critical(
            "circuit breaker: stopping %s client now.", self.symbol
        )
        self.client.stop()

    # ---------- Quoting mode ----------

    def _compute_mid(self, bbo: BBO) -> Decimal:
        """
        Reference price for quoting:
            mid = mid_price_underlier_weight * underlier_price
                + mid_price_bbo_weight * (bbo.bid_px + bbo.ask_px) / 2
        (mid_price_bbo_weight is always 1 - mid_price_underlier_weight —
        see PerpetualMarketMakingConfig.)

        Normal callers reach here only after _quote() has confirmed
        underlier_price_by_symbol has a value at least once (see
        underlier_price_confirmed in __init__) — but that confirmation
        is one-way and never re-checked per tick, so if the underlier
        feed were to genuinely stop being present in the dict after
        having been confirmed (not just stale — actually absent), this
        still needs to not crash. Falls back to pure BBO midpoint in
        that case.

        Two different reasons underlier_price can be missing here, logged
        at two different levels: with mid_price_underlier_weight == 0,
        _quote() deliberately skips ever waiting on underlier_price_
        confirmed (see that method), so arriving here with no underlier
        price yet is the expected, common case — logged at DEBUG, same
        level as _quote's own pre-confirmation gap. With a nonzero
        weight, reaching here with no underlier price means confirmation
        had already happened and then the feed genuinely went missing —
        surprising, worth seeing in logs, so that case stays at WARNING.

        bbo.bid_px/ask_px are typed Optional[Decimal] on BBO (see
        models.py), since a one-sided or empty book is a real, valid
        state. _quote() (the only caller) already asserts both are
        non-None before calling this — has_two_sided_book()'s guarantee
        — but a static type checker can't see across that call boundary
        to know this method's own use of bbo.bid_px/ask_px is covered by
        it, so the same assert is repeated here, right where it's
        actually used (same pattern as simple_maker.py's ExampleMaker.
        on_bbo).
        """
        assert bbo.bid_px is not None and bbo.ask_px is not None

        underlier_price = self.client.underlier_price_by_symbol.get(self.symbol)
        bbo_mid = (bbo.bid_px + bbo.ask_px) / 2

        if underlier_price is None:
            if self.cfg.mid_price_underlier_weight == 0:
                self.client.log.debug(
                    "no underlier price for %s yet, but "
                    "mid_price_underlier_weight=0 so it wouldn't be used "
                    "anyway — using pure BBO midpoint (%s)",
                    self.symbol,
                    bbo_mid,
                )
            else:
                self.client.log.warning(
                    "underlier price for %s missing on this tick despite "
                    "having been confirmed available previously — falling "
                    "back to pure BBO midpoint (%s) for this tick only",
                    self.symbol,
                    bbo_mid,
                )
            return bbo_mid

        mid = (
            self.cfg.mid_price_underlier_weight * underlier_price
            + self.cfg.mid_price_bbo_weight * bbo_mid
        )
        # DEBUG, not INFO — fires on every _quote() call (i.e. every BBO
        # tick once quoting has started), so INFO would be too noisy for
        # normal operation. Logs every input to the blend plus the
        # result on one line specifically so this calculation can be
        # audited from the log directly — reconstructing it by hand from
        # separate BBO/underlier log lines is error-prone and, for
        # underlier specifically, was previously not even possible: the
        # raw underlier price never appeared anywhere in qfex2's own log
        # output before this line existed (only in the websockets
        # library's own truncated wire-level debug dump, which cuts off
        # before the price field). This line is the only place
        # underlier_price is directly attributable to a specific mid
        # calculation.
        self.client.log.debug(
            "MID %s underlier_price=%s (weight=%s) bbo_mid=%s (weight=%s) "
            "bbo_bid=%s bbo_ask=%s -> mid=%s",
            self.symbol,
            underlier_price,
            self.cfg.mid_price_underlier_weight,
            bbo_mid,
            self.cfg.mid_price_bbo_weight,
            bbo.bid_px,
            bbo.ask_px,
            mid,
        )
        return mid

    async def _quote(self, bbo: BBO) -> None:
        assert bbo.bid_px is not None and bbo.ask_px is not None  # has_two_sided_book guarantee

        # With mid_price_underlier_weight == 0, underlier_price has zero
        # influence on _compute_mid (multiplied by zero) regardless of
        # whether it's ever arrived — waiting on it before quoting would
        # be delaying for a feed this instance isn't using at all.
        # Exact-zero only, same reasoning as _tick's circuit-breaker
        # skip — see that method's docstring.
        if self.cfg.mid_price_underlier_weight == 0:
            mid = self._compute_mid(bbo)
        elif not self.underlier_price_confirmed.is_set():
            if self.client.underlier_price_by_symbol.get(self.symbol) is not None:
                self.underlier_price_confirmed.set()
                self.client.log.info(
                    "underlier price for %s now confirmed available — "
                    "quoting will begin from the next tick",
                    self.symbol,
                )
            else:
                # Logged at DEBUG, not WARNING/INFO, since this is
                # expected for however many ticks it takes right after
                # connecting — not a fault. It would be noisy at a
                # higher level given this fires every tick until the
                # first underlier message arrives.
                self.client.log.debug(
                    "no underlier price for %s yet — not quoting this "
                    "tick (see underlier_price_confirmed gate)",
                    self.symbol,
                )
            return
        else:
            mid = self._compute_mid(bbo)

        desired_bid = mid * (Decimal("1") - self.cfg.bid_spread)
        desired_ask = mid * (Decimal("1") + self.cfg.ask_spread)
        # Round AWAY from mid (bid down, ask up) so the resting order is
        # never more aggressive than intended — see _quantize_to_tick's
        # docstring for the full reasoning.
        desired_bid = self._quantize_to_tick(desired_bid, self.symbol, round_up=False)
        desired_ask = self._quantize_to_tick(desired_ask, self.symbol, round_up=True)
        desired_bid, bid_alive = self._apply_price_band(desired_bid, Side.BUY)
        desired_ask, ask_alive = self._apply_price_band(desired_ask, Side.SELL)

        if bid_alive:
            await self._refresh_quote_if_stale(
                side=Side.BUY,
                desired_price=desired_bid,
                active_order_id_attr="bid_order_id",
                active_price_attr="bid_price",
            )
        elif self.bid_order_id is not None:
            # Price band moved to exclude this side entirely — pull the
            # resting order rather than leave it resting outside the band.
            await self._cancel_side(Side.BUY)

        if ask_alive:
            await self._refresh_quote_if_stale(
                side=Side.SELL,
                desired_price=desired_ask,
                active_order_id_attr="ask_order_id",
                active_price_attr="ask_price",
            )
        elif self.ask_order_id is not None:
            await self._cancel_side(Side.SELL)

    def _quantize_to_tick(
        self, price: Decimal, symbol: str, round_up: bool
    ) -> Decimal:
        """
        Rounds `price` to a valid multiple of the symbol's tick_size
        (from self.client.refdata_by_symbol — see fetch_refdata).
        Required because QFEX rejects orders whose price isn't an exact
        tick multiple.

        `round_up` picks the direction — not "round to nearest," since
        the safe direction depends on what the price is for:
        - Quoting: round_up=False for a bid, True for an ask (away from
          mid, so the resting order is never more aggressive than
          intended).
        - Exit triggers: round toward entry — for a long, round_up=True
          for stop-loss, False for take-profit; for a short, flipped.
          The rule: the exit should fire at least as readily as the
          unrounded math intended, never less.

        If refdata isn't available for `symbol`, returns `price`
        unrounded (logged at WARNING) rather than raising.
        """
        refdata = self.client.refdata_by_symbol.get(symbol)
        if refdata is None:
            self.client.log.warning(
                "no refdata for %s (call client.fetch_refdata(%r) at "
                "startup) — sending price %s unrounded; QFEX may reject "
                "it with InvalidTickSizePrecisionPrice",
                symbol,
                symbol,
                price,
            )
            return price

        tick = refdata.tick_size
        if tick <= 0:
            return price

        ticks = price / tick
        rounded_ticks = ticks.to_integral_value(
            rounding="ROUND_CEILING" if round_up else "ROUND_FLOOR"
        )
        return rounded_ticks * tick

    def _apply_price_band(self, price: Decimal, side: Side) -> tuple[Decimal, bool]:
        """
        Port of Hummingbot's apply_price_band: a BUY quote above
        price_ceiling, or a SELL quote below price_floor, is suppressed
        entirely rather than clamped — Hummingbot's version kills the
        order (sets its size to 0) rather than moving its price to the
        band edge, and this does the same (returns alive=False) rather
        than silently repricing to the boundary, since clamping would
        quietly change what price you're resting at without you asking
        for that.
        """
        if side == Side.BUY and self.cfg.price_ceiling is not None:
            if price > self.cfg.price_ceiling:
                return price, False
        if side == Side.SELL and self.cfg.price_floor is not None:
            if price < self.cfg.price_floor:
                return price, False
        return price, True

    async def _refresh_quote_if_stale(
        self,
        side: Side,
        desired_price: Decimal,
        active_order_id_attr: str,
        active_price_attr: str,
    ) -> None:
        active_order_id = getattr(self, active_order_id_attr)
        active_price = getattr(self, active_price_attr)

        if active_order_id is None:
            await self._place_quote(side, desired_price, active_order_id_attr, active_price_attr)
            return

        if self._quotes_are_stale(active_price, desired_price):
            await self._cancel_side(side)
            await self._place_quote(side, desired_price, active_order_id_attr, active_price_attr)
        # else: within tolerance — leave the resting order alone. This is
        # Hummingbot's order_refresh_tolerance_pct: avoids cancel/replace
        # churn (and exchange rate limits) for price moves too small to
        # matter.

    def _quotes_are_stale(self, active_price: Optional[Decimal], desired_price: Decimal) -> bool:
        if active_price is None or active_price == 0:
            return True
        deviation = abs(desired_price - active_price) / active_price
        return deviation > self.cfg.order_refresh_tolerance

    async def _place_quote(
        self,
        side: Side,
        price: Decimal,
        active_order_id_attr: str,
        active_price_attr: str,
    ) -> None:
        label = "BID" if side == Side.BUY else "ASK"
        try:
            result = await self.client.send_gtc_alo(
                symbol=self.symbol,
                side=side,
                quantity=self.cfg.order_amount,
                price=price,
            )
        except asyncio.TimeoutError:
            self.client.log.error("%s placement timed out waiting for ACK/REJECT", label)
            return
        except QFEXTradeError as e:
            # QFEX rejected this via the top-level err envelope (e.g.
            # InvalidTickSizePrecisionPrice) rather than a normal
            # order_response — see QFEXTradeError's docstring in
            # base_client.py. Same recovery as a normal REJECTED
            # order_response below: log and leave active_order_id_attr
            # unset so the next tick retries at a fresh price, rather than
            # letting this exception propagate up through on_bbo and take
            # down the whole MDS receive loop (this happened live before
            # this except clause existed — see git history / chat log for
            # 2026-07-25).
            self.client.log.error(
                "%s rejected: %s (error_code=%s)",
                label,
                e,
                e.err.get("error_code"),
            )
            return

        orsp = result["final_order_response"]
        status = orsp.get("status")
        if status == "ACK":
            setattr(self, active_order_id_attr, orsp.get("order_id"))
            setattr(self, active_price_attr, price)
            self.client.log.info(
                "%s ACK order_id=%s px=%s qty=%s",
                label,
                orsp.get("order_id"),
                orsp.get("price"),
                orsp.get("quantity"),
            )
        else:
            # Most commonly REJECTED (crossed the book between our BBO
            # snapshot and the order reaching the matching engine) — leave
            # active_order_id_attr as None so the NEXT tick tries again at
            # a fresh price, rather than retrying in a loop within this
            # tick.
            self.client.log.warning("%s not resting: status=%s resp=%s", label, status, orsp)

    async def _cancel_side(self, side: Side) -> None:
        order_id_attr = "bid_order_id" if side == Side.BUY else "ask_order_id"
        price_attr = "bid_price" if side == Side.BUY else "ask_price"
        label = "BID" if side == Side.BUY else "ASK"
        order_id = getattr(self, order_id_attr)
        if order_id is None:
            return
        try:
            result = await self.client.cancel_order(order_id, self.symbol)
            self.client.log.info(
                "%s cancelled: order_id=%s status=%s",
                label,
                order_id,
                result["final_order_response"].get("status"),
            )
        except asyncio.TimeoutError:
            self.client.log.error(
                "%s cancel_order timed out for order_id=%s — it may still "
                "be resting; NOT clearing local order_id so a future "
                "cancel attempt is retried rather than silently orphaning "
                "the order",
                label,
                order_id,
            )
            return
        except QFEXTradeError as e:
            # Already-safe before this except existed too (the bare
            # `except Exception` below would have caught it), but this
            # gives a clean one-line log instead of a full traceback dump
            # for what's typically just "order already gone" (e.g.
            # NO_SUCH_ORDER) rather than a genuine bug. NOT clearing
            # order_id_attr, same reasoning as the timeout case above —
            # if the rejection means the order is already gone, a future
            # cancel attempt against a nonexistent order_id is harmless;
            # if it means something else, keeping local state pointing at
            # it is safer than silently forgetting about it.
            self.client.log.error(
                "%s cancel_order rejected for order_id=%s: %s (error_code=%s)",
                label,
                order_id,
                e,
                e.err.get("error_code"),
            )
            return
        except Exception:
            self.client.log.exception(
                "%s cancel_order failed for order_id=%s", label, order_id
            )
            return
        setattr(self, order_id_attr, None)
        setattr(self, price_attr, None)

    async def _cancel_quotes_if_any(self) -> None:
        await self._await_bg_quote_cancel()
        await self._cancel_quotes_now()

    async def _cancel_quotes_now(self) -> None:
        # Both sides concurrently (changed 2026-10-14; was one after the
        # other, doubling the wait when QFEX is slow).
        sides = [s for s, oid in ((Side.BUY, self.bid_order_id), (Side.SELL, self.ask_order_id)) if oid is not None]
        if sides:
            await asyncio.gather(*(self._cancel_side(s) for s in sides))

    # ---------- Position-managing mode ----------

    async def _manage_position(
        self, bbo: BBO, position_qty: Decimal, average_price: Decimal
    ) -> None:
        """
        position_qty is signed: positive = long, negative = short (per
        PositionState.position in models.py / positions channel docs).
        average_price is QFEX's own entry price for the current position
        (docs.qfex.com/websocket/channels/trade/positions).

        Both are passed straight through to _maybe_renew_stop_orders,
        which compares them against self._stop_orders_qty/
        _stop_orders_avg_price (what the CURRENTLY resting stop orders
        were placed for) to decide whether a partial fill has made them
        stale, in which case they're cancelled and replaced immediately.
        See that method's docstring.

        Checks the BBO-triggered fast-close FIRST, before the normal
        place/renew logic — see _maybe_bbo_triggered_close. If that
        successfully closes the position this tick, the normal stop-
        order place/renew logic below is skipped entirely for this tick:
        there is no longer a position to manage, and _tick will notice
        that (position_qty == 0) on its own next call and route through
        the flat branch instead.
        """
        is_long = position_qty > 0
        exit_side = Side.SELL if is_long else Side.BUY
        exit_qty = abs(position_qty)

        # Round toward entry for both triggers — see _quantize_to_tick's
        # docstring for the full reasoning (both exits should fire no
        # later / no less readily than the unrounded math intended).
        if is_long:
            stop_loss_trigger = average_price * (Decimal("1") - self.cfg.stop_loss_spread)
            take_profit_trigger = average_price * (Decimal("1") + self.cfg.take_profit_spread)
            stop_loss_trigger = self._quantize_to_tick(stop_loss_trigger, self.symbol, round_up=True)
            take_profit_trigger = self._quantize_to_tick(take_profit_trigger, self.symbol, round_up=False)
        else:
            stop_loss_trigger = average_price * (Decimal("1") + self.cfg.stop_loss_spread)
            take_profit_trigger = average_price * (Decimal("1") - self.cfg.take_profit_spread)
            stop_loss_trigger = self._quantize_to_tick(stop_loss_trigger, self.symbol, round_up=False)
            take_profit_trigger = self._quantize_to_tick(take_profit_trigger, self.symbol, round_up=True)

        closed = await self._maybe_bbo_triggered_close(
            bbo, is_long, exit_side, exit_qty, stop_loss_trigger, take_profit_trigger
        )
        if closed:
            return

        stale_legs = self._stale_stop_legs(exit_qty, average_price)
        if stale_legs:
            # Added 2026-10-27: a resting leg sized/priced for an earlier state of the
            # position (typically the first piece of a partial fill) is renewed even
            # while the other leg is missing — before, only the missing leg was placed.
            self.client.log.info(
                "renewing stop orders for %s immediately: %s sized/priced for an "
                "earlier position (now qty=%s avg_price=%s; placed for %s)",
                self.symbol,
                "+".join(stale_legs),
                exit_qty,
                average_price,
                {k: v for k, v in self._stop_leg_placed_for.items()},
            )
            await self._cancel_stop_orders_if_any()
            await self._place_stop_orders(
                exit_side, exit_qty, stop_loss_trigger, take_profit_trigger, average_price
            )
        elif self.stop_loss_order_id is None or self.take_profit_order_id is None:
            await self._place_stop_orders(
                exit_side, exit_qty, stop_loss_trigger, take_profit_trigger, average_price
            )
        else:
            await self._maybe_renew_stop_orders(
                exit_side, exit_qty, stop_loss_trigger, take_profit_trigger, average_price
            )

    async def _maybe_bbo_triggered_close(
        self,
        bbo: BBO,
        is_long: bool,
        exit_side: Side,
        exit_qty: Decimal,
        stop_loss_trigger: Decimal,
        take_profit_trigger: Decimal,
    ) -> bool:
        """
        Confirmed live (2026-08-02, CL-USD): BBO can cross a stop-loss/
        take-profit trigger price before mark_price does, since QFEX's
        native STOP_LOSS/TAKE_PROFIT orders trigger off mark_price, not
        the live book — the same mark_price/BBO lag pattern behind
        several of this session's circuit-breaker trips can just as
        easily leave a real stop level crossed on the live book while
        the native order sits un-triggered, waiting for a slower feed to
        catch up. This checks BBO directly, every tick a position is
        open, and fires an immediate reduce_only MARKET close the moment
        it's crossed — not waiting on mark_price at all.

        Always checks the side that would actually be transacted at
        (matching how a real exit would fill): long positions check bid
        (what you'd sell into); short positions check ask (what you'd
        buy into) — bid and ask can move somewhat independently, so
        checking the wrong side could trigger on a price you wouldn't
        actually get, or miss a real cross on the side that matters.

        take_profit_hit fires the instant it's crossed, same as
        originally built. stop_loss_hit is gated by a confirmation
        streak (self.cfg.bbo_stop_loss_confirm_streak, default 1 = fires
        on the first crossing tick, unchanged from original behavior) —
        raised at the user's request (2026-08-02) after several BBO-
        triggered stop-losses fired on what looked, from the log, like a
        single-tick jump
        straight through the trigger with no visible
        run-up; whether those specific moves would have reversed is
        NOT verifiable after the fact (the position was already closed,
        so there's no continued BBO series to check against) — this
        streak exists to let that question actually be answered going
        forward, by giving a real, measurable knob rather than more
        after-the-fact log reading. Asymmetric because a slow take-
        profit only costs some unrealized gain (bounded, gradual), while
        a slow stop-loss means staying in a position actively moving
        against you for longer.

        On a successful fill: cancels both native STOP_LOSS/TAKE_PROFIT
        orders (best-effort — a cancel failure here is logged but does
        NOT block returning True, since the position is already closed;
        an uncancelled stop order pointed at a now-flat position is a
        secondary concern, and _cancel_stop_orders_if_any's own bounded
        retry — see that method — still applies to it going forward from
        the strategy's regular flat-branch handling). Returns True so
        _manage_position skips the normal place/renew logic this tick —
        there's no longer a position for it to manage.

        On no trigger, a rejection, a timeout, or an exception: makes NO
        change to the native STOP_LOSS/TAKE_PROFIT orders and returns
        False — _manage_position proceeds to its normal place/renew
        logic exactly as if this check didn't exist. This is
        deliberately fire-first-cancel-after-confirmed-fill, not
        cancel-first: the whole point is reacting faster than the native
        trigger, and cancelling first would only add latency for no
        benefit, since a failed/rejected close leaves the native orders
        exactly as protective as they already were.
        """
        assert bbo.bid_px is not None and bbo.ask_px is not None

        if is_long:
            check_price = bbo.bid_px
            stop_loss_hit = check_price <= stop_loss_trigger
            take_profit_hit = check_price >= take_profit_trigger
        else:
            check_price = bbo.ask_px
            stop_loss_hit = check_price >= stop_loss_trigger
            take_profit_hit = check_price <= take_profit_trigger

        # Confirmation streak — stop-loss only, take_profit_hit above is
        # untouched and stays instant. See self.cfg.bbo_stop_loss_confirm_
        # streak's comment for why this is asymmetric. Reset the streak
        # (not just its count, but which trigger_price it's counting
        # against) whenever stop_loss_trigger itself changes — a new
        # position, or a renewal after a partial fill changes
        # average_price, must start a fresh count rather than inherit
        # ticks accumulated against a now-irrelevant price.
        if self._stop_loss_confirm_streak_trigger != stop_loss_trigger:
            self._stop_loss_confirm_streak = 0
            self._stop_loss_confirm_streak_trigger = stop_loss_trigger
        if stop_loss_hit:
            self._stop_loss_confirm_streak += 1
        else:
            self._stop_loss_confirm_streak = 0
        # Changed 2026-10-14: the confirmation streak exists to ride out a
        # one-tick wick while the exchange-side STOP_LOSS backstops the
        # position. If no exchange stop is resting (not placed yet, rejected
        # as INVALID_PRICE, or suppressed because mark is already past it),
        # waiting is pure risk — fire on the first crossing tick.
        stop_loss_confirmed = (
            self._stop_loss_confirm_streak >= self.cfg.bbo_stop_loss_confirm_streak
            or self.stop_loss_order_id is None
        )

        if not ((stop_loss_hit and stop_loss_confirmed) or take_profit_hit):
            return False

        label = "STOP_LOSS" if (stop_loss_hit and stop_loss_confirmed) else "TAKE_PROFIT"
        trigger_price = stop_loss_trigger if label == "STOP_LOSS" else take_profit_trigger

        # Added 2026-10-19: don't send while the trade connection is down —
        # the order would only sit in the outbound queue and go out late.
        is_connected = getattr(self.client, "is_trade_connected", None)
        if is_connected is not None and not is_connected():
            self._log_bbo_close_skip(
                "BBO-triggered %s close for %s skipped: trade connection is "
                "down — will re-check after it reconnects (native stops, if "
                "still resting, keep protecting the position)",
                label, self.symbol,
            )
            return False

        # Added 2026-10-19: broken-book guard (see cfg.bbo_close_max_spread).
        if label == "STOP_LOSS" and self._book_looks_broken(bbo):
            mark = self.client.mark_price_by_symbol.get(self.symbol)
            mark_hit = mark is not None and (
                mark <= stop_loss_trigger if is_long else mark >= stop_loss_trigger
            )
            if not mark_hit:
                self._log_bbo_close_skip(
                    "BBO-triggered STOP_LOSS close for %s skipped: book looks "
                    "broken (bid=%s ask=%s, spread > %s) and mark_price=%s has "
                    "not reached trigger=%s — not market-closing into it; the "
                    "mark-triggered native STOP_LOSS stays in charge",
                    self.symbol, bbo.bid_px, bbo.ask_px,
                    self.cfg.bbo_close_max_spread, mark, stop_loss_trigger,
                )
                return False
        self.client.log.warning(
            "BBO-triggered %s close for %s: %s side price=%s crossed "
            "trigger=%s — sending immediate reduce_only MARKET close "
            "ahead of the native mark_price-triggered order",
            label,
            self.symbol,
            "bid" if is_long else "ask",
            check_price,
            trigger_price,
        )

        try:
            result = await self.client.send_market(
                symbol=self.symbol,
                side=exit_side,
                quantity=exit_qty,
                reduce_only=True,
            )
            orsp = result["final_order_response"]
        except asyncio.TimeoutError:
            self.client.log.error(
                "BBO-triggered %s close for %s timed out waiting for "
                "ACK/REJECT — leaving native STOP_LOSS/TAKE_PROFIT in "
                "place; will retry the BBO check again next tick if "
                "still crossed",
                label,
                self.symbol,
            )
            return False
        except QFEXTradeError as e:
            self.client.log.error(
                "BBO-triggered %s close for %s rejected: %s "
                "(error_code=%s) — leaving native STOP_LOSS/TAKE_PROFIT "
                "in place",
                label,
                self.symbol,
                e,
                e.err.get("error_code"),
            )
            return False

        closing_pos = exit_qty if is_long else -exit_qty
        if orsp.get("status") == "INVALID_QUANTITY":
            # reduce_only refused: the position is already flat on QFEX's
            # side (typically the native stop fired in the same instant).
            # Wait for the position update instead of re-placing stops.
            self.client.log.warning(
                "BBO-triggered %s close for %s rejected INVALID_QUANTITY — "
                "position already closed on the exchange (native stop "
                "probably fired first); waiting for the position update",
                label,
                self.symbol,
            )
            self._arm_exit_guard(closing_pos)
            return True

        if orsp.get("status") != "FILLED":
            self.client.log.warning(
                "BBO-triggered %s close for %s not filled: status=%s "
                "resp=%s — leaving native STOP_LOSS/TAKE_PROFIT in place",
                label,
                self.symbol,
                orsp.get("status"),
                orsp,
            )
            return False

        self.client.log.warning(
            "BBO-triggered %s close for %s FILLED at %s — cancelling "
            "native STOP_LOSS/TAKE_PROFIT",
            label,
            self.symbol,
            orsp.get("price"),
        )
        self._arm_exit_guard(closing_pos)
        try:
            await self._cancel_stop_orders_if_any()
        except Exception:
            self.client.log.exception(
                "BBO-triggered close for %s: failed to cancel native "
                "STOP_LOSS/TAKE_PROFIT after the close filled — position "
                "is already flat, so this is a cleanup issue, not a "
                "protection gap; _cancel_stop_orders_if_any's own "
                "bounded retry still applies going forward",
                self.symbol,
            )
        return True

    def _stale_stop_legs(self, exit_qty: Decimal, average_price: Decimal) -> list:
        """Labels of RESTING stop legs that were placed for a different qty or
        average_price than the position has now. A resting leg with no record
        (should not happen) is treated as stale so it gets re-placed. Added 2026-10-27."""
        stale = []
        for label, attr in (("STOP_LOSS", "stop_loss_order_id"), ("TAKE_PROFIT", "take_profit_order_id")):
            if getattr(self, attr) is None:
                continue
            if self._stop_leg_placed_for.get(attr) != (exit_qty, average_price):
                stale.append(label)
        return stale

    async def _place_stop_orders(
        self,
        exit_side: Side,
        exit_qty: Decimal,
        stop_loss_trigger: Decimal,
        take_profit_trigger: Decimal,
        average_price: Decimal,
    ) -> None:
        """
        Places whichever of STOP_LOSS/TAKE_PROFIT isn't currently
        resting. Each leg is gated by
        client.is_order_outstanding(client_order_id): if the previous
        attempt's client_order_id hasn't resolved yet (see
        is_order_outstanding in base_client.py), this skips retrying it
        this tick.

        Why: a placement can time out locally while QFEX is still
        processing it, and retrying immediately (the next BBO tick can
        arrive well under a second later) risks a duplicate order if the
        original still lands. Confirmed live (2026-07-27, CL-USD): a
        fixed order_update_timeout_s cooldown was NOT a safe enough gate
        — QFEX's real ACK for a locally-timed-out placement arrived up
        to several seconds after the cooldown had already elapsed, and
        duplicate STOP_LOSS/TAKE_PROFIT orders actually rested on the
        exchange simultaneously (only saved by QFEX's own server-side
        auto-cancellation of the older duplicates — not something this
        client can rely on). Gating on the specific previous attempt's
        actual resolution, not a fixed timer, closes that race — see
        is_order_outstanding's 15-second backstop for what happens if a
        response is genuinely lost rather than merely late.

        STOP_LOSS and TAKE_PROFIT are placed via the same logic
        (_place_one_stop_leg) — they differ only in which client method
        to call, which trigger price to use, and which instance
        attributes track that leg's state.
        """
        legs = []
        if self.stop_loss_order_id is None:
            legs.append(self._place_one_stop_leg(
                label="STOP_LOSS",
                send=self.client.send_stop_loss,
                exit_side=exit_side,
                exit_qty=exit_qty,
                trigger_price=stop_loss_trigger,
                average_price=average_price,
                order_id_attr="stop_loss_order_id",
                trigger_price_attr="stop_loss_trigger_price",
                last_attempt_at_attr="_stop_loss_last_attempt_at",
                last_client_oid_attr="_stop_loss_last_client_oid",
                suppressed_trigger_attr="_stop_loss_suppressed_trigger",
            ))

        if self.take_profit_order_id is None:
            legs.append(self._place_one_stop_leg(
                label="TAKE_PROFIT",
                send=self.client.send_take_profit,
                exit_side=exit_side,
                exit_qty=exit_qty,
                trigger_price=take_profit_trigger,
                average_price=average_price,
                order_id_attr="take_profit_order_id",
                trigger_price_attr="take_profit_trigger_price",
                last_attempt_at_attr="_take_profit_last_attempt_at",
                last_client_oid_attr="_take_profit_last_client_oid",
                suppressed_trigger_attr="_take_profit_suppressed_trigger",
            ))
        # Both legs concurrently (changed 2026-10-14; was one after the other).
        if legs:
            await asyncio.gather(*legs)

        self._stop_orders_qty = exit_qty
        self._stop_orders_avg_price = average_price

    def _mark_price_makes_trigger_unplaceable(
        self, label: str, exit_side: Side, trigger_price: Decimal
    ) -> bool:
        """
        True if mark_price is already on the wrong side of `trigger_price`
        for this leg — meaning QFEX will reject a placement at this exact
        price as INVALID_PRICE, since a STOP_LOSS/TAKE_PROFIT can't be
        placed at a level the reference price it triggers against has
        already passed.

        Confirmed live (2026-08-02, CL-USD): this happens without
        anything being "broken" — mark_price legitimately ran ahead of a
        newly-opened position's take-profit level before the placement
        landed (the same mark_price/BBO lag documented throughout this
        codebase, just encountered here as a placement precondition
        rather than as a stop-trigger event). Critically, in that
        incident BBO itself never reached the trigger price during the
        entire ~80s rejection window — only mark_price had drifted past
        it — so treating this as "close the position now" (the way
        _maybe_bbo_triggered_close reacts to a genuine BBO cross) would
        have force-closed the position based on a lagging feed reading a
        price the market hadn't actually reached. This deliberately does
        NOT close anything — see _place_one_stop_leg, which just skips
        the doomed placement until this stops being true. If BBO
        actually reaches the trigger in the meantime, _maybe_bbo_
        triggered_close (which checks the live book, not mark_price)
        already handles that independently of this method.

        exit_side is what closing this position actually does: SELL
        closes a long (so BUY-side triggers rise, SELL-side triggers
        fall relative to entry — mirroring _maybe_bbo_triggered_close's
        is_long/exit_side reasoning), BUY closes a short (mirrored).
        label distinguishes STOP_LOSS (triggers moving AGAINST the
        position) from TAKE_PROFIT (triggers moving WITH it) — same
        two axes _manage_position already uses to compute trigger_price
        in the first place, just re-derived here from what's available
        at the call site rather than threaded through as an extra flag.
        """
        mark_price = self.client.mark_price_by_symbol.get(self.symbol)
        if mark_price is None:
            # No mark_price yet at all — nothing to compare against, so
            # nothing to suppress; let the normal placement attempt
            # proceed and fail (or succeed) on its own terms.
            return False

        is_long = exit_side == Side.SELL
        is_stop_loss = label == "STOP_LOSS"
        # long, stop-loss: triggers on a FALL -> unplaceable if mark_price
        #   has already fallen to/through it.
        # long, take-profit: triggers on a RISE -> unplaceable if
        #   mark_price has already risen to/through it.
        # short: both mirrored (exit_side == BUY).
        triggers_on_fall = is_long == is_stop_loss
        if triggers_on_fall:
            return mark_price <= trigger_price
        else:
            return mark_price >= trigger_price

    async def _place_one_stop_leg(
        self,
        *,
        label: str,
        send: Any,
        exit_side: Side,
        exit_qty: Decimal,
        trigger_price: Decimal,
        average_price: Decimal,
        order_id_attr: str,
        trigger_price_attr: str,
        last_attempt_at_attr: str,
        last_client_oid_attr: str,
        suppressed_trigger_attr: str,
    ) -> None:
        """
        Places a single STOP_LOSS or TAKE_PROFIT leg — the shared body
        behind both branches of _place_stop_orders (see that method's
        docstring for why this gating exists). `send` is
        self.client.send_stop_loss or self.client.send_take_profit
        (identical signatures: symbol/side/quantity/trigger_price/
        client_order_id → Dict). `*_attr` args name the instance
        attributes this leg tracks — read/written via getattr/setattr,
        matching the pattern already used elsewhere in this file for the
        same STOP_LOSS/TAKE_PROFIT pairing (see _cancel_side,
        _cancel_stop_orders_if_any, _note_stop_cancel_failure).

        `average_price` is passed explicitly by the caller (rather than
        read off self._stop_orders_avg_price) because that instance
        attribute isn't updated until after BOTH legs have been
        attempted in _place_stop_orders — reading it here, on the first
        leg of a newly-opened position, would return a stale value left
        over from the previous position (or an initial default). Passing
        it as a parameter guarantees correctness regardless of call
        order. Used only for trigger_logger.log_trigger_ack below.

        Caller is responsible for the `order_id_attr is None` check —
        this method always attempts a placement (subject to the
        outstanding-order and mark_price-unplaceable gates below) when
        called.
        """
        prior_client_oid = getattr(self, last_client_oid_attr)
        still_outstanding = (
            prior_client_oid is not None
            and self.client.is_order_outstanding(prior_client_oid)
        )
        if still_outstanding:
            self.client.log.debug(
                "%s placement skipped this tick — the previous attempt "
                "(client_order_id=%s) has not yet been resolved by "
                "QFEX; will retry once it resolves or the "
                "outstanding-order backstop elapses",
                label,
                prior_client_oid,
            )
            return

        if self._mark_price_makes_trigger_unplaceable(label, exit_side, trigger_price):
            already_suppressing_this_price = getattr(self, suppressed_trigger_attr) == trigger_price
            setattr(self, suppressed_trigger_attr, trigger_price)
            if not already_suppressing_this_price:
                self.client.log.warning(
                    "%s placement for %s suppressed: trigger=%s is "
                    "already on the wrong side of mark_price=%s, so "
                    "QFEX would reject it as INVALID_PRICE — not "
                    "retrying every tick (see "
                    "_mark_price_makes_trigger_unplaceable). Will "
                    "attempt again once mark_price moves back, or "
                    "immediately if BBO reaches this level first (see "
                    "_maybe_bbo_triggered_close). Logged once per "
                    "distinct trigger price, not every tick.",
                    label,
                    self.symbol,
                    trigger_price,
                    self.client.mark_price_by_symbol.get(self.symbol),
                )
            return
        setattr(self, suppressed_trigger_attr, None)

        client_oid = secrets.token_hex(16)
        setattr(self, last_attempt_at_attr, time.monotonic())
        setattr(self, last_client_oid_attr, client_oid)
        try:
            result = await send(
                symbol=self.symbol,
                side=exit_side,
                quantity=exit_qty,
                trigger_price=trigger_price,
                client_order_id=client_oid,
            )
            orsp = result["final_order_response"]
            if orsp.get("status") == "ACK":
                setattr(self, order_id_attr, orsp.get("order_id"))
                setattr(self, trigger_price_attr, trigger_price)
                self._stop_leg_placed_for[order_id_attr] = (exit_qty, average_price)
                self.client.log.info(
                    "%s ACK order_id=%s trigger=%s qty=%s",
                    label,
                    orsp.get("order_id"),
                    trigger_price,
                    exit_qty,
                )
                self.trigger_logger.log_trigger_ack(
                    label=label,
                    symbol=self.symbol,
                    exit_side=str(exit_side),
                    order_id=orsp.get("order_id"),
                    client_order_id=client_oid,
                    trigger_price=trigger_price,
                    exit_qty=exit_qty,
                    average_price=average_price,
                    ts=time.time(),
                )
            else:
                self.client.log.warning(
                    "%s not resting: status=%s resp=%s", label, orsp.get("status"), orsp
                )
        except asyncio.TimeoutError:
            self.client.log.error(
                "%s placement timed out waiting for ACK/REJECT "
                "(client_order_id=%s) — will not retry this leg until "
                "QFEX resolves this specific attempt or the "
                "outstanding-order backstop elapses (see "
                "_place_stop_orders docstring)",
                label,
                client_oid,
            )
        except QFEXTradeError as e:
            # Same reasoning as _place_quote's QFEXTradeError catch —
            # without this, a rejection here (e.g. a stop trigger price
            # that isn't tick-aligned) would propagate up through on_bbo
            # and crash the MDS receive loop instead of just failing
            # this one placement attempt.
            self.client.log.error(
                "%s rejected: %s (error_code=%s)",
                label,
                e,
                e.err.get("error_code"),
            )

    async def _maybe_renew_stop_orders(
        self,
        exit_side: Side,
        exit_qty: Decimal,
        stop_loss_trigger: Decimal,
        take_profit_trigger: Decimal,
        average_price: Decimal,
    ) -> None:
        """
        Renews the resting stop-loss/take-profit pair if the live
        position no longer matches what the currently resting orders
        were sized/priced against (a partial fill changed size and/or
        average_price). A stale-sized stop order would over- or
        under-close the position if triggered, so this renews
        immediately rather than leaving it resting.

        No time-based fallback: both trigger prices are pure functions
        of average_price, which only changes on a fill — there is
        nothing else for a price to drift against between fills, so a
        periodic "renew just in case" on an unchanged position would
        cancel and re-place two functionally identical orders for no
        protective benefit. If average_price and exit_qty are unchanged,
        this is a no-op.
        """
        qty_or_price_changed = (
            self._stop_orders_qty != exit_qty
            or self._stop_orders_avg_price != average_price
        )
        if not qty_or_price_changed:
            return

        self.client.log.info(
            "renewing stop orders for %s immediately: position changed "
            "(qty %s -> %s, avg_price %s -> %s) since current stop "
            "orders were placed",
            self.symbol,
            self._stop_orders_qty,
            exit_qty,
            self._stop_orders_avg_price,
            average_price,
        )
        await self._cancel_stop_orders_if_any()
        await self._place_stop_orders(
            exit_side, exit_qty, stop_loss_trigger, take_profit_trigger, average_price
        )

    async def _cancel_stop_orders_if_any(self) -> None:
        """
        Cancels resting stop-loss/take-profit orders via
        client.cancel_stop_order() — not client.cancel_order(), since
        QFEX tracks stop orders in a separate registry from regular
        orders.

        Bounded retry: self._stop_cancel_failures[order_id] counts
        consecutive cancel failures (any cause) for that order_id, reset
        on success. After self.client.cfg.max_stop_cancel_failures,
        gives up: clears local tracking so the retry loop stops, and
        logs at ERROR that this is a forced abandonment, not a confirmed
        cancellation — the order may still be resting on QFEX's side;
        verify manually.
        """
        for label, order_id_attr, price_attr in (
            ("STOP_LOSS", "stop_loss_order_id", "stop_loss_trigger_price"),
            ("TAKE_PROFIT", "take_profit_order_id", "take_profit_trigger_price"),
        ):
            order_id = getattr(self, order_id_attr)
            if order_id is None:
                continue
            try:
                result = await self.client.cancel_stop_order(order_id)
                self.client.log.info(
                    "%s cancelled: order_id=%s status=%s",
                    label,
                    order_id,
                    result["final_order_response"].get("status"),
                )
                self._stop_cancel_failures.pop(order_id, None)
            except (asyncio.TimeoutError, QFEXTradeError):
                # Expected failure modes (a slow/unresponsive cancel, or a
                # rejection like NO_SUCH_ORDER for an order that already
                # resolved) — no per-attempt log here; _note_stop_cancel_
                # failure logs once, loudly, if this order hits the retry
                # cap. Not clearing order_id_attr here, so the next tick
                # retries rather than orphaning the order.
                self._note_stop_cancel_failure(label, order_id, order_id_attr, price_attr)
                continue
            except Exception:
                # Genuinely unexpected — worth a full traceback immediately
                # rather than waiting out the retry cap in silence.
                self.client.log.exception(
                    "%s cancel_stop_order failed unexpectedly for order_id=%s",
                    label,
                    order_id,
                )
                self._note_stop_cancel_failure(label, order_id, order_id_attr, price_attr)
                continue
            setattr(self, order_id_attr, None)
            setattr(self, price_attr, None)
        self._stop_orders_qty = None
        self._stop_orders_avg_price = None
        # Also clear attempt tracking — a fresh placement for a new
        # position shouldn't be gated by a leftover client_order_id from
        # a previous one. If left set, a stale id could make
        # is_order_outstanding() return a misleading answer for a future
        # attempt that reuses this leg's tracking slot; clearing it here
        # ensures _place_stop_orders always starts fresh.
        self._stop_loss_last_attempt_at = None
        self._take_profit_last_attempt_at = None
        self._stop_loss_last_client_oid = None
        self._take_profit_last_client_oid = None
        # Same reasoning: a stale suppressed trigger_price from a
        # previous position must not silently suppress a placement for a
        # NEW position that happens to compute the same trigger_price by
        # coincidence.
        self._stop_loss_suppressed_trigger = None
        self._take_profit_suppressed_trigger = None
        # Same reasoning again: a confirmation streak counted against a
        # now-closed position's trigger_price must not carry into
        # whatever position/trigger comes next.
        self._stop_loss_confirm_streak = 0
        self._stop_loss_confirm_streak_trigger = None

    def _note_stop_cancel_failure(
        self, label: str, order_id: str, order_id_attr: str, price_attr: str
    ) -> None:
        """
        Increments the consecutive-failure counter for `order_id`. At
        self.client.cfg.max_stop_cancel_failures, gives up: clears local
        tracking so _cancel_stop_orders_if_any stops retrying, and logs
        at ERROR that this is a forced abandonment, not a confirmed
        cancellation. Also schedules one final, uncounted verification
        attempt (see _verify_stop_cancel_after_giving_up) as a background
        task — fire-and-forget, since this method itself is synchronous
        and called from inside the normal tick path, which shouldn't
        block waiting on a delayed retry.
        """
        count = self._stop_cancel_failures.get(order_id, 0) + 1
        self._stop_cancel_failures[order_id] = count
        if count < self.client.cfg.max_stop_cancel_failures:
            return

        self.client.log.error(
            "%s: giving up on cancelling order_id=%s after %d consecutive "
            "failed cancel attempts — clearing local tracking so this "
            "strategy stops retrying. Scheduling one final verification "
            "attempt in %.1fs; if that also fails, QFEX has NOT confirmed "
            "this order is actually cancelled and it should be checked "
            "manually (QFEX's UI, or another cancel_stop_order call by "
            "hand).",
            label,
            order_id,
            count,
            self.client.cfg.stop_cancel_verify_delay_s,
        )
        setattr(self, order_id_attr, None)
        setattr(self, price_attr, None)
        self._stop_cancel_failures.pop(order_id, None)
        asyncio.create_task(
            self._verify_stop_cancel_after_giving_up(label, order_id)
        )

    async def _verify_stop_cancel_after_giving_up(
        self, label: str, order_id: str
    ) -> None:
        """
        One final cancel_stop_order attempt, self.client.cfg.stop_cancel_
        verify_delay_s after _note_stop_cancel_failure gave up on the
        counted retries — deliberately uncounted against
        max_stop_cancel_failures and run as a background task (via
        asyncio.create_task in _note_stop_cancel_failure) rather than
        inline, since by the time this fires the strategy has already
        moved on (order_id_attr is already cleared) and this shouldn't
        block or interact with whatever the strategy is doing by then.

        Confirmed live (2026-08-06, CL-USD): of 7 give-ups in one run, 5
        eventually got a late CANCELLED confirmation the strategy never
        saw, because nothing was still asking by the time it arrived —
        this exists to actually ask again, once, after giving the
        server-side issue a bit more time to clear, so "unconfirmed"
        resolves to a real answer more often than being a permanent
        unknown that only manual UI-checking can resolve.

        Every outcome is logged distinctly and at CRITICAL — this is a
        genuinely last check; if it also fails, the order's status truly
        is unknown and manual verification is the only remaining option:
          - CANCELLED or NO_SUCH_ORDER: confirms the order is gone (the
            original cancel almost certainly succeeded despite never
            confirming it before now, or it had already resolved some
            other way) — logged as a resolved outcome, not an error.
          - Any other terminal status, or another failure/exception:
            logged plainly as still unconfirmed — this does NOT retry
            again or re-enter the normal retry path; one verification
            attempt is the whole point, not an unbounded tail.
        """
        await asyncio.sleep(self.client.cfg.stop_cancel_verify_delay_s)
        try:
            result = await self.client.cancel_stop_order(order_id)
            status = result["final_order_response"].get("status")
            if status in ("CANCELLED", "NO_SUCH_ORDER"):
                self.client.log.critical(
                    "%s: post-give-up verification for order_id=%s "
                    "succeeded (status=%s) — this order is confirmed "
                    "gone. No further action needed.",
                    label,
                    order_id,
                    status,
                )
            else:
                self.client.log.critical(
                    "%s: post-give-up verification for order_id=%s "
                    "returned status=%s — still not a confirmed "
                    "cancellation. This was the final automatic check; "
                    "VERIFY MANUALLY on QFEX whether this order is still "
                    "resting.",
                    label,
                    order_id,
                    status,
                )
        except Exception:
            self.client.log.exception(
                "%s: post-give-up verification for order_id=%s also "
                "failed — this was the final automatic check; VERIFY "
                "MANUALLY on QFEX whether this order is still resting.",
                label,
                order_id,
            )


async def drain_until_flat(
    client: Any,
    strat: "PerpetualMarketMakingStrategy",
    symbol: str,
    stop_cb: Callable[[], Any],
    poll_s: float = 0.5,
    confirm_s: float = 1.0,
    open_orders_grace_s: float = 30.0,
) -> None:
    """
    Graceful "stop when flat" (added 2026-10-14).

    1. strat.draining = True: no new quotes from now on; an open position is
       still managed (BBO close + native stop) by the normal tick logic.
    2. Wait until the exchange shows the position flat.
    3. Cancel any resting quotes, wait confirm_s, and re-check that the
       position is still flat (a quote could have filled while its cancel
       was in flight -- if so, go back to step 2 and let the tick logic
       manage that position).
    4. Stop once the exchange also reports open_orders == 0 (or, after
       open_orders_grace_s of being flat with nothing tracked locally, stop
       anyway -- position_response's open_orders can lag).
    """
    log = client.log
    strat.draining = True
    log.warning("DRAIN requested for %s: no new quotes; will exit once flat", symbol)
    last_note = 0.0
    flat_since: Optional[float] = None
    while True:
        await asyncio.sleep(poll_s)
        pos = client.get_position(symbol)
        if pos.position != 0:
            flat_since = None
            now = time.monotonic()
            if now - last_note >= 60.0:
                last_note = now
                log.warning("DRAIN waiting: %s position=%s -- exits still managed", symbol, pos.position)
            continue
        await strat.drain_cancel_quotes()
        if strat.bid_order_id is not None or strat.ask_order_id is not None:
            continue
        await asyncio.sleep(confirm_s)
        pos = client.get_position(symbol)
        if pos.position != 0:
            log.warning("DRAIN: a quote filled during cancel -- %s position=%s; managing it first", symbol, pos.position)
            flat_since = None
            continue
        if flat_since is None:
            flat_since = time.monotonic()
        if pos.open_orders != 0 and time.monotonic() - flat_since < open_orders_grace_s:
            continue
        if pos.open_orders != 0:
            log.warning("DRAIN: exchange still reports open_orders=%s after %ss flat -- stopping anyway", pos.open_orders, open_orders_grace_s)
        log.warning("DRAIN complete: %s flat, no quotes resting -- stopping", symbol)
        res = stop_cb()
        if asyncio.iscoroutine(res):
            await res
        return
