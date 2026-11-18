"""
QFEXTraderClient

The single client class for order placement/cancellation on QFEX — ALO
(post-only resting), IOC/MARKET (immediate), and STOP_LOSS/TAKE_PROFIT
(native exchange-triggered exits), all on one class, one connection, one
account/position/balance view.

Previously split across three classes (QFEXMakerClient, QFEXTakerClient,
QFEXTraderClient) composed from separate mixins, on the theory that a
narrowly-typed client would stop a maker-only or taker-only strategy from
accidentally reaching for a method it didn't need. Consolidated into one
class because that theory didn't hold in practice: real strategies
generally need more than one order-entry style (e.g.
PerpetualMarketMakingStrategy needs ALO for quoting, STOP_LOSS/
TAKE_PROFIT for routine position protection, and MARKET for its
mark-price circuit breaker), and the mixin split was itself a source of
a real bug — QFEXTraderClient was switched to for one method
(send_market) without checking it still composed _StopOrderMethods,
which it didn't; every stop-loss/take-profit placement crashed with
AttributeError for the length of an open position before that was
caught. One always-complete class removes that failure mode entirely.

_handle_order_response (base_client.py) resolves each in-flight order's
Future using a terminal-status set registered per-order at send time,
not one hardcoded rule for the whole class — this is what lets ALO
(waiting on ACK), MARKET (waiting on fill/reject), and STOP_LOSS/
TAKE_PROFIT (also waiting on ACK, but tracked/cancelled via a separate
registry — see cancel_stop_order below) all be in flight simultaneously
on one connection without interfering with each other.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Optional

from .base_client import _QFEXBaseClient, _OrderEntryHost
from .config import QFEXConfig
from .models import OrderType, Side, TimeInForce
from .strategy import TraderStrategy

_ALO_REJECT_STATUSES = (
    "REJECTED",
    "NO_SUCH_ORDER",
    "INVALID_ORDER_TYPE",
    "BAD_SYMBOL",
    "FAILED_MARGIN_CHECK",
    "PRICE_LESS_THAN_MIN_PRICE",
    "PRICE_GREATER_THAN_MAX_PRICE",
    "INVALID_TICK_SIZE_PRECISION_PRICE",
    "INVALID_TICK_SIZE_PRECISION_QUANTITY",
    "QUANTITY_LESS_THAN_MIN_QUANTITY",
    "QUANTITY_GREATER_THAN_MAX_QUANTITY",
    "INVALID_TIME_IN_FORCE",
    "REJECTED_WOULD_BREACH_MAX_NOTIONAL",
    "REJECTED_MARKET_CLOSED",
    "REJECTED_FAILED_TO_PROCESS",
    "RATE_LIMITED",
    "REJECTED_TOO_MANY_OPEN_ORDERS",
    "REJECTED_OPEN_INTEREST_LIMIT",
)
# ACK (accepted, resting on book) is terminal for an ALO placement wait —
# unlike IOC/MARKET, where ACK is just a waypoint before the real
# fill/reject outcome, or STOP_LOSS/TAKE_PROFIT, which also resolve on
# ACK but are tracked/cancelled through a separate mechanism (see
# cancel_stop_order).
_ALO_TERMINAL_STATUSES = ("ACK",) + _ALO_REJECT_STATUSES

# IOC/MARKET orders resolve immediately (fill-or-die, or fill-or-reject
# for MARKET) by definition — never wait for ACK the way a resting ALO
# order does. FILLED-with-zero-remaining is handled separately,
# unconditionally, in _QFEXBaseClient._handle_order_response.
_FILL_OR_REJECT_TERMINAL_STATUSES = (
    "CANCELLED",
    "REJECTED",
    "NO_SUCH_ORDER",
    "INVALID_ORDER_TYPE",
    "BAD_SYMBOL",
    "FAILED_MARGIN_CHECK",
    # Added 2026-10-13. Confirmed live (CL-USD 2026-10-12, US500-USD
    # 2026-10-13): a reduce_only MARKET close sent when the position is
    # already flat (native stop fired first, or a second close right after
    # the first one filled) comes back INVALID_QUANTITY. It was missing
    # here, so the caller waited out the full timeout and logged a
    # misleading "timed out waiting for ACK/REJECT" ERROR. The rest are the
    # generic add_order rejection codes, added for the same reason.
    "INVALID_QUANTITY",
    "QUANTITY_LESS_THAN_MIN_QUANTITY",
    "QUANTITY_GREATER_THAN_MAX_QUANTITY",
    "INVALID_TICK_SIZE_PRECISION_QUANTITY",
    "INVALID_TIME_IN_FORCE",
    "REJECTED_WOULD_BREACH_MAX_NOTIONAL",
    "REJECTED_MARKET_CLOSED",
    "REJECTED_FAILED_TO_PROCESS",
    "RATE_LIMITED",
    "REJECTED_OPEN_INTEREST_LIMIT",
)

# ACK (accepted, resting and waiting for the mark-price trigger) is
# terminal for a stop order placement wait — same shape as ALO's wait,
# but defined independently since TAKE_PROFIT/STOP_LOSS are a distinct
# QFEX order family from ALO. Reject statuses here are the same generic
# add_order rejection codes ALO can hit, plus stop-specific ones from
# QFEX's OrderStatus enum (docs.qfex.com/api-reference/enums#orderstatus).
_STOP_REJECT_STATUSES = (
    "REJECTED",
    "NO_SUCH_ORDER",
    "INVALID_ORDER_TYPE",
    "BAD_SYMBOL",
    "FAILED_MARGIN_CHECK",
    "PRICE_LESS_THAN_MIN_PRICE",
    "PRICE_GREATER_THAN_MAX_PRICE",
    "INVALID_TICK_SIZE_PRECISION_PRICE",
    "INVALID_TICK_SIZE_PRECISION_QUANTITY",
    "QUANTITY_LESS_THAN_MIN_QUANTITY",
    "QUANTITY_GREATER_THAN_MAX_QUANTITY",
    "INVALID_TIME_IN_FORCE",
    "REJECTED_WOULD_BREACH_MAX_NOTIONAL",
    "REJECTED_MARKET_CLOSED",
    "REJECTED_FAILED_TO_PROCESS",
    "RATE_LIMITED",
    "REJECTED_TOO_MANY_OPEN_ORDERS",
    "REJECTED_OPEN_INTEREST_LIMIT",
    "INVALID_TAKE_PROFIT_PRICE",
    "INVALID_STOP_LOSS_PRICE",
    # Confirmed live: QFEX rejects a STOP_LOSS/TAKE_PROFIT trigger price
    # with this plain status — not INVALID_STOP_LOSS_PRICE/
    # INVALID_TAKE_PROFIT_PRICE above — when the trigger doesn't make
    # sense relative to current mark price (e.g. a short's stop-loss
    # trigger computed below a mark price that's stuck/stale above
    # entry). Before this was added, INVALID_PRICE wasn't a registered
    # terminal status, so _handle_order_response never resolved the
    # pending future for it — the caller just timed out after the full
    # order_update_timeout_s instead of seeing an immediate, correctly
    # attributed rejection.
    "INVALID_PRICE",
    # Confirmed live (2026-07-28, CL-USD): QFEX rejects a STOP_LOSS/
    # TAKE_PROFIT placement with this status when the requested quantity
    # no longer matches the live position by the time the request is
    # processed — observed specifically in the narrow window right after
    # a phantom-FILLED stop order closes the position: _tick can fire on
    # a BBO tick that lands before this client's own position_response
    # for that fill has arrived, so _manage_position computes an
    # exit_qty against a position that's already flat on QFEX's side.
    # Same class of gap as INVALID_PRICE above (a rejection this client
    # didn't yet know to treat as terminal) — before this was added, the
    # placement just timed out after the full order_update_timeout_s
    # instead of failing fast with a correctly attributed rejection. The
    # underlying race itself (a position update arriving slightly after
    # the fill that caused it) isn't fixed by this — this only makes the
    # client recognize and report the resulting rejection immediately
    # rather than waiting it out; the next tick's fresh position read
    # already recovers correctly on its own regardless.
    "INVALID_QUANTITY",
)
_STOP_TERMINAL_STATUSES = ("ACK",) + _STOP_REJECT_STATUSES


class QFEXTraderClient(_QFEXBaseClient):
    def __init__(self, cfg: QFEXConfig, strategy: TraderStrategy):
        super().__init__(cfg, strategy, log_name="qfex.trader")

    # ---------- ALO (post-only resting orders) ----------

    async def send_gtc_alo(
        self: _OrderEntryHost,
        symbol: str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends a resting, post-only (ALO / "Add Liquidity Only") order via the
        trade websocket add_order call, with order_time_in_force=GTC.

        Resolves as soon as the order reaches ACK (accepted, resting on the
        book) or is REJECTED for any reason. Does NOT wait for a fill —
        an ALO/GTC order can rest indefinitely, so fills should be consumed
        via strategy.on_fill(), same as with any other order flow.

        Returns the order_response dict for the ACK/REJECT event. On success
        (ACK) this dict's "order_id" is what you'll need to later call
        cancel_order().
        """
        return await self._send_order_and_wait(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.ALO.value,
            time_in_force=TimeInForce.GTC.value,
            price=price,
            terminal_statuses=_ALO_TERMINAL_STATUSES,
            client_order_id=client_order_id,
        )

    async def cancel_order(
        self: _OrderEntryHost, order_id: str, symbol: str
    ) -> Dict[str, Any]:
        """
        Cancels a single resting order by its exchange-assigned order_id
        (the value returned as "order_id" in the ACK order_response from
        send_gtc_alo()).

        Resolves on CANCELLED / CANCELLED_STP (success), or on
        NO_SUCH_ORDER / REJECTED (failure — order may have already filled,
        been cancelled, or never existed). Raises asyncio.TimeoutError if no
        order_response arrives within cfg.order_update_timeout_s.
        """
        return await self._send_cancel_and_wait(order_id, symbol)

    # ---------- IOC / MARKET (immediate orders) ----------

    async def send_ioc_limit(
        self: _OrderEntryHost,
        symbol: str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends a LIMIT IOC order via trade websocket add_order.
        Returns a dict describing the final outcome we observed for this client_order_id
        (ACK/FILLED/CANCELLED/REJECTED, etc.).
        """
        return await self._send_order_and_wait(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.LIMIT.value,
            time_in_force=TimeInForce.IOC.value,
            price=price,
            terminal_statuses=_FILL_OR_REJECT_TERMINAL_STATUSES,
            client_order_id=client_order_id,
        )

    async def send_market(
        self: _OrderEntryHost,
        symbol: str,
        side: Side,
        quantity: Decimal,
        client_order_id: Optional[str] = None,
        reduce_only: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Sends a MARKET order — fills immediately against available
        liquidity at whatever price the book offers.

        `price` is sent as 0 and `order_time_in_force` as IOC — both
        fields are required in the JSON payload despite QFEX's docs
        implying price is optional for MARKET orders; the value of
        `price` is genuinely ignored, but the field must be present.

        `reduce_only`, when True, tells QFEX this order may only shrink
        or close an existing position — never open a new one or flip its
        sign. Defaults to None, meaning the field is omitted from the
        payload entirely — same as every existing caller's behavior
        before this parameter existed, and deliberately not defaulted to
        an explicit False, since this codebase hasn't confirmed whether
        QFEX treats an explicit reduce_only=false differently from the
        field being absent. Use True for a force-close (e.g. a circuit
        breaker bailing out of a position when its protective stop orders
        can no longer be trusted) where opening or flipping the position
        instead of closing it would be exactly the wrong failure mode.

        Resolves the same way as send_ioc_limit() — fill or reject,
        immediately, no ACK-based wait.

        Returns a dict describing the final outcome for this
        client_order_id (FILLED/REJECTED/etc.).
        """
        return await self._send_order_and_wait(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.MARKET.value,
            time_in_force=TimeInForce.IOC.value,
            price=Decimal("0"),
            terminal_statuses=_FILL_OR_REJECT_TERMINAL_STATUSES,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
        )

    # ---------- STOP_LOSS / TAKE_PROFIT (native triggered exits) ----------

    async def send_stop_loss(
        self: _OrderEntryHost,
        symbol: str,
        side: Side,
        quantity: Decimal,
        trigger_price: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Places a STOP_LOSS order: rests, invisible to the book, until
        QFEX's mark price for `symbol` crosses `trigger_price`, then
        fires an immediate MARKET order in `side` direction for
        `quantity`. The trigger guarantees the order fires, not the fill
        price — slippage between trigger and fill is expected.

        `side` is the direction of the resulting exit order, not the
        position being protected — e.g. protecting a long means passing
        Side.SELL.

        Resolves on ACK (resting, armed) or REJECTED — does not wait for
        the trigger or fill. Consume the eventual fill via
        strategy.on_fill(). Use cancel_stop_order() with the returned
        "order_id" to cancel before it triggers.

        order_time_in_force is sent as GTC, though QFEX ignores it for
        this order_type — required in the payload regardless.
        """
        return await self._send_order_and_wait(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.STOP_LOSS.value,
            time_in_force=TimeInForce.GTC.value,
            price=trigger_price,
            terminal_statuses=_STOP_TERMINAL_STATUSES,
            client_order_id=client_order_id,
        )

    async def send_take_profit(
        self: _OrderEntryHost,
        symbol: str,
        side: Side,
        quantity: Decimal,
        trigger_price: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Places a TAKE_PROFIT order via add_order. Identical mechanics to
        send_stop_loss() above — same ACK-based wait, same eventual
        conversion to a MARKET order once QFEX's mark price for `symbol`
        crosses `trigger_price`, same slippage caveat. TAKE_PROFIT and
        STOP_LOSS are separate order_type values on QFEX's side
        (distinguishing intent for the exchange's own bookkeeping/
        reporting), but placement and the ACK/REJECT wait are identical
        between them — see cancel_stop_order() below for the one real
        difference (cancellation uses a different registry from regular
        orders, not a difference between TAKE_PROFIT and STOP_LOSS
        themselves).
        """
        return await self._send_order_and_wait(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.TAKE_PROFIT.value,
            time_in_force=TimeInForce.GTC.value,
            price=trigger_price,
            terminal_statuses=_STOP_TERMINAL_STATUSES,
            client_order_id=client_order_id,
        )

    async def cancel_stop_order(
        self: _OrderEntryHost, stop_order_id: str
    ) -> Dict[str, Any]:
        """
        Cancels a single resting stop order (STOP_LOSS or TAKE_PROFIT) by
        its exchange-assigned id (returned as "order_id" in the ACK from
        send_stop_loss()/send_take_profit()).

        Not the same call as cancel_order() — QFEX tracks stop orders in
        a separate registry, and cancel_order() rejects a stop order's id
        with NO_SUCH_ORDER. This sends {"type": "cancel_stop_order",
        "params": {"stop_order_id": ...}} instead — no `symbol` needed,
        unlike cancel_order().

        Resolves on CANCELLED or a reject-family status, via a normal
        order_response envelope. Raises asyncio.TimeoutError if no
        response arrives within cfg.order_update_timeout_s.
        """
        return await self._send_stop_cancel_and_wait(stop_order_id)
