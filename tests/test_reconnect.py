"""Tests for the reconnect re-verify fix (2026-10-17).

Run from the repo root:  PYTHONPATH=src:tests python tests/test_reconnect.py
"""
import asyncio
from decimal import Decimal

from qfex2.base_client import QFEXTradeError
from qfex2.models import BBO, PositionState
from test_orphan_fix import make_strategy

SYM = "GOLD-USD"


async def make(pos="0.05", cancel_outcome="NO_SUCH_ORDER"):
    s, c = make_strategy()
    object.__setattr__(s.cfg, "stop_loss_spread", Decimal("0.0008"))
    object.__setattr__(s.cfg, "take_profit_spread", Decimal("0.0008"))
    c.positions[SYM] = PositionState(symbol=SYM, position=Decimal(pos), average_price=Decimal("4163.2"))
    s._quantize_to_tick = lambda p, sym, round_up: p
    s._mark_price_makes_trigger_unplaceable = lambda *a: False
    s.stop_loss_order_id, s.take_profit_order_id = "old-SL", "old-TP"
    s.stop_loss_trigger_price, s.take_profit_trigger_price = Decimal("4159.9"), Decimal("4166.5")
    s._stop_orders_qty, s._stop_orders_avg_price = Decimal(pos), Decimal("4163.2")
    c.stop_cancels, c.placed = [], []
    async def fake_cancel_stop(order_id):
        c.stop_cancels.append(order_id)
        if cancel_outcome == "timeout":
            raise asyncio.TimeoutError()
        if cancel_outcome == "servererror":
            raise QFEXTradeError({"error_code": "ServerError", "message": "unexpected error"})
        return {"final_order_response": {"status": cancel_outcome}}
    c.cancel_stop_order = fake_cancel_stop
    def mk(label):
        async def send(symbol, side, quantity, trigger_price, client_order_id=None):
            c.placed.append((label, trigger_price))
            return {"final_order_response": {"status": "ACK", "order_id": "new-" + label}}
        return send
    c.send_stop_loss, c.send_take_profit = mk("SL"), mk("TP")
    async def no_market(**k):
        raise AssertionError("no market order expected")
    c.send_market = no_market
    return s, c


QUIET = BBO(symbol=SYM, bid_px=Decimal("4163.7"), bid_qty=Decimal("1"), ask_px=Decimal("4165.0"), ask_qty=Decimal("1"))


async def check_replaced(outcome):
    s, c = await make(cancel_outcome=outcome)
    await s.on_trade_reconnected()
    assert c.stop_cancels == ["old-SL", "old-TP"], c.stop_cancels
    assert s.stop_loss_order_id is None and s.take_profit_order_id is None
    await s._tick(QUIET)                                   # position still open -> re-place both
    assert sorted(l for l, _ in c.placed) == ["SL", "TP"], c.placed
    assert s.stop_loss_order_id == "new-SL" and s.take_profit_order_id == "new-TP"


async def test_gone_stops_are_replaced():
    await check_replaced("NO_SUCH_ORDER")


async def test_live_stops_are_cancelled_then_replaced():
    await check_replaced("CANCELLED")


async def test_servererror_counts_as_gone():
    await check_replaced("servererror")                    # QFEX's ServerError still cancels


async def test_timeout_keeps_tracking_and_does_not_duplicate():
    s, c = await make(cancel_outcome="timeout")
    await s.on_trade_reconnected()
    assert s.stop_loss_order_id == "old-SL" and s.take_profit_order_id == "old-TP"
    await s._tick(QUIET)
    assert c.placed == []


async def test_nothing_tracked_nothing_sent():
    s, c = await make(pos="0")
    s.stop_loss_order_id = s.take_profit_order_id = None
    await s.on_trade_reconnected()
    assert c.stop_cancels == []


async def test_stale_quotes_cleared_when_flat():
    s, c = await make(pos="0")
    s.stop_loss_order_id = s.take_profit_order_id = None
    s.bid_order_id, s.ask_order_id = "b", "a"
    calls = []
    async def fake_cancel_order(order_id, symbol):
        calls.append(order_id)
        return {"final_order_response": {"status": "NO_SUCH_ORDER"}}
    c.cancel_order = fake_cancel_order
    await s.on_trade_reconnected()
    assert sorted(calls) == ["a", "b"] and s.bid_order_id is None and s.ask_order_id is None


async def test_hook_only_fires_from_second_session():
    s, c = make_strategy()
    fired = []
    async def hook(): fired.append(1)
    s.on_trade_reconnected = hook
    c._on_trade_session_ready(); await asyncio.sleep(0.01)
    assert fired == []                                     # first connect: nothing to re-verify
    c._on_trade_session_ready(); await asyncio.sleep(0.01)
    assert fired == [1]


async def test_hook_waits_for_running_tick():
    s, c = await make(cancel_outcome="NO_SUCH_ORDER")
    order = []
    async with s._reentrant_lock:
        t = asyncio.create_task(s.on_trade_reconnected())
        await asyncio.sleep(0.05)
        order.append(("tick_done", list(c.stop_cancels)))
    await t
    assert order == [("tick_done", [])] and c.stop_cancels == ["old-SL", "old-TP"]


if __name__ == "__main__":
    for k, v in dict(globals()).items():
        if k.startswith("test_"):
            asyncio.run(v()); print("PASS", k)
