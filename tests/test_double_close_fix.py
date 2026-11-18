"""Tests for the 2026-10-13 double-close fix.

Run from the repo root:  PYTHONPATH=src python tests/test_double_close_fix.py
"""
import asyncio
import time
from decimal import Decimal

from qfex2.models import BBO, PositionState
from qfex2.trader_client import _FILL_OR_REJECT_TERMINAL_STATUSES, Side
from test_orphan_fix import make_client, make_strategy

SYM = "GOLD-USD"


def setup(market_status="FILLED", pos="0.01"):
    s, c = make_strategy()
    c.positions[SYM] = PositionState(symbol=SYM, position=Decimal(pos), average_price=Decimal("4000"))
    s._quantize_to_tick = lambda p, sym, round_up: p          # no refdata in tests
    s.stops_placed = []
    async def fake_place(*a, **k): s.stops_placed.append(a)
    s._place_stop_orders = fake_place
    async def fake_market(symbol, side, quantity, reduce_only=None, client_order_id=None):
        c.markets.append((side, quantity, reduce_only))
        return {"final_order_response": {"status": market_status, "price": 4002.0}}
    c.send_market = fake_market
    async def no_quote(bbo): s.quoted = True
    s._quote = no_quote
    return s, c


TP_BBO = BBO(symbol=SYM, bid_px=Decimal("4100"), bid_qty=Decimal("1"), ask_px=Decimal("4100.1"), ask_qty=Decimal("1"))


async def test_market_invalid_quantity_resolves_immediately():
    assert "INVALID_QUANTITY" in _FILL_OR_REJECT_TERMINAL_STATUSES
    c = make_client()
    async def fake_enqueue(msg):
        p = msg["params"]
        asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(c._handle_order_response({
            "order_id": "x", "client_order_id": p["client_order_id"], "symbol": SYM,
            "status": "INVALID_QUANTITY", "type": "MARKET", "quantity": 0.02,
            "quantity_remaining": 0.0, "price": 0.0, "side": p["side"], "trade_id": None})))
    c._enqueue_trade = fake_enqueue
    t0 = time.monotonic()
    r = await c.send_market(symbol=SYM, side=Side.SELL, quantity=Decimal("0.02"), reduce_only=True)
    assert r["final_order_response"]["status"] == "INVALID_QUANTITY"
    assert time.monotonic() - t0 < 0.5      # no 5 s timeout any more


async def test_no_second_close_before_position_update():
    s, c = setup("FILLED")
    await s._tick(TP_BBO)
    await s._tick(TP_BBO)                    # position_response not yet arrived
    await s._tick(TP_BBO)
    assert len(c.markets) == 1, c.markets
    assert s.stops_placed == []
    c.positions[SYM].position = Decimal("0")  # exchange confirms flat
    await s._tick(TP_BBO)
    assert len(c.markets) == 1 and s._exit_guard_pos is None


async def test_invalid_quantity_means_already_closed():
    s, c = setup("INVALID_QUANTITY")
    await s._tick(TP_BBO)
    await s._tick(TP_BBO)
    assert len(c.markets) == 1, c.markets     # no retry storm
    assert s.stops_placed == []               # no stops for a position that is gone


async def test_guard_expires_as_fallback():
    s, c = setup("FILLED")
    s.EXIT_GUARD_S = 0.0
    await s._tick(TP_BBO)
    await s._tick(TP_BBO)                     # guard expired, position still shows 0.01 -> retry allowed
    assert len(c.markets) == 2, c.markets


async def test_position_change_disarms_guard():
    s, c = setup("FILLED", pos="0.01")
    await s._tick(TP_BBO)
    c.positions[SYM].position = Decimal("0.004")  # different size -> guard off, manage normally
    await s._tick(TP_BBO)
    assert len(c.markets) == 2, c.markets


async def test_no_trigger_no_close():
    s, c = setup("FILLED")
    quiet = BBO(symbol=SYM, bid_px=Decimal("4000.1"), bid_qty=Decimal("1"), ask_px=Decimal("4000.2"), ask_qty=Decimal("1"))
    await s._tick(quiet)
    assert c.markets == [] and len(s.stops_placed) == 1


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    for t in tests:
        asyncio.run(t()); print("PASS", t.__name__)
