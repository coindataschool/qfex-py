"""Tests for the fill fence (2026-11-22): after one of our quotes fills, place no
quotes until the exchange's position_response reflects the fill.

Live incident replayed in test_ask_fill_then_stale_refresh_does_not_double:
US500 Nov 18 22:36:31 — ASK filled at .651, the tick already in progress got
NO_SUCH_ORDER for its cancel at .752, re-placed the ASK at .861, it filled at
.871 and the position became -0.14 (order_amount 0.07).

Run from the repo root:  PYTHONPATH=src:tests python tests/test_fill_fence.py
"""
import asyncio
import time
from decimal import Decimal

from qfex2.models import BBO, PositionState
from qfex2.trader_client import Side
from test_orphan_fix import make_strategy

SYM = "GOLD-USD"


def setup(cancel_status="CANCELLED"):
    s, c = make_strategy()
    c.positions[SYM] = PositionState(symbol=SYM, position=Decimal("0"), average_price=Decimal("0"))
    s._quantize_to_tick = lambda p, sym, round_up: p
    s.placed, s.cancelled = [], []
    n = [0]
    async def fake_alo(symbol, side, quantity, price):
        n[0] += 1
        s.placed.append((side, price))
        return {"final_order_response": {"status": "ACK", "order_id": f"new-{n[0]}", "price": float(price),
                                         "quantity": float(quantity)}}
    c.send_gtc_alo = fake_alo
    async def fake_cancel(order_id, symbol):
        s.cancelled.append(order_id)
        st = cancel_status(order_id) if callable(cancel_status) else cancel_status
        return {"final_order_response": {"status": st}}
    c.cancel_order = fake_cancel
    s.bid_order_id, s.bid_price = "bid-1", Decimal("3999")
    s.ask_order_id, s.ask_price = "ask-1", Decimal("4001")
    return s, c


def fill(oid, side, qty, rem):
    return {"order_response": {"order_id": oid, "side": side, "quantity_remaining": rem, "status":
                               "FILLED" if rem == 0 else "PARTIALLY_FILLED", "type": "ALO", "symbol": SYM,
                               "price": 4001.0, "trade_id": "t1"},
            "filled_qty_delta": qty, "order_id": oid, "symbol": SYM}


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


async def test_fill_arms_fence_and_blocks_quoting():
    s, c = setup()
    await s.on_fill(fill("ask-1", "SELL", 0.01, 0.0))
    assert s._fill_fence_pos == Decimal("-0.01") and s.ask_order_id is None
    assert s._fill_fence_active()
    await s._refresh_quote_if_stale(Side.SELL, Decimal("4002"), "ask_order_id", "ask_price")
    assert s.placed == []                                    # the filled side is not re-placed
    bbo = BBO(symbol=SYM, bid_px=Decimal("4000"), bid_qty=Decimal("1"), ask_px=Decimal("4000.1"), ask_qty=Decimal("1"))
    await s._tick(bbo)                                       # flat per exchange, but fenced
    await settle()
    assert s.placed == [] and s.cancelled == ["bid-1"]       # other quote pulled, nothing placed
    c.positions[SYM].position = Decimal("-0.01")             # position_response arrives
    assert not s._fill_fence_active() and s._fill_fence_pos is None


async def test_ask_fill_then_stale_refresh_does_not_double():
    # the tick in progress: cancel of the (already filled) ask returns NO_SUCH_ORDER
    s, c = setup(cancel_status=lambda oid: "NO_SUCH_ORDER" if oid == "ask-1" else "CANCELLED")
    await s.on_fill(fill("ask-1", "SELL", 0.01, 0.0))
    s.ask_order_id, s.ask_price = "ask-1", Decimal("4001")   # worst case: tick still holds the old id
    await s._refresh_quote_if_stale(Side.SELL, Decimal("4003"), "ask_order_id", "ask_price")
    assert s.placed == [], s.placed


async def test_no_such_order_alone_skips_replace_this_tick():
    s, c = setup(cancel_status="NO_SUCH_ORDER")              # no fill seen (yet)
    await s._refresh_quote_if_stale(Side.BUY, Decimal("3990"), "bid_order_id", "bid_price")
    assert s.placed == [] and s.bid_order_id is None         # next tick decides afresh
    await s._refresh_quote_if_stale(Side.BUY, Decimal("3990"), "bid_order_id", "bid_price")
    assert s.placed == [(Side.BUY, Decimal("3990"))]


async def test_normal_stale_refresh_still_replaces():
    s, c = setup("CANCELLED")
    await s._refresh_quote_if_stale(Side.BUY, Decimal("3990"), "bid_order_id", "bid_price")
    assert s.cancelled == ["bid-1"] and s.placed == [(Side.BUY, Decimal("3990"))] and s.bid_order_id == "new-1"


async def test_partial_fills_accumulate_and_keep_order():
    s, c = setup()
    await s.on_fill(fill("bid-1", "BUY", 0.004, 0.006))
    assert s.bid_order_id == "bid-1" and s._fill_fence_pos == Decimal("0.004")
    await s.on_fill(fill("bid-1", "BUY", 0.006, 0.0))
    assert s.bid_order_id is None and s._fill_fence_pos == Decimal("0.010")
    c.positions[SYM].position = Decimal("0.004")             # first update only: still fenced
    assert s._fill_fence_active()
    c.positions[SYM].position = Decimal("0.010")
    assert not s._fill_fence_active()


async def test_fence_expires_as_fallback():
    s, c = setup()
    s.FILL_FENCE_S = 0.05
    await s.on_fill(fill("ask-1", "SELL", 0.01, 0.0))
    assert s._fill_fence_active()
    await asyncio.sleep(0.06)
    assert not s._fill_fence_active()                        # never confirmed -> normal logic resumes


async def test_other_fills_do_not_arm_fence():
    s, c = setup()
    await s.on_fill(fill("market-exit-7", "BUY", 0.01, 0.0))
    assert s._fill_fence_pos is None and s.bid_order_id == "bid-1" and s.ask_order_id == "ask-1"


if __name__ == "__main__":
    for k, v in dict(globals()).items():
        if k.startswith("test_"):
            asyncio.run(v()); print("PASS", k)
