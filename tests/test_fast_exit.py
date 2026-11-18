"""Tests for the fast-exit changes (2026-10-14): BBO conflation, exits before quote cancels,
no confirmation streak without an exchange stop, concurrent stop legs.

Run from the repo root:  PYTHONPATH=src:tests python tests/test_fast_exit.py
"""
import asyncio
import json
import time
from decimal import Decimal

from qfex2.models import BBO, PositionState
from test_orphan_fix import make_strategy

SYM = "GOLD-USD"


def bbo(bid, ask):
    return BBO(symbol=SYM, bid_px=Decimal(str(bid)), bid_qty=Decimal("1"), ask_px=Decimal(str(ask)), ask_qty=Decimal("1"))


async def make(pos="0.01", streak=5, cancel_delay=0.0, stop_status="ACK", stop_delay=0.0):
    s, c = make_strategy()
    object.__setattr__(s.cfg, "bbo_stop_loss_confirm_streak", streak)
    object.__setattr__(s.cfg, "stop_loss_spread", Decimal("0.0008"))
    object.__setattr__(s.cfg, "take_profit_spread", Decimal("0.0008"))
    c.positions[SYM] = PositionState(symbol=SYM, position=Decimal(pos), average_price=Decimal("4000"))
    s._quantize_to_tick = lambda p, sym, round_up: p
    s._mark_price_makes_trigger_unplaceable = lambda *a: False
    c.t0 = time.monotonic()
    c.events = []
    async def fake_cancel_side(side):
        c.events.append(("cancel_start", side.value))
        await asyncio.sleep(cancel_delay)
        if side.value == "BUY": s.bid_order_id = None
        else: s.ask_order_id = None
        c.events.append(("cancel_done", side.value, round(time.monotonic() - c.t0, 2)))
    s._cancel_side = fake_cancel_side
    async def fake_market(symbol, side, quantity, reduce_only=None, client_order_id=None):
        c.events.append(("market", round(time.monotonic() - c.t0, 2)))
        c.positions[SYM].position = Decimal("0")
        return {"final_order_response": {"status": "FILLED", "price": 3990.0}}
    c.send_market = fake_market
    def mk(label):
        async def send(symbol, side, quantity, trigger_price, client_order_id=None):
            c.events.append((label + "_start", round(time.monotonic() - c.t0, 2)))
            await asyncio.sleep(stop_delay)
            st = stop_status if label == "SL" else "ACK"
            return {"final_order_response": {"status": st, "order_id": label + "-id"}}
        return send
    c.send_stop_loss = mk("SL")
    c.send_take_profit = mk("TP")
    return s, c


def t_of(c, name):
    return [e[-1] for e in c.events if e[0] == name]


async def test_reader_not_blocked_and_conflated():
    s, c = make_strategy()
    got = []
    async def slow_on_bbo(b):
        got.append(b.bid_px); await asyncio.sleep(0.3)
    s.on_bbo = slow_on_bbo
    t = time.monotonic()
    for i in range(5):
        msg = {"type": "bbo", "symbol": SYM, "sequence": i + 1, "time": "2026-10-14T12:30:00Z",
               "bid": [[str(4000 + i), "1"]], "ask": [[str(4001 + i), "1"]]}
        await c._handle_mds_message(json.dumps(msg))
    assert time.monotonic() - t < 0.05                       # reader never waited on the strategy
    await asyncio.sleep(0.8)
    assert got == [Decimal("4004")], got                      # only the newest snapshot is delivered


async def test_exit_not_blocked_by_slow_quote_cancels():
    # replay of 19:30: quote cancels take 3 s; price already through the stop
    s, c = await make(cancel_delay=3.0, streak=5)
    s.bid_order_id = "b"; s.ask_order_id = "a"
    await s._tick(bbo(3990, 3991))                           # stop at 3996.8; no exchange stop yet
    m = t_of(c, "market")
    assert m and m[0] < 0.2, c.events                         # closed at once, not after 6 s of cancels
    await asyncio.sleep(3.2)                                  # background cancels still complete
    assert s.bid_order_id is None and s.ask_order_id is None


async def test_quote_cancels_run_concurrently():
    s, c = await make(pos="0", cancel_delay=0.3)
    s.bid_order_id = "b"; s.ask_order_id = "a"
    t = time.monotonic()
    await s._cancel_quotes_if_any()
    assert time.monotonic() - t < 0.5                         # 0.3 s, not 0.6 s


async def test_streak_kept_when_exchange_stop_is_resting():
    s, c = await make(streak=5)
    s.stop_loss_order_id = "SL-id"; s.take_profit_order_id = "TP-id"
    s._stop_orders_qty = Decimal("0.01"); s._stop_orders_avg_price = Decimal("4000")
    # what an ACK records per leg (2026-10-27); without it the legs look stale and get renewed
    s._stop_leg_placed_for = {"stop_loss_order_id": (Decimal("0.01"), Decimal("4000")),
                              "take_profit_order_id": (Decimal("0.01"), Decimal("4000"))}
    await s._tick(bbo(3995, 3996))
    assert t_of(c, "market") == []                            # wick filter still applies
    for _ in range(4):
        await s._tick(bbo(3995, 3996))
    assert len(t_of(c, "market")) == 1                        # fires on the 5th crossing tick


async def test_no_streak_when_exchange_stop_rejected():
    s, c = await make(streak=5, stop_status="INVALID_PRICE")
    await s._tick(bbo(4000, 4000.5))                          # not crossed: places stops, SL rejected
    assert s.stop_loss_order_id is None and s.take_profit_order_id == "TP-id"
    await s._tick(bbo(3995, 3996))                            # first crossing tick
    assert len(t_of(c, "market")) == 1, c.events


async def test_stop_legs_placed_concurrently():
    s, c = await make(stop_delay=0.4)
    t = time.monotonic()
    await s._tick(bbo(4000, 4000.5))
    assert time.monotonic() - t < 0.6, time.monotonic() - t   # 0.4 s, not 0.8 s
    assert s.stop_loss_order_id == "SL-id" and s.take_profit_order_id == "TP-id"


async def test_flat_waits_for_background_cancel_before_quoting():
    s, c = await make(cancel_delay=0.3)
    s.bid_order_id = "b"; s.ask_order_id = "a"
    s._cancel_quotes_in_background()
    c.positions[SYM].position = Decimal("0")
    quoted = []
    async def q(b): quoted.append((s.bid_order_id, s.ask_order_id))
    s._quote = q
    await s._tick(bbo(4000, 4000.5))
    assert quoted == [(None, None)], quoted                   # quoting only after the cancels finished


async def test_dispatch_delivers_latest_after_a_slow_tick():
    s, c = make_strategy()
    got = []
    async def slow_on_bbo(b):
        got.append(b.bid_px); await asyncio.sleep(0.3)
    s.on_bbo = slow_on_bbo
    def msg(i):
        return json.dumps({"type": "bbo", "symbol": SYM, "sequence": i + 1, "time": "2026-10-14T12:30:00Z",
                           "bid": [[str(4000 + i), "1"]], "ask": [[str(4001 + i), "1"]]})
    await c._handle_mds_message(msg(0))
    await asyncio.sleep(0.05)                                 # tick 0 now running
    for i in range(1, 5):
        await c._handle_mds_message(msg(i))
    await asyncio.sleep(0.8)
    assert got == [Decimal("4000"), Decimal("4004")], got     # skips the stale 4001-4003


if __name__ == "__main__":
    for k, v in dict(globals()).items():
        if k.startswith("test_"):
            asyncio.run(v()); print("PASS", k)
