"""Tests for the stop-leg resize fix (2026-10-27).

Replay of US500 2026-10-26 20:51: a BUY quote filled in two pieces (0.014, then 0.056).
The STOP_LOSS was placed for the first 0.014 while the TAKE_PROFIT was suppressed (mark
already past its trigger). Once the TP was placed for 0.07 the SL stayed at 0.014.

Run from the repo root:  PYTHONPATH=src:tests python tests/test_stop_resize.py
"""
import asyncio
from decimal import Decimal

from qfex2.models import BBO, PositionState
from test_orphan_fix import make_strategy

SYM = "GOLD-USD"   # the shared test strategy is built on GOLD-USD; the logic is symbol-agnostic


def bbo(bid, ask):
    return BBO(symbol=SYM, bid_px=Decimal(str(bid)), bid_qty=Decimal("1"), ask_px=Decimal(str(ask)), ask_qty=Decimal("1"))


def setup(tp_unplaceable):
    s, c = make_strategy()
    object.__setattr__(s.cfg, "stop_loss_spread", Decimal("0.0001"))
    object.__setattr__(s.cfg, "take_profit_spread", Decimal("0.0001"))
    object.__setattr__(s.cfg, "bbo_stop_loss_confirm_streak", 2)
    s._quantize_to_tick = lambda p, sym, round_up: p
    s._mark_price_makes_trigger_unplaceable = lambda label, side, trig: label == "TAKE_PROFIT" and tp_unplaceable[0]
    c.placed, c.cancelled, n = [], [], [0]
    def mk(label):
        async def send(symbol, side, quantity, trigger_price, client_order_id=None):
            n[0] += 1; oid = "%s-%d" % (label, n[0])
            c.placed.append((label, quantity, oid))
            return {"final_order_response": {"status": "ACK", "order_id": oid}}
        return send
    c.send_stop_loss = mk("SL"); c.send_take_profit = mk("TP")
    async def cancel_stop(order_id):
        c.cancelled.append(order_id)
        return {"final_order_response": {"status": "CANCELLED"}}
    c.cancel_stop_order = cancel_stop
    return s, c


def live_qty(c, label):
    """quantity of the leg of this type that is still resting (placed and not cancelled)"""
    alive = [q for (l, q, oid) in c.placed if l == label and oid not in c.cancelled]
    return alive[-1] if alive else None


B = bbo(7656.8, 7657.0)          # price sits between the stop (7656.13) and the take-profit (7657.67)
AVG = Decimal("7656.9")


async def test_replay_2051_sl_resized_after_tp_placed():
    tp_blocked = [True]
    s, c = setup(tp_blocked)
    await s._manage_position(B, Decimal("0.014"), AVG)       # first piece filled
    assert live_qty(c, "SL") == Decimal("0.014") and live_qty(c, "TP") is None
    await s._manage_position(B, Decimal("0.07"), AVG)        # second piece; TP still blocked
    assert live_qty(c, "SL") == Decimal("0.07"), c.placed     # old code: stayed 0.014 here
    tp_blocked[0] = False
    await s._manage_position(B, Decimal("0.07"), AVG)        # TP now placeable
    assert live_qty(c, "SL") == Decimal("0.07") and live_qty(c, "TP") == Decimal("0.07"), c.placed
    n = len(c.placed)
    await s._manage_position(B, Decimal("0.07"), AVG)        # nothing changed -> no churn
    assert len(c.placed) == n and live_qty(c, "SL") == Decimal("0.07")


async def test_both_legs_resting_partial_fill_still_renews():
    s, c = setup([False])
    await s._manage_position(B, Decimal("0.014"), AVG)
    assert live_qty(c, "SL") == live_qty(c, "TP") == Decimal("0.014")
    await s._manage_position(B, Decimal("0.07"), AVG)
    assert live_qty(c, "SL") == live_qty(c, "TP") == Decimal("0.07"), c.placed
    assert len(c.cancelled) == 2


async def test_avg_price_change_renews_single_resting_leg():
    s, c = setup([True])
    await s._manage_position(B, Decimal("0.07"), AVG)
    first = [oid for (l, q, oid) in c.placed if l == "SL"][0]
    await s._manage_position(B, Decimal("0.07"), Decimal("7656.5"))   # same qty, new average price
    assert first in c.cancelled and live_qty(c, "SL") == Decimal("0.07")


async def test_unchanged_position_places_nothing_new():
    s, c = setup([False])
    for _ in range(5):
        await s._manage_position(B, Decimal("0.07"), AVG)
    assert len(c.placed) == 2 and c.cancelled == []


if __name__ == "__main__":
    import sys
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            try:
                asyncio.run(fn()); print("PASS", name)
            except Exception as e:
                fails += 1; import traceback; traceback.print_exc(); print("FAIL", name, e)
    sys.exit(1 if fails else 0)
