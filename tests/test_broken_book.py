"""Tests for the broken-book / disconnected-close fixes (2026-10-19).

Replays the Oct 18 15:38 GOLD incident: the book emptied (bid far below, ask far
above) while mark stayed fair, and the trade connection was down.

Run from the repo root:  PYTHONPATH=src:tests python tests/test_broken_book.py
"""
import asyncio
from decimal import Decimal

from test_fast_exit import make, bbo, t_of, SYM

# make(): long 0.01 @ 4000, SL/TP 8 bps -> SL trigger 3996.8, TP trigger 4003.2


async def test_broken_book_mark_fair_no_market_close():
    s, c = await make(streak=5)
    s.stop_loss_order_id = "sl"; s.take_profit_order_id = "tp"   # native stops resting
    c.mark_price_by_symbol[SYM] = Decimal("4000.5")
    for _ in range(8):                                           # streak satisfied many times over
        await s._maybe_bbo_triggered_close(bbo(3920, 4600), True, None, Decimal("0.01"),
                                           Decimal("3996.8"), Decimal("4003.2"))
    assert t_of(c, "market") == [], c.events


async def test_broken_book_but_mark_agrees_still_closes():
    s, c = await make(streak=1)
    s.stop_loss_order_id = "sl"; s.take_profit_order_id = "tp"
    c.mark_price_by_symbol[SYM] = Decimal("3990")                # mark also through the stop
    ok = await s._maybe_bbo_triggered_close(bbo(3920, 4600), True, None, Decimal("0.01"),
                                            Decimal("3996.8"), Decimal("4003.2"))
    assert ok and len(t_of(c, "market")) == 1, c.events


async def test_broken_book_no_mark_skips():
    s, c = await make(streak=1)
    s.stop_loss_order_id = "sl"; s.take_profit_order_id = "tp"
    c.mark_price_by_symbol.pop(SYM, None)
    ok = await s._maybe_bbo_triggered_close(bbo(3920, 4600), True, None, Decimal("0.01"),
                                            Decimal("3996.8"), Decimal("4003.2"))
    assert not ok and t_of(c, "market") == []


async def test_normal_book_fast_move_still_closes():
    # CPI-type move: tight book, mark lagging above the stop -> must still fire (unchanged)
    s, c = await make(streak=1)
    s.stop_loss_order_id = "sl"; s.take_profit_order_id = "tp"
    c.mark_price_by_symbol[SYM] = Decimal("4000")
    ok = await s._maybe_bbo_triggered_close(bbo(3990, 3991), True, None, Decimal("0.01"),
                                            Decimal("3996.8"), Decimal("4003.2"))
    assert ok and len(t_of(c, "market")) == 1, c.events


async def test_short_broken_book_mark_fair_skips():
    s, c = await make(pos="-0.01", streak=1)
    s.stop_loss_order_id = "sl"; s.take_profit_order_id = "tp"
    c.mark_price_by_symbol[SYM] = Decimal("4000")
    ok = await s._maybe_bbo_triggered_close(bbo(3400, 4080), False, None, Decimal("0.01"),
                                            Decimal("4003.2"), Decimal("3996.8"))
    assert not ok and t_of(c, "market") == []


async def test_take_profit_not_affected_by_spread_guard():
    s, c = await make(streak=1)
    s.stop_loss_order_id = "sl"; s.take_profit_order_id = "tp"
    c.mark_price_by_symbol[SYM] = Decimal("4000")
    ok = await s._maybe_bbo_triggered_close(bbo(4010, 4700), True, None, Decimal("0.01"),
                                            Decimal("3996.8"), Decimal("4003.2"))
    assert ok and len(t_of(c, "market")) == 1


async def test_disconnected_no_market_and_log_throttled():
    s, c = await make(streak=1)
    s.stop_loss_order_id = None
    c._trade_ready.clear()
    warnings = []
    c.log.warning = lambda fmt, *a: warnings.append(fmt % a)
    for _ in range(5):
        ok = await s._maybe_bbo_triggered_close(bbo(3990, 3991), True, None, Decimal("0.01"),
                                                Decimal("3996.8"), Decimal("4003.2"))
        assert not ok
    assert t_of(c, "market") == []
    skips = [w for w in warnings if "skipped: trade connection is down" in w]
    assert len(skips) == 1, warnings
    c._trade_ready.set()                                        # back online -> fires
    ok = await s._maybe_bbo_triggered_close(bbo(3990, 3991), True, None, Decimal("0.01"),
                                            Decimal("3996.8"), Decimal("4003.2"))
    assert ok and len(t_of(c, "market")) == 1


async def test_reconnect_purges_stale_queue():
    s, c = await make()
    for i in range(138):
        await c._enqueue_trade({"type": "add_order", "params": {"i": i}})
    await c._enqueue_trade({"type": "cancel_order", "params": {}})
    assert c._trade_out_q.qsize() == 139
    n = c._purge_stale_trade_queue()
    assert n == 139 and c._trade_out_q.qsize() == 0
    assert c._purge_stale_trade_queue() == 0


def test_session_code_purges_only_after_first_session():
    import inspect
    from qfex2 import base_client
    src = inspect.getsource(base_client)
    i = src.index("self._purge_stale_trade_queue()\n            self._trade_ready.set()")
    assert "if self._trade_session_count >= 1:" in src[i - 80:i]


if __name__ == "__main__":
    import sys
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            try:
                r = fn()
                if asyncio.iscoroutine(r):
                    asyncio.run(r)
                print("PASS", name)
            except Exception as e:
                fails += 1
                import traceback; traceback.print_exc()
                print("FAIL", name, e)
    sys.exit(1 if fails else 0)
