"""Tests for drain mode (strategy) and examples/bot_supervisor.py (2026-10-14).

Run from the repo root:  PYTHONPATH=src:tests:examples python tests/test_drain_and_supervisor.py
"""
import asyncio
import datetime as dt
import os
import signal
import sys
import tempfile
import time
from decimal import Decimal

from qfex2.models import BBO, PositionState
from qfex2.perpetual_market_making import drain_until_flat
from test_orphan_fix import make_strategy
import bot_supervisor as S

SYM = "GOLD-USD"
FLAT_BBO = BBO(symbol=SYM, bid_px=Decimal("4000"), bid_qty=Decimal("1"), ask_px=Decimal("4000.5"), ask_qty=Decimal("1"))


def strat_with(pos="0", open_orders="0"):
    s, c = make_strategy()
    c.positions[SYM] = PositionState(symbol=SYM, position=Decimal(pos), open_orders=Decimal(open_orders),
                                     average_price=Decimal("4000"))
    s.cancelled = []
    async def fake_cancel_quotes():
        s.cancelled.append(time.monotonic()); s.bid_order_id = None; s.ask_order_id = None
    s._cancel_quotes_if_any = fake_cancel_quotes
    return s, c


async def test_draining_strategy_does_not_quote():
    s, c = strat_with()
    s.quoted = False
    async def q(bbo): s.quoted = True
    s._quote = q
    s.draining = True
    await s._tick(FLAT_BBO)
    assert not s.quoted and s.cancelled


async def test_drain_flat_stops():
    s, c = strat_with()
    s.bid_order_id = "b"; s.ask_order_id = "a"
    stopped = []
    await asyncio.wait_for(drain_until_flat(c, s, SYM, lambda: stopped.append(1), poll_s=0.01, confirm_s=0.01), 2)
    assert stopped == [1] and s.draining and s.cancelled


async def test_drain_waits_while_in_position():
    s, c = strat_with(pos="0.05")
    stopped = []
    task = asyncio.create_task(drain_until_flat(c, s, SYM, lambda: stopped.append(1), poll_s=0.01, confirm_s=0.01))
    await asyncio.sleep(0.2)
    assert stopped == [] and not task.done()          # never stops with a position open
    c.positions[SYM].position = Decimal("0")             # exit filled
    await asyncio.wait_for(task, 2)
    assert stopped == [1]


async def test_drain_fill_during_cancel_is_managed_first():
    s, c = strat_with()
    s.bid_order_id = "b"
    async def cancel_but_fill():
        s.bid_order_id = None; s.ask_order_id = None
        c.positions[SYM].position = Decimal("0.05")      # quote filled while its cancel was in flight
    s._cancel_quotes_if_any = cancel_but_fill
    stopped = []
    task = asyncio.create_task(drain_until_flat(c, s, SYM, lambda: stopped.append(1), poll_s=0.01, confirm_s=0.01))
    await asyncio.sleep(0.2)
    assert stopped == []
    async def noop(): pass
    s._cancel_quotes_if_any = noop
    c.positions[SYM].position = Decimal("0")
    await asyncio.wait_for(task, 2)
    assert stopped == [1]


async def test_drain_waits_for_exchange_open_orders_then_grace():
    s, c = strat_with(open_orders="2")
    stopped = []
    task = asyncio.create_task(drain_until_flat(c, s, SYM, lambda: stopped.append(1), poll_s=0.01, confirm_s=0.01,
                                                open_orders_grace_s=0.3))
    await asyncio.sleep(0.1)
    assert stopped == []
    await asyncio.wait_for(task, 2)                    # grace expires -> stops anyway
    assert stopped == [1]


def test_schedule():
    S.BLACKOUTS = []          # the shipped blackout list is tested separately below
    sch = [("01:00", "GOLD"), ("12:00", "US500")]
    d = lambda hh, mm, day=14: dt.datetime(2026, 10, day, hh, mm)
    assert S.desired_bot(d(0, 59), sch, set()) == "US500"
    assert S.desired_bot(d(1, 0), sch, set()) == "GOLD"
    assert S.desired_bot(d(11, 59), sch, set()) == "GOLD"
    assert S.desired_bot(d(12, 0), sch, set()) == "US500"
    assert S.desired_bot(d(19, 0), sch, set()) == "US500"
    dd = {"2026-10-15"}
    assert S.desired_bot(d(12, 10, 15), sch, dd) is None
    assert S.desired_bot(d(12, 35, 15), sch, dd) == "US500"
    assert S.desired_bot(d(12, 10, 14), sch, dd) == "US500"
    bo = [("2026-10-15", "12:20", "12:40"), ("2026-10-28", "17:50", "18:45")]
    assert S.desired_bot(d(12, 19, 15), sch, set(), bo) == "US500"
    assert S.desired_bot(d(12, 20, 15), sch, set(), bo) is None
    assert S.desired_bot(d(12, 40, 15), sch, set(), bo) == "US500"
    assert S.desired_bot(d(18, 0, 28), sch, set(), bo) is None
    assert S.desired_bot(d(18, 0, 27), sch, set(), bo) == "US500"


def test_supervisor_switch_and_restart():
    # dummy bot: logs its args, exits ~0.5 s after SIGUSR1; "CRASH" arg exits at once
    tmp = tempfile.mkdtemp()
    out = os.path.join(tmp, "events.txt")
    dummy = os.path.join(tmp, "dummy.py")
    open(dummy, "w").write(f'''
import signal, sys, time
open({out!r}, "a").write("start " + sys.argv[1] + "\\n")
if sys.argv[1] == "CRASH": sys.exit(3)
stop = []
signal.signal(signal.SIGUSR1, lambda *a: stop.append(time.time()))
while not stop or time.time() - stop[0] < 0.5: time.sleep(0.05)
open({out!r}, "a").write("drained " + sys.argv[1] + "\\n")
''')
    S.MAKER = dummy
    S.BOTS = {"A": ["A"], "B": ["B"], "CRASH": ["CRASH"]}
    S.RESTART_DELAY_S = 0
    S.MAX_RESTARTS_PER_HOUR = 2
    want = ["A"]
    S.desired_bot = lambda now: want[0]
    sup = S.Supervisor()
    def run_for(sec):
        t = time.time()
        while time.time() - t < sec:
            sup.step(); time.sleep(0.05)
    run_for(0.5)
    want[0] = "B"; run_for(2.0)
    ev = open(out).read().split("\n")
    assert ev[:3] == ["start A", "drained A", "start B"], ev       # never two at once, drained first
    want[0] = "CRASH"; run_for(2.0)
    ev = open(out).read().split("\n")
    assert ev.count("start CRASH") == 3 and sup.gave_up_on == "CRASH", ev   # 1 + 2 restarts, then gives up
    want[0] = None; run_for(0.3)
    assert sup.proc is None


def test_weekend_pauses():
    # Oct 23 2026 = Friday (UTC). Paused: GOLD Sat/Sun 01:00 sessions, US500 Sat 12:00 session.
    import importlib
    importlib.reload(S)       # an earlier test monkeypatches S.desired_bot
    sch = [("01:00", "GOLD"), ("12:00", "US500")]
    want = {
        (23, 13, 0): "US500",   # Fri night
        (24, 0, 59): "US500",   # Fri-night session runs to Sat 01:00
        (24, 1, 0): None,       # Sat GOLD paused
        (24, 12, 0): None,      # Sat-night US500 paused
        (24, 20, 0): None,      # ...still Saturday's session
        (25, 1, 0): None,       # Sun GOLD paused
        (25, 12, 0): "US500",   # Sun night kept (Monday futures reopen)
        (26, 0, 59): "US500",
        (26, 1, 0): "GOLD",     # Monday GOLD
    }
    for (day, hh, mm), w in want.items():
        got = S.desired_bot(dt.datetime(2026, 10, day, hh, mm), sch, set(), [],
                            {("GOLD", 5), ("GOLD", 6), ("US500", 5)}, weekend_pause=None)
        assert got == w, (day, hh, mm, got, w)
    # an empty pause set restores the old behaviour
    assert S.desired_bot(dt.datetime(2026, 10, 24, 1, 0), sch, set(), [], set(), weekend_pause=None) == "GOLD"
    # shipped schedule (UTC): US500 day + night; since 2026-11-16 nothing Sat 01:00 -> Mon 00:00
    for day, hh, w in [(24, 0, "US500"), (24, 1, None), (24, 12, None), (25, 1, None), (25, 12, None), (26, 0, "US500"), (26, 1, "US500_DAY")]:
        assert S.desired_bot(dt.datetime(2026, 10, day, hh, 0), None, set(), []) == w, (day, hh)


def test_us_open_pause():
    # 9:30 am New York = 13:30 UTC until Nov 1 2026, 14:30 after; weekdays only
    import importlib
    importlib.reload(S)
    sch = [("01:00", "US500_DAY"), ("12:00", "US500")]
    want = [("2026-10-30 13:24", "US500"), ("2026-10-30 13:25", None), ("2026-10-30 13:44", None),
            ("2026-10-30 13:45", "US500"), ("2026-10-31 13:30", "US500"),      # Saturday: no pause
            ("2026-11-02 13:30", "US500"), ("2026-11-02 14:25", None), ("2026-11-02 14:44", None),
            ("2026-11-02 14:45", "US500"), ("2026-11-08 14:30", "US500")]      # Sunday: no pause
    for t, w in want:
        got = S.desired_bot(dt.datetime.strptime(t, "%Y-%m-%d %H:%M"), sch, set(), [], set(), weekend_pause=None)
        assert got == w, (t, got, w)
    # can be switched off
    assert S.desired_bot(dt.datetime(2026, 10, 30, 13, 30), sch, set(), [], set(), None) == "US500"


def test_us_open_pause_far_east_timezone():
    # In Sydney (UTC+11 in November) the New York open is 01:30 the NEXT local day:
    # Monday's open is Tuesday 01:30 and Friday's is Saturday 01:30.
    import importlib
    from zoneinfo import ZoneInfo
    importlib.reload(S)
    S._TZ = ZoneInfo("Australia/Sydney")
    assert S.in_us_open_pause(dt.datetime(2026, 11, 3, 1, 30), (5, 15))     # Mon NY open
    assert S.in_us_open_pause(dt.datetime(2026, 11, 7, 1, 30), (5, 15))     # Fri NY open
    assert not S.in_us_open_pause(dt.datetime(2026, 11, 2, 1, 30), (5, 15))  # Sunday in New York
    importlib.reload(S)


if __name__ == "__main__":
    for k, v in dict(globals()).items():
        if k.startswith("test_"):
            r = v()
            if asyncio.iscoroutine(r): asyncio.run(r)
            print("PASS", k)
