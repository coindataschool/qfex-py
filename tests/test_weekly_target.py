"""Tests for the weekly taker-volume target (2026-10-31): bot-side taker ledger and the
supervisor's cap / boost logic.

Run from the repo root:  PYTHONPATH=src:tests:examples python tests/test_weekly_target.py
"""
import asyncio, datetime as dt, json, os, tempfile, importlib
from decimal import Decimal

import bot_supervisor as S
from test_orphan_fix import make_strategy

D = dt.datetime


def test_week_start_boundaries():
    assert S.week_start(D(2026, 11, 1, 23, 59)) == D(2026, 10, 26, 0, 0)
    assert S.week_start(D(2026, 11, 2, 0, 0)) == D(2026, 11, 2, 0, 0)
    assert S.week_start(D(2026, 11, 8, 16, 59)) == D(2026, 11, 2, 0, 0)
    # Mon 00:00 local == Mon 00:00 UTC (TIMEZONE = UTC)
    assert S.local_to_epoch(D(2026, 11, 2, 0, 0)) == dt.datetime(2026, 11, 2, tzinfo=dt.timezone.utc).timestamp()


def test_weekly_taker_sum():
    p = os.path.join(tempfile.mkdtemp(), "l.jsonl")
    assert S.weekly_taker(p, 0) == 0.0                     # no ledger yet
    t0 = S.local_to_epoch(D(2026, 11, 2, 0, 0))
    with open(p, "w") as fh:
        fh.write(json.dumps({"t": t0 - 1, "notional": 999}) + "\n")     # last week
        fh.write(json.dumps({"t": t0 + 10, "notional": 500.5}) + "\n")
        fh.write("garbage\n")
        fh.write(json.dumps({"t": t0 + 20, "notional": 499.5}) + "\n")
    assert S.weekly_taker(p, t0) == 1000.0


def test_pace_and_modes():
    T = 100_000
    assert S.pace_required(D(2026, 11, 2, 0, 0), T) == 0
    assert round(S.pace_required(D(2026, 11, 4, 12, 30), T)) == round(T * 60.5 / S.PACE_END_HOURS)
    assert S.pace_required(D(2026, 11, 7, 5, 0), T) == T                      # after Fri 00:00
    assert S.volume_mode(0, 5_000, T, "normal") == "normal"                    # too early to boost
    assert S.volume_mode(10_000, 20_000, T, "normal") == "boost"
    assert S.volume_mode(18_000, 20_000, T, "boost") == "boost"                # hysteresis band
    assert S.volume_mode(18_000, 20_000, T, "normal") == "normal"
    assert S.volume_mode(20_000, 20_000, T, "boost") == "normal"
    assert S.volume_mode(100_000, 50_000, T, "normal") == "capped"


def _sup(ledger, now):
    importlib.reload(S)
    S.TAKER_LEDGER = ledger
    S.PNL_LEDGER = ledger + ".pnl"
    S.WEEKLY_TARGET_FROM = None
    S.local_now = lambda: now[0]
    S.notify = lambda msg: None
    return S.Supervisor()


def test_supervisor_cap_boost_and_new_week():
    p = os.path.join(tempfile.mkdtemp(), "l.jsonl")
    now = [D(2026, 11, 4, 14, 0)]          # Wed 14:00: pace ~ $65k due (96 h line)
    sup = _sup(p, now)
    S.POST_TARGET_TRADING = False                                    # old behaviour: pause at target
    assert sup.apply_weekly_target("US500") == "US500_BOOST"        # nothing done -> boost
    assert sup.apply_weekly_target(None) is None
    t = S.local_to_epoch(D(2026, 11, 4, 13, 0))
    with open(p, "a") as fh:
        fh.write(json.dumps({"t": t, "notional": 70_000}) + "\n")
    sup.taker_checked_at = -1e9                                        # force a ledger re-read
    assert sup.apply_weekly_target("US500") == "US500_BOOST"           # caught up, but dwell not over
    sup.mode_since -= S.MODE_MIN_DWELL_S
    sup.taker_checked_at = -1e9
    assert sup.apply_weekly_target("US500") == "US500"                 # back to normal
    with open(p, "a") as fh:
        fh.write(json.dumps({"t": t + 60, "notional": 31_000}) + "\n")
    sup.taker_checked_at = -1e9
    assert sup.apply_weekly_target("US500") is None and sup.mode == "capped"   # cap at once
    assert sup.apply_weekly_target("US500_DAY") is None
    now[0] = D(2026, 11, 9, 0, 1)                                      # next week
    assert sup.apply_weekly_target("US500_DAY") == "US500_DAY" and sup.mode == "normal"


def _write(path, rows):
    with open(path, "a") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")


def _post_setup(now):
    """Target reached Wed 20:00; pnl snapshots start Wed 19:00 at realised 0 / fees 0."""
    p = os.path.join(tempfile.mkdtemp(), "l.jsonl")
    sup = _sup(p, now)
    t = S.local_to_epoch(D(2026, 11, 4, 13, 0))
    _write(p, [{"t": t - 4 * 3600, "notional": 99_000}, {"t": t, "notional": 2_000}])
    _write(S.PNL_LEDGER, [{"t": t - 3600, "realised": 0.0, "fees": 0.0, "funding": 0.0}])
    return p, sup, t


def _refresh(sup):
    sup.taker_checked_at = -1e9


def test_post_target_keeps_trading_when_cheap():
    now = [D(2026, 11, 4, 14, 0)]
    p, sup, t = _post_setup(now)
    # $8k taker (~$16k total) in the last hour costing $0.50 -> $0.31/10k: keep going
    _write(p, [{"t": t + 1800, "notional": 8_000}])
    _write(S.PNL_LEDGER, [{"t": t + 1800, "realised": -0.30, "fees": 0.20, "funding": 0.0}])
    assert sup.apply_weekly_target("US500") == "US500" and sup.mode == "post"
    assert sup.apply_weekly_target("US500_DAY") == "US500_DAY"        # never boosted after target
    assert round(sup.post_cost, 2) == 0.50


def test_post_target_pauses_when_costly_then_resumes_at_switch():
    now = [D(2026, 11, 4, 14, 0)]
    p, sup, t = _post_setup(now)
    assert sup.apply_weekly_target("US500") == "US500"                 # just crossed, no data yet
    # $3k taker (~$6k total) costing $0.80 -> $1.33/10k: pause until 01:00
    _write(p, [{"t": t + 1800, "notional": 3_000}])
    _write(S.PNL_LEDGER, [{"t": t + 1800, "realised": -0.60, "fees": 0.20, "funding": 0.0}])
    _refresh(sup)
    assert sup.apply_weekly_target("US500") is None and sup.mode == "paused"
    assert sup.paused_until == D(2026, 11, 5, 1, 0)
    now[0] = D(2026, 11, 5, 0, 59)
    _refresh(sup)
    assert sup.apply_weekly_target("US500") is None
    now[0] = D(2026, 11, 5, 1, 0)                                      # session switch: try again
    _refresh(sup)
    assert sup.apply_weekly_target("US500_DAY") == "US500_DAY" and sup.mode == "post"
    assert sup.trail[0] == 0.0                                          # old bad window forgotten


def test_post_target_needs_min_volume():
    now = [D(2026, 11, 4, 14, 0)]
    p, sup, t = _post_setup(now)
    # $2.3k taker in the window (~$4.6k total) costing $0.40, too little volume to judge
    _write(p, [{"t": t + 1800, "notional": 300}])
    _write(S.PNL_LEDGER, [{"t": t + 1800, "realised": -0.30, "fees": 0.10, "funding": 0.0}])
    assert sup.apply_weekly_target("US500") == "US500" and sup.mode == "post"


def test_post_target_budget_caps_until_next_week():
    now = [D(2026, 11, 5, 20, 0)]
    p, sup, t = _post_setup(now)
    # cheap per $10k in the last 2 h, but $5.50 spent since the target in total
    _write(p, [{"t": t + 3600 * 20, "notional": 40_000}])
    _write(S.PNL_LEDGER, [{"t": t + 3600 * 5, "realised": -4.0, "fees": 1.0, "funding": 0.0},
                          {"t": t + 3600 * 29, "realised": -4.3, "fees": 1.2, "funding": 0.0}])
    assert sup.apply_weekly_target("US500") is None and sup.mode == "capped"
    now[0] = D(2026, 11, 6, 12, 0)                                     # session switch: still capped
    _refresh(sup)
    assert sup.apply_weekly_target("US500") is None
    now[0] = D(2026, 11, 9, 0, 1)                                      # new week
    _refresh(sup)
    assert sup.apply_weekly_target("US500_DAY") == "US500_DAY" and sup.mode in ("normal", "boost")
    assert sup.paused_until is None and sup.post_cost == 0.0


def test_helpers():
    assert S.next_session_switch(D(2026, 11, 4, 14, 0)) == D(2026, 11, 5, 1, 0)
    assert S.next_session_switch(D(2026, 11, 5, 1, 0)) == D(2026, 11, 5, 12, 0)
    assert S.next_session_switch(D(2026, 11, 5, 0, 0)) == D(2026, 11, 5, 1, 0)
    rows = [{"t": 1, "notional": 60}, {"t": 2, "notional": 50}]
    assert S.cross_time(rows, 100) == 2 and S.cross_time(rows, 200) is None
    snaps = [{"t": 10, "realised": 1.0, "fees": 0.5}, {"t": 20, "realised": 0.0, "fees": 0.7}]
    assert round(S.cost_between(snaps, 10, 25), 2) == 1.2
    assert S.cost_between([], 0, 1) == 0.0


def test_not_active_before_start_date():
    importlib.reload(S)
    S.local_now = lambda: D(2026, 10, 31, 5, 0)          # Sat before the first target week
    S.TAKER_LEDGER = "/nonexistent/ledger.jsonl"
    assert S.Supervisor().apply_weekly_target("US500_DAY") == "US500_DAY"   # no boost on old week
    importlib.reload(S)


def test_target_off():
    importlib.reload(S)
    S.WEEKLY_TAKER_TARGET = None
    assert S.Supervisor().apply_weekly_target("US500") == "US500"
    importlib.reload(S)


def test_ledger_written_only_for_taker_fills():
    s, c = make_strategy()
    p = os.path.join(tempfile.mkdtemp(), "taker.jsonl")
    s.taker_ledger_file = p
    def fill(otype, status, qty="0.07", px=7650.0):
        return {"order_response": {"symbol": "US500-USD", "type": otype, "status": status, "price": px,
                                   "side": "SELL", "order_id": "x"},
                "filled_qty_delta": Decimal(qty)}
    s._record_taker_fill(fill("MARKET", "FILLED"))
    s._record_taker_fill(fill("STOP_MARKET", "FILLED", "0.03"))
    s._record_taker_fill(fill("ALO", "FILLED"))                 # our maker quote: not taker
    s._record_taker_fill(fill("TAKE_PROFIT", "CANCELLED", "0.014"))   # QFEX resize artifact
    rows = [json.loads(l) for l in open(p)]
    assert [r["type"] for r in rows] == ["MARKET", "STOP_MARKET"]
    assert abs(sum(r["notional"] for r in rows) - 0.10 * 7650.0) < 1e-6
    s.taker_ledger_file = None
    s._record_taker_fill(fill("MARKET", "FILLED"))              # disabled -> no write
    assert len(open(p).readlines()) == 2



def test_pnl_ledger_written_on_change_only():
    s, _ = make_strategy()
    p = os.path.join(tempfile.mkdtemp(), "pnl.jsonl")
    s.pnl_ledger_file = p
    s.on_balance({"realised_pnl": "1.5", "fees": "0.2", "net_funding": "0"})
    s.on_balance({"realised_pnl": "1.5", "fees": "0.2", "net_funding": "0.01"})   # no change
    s.on_balance({"realised_pnl": "1.4", "fees": "0.25"})
    s.on_balance({"realised_pnl": "bad"})                                  # never raises
    rows = [json.loads(l) for l in open(p)]
    assert [(r["realised"], r["fees"]) for r in rows] == [(1.5, 0.2), (1.4, 0.25)]
    s.pnl_ledger_file = None
    s.on_balance({"realised_pnl": "9", "fees": "9"})
    assert len(open(p).readlines()) == 2



def test_weekend_pause():
    importlib.reload(S)
    S.local_now = lambda: D(2026, 11, 20, 5, 0)
    assert S.in_weekend_pause(D(2026, 11, 21, 0, 59)) is False       # Sat 00:59: Fri night session
    assert S.in_weekend_pause(D(2026, 11, 21, 1, 0)) is True         # Sat 01:00
    assert S.in_weekend_pause(D(2026, 11, 22, 16, 0)) is True        # Sun night
    assert S.in_weekend_pause(D(2026, 11, 22, 23, 59)) is True        # Sun 23:59
    assert S.in_weekend_pause(D(2026, 11, 23, 0, 0)) is False        # Mon 00:00
    assert S.in_weekend_pause(D(2026, 11, 18, 5, 0)) is False       # Wed
    assert S.desired_bot(D(2026, 11, 21, 0, 59)) == "US500"
    assert S.desired_bot(D(2026, 11, 21, 1, 0)) is None
    assert S.desired_bot(D(2026, 11, 22, 12, 0)) is None
    assert S.desired_bot(D(2026, 11, 23, 0, 0)) == "US500"          # Sun-night session's last hour
    assert S.desired_bot(D(2026, 11, 23, 1, 0)) == "US500_DAY"
    assert S.desired_bot(D(2026, 11, 21, 5, 0), weekend_pause=None) == "US500_DAY"
    S.WEEKEND_PAUSE = None
    assert S.desired_bot(D(2026, 11, 21, 5, 0)) == "US500_DAY"
    importlib.reload(S)


if __name__ == "__main__":
    import sys
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            try:
                r = fn()
                if asyncio.iscoroutine(r): asyncio.run(r)
                print("PASS", name)
            except Exception as e:
                fails += 1; import traceback; traceback.print_exc(); print("FAIL", name, e)
    sys.exit(1 if fails else 0)
