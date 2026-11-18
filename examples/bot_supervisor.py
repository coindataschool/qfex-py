"""
bot_supervisor.py — runs ONE qfex2 perpetual maker at a time and switches
instrument on a schedule. All times below are in TIMEZONE (UTC by default).
Added 2026-10-14.

How a switch works:
  1. send SIGUSR1 to the running bot -> it stops quoting, finishes any open
     position (its exits keep working), and exits by itself once flat
     (see qfex2.perpetual_market_making.drain_until_flat);
  2. wait for that process to exit (no timeout — it is never killed while it
     might hold a position; a reminder is logged every 5 minutes);
  3. start the next bot.
A bot that dies on its own is restarted after RESTART_DELAY_S (at most
MAX_RESTARTS_PER_HOUR times per hour, then the supervisor stops trying and
raises a macOS notification).

Run from the repo root (the folder that contains src/ and examples/):
    caffeinate -i .venv/bin/python examples/bot_supervisor.py
Stop it with Ctrl+C: the running bot is drained (exits when flat), then the
supervisor exits. Ctrl+C a second time stops the bot immediately (its normal
SIGINT handling: cancels orders, does NOT close a position).

API keys: the bots read QFEX_PUBLIC_KEY / QFEX_SECRET_KEY from the
environment, which they inherit from this process.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import signal
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

# ----------------------------- settings --------------------------------------

# Timezone for every clock time and date in these settings (schedule, blackouts,
# weekend pause, week start). Any IANA name works, e.g. "UTC", "Europe/London",
# "America/New_York", "Asia/Bangkok"; daylight saving is handled. If you change it,
# convert all the times below to the new zone as well.
TIMEZONE = "UTC"
_TZ = ZoneInfo(TIMEZONE)

# Daily schedule: (start "HH:MM", bot name). Each entry runs until the next
# entry's start time (wrapping past midnight).
SCHEDULE: list[tuple[str, str]] = [
    ("01:00", "US500_DAY"),
    ("12:00", "US500"),
]

# Big US data days (dates, "YYYY-MM-DD"): on these days the US500 start is
# delayed to US500_DATA_DAY_START, and nothing runs in between (GOLD is still
# drained at 12:00, before the 12:30 UTC release in US summer time).
DATA_DAYS: set = set()  # e.g. {"2026-10-15", "2026-11-06"}
US500_DATA_DAY_START = "12:35"

# Sessions that don't run, keyed by (bot, weekday the session STARTS on;
# Mon=0 ... Sat=5, Sun=6). Added 2026-10-19: the underlying markets are shut
# from Fri ~21:00 to Sun ~22:00 UTC, books are thin (the Oct 18 GOLD book
# emptied), and volume was tiny. Paused: GOLD Sat 01:00 and Sun 01:00,
# US500 Sat 12:00 (-> Sun 01:00). Kept: US500 Sun 12:00, which
# spans the Monday futures reopen.
# 2026-10-21: US500 weekend sessions un-paused (QFEX US500 still trades ~13k times/day
# on weekends; sim cost at or below weekdays). GOLD entries kept in case GOLD returns.
PAUSED_SESSIONS: set = {
    ("GOLD", 5),
    ("GOLD", 6),
}

# Weekend pause (added 2026-11-16): no bot from Sat 01:00 to Mon 00:00 UTC,
# whatever the weekly target says. Week of Nov 9: weekend sessions cost $1.96 on
# $29k volume ($0.67 per $10k) vs about break-even on weekdays. The Friday night
# session (US Friday) still runs to Sat 01:00. None turns it off.
# ((weekday, "HH:MM") start, (weekday, "HH:MM") end), Mon=0 ... Sun=6.
WEEKEND_PAUSE: tuple[tuple[int, str], tuple[int, str]] | None = (
    (5, "01:00"),
    (0, "00:00"),
)


def in_weekend_pause(now_local: dt.datetime, wp=None) -> bool:
    """True inside the weekend pause window (it may wrap past Sunday)."""
    wp = WEEKEND_PAUSE if wp is None else wp
    if not wp:
        return False

    def wmin(day: int, hhmm: str) -> int:
        h, m = (int(x) for x in hhmm.split(":"))
        return day * 1440 + h * 60 + m

    a, b = wmin(*wp[0]), wmin(*wp[1])
    cur = wmin(now_local.weekday(), now_local.strftime("%H:%M"))
    return a <= cur < b if a < b else (cur >= a or cur < b)


# Blackout windows (date, start, end): NO bot runs inside them — the running
# bot is drained just before (it still finishes any open position).
# Added 2026-10-14 after a CPI release cost ~$1.60 in two minutes.
# US releases at 8:30 am New York = 12:30 UTC until Nov 1 (13:30 after US
# clocks change); FOMC decisions at 2:00 pm New York = 18:00 UTC (19:00 in winter).
# These are the author's dates for late 2026 — replace them with the releases
# that matter for your instrument and period.
# Sources: bls.gov/schedule/2026/10_sched_list.htm, federalreserve.gov FOMC calendar.
BLACKOUTS: list[tuple[str, str, str]] = [
    ("2026-10-15", "12:20", "12:40"),  # PPI
    ("2026-10-16", "12:20", "12:40"),  # import/export prices (minor)
    ("2026-10-28", "17:50", "18:45"),  # FOMC decision Oct 28 2:00 pm NY
    ("2026-10-30", "12:20", "12:40"),  # Employment Cost Index
    # ---- from Nov 1: US on standard time -> 8:30 am New York = 13:30 UTC,
    #      10:00 am NY = 15:00 UTC, FOMC 2:00 pm NY = 19:00 UTC.
    # Major releases (jobs, CPI, FOMC): paused ~3 h from just before the release
    # (added 2026-10-29 after the Oct 28 FOMC: the 18:45 UTC restart walked into the
    # press-conference volatility and cost ~$12 in 2 h 15 min).
    # Others: short 20-minute window as before.
    # Sources: bls.gov/schedule/2026/home.htm, bea.gov/news/schedule,
    #          federalreserve.gov/monetarypolicy/fomccalendars.htm (read 2026-10-29).
    ("2026-11-03", "14:50", "15:10"),  # JOLTS (10:00 NY)
    ("2026-11-05", "13:20", "13:40"),  # Productivity and Costs
    ("2026-11-06", "13:20", "16:30"),  # MAJOR: Employment Situation (jobs report)
    ("2026-11-10", "13:20", "16:30"),  # MAJOR: CPI
    ("2026-11-13", "13:20", "13:40"),  # PPI
    ("2026-11-17", "13:20", "13:40"),  # import/export prices (minor)
    (
        "2026-11-25",
        "13:20",
        "13:40",
    ),  # GDP 2nd estimate + PCE (Personal Income and Outlays)
    ("2026-12-01", "14:50", "15:10"),  # JOLTS (10:00 NY)
    ("2026-12-04", "13:20", "16:30"),  # MAJOR: Employment Situation (jobs report)
    ("2026-12-08", "13:20", "13:40"),  # Productivity and Costs (rev) + trade balance
    (
        "2026-12-09",
        "18:50",
        "22:00",
    ),  # MAJOR: FOMC Dec 8-9 decision 2:00 pm NY + press conference
    ("2026-12-10", "13:20", "16:30"),  # MAJOR: CPI
    ("2026-12-15", "13:20", "13:40"),  # PPI
    ("2026-12-17", "13:20", "13:40"),  # import/export prices (minor)
]

# Daily pause around the 9:30 am New York stock-market open, Mon-Fri (added 2026-10-30:
# QFEX slowed down at the open on Oct 26, 27, 28 and 30, and the first half hour after
# the open was the priciest of the night). (minutes before, minutes after) the open.
# The open is 13:30 UTC while the US is on summer time and 14:30 in winter —
# computed from the New York clock, so it follows the change automatically.
# US market holidays are not skipped (a 20-minute pause on a closed day costs nothing).
US_OPEN_PAUSE: tuple[int, int] | None = (5, 15)
_NY = ZoneInfo("America/New_York")


def us_open_local(day: dt.date) -> dt.datetime:
    """9:30 am New York on New York date `day`, as a naive TIMEZONE-local datetime."""
    ny = dt.datetime(day.year, day.month, day.day, 9, 30, tzinfo=_NY)
    return ny.astimezone(_TZ).replace(tzinfo=None)


def in_us_open_pause(now_local: dt.datetime, pause: tuple[int, int] | None) -> bool:
    """True within pause=(minutes before, minutes after) a Mon-Fri New York open.
    Checks the neighbouring New York dates too, so it works in any TIMEZONE."""
    if not pause:
        return False
    for k in (-1, 0, 1):
        d = now_local.date() + dt.timedelta(days=k)
        if d.weekday() >= 5:
            continue
        o = us_open_local(d)
        if (
            o - dt.timedelta(minutes=pause[0])
            <= now_local
            < o + dt.timedelta(minutes=pause[1])
        ):
            return True
    return False


COMMON = [
    "--is-prod",
    "--underlier-weight",
    "0",
    "--order-refresh-tolerance",
    "0.00005",
    "--log-file",
    "logs/mm.log",
    "--log-level",
    "DEBUG",  # my log checks need the DEBUG lines (positions, balances, order responses)
]
BOTS: dict[str, list[str]] = {
    "GOLD": [
        "--symbol",
        "GOLD-USD",
        "--order-amount",
        "0.07",
        "--leverage",
        "15",
        "--bid-spread",
        "0.0002",
        "--ask-spread",
        "0.0002",
        "--stop-loss-spread",
        "0.0008",
        "--take-profit-spread",
        "0.0008",
        "--bbo-stop-loss-confirm-streak",
        "5",
    ]
    + COMMON,
    "US500": [
        "--symbol",
        "US500-USD",
        "--order-amount",
        "0.07",  # was 0.05; raised 2026-10-26
        "--leverage",
        "15",
        # 3.0 bps from 2026-10-31 (2.5 on Oct 30, 2.0 Oct 21-29). Backtest Oct 7-31 at
        # 0.07: 3.0 bps ~$0.46 per $10k vs 2.5 bps ~$0.55; the BOOST variant below tops
        # volume up in quiet weeks.
        "--bid-spread",
        "0.0003",
        "--ask-spread",
        "0.0003",
        "--stop-loss-spread",
        "0.0001",
        "--take-profit-spread",
        "0.0001",
        # 2 ticks (~1 s), not 5: with 1 bp exits a 2.5 s wait is too slow in a fast move (2026-10-14)
        "--bbo-stop-loss-confirm-streak",
        "2",
    ]
    + COMMON,
}
# Daytime US500 (01:00-12:00 UTC) — same settings as the night bot for now. A separate
# name so the weekend pauses can tell the day and night sessions apart, and so the
# day settings can be tuned on their own later. The 12:00 switch restarts the bot.
BOTS["US500_DAY"] = list(BOTS["US500"])


def _set_arg(args: list[str], flag: str, value: str) -> None:
    args[args.index(flag) + 1] = value


# 2026-10-26: daytime 2.5 bps x 0.10 (sim: ~70% of the 2.0 x 0.07 daytime volume at
# about a quarter of the cost per $). Nights stay 2.0 bps x 0.07.
_set_arg(BOTS["US500_DAY"], "--order-amount", "0.10")
_set_arg(BOTS["US500_DAY"], "--bid-spread", "0.00025")
_set_arg(BOTS["US500_DAY"], "--ask-spread", "0.00025")

# ---- weekly taker-volume target (added 2026-10-31) ----------------------------
# The bots append every taker fill (their exits) to TAKER_LEDGER. Each week
# (from WEEK_START_*) the supervisor:
#   * stops all trading once the week's taker volume reaches WEEKLY_TAKER_TARGET
#     (resumes automatically when the next week starts);
#   * runs the "_BOOST" variant of a bot (tighter quotes -> ~2x volume) while the
#     week is behind pace, i.e. below BOOST_BELOW x the straight line from the week
#     start to PACE_END_HOURS later (Mon 00:00 + 96 h = Fri 00:00 UTC, leaving
#     a day of margin before the US Friday close), and the normal variant once it has caught up (>= 1.0 x pace).
#   Mode changes are applied at most every MODE_MIN_DWELL_S, each one is a normal
#   drain + restart. WEEKLY_TAKER_TARGET = None turns all of this off.
WEEKLY_TAKER_TARGET: float | None = 100_000.0
TAKER_LEDGER = "logs/taker_fills.jsonl"
WEEK_START_WEEKDAY = 0  # Monday
WEEK_START_TIME = "00:00"
PACE_END_HOURS = (
    96.0  # Mon 00:00 + 96 h = Fri 00:00 UTC (was 117 = Fri 21:00; replay of
)
# Oct 12-31 showed weeks reaching the target only on Fri night/Sun)
BOOST_BELOW = 0.85
BOOST_MIN_PACE = 0.10  # no boosting before 10% of the target is "due" (~Mon evening)
MODE_MIN_DWELL_S = 3600
LEDGER_REFRESH_S = 60
# The ledger only starts with this change, so the target logic starts with the
# first full week after it (the week of Oct 26 was already ~4x the target).
WEEKLY_TARGET_FROM = dt.datetime(2026, 11, 2, 0, 0)

# ---- after the weekly target (added 2026-11-13) ---------------------------------
# Once the week has WEEKLY_TAKER_TARGET of taker volume the bot keeps trading
# (normal settings, never boost) unless conditions look bad:
#   * last BAD_WINDOW_S (2 h) of the bot's own results cost more than
#     BAD_COST_PER_10K per $10k traded (judged only once >= BAD_MIN_VOLUME has traded
#     in the window, and only on trading since the last resume) -> pause until the
#     next session switch (a SCHEDULE start time), then try again;
#   * cost spent since the target was reached exceeds POST_TARGET_BUDGET -> pause
#     until the next week.
# Cost = change in account realised P&L + fees (logs/pnl_ledger.jsonl, written by
# the bot). It is account-wide, so manual trades on the same account count too.
# POST_TARGET_TRADING = False restores the old behaviour (pause at the target).
POST_TARGET_TRADING = True
PNL_LEDGER = "logs/pnl_ledger.jsonl"
BAD_COST_PER_10K = 0.60
BAD_WINDOW_S = 7200
BAD_MIN_VOLUME = 5_000.0  # total (maker + taker) volume in the window
POST_TARGET_BUDGET = 5.0

BOTS["US500_BOOST"] = list(BOTS["US500"])  # night boost: 2.0 bps x 0.07
_set_arg(BOTS["US500_BOOST"], "--bid-spread", "0.0002")
_set_arg(BOTS["US500_BOOST"], "--ask-spread", "0.0002")
BOTS["US500_DAY_BOOST"] = list(BOTS["US500_DAY"])  # day boost: 2.0 bps x 0.10
_set_arg(BOTS["US500_DAY_BOOST"], "--bid-spread", "0.0002")
_set_arg(BOTS["US500_DAY_BOOST"], "--ask-spread", "0.0002")

RESTART_DELAY_S = 15
MAX_RESTARTS_PER_HOUR = 5
POLL_S = 5
SUPERVISOR_LOG = "logs/supervisor.log"

# ------------------------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
MAKER = os.path.join(HERE, "perpetual_maker.py")
log = logging.getLogger("supervisor")


def local_now() -> dt.datetime:
    """Current wall-clock time in TIMEZONE, as a naive datetime."""
    return dt.datetime.now(_TZ).replace(tzinfo=None)


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def desired_bot(
    now_local: dt.datetime,
    schedule: list[tuple[str, str]] = None,
    data_days: set = None,
    blackouts: list[tuple[str, str, str]] = None,
    paused_sessions: set = None,
    us_open_pause: tuple[int, int] | None = "default",
    weekend_pause="default",
) -> str | None:
    """Which bot should be running at this TIMEZONE-local time (None = none)."""
    if in_weekend_pause(
        now_local,
        WEEKEND_PAUSE if weekend_pause == "default" else (weekend_pause or ()),
    ):
        return None
    us_open_pause = US_OPEN_PAUSE if us_open_pause == "default" else us_open_pause
    if in_us_open_pause(now_local, us_open_pause):
        return None
    schedule = SCHEDULE if schedule is None else schedule
    data_days = DATA_DAYS if data_days is None else data_days
    blackouts = BLACKOUTS if blackouts is None else blackouts
    paused_sessions = PAUSED_SESSIONS if paused_sessions is None else paused_sessions
    today = now_local.strftime("%Y-%m-%d")
    cur_m = now_local.hour * 60 + now_local.minute
    for day, a, b in blackouts:
        if day == today and _minutes(a) <= cur_m < _minutes(b):
            return None
    entries = sorted((_minutes(t), name) for t, name in schedule)
    cur = now_local.hour * 60 + now_local.minute
    name = entries[-1][1]  # before the first entry -> previous day's last
    start = entries[-1][0]
    start_day = now_local.date() - dt.timedelta(days=1)
    for m, n in entries:
        if cur >= m:
            name, start = n, m
            start_day = now_local.date()
    if (name, start_day.weekday()) in paused_sessions:
        return None
    if name == "US500" and now_local.strftime("%Y-%m-%d") in data_days:
        if start <= cur < _minutes(US500_DATA_DAY_START):
            return None
    return name


def week_start(now_local: dt.datetime) -> dt.datetime:
    """Start (TIMEZONE-local, naive) of the target week containing now_local."""
    h, m = (int(x) for x in WEEK_START_TIME.split(":"))
    d0 = now_local.date() - dt.timedelta(
        days=(now_local.weekday() - WEEK_START_WEEKDAY) % 7
    )
    start = dt.datetime(d0.year, d0.month, d0.day, h, m)
    if start > now_local:
        start -= dt.timedelta(days=7)
    return start


def local_to_epoch(t: dt.datetime) -> float:
    """Unix time of a naive TIMEZONE-local datetime."""
    return t.replace(tzinfo=_TZ).timestamp()


def weekly_taker(path: str, since_epoch: float) -> float:
    """Sum of taker notional in the ledger since since_epoch (0 if no ledger yet)."""
    total = 0.0
    try:
        with open(path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("t", 0) >= since_epoch:
                    total += float(r.get("notional", 0.0))
    except FileNotFoundError:
        return 0.0
    return total


def read_jsonl(path: str, since_epoch: float = 0.0) -> list[dict]:
    """Rows of a JSONL ledger with t >= since_epoch, sorted by t ([] if no file)."""
    rows = []
    try:
        with open(path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("t", 0) >= since_epoch:
                    rows.append(r)
    except FileNotFoundError:
        return []
    rows.sort(key=lambda r: r.get("t", 0))
    return rows


def cross_time(taker_rows: list[dict], target: float) -> float | None:
    """Epoch at which the cumulative taker notional first reached target."""
    tot = 0.0
    for r in taker_rows:
        tot += float(r.get("notional", 0.0))
        if tot >= target:
            return r["t"]
    return None


def pnl_at(snaps: list[dict], t: float) -> tuple[float, float] | None:
    """(realised, fees) of the last snapshot at or before t (first one if none)."""
    if not snaps:
        return None
    best = snaps[0]
    for r in snaps:
        if r["t"] <= t:
            best = r
        else:
            break
    return float(best.get("realised", 0.0)), float(best.get("fees", 0.0))


def cost_between(snaps: list[dict], a: float, b: float) -> float:
    """Trading cost between epochs a and b: -(change in realised P&L) + change in fees."""
    pa, pb = pnl_at(snaps, a), pnl_at(snaps, b)
    if pa is None or pb is None:
        return 0.0
    return -(pb[0] - pa[0]) + (pb[1] - pa[1])


def next_session_switch(
    now_local: dt.datetime, schedule: list[tuple[str, str]] = None
) -> dt.datetime:
    """Next schedule boundary (e.g. 01:00 / 12:00) strictly after now_local."""
    schedule = SCHEDULE if schedule is None else schedule
    cands = []
    for day in (0, 1):
        d = now_local.date() + dt.timedelta(days=day)
        for hhmm, _ in schedule:
            h, m = (int(x) for x in hhmm.split(":"))
            t = dt.datetime(d.year, d.month, d.day, h, m)
            if t > now_local:
                cands.append(t)
    return min(cands)


def pace_required(now_local: dt.datetime, target: float) -> float:
    """Taker volume the week 'should' have by now (straight line to PACE_END_HOURS)."""
    hours = (now_local - week_start(now_local)).total_seconds() / 3600.0
    return target * max(0.0, min(1.0, hours / PACE_END_HOURS))


def volume_mode(done: float, required: float, target: float, current: str) -> str:
    """'capped' / 'boost' / 'normal', with hysteresis between boost and normal."""
    if done >= target:
        return "capped"
    if required >= BOOST_MIN_PACE * target and done < BOOST_BELOW * required:
        return "boost"
    if done >= required:
        return "normal"
    return current if current in ("boost", "normal") else "normal"


def notify(msg: str) -> None:
    log.critical(msg)
    try:
        subprocess.run(
            [
                "osascript",
                "-e",
                f'display notification "{msg}" with title "qfex supervisor"',
            ],
            timeout=5,
            check=False,
        )
    except Exception:
        pass


class Supervisor:
    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.name: str | None = None
        self.draining = False
        self.drain_sent_at = 0.0
        self.stopping = False
        self.restarts: list[float] = []
        self.gave_up_on: str | None = None
        self.mode = "normal"  # weekly target: normal / boost / capped
        self.mode_since = 0.0
        self.taker_done = 0.0
        self.taker_checked_at = 0.0
        self.cap_week: dt.datetime | None = None
        self.status_logged_at = 0.0
        self.week_seen: dt.datetime | None = None
        self.taker_rows: list[dict] = []
        self.pnl_snaps: list[dict] = []
        self.paused_until: dt.datetime | None = None
        self.resumed_at: float | None = None
        self.post_cost = 0.0
        self.trail = (0.0, 0.0)  # (volume, cost) of the last post-target window

    def start(self, name: str) -> None:
        cmd = [sys.executable, MAKER] + BOTS[name]
        log.info("starting %s: %s", name, " ".join(cmd[1:]))
        # own process group: a Ctrl+C in this terminal reaches only the
        # supervisor, which then drains the bot instead of killing it.
        self.proc = subprocess.Popen(cmd, start_new_session=True)
        self.name = name
        self.draining = False

    def apply_weekly_target(self, want: str | None) -> str | None:
        """Weekly taker target: boost before it, keep trading after it unless costs
        turn bad (see WEEKLY_TAKER_TARGET and the post-target settings)."""
        if WEEKLY_TAKER_TARGET is None:
            return want
        now_l = local_now()
        if WEEKLY_TARGET_FROM is not None and now_l < WEEKLY_TARGET_FROM:
            return want
        mono = time.monotonic()
        wk = week_start(now_l)
        now_e = local_to_epoch(now_l)
        target = WEEKLY_TAKER_TARGET
        new_week = wk != self.week_seen
        if new_week and self.week_seen is not None:
            self.paused_until, self.resumed_at, self.post_cost, self.trail = (
                None,
                None,
                0.0,
                (0.0, 0.0),
            )
        if (
            new_week
            or self.taker_checked_at == 0.0
            or mono - self.taker_checked_at >= LEDGER_REFRESH_S
        ):
            self.week_seen = wk
            self.taker_rows = read_jsonl(TAKER_LEDGER, local_to_epoch(wk))
            self.taker_done = sum(
                float(r.get("notional", 0.0)) for r in self.taker_rows
            )
            self.pnl_snaps = read_jsonl(PNL_LEDGER, local_to_epoch(wk) - 86400)
            self.taker_checked_at = mono
        req = pace_required(now_l, target)
        why = ""
        if self.taker_done >= target:
            new, why = self._post_target_mode(now_l, now_e, wk, target)
        else:
            cur = self.mode if self.mode in ("boost", "normal") else "normal"
            new = volume_mode(self.taker_done, req, target, cur)
        if new != self.mode:
            pre = ("boost", "normal")
            if (
                new not in pre
                or self.mode not in pre
                or self.mode_since == 0.0
                or mono - self.mode_since >= MODE_MIN_DWELL_S
            ):
                log.info(
                    "weekly taker $%.0f of $%.0f (pace $%.0f): mode %s -> %s%s",
                    self.taker_done,
                    target,
                    req,
                    self.mode,
                    new,
                    why,
                )
                if new == "paused":
                    notify(f"Bot paused: costs turned bad after the weekly target{why}")
                elif new == "capped":
                    self.cap_week = wk
                    notify(
                        f"Bot paused until {(wk + dt.timedelta(days=7)).strftime('%a %d %b %H:%M')} {TIMEZONE}{why}"
                    )
                self.mode, self.mode_since = new, mono
        if mono - self.status_logged_at >= 3600:
            extra = ""
            if self.taker_done >= target:
                v, c = self.trail
                extra = (
                    ", post-target cost $%.2f of $%.2f, last window $%.2f on $%.0f"
                    % (self.post_cost, POST_TARGET_BUDGET, c, v)
                )
            log.info(
                "weekly taker $%.0f of $%.0f, pace $%.0f, mode %s%s",
                self.taker_done,
                target,
                req,
                self.mode,
                extra,
            )
            self.status_logged_at = mono
        if want is None or self.mode in ("paused", "capped"):
            return None
        if self.mode == "boost" and f"{want}_BOOST" in BOTS:
            return f"{want}_BOOST"
        return want

    def _post_target_mode(
        self, now_l: dt.datetime, now_e: float, wk: dt.datetime, target: float
    ):
        """Mode once the week's target is reached: post / paused / capped, plus a reason."""
        if self.mode == "capped" and self.cap_week == wk:
            return "capped", ""
        if not POST_TARGET_TRADING:
            return "capped", " (weekly taker target reached)"
        t_cross = cross_time(self.taker_rows, target) or now_e
        self.post_cost = cost_between(self.pnl_snaps, t_cross, now_e)
        if self.post_cost > POST_TARGET_BUDGET:
            return "capped", " (cost since the target $%.2f > $%.2f budget)" % (
                self.post_cost,
                POST_TARGET_BUDGET,
            )
        if self.paused_until is not None:
            if now_l < self.paused_until:
                return "paused", ""
            self.paused_until, self.resumed_at = None, now_e
        start = now_e - BAD_WINDOW_S
        if self.resumed_at is not None:
            start = max(start, self.resumed_at)
        vol = 2.0 * sum(
            float(r.get("notional", 0.0))
            for r in self.taker_rows
            if start <= r["t"] <= now_e
        )
        cost = cost_between(self.pnl_snaps, start, now_e)
        self.trail = (vol, cost)
        if vol >= BAD_MIN_VOLUME and cost / vol * 1e4 > BAD_COST_PER_10K:
            self.paused_until = next_session_switch(now_l)
            return (
                "paused",
                " (last %.1f h: $%.2f on $%.0f = $%.2f per $10k > $%.2f; until %s)"
                % (
                    (now_e - start) / 3600,
                    cost,
                    vol,
                    cost / vol * 1e4,
                    BAD_COST_PER_10K,
                    self.paused_until.strftime("%a %H:%M"),
                ),
            )
        return "post", ""

    def drain(self) -> None:
        if self.proc and not self.draining:
            log.info("draining %s (pid %s): exits once flat", self.name, self.proc.pid)
            self.proc.send_signal(signal.SIGUSR1)
            self.draining = True
            self.drain_sent_at = time.monotonic()

    def step(self) -> None:
        # 1. reap a finished process
        if self.proc and self.proc.poll() is not None:
            rc = self.proc.returncode
            if self.draining:
                log.info("%s exited after drain (rc=%s)", self.name, rc)
            else:
                now = time.time()
                self.restarts = [t for t in self.restarts if now - t < 3600] + [now]
                log.error("%s exited unexpectedly (rc=%s)", self.name, rc)
                if len(self.restarts) > MAX_RESTARTS_PER_HOUR:
                    self.gave_up_on = self.name
                    notify(
                        f"{self.name} died {len(self.restarts)} times in an hour — not restarting. Check the logs."
                    )
                else:
                    time.sleep(RESTART_DELAY_S)
            self.proc, self.name, self.draining = None, None, False

        want = None if self.stopping else desired_bot(local_now())
        want = self.apply_weekly_target(want)
        if want is not None and want == self.gave_up_on:
            want = None
        elif want != self.gave_up_on:
            self.gave_up_on = None

        # 2. running the wrong bot (or should run none) -> drain it
        if self.proc and self.name != want:
            self.drain()
            if self.draining and time.monotonic() - self.drain_sent_at > 300:
                log.warning(
                    "still waiting for %s to finish its position before switching",
                    self.name,
                )
                self.drain_sent_at = time.monotonic()
            return
        # 3. nothing running and something wanted -> start it
        if self.proc is None and want is not None:
            self.start(want)

    def run(self) -> None:
        def _sigint(*_):
            if self.stopping and self.proc:
                log.warning("second Ctrl+C: stopping %s immediately", self.name)
                self.proc.send_signal(signal.SIGINT)
                return
            log.info("Ctrl+C: draining the running bot, then exiting")
            self.stopping = True

        signal.signal(signal.SIGINT, _sigint)
        signal.signal(signal.SIGTERM, _sigint)
        log.info(
            "supervisor started; timezone %s, schedule %s, data days %s, blackouts %s, "
            "paused sessions (bot, start weekday Mon=0) %s, weekend pause %s, US-open pause %s min "
            "before/after (today's open %s), weekly taker target %s (week from %s %s)",
            TIMEZONE,
            SCHEDULE,
            sorted(DATA_DAYS),
            BLACKOUTS,
            sorted(PAUSED_SESSIONS),
            WEEKEND_PAUSE,
            US_OPEN_PAUSE,
            us_open_local(local_now().date()).strftime("%H:%M"),
            WEEKLY_TAKER_TARGET,
            ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][WEEK_START_WEEKDAY],
            WEEK_START_TIME,
        )
        while True:
            self.step()
            if self.stopping and self.proc is None:
                log.info("supervisor exiting")
                return
            time.sleep(POLL_S)


def main() -> None:
    os.makedirs(os.path.dirname(SUPERVISOR_LOG) or ".", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(SUPERVISOR_LOG), logging.StreamHandler()],
    )
    Supervisor().run()


if __name__ == "__main__":
    main()
