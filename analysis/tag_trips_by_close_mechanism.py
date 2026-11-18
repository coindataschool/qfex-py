"""
Tags each reconstructed Trip by which mechanism closed it — BBO-triggered
reduce_only close, or native STOP_LOSS/TAKE_PROFIT resolution — then
splits win rate and payoff ratio by that tag.

Answers: does Aug 7's low realized win rate (well below the theoretical
~0.50) skew toward trips closed by one mechanism vs the other? Slippage
alone (already measured in join_triggers_with_trips.py) explains
payoff-ratio drag but NOT win-rate drag, since slippage doesn't change
which barrier gets hit — only fill quality once it does. If BBO-triggered
trips have a meaningfully different win rate than native-triggered ones,
that's a stronger, more direct signal than the earlier markout/reversion
analysis (which only looked at price behavior, not actual trip outcomes).

MATCHING: for each Trip, find the BBO-triggered close event (if any)
whose (a) symbol matches, (b) log timestamp is within a few seconds of
the trip's exit_trade.order_timestamp, and (c) fill price is close to
exit_trade.price. If no BBO event matches within tolerance, the trip is
tagged NATIVE (closed via the ordinary STOP_LOSS/TAKE_PROFIT trigger
path, no BBO race involved). This mirrors join_triggers_with_trips.py's
nearest-prior-arm matching philosophy, just matching against BBO CLOSE
events instead of trigger ARM events, and matching by proximity rather
than strict "most recent before" since BBO fill events and /user/trade
order_timestamps come from two different clocks/pipelines and won't
align to the microsecond.
"""

import ast
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from zoneinfo import ZoneInfo

from extract_bbo_slippage import BBO_TRIGGER_RE, ENQUEUED_ORDER_RE, TRADE_RECEIVED_RE
from fetch_and_reconstruct_trips import (
    Trip,
    fetch_all_trades,
    reconstruct_trips,
)

LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")

# The bot's log timestamps are in the local time of the machine that wrote
# them (logging's default). Set LOG_TIMEZONE to that machine's IANA zone,
# e.g. "UTC" or "Europe/London", or leave it None to use this machine's own
# local timezone (correct when you analyse logs on the machine that wrote them).
# The trade data's order_timestamp (from /user/trade) is a plain unix
# timestamp, so converting the log time to UTC makes the two comparable.
LOG_TIMEZONE: Optional[str] = None

# How close a Trip's exit_trade.order_timestamp and a BBO-close event's
# log timestamp need to be to count as the same event. Generous but
# bounded — these are two different timestamp sources (QFEX's own
# /user/trade order_timestamp vs this bot's local log clock), not
# expected to align exactly, but should be within a couple seconds for
# the same real-world event, given fills resolve quickly once triggered.
MATCH_TOLERANCE_SECONDS = 5.0
PRICE_TOLERANCE = Decimal("0.05")


def _parse_log_ts(line: str) -> Optional[datetime]:
    m = LOG_TS_RE.match(line)
    if not m:
        return None
    # Log timestamps are local time (see LOG_TIMEZONE above). Parse as naive,
    # attach the zone, then convert to UTC so this is directly comparable with
    # exit_dt below (built from order_timestamp, a unix timestamp).
    naive = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S.%f")
    if LOG_TIMEZONE is None:
        return naive.astimezone(timezone.utc)      # naive = this machine's local time
    return naive.replace(tzinfo=ZoneInfo(LOG_TIMEZONE)).astimezone(timezone.utc)


@dataclass
class BboCloseFill:
    ts: datetime
    label: str
    symbol: str
    fill_price: Decimal


def extract_bbo_close_fills(log_paths: List[str]) -> List[BboCloseFill]:
    """
    Re-derives BBO-triggered close events WITH their log-line timestamp
    and real terminal fill price — extract_bbo_slippage.py's
    extract_bbo_trigger_slippage() computes fill_price/slippage but
    doesn't expose the log timestamp on BboTriggerEvent, so this redoes
    the extraction here rather than modifying that already-verified
    script for a need specific to this analysis (same approach used by
    mark_price_drift.py / mark_price_confirmation.py previously).
    """
    fills: List[BboCloseFill] = []

    for path in log_paths:
        with open(path, errors="replace") as f:
            lines = f.readlines()

        for i, line in enumerate(lines):
            m = BBO_TRIGGER_RE.search(line)
            if not m:
                continue
            ts = _parse_log_ts(line)
            if ts is None:
                continue
            label = m.group("label")
            symbol = m.group("symbol")

            coid = None
            for j in range(i + 1, min(i + 5, len(lines))):
                cm = ENQUEUED_ORDER_RE.search(lines[j])
                if cm:
                    coid = cm.group("coid")
                    break
            if coid is None:
                continue

            fill_price = None
            for j in range(i + 1, min(i + 500, len(lines))):
                if coid not in lines[j]:
                    continue
                tm = TRADE_RECEIVED_RE.search(lines[j])
                if not tm:
                    continue
                try:
                    parsed = ast.literal_eval(tm.group(1))
                except (ValueError, SyntaxError):
                    continue
                orsp = parsed.get("order_response", {})
                if orsp.get("client_order_id") != coid:
                    continue
                if orsp.get("status") == "FILLED":
                    fill_price = Decimal(str(orsp["price"]))
                    break

            if fill_price is not None:
                fills.append(
                    BboCloseFill(
                        ts=ts, label=label, symbol=symbol, fill_price=fill_price
                    )
                )

    fills.sort(key=lambda f: f.ts)
    return fills


def tag_trips_by_mechanism(
    trips: List[Trip], bbo_fills: List[BboCloseFill]
) -> dict[int, str]:
    """
    Returns {index into trips list -> "BBO" | "NATIVE"}.

    A trip is tagged BBO if some bbo_fills entry matches its exit on
    symbol + timestamp proximity + price proximity, all three — price
    proximity specifically guards against matching the WRONG BBO event
    when several close together in time (e.g. rapid consecutive trips),
    same reasoning as join_triggers_with_trips.py's ambiguity check.
    Otherwise NATIVE.
    """
    tags: dict[int, str] = {}
    used_fill_indices: set[int] = set()

    for idx, trip in enumerate(trips):
        exit_ts_unix = trip.exit_trade.order_timestamp
        exit_dt = datetime.fromtimestamp(exit_ts_unix, tz=timezone.utc)

        best_j = None
        best_dt_diff = None
        for j, fill in enumerate(bbo_fills):
            if j in used_fill_indices:
                continue
            if fill.symbol != trip.symbol:
                continue
            dt_diff = abs((fill.ts - exit_dt).total_seconds())
            if dt_diff > MATCH_TOLERANCE_SECONDS:
                continue
            price_diff = abs(fill.fill_price - trip.exit_price)
            if price_diff > PRICE_TOLERANCE:
                continue
            if best_dt_diff is None or dt_diff < best_dt_diff:
                best_dt_diff = dt_diff
                best_j = j

        if best_j is not None:
            tags[idx] = "BBO"
            used_fill_indices.add(best_j)
        else:
            tags[idx] = "NATIVE"

    return tags


def summarize_by_mechanism(trips: List[Trip], tags: dict[int, str]) -> None:
    for mechanism in ("BBO", "NATIVE"):
        subset = [trip for idx, trip in enumerate(trips) if tags.get(idx) == mechanism]
        n = len(subset)
        if n == 0:
            print(f"{mechanism}: no trips")
            continue
        wins = [t for t in subset if t.is_win]
        win_rate = len(wins) / n
        avg_win = (
            (sum((t.realized_pnl for t in wins), Decimal(0)) / len(wins))
            if wins
            else Decimal(0)
        )
        losses = [t for t in subset if not t.is_win]
        avg_loss = (
            (sum((t.realized_pnl for t in losses), Decimal(0)) / len(losses))
            if losses
            else Decimal(0)
        )
        payoff = (avg_win / abs(avg_loss)) if avg_loss != 0 else None
        total_pnl = sum((t.realized_pnl for t in subset), Decimal(0))
        print(
            f"{mechanism}: n={n}  win_rate={win_rate:.4f} ({len(wins)}/{len(subset) - len(wins)})  "
            f"payoff_ratio={payoff}  total_pnl={total_pnl}"
        )


if __name__ == "__main__":
    PUBLIC_KEY = os.environ["QFEX_PUBLIC_KEY"]
    SECRET_KEY = os.environ["QFEX_SECRET_KEY"]

    # enter your own values
    SYMBOL = "GOLD-USD"
    START_ISO = "2026-10-05T23:47:42.835075Z"
    END_ISO = "2026-10-06T23:59:59Z"

    LOG_PATHS = [
        # "../logs/mm_20260901_095013.log",
        # "../logs/mm_20260901_095013.log.2026-09-01",
        # "../logs/mm_20260901_095013.log.2026-09-02",
        # "../logs/mm_20260901_095013.log.2026-09-03",
        # "../logs/mm_20260904_225303.log",
        # "../logs/mm_20260904_225303.log.2026-09-04",
        # "../logs/mm_20260904_225303.log.2026-09-05",
        # "../logs/mm_20260907_091021.log",
        # "../logs/mm_20260907_091021.log.2026-09-07",
        # "../logs/mm_20260907_091021.log.2026-09-08",
        # "../logs/mm_20260907_091021.log.2026-09-09",
        # "../logs/mm_20260907_091021.log.2026-09-10",
        # "../logs/mm_20260907_091021.log.2026-09-11",
        # "../logs/mm_20260907_091021.log.2026-09-12",
        # "../logs/mm_20260907_091021.log.2026-09-13",
        # "../logs/mm_20260907_091021.log.2026-09-14",
        # "../logs/mm_20260907_091021.log.2026-09-15",
        # "../logs/mm_20260907_091021.log.2026-09-16",
        # "../logs/mm_20260907_091021.log.2026-09-17",
        # "../logs/mm_20260907_091021.log.2026-09-18",
        # "../logs/mm_20260919_140942.log.2026-09-19",
        # "../logs/mm_20260919_140942.log.2026-09-20",
        # "../logs/mm_20260919_140942.log.2026-09-21",
        # "../logs/mm_20260919_140942.log.2026-09-22",
        # "../logs/mm_20260919_140942.log.2026-09-23",
        # "../logs/mm_20260924_120032.log",
        # "../logs/mm_20260924_120032.log.2026-09-24",
        # "../logs/mm_20260924_120032.log.2026-09-25",
        # "../logs/mm_20260926_130104.log",
        # "../logs/mm_20260926_130104.log.2026-09-26",
        # "../logs/mm_20260926_130104.log.2026-09-27",
        # "../logs/mm_20260926_130104.log.2026-09-28",
        # "../logs/mm_20260929_131332.log",
        # "../logs/mm_20260929_131332.log.2026-09-29",
        # "../logs/mm_20260929_131332.log.2026-09-30",
        # "../logs/mm_20260929_131332.log.2026-10-01",
        # "../logs/mm_20261002_121335.log",
        # "../logs/mm_20261002_121335.log.2026-10-02",
        "../logs/mm_20261003_094544.log",
        "../logs/mm_20261003_094544.log.2026-10-03",
        "../logs/mm_20261003_094544.log.2026-10-04",
        "../logs/mm_20261003_094544.log.2026-10-05",
        "../logs/mm_20261006_122450.log",
        "../logs/mm_20261006_122450.log.2026-10-06",
    ]

    trades = fetch_all_trades(PUBLIC_KEY, SECRET_KEY, SYMBOL, START_ISO, END_ISO)
    trips = reconstruct_trips(trades)
    print(f"\nReconstructed {len(trips)} trips")

    bbo_fills = extract_bbo_close_fills(LOG_PATHS)
    print(f"Extracted {len(bbo_fills)} BBO-triggered close fills from logs\n")

    tags = tag_trips_by_mechanism(trips, bbo_fills)
    n_bbo = sum(1 for v in tags.values() if v == "BBO")
    n_native = sum(1 for v in tags.values() if v == "NATIVE")
    print(
        f"Tagged: {n_bbo} BBO-closed, {n_native} native-closed "
        f"(out of {len(trips)} total trips)\n"
    )

    summarize_by_mechanism(trips, tags)
