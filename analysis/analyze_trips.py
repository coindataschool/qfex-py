"""
Combines fetch_and_reconstruct_trips.py and join_triggers_with_trips.py
into one run: fetches trades once, reconstructs trips once, then prints
both the trip-level metrics (win rate, realized payoff ratio) AND the
trigger-join slippage breakdown (SL vs TP, by matched label) together —
rather than running two separate scripts that each redundantly
fetch/reconstruct the same trades.

Requires trigger_log.jsonl to already exist for the window you're
analyzing (see TriggerLogger / perpetual_maker.py's --log-file-derived
trigger log path) — the slippage section will simply show 0 matched
trips if it doesn't, same as join_triggers_with_trips.py does standalone.
"""

import os

from fetch_and_reconstruct_trips import (
    fetch_all_trades,
    reconstruct_trips,
    summarize_trips,
)
from join_triggers_with_trips import join_trips_with_triggers, load_triggers, summarize

if __name__ == "__main__":
    PUBLIC_KEY = os.environ["QFEX_PUBLIC_KEY"]
    SECRET_KEY = os.environ["QFEX_SECRET_KEY"]

    # Enter your own values
    SYMBOL = "GOLD-USD"
    START_ISO = "2026-10-05T23:47:42.835075Z"
    END_ISO = "2026-10-06T23:59:59Z"

    TRIGGER_LOG_PATHS = [
        # "../logs/triggers_20260901_095013.jsonl",
        # "../logs/triggers_20260904_225303.jsonl",
        # "../logs/triggers_20260907_091021.jsonl",
        # "../logs/triggers_20260919_140942.jsonl",
        # "../logs/triggers_20260924_120032.jsonl",
        # "../logs/triggers_20260926_130104.jsonl",
        # "../logs/triggers_20260929_131332.jsonl",
        "../logs/triggers_20261003_094544.jsonl",
        "../logs/triggers_20261006_122450.jsonl",
    ]

    trades = fetch_all_trades(PUBLIC_KEY, SECRET_KEY, SYMBOL, START_ISO, END_ISO)
    print(f"Fetched {len(trades)} trades for {SYMBOL}")

    trips = reconstruct_trips(trades)

    print("\n=== Trip metrics ===")
    summarize_trips(trips)

    triggers = load_triggers(TRIGGER_LOG_PATHS)
    joined = join_trips_with_triggers(trips, triggers)

    print("\n=== Trigger-join slippage ===")
    summarize(joined)
