"""
Joins trigger_log.jsonl (from TriggerLogger, written live at ACK time in
_place_one_stop_leg) against reconstructed trips (fetch_and_reconstruct_
trips.py, from /user/trade) to get:

  1. CERTAIN SL vs TP tagging per exit — trigger_log.jsonl's `label` is
     known with certainty at placement time; no more inferring from
     price direction, and no more depending on order_type (confirmed
     collapsed to STOP_MARKET on the executed trade regardless of
     origin — see fetch_and_reconstruct_trips.py's docstring).
  2. Real slippage per exit: trigger_price (what was armed) vs
     exit_trade.price (what actually filled). This is the number the
     whole historic-orders detour was trying to reach.

WHY NOT an order_id join: confirmed against base_client.py's
_handle_order_response (step 1b, "phantom FILLED") that when a staged
STOP_LOSS/TAKE_PROFIT triggers, QFEX spawns a SEPARATE order to execute
the trade — the executed trade's order_id (what /user/trade returns) is
NOT the same order_id ACKed when the stop was originally placed (what
trigger_log.jsonl records). No shared id exists across the two sources,
so the join below is inferential: symbol + nearest-prior-arm-time,
cross-checked by price proximity.

MATCHING ALGORITHM, per symbol, chronologically:
  For each Trip's exit_trade (real fill: symbol, price, order_timestamp):
    - Candidate trigger records are ones with the same symbol, ts <=
      exit's order_timestamp, not yet consumed by an earlier trip.
    - Because _place_stop_orders always arms BOTH legs together per
      trip, take the two most-recently-armed trigger records (one
      SL, one TP) at or before this exit's timestamp.
    - Pick whichever of those two has trigger_price closer to
      exit_trade.price — that's almost certainly the one that fired
      (the other leg's trigger_price should be on the far side of
      entry_price, i.e. nowhere near where price actually was at
      exit). This price-proximity check is what protects against a
      pure timing-based mismatch if arm order or clock skew is ever
      off.
    - Mark BOTH of that trip's trigger records (SL and TP) as
      consumed once matched, so a later trip's search starts fresh
      from its own arm event.

This assumes the one-position-at-a-time architecture holds (confirmed
elsewhere: PerpetualMarketMakingStrategy never quotes while
position != 0) — if it's ever violated, matches should be treated with
suspicion; this script flags but does not silently drop unmatched
trips or leftover triggers.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from fetch_and_reconstruct_trips import Trip, fetch_all_trades, reconstruct_trips


@dataclass
class TriggerRecord:
    ts: float
    label: str  # "STOP_LOSS" or "TAKE_PROFIT"
    symbol: str
    exit_side: str
    order_id: Optional[str]
    client_order_id: Optional[str]
    trigger_price: Decimal
    exit_qty: Decimal
    average_price: Decimal
    consumed: bool = False  # set True once matched to a trip, so a
    # later trip's search doesn't re-match it


def load_triggers(paths: list[str]) -> List[TriggerRecord]:
    records = []
    for path in paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                raw = json.loads(line)
                records.append(
                    TriggerRecord(
                        ts=float(raw["ts"]),
                        label=raw["label"],
                        symbol=raw["symbol"],
                        exit_side=raw["exit_side"],
                        order_id=raw.get("order_id"),
                        client_order_id=raw.get("client_order_id"),
                        trigger_price=Decimal(raw["trigger_price"]),
                        exit_qty=Decimal(raw["exit_qty"]),
                        average_price=Decimal(raw["average_price"]),
                    )
                )
    records.sort(key=lambda r: r.ts)
    return records


@dataclass
class JoinedExit:
    trip: Trip
    matched_label: Optional[str]  # "STOP_LOSS" / "TAKE_PROFIT" / None if unmatched
    trigger_price: Optional[Decimal]
    slippage: Optional[
        Decimal
    ]  # (exit_trade.price - trigger_price) * direction_of_exit,
    # positive = cost you money, negative = favorable
    match_quality: (
        str  # "matched" / "no_candidates" / "ambiguous" — see join_trips_with_triggers
    )


def join_trips_with_triggers(
    trips: List[Trip], triggers: List[TriggerRecord]
) -> List[JoinedExit]:
    results: List[JoinedExit] = []

    for trip in trips:
        exit_ts = trip.exit_trade.order_timestamp
        symbol = trip.symbol

        # Candidates: same symbol, armed at or before this exit, not yet
        # consumed by an earlier trip.
        candidates = [
            r
            for r in triggers
            if r.symbol == symbol and r.ts <= exit_ts and not r.consumed
        ]
        if not candidates:
            results.append(JoinedExit(trip, None, None, None, "no_candidates"))
            continue

        # The two most-recently-armed candidates (should be one SL, one
        # TP from the same _place_stop_orders call for this trip) — take
        # the last two by ts, then pick whichever trigger_price is
        # closer to the real exit price.
        candidates.sort(key=lambda r: r.ts)
        recent = candidates[-2:]  # up to 2; could be 1 if e.g. a leg's
        # ACK never landed for this trip

        best = min(recent, key=lambda r: abs(r.trigger_price - trip.exit_trade.price))

        # Sanity check: the OTHER candidate (if any) should be
        # meaningfully farther from exit price than `best` — if they're
        # nearly equidistant, this match is ambiguous and shouldn't be
        # trusted blindly.
        quality = "matched"
        if len(recent) > 1:
            other = recent[0] if best is recent[1] else recent[1]
            best_dist = abs(best.trigger_price - trip.exit_trade.price)
            other_dist = abs(other.trigger_price - trip.exit_trade.price)
            if other_dist > 0 and best_dist / other_dist > Decimal("0.5"):
                quality = "ambiguous"

        best.consumed = True
        for r in recent:
            r.consumed = True  # both legs of this trip are done regardless
            # of which fired — the unfired one will
            # never be matched to a future trip's exit

        direction = trip.direction  # +1 long, -1 short — same convention
        # fetch_and_reconstruct_trips.py
        # already uses for pnl_price_move
        # exit_side is BUY (closing a short) or SELL (closing a long) —
        # slippage sign convention: positive = cost you money relative to
        # the armed trigger, regardless of SL or TP or long or short.
        # For a long (direction=+1), the exit SELLS; slippage against you
        # means filling BELOW trigger_price on a TP (missed some profit)
        # or BELOW trigger_price on an SL (worse loss) — either way,
        # (trigger_price - fill_price) > 0 is bad. For a short mirrored.
        raw_diff = best.trigger_price - trip.exit_trade.price
        slippage = raw_diff * direction

        results.append(
            JoinedExit(
                trip=trip,
                matched_label=best.label,
                trigger_price=best.trigger_price,
                slippage=slippage,
                match_quality=quality,
            )
        )

    return results


def summarize(joined: List[JoinedExit]) -> None:
    matched = [j for j in joined if j.matched_label is not None]
    unmatched = [j for j in joined if j.matched_label is None]
    ambiguous = [j for j in matched if j.match_quality == "ambiguous"]

    print(f"Total trips: {len(joined)}")
    print(
        f"Matched: {len(matched)}  Unmatched: {len(unmatched)}  Ambiguous: {len(ambiguous)}"
    )

    if unmatched:
        print(
            f"\nWARNING: {len(unmatched)} trip(s) had no trigger_log.jsonl "
            f"candidates — likely trips that predate when trigger logging "
            f"started, or a gap in the log. Their slippage is unknown, not "
            f"zero — don't silently treat as zero-slippage in any average."
        )
        for j in unmatched:
            print(
                f"  entry_id={j.trip.entry_trade.id} exit_id={j.trip.exit_trade.id}\n"
                f"      symbol={j.trip.symbol} exit_price={j.trip.exit_trade.price} "
                f"ts={datetime.fromtimestamp(j.trip.exit_trade.order_timestamp, tz=timezone.utc)}"
            )

    if ambiguous:
        print(
            f"\nWARNING: {len(ambiguous)} trip(s) matched a trigger record "
            f"but the two candidate legs were close enough in price that "
            f"the match isn't confident. Inspect these individually before "
            f"trusting their slippage number:"
        )
        for j in ambiguous[:10]:
            print(
                f"  entry_id={j.trip.entry_trade.id} exit_id={j.trip.exit_trade.id}\n"
                f"      exit @ {j.trip.exit_trade.price} matched {j.matched_label} "
                f"trigger={j.trigger_price} ts={datetime.fromtimestamp(j.trip.exit_trade.order_timestamp, tz=timezone.utc)}"
            )

    by_label: dict[str, List[Decimal]] = {}
    for j in matched:
        if j.match_quality != "matched":
            continue  # exclude ambiguous from the clean summary stats
        # matched_label is guaranteed non-None here at runtime (this loop
        # only iterates `matched`, itself filtered by
        # `j.matched_label is not None` above) — but that narrowing
        # happened on a different list comprehension, which the type
        # checker can't see across. Same situation as
        # simple_maker.py's bid_px/ask_px asserts after
        # has_two_sided_book(): make the already-true runtime guarantee
        # visible to the checker right where it's needed.
        assert j.matched_label is not None
        assert j.slippage is not None  # same reasoning as above — both
        # fields are set together in
        # join_trips_with_triggers
        # whenever match_quality is set
        # to "matched"
        by_label.setdefault(j.matched_label, []).append(j.slippage)

    print()
    four_places = Decimal("0.0001")
    for label, slippages in by_label.items():
        avg = sum(slippages, Decimal(0)) / len(slippages)
        total = sum(slippages, Decimal(0))
        print(
            f"{label}: n={len(slippages)}  "
            f"avg_slippage={avg.quantize(four_places)}  "
            f"total_slippage={total.quantize(four_places)}"
        )


if __name__ == "__main__":
    PUBLIC_KEY = os.environ["QFEX_PUBLIC_KEY"]
    SECRET_KEY = os.environ["QFEX_SECRET_KEY"]

    # enter your own values
    SYMBOL = "CL-USD"
    START_ISO = "2026-08-07T23:47:35.104672Z"
    END_ISO = "2026-08-08T23:59:59Z"
    TRIGGER_LOG_PATH = "logs/triggers_20260807_051618.jsonl"

    trades = fetch_all_trades(PUBLIC_KEY, SECRET_KEY, SYMBOL, START_ISO, END_ISO)
    trips = reconstruct_trips(trades)
    triggers = load_triggers(TRIGGER_LOG_PATH)

    joined = join_trips_with_triggers(trips, triggers)
    summarize(joined)
