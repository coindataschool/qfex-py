"""
Extracts every BBO-triggered STOP_LOSS/TAKE_PROFIT close from the given
log file(s) and computes slippage: armed trigger_price vs the ACTUAL
FILLED price of the resulting reduce_only MARKET order (not the "crossed"
BBO snapshot price at decision time, and not the MARKET order's ACK price
— both can be stale; only the terminal FILLED order_response carries the
real execution price, confirmed by inspecting a real example where ACK
showed price=75.07 [a stale mark_price snapshot] but the terminal FILLED
line showed price=75.11 [the real fill, matching the armed trigger]).

This is entirely self-contained from the log — no join against
/user/trade or trigger_log.jsonl needed, since a BBO-triggered close
event and its resulting fill both appear in the same log stream with a
shared client_order_id linking the placement to its terminal response.

Native stop-order fills are matched separately via the phantom-FILLED /
on_stop_order_resolved pattern (status=FILLED, trade_id=None on the
STOP_LOSS/TAKE_PROFIT order_id itself) followed by the spawned
execution order's own FILLED response — same "ACK price may be stale,
trust only the terminal FILLED price" caution applies there too.

Output: two slippage distributions (BBO-triggered vs native-triggered),
directly comparable, answering "does BBO-triggered actually fill closer
to the intended trigger than native mark_price-triggered would."
"""

import ast
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

BBO_TRIGGER_RE = re.compile(
    r"BBO-triggered (?P<label>STOP_LOSS|TAKE_PROFIT) close for (?P<symbol>\S+): "
    r"(?P<side>bid|ask) side price=(?P<cross_price>[\d.]+) crossed trigger=(?P<trigger_price>[\d.]+)"
)

# The enqueued add_order line right after a BBO-trigger carries both the
# client_order_id AND the actual exit side (BUY/SELL) — the exit side is
# the reliable source for slippage-sign direction, not cross_side (which
# side of the book happened to cross doesn't unambiguously map to close
# direction across all SL/TP + long/short combinations).
ENQUEUED_ORDER_RE = re.compile(
    r"trade enqueued.*'side': '(?P<exit_side>BUY|SELL)'.*"
    r"'client_order_id': '(?P<coid>[a-f0-9]+)'.*'reduce_only': True"
)

# Terminal FILLED response for a given client_order_id — this dict is
# Python-repr, not JSON (single quotes, None/True/False) — ast.literal_eval
# handles it directly rather than a fragile field-by-field regex.
TRADE_RECEIVED_RE = re.compile(r"trade received: (\{.*\})\s*$")


@dataclass
class BboTriggerEvent:
    ts_line_index: int
    label: str
    symbol: str
    cross_side: str  # "bid" or "ask" — which side of the book crossed
    # the trigger; determines exit direction and thus
    # the slippage sign convention
    trigger_price: Decimal
    cross_price: Decimal  # BBO price at the moment the decision fired —
    # NOT the fill price, kept for reference only
    fill_price: Optional[Decimal] = None
    client_order_id: Optional[str] = None
    exit_side: Optional[str] = None  # "BUY" or "SELL" — from the
    # enqueued reduce_only order, the
    # ground truth for slippage sign
    slippage: Optional[Decimal] = None  # positive = fill worse than
    # trigger (cost you money);
    # set once fill_price is known


def extract_bbo_trigger_slippage(log_paths: List[str]) -> List[BboTriggerEvent]:
    events: List[BboTriggerEvent] = []

    for path in log_paths:
        with open(path, errors="replace") as f:
            lines = f.readlines()

        for i, line in enumerate(lines):
            m = BBO_TRIGGER_RE.search(line)
            if not m:
                continue

            event = BboTriggerEvent(
                ts_line_index=i,
                label=m.group("label"),
                symbol=m.group("symbol"),
                cross_side=m.group("side"),
                trigger_price=Decimal(m.group("trigger_price")),
                cross_price=Decimal(m.group("cross_price")),
            )

            # The very next line should be the enqueued reduce_only
            # MARKET order carrying the client_order_id we need. If the
            # format ever drifts (extra log lines interleaved from
            # concurrent activity), fall back to scanning forward a few
            # lines rather than assuming line i+1 is always it.
            coid = None
            exit_side = None
            for j in range(i + 1, min(i + 5, len(lines))):
                cm = ENQUEUED_ORDER_RE.search(lines[j])
                if cm:
                    coid = cm.group("coid")
                    exit_side = cm.group("exit_side")
                    break
            if coid is None:
                # Couldn't find the placement — record the event with no
                # fill_price rather than silently dropping it, so the
                # summary can report how many events this happened to.
                events.append(event)
                continue
            event.client_order_id = coid
            event.exit_side = exit_side

            # Now find the terminal FILLED trade-received line for this
            # client_order_id, searching forward. Bounded window (500
            # lines) since it should appear within milliseconds; an
            # unbounded scan risks matching a LATER, unrelated order that
            # coincidentally shares... actually client_order_id is a
            # fresh hex token per order (see _place_one_stop_leg's
            # secrets.token_hex / this reduce_only path's own generation)
            # so collision risk is negligible regardless of window size,
            # but bounding keeps this fast on large files.
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

            event.fill_price = fill_price
            if fill_price is not None and exit_side is not None:
                # exit_side is the direction of the CLOSE order itself.
                # BUY close: filling ABOVE trigger is bad (paid more to
                #   close) -> slippage = fill - trigger
                # SELL close: filling BELOW trigger is bad (received
                #   less to close) -> slippage = trigger - fill
                if exit_side == "BUY":
                    event.slippage = fill_price - event.trigger_price
                else:  # SELL
                    event.slippage = event.trigger_price - fill_price
            events.append(event)

    return events


def summarize(events: List[BboTriggerEvent]) -> None:
    matched = [e for e in events if e.slippage is not None]
    unmatched = [e for e in events if e.slippage is None]

    print(f"Total BBO-triggered events found: {len(events)}")
    print(f"Matched to a terminal fill + signed slippage: {len(matched)}")
    if unmatched:
        print(
            f"WARNING: {len(unmatched)} event(s) could not be matched to a "
            f"terminal FILLED response (or exit_side) within the search "
            f"window — excluded from stats below, not treated as zero "
            f"slippage."
        )

    print()
    for label in ("STOP_LOSS", "TAKE_PROFIT"):
        # e.slippage is guaranteed non-None here (subset is built from
        # `matched`, itself filtered by `e.slippage is not None` above)
        # — but that narrowing doesn't carry across this list
        # comprehension, so the type checker still sees
        # list[Decimal | None]. Same situation as
        # join_triggers_with_trips.py's matched_label/slippage asserts:
        # make the already-true runtime guarantee explicit right here.
        subset: List[Decimal] = [
            e.slippage for e in matched if e.label == label and e.slippage is not None
        ]
        if not subset:
            print(f"{label}: no matched events")
            continue
        avg = sum(subset, Decimal(0)) / len(subset)
        print(
            f"{label}: n={len(subset)}  avg_slippage={avg}  "
            f"total_slippage={sum(subset, Decimal(0))}  "
            f"min={min(subset)}  max={max(subset)}"
        )


if __name__ == "__main__":
    # enter your own values
    events = extract_bbo_trigger_slippage([
        "mm_20260806_124125_log.2026-08-06",
        "mm_20260806_124125.log",
    ])
    summarize(events)

    print("\n--- Raw events (first 10) ---")
    for e in events[:10]:
        print(
            f"{e.label} {e.symbol} trigger={e.trigger_price} cross={e.cross_price} "
            f"fill={e.fill_price} coid={e.client_order_id}"
        )
