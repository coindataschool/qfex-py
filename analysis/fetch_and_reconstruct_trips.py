"""
Downloads historical fills from QFEX's /user/trade REST endpoint and
reconstructs entry->exit trips, tagging each exit as a win (favorable
vs entry) or loss (adverse vs entry) using PRICE, not order_type.

Why price-based, not order_type-based: QFEX's enum docs show a triggered
stop order's resulting execution can appear as order_type=STOP_MARKET
regardless of whether the original staged order was TAKE_PROFIT or
STOP_LOSS (see docs.qfex.com/api-reference/enums, OrderType table) — so
order_type on the *executed* trade may not distinguish TP from SL exits
reliably. Price direction relative to entry does, unambiguously, and
doesn't depend on an unconfirmed collapse behavior.

The raw order_type IS kept in the output for a free, zero-cost
cross-check — if it turns out QFEX does preserve TAKE_PROFIT/STOP_LOSS on
the trade record, any mismatch against the price-based tag is worth a
look; if it's uniformly STOP_MARKET, this column will just say so and
can be ignored.

Auth scheme matches base_client.py's _build_auth_message exactly:
    signature = HMAC_SHA256(secret_key, f"{nonce}:{unix_ts}")
REST host assumed api.{domain} per base_client.py's _rest_url() (PROD
confirmed as api.qfex.com by QFEX's docs; UAT is an unverified guess
there too — same caveat applies here).
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

import requests  # not in qfex2's existing deps (which use stdlib urllib

# for fetch_refdata) — this is a standalone offline
# analysis script, not part of the live client, so
# pulling in requests here doesn't touch base_client.py


@dataclass
class Trade:
    id: str
    order_timestamp: float
    symbol: str
    price: Decimal
    quantity: Decimal
    side: str  # "BUY" or "SELL" per docs — NOT yet confirmed against a
    # real response; if this assumption is wrong the fix is
    # local to _parse_trade below
    fee: Decimal
    order_type: str  # kept for the cross-check described in the module
    # docstring; not used for win/loss tagging
    order_id: str
    realised_pnl_change: Decimal


def _build_auth_headers(public_key: str, secret_key: str) -> dict:
    """Mirrors base_client.py's _build_auth_message, adapted for REST
    headers per docs.qfex.com's PublicKey securityScheme (x-qfex-* header
    names) rather than the websocket auth message's nested JSON shape."""
    nonce = secrets.token_hex(16)
    unix_ts = int(time.time())
    payload = f"{nonce}:{unix_ts}".encode("utf-8")
    signature = hmac.new(
        secret_key.encode("utf-8"), payload, hashlib.sha256
    ).hexdigest()
    return {
        "x-qfex-public-key": public_key,
        "x-qfex-nonce": nonce,
        "x-qfex-timestamp": str(unix_ts),
        "x-qfex-hmac-signature": signature,
    }


def _parse_trade(raw: dict) -> Trade:
    return Trade(
        id=raw["id"],
        order_timestamp=float(raw["order_timestamp"]),
        symbol=raw["symbol"],
        price=Decimal(str(raw["price"])),
        quantity=Decimal(str(raw["quantity"])),
        side=raw["side"],
        fee=Decimal(str(raw["fee"])),
        order_type=raw.get("order_type", ""),
        order_id=raw.get("order_id", ""),
        realised_pnl_change=Decimal(str(raw.get("realised_pnl_change", "0"))),
    )


def fetch_all_trades(
    public_key: str,
    secret_key: str,
    symbol: str,
    start_iso: str,
    end_iso: str,
    rest_host: str = "https://api.qfex.com",
) -> List[Trade]:
    """
    Paginates through GET /user/trade (max 1000/page per the OpenAPI
    schema's `limit` field) until a short page signals no more data.
    New HMAC headers are generated per request — the nonce/timestamp
    pair is single-use by design (see docs.qfex.com's Nonce
    securityScheme: "must be unique within a 15 minute window"), so
    this cannot be hoisted out of the loop.

    Deduplicates by Trade.id before returning. Confirmed from real data:
    the exact same trade id can appear twice in the combined page
    results (offset-based pagination is vulnerable to this — if a new
    trade lands between two page fetches, the shifting total can cause
    the same record to land on two consecutive pages). Same id twice
    means the same underlying trade record fetched twice, not two real
    fills that happen to coincide — dropped here, at the source, so
    reconstruct_trips and everything downstream never sees them. Flagged
    to stderr-equivalent (print) rather than silently dropped, since a
    LARGE number of duplicates would suggest a bigger pagination problem
    worth knowing about, not just one-off overlap.
    """
    trades: List[Trade] = []
    offset = 0
    limit = 1000
    while True:
        headers = _build_auth_headers(public_key, secret_key)
        params = {
            "symbol": symbol,
            "start": start_iso,
            "end": end_iso,
            "limit": limit,
            "offset": offset,
        }
        resp = requests.get(
            f"{rest_host}/user/trade", headers=headers, params=params, timeout=30
        )
        resp.raise_for_status()
        body = resp.json()
        page = body.get("data") or []
        trades.extend(_parse_trade(t) for t in page)
        if len(page) < limit:
            break
        offset += limit

    seen_ids: dict[str, Trade] = {}
    duplicates: List[Trade] = []
    deduped: List[Trade] = []
    for t in trades:
        if t.id in seen_ids:
            duplicates.append(t)
            continue
        seen_ids[t.id] = t
        deduped.append(t)

    if duplicates:
        print(
            f"WARNING: {len(duplicates)} duplicate trade record(s) dropped "
            f"(same id fetched more than once — likely pagination overlap, "
            f"see fetch_all_trades docstring):"
        )
        for t in duplicates:
            print(
                f"  id={t.id} {t.symbol} {t.side} @ {t.price} qty={t.quantity} "
                f"ts={datetime.fromtimestamp(t.order_timestamp, tz=timezone.utc)}"
            )

    deduped.sort(key=lambda t: t.order_timestamp)
    return deduped


# ---------------------------------------------------------------------
# Trip reconstruction
# ---------------------------------------------------------------------


@dataclass
class Trip:
    symbol: str
    entry_trades: List[Trade]  # one or more same-side trades whose
    # quantities sum to the position size —
    # more than one when the entry itself
    # partial-fills across multiple trade
    # records (same order_id, or a rapid
    # sequence of matches against the book)
    exit_trades: List[Trade]  # same idea for the exit — confirmed real
    # via a live example: a single STOP_MARKET
    # trigger reported as two SELL trade
    # records (0.1 + 0.4 = 0.5) a few
    # microseconds apart, both same order_type/
    # time_in_force, summing exactly to the
    # entry's quantity
    entry_price: Decimal  # quantity-weighted average across entry_trades
    exit_price: Decimal  # quantity-weighted average across exit_trades
    quantity: Decimal  # total size of the trip (sum of either leg's
    # quantities — they should match; see
    # reconstruct_trips for what happens if they don't)
    direction: int  # +1 long, -1 short (from first entry trade's side)
    pnl_price_move: Decimal  # (exit_price - entry_price) * direction,
    # weighted-average price units, BEFORE fees
    realized_pnl: Decimal  # sum of realised_pnl_change across ALL trades
    # in both legs — QFEX's own number, ground truth
    is_win: bool  # price-based: pnl_price_move > 0
    exit_order_types: List[str]  # raw, one per exit trade, kept for the
    # free cross-check only (see module
    # docstring) — a list now since a
    # partial-fill group can mix types in
    # principle, though in practice we'd
    # expect them identical within one group

    @property
    def entry_trade(self) -> Trade:
        """Back-compat accessor for callers written against the old
        single-trade shape (e.g. join_triggers_with_trips.py's use of
        trip.exit_trade.order_timestamp/.price) — returns the LAST trade
        in the group chronologically, since that's the one whose
        order_timestamp is closest to when the position was actually
        fully established. Prefer entry_trades directly in new code."""
        return self.entry_trades[-1]

    @property
    def exit_trade(self) -> Trade:
        """Same idea as entry_trade above, for the exit side — returns
        the LAST trade in the exit group, i.e. the one whose timestamp
        marks when the position was actually fully closed. This is what
        join_triggers_with_trips.py's nearest-prior-arm-time matching
        should key off, not the first partial fill."""
        return self.exit_trades[-1]

    @property
    def exit_order_type(self) -> str:
        """Back-compat: the exit_order_types cross-check, collapsed to
        one value for old callers. If the group ever contains more than
        one distinct type, this returns the first and callers checking
        the raw exit_order_types list will see the full picture — this
        property is a convenience, not the source of truth."""
        return self.exit_order_types[0] if self.exit_order_types else ""


def reconstruct_trips(trades: List[Trade]) -> List[Trip]:
    """
    Segments trades using order_type == "ALO" as the entry marker,
    rather than inferring entry-vs-exit from side-alternation or from
    grouping consecutive same-side trades by quantity (both tried
    first; both proved fragile against real data — see below).

    Why ALO is the right marker: PerpetualMarketMakingStrategy is
    DESIGNED so that create_base_proposal's resting ALO quote is the
    only way a position opens — every other order type (STOP_MARKET,
    MARKET, STOP_LOSS/TAKE_PROFIT triggers, BBO-triggered reduce_only
    closes) is only ever supposed to fire as part of managing/closing an
    already-open position. In the NORMAL case this makes "is this trade
    the start of a new trip" unambiguous per-trade: order_type == "ALO".

    This assumption is not absolute, though — confirmed live (2026-08-11,
    CL-USD) that a race between _maybe_bbo_triggered_close's reduce_only
    MARKET close and an independently-resolving native STOP_LOSS/
    TAKE_PROFIT order can both fire within the same ~100ms window,
    overshooting flat into a brief unintended opposite-side position.
    When that happens, a non-ALO trade CAN appear exactly where a new
    trip's entry would be expected. This function does not attempt to
    reconstruct what happened in that case — it can't be modeled as an
    ordinary trip — it detects and loudly flags it instead (see the
    "POSSIBLE BBO-vs-NATIVE RACE DETECTED" print in the segmentation
    loop below) so it surfaces immediately rather than silently
    corrupting a trip's boundaries or getting lost among routine
    data-quality anomalies (partial fills, pagination dupes, etc.).

    Why the earlier side-alternation-grouping approach was wrong: it
    assumed the only reason to see 2+ consecutive same-side trades was
    a partial fill of ONE order, and merged them into a single group
    accordingly. But this strategy can also produce consecutive
    same-side trades from genuinely DIFFERENT events on the exit side —
    e.g. a BBO-triggered reduce_only MARKET close landing at nearly the
    same moment as the native stop's own phantom-FILLED resolution, both
    same side, both real records for the same trip's exit. Grouping by
    side alone conflated these with true partial fills, which then
    tripped the quantity-mismatch check and got flagged — explaining a
    spike in flagged trades rather than a reduction. Segmenting by ALO
    entry markers sidesteps this entirely: it doesn't matter how many
    exit-side trades there are or why, they're all just "everything
    between this ALO and the next ALO."

    Race condition — both sides of the resting quote fill before the
    counterpart cancel lands: confirmed from real data (two ALO trades,
    opposite sides, well under a second apart — e.g. BUY ALO @ 80.91
    immediately followed by SELL ALO @ 80.85, 0.85s later). Structurally
    this can ONLY be "open then immediately close" for one trip, never
    two separate trips — a genuine new trip's ALO entry can only occur
    after the strategy goes flat again, which requires the PREVIOUS
    position to have actually closed first.

    Implementation: rather than pre-computing ALO "segment boundaries"
    and then splitting each segment (an earlier version of this
    function did that, and got the boundary rule wrong — see below),
    this walks trades in order maintaining a RUNNING SIGNED POSITION
    (starts at 0, BUY adds, SELL subtracts). A trip begins the moment
    position moves away from 0, and ends the instant it returns to 0.
    Every trade encountered while position != 0 belongs to the
    in-progress trip; trades are further bucketed into that trip's
    "entry" vs "exit" list by whether they're same-side as the trip's
    OPENING trade or not. This handles the race condition naturally: a
    same-side ALO right after the opening ALO extends the entry (partial
    fill); an OPPOSITE-side ALO right after it drives position back to
    0, ending the trip via the exit bucket — no special-casing needed,
    it falls out of tracking quantity directly rather than guessing from
    adjacency patterns.

    Why not the earlier "any ALO adjacent to another ALO is not a new
    segment start" rule: that broke when a race-exit ALO was itself
    immediately followed by a THIRD, genuinely new entry ALO (a fast
    re-quote) — the old rule swallowed the real new entry into the
    previous trip's segment, since it only looked at "is the previous
    trade also ALO," not at whether the position had actually returned
    to flat in between. Running-position tracking doesn't have this
    failure mode, since it's driven by actual quantity, not by pattern-
    matching adjacent order_types.

    If a mismatch ever occurs (e.g. an exit trade's cumulative quantity
    would overshoot 0 rather than land exactly on it), that's a genuine
    anomaly — flagged rather than silently building a Trip with
    inconsistent size.

    Non-ALO trades appearing BEFORE the first ALO in the window (e.g. a
    position still being managed from before the query's start date)
    are skipped with a warning — there's no entry data for them in this
    window, so no Trip can be built.

    Defensively drops duplicate-id trades on entry, even though
    fetch_all_trades already dedupes at the source — this function is
    public and could be called with a trade list built some other way
    (tests, a different fetch path), and silently double-counting a
    duplicated fill into a trip's quantity/PnL would be a worse failure
    mode than a redundant check costs here.
    """
    trips: List[Trip] = []
    skipped_groups: List[List[Trade]] = []
    race_detected_ids: set[str] = set()  # trade ids already printed by the
                                           # "POSSIBLE BBO-vs-NATIVE RACE
                                           # DETECTED" block below — kept
                                           # out of the generic end-of-
                                           # function warning printout so
                                           # they're not shown twice, while
                                           # still counted in
                                           # skipped_groups's totals

    seen_ids: dict[str, Trade] = {}
    duplicates: List[Trade] = []
    trades_deduped: List[Trade] = []
    for t in trades:
        if t.id in seen_ids:
            duplicates.append(t)
            continue
        seen_ids[t.id] = t
        trades_deduped.append(t)
    if duplicates:
        print(
            f"WARNING: {len(duplicates)} duplicate-id trade(s) dropped "
            f"inside reconstruct_trips (should normally be caught earlier "
            f"by fetch_all_trades — seeing this here means trades were "
            f"supplied from elsewhere with duplicates already present):"
        )
        for t in duplicates:
            print(f"  id={t.id} {t.symbol} {t.side} @ {t.price} qty={t.quantity}")

    by_symbol: dict[str, List[Trade]] = {}
    for t in trades_deduped:
        by_symbol.setdefault(t.symbol, []).append(t)

    for symbol, sym_trades in by_symbol.items():
        n = len(sym_trades)

        # Skip any leading run of non-ALO trades before the first ALO —
        # no entry data for them in this window.
        first_alo = next(
            (idx for idx, t in enumerate(sym_trades) if t.order_type == "ALO"), None
        )
        if first_alo is None:
            if sym_trades:
                skipped_groups.append(sym_trades)
            continue
        if first_alo > 0:
            skipped_groups.append(sym_trades[:first_alo])

        i = first_alo
        while i < n:
            if sym_trades[i].order_type != "ALO":
                # CONFIRMED REAL BUG SIGNATURE (2026-08-11, CL-USD,
                # traced against the live log): a non-ALO trade landing
                # exactly where a new trip's entry is expected means the
                # position was NOT actually flat here — a BBO-triggered
                # reduce_only close and the native STOP_LOSS/TAKE_PROFIT
                # order independently resolved within the same ~100ms
                # window (see _maybe_bbo_triggered_close's fire-first-
                # cancel-after design), both filling and overshooting
                # flat into an unintended opposite-side position. The
                # "MARKET"/"STOP_MARKET" trade that then appears here
                # isn't a new trip's entry at all — it's a THIRD fill
                # from that same race, closing out the accidental
                # exposure. This is NOT an ordinary data-quality anomaly
                # (partial fill, pagination dupe, etc.) — it's evidence
                # of a live position-sizing bug that briefly held
                # unintended directional exposure. Flagged separately
                # and loudly from the generic skipped_groups warning
                # below so it can't be missed among routine anomalies —
                # if this fires, go read the log around this timestamp
                # (converted to the bot's actual local timezone) before
                # trusting anything about this window's trip count or
                # PnL.
                j = i
                while j < n and sym_trades[j].order_type != "ALO":
                    j += 1
                non_alo_group = sym_trades[i:j]
                print(
                    f"\n*** POSSIBLE BBO-vs-NATIVE RACE DETECTED *** "
                    f"({len(non_alo_group)} non-ALO trade(s) found where a "
                    f"new trip's ALO entry was expected — see "
                    f"reconstruct_trips docstring/comment for the confirmed "
                    f"2026-08-11 incident this matches):"
                )
                for t in non_alo_group:
                    print(
                        f"    id={t.id} {t.symbol} {t.side} {t.order_type} "
                        f"@ {t.price} qty={t.quantity} "
                        f"ts={datetime.fromtimestamp(t.order_timestamp, tz=timezone.utc)}"
                    )
                    race_detected_ids.add(t.id)
                skipped_groups.append(non_alo_group)
                i = j
                continue

            entry_side = sym_trades[i].side
            signed_qty = (
                sym_trades[i].quantity
                if entry_side == "BUY"
                else -sym_trades[i].quantity
            )
            entry_group = [sym_trades[i]]
            exit_group: List[Trade] = []

            k = i + 1
            anomaly = False
            while signed_qty != 0:
                if k >= n:
                    # Ran out of trades with position still open — not
                    # closed within this window.
                    anomaly = True
                    break
                t = sym_trades[k]
                delta = t.quantity if t.side == "BUY" else -t.quantity
                new_signed_qty = signed_qty + delta
                if (new_signed_qty > 0) != (signed_qty > 0) and new_signed_qty != 0:
                    # This trade would flip position through zero and
                    # past it in one step (overshoot) rather than landing
                    # exactly on flat — a genuine size mismatch, not an
                    # ordinary partial fill.
                    anomaly = True
                    break
                if t.side == entry_side:
                    entry_group.append(t)
                else:
                    exit_group.append(t)
                signed_qty = new_signed_qty
                k += 1

            if anomaly or not exit_group:
                # Either a genuine mismatch, or the position never
                # returned to flat within this window (trailing open
                # position) — flag rather than guess.
                skipped_groups.append(entry_group + exit_group)
                i = k if k > i else i + 1  # always advance to avoid
                # an infinite loop
                continue

            entry_qty = sum((t.quantity for t in entry_group), Decimal(0))
            exit_qty = sum((t.quantity for t in exit_group), Decimal(0))
            direction = 1 if entry_side == "BUY" else -1

            entry_price = (
                sum((t.price * t.quantity for t in entry_group), Decimal(0)) / entry_qty
            )
            exit_price = (
                sum((t.price * t.quantity for t in exit_group), Decimal(0)) / exit_qty
            )

            price_move = (exit_price - entry_price) * direction
            realized = sum(
                (t.realised_pnl_change for t in entry_group + exit_group),
                Decimal(0),
            )

            trips.append(
                Trip(
                    symbol=symbol,
                    entry_trades=entry_group,
                    exit_trades=exit_group,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    quantity=entry_qty,
                    direction=direction,
                    pnl_price_move=price_move,
                    realized_pnl=realized,
                    is_win=price_move > 0,
                    exit_order_types=[t.order_type for t in exit_group],
                )
            )
            i = k

    non_race_groups = [
        g for g in skipped_groups if not all(t.id in race_detected_ids for t in g)
    ]
    if non_race_groups:
        n_trades = sum(len(g) for g in non_race_groups)
        print(
            f"WARNING: {len(non_race_groups)} group(s) totaling {n_trades} "
            f"trade(s) skipped — leading trades with no ALO entry in "
            f"this window, a trailing open position with no exit yet, "
            f"or an entry/exit quantity mismatch. Inspect these before "
            f"trusting the trip count:"
        )
        for group in non_race_groups:
            for t in group:
                print(
                    f"  {t.id} {t.symbol} {t.side} {t.order_type} @ {t.price} "
                    f"qty={t.quantity} "
                    f"ts={datetime.fromtimestamp(t.order_timestamp, tz=timezone.utc)}"
                )

    return trips


# ---------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------


def summarize_trips(trips: List[Trip]) -> None:
    if not trips:
        print("No trips reconstructed.")
        return

    n = len(trips)
    wins = [t for t in trips if t.is_win]
    losses = [t for t in trips if not t.is_win]
    win_rate = len(wins) / n

    avg_win = (
        sum((t.realized_pnl for t in wins), Decimal(0)) / len(wins)
        if wins
        else Decimal(0)
    )
    avg_loss = (
        sum((t.realized_pnl for t in losses), Decimal(0)) / len(losses)
        if losses
        else Decimal(0)
    )
    # payoff ratio on REALIZED dollars (post-fee, post-slippage) — not
    # the configured TP/SL spread ratio. abs() since avg_loss is negative.
    realized_payoff_ratio = (avg_win / abs(avg_loss)) if avg_loss != 0 else None

    total_realized = sum((t.realized_pnl for t in trips), Decimal(0))

    exit_order_types = {}
    for t in trips:
        for ot in t.exit_order_types:
            exit_order_types[ot] = exit_order_types.get(ot, 0) + 1

    n_partial_entry = sum(1 for t in trips if len(t.entry_trades) > 1)
    n_partial_exit = sum(1 for t in trips if len(t.exit_trades) > 1)

    print(f"Trips reconstructed: {n}")
    print(
        f"Realized win rate:   {win_rate:.4f}  ({len(wins)} wins / {len(losses)} losses)"
    )
    print(f"Avg realized PnL per win:  {avg_win}")
    print(f"Avg realized PnL per loss: {avg_loss}")
    print(f"Realized payoff ratio (|avg win| / |avg loss|): {realized_payoff_ratio}")
    print(f"Total realized PnL across all trips: {total_realized}")
    print(f"Trips with a partial-fill entry (>1 trade): {n_partial_entry}")
    print(f"Trips with a partial-fill exit (>1 trade):  {n_partial_exit}")
    print(
        f"Exit order_type distribution across all exit trades "
        f"(cross-check only, not used for tagging): {exit_order_types}"
    )


if __name__ == "__main__":
    # Fill these in, or wire to config.py's QFEXConfig if preferred —
    # left as plain locals so this stays a standalone, side analysis
    # script per your request, not wired into the live client.
    PUBLIC_KEY = os.environ["QFEX_PUBLIC_KEY"]
    SECRET_KEY = os.environ["QFEX_SECRET_KEY"]
    SYMBOL = "CL-USD"
    START_ISO = "2026-08-07T23:47:35.104672Z"
    END_ISO = "2026-08-08T23:59:59Z"

    trades = fetch_all_trades(PUBLIC_KEY, SECRET_KEY, SYMBOL, START_ISO, END_ISO)
    print(f"Fetched {len(trades)} trades for {SYMBOL}")

    trips = reconstruct_trips(trades)
    summarize_trips(trips)
