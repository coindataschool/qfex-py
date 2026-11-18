from dataclasses import dataclass
from decimal import Decimal
from typing import List


@dataclass(frozen=True)
class QFEXConfig:
    is_prod: bool
    symbol_list: List[str]
    public_key: str
    secret_key: str

    # Reliability knobs
    connect_timeout_s: float = 10.0
    heartbeat_interval_s: float = 15.0
    heartbeat_timeout_s: float = 10.0
    reconnect_base_delay_s: float = 0.5
    reconnect_max_delay_s: float = 15.0
    # websockets.connect()'s close_timeout for both MDS and trade — how
    # long to wait for the server's close handshake before giving up.
    # Shared value for both (unlike max_queue below, which differs) —
    # no observed reason either needs its own budget.
    close_timeout_s: float = 2.0

    # Order handling
    order_update_timeout_s: float = 5.0  # IOC should resolve quickly; tune if needed
    # Timeout for fetch_refdata()'s REST call — a plain HTTP request, not
    # a websocket message, with its own latency profile, so it doesn't
    # reuse connect_timeout_s (which governs the websocket handshake).
    refdata_fetch_timeout_s: float = 10.0
    # Margin added to order_update_timeout_s to derive is_order_
    # outstanding()'s backstop (base_client.py) — past this many seconds
    # since send, an order is treated as no longer outstanding even
    # without a terminal response, so a lost response doesn't block
    # retries forever. Confirmed live (2026-07-27, CL-USD): a locally-
    # timed-out STOP_LOSS/TAKE_PROFIT's real ACK arrived up to ~6.5s
    # after order_update_timeout_s fired — this default has headroom
    # above that. Widen it the same way mark_price/underlier_price_
    # deviation_threshold were widened, if a longer tail is ever observed.
    order_outstanding_backstop_margin_s: float = 10.0

    # Price-feed circuit breaker thresholds — see is_mark_price_broken/
    # is_underlier_price_broken in base_client.py. Correctness depends on
    # the specific symbol's typical mark_price/underlier_price behavior,
    # not on anything intrinsic to this client, so these are per-instance
    # config — but deliberately not CLI flags, same as every other
    # reliability knob here: tune by editing this file (or constructing
    # QFEXConfig directly as a library) before running. Defaults are
    # values this client has actually run with in production.
    mark_price_deviation_threshold: Decimal = Decimal("0.01")
    mark_price_deviation_streak_required: int = 10
    underlier_price_deviation_threshold: Decimal = Decimal("0.01")
    underlier_price_deviation_streak_required: int = 10
    # No corresponding *_threshold field — a one-sided/empty BBO is a
    # presence/absence check, not a magnitude one, so there's nothing to
    # set a deviation threshold against; only the confirmation-window
    # streak is meaningful here. See is_one_sided_book_broken in
    # base_client.py.
    one_sided_book_streak_required: int = 5

    # Non-price circuit breaker: consecutive rejected/errored terminal
    # order responses (INVALID_QUANTITY, ServerError, or any other
    # non-ACK/non-FILLED terminal status) for the SAME logical
    # order-management action (tracked by client_order_id — see
    # _order_error_streak in perpetual_market_making.py). Independent of
    # the three price-feed breakers above: those detect a bad MARKET
    # DATA feed, this detects a bad EXCHANGE RESPONSE pattern — QFEX
    # itself repeatedly rejecting or erroring on the same request, which
    # a price-only check can't see (the price feed can look completely
    # normal while this is happening).
    #
    # Added after a live incident (2026-08-13/14, CL-USD): a force-close
    # retry loop got INVALID_QUANTITY 14 times in a row over 65s, then
    # ServerError on cancel_stop_order for another ~7s, before finally
    # resolving — all while the price feed looked fine throughout. The
    # eventual price-feed circuit breaker didn't trip until 13 minutes
    # LATER, on an unrelated position opened after this episode
    # "resolved" and traded normally in the meantime. This breaker exists
    # to catch that earlier, independent signal instead of waiting for a
    # possibly-unrelated price-feed trip afterward.
    #
    # Deliberately a single global counter, NOT scoped per
    # client_order_id: the real incident's retry loop minted a fresh
    # client_order_id every attempt (see _place_one_stop_leg's
    # secrets.token_hex(16) pattern), so a per-id counter would never
    # accumulate past 1 for any single id and would miss the pattern
    # entirely. Tradeoff accepted knowingly: this can in principle trip
    # from a handful of small, genuinely UNRELATED failures on different
    # actions happening to land consecutively, not just one sustained
    # single-cause failure — chosen anyway since it's the only design
    # that's guaranteed to catch the incident this exists for. Revisit
    # if it ever fires on a day with no real underlying problem.
    #
    # order_error_streak_required=3 is deliberately far tighter than the
    # price-feed breakers' streak=10: those are counting BBO ticks
    # (roughly one every ~0.5s in that incident, so streak=10 ≈ 5s of
    # sustained deviation), while THIS streak counts actual QFEX
    # responses to retried requests, which in the same incident arrived
    # roughly every 5s (the retry interval itself) — 3 consecutive
    # failures here is already ~10-15s of a stuck request, a much
    # stronger and rarer signal than a single transient rejection.
    order_error_streak_required: int = 3

    # Retry policy for order-cancellation and emergency-close attempts —
    # strategy-agnostic (bounded-retry-then-give-up isn't specific to any
    # one strategy's tracking layout), so it lives here even though
    # PerpetualMarketMakingStrategy is currently the only consumer.
    #
    # Consecutive cancel_stop_order failures (any cause) tolerated for a
    # single order_id before a strategy gives up, force-clears local
    # tracking, and logs a loud warning that the order's exchange-side
    # state is unconfirmed (see PerpetualMarketMakingStrategy._note_stop_
    # cancel_failure) — followed by one final, uncounted verification
    # attempt after a short delay, logged distinctly, so "unconfirmed"
    # resolves to a real answer more often than not (see
    # _verify_stop_cancel_after_giving_up).
    #
    # Raised from an initial 5 after live data (2026-08-06, CL-USD): 7
    # give-ups occurred in one ~16h run, and 5 of those 7 DID eventually
    # cancel successfully on a later retry (visible via a subsequent
    # order_response, after this strategy had already stopped trying) —
    # meaning 5 retries wasn't always enough to ride out a transient
    # server-side issue (repeated ServerError/"unexpected error" on
    # cancel_stop_order) that could clear up given a few more attempts.
    # The other 2 never got a confirmed cancellation anywhere in that
    # log — this can't fully prevent that outcome, only make it rarer.
    max_stop_cancel_failures: int = 8
    # Delay before the one final, uncounted verification attempt after
    # giving up on cancelling a stop order (see max_stop_cancel_failures'
    # comment and _verify_stop_cancel_after_giving_up) — deliberately
    # longer than the natural spacing between the counted retries (which
    # just happens once per BBO tick, not on any deliberate delay), to
    # give a transient server-side issue more real time to clear before
    # this last check, rather than firing immediately in the same burst
    # that already failed max_stop_cancel_failures times in a row.
    stop_cancel_verify_delay_s: float = 5.0
    # Attempts for the reduce_only MARKET force-close a strategy fires
    # when its price-feed circuit breaker trips — the one action that
    # actually protects capital during that shutdown, so it gets its own
    # retry tolerance rather than sharing order_update_timeout_s's
    # implicit single-attempt behavior. Still bounded, since an unbounded
    # retry here would delay the breaker's hard stop indefinitely if QFEX
    # can't process the close at all.
    mark_price_break_market_close_max_attempts: int = 3

    # Backpressure
    outbound_queue_max: int = 10_000
    # websockets.connect()'s max_queue for each connection — the cap on
    # buffered-but-unprocessed messages before backpressure applies.
    # Two separate fields, not one shared value: trade's default (4096)
    # is larger than MDS's (1024), presumably because a burst of order/
    # balance/position updates runs heavier than a burst of market-data
    # ticks — kept as two fields so that asymmetry stays expressible.
    mds_max_queue: int = 1024
    trade_max_queue: int = 4096
