import argparse
import asyncio
import os
import signal
from datetime import datetime
from decimal import Decimal

from qfex2 import (
    QFEXConfig,
    QFEXTradeError,
    QFEXTraderClient,
    resolve_credentials,
    run_leverage_gate,
    setup_logging,
)
from qfex2.perpetual_market_making import (
    PerpetualMarketMakingConfig,
    PerpetualMarketMakingStrategy,
    drain_until_flat,
)

# Runnable wrapper around PerpetualMarketMakingStrategy (see
# qfex2/perpetual_market_making.py for the actual strategy logic). This
# script's job is just wiring: CLI args -> PerpetualMarketMakingConfig,
# leverage setup, graceful shutdown.
#
# Unlike a one-shot script that places once and never reprices, this runs
# indefinitely: quoting while flat, managing stop-loss/take-profit exits
# once a position opens.


async def main() -> None:
    parser = argparse.ArgumentParser("qfex-perpetual-maker")
    parser.add_argument(
        "--is-prod",
        action="store_true",
        help="use qfex.com (prod). default uses qfex.io (uat)",
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="single symbol to quote, e.g. AAPL-USD",
    )
    parser.add_argument(
        "--order-amount",
        required=True,
        type=Decimal,
        help="order size for both the resting bid and resting ask, and for "
        "the resulting stop-loss/take-profit exit orders",
    )
    parser.add_argument(
        "--bid-spread",
        type=Decimal,
        default=Decimal("0.001"),
        help="fraction below the reference (mid) price to place the bid "
        "(default 0.001 = 0.1%%) — see --underlier-weight "
        "for how the reference price itself is computed",
    )
    parser.add_argument(
        "--ask-spread",
        type=Decimal,
        default=Decimal("0.001"),
        help="fraction above the reference (mid) price to place the ask "
        "(default 0.001 = 0.1%%) — see --underlier-weight "
        "for how the reference price itself is computed",
    )
    parser.add_argument(
        "--underlier-weight",
        type=Decimal,
        default=Decimal("0.8"),
        help="weight on underlier price in the quoting reference price: "
        "mid = underlier_weight*underlier_price + "
        "(1-underlier_weight)*bbo_midpoint (default 0.8). The BBO weight "
        "is always 1-underlier_weight, not independently settable, so "
        "the two always sum to 1. Quoting is paused entirely (not just "
        "falling back to pure BBO midpoint) until at least one underlier "
        "price has been received for this symbol — see "
        "PerpetualMarketMakingStrategy's underlier_price_confirmed gate.",
    )
    parser.add_argument(
        "--order-refresh-tolerance",
        type=Decimal,
        default=Decimal("0.0002"),
        help="fraction the desired quote price must move, relative to the "
        "currently-resting price, before cancel+replace fires (default "
        "0.0002 = 0.02%%). Avoids cancel/replace churn on price moves too "
        "small to matter — see PerpetualMarketMakingStrategy._quotes_are_stale.",
    )
    parser.add_argument(
        "--price-ceiling",
        type=Decimal,
        default=None,
        help="if set, never place a bid above this price (default: no ceiling)",
    )
    parser.add_argument(
        "--price-floor",
        type=Decimal,
        default=None,
        help="if set, never place an ask below this price (default: no floor)",
    )
    parser.add_argument(
        "--stop-loss-spread",
        type=Decimal,
        default=Decimal("0.01"),
        help="fraction beyond entry price, in the adverse direction, to "
        "trigger the stop-loss exit (default 0.01 = 1%%)",
    )
    parser.add_argument(
        "--take-profit-spread",
        type=Decimal,
        default=Decimal("0.01"),
        help="fraction beyond entry price, in the favorable direction, to "
        "trigger the take-profit exit (default 0.01 = 1%%)",
    )
    parser.add_argument(
        "--bbo-stop-loss-confirm-streak",
        type=int,
        default=1,
        help="consecutive BBO ticks the stop-loss trigger must stay "
        "crossed before the BBO-triggered fast close actually fires "
        "(default 1 = fires on the very first crossing tick, i.e. as "
        "fast as possible). Raise this to filter out a brief BBO wick "
        "that crosses the trigger and reverses before a real move is "
        "confirmed, at the cost of reacting slightly slower to a "
        "genuine move. Does NOT apply to take-profit, which always "
        "fires on the first crossing tick regardless of this value — "
        "see PerpetualMarketMakingStrategy._maybe_bbo_triggered_close.",
    )
    # Spread gate. Defaults are read from PerpetualMarketMakingConfig itself
    # (frozen dataclass, so the class attribute is the default) rather than
    # repeated here, so the calibrated values live in exactly one place.
    parser.add_argument(
        "--quote-pause-spread",
        type=Decimal,
        default=Decimal("0.002"),
        help="pause quoting when the BBO spread, as a fraction of mid, stays "
        "above this for --quote-pause-spread-bad-ticks consecutive ticks "
        "(default %(default)s; e.g. 0.002 = 20 bps = 0.2%%). Pass 'none' to "
        "disable the spread gate entirely. The right value is "
        "symbol-specific: if a symbol's ordinary spread is anywhere near "
        "this, the gate will pause constantly — check that symbol's "
        "spread distribution first. See "
        "PerpetualMarketMakingStrategy._update_spread_gate.",
    )
    parser.add_argument(
        "--quote-pause-spread-bad-ticks",
        type=int,
        default=1,
        help="consecutive two-sided ticks with spread above "
        "--quote-pause-spread before the gate pauses quoting "
        "(default %(default)s = pause on the first blown-out tick). Raise "
        "it if the symbol's book often flickers wide for a single tick. "
        "Must be >= 1.",
    )
    parser.add_argument(
        "--quote-resume-spread",
        type=Decimal,
        default=Decimal("0.0012"),
        help="the spread gate clears the first tick the spread is back "
        "strictly below this (default %(default)s). Must be "
        "below --quote-pause-spread, otherwise the gate has no hysteresis "
        "and can flap. After it clears, breaker_resume_healthy_ticks must "
        "still elapse before quoting resumes.",
    )
    parser.add_argument(
        "--leverage",
        type=int,
        default=5,
        help="leverage to set for the symbol before quoting starts "
        "(default: 5). Must be a whole number — see set_user_leverage()'s "
        "docstring.",
    )
    parser.add_argument(
        "--public-key",
        default=None,
        help="qfex public key. If omitted, reads from the QFEX_PUBLIC_KEY "
        "environment variable instead.",
    )
    parser.add_argument(
        "--secret-key",
        default=None,
        help="qfex secret key. If omitted, reads from the QFEX_SECRET_KEY "
        "environment variable instead.",
    )
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument(
        "--log-file",
        required=True,
        help="write logs to this file path (rotates daily at midnight, "
        "keeps 14 days). Parent directory must already exist. Console "
        "output is always suppressed — this is the only place logs go. "
        "This run's start time (YYYYMMDD_HHMMSS) is automatically "
        "inserted before the extension — e.g. 'logs/mm.log' becomes "
        "'logs/mm_20260727_175258.log' — so each run gets its own file "
        "instead of every run appending to one shared log, which "
        "otherwise makes it easy to mix up which lines belong to which "
        "run.",
    )
    args = parser.parse_args()

    # 0 (or negative) would make the gate trip on every tick, since
    # "bad_ticks >= 0" is always true — reject it up front.
    if args.quote_pause_spread_bad_ticks < 1:
        parser.error("--quote-pause-spread-bad-ticks must be >= 1")

    public_key, secret_key = resolve_credentials(parser, args)

    # Per-run log filename: this run's start time is automatically
    # inserted before the extension, so e.g. 'logs/mm.log' becomes
    # 'logs/mm_20260727_175258.log' — a fresh file each run instead of
    # every run appending to the same one. Uses os.path.splitext so this
    # works regardless of extension (.log, .txt, or none at all) and
    # regardless of how many directory components precede the filename.
    run_started_at = datetime.now().strftime("%Y%m%d_%H%M%S")

    root, ext = os.path.splitext(args.log_file)
    log_file = f"{root}_{run_started_at}{ext}"

    # trigger_log_file: no separate CLI flag — always
    # "<dir of --log-file>/triggers_<run_started_at>.jsonl", fixed
    # filename, not user-configurable (there's no real case for wanting
    # this named differently run to run), sharing the same directory and
    # timestamp as the main log file.
    log_dir = os.path.dirname(args.log_file)
    if log_dir == "":
        log_dir = "."
    trigger_log_file = os.path.join(log_dir, f"triggers_{run_started_at}.jsonl")

    setup_logging(
        args.log_level,
        log_file=log_file,
        console=False,
    )

    cfg = QFEXConfig(
        is_prod=bool(args.is_prod),
        symbol_list=[args.symbol],
        public_key=public_key,
        secret_key=secret_key,
    )

    # PerpetualMarketMakingConfig.__post_init__ raises ValueError if
    # quote_resume_spread >= quote_pause_spread; surface that as a normal
    # usage error rather than a traceback.
    try:
        strategy_cfg = PerpetualMarketMakingConfig(
            order_amount=args.order_amount,
            bid_spread=args.bid_spread,
            ask_spread=args.ask_spread,
            mid_price_underlier_weight=args.underlier_weight,
            order_refresh_tolerance=args.order_refresh_tolerance,
            price_ceiling=args.price_ceiling,
            price_floor=args.price_floor,
            stop_loss_spread=args.stop_loss_spread,
            take_profit_spread=args.take_profit_spread,
            bbo_stop_loss_confirm_streak=args.bbo_stop_loss_confirm_streak,
            quote_pause_spread=args.quote_pause_spread,
            quote_pause_spread_bad_ticks=args.quote_pause_spread_bad_ticks,
            quote_resume_spread=args.quote_resume_spread,
        )
    except ValueError as e:
        parser.error(str(e))

    # build client + strategy (late-bind pattern, since the strategy
    # needs the client and the client needs the strategy)
    client = QFEXTraderClient(cfg, strategy=None)  # type: ignore
    strat = PerpetualMarketMakingStrategy(
        client,
        symbol=args.symbol,
        cfg=strategy_cfg,
        trigger_log_file=trigger_log_file,
        # one file across runs (not per run): the supervisor sums it per week
        taker_ledger_file=os.path.join(log_dir, "taker_fills.jsonl"),
        pnl_ledger_file=os.path.join(log_dir, "pnl_ledger.jsonl"),
    )
    client.strategy = strat  # late bind

    # Graceful shutdown: cancel_on_disconnect covers a crash/kill/
    # network-drop, but a clean Ctrl+C should still actively cancel
    # whatever's resting (bid+ask, or stop_loss+take_profit — never both
    # at once, see PerpetualMarketMakingStrategy's state machine), since
    # leftover orders also block set_user_leverage() on the next run.
    #
    # IMPORTANT: this does NOT close an open position. If you're holding
    # one when you Ctrl+C, cancelling its stop-loss/take-profit orders
    # leaves it with no exit orders resting until you restart or place
    # one manually. Deliberate — this script doesn't silently flatten
    # your position on a routine restart. Exit explicitly (e.g. a manual
    # send_stop_loss/send_market call) before sending SIGINT/SIGTERM if
    # that's what you want.
    async def _cleanup_and_stop() -> None:
        # BID/ASK are regular ALO orders (cancel_order()); STOP_LOSS/
        # TAKE_PROFIT are stop orders in a separate QFEX registry
        # (cancel_stop_order()) — using cancel_order() for a stop order's
        # id gets NO_SUCH_ORDER without actually cancelling it.
        for label, order_id in (
            ("BID", strat.bid_order_id),
            ("ASK", strat.ask_order_id),
        ):
            if order_id is None:
                continue
            try:
                result = await client.cancel_order(order_id, args.symbol)
                client.log.info(
                    "%s cancelled on shutdown: order_id=%s status=%s",
                    label,
                    order_id,
                    result["final_order_response"].get("status"),
                )
            except asyncio.TimeoutError:
                client.log.error(
                    "%s cancel_order timed out on shutdown for order_id=%s — "
                    "verify manually that it's not still resting",
                    label,
                    order_id,
                )
            except QFEXTradeError as e:
                client.log.error(
                    "%s cancel_order rejected on shutdown for order_id=%s: "
                    "%s (error_code=%s) — verify manually",
                    label,
                    order_id,
                    e,
                    e.err.get("error_code"),
                )
            except Exception:
                client.log.exception(
                    "%s cancel_order failed on shutdown for order_id=%s",
                    label,
                    order_id,
                )

        for label, order_id in (
            ("STOP_LOSS", strat.stop_loss_order_id),
            ("TAKE_PROFIT", strat.take_profit_order_id),
        ):
            if order_id is None:
                continue
            try:
                result = await client.cancel_stop_order(order_id)
                client.log.info(
                    "%s cancelled on shutdown: order_id=%s status=%s",
                    label,
                    order_id,
                    result["final_order_response"].get("status"),
                )
            except asyncio.TimeoutError:
                client.log.error(
                    "%s cancel_stop_order timed out on shutdown for "
                    "order_id=%s — verify manually that it's not still "
                    "resting/armed",
                    label,
                    order_id,
                )
            except QFEXTradeError as e:
                client.log.error(
                    "%s cancel_stop_order rejected on shutdown for "
                    "order_id=%s: %s (error_code=%s) — verify manually",
                    label,
                    order_id,
                    e,
                    e.err.get("error_code"),
                )
            except ValueError:
                # A partial-fill-triggered renewal was already mid-cancel
                # for this order_id when shutdown fired its own cancel for
                # the same id — benign race, not a failure; the in-flight
                # attempt is still outstanding and will resolve on its own.
                client.log.info(
                    "%s cancel_stop_order for order_id=%s already "
                    "in-flight from a prior renewal attempt — skipping "
                    "the shutdown-triggered duplicate; the earlier "
                    "attempt is still outstanding and will resolve on "
                    "its own",
                    label,
                    order_id,
                )
            except Exception:
                client.log.exception(
                    "%s cancel_stop_order failed on shutdown for order_id=%s",
                    label,
                    order_id,
                )
        if (
            strat.stop_loss_order_id is not None
            or strat.take_profit_order_id is not None
        ):
            client.log.warning(
                "shutting down while a position may still be open on %s — "
                "this script does NOT close positions on shutdown, only "
                "cancels resting orders (see main()'s _cleanup_and_stop "
                "docstring comment); verify your position manually",
                args.symbol,
            )
        client.stop()

    # SIGUSR1 = drain: stop quoting, finish any open position, exit once
    # flat (used by examples/bot_supervisor.py). SIGINT/SIGTERM keep their
    # old meaning (cancel everything and exit immediately).
    drain_tasks: list = []

    def _start_drain() -> None:
        if drain_tasks:
            return
        drain_tasks.append(asyncio.create_task(
            drain_until_flat(client, strat, args.symbol, _cleanup_and_stop)
        ))

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGUSR1, _start_drain)
    except (NotImplementedError, AttributeError):
        pass
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(_cleanup_and_stop())
            )
        except NotImplementedError:
            pass

    # Leverage gate: trading_enabled must only be set AFTER
    # set_user_leverage() has resolved, not just after entering the
    # client's context — the MDS feed starts delivering callbacks
    # immediately on connect, which can race any setup still running here.
    async with client:
        await client.wait_trade_ready(timeout=cfg.connect_timeout_s)

        # Fetch tick_size/lot_size/price bounds before enabling trading.
        # The strategy computes prices rather than echoing observed ones,
        # and QFEX rejects orders whose price isn't tick-aligned — this
        # blocks startup and refuses to proceed on failure, since
        # proceeding without it would just reproduce that rejection on
        # every placement instead of failing loudly once, up front.
        try:
            await client.fetch_refdata(args.symbol)
        except Exception:
            client.log.exception(
                "fetch_refdata failed for %s — cannot safely compute "
                "tick-aligned prices, refusing to start trading. Check "
                "network connectivity to QFEX's REST API and that "
                "--symbol is a valid QFEX ticker.",
                args.symbol,
            )
            client.stop()
            return

        await run_leverage_gate(
            client,
            args.symbol,
            args.leverage,
            strat.trading_enabled,
            connect_timeout_s=cfg.connect_timeout_s,
        )
        await client.wait_stopped()


if __name__ == "__main__":
    asyncio.run(main())
