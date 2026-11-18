import asyncio
import signal
import argparse
from decimal import Decimal
from typing import Dict, Any

from qfex2 import (
    QFEXTraderClient,
    QFEXConfig,
    TraderStrategy,
    BBO,
    Side,
    setup_logging,
    resolve_credentials,
    run_leverage_gate,
)

# ----------------------------
# Example user strategy + CLI
# ----------------------------


class ExampleTaker(TraderStrategy):
    def __init__(self, client: QFEXTraderClient):
        super().__init__(client)
        # trading_enabled (and guard_reentrant()) come from TraderStrategy
        # (strategy.py) now — see that module for what they do and why.
        # This example's on_bbo/on_trade don't place any orders today, so
        # nothing currently races on this, but if you extend this strategy
        # to call client.send_ioc_limit() from a callback, check
        # self.trading_enabled first — see simple_maker.py's ExampleMaker
        # for the version of this bug that DID bite in practice (orders
        # placed before leverage had actually been set, which then made
        # the leverage change itself fail).

    async def on_bbo(self, bbo: BBO) -> None:
        # Example: print top-of-book. Logged unconditionally, one-sided
        # book or not — has_two_sided_book() is for gating actual order
        # PLACEMENT (see simple_maker.py's ExampleMaker.on_bbo), not
        # logging. This script's own send_ioc_limit() call in main() also
        # doesn't check it, for the same reason: a LIMIT order placed at a
        # specific --price doesn't need to consult current book state
        # before sending — QFEX's matching engine is what actually decides
        # whether it fills, not this client.
        self.log_bbo(bbo)
        # If you add order placement here, guard it with:
        #   if not self.trading_enabled.is_set(): return
        # and wrap the placement logic in `async with self.guard_reentrant()`
        # — see simple_maker.py's ExampleMaker.on_bbo for the pattern.

    async def on_trade(self, trade_msg: Dict[str, Any]) -> None:
        # Example: ignore
        return

    async def on_fill(self, fill: Dict[str, Any]) -> None:
        self.log_fill(fill)


async def main() -> None:
    parser = argparse.ArgumentParser("qfex-taker")
    parser.add_argument(
        "--is-prod",
        action="store_true",
        help="use qfex.com (prod). default uses qfex.io (uat)",
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="symbol to trade/watch, e.g. AAPL-USD",
    )
    parser.add_argument(
        "--leverage",
        type=int,
        default=5,
        help="leverage to set for --symbol before trading starts (default: 5). "
        "Must be a whole number — see set_user_leverage()'s docstring.",
    )
    parser.add_argument(
        "--side",
        choices=["BUY", "SELL"],
        default=None,
        help="if given, place ONE LIMIT IOC order for this side against "
        "--symbol, then exit. Requires --price and --quantity. If "
        "omitted, this script just watches the market (BBO/fills) as "
        "before and never places anything.",
    )
    parser.add_argument(
        "--price",
        type=Decimal,
        default=None,
        help="limit price for the IOC order (required if --side is given)",
    )
    parser.add_argument(
        "--quantity",
        type=Decimal,
        default=None,
        help="order size for the IOC order (required if --side is given)",
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
        "output is always suppressed — this is the only place logs go.",
    )
    args = parser.parse_args()

    if args.side is not None:
        missing = [
            name
            for name, val in (("--price", args.price), ("--quantity", args.quantity))
            if val is None
        ]
        if missing:
            parser.error(
                f"--side was given, so {', '.join(missing)} "
                f"{'is' if len(missing) == 1 else 'are'} also required"
            )

    public_key, secret_key = resolve_credentials(parser, args)

    # Configure logging (application responsibility) — see
    # qfex2/logging_setup.py for why this is a shared helper rather than
    # something QFEXConfig does itself.
    setup_logging(args.log_level, log_file=args.log_file, console=False)

    cfg = QFEXConfig(
        is_prod=bool(args.is_prod),
        symbol_list=[args.symbol],
        public_key=public_key,
        secret_key=secret_key,
    )

    # build client + strategy
    client = QFEXTraderClient(cfg, strategy=None)  # type: ignore
    strat = ExampleTaker(client)
    client.strategy = strat  # late bind

    # graceful shutdown — IOC orders never rest (fill-or-die), so unlike
    # the maker scripts there's nothing to cancel on shutdown; stopping
    # the client is the whole story.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, client.stop)
        except NotImplementedError:
            pass

    # Set leverage for the symbol we'll trade, before the strategy has any
    # chance to place an order. QFEX rejects leverage changes once you have
    # an open order or non-zero position on that symbol, so this must not
    # just run first in program order, it must actually complete before
    # on_bbo/on_trade are allowed to place anything. Entering
    # `async with client:` starts the MDS background loop immediately,
    # which delivers callbacks concurrently with whatever's still awaited
    # here — a plain "await set_user_leverage() then let the strategy do
    # its thing" ordering in source code does NOT guarantee that ordering
    # at runtime. strat.trading_enabled is what actually enforces it (see
    # ExampleTaker); run_leverage_gate only sets it after the leverage
    # call has resolved one way or another.
    #
    # client.run() blocks until stop() is called, so it can't be used here
    # directly — we manage the async context manager ourselves instead, to
    # get a window between "trade session is up" and "strategy is allowed
    # to act" to run this setup step.
    async with client:
        await client.wait_trade_ready(timeout=cfg.connect_timeout_s)
        await run_leverage_gate(
            client,
            args.symbol,
            args.leverage,
            strat.trading_enabled,
            connect_timeout_s=cfg.connect_timeout_s,
        )

        if args.side is not None:
            # One-shot order path: place a single LIMIT IOC order against
            # --symbol, log the outcome, then exit — do NOT fall through
            # to wait_stopped(). IOC orders resolve in one round trip
            # (fill or reject, no resting), so there's nothing further to
            # wait for once send_ioc_limit() returns; staying alive
            # afterward to watch BBO/fills would be a different use case
            # (the watch-only path below) than "place one order and get
            # out."
            side_enum = Side.BUY if args.side == "BUY" else Side.SELL
            try:
                result = await client.send_ioc_limit(
                    symbol=args.symbol,
                    side=side_enum,
                    quantity=args.quantity,
                    price=args.price,
                )
                orsp = result["final_order_response"]
                client.log.info(
                    "IOC order result: symbol=%s side=%s status=%s "
                    "order_id=%s price=%s quantity=%s remaining=%s",
                    args.symbol,
                    args.side,
                    orsp.get("status"),
                    orsp.get("order_id"),
                    orsp.get("price"),
                    orsp.get("quantity"),
                    orsp.get("quantity_remaining"),
                )
            except asyncio.TimeoutError:
                client.log.error(
                    "IOC order timed out waiting for a terminal response "
                    "(symbol=%s side=%s) — check your account/order history "
                    "manually to confirm what actually happened",
                    args.symbol,
                    args.side,
                )
            client.stop()
            return

        # Watch-only path (unchanged from before --side existed): stream
        # BBO/fills until stopped.
        await client.wait_stopped()


if __name__ == "__main__":
    asyncio.run(main())
