import asyncio
import signal
import argparse
from decimal import Decimal
from typing import Dict, Any, Optional

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
#
# This places exactly ONE resting bid and ONE resting ask, both post-only
# (ALO/GTC), then leaves them alone. It does NOT reprice them as the market
# moves — a real quoting strategy needs to cancel/replace on every
# meaningful BBO change, inventory skew logic, spread management, etc.
# This script's only job is to show the mechanics of QFEXTraderClient's
# maker-style methods: how to place, how fills/rejects surface, and how
# cancellation works. Treat it as a starting point, not a strategy.
#
# Because this only places once, orders are keyed off the FIRST on_bbo
# callback we receive — there's no bid/ask to join until MDS gives us one,
# so placement is deferred with an asyncio.Event rather than attempted at
# construction time.


class ExampleMaker(TraderStrategy):
    def __init__(self, client: QFEXTraderClient, symbol: str, quantity: Decimal):
        super().__init__(client)
        self.symbol = symbol
        self.quantity = quantity
        # trading_enabled and guard_reentrant() come from TraderStrategy
        # (strategy.py) — see that module for what they do and why. main()
        # sets trading_enabled after set_user_leverage() resolves; the
        # placement logic below uses guard_reentrant() instead of a manual
        # flag so a still-in-flight placement can't be re-triggered by a
        # second on_bbo callback.
        self._placed = asyncio.Event()
        # order_ids of our resting bid/ask, once ACK'd. None if that side
        # was never placed or was REJECTED. Used by main() to cancel our
        # own orders on shutdown — see the cleanup step there for why this
        # matters (leftover resting orders block set_user_leverage on the
        # NEXT run of this script).
        self.bid_order_id: Optional[str] = None
        self.ask_order_id: Optional[str] = None

    async def on_bbo(self, bbo: BBO) -> None:
        # No symbol filter here: this client only ever subscribes to a
        # single symbol (symbol_list=[args.symbol] in main()), so on_bbo
        # can never actually fire for anything other than self.symbol —
        # see _mds_subscribe in base_client.py, which subscribes to
        # exactly self.cfg.symbol_list. If this script is ever changed to
        # support multiple symbols again, this class's single-value
        # self.symbol / self.bid_order_id / self.ask_order_id fields would
        # ALSO need to become per-symbol structures — reintroducing a
        # symbol filter alone would not be enough to make that safe.
        self.log_bbo(bbo)

        if not self.trading_enabled.is_set():
            return
        if self._placed.is_set():
            return
        if not self.has_two_sided_book(bbo):
            # No two-sided book yet (e.g. right after connecting) — wait for
            # a snapshot that actually has both sides before joining it.
            return

        # bbo.bid_px / bbo.ask_px are typed Optional[Decimal] on BBO (see
        # models.py) since a one-sided or empty book is a real, valid
        # state. has_two_sided_book() above already guarantees both are
        # non-None by this point at RUNTIME, but a static type checker
        # (Pylance/mypy) can't see across that call to know its return
        # value implies that narrowing — from its perspective,
        # bbo.bid_px is still "Decimal | None" here, which is why it
        # flagged passing it into _place_initial_quotes(bid_px: Decimal,
        # ...). These asserts make the already-true runtime guarantee
        # visible to the type checker too, right where it's established,
        # rather than relying on has_two_sided_book's implementation
        # happening to line up with what _place_initial_quotes needs.
        assert bbo.bid_px is not None
        assert bbo.ask_px is not None

        async with self.guard_reentrant() as acquired:
            if not acquired:
                # A previous on_bbo's placement attempt is still in flight
                # (send_gtc_alo can take up to order_update_timeout_s to
                # resolve) — skip this tick rather than racing it.
                return
            await self._place_initial_quotes(bbo.bid_px, bbo.ask_px)

    async def _place_initial_quotes(self, bid_px: Decimal, ask_px: Decimal) -> None:
        # Joining the book exactly like this means: (a) we're behind
        # whoever's already resting at that price (queue priority), and
        # (b) if the book moves between this BBO snapshot and our order
        # reaching the matching engine, ALO will REJECT rather than cross
        # — that's correct post-only behavior, not a bug, but it does mean
        # placement can legitimately fail on a fast-moving symbol. We log
        # and move on rather than retrying in a loop, since this script
        # intentionally doesn't reprice.
        try:
            bid_result = await self.client.send_gtc_alo(
                symbol=self.symbol,
                side=Side.BUY,
                quantity=self.quantity,
                price=bid_px,
            )
            self._log_placement_result("BID", bid_result)
            bid_orsp = bid_result["final_order_response"]
            if bid_orsp.get("status") == "ACK":
                self.bid_order_id = bid_orsp.get("order_id")
        except asyncio.TimeoutError:
            self.client.log.error("BID placement timed out waiting for ACK/REJECT")

        try:
            ask_result = await self.client.send_gtc_alo(
                symbol=self.symbol,
                side=Side.SELL,
                quantity=self.quantity,
                price=ask_px,
            )
            self._log_placement_result("ASK", ask_result)
            ask_orsp = ask_result["final_order_response"]
            if ask_orsp.get("status") == "ACK":
                self.ask_order_id = ask_orsp.get("order_id")
        except asyncio.TimeoutError:
            self.client.log.error("ASK placement timed out waiting for ACK/REJECT")

        self._placed.set()

    def _log_placement_result(self, label: str, result: Dict[str, Any]) -> None:
        orsp = result["final_order_response"]
        status = orsp.get("status")
        if status == "ACK":
            self.client.log.info(
                "%s ACK order_id=%s px=%s qty=%s",
                label,
                orsp.get("order_id"),
                orsp.get("price"),
                orsp.get("quantity"),
            )
        else:
            # Most commonly REJECTED (crossed the book on entry) — not
            # treated as fatal, just logged, since a real strategy would
            # decide here whether to retry at a different price.
            self.client.log.warning("%s not resting: status=%s resp=%s", label, status, orsp)

    async def on_trade(self, trade_msg: Dict[str, Any]) -> None:
        # Example: ignore public trade prints.
        return

    async def on_fill(self, fill: Dict[str, Any]) -> None:
        self.log_fill(fill)


async def main() -> None:
    parser = argparse.ArgumentParser("qfex-maker")
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
        "--quantity",
        required=True,
        type=Decimal,
        help="order size for both the resting bid and resting ask",
    )
    parser.add_argument(
        "--leverage",
        type=int,
        default=5,
        help="leverage to set for the symbol before quoting starts (default: 5). "
        "Must be a whole number — see set_user_leverage()'s docstring.",
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

    # build client + strategy (late-bind pattern: the strategy needs the
    # client and the client needs the strategy, so QFEXTraderClient is
    # constructed with strategy=None and the real strategy is assigned
    # onto client.strategy right after — see perpetual_maker.py for the
    # same pattern used with PerpetualMarketMakingStrategy)
    client = QFEXTraderClient(cfg, strategy=None)  # type: ignore
    strat = ExampleMaker(client, symbol=args.symbol, quantity=args.quantity)
    client.strategy = strat  # late bind

    # graceful shutdown
    #
    # Stopping the client does NOT, by itself, cancel your resting orders —
    # _QFEXBaseClient enables cancel_on_disconnect on the exchange side on
    # every (re)connect, so a hard disconnect (crash, killed process) will
    # get your orders pulled server-side, but a clean Ctrl+C previously just
    # tore down the local connections and left resting orders live.
    #
    # That's not just a cosmetic gap: leftover resting orders from a
    # previous run block set_user_leverage() on the NEXT run (QFEX rejects
    # leverage changes while you have open orders or a non-zero position),
    # which is exactly what happened running this script back to back. So
    # on a clean shutdown (SIGINT/SIGTERM), we now explicitly cancel our
    # known bid/ask order_ids before tearing down the connection, using the
    # same cancel_order() call a real strategy would use to reprice.
    #
    # This does NOT cover every case — a crash, a killed process, or a
    # network drop still relies on cancel_on_disconnect server-side (which
    # is a real, separate safety net, just not this one). And this only
    # cancels the two orders THIS script instance placed; if you have other
    # resting orders on the account/symbol from elsewhere, those aren't
    # touched here — see the get_available_leverage / get_user_orders trade
    # channels if you need a more general "cancel everything on this
    # symbol before I start" cleanup.
    async def _cleanup_and_stop() -> None:
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
            except Exception:
                client.log.exception(
                    "%s cancel_order failed on shutdown for order_id=%s",
                    label,
                    order_id,
                )
        client.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(_cleanup_and_stop())
            )
        except NotImplementedError:
            pass

    # Set leverage before the strategy places any resting orders. QFEX
    # rejects leverage changes once you have an open order or non-zero
    # position on the symbol — so this must not just run first in program
    # order, it must actually complete before on_bbo is allowed to place
    # anything. Entering `async with client:` starts the MDS background
    # loop immediately, which delivers on_bbo callbacks concurrently with
    # whatever's still awaited here — so a plain "await set_user_leverage()
    # then let on_bbo do its thing" ordering in source code does NOT
    # guarantee that ordering at runtime. strat.trading_enabled is what
    # actually enforces it; on_bbo checks it before placing anything, and
    # run_leverage_gate only sets it after the leverage call has resolved
    # one way or another.
    async with client:
        await client.wait_trade_ready(timeout=cfg.connect_timeout_s)
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
