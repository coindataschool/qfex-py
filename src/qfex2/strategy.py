from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict

if TYPE_CHECKING:
    from .trader_client import QFEXTraderClient
    from .models import BBO


class TraderStrategy:
    """
    Users subclass this and override callbacks, for use with
    QFEXTraderClient. Provides shared trading-lifecycle plumbing:

    - trading_enabled: an asyncio.Event to check at the top of any
      callback before placing or cancelling anything. Set it only after
      set_user_leverage() has resolved — entering the client's context
      starts the MDS feed immediately, so without this gate a strategy
      can place orders before leverage is actually set.

    - guard_reentrant(): prevents a callback from re-entering itself
      while a previous invocation is still in-flight:

          async def on_bbo(self, bbo):
              if not self.trading_enabled.is_set():
                  return
              async with self.guard_reentrant() as acquired:
                  if not acquired:
                      return
                  ... your logic ...

      Yields False (not acquired) if a call is already in-flight —
      check and return early. A context manager rather than a bool flag
      so the in-flight state clears even if the callback body raises.

    - has_two_sided_book(bbo): true if both bid and ask are present.

    - log_bbo(bbo) / log_fill(fill): thin INFO-level logging helpers —
      don't filter or decide when to log, just format and emit.

    Deliberately NOT included: what you trade, when you reprice, how
    many orders you keep resting — that belongs in your subclass.
    """

    def __init__(self, client: "QFEXTraderClient") -> None:
        self.client = client
        self.trading_enabled = asyncio.Event()
        self._reentrant_lock = asyncio.Lock()

    @asynccontextmanager
    async def guard_reentrant(self) -> AsyncGenerator[bool, None]:
        if self._reentrant_lock.locked():
            yield False
            return
        async with self._reentrant_lock:
            yield True

    @staticmethod
    def has_two_sided_book(bbo: "BBO") -> bool:
        """
        True if `bbo` has both a bid and an ask — safe to quote around or
        compute a mid from. False for a one-sided or empty book (e.g.
        right after connecting).
        """
        return bbo.bid_px is not None and bbo.ask_px is not None

    def log_bbo(self, bbo: "BBO") -> None:
        """
        Logs a BBO tick at INFO: "BBO {symbol} bid={qty}@{px} ask={qty}@{px}".
        Purely a formatting helper — doesn't filter by symbol or check
        has_two_sided_book(); call it from your own on_bbo after whatever
        guards make sense for your strategy.
        """
        self.client.log.info(
            "BBO %s bid=%s@%s ask=%s@%s",
            bbo.symbol,
            bbo.bid_qty,
            bbo.bid_px,
            bbo.ask_qty,
            bbo.ask_px,
        )

    def log_fill(self, fill: Dict[str, Any]) -> None:
        """
        Logs a fill at INFO: "FILL {symbol} side={side} px={px}
        dqty={delta} rem={remaining} status={status} trade_id={trade_id}".
        `fill` is the exact dict on_fill() receives. Purely a formatting
        helper — doesn't decide whether/when to log.
        """
        orsp = fill["order_response"]
        self.client.log.info(
            "FILL %s side=%s px=%s dqty=%s rem=%s status=%s trade_id=%s",
            orsp.get("symbol"),
            orsp.get("side"),
            orsp.get("price"),
            fill["filled_qty_delta"],
            orsp.get("quantity_remaining"),
            orsp.get("status"),
            orsp.get("trade_id"),
        )

    def log_mark_price(self, symbol: str) -> None:
        """
        Logs the current mark price for `symbol` at INFO:
        "MARK {symbol} price={price}". Unlike log_bbo/log_fill, takes no
        value argument — reads self.client.mark_price_by_symbol.get(symbol)
        fresh each call, since mark price is a live snapshot rather than
        an event. Logs price=NaN (a real Decimal, not the string "None")
        if nothing has arrived for `symbol` yet.
        """
        price = self.client.mark_price_by_symbol.get(symbol)
        if price is None:
            price = Decimal("NaN")
        self.client.log.info("MARK %s price=%s", symbol, price)

    async def on_stop_order_resolved(self, order_id: str, orsp: Dict[str, Any]) -> None:
        """
        Fires when _handle_order_response (base_client.py) detects a
        stop order (STOP_LOSS/TAKE_PROFIT) was resolved by QFEX without
        this client asking — two distinct cases, same reaction needed
        for both:

        - "Phantom FILLED": status FILLED but trade_id missing, meaning
          the order's trigger fired and QFEX spawned a separate order to
          execute the trade rather than filling this one. Mutually
          exclusive with on_fill for a given order_response.
        - Externally cancelled: status CANCELLED for a stop order this
          client did not itself send a cancel for — confirmed live that
          QFEX auto-cancels the other leg of a bracket when one leg
          triggers. Cancelling it again yourself gets a ServerError.

        Either way, `order_id` is no longer live on QFEX's side and any
        further cancel attempt against it will fail — this is the signal
        to stop tracking it.

        Default is a no-op — override to react (see
        PerpetualMarketMakingStrategy for an example that clears its own
        order-id tracking).
        """
        return

    async def on_bbo(self, bbo: "BBO") -> None:
        pass

    async def on_trade(self, trade_msg: Dict[str, Any]) -> None:
        pass

    async def on_fill(self, fill: Dict[str, Any]) -> None:
        pass
