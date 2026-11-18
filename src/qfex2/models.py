from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    # Post-only resting order ("Add Liquidity Only") — rejected if it
    # would cross the book. Time in force is always GTC.
    ALO = "ALO"
    # Native exchange-triggered exit orders, sent via the same add_order
    # call as any other type. Distinct from the take_profit/stop_loss
    # *fields* attached to every order as bracket-order parameters (see
    # base_client.py) — a different QFEX mechanism. Setting
    # order_type=TAKE_PROFIT doesn't interact with those fields.
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"


class TimeInForce(str, Enum):
    IOC = "IOC"
    GTC = "GTC"
    FOK = "FOK"


@dataclass
class BalanceState:
    deposit: Decimal = Decimal("0")
    realised_pnl: Decimal = Decimal("0")
    unrealised_pnl: Decimal = Decimal("0")
    net_funding: Decimal = Decimal("0")
    order_margin: Decimal = Decimal("0")
    position_margin: Decimal = Decimal("0")
    available_balance: Decimal = Decimal("0")


@dataclass
class PositionState:
    symbol: str
    position: Decimal = Decimal("0")
    margin_alloc: Decimal = Decimal("0")
    realised_pnl: Decimal = Decimal("0")
    unrealised_pnl: Decimal = Decimal("0")
    net_funding: Decimal = Decimal("0")
    open_orders: Decimal = Decimal("0")
    open_quantity: Decimal = Decimal("0")
    leverage: Decimal = Decimal("0")
    initial_margin: Decimal = Decimal("0")
    maintenance_margin: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")


@dataclass(frozen=True)
class BBO:
    symbol: str
    bid_px: Optional[Decimal] = None
    bid_qty: Optional[Decimal] = None
    ask_px: Optional[Decimal] = None
    ask_qty: Optional[Decimal] = None
    sequence: Optional[int] = None
    ts: Optional[str] = None


@dataclass(frozen=True)
class SymbolRefdata:
    """
    Trading rules for a symbol, from QFEX's REST /refdata endpoint (see
    fetch_refdata in base_client.py). Frozen, like BBO — a snapshot
    fetched once at startup, not updated on a live stream.

    All numeric fields are Decimal, converted from the strings QFEX
    sends. tick_size is the minimum price increment QFEX will accept;
    lot_size is the equivalent for quantity.
    """

    symbol: str
    tick_size: Decimal
    lot_size: Decimal
    min_price: Decimal
    max_price: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
