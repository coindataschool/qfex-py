"""Tests for the orphan-quote fix (base_client) and excess-position trim (strategy)."""
import asyncio
from decimal import Decimal

from qfex2.base_client import QFEXTradeError
from qfex2.config import QFEXConfig
from qfex2.trader_client import QFEXTraderClient
from qfex2.perpetual_market_making import PerpetualMarketMakingStrategy, PerpetualMarketMakingConfig


class _Dummy:
    async def on_fill(self, f): pass
    async def on_stop_order_resolved(self, oid, orsp): pass


def make_client():
    cfg = QFEXConfig(is_prod=False, symbol_list=["GOLD-USD"], public_key="k", secret_key="s")
    c = QFEXTraderClient(cfg, _Dummy())
    c.cancels = []
    async def fake_cancel(order_id, symbol):
        c.cancels.append((order_id, symbol))
        return {"final_order_response": {"status": "CANCELLED"}}
    c._send_cancel_and_wait = fake_cancel
    return c


def orsp(coid, typ="ALO", status="ACK", oid="oid-1"):
    return {"order_id": oid, "client_order_id": coid, "symbol": "GOLD-USD", "status": status,
            "type": typ, "quantity": 0.01, "quantity_remaining": 0.01, "price": 4210.2,
            "side": "BUY", "trade_id": None}


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


async def test_late_alo_ack_is_cancelled():
    c = make_client()
    c._recently_timed_out_oids.insert("coid-late")
    await c._handle_order_response(orsp("coid-late"))
    await settle()
    assert c.cancels == [("oid-1", "GOLD-USD")], c.cancels


async def test_on_time_alo_ack_not_cancelled():
    c = make_client()
    await c._handle_order_response(orsp("coid-normal"))
    await settle()
    assert c.cancels == []


async def test_late_non_alo_not_cancelled():
    c = make_client()
    for typ in ("MARKET", "STOP_LOSS", "TAKE_PROFIT"):
        c._recently_timed_out_oids.insert("coid-" + typ)
        await c._handle_order_response(orsp("coid-" + typ, typ=typ, oid="oid-" + typ))
    await settle()
    assert c.cancels == []


async def test_late_alo_cancelled_status_not_cancelled():
    c = make_client()
    c._recently_timed_out_oids.insert("coid-x")
    await c._handle_order_response(orsp("coid-x", status="REJECTED"))
    await settle()
    assert c.cancels == []


async def test_orphan_cancel_rejected_no_retry():
    c = make_client()
    calls = []
    async def reject(order_id, symbol):
        calls.append(order_id)
        raise QFEXTradeError({"error_code": "NO_SUCH_ORDER"})
    c._send_cancel_and_wait = reject
    await c._cancel_orphan("oid-9", "GOLD-USD")
    assert calls == ["oid-9"]


def make_strategy(tmp="/tmp/trig_test.jsonl"):
    c = make_client()
    cfg = PerpetualMarketMakingConfig(order_amount=Decimal("0.01"), bid_spread=Decimal("0.0003"),
                                      ask_spread=Decimal("0.0003"))
    s = PerpetualMarketMakingStrategy(c, "GOLD-USD", cfg, trigger_log_file=tmp)
    c.strategy = s
    c._trade_ready.set()   # tests run as if the trade session is up
    c.markets = []
    async def fake_market(symbol, side, quantity, reduce_only=None, client_order_id=None):
        c.markets.append((side.value if hasattr(side, "value") else side, quantity, reduce_only))
        return {"final_order_response": {"status": "FILLED"}}
    c.send_market = fake_market
    return s, c


async def test_trim_excess_long_and_cooldown():
    s, c = make_strategy()
    await s._trim_excess_position(Decimal("0.03"))
    assert len(c.markets) == 1 and c.markets[0][1] == Decimal("0.02") and c.markets[0][2] is True, c.markets
    assert c.markets[0][0] in ("SELL", "Side.SELL"), c.markets
    await s._trim_excess_position(Decimal("0.03"))      # within cooldown -> no second order
    assert len(c.markets) == 1


async def test_no_trim_at_or_below_order_amount():
    s, c = make_strategy()
    for q in ("0.01", "-0.01", "0.0031", "-0.0069"):
        await s._trim_excess_position(Decimal(q))
    assert c.markets == []


async def test_trim_excess_short():
    s, c = make_strategy()
    await s._trim_excess_position(Decimal("-0.02"))
    assert c.markets and c.markets[0][1] == Decimal("0.01") and c.markets[0][0] in ("BUY", "Side.BUY"), c.markets


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    for t in tests:
        asyncio.run(t()); print("PASS", t.__name__)
