"""
account_check.py — READ-ONLY snapshot of your QFEX account. Added 2026-11-05.

Connects to the trade websocket, authenticates, asks for balances, positions,
open orders and stop orders, prints a summary and exits. It never places,
modifies or cancels anything, and it does NOT turn on cancel_on_disconnect
(so running it cannot cancel your manual orders).

Run from the repo root (keys come from the environment, like the bots); the
report is printed and also saved as logs/account_check_<time>.txt:
    .venv/bin/python examples/account_check.py
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
from typing import Any, Dict, List

LISTEN_S = 6.0
TRADE_URL = "wss://trade.qfex.com?api_key={pk}"


def auth_message(public_key: str, secret_key: str) -> Dict[str, Any]:
    nonce = secrets.token_hex(16)
    ts = int(time.time())
    sig = hmac.new(secret_key.encode(), f"{nonce}:{ts}".encode(), hashlib.sha256).hexdigest()
    return {"type": "auth", "params": {"hmac": {"public_key": public_key, "nonce": nonce,
                                                "unix_ts": ts, "signature": sig}}}


def summarize(msgs: List[Dict[str, Any]]) -> str:
    bal = None
    positions: Dict[str, Dict[str, Any]] = {}
    orders: Dict[str, Dict[str, Any]] = {}
    stops: Dict[str, Dict[str, Any]] = {}
    for m in msgs:
        if "balance_response" in m:
            bal = m["balance_response"]
        elif "position_response" in m:
            p = m["position_response"]
            positions[p.get("symbol", "?")] = p
        elif "all_orders_response" in m:
            for o in m["all_orders_response"].get("orders") or []:
                orders[o.get("order_id", "?")] = o
        elif "stop_order_response" in m:
            s = m["stop_order_response"]
            stops[s.get("stop_order_id") or s.get("order_id") or "?"] = s
    out = []
    if bal:
        f = lambda k: float(bal.get(k) or 0.0)
        equity = f("deposit") + f("realised_pnl") + f("unrealised_pnl") + f("net_funding") - f("fees")
        out.append("BALANCE")
        out.append(f"  equity (approx)   ${equity:,.2f}   = deposit {f('deposit'):.2f} + realised {f('realised_pnl'):.2f}"
                   f" + unrealised {f('unrealised_pnl'):.2f} + funding {f('net_funding'):.2f} - fees {f('fees'):.2f}")
        out.append(f"  available         ${f('available_balance'):,.2f}")
        out.append(f"  position margin   ${f('position_margin'):,.2f}")
        out.append(f"  order margin      ${f('order_margin'):,.2f}")
    else:
        out.append("BALANCE: no balance message received")
    open_pos = {s: p for s, p in positions.items() if abs(float(p.get("position") or 0)) > 1e-12}
    out.append(f"OPEN POSITIONS ({len(open_pos)})")
    for s, p in sorted(open_pos.items()):
        out.append(f"  {s:12s} size {float(p['position']):+g}  avg {p.get('average_price', p.get('avg_price', '?'))}"
                   f"  unrealised {float(p.get('unrealised_pnl') or 0):+.2f}  margin {float(p.get('margin_alloc') or 0):.2f}"
                   f"  open_orders(count, excl. stops) {p.get('open_orders', '?')}")
    out.append(f"OPEN ORDERS ({len(orders)})")
    for o in sorted(orders.values(), key=lambda o: (o.get("symbol", ""), o.get("type", ""))):
        out.append(f"  {o.get('symbol','?'):12s} {o.get('type','?'):12s} {o.get('side','?'):4s} qty {o.get('quantity_remaining', o.get('quantity'))}"
                   f" @ {o.get('price')}  status {o.get('status')}  id {o.get('order_id')}")
    out.append(f"STOP ORDERS SEEN ({len(stops)})")
    for s in stops.values():
        out.append(f"  {s.get('symbol','?'):12s} {s.get('stop_order_type', s.get('type','?')):12s} {s.get('direction', s.get('side','?'))}"
                   f" qty {s.get('quantity')} trigger {s.get('price')}  id {s.get('stop_order_id') or s.get('order_id')}")
    if not stops:
        out.append("  (QFEX doesn't document a stop-order snapshot, so 'none' here is NOT proof there are none —"
                   " check stop orders on the website)")
    return "\n".join(out)


async def main(url: str = None, public_key: str = None, secret_key: str = None) -> str:
    import websockets
    pk = public_key or os.environ.get("QFEX_PUBLIC_KEY")
    sk = secret_key or os.environ.get("QFEX_SECRET_KEY")
    if not pk or not sk:
        sys.exit("QFEX_PUBLIC_KEY / QFEX_SECRET_KEY are not set in this terminal")
    url = url or TRADE_URL.format(pk=pk)
    msgs: List[Dict[str, Any]] = []
    async with websockets.connect(url, open_timeout=15) as ws:
        await ws.send(json.dumps(auth_message(pk, sk)))
        first = json.loads(await asyncio.wait_for(ws.recv(), 15))
        if not first.get("authenticated"):
            raise SystemExit("authentication failed (response withheld)")
        await ws.send(json.dumps({"type": "subscribe",
                                  "params": {"channels": ["balances", "positions", "stop_orders"]}}))
        await ws.send(json.dumps({"type": "get_user_orders", "params": {"limit": 1000, "offset": 0}}))
        deadline = time.monotonic() + LISTEN_S
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), left)
            except asyncio.TimeoutError:
                break
            try:
                msgs.append(json.loads(raw))
            except ValueError:
                pass
    return summarize(msgs)


if __name__ == "__main__":
    report = asyncio.run(main())
    print(report)
    # also saved under logs/ so it can be reviewed later
    os.makedirs("logs", exist_ok=True)
    path = os.path.join("logs", time.strftime("account_check_%Y%m%d_%H%M%S.txt"))
    with open(path, "w") as fh:
        fh.write(report + "\n")
    print(f"\nsaved to {path}")
