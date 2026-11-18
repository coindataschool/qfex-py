"""
Shared helpers for qfex2 runnable scripts. This module exists
so credential-resolution and leverage-gate control flow live in one
place rather than drifting independently across near-identical copies
in separate scripts (this already happened once, when QFEXTradeError
handling on set_user_leverage was added to one script but not others
that existed at the time).

Deliberately NOT shared: shutdown/cleanup. A maker-style script has
resting orders to cancel before stopping; a taker-style script (IOC/
MARKET orders never rest) can just call client.stop() directly — forcing
both into one function would mean every caller describing its orders in
a shape general enough to cover both cases, more ceremony than a small,
obvious call site per script.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from typing import Tuple

from .base_client import QFEXTradeError, _QFEXBaseClient


def resolve_credentials(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> Tuple[str, str]:
    """
    Resolves (public_key, secret_key) from --public-key/--secret-key,
    falling back to the QFEX_PUBLIC_KEY/QFEX_SECRET_KEY environment
    variables. Env vars are the default path to keep credentials out of
    shell history and process listings (`ps` shows CLI args to any user
    on a shared box); the CLI flags stay available for quick overrides.

    Calls parser.error(...) (prints usage, raises SystemExit) if either
    credential is missing from both sources — same as any other argparse
    validation failure, so callers don't need to separately handle a
    missing-credentials case.
    """
    public_key = args.public_key or os.environ.get("QFEX_PUBLIC_KEY")
    secret_key = args.secret_key or os.environ.get("QFEX_SECRET_KEY")
    if not public_key or not secret_key:
        missing = [
            name
            for name, val in (
                ("--public-key / QFEX_PUBLIC_KEY", public_key),
                ("--secret-key / QFEX_SECRET_KEY", secret_key),
            )
            if not val
        ]
        parser.error(
            "missing credentials: " + ", ".join(missing) + " — pass the "
            "CLI flag or set the environment variable"
        )
        raise AssertionError("unreachable")  # parser.error() always raises SystemExit
    return public_key, secret_key


async def run_leverage_gate(
    client: _QFEXBaseClient,
    symbol: str,
    leverage: int,
    trading_enabled: asyncio.Event,
    connect_timeout_s: float,
) -> None:
    """
    Waits for the trade session to be ready, then sets `leverage` for
    `symbol` — skipping the change (with a warning) if you already have
    an open position or resting orders on that symbol, since QFEX rejects
    leverage changes in that state with a generic, unhelpful error. Sets
    `trading_enabled` once this resolves one way or another (success,
    skip, timeout, or rejection) — never before, since the MDS feed
    starts delivering callbacks the moment the client's connection
    context is entered, which can race any setup still running here.

    Call this from inside `async with client:`, after
    `await client.wait_trade_ready(...)` — this function does NOT call
    wait_trade_ready itself, since callers that need to do something else
    first (e.g. perpetual_maker.py's fetch_refdata) need that ordering
    control.

    Continues (does not raise) on a set_user_leverage timeout or
    QFEXTradeError rejection — logs and proceeds trading at whatever
    leverage is already set on the account, rather than aborting the
    script over a leverage-set failure. For different behavior (e.g.
    refuse to trade), don't call this — write your own gate.
    """
    position_known = await client.wait_position_known(symbol, timeout=connect_timeout_s)
    if not position_known:
        client.log.warning(
            "no position_response received for %s within %ss — "
            "proceeding to set leverage anyway without confirming "
            "current position/open-order state first; if this fails, "
            "that's likely why",
            symbol,
            connect_timeout_s,
        )
        skip_leverage = False
    else:
        pos = client.get_position(symbol)
        skip_leverage = pos.position != 0 or pos.open_orders != 0
        if skip_leverage:
            client.log.warning(
                "skipping set_user_leverage for %s: existing "
                "position=%s open_orders=%s (QFEX rejects leverage "
                "changes while either is non-zero) — trading will "
                "proceed using whatever leverage is already set on "
                "the account, NOT --leverage=%s",
                symbol,
                pos.position,
                pos.open_orders,
                leverage,
            )

    if not skip_leverage:
        try:
            await client.set_user_leverage(symbol, leverage)
            client.log.info("leverage set: %s -> %sx", symbol, leverage)
        except asyncio.TimeoutError:
            client.log.error(
                "set_user_leverage timed out for %s — continuing anyway; "
                "verify your account's leverage manually before trading",
                symbol,
            )
        except QFEXTradeError as e:
            client.log.error(
                "set_user_leverage rejected for %s: %s (error_code=%s) "
                "— continuing anyway with whatever leverage is already "
                "set; verify manually before trading",
                symbol,
                e,
                e.err.get("error_code"),
            )

    trading_enabled.set()
