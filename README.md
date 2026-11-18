# qfex2

A modern, high-performance Python SDK for the [QFEX](https://qfex.com) API.

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

## Features

## Installation

Install via pip:

```bash
pip install qfex2
```

Or from source:

```bash
git clone https://github.com/coindataschool/qfex-py.git
cd qfex-py
pip install .
```

## Quick Start

Get your QFEX API keys. See [instruction](https://docs.qfex.com/api-reference/introduction).

Store your keys in env vars by adding the following lines to the right startup
file (`~/.zprofile`, `~/.profile`, `~/.zshrc`, `~/.bashrc`) for your shell:

```bash
export QFEX_PUBLIC_KEY="your_public_key"
export QFEX_SECRET_KEY="your_secret_key" 
```

Run simple maker stratey to place GTC Add Liquidity Orders:

```bash
python examples/simple_maker.py \
    --is-prod \
    --symbol CRCL-USD \
    --quantity 0.1 \
    --leverage 3 \
    --log-level INFO \
    --log-file logs/maker.log
```

Run simple taker stratey to place MARKET orders:

```bash
python examples/simple_taker_market.py \
    --is-prod \
    --symbol CRCL-USD \
    --side BUY \
    --quantity 0.1 \
    --leverage 3 \
    --log-level INFO \
    --log-file logs/taker.log
```

Run simple taker stratey to place IOC LIMIT orders:

```bash
python examples/simple_taker_limit_ioc.py \
    --is-prod \
    --symbol CRCL-USD \
    --side BUY \
    --price 62.45 \
    --quantity 0.1 \
    --leverage 5 \
    --log-level INFO \
    --log-file logs/taker.log
```

Run simple perpetual market making strategy:

```bash
python examples/perpetual_maker.py \
    --is-prod \
    --symbol GOLD-USD \
    --order-amount 0.03 \
    --bid-spread 0.0002 \
    --ask-spread 0.0002 \
    --underlier-weight 0 \
    --order-refresh-tolerance 0.00005 \
    --stop-loss-spread 0.0008 \
    --take-profit-spread 0.0008 \
    --bbo-stop-loss-confirm-streak 5 \
    --quote-pause-spread 0.002 \
    --quote-pause-spread-bad-ticks 1 \
    --quote-resume-spread 0.0012 \
    --leverage 5 \
    --log-level DEBUG \
    --log-file logs/mm.log
```

```bash
python examples/perpetual_maker.py \
    --is-prod \
    --symbol CL-USD \
    --order-amount 1 \
    --bid-spread 0.0004 \
    --ask-spread 0.0004 \
    --underlier-weight 0 \
    --order-refresh-tolerance 0.0001 \
    --stop-loss-spread 0.0015 \
    --take-profit-spread 0.0015 \
    --bbo-stop-loss-confirm-streak 5 \
    --quote-pause-spread 0.002 \
    --quote-pause-spread-bad-ticks 1 \
    --quote-resume-spread 0.0012 \
    --leverage 5 \
    --log-level DEBUG \
    --log-file logs/mm.log
```

```bash
python examples/perpetual_maker.py \
    --is-prod \
    --symbol US500-USD \
    --order-amount 0.02 \
    --bid-spread 0.00025 \
    --ask-spread 0.00025 \
    --underlier-weight 0 \
    --order-refresh-tolerance 0.00005 \
    --stop-loss-spread 0.0001 \
    --take-profit-spread 0.0001 \
    --bbo-stop-loss-confirm-streak 5 \
    --quote-pause-spread 0.002 \
    --quote-pause-spread-bad-ticks 1 \
    --quote-resume-spread 0.0012 \
    --leverage 5 \
    --log-level DEBUG \
    --log-file logs/mm.log
```

Run a pure market making bot:

```bash
caffeinate -i .venv/bin/python examples/bot_supervisor.py
```

`examples/bot_supervisor.py` runs one bot at a time on a daily schedule, with
pauses around news releases, the US stock-market open and weekends. All of its
times are in UTC by default (`TIMEZONE = "UTC"`). To use your own timezone, set
`TIMEZONE` to an IANA name such as `"Europe/London"` and convert the schedule,
blackouts, weekend pause and week start to it. The shipped schedule, sizes and
blackout dates are the author's own settings for late 2026 — review and replace
them before running with real money. (`caffeinate` keeps a Mac awake; on Linux
just run `python examples/bot_supervisor.py`.)

Hint: change `INFO` to `DEBUG`, `WARNING`, `ERROR`, `CRITICAL` for details.

## Requirements

* Python 3.10+
* `websockets`

## License

MIT
