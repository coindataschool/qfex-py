# qfex2

A Python SDK for the [QFEX](https://docs.qfex.com/api-reference/introduction) API. The `examples/bot_supervisor.py`
is a pure market making bot that you can run out of the box or with customization.
By default, it continuously quotes both a buy (bid) and a sell (ask) price for
US500 around a mid-price, aiming to capture the bid–ask spread without taking a
directional bet.

![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

## Disclaimers

This package is written by AI and backtested under my supervision. After two
months of tinkering, I'm releasing it as version 1.0.0. The `examples/bot_supervisor.py`
contains the settings I currently use, so if you just want to passively farming
QFEX points on autopilot, you can just run:

```bash
caffeinate -i .venv/bin/python examples/bot_supervisor.py
```

Of course, you will need to create an account at QFEX first. You can do that with
my referral [https://qfex.com/@re83f92f](https://qfex.com/@re83f92f) to save
10%+ on trading fees.

This software is provided "as is," without warranty of any kind, express or implied.
It is intended for educational and informational purposes only and does not
constitute financial, investment, or trading advice. Trading and investing involve
significant risk, and you may incur capital losses. Never risk money you cannot
afford to lose. By downloading, installing, or using this package, you agree that
the authors, publishers, and contributors are not liable for any financial losses,
damages, or technical issues resulting from its use. You use this software entirely
at your own risk.

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

Create an account at [QFEX](https://qfex.com/@re83f92f) and deposit \$50~\$200.
(You can deposit more later after you feel comfortable with the bot.)

Get your QFEX API keys. See [instruction](https://docs.qfex.com/api-reference/introduction).

Store your keys in env vars by adding the following lines to the right startup
file (`~/.zprofile`, `~/.profile`, `~/.zshrc`, `~/.bashrc`) for your shell:

```bash
export QFEX_PUBLIC_KEY="your_public_key"
export QFEX_SECRET_KEY="your_secret_key" 
```

Run pure market making bots:

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

## Build Your Own Bots

The `examples/` folder contains several other scripts, each implementing a simple
strategy. These scripts are meant to demonstrate the features of the `qfex2`
package, and are not necessarily good trading strategies. You can use them as
starting points to build your own bots.

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

Hint: change `INFO` to `DEBUG`, `WARNING`, `ERROR`, `CRITICAL` for details.

## Requirements

* Python 3.12+
* `websockets`
* `requests`

## License

MIT
