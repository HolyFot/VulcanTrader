# VulcanTrader

A backtesting, live-trading and web-dashboard stack for
crypto strategies. Based off the latest FreqTrade, we added a better UI built into the project, pair finding,  regime analysis, timing analysis, monthly/daily performance boxes, uncorruptable json DBs, backtesting/hyperopt/pairfinding all in the UI, and a more compact project structure. Also support for Drift & Bitunix exchanges.

Drop a Freqtrade-style `IStrategy` subclass into
[user_data/strategies/](user_data/strategies) and it should run (just rename imports to VulcanTrader).
---

## Requirements

- **Python 3.12**
- **Visual Studio Code** https://code.visualstudio.com/
   install VS Code, you pick Open Folder and open this folder.

Install:

### Install Windows (PowerShell)

```powershell
.\install.bat
```

Re-activate the venv later with:

```powershell
.\.venv\Scripts\Activate.ps1
```

### Install Linux Ubuntu 22.0 / macOS (bash)

```bash
./install.sh
```
---

### Start the web dashboard only (no trading)

Launches the FastAPI portal so you can browse backtest results, review
closed trades, and run analysis tools without starting any live or
paper-trading session.

Windows:

```powershell
.\run-app.bat
```

Linux / macOS:

```bash
./run-app.sh
```
Then open **http://localhost:8080** in your browser (default password: `VulcanTrader`).


### Start a paper-trading session

Note: you will have to edit this bat to specify your config & strategy.

Windows:
```powershell/terminal:
.\run-paper.bat
```

Linux / macOS:
```bash
./run-paper.sh
```

### Start a live (real-money) session

The live launchers prompt for confirmation before placing real orders.
Note: you will have to edit this bat to specify your config & strategy.

Windows:
```powershell
.\run-live.bat
```

Linux / macOS:
```bash
./run-live.sh
```

### Override the defaults

Each script honours `CONFIG`, `STRATEGY` and `DB_URL` environment
variables, and forwards extra arguments straight to
`python -m VulcanTrader.bot trade`.

```powershell
# Different strategy / config
set CONFIG=configBinance
set STRATEGY=AlphaHunterV4MR
.\run-paper.bat

# Disable the embedded web portal
.\run-paper.bat --no-web
```

```bash
CONFIG=configBinance STRATEGY=AlphaHunterV5 ./run-paper.sh
./run-paper.sh --no-web
```

### Use the web dashboard

Once a `run-paper` / `run-live` session is up, the FastAPI portal is
served on `http://localhost:8080` (port comes from
`config["api_server"]["listen_port"]`). Log in with the bearer password
from `config["api_server"]["password"]` (default `VulcanTrader`).

The dashboard exposes everything you need day-to-day so you rarely have
to touch the CLI:

- **Trading** (`/`) — live open/closed trades, wallet balances,
  per-pair candle charts with strategy plot overlays, pair locks, and
  the **Trade Timing Analysis** card that scans each closed trade for
  better entry/exit prices and simulates whether ignoring stoploss
  exits would have recovered or turned profitable.
- **Backtester** (`/backtester`) — pick any JSON file from
  `user_data/backtest_results/` and inspect performance metrics,
  monthly/daily breakdowns, equity & drawdown curves, hourly P&L /
  profit-factor / drawdown, best/worst pairs, and the full per-trade
  table. The same **Trade Timing Analysis** card is available here and
  replays each backtest trade against the cached OHLCV.
- **Backtest results browser** — drop new result files into
  `user_data/backtest_results/`; they show up in the dropdown
  automatically.

To run the portal **without** a live bot (e.g. just to browse backtest
results), use `run-app.bat` / `run-app.sh` or the standalone subcommand directly:

```powershell
python -m VulcanTrader.bot webserver --port 8080
```


### Advanced Commands (most of this can be done in the web dashboard)

Re-activate the venv later with:

```bash
source .venv/bin/activate
```

```powershell
# Live (dry-run) trading
.venv\Scripts\python.exe -m VulcanTrader.bot trade -c live --strategy AlphaHunterV5 --dry-run

# Web portal only (no bot)
.venv\Scripts\python.exe -m VulcanTrader.bot webserver --port 8080

# Single backtest
.venv\Scripts\python.exe -m VulcanTrader.bot backtest -c configs/configAlphaHunterV5_Paper.json -s AlphaHunterV5 --timerange 20250101- --datadir user_data/data/hyperliquid


# Download last 90 days of OHLCV for two pairs at three timeframes
.venv\Scripts\python.exe -m VulcanTrader.bot download-data -c live `
    --pairs BTC/USDT ETH/USDT --timeframes 1m 5m 1h --days 90

# Downloading Hyperliquid
Download it from: http://frequenthippo-dl.ddns.net/wp-content/uploads/hyperliquid_download-data.7z
And put it correctly in the user_data\data folder. (should be user_data\data\hyperliquid\futures with a bunch of feather files in there.

# Look-ahead bias check (signals + indicators)
.venv\Scripts\python.exe -m VulcanTrader.bot lookahead-analysis -c live -s AlphaHunterV5 `
    --timerange 20250101- --pairs BTC/USDT ETH/USDT `
    --minimum-trade-amount 10 --targeted-trade-amount 50

# Recursive (startup-candle) bias check
.venv\Scripts\python.exe -m VulcanTrader.bot recursive-analysis -c live -s AlphaHunterV5 `
    --timerange 20250101- --pairs BTC/USDT `
    --startup-candle 199 399 999
```

---

## Hosting on a Linux server

Run the bot inside `tmux` (or `screen`/`systemd`) so it survives SSH
disconnects. The web portal is started automatically by the `trade`
subcommand unless you pass `--no-web`.

### Start a dry-run trading session

```bash
source .venv/bin/activate
tmux new -s Bot1
python -m VulcanTrader.bot trade \
    --dry-run \
    -c configAlphaHunterV5_Paper \
    --strategy AlphaHunterV5 \
    --db-url json:///user_data/trades.dry_run.json
```

Detach with `Ctrl-b d` to leave the bot running.

### Re-attach to a running session

```bash
tmux attach -t Bot1
```

List sessions:

```bash
tmux ls
```

### Kill a session

```bash
tmux kill-session -t Bot1
```

### Live (real-money) trading

Drop `--dry-run` and point `--db-url` at a non-dry-run database:

```bash
python -m VulcanTrader.bot trade \
    -c configs/configAlphaHunterV5_Live \
    --strategy AlphaHunterV5 \
    --db-url json:///user_data/trades.live.json
```

---


## OHLCV data

Cached as feather files at:

```
user_data/data/<exchange>/<PAIR>-<timeframe>.feather
user_data/data/<exchange>/futures/<PAIR>-<timeframe>-<candletype>.feather
```

The repo ships with a small Binance spot cache (BTC/USDT, ETH/USDT at
1m/5m/30m/1h/4h) and a `hyperliquid/` folder so you can experiment
without hitting an exchange first.

---

## Rust backtester & indicator bridge

`VulcanTrader/backtester/` is a single **Rust** crate that is two things at
once: a fast backtest engine plus a library of the 23 standard indicators
(`fast_indicators`), and — when built with the `extension-module` feature — a
[PyO3](https://pyo3.rs) Python extension module, `vulcan_rust_indicators`.
Strategies are **not** written in Rust — they live in Python under
[user_data/strategies/](user_data/strategies); the crate holds no strategies.

A strategy can pull the engine's standard indicator series straight from Rust —
the exact same code the engine uses — instead of recomputing them in TA-Lib:

```python
import vulcan_rust_indicators as vri

ind = vri.calculate_standard_indicators(close, high, low, volume)  # float64 arrays
dataframe["rsi"] = ind[0]    # RSI(14)
dataframe["atr"] = ind[14]   # ATR(14)
```

`ind` is a dict `{index: array}` of all 23 standard series — see
`AllIndicatorsDemoStrategy` for the full index table. Anything the standard set
doesn't cover you build yourself in pandas/numpy; see `FisherStatReversion` and
`IchimokuCloud` for custom-indicator examples (Fisher Transform, z-score,
linreg slope, the full Ichimoku system), and `DonchianBreakout` for a mix
(bridge RSI/ATR + custom Donchian channels).

The crate is built automatically by `install.bat` / `install.sh`, which need a
Rust toolchain (`cargo`) and `maturin`. To rebuild the extension by hand into
the active venv:

```
cd VulcanTrader/backtester
maturin develop --release --features extension-module
```

> The `backtest` CLI command runs the Python `backtesting.py` engine; the Rust
> crate currently feeds Python only through this indicator bridge.

---

## CLI (Advanced)

All commands funnel through [VulcanTrader/bot.py](VulcanTrader/bot.py):

```powershell
python -m VulcanTrader.bot <subcommand> [options]
```

| Subcommand            | Purpose                                                            |
| --------------------- | ------------------------------------------------------------------ |
| `backtest`            | Run one or more strategies through the backtester (async fan-out). |
| `download-data`       | Pull historical OHLCV for the configured pairs / timeframes.       |
| `trade`               | Start the live (or `--dry-run`) trading daemon + web portal.       |
| `webserver`           | Run the web portal stand-alone (browse backtest results).          |
| `lookahead-analysis`  | Detect look-ahead bias in strategy entry/exit signals + indicators.|
| `recursive-analysis`  | Detect recursive-formula bias from insufficient `startup_candle_count`. |
| `hyperopt`            | Bayesian strategy-parameter optimisation via Optuna.               |

### Config resolution

`-c / --config` accepts either a path or a bare name. Bare names are
resolved against `<user_data>/configs/`, with `.json` appended if no
extension is given. Multiple `-c` flags merge left-to-right.


## Web portal

`VulcanTrader/web_portal.py` is a FastAPI app that doubles as the bot's
notification sink (replacing freqtrade's `RPCManager`) and serves the
HTML dashboards in [template/](template).

Authentication: bearer token derived from
`config["api_server"]["password"]` (default `"VulcanTrader"`), checked
with `secrets.compare_digest`.

---


## Layout

```
VulcanTrader/            ← Python package (imported as VulcanTrader.*)
  bot.py                 ← CLI entry point (all subcommands)
  backtesting.py         ← historical replay engine
  trader_bot.py          ← live / dry-run trading daemon
  web_portal.py          ← FastAPI dashboard + notification sink
  pairs_bt_finder.py     ← utility: find best pairs for backtesting
  regime_analysis.py     ← market-regime detection helpers
  backtester/            ← Rust crate: backtest engine + `vulcan_rust_indicators` PyO3 module (no strategies)
  config/                ← Configuration loader + JSON-schema validation
  data/                  ← OHLCV loaders, converters, btanalysis, metrics
  enums/                 ← All Enum types
  exchange/              ← CCXT exchange wrappers + order utilities
  hyperopt/              ← Bayesian parameter optimiser (Optuna-backed)
  optimize/              ← Hyperopt parameter-space helpers
  pairlist/              ← IPairList handlers (filter pipeline)
  persistence/           ← JSON-backed Trade/Order/PairLock storage
  resolvers/             ← Dynamic class loaders
  strategy/              ← IStrategy interface + HyperOpt mixin
  util/                  ← supporting helpers and managers

template/                ← HTML served by the web portal
  login.html  trading.html  backtester.html  exampleStyle.html

user_data/               ← per-user, NOT under version control by default
  configs/               ← *.json config files
  data/                  ← OHLCV cache (per exchange / per timeframe)
  strategies/            ← your IStrategy subclasses
  backtest_results/      ← JSON output consumed by the web portal
  *.py                   ← strategy files can also live directly in user_data/
```

---

## License

MIT