# VulcanTrader

A backtesting, live-trading and web-dashboard stack for
crypto strategies. Based off the latest FreqTrade, we added an improved UI built into the project, multi subprocess bot handling, discord bot (with control cmds), data server (with sub servers) to counter rate limiting, pair finding tool, regime analysis, MAE/MFE analysis, monthly/daily performance boxes, uncorruptable json DBs, headless subproc bots, backtesting/hyperopt/pairfinding all in the UI, and a more compact project structure. Also support for Drift, Bitunix & Coinbase (Advanced Trade) exchanges.

Drop a Freqtrade-style `IStrategy` subclass into
[user_data/strategies/](user_data/strategies) and it should run (just rename imports to VulcanTrader). You are still allowed to use TA Indicators, but there are faster rust indicators provided.
---

## Requirements

- **Python 3.12+**
- **Visual Studio Code (Recommended)** https://code.visualstudio.com/
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

Launches the web portal so you can browse backtest results, backtest, hyperopt, find pairs, review closed trades, and run analysis tools without starting any live or paper-trading session. Manage your bots and edit configs.

Windows:

```powershell
.\run-app.bat
```

Linux / macOS:

```bash
./run-app.sh
```
Then open **http://localhost:8060** in your browser (default password: `test`).


### Use the web dashboard

Once a `run-paper` / `run-live` session is up, the Web Portal is
served on `http://localhost:8060` (port comes from
`config["api_server"]["listen_port"]`). Log in with the bearer password
from `config["api_server"]["password"]` (default `VulcanTrader`).

The dashboard exposes everything you need day-to-day so you rarely have
to touch the CLI:

- **Trading** (`/`) — live open/closed trades, wallet balances,
  per-pair candle charts with strategy plot overlays, and pair locks.
- **Bot account dropdown** (navbar) — the portal scans
  `user_data/accounts/*.json` persistence files and detects which
  `trader_bot` processes are currently running (each running bot holds an
  OS-level lock on its `<account>.json.lock`, plus writes
  `is_running`/`last_heartbeat` markers into the account file). The
  dropdown lists every account — `●` running (with uptime), `○` stopped,
  `⚠` crashed (marked running but no live process) — and selecting one
  loads that bot's full trades/stats/metrics into the dashboard, headless
  bots included. The **Stop** button then gracefully shuts down that bot's
  process: the portal drops a `<account>.json.stop` file which the bot's
  trade loop picks up within ~2 s, exiting cleanly (cleanup, state saved,
  lock released) once its current cycle finishes. API: `GET /api/livebots`,
  `GET /api/dashboard?bot=<name>`, `POST /api/livebots/<name>/stop`.
- **Backtester** (`/backtester`) — pick any JSON file from
  `user_data/backtest_results/` and inspect performance metrics,
  monthly/daily breakdowns, equity & drawdown curves, hourly P&L /
  profit-factor / drawdown, best/worst pairs, regime and MAE/MFE
  analysis, and the full per-trade table.
- **Backtest results browser** — drop new result files into
  `user_data/backtest_results/`; they show up in the dropdown
  automatically.

---

## Running multiple bots (fleet control)

Each config under `user_data/configs/*.json` is treated as a **individual bot config** — its own strategy, its own persistence account, its own process. You can manage the whole fleet from the dashboard, or from the CLI.

### Per-bot config fields

- **`is_bot`** — `true` (default) means this config is a fleet-managed bot: it
  shows up in the dashboard's Configs table and is eligible for `autostart`.
  Set to `false` for a shared base/template config not meant to run as a bot
  in its own right (still usable for `backtest`/`hyperopt` etc., just hidden
  from the fleet list — and skipped by `autostart` even if `auto_start` is
  also `true`).
- **`strategy`** — which `IStrategy` class this config runs. Overridden by
  `-s`/`--strategy` on the CLI when given. **Ignored by `backtest`/
  `hyperopt`** — those always need an explicit `-s`/`--strategies`, so one
  config can still be backtested against many strategies.
- **`auto_start`** — if `true`, gets launched automatically as soon as the
  fleet-control process starts — either `python -m VulcanTrader.bot webserver`
  (which scans for these on every startup; pass `--no-autostart` to skip)
  or the standalone `python -m VulcanTrader.bot autostart` command.
- **`show_terminal`** *(Windows)* — `true` (default) opens a new console
  window per bot; `false` launches with no visible window.
- **`tmux`** *(Linux/macOS)* — `true` opens each bot in its own tmux window
  (session `VulcanTrader`, window named after `bot_name`); `false` (default)
  runs as a plain background process. Falls back to plain background if
  `tmux` isn't installed. A stale window from a previous crashed run of the
  *same* bot is killed automatically before a fresh one opens, so restarts
  never pile up duplicates.


Either path scans every config, launches each `auto_start: true` one as its
own detached `trade` subprocess, and moves on — each bot then runs
independently (persistence account `user_data/accounts/<config-name>_
<dry_run|live>.json`, keyed by the config's own filename so it never
collides with another config sharing the same strategy). The standalone
`autostart` command exits immediately afterward; `webserver` keeps running
the dashboard/Discord bot.

### Shared settings: `user_data/configs_system/system.json`

`api_server` (dashboard host/port/credentials), `discord`, and `telegram`
live in **one** shared file instead of being duplicated into every config.
It's loaded first, so any individual config can still override just one key
(e.g. to disable Discord for that specific bot) without repeating the whole
block. `configs_system/data_server.json` is separate and auto-managed — it's
the shared `data_server.py` master's own config, not something you edit
directly. Its `show_terminal`/`tmux` settings (both default `false`) control
whether an auto-launched master gets its own visible console window/tmux
pane — set them on a bot config's own `"data_server"` block instead (they're
copied through on every bot startup, so editing this file by hand doesn't
stick).

### Running the data server master explicitly

Normally you don't need to think about the `data_server` master at all — the
first bot to start auto-launches one (`ensure_master_running`) and every
later bot just connects to it. But that auto-launch is a probe-then-spawn
race: if two independent fleet launches (e.g. `run-app` started twice, or
under two different Python interpreters) both find nothing listening before
the first one finishes binding, **both** spawn their own master, and you end
up with two processes independently hammering the exchange for the same
pairs — a real cause of chronic rate-limit errors and stale candles.

`run-datamaster.bat` / `run-datamaster.sh` start the master as its own
standalone, explicitly-managed process, refusing to start if something is
already listening on its port. Run it once, before starting the fleet:

Windows:

```powershell
.\run-datamaster.bat
```

Linux / macOS:

```bash
./run-datamaster.sh
```

Override `CONFIG` / `EXCHANGE` / `HOST` / `PORT` / `SUBSERVER_PORT` /
`TIMEFRAMES` via environment variables if you're not running the default
`hyperliquid`, 15m, `127.0.0.1:8720` setup. Pair it with `run-subserver.bat`
/ `.sh` on a second machine to add collection capacity.

### Managing configs from the dashboard

The Trading page has a **Configs** panel — every config in one table, with:

- **Strategy** dropdown and **Auto Start** checkbox, saved immediately.
- **Start** / **Stop** — launches or gracefully stops that config's bot
  process, honouring its `show_terminal`/`tmux` settings.
- **Edit JSON** — a raw editor for the whole config file.
- **Duplicate** / **Rename** / **Delete** — copy an existing config to spin
  up a variant (different pairs, different strategy) without hand-editing
  JSON, rename it, or remove it (blocked while its bot is running).

### Discord control (optional)

If `discord.bot_token` is set (normally via the shared `system.json`), the
interactive Discord client runs from `bot.py webserver` — **not** from any
individual `trade` process. It's a control-plane concern that belongs to
the one persistent process watching the whole fleet (the same account scan
the dashboard uses), not something tied to whichever trading bot happens to
have a token. A `trade` instance's fills/entries/exits still reach Discord
either way, via the plain webhook — no interactive client needed for that.

```powershell
python -m VulcanTrader.bot webserver --port 8060
```

```bash
python -m VulcanTrader.bot webserver --port 8060
```

`!`-prefix commands — `!bots`, `!opentrades <bot>`, `!exit <bot> <pair>`,
`!stats <bot>`, `!stop <bot>` — where `<bot>` is the account name shown by
`!bots`, so a single Discord channel can watch and control every bot in the
fleet. If you run more than one `webserver` instance, pass `--no-discord-bot`
on all but one — two interactive clients sharing one token means two
gateway logins fighting over the same session.

### Setting up Discord

There are two independent pieces — use either alone, or both. Both config
keys normally go in `user_data/configs_system/system.json` (or a specific
config's own `discord` block to override just that bot).

**Webhook — trade notifications, no bot required**

Fills `discord.webhook_url`. This is all you need for entry/exit/startup
messages to show up in a channel; skip the rest of this section entirely if
that's all you want.

1. In Discord, open the server and channel you want notifications posted to.
2. Channel settings (gear icon next to the channel name) → **Integrations**
   → **Webhooks** → **New Webhook**.
3. Name it (e.g. "VulcanTrader"), then **Copy Webhook URL**.
4. Paste that into `discord.webhook_url`.

**Bot — interactive `!` commands (`!bots`, `!exit`, `!stop`, …)**

Fills `discord.bot_token`. Only needed if you want to control the fleet
from Discord, not just watch it.

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications)
   → **New Application** → give it a name.
2. Open that app's **Bot** page in the left sidebar
   (`https://discord.com/developers/applications/<app id>/bot`).
3. **Reset Token** → copy it into `discord.bot_token`.
4. On that same page, scroll to **Privileged Gateway Intents** and enable
   **MESSAGE CONTENT INTENT** — required because commands are plain `!text`,
   not Discord's slash-command system. Skip this and the bot logs in but
   never sees a `!command` — worse, it can't even log in at all and instead
   crashes that one thread with `discord.errors.PrivilegedIntentsRequired`
   (the rest of the process, including webhook logging, is unaffected).
   **You must click the Save Changes button at the bottom of the page** after
   toggling it on — the toggle alone does not persist, and this is the most
   common reason this step "doesn't work." Refresh the page afterward and
   confirm the toggle is still on before assuming it saved. Also double-check
   you're looking at the **same application** the configured `bot_token`
   belongs to — if you're logged into Discord as a different account, or
   editing a different app, the toggle has no effect on the actual bot.
5. **OAuth2 → URL Generator** (left sidebar): check the **bot** scope, then
   under **Bot Permissions** check at least *Send Messages*, *Read Message
   History*, *Attach Files* (open-trade charts), and *Embed Links*.
6. Open the URL the generator produces, pick your server, and authorize —
   this actually adds the bot to the server (creating the application alone
   doesn't).
7. Optional: restrict who can issue commands via `discord.allowed_user_ids`
   — get your Discord user ID with `!myid` once the bot's running (works for
   anyone, whitelisted or not), or via Discord's Settings → Advanced →
   Developer Mode, then right-click your name → Copy User ID. Leave the list
   empty to allow anyone in the server.

---

## Rust backtester & indicator bridge

`VulcanTrader/backtester/` is a single **Rust** crate that is two things at
once: a fast backtest engine plus a library of the 23 standard indicators
(`fast_indicators`), and — when built with the `extension-module` feature — a
[PyO3](https://pyo3.rs) Python extension module, `vulcan_rust_indicators`.
Strategies are **not** written in Rust — they live in Python under
[user_data/strategies/](user_data/strategies); the crate holds no strategies.

A strategy has two equally valid ways to get its indicators — pick per strategy,
or mix both in the same file:

**Rust-bridged** — pull the engine's standard indicator series straight from
Rust, the exact same code the engine itself uses, instead of recomputing them
in TA-Lib:

```python
import vulcan_rust_indicators as vri

ind = vri.calculate_standard_indicators(close, high, low, volume)  # float64 arrays
dataframe["rsi"] = ind[0]    # RSI(14)
dataframe["atr"] = ind[14]   # ATR(14)
```

`ind` is a dict `{index: array}` of all 23 standard series. See
[user_data/strategies/AllIndicatorsDemoStrategy.py](user_data/strategies/AllIndicatorsDemoStrategy.py)
for the full index table and a worked example reading every one of them.

**Plain TA-Lib** — no Rust dependency at all, just the standard library every
freqtrade strategy already uses:

```python
import talib.abstract as ta

dataframe["ema9"] = ta.EMA(dataframe, timeperiod=9)
dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
dataframe["macdhist"] = ta.MACD(dataframe)["macdhist"]
```

See [user_data/strategies/EmaTrendRsiAdx.py](user_data/strategies/EmaTrendRsiAdx.py)
for a complete TA-Lib-only trend-following strategy (EMA stack + RSI + ADX +
MACD histogram).

Anything neither covers you build yourself in pandas/numpy — custom
statistics, calendar/session-anchored levels, and the like.

The crate is built automatically by `install.bat` / `install.sh`, which need a
Rust toolchain (`cargo`) and `maturin`. To rebuild the extension by hand into
the active venv:

```
cd VulcanTrader/backtester
maturin develop --release --features extension-module
```

## Hosting on a Linux server

Run bots inside `tmux` so they survive SSH disconnects. Set `"tmux": true`
in each config you want this for (see "Running multiple bots" above) — the
web portal is started automatically by the `trade` subcommand unless that
config has `--headless` (alias: `--no-web`).

### Start a dry-run trading session

Set `"tmux": true` on each config, then run `autostart` (or hit **Start** on
a config in the dashboard) — every bot with `auto_start: true` opens as its
own window inside **one shared session** named `VulcanTrader`:

```bash
source .venv/bin/activate
python -m VulcanTrader.bot autostart
```

### Re-attach to a running TMUX session

All fleet bots live in that one shared session, as separate windows named
after each bot's `bot_name`:

```bash
tmux attach -t VulcanTrader          # attach to the shared session
tmux list-windows -t VulcanTrader    # see every bot's window name
tmux select-window -t VulcanTrader:BotName   # jump straight to one bot
```

### Kill a bot / the whole fleet

Stop one bot without touching the others (its persistence + stop-file
mechanism handle a clean shutdown either way, but `Ctrl-C` inside the window
is the graceful path):

```bash
tmux kill-window -t VulcanTrader:BotName
```

Kill the entire fleet at once:

```bash
tmux kill-session -t VulcanTrader
```

## OHLCV data

Cached as feather files at:

```
user_data/data/<exchange>/<PAIR>-<timeframe>.feather
user_data/data/<exchange>/futures/<PAIR>-<timeframe>-<candletype>.feather
```

### Data Manager dashboard

`http://localhost:8060/datamanager` — per-exchange view of exactly what's
cached: every pair/timeframe/candle-type combo with its date range, candle
count, and file size, plus a form to launch `download-data` for a config
(optionally overriding pairs/timeframes/exchange/date range) as a background
job with a live log, same as backtests/hyperopt. Exchanges list includes
any exchange used by a config even before its first download, so a newly
added exchange shows up (as empty coverage) right away.

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
  configs/               ← *.json config files, one per bot (is_bot, strategy, auto_start, show_terminal, tmux, ...)
  configs_system/        ← system.json (shared api_server/discord/telegram) + data_server.json (auto-managed)
  accounts/              ← per-bot persistence JSON, <config-name>_<dry_run|live>.json
  data/                  ← OHLCV cache (per exchange / per timeframe)
  strategies/            ← your IStrategy subclasses
  backtest_results/      ← JSON output consumed by the web portal
  *.py                   ← strategy files can also live directly in user_data/
```

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
| `trade`               | Start the live (or `--dry-run`) trading daemon + web portal (`--headless` skips the portal). |
| `autostart`           | Scan `user_data/configs/*.json` for `auto_start: true` and launch each as its own bot process, then exit (see "Running multiple bots"). |
| `webserver`           | Run the web portal + Discord bot; also does the same `auto_start:true` scan on startup (`--no-autostart` to skip). |
| `lookahead-analysis`  | Detect look-ahead bias in strategy entry/exit signals + indicators.|
| `recursive-analysis`  | Detect recursive-formula bias from insufficient `startup_candle_count`. |
| `hyperopt`            | Bayesian strategy-parameter optimisation via Optuna.               |

### Config resolution

`-c / --config` accepts either a path or a bare name. Bare names are
resolved against `<user_data>/configs/`, with `.json` appended if no
extension is given. Multiple `-c` flags merge left-to-right. For `trade` /
`webserver`, `user_data/configs_system/system.json` (shared `api_server` /
`discord` / `telegram` settings) is always merged in first, if present, ahead
of whatever `-c` passes.

### Advanced Commands (most of this can be done in the web dashboard)

Re-activate the venv later with:

```bash
source .venv/bin/activate
```

```powershell
# Live (dry-run) trading
.venv\Scripts\python.exe -m VulcanTrader.bot trade -c live --strategy AlphaHunterV5 --dry-run

# Web portal only (no bot)
.venv\Scripts\python.exe -m VulcanTrader.bot webserver --port 8060

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

### Override the defaults

Each script honors `CONFIG`, `STRATEGY` and `DB_URL` environment
variables, and forwards extra arguments straight to
`python -m VulcanTrader.bot trade`.

```powershell
# Different strategy / config
set CONFIG=configBinance
set STRATEGY=AlphaHunterV4MR
.\run-paper.bat

# Disable the embedded web portal (headless)
.\run-paper.bat --headless
```

```bash
CONFIG=configBinance STRATEGY=YourStrat ./run-paper.sh
./run-paper.sh --headless
```

### Run headless (no web portal)

Pass `--headless` (alias: `--no-web`) to the `trade` subcommand to run the
trading bot without starting `web_portal.py` at all — no FastAPI server, no
open port. Trade notifications that would normally go to the dashboard are
written to the log instead (`user_data/logs/bot.log`). Useful for servers
where you don't want an exposed HTTP port, or for running several bot
processes without port conflicts. A headless bot is still fully visible in
any running portal (e.g. `run-app.bat`): pick its account from the bot
dropdown on the Trading page to see its trades, stats and uptime.

```powershell
.venv\Scripts\python.exe -m VulcanTrader.bot trade -c live --strategy YourStrat --dry-run --headless
```

```bash
python -m VulcanTrader.bot trade -c live --strategy YourStrat --dry-run --headless
```



## License

MIT