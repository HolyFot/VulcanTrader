# VulcanTrader — AI Agent Reference

Comprehensive codebase guide for AI coding assistants. Read this before editing any source file.

---

## Repository layout

```
main.py                     ← Thin root-level entry point: `python main.py` == `python -m
                               VulcanTrader.bot webserver` (see "single_process_mode" below)
tests/                       ← stdlib unittest suite (no pytest) - persistence isolation +
                               in-process launcher mechanics, see "Testing" below

VulcanTrader/               ← Python package root
  bot.py                    ← CLI entry-point (all subcommands)
  trader_bot.py             ← Live / dry-run trading engine
  backtesting.py            ← Historical backtesting engine
  web_portal.py             ← FastAPI dashboard (replaces RPC stack)
  wallets.py                ← Balance / stake-amount tracking
  indicators.py             ← Shared technical-indicator helpers
  constants.py              ← Package-wide constants and type aliases
  pairs_bt_finder.py        ← Utility: find best pairs for backtesting
  regime_analysis.py        ← Market-regime detection helpers
  config/                   ← Config loading, validation, secrets
  data/                     ← OHLCV fetching, caching, converters
  enums/                    ← All Enum types (State, ExitType, RunMode, …)
  exchange/                 ← CCXT exchange wrappers + order utilities
  hyperopt/                 ← Bayesian parameter optimiser (Optuna-backed)
    hyperopt/               ← Core Hyperopt class + optimizer loop
    hyperopt_loss/          ← Loss-function implementations (Sharpe, Sortino, …)
    optimize_reports/       ← Epoch storage and output formatting
    hyperopt_epoch_filters.py
    hyperopt_resolver.py
    hyperopt_tools.py
  optimize/                 ← Hyperopt parameter-space helpers (space.py)
  pairlist/                 ← IPairList handlers (filter pipeline)
  persistence/              ← JSON-backed Trade/Order/PairLock storage
  resolvers/                ← Dynamic class loaders (strategy, exchange, pairlist)
  strategy/                 ← IStrategy interface + HyperOpt mixin
  trader_types/             ← Shared TypedDict / annotation types
  util/                     ← PairListManager, ProtectionManager, discord, charts, proc_launch, …
  backtester/               ← Rust crate: fast backtest engine + indicator library, and (with the
                              `extension-module` feature) the `vulcan_rust_indicators` PyO3 module. No strategies.

user_data/
  configs/                  ← Per-bot JSON configs (see "Multi-bot fleet control" below) - each
                              one is treated as one bot definition: is_bot, strategy, auto_start,
                              show_terminal, tmux, dry_run, db_url, bot_name, exchange, ...
  configs_system/           ← configs_system/system.json (shared api_server/discord/telegram -
                              auto-merged into every config, see bot.py::_with_system_config)
                              and data_server.json (data_server's own auto-written config)
  accounts/                 ← Per-bot persistence JSON (<account-name>_<dry_run|live>.json),
                              named after the config filename (or strategy, for the older
                              `bot.py autostart` account naming) - not the bot_name label
  data/                     ← OHLCV cache (per exchange / timeframe)
  strategies/               ← User strategy modules (.py files)
  backtest_results/         ← Backtest output JSON (read by web portal)
  logs/                     ← Runtime logs + heartbeat.json
  *.py                      ← Strategy files can also live directly in user_data/

template/                   ← Jinja/HTML templates served by web_portal
static/                     ← Static assets (JS, CSS)
```

---

## bot.py — CLI orchestrator

**Entry-point:** `python -m VulcanTrader.bot <subcommand>`. Root-level
`main.py` is a thin alias for `python -m VulcanTrader.bot webserver` — see
"single_process_mode" under "Multi-bot fleet control" below.

### Subcommands

| Subcommand | Handler | Purpose |
|---|---|---|
| `trade` | `cmd_trade()` | Launch live / dry-run bot |
| `autostart` | `cmd_autostart()` | Scan `user_data/configs/*.json` for `auto_start: true`, launch each - as its own detached `trade` subprocess, or as a thread in THIS process if `single_process_mode` is set (thin wrapper around `_launch_autostart_configs()`) |
| `backtest` | `cmd_backtest()` | Run one or many strategy backtests (async fan-out) |
| `download-data` | `cmd_download_data()` | Fetch OHLCV history |
| `webserver` | `cmd_webserver()` | Web portal + Discord bot; also calls `_launch_autostart_configs()` itself on startup (opt out with `--no-autostart`) - see "Multi-bot fleet control" |
| `lookahead-analysis` | `cmd_lookahead_analysis()` | Bias detection |
| `recursive-analysis` | `cmd_recursive_analysis()` | Recursive bias detection |
| `hyperopt` | `cmd_hyperopt()` | Bayesian strategy-parameter optimisation (Optuna) |

### `cmd_trade()` pipeline

```
configs = _resolve_configs(args.config)
configs = _with_system_config(configs, args)  # prepends configs_system/system.json if present
_load_configuration(configs)

# VulcanTraderBot(config) and bot.startup() are each wrapped in a retry loop
# (30s backoff) instead of being allowed to crash the process. Exchange-side
# blips at this stage (rate limits, a temporary 5xx/451, markets not
# loading) have nothing to do with the strategy - killing the whole process
# over one means a silently-stopped fleet bot that needs a manual restart.
# The loop logs the exception and keeps retrying; the console window/tmux
# pane/dashboard/Discord all stay live and show it. A stop request (Ctrl+C,
# or the cross-process "<db>.json.stop" file - its watcher thread starts
# BEFORE this retry loop specifically so it can interrupt one) breaks out
# and exits cleanly instead. A `_load_configuration()` failure above this
# (bad config/strategy path) is still fatal - retrying can't fix that.
while bot is None and not stop_event.is_set():
    try:
        bot = VulcanTraderBot(config)      # __init__ (~5 phases, timed)
            └─ ExchangeResolver.load_exchange()
            └─ StrategyResolver.load_strategy()
            └─ init_db()                    # persistence bootstrap
            └─ Wallets(), PairListManager()
            └─ DataProvider()
            └─ PairListManager.refresh_pairlist()
            └─ ProtectionManager()
    except Exception:
        log + stop_event.wait(30)

WebPortal(bot).start(blocking=False)   # FastAPI in background thread
_attach_portal(bot, portal)            # rewires bot._emit → portal.send_msg() + _dispatch_discord()
# No DiscordBot here - the interactive (!) client runs from cmd_webserver()
# instead (control-plane concern, not tied to this subprocess's lifecycle -
# see "Multi-bot fleet control"). This bot's events still reach Discord via
# the webhook, through _dispatch_discord above.

while not stop_event.is_set():
    try:
        bot.startup()                      # one-shot: precision backfill, open-order sync
        break
    except Exception:
        log + stop_event.wait(30)
bot.state = State.RUNNING

# Main loop (runs until SIGINT/SIGTERM, or the cross-process "<db>.json.stop" file
# a web-portal Stop button / bot.py autostart-launched fleet member notices):
while not stop_event.is_set():
    bot.process()
    stop_event.wait(timeout=process_throttle_secs)

bot.cleanup()
```

Everything from the stop-file/forceexit-file watcher setup through this final
`bot.cleanup()` lives in one shared function, `bot.py::run_bot_lifecycle(config,
stop_event, *, no_web=False)` — `cmd_trade()` is now a thin wrapper that
resolves `config` from argparse, installs signal handlers (SIGINT/SIGTERM,
Windows console-close) around its own `stop_event`, and calls it. This is the
same function `launch_bot_in_process()` runs as a daemon thread when
`single_process_mode` is set — see "single_process_mode" under "Multi-bot
fleet control" below. Signal/console-handler installation deliberately stayed
in `cmd_trade()` rather than moving into `run_bot_lifecycle()`: `signal.signal()`
only works on the main thread, and under `single_process_mode` several bots'
worth of this function run as background threads in one process, so only the
process's own top-level entry point may touch OS signal handlers.

Cross-process force-exit works the same way: `<db>.json.forceexit` (a JSON list
of pair strings), polled every ~2s, drained into `_pending_force_exit_pairs` →
resolved to trade ids → merged into `_pending_force_exits` on the next
`process()` cycle. See "Multi-bot fleet control" below for who writes it.

### Config resolution

`--config live` resolves as:
1. Literal path if it exists.
2. `user_data/configs/live.json` (adds `.json` if no extension).

`_with_system_config()` then prepends `user_data/configs_system/system.json`
(if it exists) to the front of that list, so it's loaded first and any
per-bot config can still override individual keys (deep-merge, not
whole-block replace — see "Multi-bot fleet control").

---

## trader_bot.py — VulcanTraderBot

**Class:** `VulcanTraderBot(LoggingMixin)`

### Key instance attributes

| Attribute | Type | Purpose |
|---|---|---|
| `self.exchange` | `Exchange` | CCXT wrapper |
| `self.strategy` | `IStrategy` | Loaded user strategy |
| `self.dataprovider` | `DataProvider` | OHLCV + analyzed-df access |
| `self.pairlists` | `PairListManager` | Active whitelist |
| `self.wallets` | `Wallets` | Balance tracking |
| `self.protections` | `ProtectionManager` | Cool-down / drawdown guards |
| `self.state` | `State` | `RUNNING` / `STOPPED` / `RELOAD_CONFIG` |
| `self._exit_lock` | `Lock` | Serialises all exit-path code |
| `self._pending_force_exits` | `set[int]` | Trade IDs queued for force-exit (Discord, same-process) |
| `self._pending_force_exit_pairs` | `set[str]` | Pairs queued for force-exit from a cross-process request (`<db>.json.forceexit`) — resolved to trade ids each cycle |
| `self._last_heartbeat` | `datetime \| None` | Throttle for stdout heartbeat (1 min) |
| `self.last_process` | `datetime \| None` | Timestamp of last completed cycle |

### process() — per-cycle pipeline

```
exchange.reload_markets()
update_trades_without_assigned_fees()
trades = Trade.get_open_trades()
active_pair_whitelist = _refresh_active_whitelist(trades)
dataprovider.refresh(pairs, informative_pairs)
strategy.bot_loop_start()
strategy.analyze(active_pair_whitelist)          # populates analyzed dataframes

with _exit_lock:
    manage_open_orders()                         # cancel/replace timed-out orders

with _exit_lock:
    [drain _pending_force_exits → execute_trade_exit() for each]

with _exit_lock:
    exit_positions(trades)                       # ROI / SL / signal exits

if position_adjustment_enable:
    with _exit_lock:
        process_open_trade_positions()           # DCA / reduce

if state == RUNNING and get_free_open_trades():
    enter_positions()                            # new entries

_schedule.run_pending()                          # funding-fee updates, ws resets
Trade.commit()
_write_heartbeat()                               # stdout, once per minute
```

### Entry pipeline (`execute_entry`)

```
get_valid_enter_price_and_stake()   # price, stake, leverage validation
strategy.confirm_trade_entry()      # optional user gate
exchange.create_order()             # place limit/market order
Order.parse_from_ccxt_object()
trade.orders.append(order_obj)
trade.recalc_trade_from_orders()
Trade.commit()
wallets.update()
_notify_enter()  →  _emit()  →  portal.send_msg() + discord
```

### Exit pipeline (`execute_trade_exit`)

```
trade.set_funding_fees()
cancel_stoploss_on_exchange()
_safe_exit_amount()                 # wallet guard
strategy.confirm_trade_exit()       # optional user gate
exchange.create_order()             # place exit order
_exit_reason_cache[key] = dt_now()
trade.exit_reason = exit_reason
Trade.commit()
_notify_exit()  →  _emit()
```

### Notifications (`_emit`)

All bot events flow through `_emit(msg: dict)`, monkey-patched by
`bot.py::_attach_portal()` to do both of:
- `portal.send_msg(msg)` — buffers into the web portal's message deque.
- `self._dispatch_discord(msg)` — forwards to the Discord webhook via
  `util/discord_logger.py` (tagged `[bot_name]`, since one webhook is
  typically shared across every bot's config).

**Important:** both calls must stay in `_attach_portal`'s replacement `_emit`
— it fully replaces the class's own `_emit` method, so forgetting either one
silently kills that notification channel for every bot with a portal
attached (the default). This exact bug existed here before and was fixed.

- `msg["type"]` is an `RPCMessageType` enum value.
- Discord messages include: entry/exit fill text, SNR quality, R:R, Sharpe, CAGR, profit factor, and an OHLCV chart image (attached-bot path only — see below).
- `startup()` also emits one `RPCMessageType.STARTUP` message with the bot's balance and open-trade summary, so every launch produces an immediately verifiable Discord post.

### Heartbeat

`_write_heartbeat()` prints to stdout once per minute:
```
HEARTBEAT 2026-05-25T14:32:07+00:00 state=running open_trades=2
```
Parent process / monitoring wrapper parses this line to verify liveness.

---

## backtesting.py — Backtesting

**Class:** `Backtesting`

### Pipeline

```
Backtesting.__init__()
    └─ ExchangeResolver.load_exchange()
    └─ StrategyResolver.load_strategy()     # iterates strategy list
    └─ _set_strategy()                      # disables stoploss_on_exchange
    └─ disable_database_use()               # all trades in LocalTrade (RAM only)
    └─ PairListManager, Wallets, DataProvider

Backtesting.start()
    └─ load_bt_data()                       # fetch + slice OHLCV per timeframe
    └─ _get_ohlcv_as_lists()               # pre-compute signals into tuples
    └─ strategy.analyze_ticker()            # populate indicators / signals
    └─ time_pair_generator()               # candle-by-candle iterator
    └─ backtest_loop()
          ├─ _check_trade_profitability()   # ROI / SL / signal exit per candle
          ├─ handle_trade_roi()
          ├─ handle_stoploss_on_candle()
          ├─ enter_trade()                  # strategy entry signals
          └─ adjust_trade_position()        # DCA
    └─ generate_backtest_stats()            # aggregate trade ledger → stats dict
    └─ store_backtest_results()             # write JSON to user_data/backtest_results/
    └─ show_backtest_results()              # print summary table
```

**Key differences from live trading:**
- `LocalTrade` (in-memory) instead of persisted `Trade`.
- `disable_database_use()` / `enable_database_use()` toggle JSON persistence off.
- No exchange connectivity; candle data comes from local cache (`user_data/data/`).
- Stop-loss fills are assumed perfect (no slippage simulation).
- `bot.py` fans out multiple strategies concurrently via `asyncio.gather`.

---

## backtester/ — Rust engine + indicator bridge

**One** Rust crate that is both the backtest engine and the Python indicator
module. Strategies are never written in Rust — they are Python `IStrategy`
subclasses in `user_data/strategies/`. The crate holds no strategies.

`[lib]` is `name = "vulcan_rust_indicators"`, `crate-type = ["cdylib", "rlib"]`
(the lib is named for the Python module because a PyO3 extension's import name
must equal its cdylib name; the `[package]` is still `backtester`).

| Module | Purpose |
|---|---|
| `src/fast_indicators.rs` | The 23 standard indicators; `calculate_standard_indicators(close, high, low, volume) -> HashMap<usize, Vec<f32>>` |
| `src/cpu_engine.rs` | Simulation loop (`run_strategy_backtest*`, `run_param_sweep`) |
| `src/backtest.rs` | Shared types, configs, and the `Strategy` trait |
| `src/metrics.rs` | Result metrics (Sharpe, Sortino, CAGR, drawdown, …) |
| `src/python.rs` | PyO3 bridge (`#[cfg(feature = "python")]`) — the `vulcan_rust_indicators` module |

There is **no** `src/strategies/` — strategy ports were removed; strategies live
in Python. The `Strategy` trait and `&dyn Strategy` engine interface remain but
are exercised only by the crate's own Rust tests.

**`--engine rust -c …` (and the portal's Engine dropdown) does NOT call into the
Rust crate's `run_backtest` bridge function today**, despite the name — that
function, and the three Rust-side simulation loops it fronts
(`unified_backtest_pythonstyle`, `unified_backtest_pythonstyle_joint`,
`sweep_dir` in `cpu_engine.rs`), are reachable only from the crate's own
`cargo test`; nothing in the Python codebase calls them (verified by grepping
`rust_backtest.py`/`backtesting.py`/`web_portal.py`/`bot.py`). Flow:
`backtesting.py::backtest()` branches to `_backtest_rust()` when
`config["engine"]=="rust"` → `VulcanTrader/rust_backtest.py::run_rust_backtest()`
computes the strategy's signals the normal way, then runs them through
`_joint_multi_pair_backtest()` — a ~1300-line **pure-Python** bar-by-bar
simulator in that same file (the only real Rust call left in it,
`vri.calculate_standard_indicators`, is gated behind a documented-vestigial
`atr_stop_enabled` flag) — and formats trades into the standard
`BT_DATA_COLUMNS` DataFrame so `generate_backtest_stats` + the portal work
unchanged. Unlike the Rust-side loops, this Python simulator DOES invoke
`leverage()`/`custom_stake_amount()`/`confirm_trade_entry()`/`custom_exit()`/
`custom_stoploss()` per trade, with a real `self.dp` wired to the same
`Exchange`/`DataProvider` the plain Python engine uses — see `_confirm_entry()`/
`do_exit_check()` in `rust_backtest.py` for exactly where. `trading_mode: "spot"`
is handled too (`can_short` forced `False`, all funding/mark-price machinery
skipped) — this has been exercised end-to-end against real non-crypto exchange
data (Polymarket, prices bounded [0,1]) and produces identical trade-by-trade
results to `--engine python` on the same data.

### Feature gating (important)

- Default build / `cargo test` → pure engine, **no** PyO3 (`pyo3` is an optional
  dep). Tests link a real interpreter, so they must not enable extension-module.
- `--features extension-module` (what maturin builds with) → compiles
  `src/python.rs` and PyO3's extension-module mode, producing the
  `vulcan_rust_indicators` cdylib. Never enable this for `cargo test`.

### The bridge module

`src/python.rs` exposes one function as the Python module `vulcan_rust_indicators`:

```python
import vulcan_rust_indicators as vri
ind = vri.calculate_standard_indicators(close, high, low, volume)  # float64 arrays in
# ind: {index: [f32, ...]} — 23 series by fixed index; NaN during warmup
dataframe["rsi"] = ind[0]    # RSI(14)
dataframe["atr"] = ind[14]   # ATR(14)
```

Index → series map is documented in `AllIndicatorsDemoStrategy` (indices 0–22:
rsi, sma10/20/50, ema9/21/55, macd/signal/hist, bb_pos/upper/mid/lower, atr,
roc, mfi, cci, adx, fvg, vwap, chop, trend_eff). Periods are **fixed** (RSI 14,
ATR 14, …) — a strategy needing a tunable period must compute that one itself.

`src/python.rs` also exposes `run_backtest(open, high, low, close, volume, rsi,
macd_hist, bb_pos, atr, cci, entry_signals, exit_signals, direction,
config_json)` — a bridge to the Rust-side `unified_backtest*` loops. **Not
currently the live `--engine rust` path** (see above) — nothing in the Python
codebase calls it today; it exists for the crate's own `cargo test` and as a
ready-made faster-path if `_joint_multi_pair_backtest`'s pure-Python loop ever
becomes a bottleneck. `config_json` maps to `UnifiedBacktestConfig` fields
(which now derive `serde::Deserialize`, `#[serde(default)]`).

### Limit-order simulation

Entries and signal-driven exits are modelled as orders with a placement bar, a
price and a fill test (freqtrade's `_get_order_filled`: `low <= price <= high`),
in all three simulation loops — `unified_backtest_pythonstyle`,
`unified_backtest_pythonstyle_joint` and the `sweep_dir` fast path — and in
`rust_backtest.py`'s Python joint simulator.

| Config field (`UnifiedBacktestConfig`) | Meaning |
|---|---|
| `entry_order_type` / `exit_order_type` | `"limit"` (default, matching freqtrade's own `order_types`) or `"market"` |
| `entry_limit_offset` / `exit_limit_offset` | Fraction of price the resting order sits on the FAVOURABLE side of the bar open. **0 (default) = always fills instantly at the open**, identical to a market fill |
| `entry_fill_timeout_bars` / `exit_fill_timeout_bars` | Bars an unfilled order lives past its placement bar. `usize::MAX` (default) = no timeout, matching freqtrade with no `unfilledtimeout`; 0 = fill-or-kill |

`apply_overlay_to_ub_cfg` also accepts freqtrade's native shapes verbatim:
`order_types: {entry, exit}` and `unfilledtimeout: {entry, exit, unit}` (minutes
→ bars, using the config's own timeframe).

Fees: an order that RESTS (offset > 0) and later gets touched is a maker fill
and pays `fee_maker`; a marketable order and every protective exit pay
`fee_taker`. Stoploss / trailing / ROI / max-hold exits are never turned into
resting orders (`cpu_engine::is_signal_exit` draws the line) — they already
resolve to their own fill price, and making them "maybe fill" would leave
positions open through the moves a stop exists to escape.

Results carry `timedout_entry_orders` / `timedout_exit_orders` (plus
`entry_orders_placed` / `exit_orders_placed`), matching freqtrade's stats keys.
On the Python side the knobs come from the config's `limit_order_simulation`
block (`entry_offset`, `exit_offset`, `entry_timeout_bars`, `exit_timeout_bars`,
`fee_maker`), or from same-named strategy attributes, which win.

#### Strategy-driven orders (`rust_backtest.py`)

In the Python joint simulator a strategy places, tracks and cancels its own
limit orders through the normal IStrategy callbacks — the config offsets above
are only the fallback for strategies that don't:

| Callback | Role |
|---|---|
| `custom_entry_price` / `custom_exit_price` | **Place** — names the limit price; a strategy-priced order ignores the config offset entirely |
| `adjust_order_price` | **Track** — called every bar an order rests, with the real open `Order` on a real `LocalTrade`. Same rate = keep, different rate = cancel-and-replace (restarts the timeout clock, re-clamped into the candle), `None` = cancel |
| `check_entry_timeout` / `check_exit_timeout` (via `trader_check_timed_out`) | **Cancel** — checked before repricing, alongside the config `unfilledtimeout` |
| `order_filled` | Fired at the fill with that same order object, now closed |

The entry `LocalTrade` + `Order` are registered at PLACEMENT (`amount=0` until
filled), so callbacks that inspect open trades see the pending order the way
freqtrade's do; a cancelled entry removes the trade and frees its slot and
capital. Maker-vs-taker is decided per fill by where the order sat relative to
the bar open (`fill_fee`), which covers strategy-priced orders too. The engine
keeps its own trade accounting — `Order.close_bt_order`'s
`recalc_trade_from_orders` is deliberately not used (see `_close_order_object`).

### Strategy authoring pattern

- **Standard indicators** → pull from the bridge (identical to the engine, no
  TA-Lib recompute). Example: `AllIndicatorsDemoStrategy`.
- **Custom indicators** → compute in pandas/numpy. Examples: `FisherStatReversion`
  (Fisher Transform, return z-score, linreg slope), `IchimokuCloud` (full
  Ichimoku system). `DonchianBreakout` mixes both (bridge RSI/ATR + custom
  Donchian channels).

### Build notes (agents: read before touching Rust)

- `install.bat` / `install.sh` build the crate with
  `maturin develop --release --features extension-module` from
  `VulcanTrader/backtester` (maturin compiles the engine too).
- On Windows, `cargo`/`maturin` fail to link (`LNK1181: kernel32.lib`) unless the
  MSVC dev environment is loaded first. Load `vcvars64.bat` (VS 2022 BuildTools),
  e.g. from PowerShell:
  `cmd /c '"…\VC\Auxiliary\Build\vcvars64.bat" && cargo build --release --features extension-module'`.
- The bridge uses `abi3-py39` (one artifact for any CPython ≥ 3.9). Building
  against a Python newer than PyO3 knows needs `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`
  (the install scripts set it).
- Manual install without maturin: build with `--features extension-module`, then
  copy `VulcanTrader/backtester/target/release/vulcan_rust_indicators.dll` to
  `<site-packages>/vulcan_rust_indicators.pyd`.

---

## hyperopt/ — Hyperopt (Bayesian Optimiser)

**Entry-point:** `cmd_hyperopt()` in `bot.py` → `Hyperopt(config).start()`

### Module map

| Path | Purpose |
|---|---|
| `hyperopt/hyperopt/hyperopt.py` | Main `Hyperopt` class; orchestrates the optimisation loop |
| `hyperopt/hyperopt/hyperopt_optimizer.py` | Optuna study creation, objective function, sampler config |
| `hyperopt/hyperopt/hyperopt_auto.py` | Auto-space detection from strategy parameter declarations |
| `hyperopt/hyperopt/hyperopt_interface.py` | Abstract interface / mixin used by `Hyperopt` |
| `hyperopt/hyperopt/hyperopt_output.py` | Rich table rendering of epoch results |
| `hyperopt/hyperopt/hyperopt_logger.py` | Per-epoch logging helpers |
| `hyperopt/hyperopt_epoch_filters.py` | Filter epoch results (min trades, loss threshold, …) |
| `hyperopt/hyperopt_tools.py` | Serialise / deserialise epochs; load best params |
| `hyperopt/hyperopt_resolver.py` | Dynamically loads a `HyperOptLoss` class by name |
| `hyperopt/hyperopt_loss/` | Loss function implementations (see below) |
| `hyperopt/optimize_reports/bt_storage.py` | Persist epochs to disk |
| `hyperopt/optimize_reports/bt_output.py` | Format epoch output tables |
| `hyperopt/optimize_reports/optimize_reports.py` | Aggregate epoch stats |
| `optimize/space.py` | Parameter-space definitions (`IntSpace`, `DecimalSpace`, …) |

### Loss functions (`hyperopt/hyperopt_loss/`)

| File | Loss class |
|---|---|
| `hyperopt_loss_sharpe.py` | `SharpeHyperOptLoss` |
| `hyperopt_loss_sharpe_daily.py` | `SharpeHyperOptLossDaily` (default) |
| `hyperopt_loss_sortino.py` | `SortinoHyperOptLoss` |
| `hyperopt_loss_sortino_daily.py` | `SortinoHyperOptLossDaily` |
| `hyperopt_loss_calmar.py` | `CalmarHyperOptLoss` |
| `hyperopt_loss_max_drawdown.py` | `MaxDrawdownHyperOptLoss` |
| `hyperopt_loss_max_drawdown_relative.py` | `MaxDrawdownRelativeHyperOptLoss` |
| `hyperopt_loss_max_drawdown_per_pair.py` | `MaxDrawdownPerPairHyperOptLoss` |
| `hyperopt_loss_profit_drawdown.py` | `ProfitDrawDownHyperOptLoss` |
| `hyperopt_loss_onlyprofit.py` | `OnlyProfitHyperOptLoss` |
| `hyperopt_loss_short_trade_dur.py` | `ShortTradeDurHyperOptLoss` |
| `hyperopt_loss_multi_metric.py` | `MultiMetricHyperOptLoss` |

### Key CLI flags

```
--epochs / -e        Number of Optuna trials (default: 100)
--spaces             Spaces to search: buy sell roi stoploss trailing protection all
--hyperopt-loss      Loss function class name (default: SharpeHyperOptLossDaily)
-j / --jobs          Parallel workers; -1 = all CPUs
--min-trades         Discard epochs with fewer than N trades
--timerange          Date range (same format as backtest)
```

---

## web_portal.py — WebPortal (FastAPI)

**Class:** `WebPortal`

### Responsibilities

1. **Notification sink** — `send_msg(msg)` buffers up to 500 `RPCMessageType`-keyed dicts in a `deque`.
2. **Read-only REST API** — exposes live bot state, trades, wallet, pairlist, strategy metadata, analyzed candles, and backtest result files.
3. **Multi-bot account scan** — `_scan_live_bots()` reads every `user_data/accounts/*.json` and, for each, checks `bot.py::in_process_bot_running()` first (this process's own `single_process_mode` registry) and only falls back to probing the `.lock` file when that returns `None` (not one of this process's own bots) — see "single_process_mode" below for why the fallback alone isn't reliable for a bot hosted in THIS process. Tells "running" from "crashed" from "stopped" either way, and lets this portal show/control bots it isn't attached to in-process.
4. **Config manager** — CRUD + start/stop over `user_data/configs/*.json`, each treated as one bot definition (see "Multi-bot fleet control" below).
5. **Static asset host** — serves `template/` HTML files and `static/` assets.

### REST endpoints (all require `Authorization: Bearer <token>` except `/api/login`)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/login` | Returns bearer token |
| `GET` | `/api/status` | Bot state, exchange, strategy name |
| `GET` | `/api/messages?limit=N` | Recent `_emit` notifications |
| `GET` | `/api/trades/open` | Open trades with enriched fields |
| `GET` | `/api/trades/closed?limit=N` | Closed trade history |
| `GET` | `/api/whitelist` | Current pair whitelist |
| `GET` | `/api/locks` | Active `PairLock` entries |
| `GET` | `/api/backtests` | List backtest result files |
| `GET` | `/api/backtests/{name}` | Load a backtest result JSON |
| `GET` | `/api/strategy/info` | `plot_config` + strategy metadata |
| `GET` | `/api/pair/candles?pair=&tf=&n=` | Analyzed dataframe for a pair |
| `GET` | `/api/pair/trades?pair=` | Per-pair trade history |
| `GET` | `/api/livebots` | Scan `user_data/accounts/*.json` — every detected bot + running/crashed state |
| `POST` | `/api/livebots/{name}/stop` | Graceful stop - `stop_in_process_bot()` (instant, sets that bot's `stop_event` directly) if it's hosted in THIS process under `single_process_mode`, else the cross-process stop-file (`<db>.json.stop`) |
| `POST` | `/api/livebots/{name}/forceexit?pair=` | Cross-process force-exit (writes `<db>.json.forceexit`) |
| `GET` | `/api/dashboard?bot=<name>` | Full dashboard payload for another account's bot |
| `GET` | `/api/configs` | List configs with `is_bot` not explicitly false, each with `strategy`/`auto_start`/`dry_run`/`running`/`crashed` |
| `GET`/`PUT` | `/api/configs/{name}/raw` | Read/write a config's raw JSON |
| `POST` | `/api/configs/{name}/duplicate` \| `/rename` | Copy / rename a config file |
| `DELETE` | `/api/configs/{name}` | Delete a config (blocked while its bot is running) |
| `POST` | `/api/configs/{name}/start` \| `/stop` | Launch/stop that config - as its own bot process, or as a thread in THIS process under `single_process_mode` (see below) |
| `GET` | `/api/strategies` | Every `IStrategy` subclass found under `user_data/strategies/` |
| `GET` | `/api/exchanges` | Every exchange used by a config OR with existing `user_data/data/<exchange>/` contents (`_list_exchanges()`) |
| `GET` | `/api/data/coverage?exchange=` | Per pair+timeframe+candle_type OHLCV coverage for one exchange - start/end date, candle count, file size (`_data_coverage()`) |
| `POST` | `/api/data/download` | Queue a `download-data` job (same job queue as backtest/hyperopt/pairs_finder - list/get/cancel via the existing `/api/backtests/jobs*` routes, filtered client-side by `kind == "download"`) |

### Data manager (`template/datamanager.html`, served at `/datamanager`)
Per-exchange OHLCV coverage viewer + download-data launcher. `_data_coverage()`
reuses `IDataHandler`/`FeatherDataHandler` (`data/history/datahandlers/`) to
enumerate available (pair, timeframe, candle_type) tuples from filenames -
correctness stays in sync with what bots/backtester actually load - but
**does not** use `FeatherDataHandler.ohlcv_data_min_max()` for the date range,
since that loads the full dataframe (every OHLCV column, dtype casts) per
file. An exchange like Hyperliquid has 10,000+ per-pair/timeframe/candle-type
files; full loads made this endpoint take minutes. `_feather_date_min_max_count()`
instead reads ONLY the `date` column via `pyarrow.feather.read_table(path,
columns=["date"])`, fanned out across a 32-worker `ThreadPoolExecutor` (files
are read concurrently since pyarrow releases the GIL during the actual read) -
brings the same Hyperliquid scan down from "didn't finish in 120s" to ~12s.

### Authentication
Single bearer token derived from `config["api_server"]["password"]` (default `"VulcanTrader"`), compared with `secrets.compare_digest` (constant-time).

### Wiring into bot
`bot.py::cmd_trade()` calls `_attach_portal(bot, portal)` which monkey-patches `bot._emit` to call **both** `portal.send_msg(msg)` and `bot._dispatch_discord(msg)`.

`bot.py::cmd_webserver()` is where the interactive Discord `!`-client actually
starts (see "Multi-bot fleet control") — this portal is unattached
(`WebPortal(None)` or a config-less stub) but still fully account-scan-aware,
which is all `DiscordBot` needs for multi-bot control.

---

## Multi-bot fleet control

Running several `trader_bot.py` processes at once — one per config in
`user_data/configs/` — is a first-class, designed-for scenario, not an
afterthought. Three pieces make a config "one bot":

### Per-bot config fields (`config_schema.py`)

| Key | Type | Purpose |
|---|---|---|
| `is_bot` | bool | Default true. Gates two things: whether this config appears in `web_portal.py`'s `_list_configs()` (and hence the dashboard's Configs table), and whether `bot.py`'s `_scan_autostart_configs()` will ever launch it — set explicitly false and it's skipped by `autostart` even with `auto_start: true`. For a shared base/template config not meant to run as a bot in its own right. |
| `strategy` | str | Strategy class name for `trade`/`autostart` (functionally the same key freqtrade always used — `-s`/`--strategy` overrides it). **Ignored by `backtest`/`hyperopt`**: `_run_one_backtest()` force-overwrites `config["strategy"]` from the CLI-resolved value so one shared config can be backtested against many strategies without this field pinning it to one. |
| `auto_start` | bool | If true, `bot.py autostart` launches this config automatically (unless `is_bot` is explicitly false). |
| `show_terminal` | bool | Windows only, default true. New console window per launch, or none. |
| `tmux` | bool | Linux/macOS only, default false. New tmux window (session `"VulcanTrader"`) per launch instead of an invisible background process. Falls back to plain background if tmux isn't installed. |

`show_terminal`/`tmux` are deliberately **per-bot-config**, not system-wide —
different bots may want different terminal-visibility behavior. Read from
the specific config being launched by every launch call site (`bot.py`'s
`_scan_autostart_configs`, `web_portal.py`'s `_start_config_bot`/`api_start`),
never from `configs_system/system.json`.

`cmd_webserver()` calls `_launch_autostart_configs()` itself on every
startup (after Discord bot setup, before the blocking `portal.start()`) —
previously `auto_start: true` configs required a separate, manual
`bot.py autostart` invocation; starting just the dashboard/Discord process
launched nothing. Opt out with `--no-autostart` (e.g. running more than one
`webserver` instance and only wanting one responsible for the fleet, same
reasoning as `--no-discord-bot`). The port-picker in
`_launch_autostart_configs()` does a real socket-bind probe per candidate
port, not just "not already picked in this same batch" - the latter let an
auto_start config with no explicit `api_server.listen_port` try to grab
8060, the same port the calling webserver itself is normally already bound
to (confirmed directly: the launched bot's own dashboard failed to bind
while its trade loop kept running regardless - subtle enough to go
unnoticed for a while).

### The shared system config (`configs_system/system.json`)

`api_server`, `discord`, `telegram` used to be duplicated verbatim into every
config (same credentials copy-pasted N times). Now they live once in
`user_data/configs_system/system.json` and `bot.py::_with_system_config()`
prepends that file to every `-c` list before `_load_configuration()` runs —
loaded first, so a specific config (or a later CLI flag) can still override
one key via deep-merge (e.g. `configCoinbase.json` sets
`"discord": {"enabled": false, "webhook_url": "", "bot_token": ""}` to fully
opt out, without needing to also duplicate the rest of the shared block).

`configs_system/data_server.json` is unrelated — it's `data_server.py`'s own
auto-written config (see `trader_bot.py::_write_data_server_config()`), not
something a user edits directly. Its `show_terminal`/`tmux` fields (default
false for both) control the auto-launched master's own window visibility
the same way per-bot configs do for trade subprocesses (`ensure_master_
running()` in data_server.py, via `util/proc_launch.py::launch_detached()`
for the tmux case) — but set them on the launching bot's own `"data_server"`
config block (config_schema.py), not by hand-editing this file, since it's
fully overwritten on every bot startup. Same for `master_host` — see below.

### data_server: bind address vs dial address

Two different questions, two different settings. Conflating them is what
kept remote access from working at all before this split existed:

| Setting | Question | Default |
|---|---|---|
| `data_server.bind_host` (per-bot) / `data_server_bind_ip` (system.json) | which interface a master **launched by this bot** listens on | `127.0.0.1` |
| `data_server.host` (per-bot) / `data_server_ip` (system.json) | which address a bot **dials** to reach a master | `127.0.0.1` |

`ensure_master_running(host, port, ..., bind_host=...)` keeps them separate:
`connect_host` (probe + dial, with `0.0.0.0`/`""` normalized to loopback
because dialing a wildcard is meaningless) and `listen_host` (`bind_host or
connect_host`, passed as the spawned master's `--host`). It used to pass
`connect_host` as `--host`, so an auto-launched master was **always**
loopback-only no matter what `data_server.json`'s `master_host` said — the
config field was written but never took effect, because the CLI flag wins in
`main()`. Remote access only worked via an explicitly-run
`run-dataserver.bat`/`.sh` with `HOST` set.

`_write_data_server_config()` and the `ensure_master_running()` call now both
resolve through `trader_bot.py::_resolve_data_server_bind_host()`, so the
written file and the actual bind can never disagree. That matters because
the file *is* the only source of truth for a master a human later restarts
straight from it (`run-dataserver.bat` with no `HOST` set).

Accepted bind values: `127.0.0.1` (local only), `0.0.0.0` (every interface),
or a single interface IP. `MasterServer.start()` logs a WARNING whenever it
binds non-loopback — deliberate config, so a warning rather than an error,
but it must be visible: **neither port authenticates anything.** The client
port accepts `register_interest` (any reachable peer can steer what this
master collects) and the subserver port accepts `push_ohlcv`/`push_trades`,
which `DataCache.merge_*` writes into the feather files the backtester
reads. A hostile subserver connection can poison on-disk history. Private
networks or firewalled ports only.

Multi-machine fleets: master binds `0.0.0.0`; remote bots set
`data_server_ip` to its LAN address **and** `auto_start_data_server: false`,
otherwise each remote bot spawns its own useless local master when its first
probe of a not-yet-reachable address fails.

### Account naming (how a config's bot is identified while running)

A running bot's persistence account is always
`user_data/accounts/<config-filename>_<dry_run|live>.json` — keyed by the
**config's own filename** (unique by construction), not `strategy` or
`bot_name` (either could collide across configs). `_scan_live_bots()` /
`_list_configs()` both compute this same name to report `running`/`crashed`
status, so they always agree with what `_start_config_bot()`/`cmd_autostart()`
actually launched.

### Launching: `util/proc_launch.py::launch_detached()`

The function every launch call site goes through
(`cmd_autostart`, `_start_config_bot`, the legacy `api_start`) **when
`single_process_mode` is false** (the default) — see below for the
in-process alternative each of those same call sites branches to otherwise.
Given a `cmd` list and the specific bot config's dict:
- Windows: `show_terminal` → `CREATE_NEW_CONSOLE` or plain `Popen`.
- Linux/macOS: `tmux` → `tmux kill-window -t VulcanTrader:<bot_name>` first
  (clears any stale window from a crashed previous run of *this* bot, so
  restarts never pile up duplicate windows under the same name), then
  `new-window`/`new-session -s VulcanTrader -n <bot_name>`. Window name is
  the bot's own `bot_name` (sanitized to tmux-safe characters), not a
  generic label — falls back to plain background if tmux isn't installed.

### `single_process_mode` — run the whole fleet in one process

Fleet-wide flag in `configs_system/system.json` (`config_schema.py`, default
`false`). Motivated by per-process RAM overhead (see
[[trader-bot-ram-investigation]] in memory / `_ccxt_lazy.py`) — every
subprocess-per-bot launch pays its own cold `pandas`/`numpy`/`ccxt` import
and interpreter startup cost. Measured on a 3-bot fleet: **586 MB across 4
processes (subprocess mode) vs 186 MB in one process (single_process_mode)**,
and **10.35s vs 4.36s** aggregate wall-clock to bring all 3 up (first bot in
each process pays the cold-import tax; every bot after that is nearly free
within that same process). Trade-off: bots sharing one process compete for
the same GIL instead of getting genuine OS-level parallelism across cores -
usually negligible for I/O-bound strategies (mostly waiting on
exchange/data_server calls), more of a concern for many bots doing heavy
CPU-bound indicator work simultaneously.

**Every call site that used to unconditionally call `launch_detached()`
branches on this flag instead**: `bot.py::_launch_autostart_configs()`,
`web_portal.py::_start_config_bot()`, and the legacy `_stop_config_bot()`/
`api_start`/`api_stop` pair. When true, each launches
`bot.py::launch_bot_in_process(spec, user_data_dir, port)` instead of
spawning a subprocess:

- Builds this config's fully-resolved config dict the same way the `trade`
  subcommand's CLI args would (config file(s) + `system.json` + the same
  `db_url`/`port`/`bot_name` overrides `cmd_trade()` applies), then runs
  `run_bot_lifecycle(config, stop_event)` (see "cmd_trade() pipeline" above -
  the exact same function subprocess mode uses) as a daemon `threading.Thread`.
- Registers the thread + its own `stop_event` in a module-level registry
  (`bot.py::_IN_PROCESS_BOTS`, keyed by account name) so it can be looked up
  again: `stop_in_process_bot(name)` (instant stop, used by `/api/livebots/
  {name}/stop`), `in_process_bot_running(name)` (preferred over the lock-file
  probe for accounts hosted in this process - see "Why not just the lock
  probe" below), and `stop_all_in_process_bots()` (fan-out, wired into
  `WebPortal`'s `on_shutdown` callback so Ctrl+C on the fleet-control process
  also gracefully stops every bot it's hosting, not just its own uvicorn
  server).
- Everything downstream of that — persistence file, OS-level writer lock,
  stop-file/forceexit-file watcher threads, heartbeat, embedded `WebPortal`
  on its own port — behaves **identically** to the subprocess path; only the
  OS-process boundary is gone. This is why `/api/livebots/{name}/stop`'s
  cross-process stop-file mechanism *also* still works unmodified for an
  in-process bot: its watcher thread is running regardless of which process
  hosts it.

**Persistence isolation** (why this needed more than a launcher change):
`persistence/base.py`'s `PersistenceContext` holds one bot's entire
Trade/Order/PairLock/CustomData/KeyValueStore state, bound via `contextvars`
(not `threading.local()` - contextvars propagate correctly through FastAPI's
sync-endpoint threadpool dispatch, thread-locals don't) instead of the
class-level lists a single-bot-per-process design could get away with. A
thread that never called `init_db()` - or wasn't started via
`util/ctx_thread.py::start_thread_with_context()` from one that did - sees
**no** bot's data at all (raises, doesn't silently leak another bot's). See
the "persistence/" section below for the full contextvar design.

**Why not just the lock probe (`WebPortal._probe_db_lock`)**: fcntl advisory
locks on POSIX are scoped per-*process*, not per file-handle - a probe issued
from the very process that already holds the lock (true whenever the account
being scanned is hosted in this same `single_process_mode` process) always
reports "free" there, so the fleet scan needs `in_process_bot_running()`
checked first. Also: a bot's graceful in-process shutdown does **not**
release its OS writer-lock fd by itself (fds are process-scoped, not
thread-scoped - closing a thread doesn't close fds it opened), so
`persistence/models.py::release_writer_lock()` explicitly closes it,
called from `run_bot_lifecycle`'s cleanup path right after
`bot.cleanup()`/`portal.cleanup()`. Skipping this left a cleanly-stopped
bot's account permanently reporting `running: true` for the rest of the
process's life even though its own `is_running` KV flag correctly read 0 -
found live, not by a unit test.

### Discord: multi-bot `!` prefix commands (`util/discord_bot.py`)

`DiscordBot` runs from `bot.py::cmd_webserver()` — a control-plane concern
belonging to the one persistent process watching the whole fleet, started
whenever `discord.bot_token` is set **and** `discord.enabled` isn't false
(and `--no-discord-bot` wasn't passed). **Not** from `cmd_trade()` — a
`trade` subprocess never runs the interactive client, only the plain
webhook (`_dispatch_discord`), so this doesn't tie the Discord control plane
to any one trading bot's lifecycle. Commands take a required `bot` argument
(the account name from `/api/livebots`/`!bots`):

| Command | Behavior |
|---|---|
| `!bots` | List every detected account: running/crashed/stopped, strategy, exchange, PnL |
| `!opentrades <bot>` | Open trades from that bot's account JSON snapshot |
| `!exit <bot> <pair>` | Cross-process force-exit (writes `<db>.json.forceexit`, drained on that bot's next cycle) |
| `!stats <bot>` | Session stats computed from the account JSON (no live CAGR/Sharpe/ROI - those need a starting balance this process has no reliable way to know for a bot it isn't attached to) |
| `!stop <bot>` | Cross-process graceful stop (writes `<db>.json.stop`) |

`DiscordBot` also supports an "attached bot" fast path (live rate, chart,
full CAGR/Sharpe/ROI stats, in-process pause instead of terminate) when
constructed with `trading_bot=<VulcanTraderBot instance>` — but no current
call site does that; `cmd_webserver()`'s portal is always unattached
(`trading_bot=None`), so every command today goes through the JSON-snapshot
path uniformly, even for a bot on the same machine.

**Only run one interactive client per Discord bot token.** If you run more
than one `webserver` instance (e.g. a secondary read-only dashboard
elsewhere), pass `--no-discord-bot` on all but one — two gateway logins
sharing one token fight over the same session (session invalidation, or
interactions landing on a stale session).

Requires the Message Content Intent enabled in the Discord Developer Portal
(Bot page → Privileged Gateway Intents) — prefix commands need to read raw
message text; without it the client logs in but never sees a `!command`.

---

## persistence/

**No SQLAlchemy.** All state is Python objects serialised to a single JSON
file, held in a `contextvars`-scoped `PersistenceContext` — **not**
class-level lists (that was the design before `single_process_mode`; several
bots sharing one interpreter would otherwise all read/write the same global
`Trade`/`Order`/... lists and corrupt each other's ledgers, exactly the
`LocalTrade.bt_trades*` process-global problem `backtesting.py` already
avoids by requiring separate worker processes per concurrent run - see
`bot.py::_backtest_worker`'s docstring for that precedent).

### `PersistenceContext` (`base.py`)

One bot's entire store: `_instances`/`_next_id` dicts keyed by model class,
`active_path`, `writer_lock_fd`/`writer_lock_path`, `save_callback`, its own
`save_lock`. Bound to the module-level `_current_db: ContextVar
[PersistenceContext | None]`. `ModelBase._instances()`/`_add_instance()`/
`_delete_instance()`/`_reset_instances()` all resolve the ambient context via
`_ctx()` (raises `RuntimeError: No persistence context bound` if none is
bound on the calling thread/task - loud failure instead of silently reading
another bot's data or nothing).

**Isolation mechanic**: a plain `threading.Thread` starts with a *blank*
contextvars context by design - that blankness IS what keeps two bots'
threads from seeing each other's `PersistenceContext`, since neither one's
`init_db()`-bound context propagates to a new OS thread automatically. Any
thread spawned *on behalf of* an already-initialised bot (its own embedded
`WebPortal` server thread, its stop-file/forceexit-file watcher threads) must
instead go through `util/ctx_thread.py::start_thread_with_context(target,
...)`, called from the bot's own already-bound thread, which does
`contextvars.copy_context()` and runs `target` inside it. `asyncio.to_thread`
/ FastAPI's sync-endpoint threadpool dispatch already copy the ambient
context automatically (stdlib guarantee) - confirmed live: a bot's embedded
dashboard's `/api/trades/open` etc. resolve correctly with zero extra wiring.

### init_db(db_url)

- Accepts `json:///path/to/file.json` or legacy `sqlite:///` (rewritten to `.json`).
- Re-initialising the **same path from the same context** (tests,
  `RELOAD_CONFIG`) reuses the already-held writer lock and context identity;
  re-initialising a **different** path, or the same path from a **different**
  context, creates a fresh `PersistenceContext` and acquires a new OS-level
  writer lock (`_acquire_writer_lock` - `msvcrt.locking` on Windows,
  `fcntl.flock` elsewhere) on a sibling `<name>.lock` file, refusing outright
  (`OperationalException`) if another writer already holds it.
- Loads existing JSON into that context's `_instances`, keyed by class -
  `Trade`, `Order`, `PairLock`, `_CustomData`, `_KeyValueStoreModel`. A
  malformed file is backed up to `<name>.corrupt-<timestamp>` and the context
  starts empty rather than crashing or silently overwriting it.
- Binds `ctx.save_callback = functools.partial(_save_snapshot, ctx)` — every
  `Trade.session.commit()` (really `_FakeSession.commit()`, resolving the
  *ambient* context) invokes that same bound `ctx`'s callback, so the
  snapshot writer always sees the same context it committed against, by
  construction (see `_save_snapshot`'s docstring).
- `release_writer_lock()` (called from `bot.py::run_bot_lifecycle`'s cleanup
  path) explicitly closes the ambient context's writer-lock fd and clears its
  `save_callback`. Needed under `single_process_mode` specifically: a
  subprocess-per-bot's OS-level exit releases every fd it holds for free, but
  one bot's thread exiting inside a still-alive shared process does not (fds
  are process-scoped, not thread-scoped) - see "single_process_mode" above.

### Atomic writes (`_save_snapshot`)

write-to-per-PID-tmp-file → flush → `fsync` → read-back-and-verify-parses →
`os.replace(tmp, active_path)` (via `_replace_with_retry`). The verify step
means a bad snapshot (serialization bug, disk fault, mid-write crash) can
never become the live file - the previous good snapshot survives untouched,
and any failure is logged, not raised (a bad commit shouldn't crash the trade
loop). `_replace_with_retry` retries `os.replace()` itself with capped
exponential backoff (up to 25 attempts, ~a few seconds worst case) because
Windows refuses to replace a file another handle currently has open for
reading (no `FILE_SHARE_DELETE`) - and a concurrent reader of THIS exact file
is real and reachable: `web_portal.py`'s account scan (behind `/api/livebots`)
reads every account JSON directly off disk, including an in-process bot's own
file while that bot is mid-commit. Found via a deliberately adversarial
concurrent read/write stress test, not the live 2-bot smoke test (dashboard
polling alone was too infrequent to reliably hit the window).

### Key classes

| Class | Module | Purpose |
|---|---|---|
| `Trade` | `trade_model.py` | Open/closed trade record; query methods resolve the ambient context's instance list |
| `Order` | `trade_model.py` | CCXT order mirror; one-to-many with `Trade` |
| `LocalTrade` | `trade_model.py` | Backtesting-only in-memory trade (process-global by design - see `bot.py::_backtest_worker`, unrelated to `PersistenceContext`) |
| `PairLock` | `pairlock.py` | Time-bounded trading locks per pair or global |
| `CustomDataWrapper` | `custom_data.py` | Strategy-scoped key/value blob storage |
| `KeyValueStore` | `key_value_store.py` | Bot-scoped persistent key/value store |
| `PersistenceContext` | `base.py` | Per-bot contextvar-bound store - see above |
| — | `models.py` | `init_db`/snapshot save-load, writer-lock, `_replace_with_retry` |
| — | `pairlock_middleware.py` | Middleware helpers for PairLock queries |
| — | `usedb_context.py` | Context manager for enabling/disabling persistence |

### Important Trade methods

```python
Trade.get_open_trades()                    # list of open trades
Trade.get_open_trade_count()               # integer count
Trade.get_closed_trades_without_assigned_fees()
Trade.get_trades(filter_fn)               # filter by callable predicate
Trade.stoploss_reinitialization(sl)        # re-apply stoploss on startup
trade.calc_profit_ratio(rate)              # unrealised PnL ratio
trade.recalc_trade_from_orders()           # recompute open_rate/amount from fills
Trade.commit()                             # flush to JSON
```

---

## pairlist/

Each pairlist handler inherits `IPairList` (abstract base in `IPairList.py`).

### Handler pipeline

`PairListManager.refresh_pairlist()` iterates `_pairlist_handlers` in order. Each handler implements:

```python
def filter_pairlist(self, pairlist: list[str], tickers: Tickers) -> list[str]:
    ...  # return filtered / sorted list
```

The output of handler N is the input to handler N+1 (pipeline pattern).

### Built-in handlers

| Handler | Behaviour |
|---|---|
| `StaticPairList` | Fixed list from config |
| `VolumePairList` | Top-N by 24h quote volume |
| `PercentChangePairList` | Top-N by % price change |
| `MarketCapPairList` | Top-N by market cap |
| `AgeFilter` | Min listing age |
| `PriceFilter` | Min/max price bounds |
| `SpreadFilter` | Max bid/ask spread |
| `VolatilityFilter` | ATR-based volatility range |
| `RangeStabilityFilter` | Exclude ranging pairs |
| `PerformanceFilter` | Exclude recent losers |
| `FullTradesFilter` | Exclude pairs at max open trades |
| `ShuffleFilter` | Random ordering |
| `OffsetFilter` | Slice offset into list |
| `DelistFilter` | Exclude delisting pairs |
| `RemotePairList` | Fetch list from URL |
| `ProducerPairList` | Consume from external bot producer |
| `PairInformationFilter` | Filter by exchange pair metadata fields |
| `PrecisionFilter` | Exclude pairs with insufficient price precision |

---

## strategy/

### IStrategy (interface.py)

Abstract base class every user strategy must subclass.

```python
class MyStrategy(IStrategy):
    timeframe = "5m"
    stoploss = -0.10
    minimal_roi = {"0": 0.05}

    # --- Required ---
    def populate_indicators(self, df: DataFrame, metadata: dict) -> DataFrame: ...
    def populate_entry_trend(self, df: DataFrame, metadata: dict) -> DataFrame: ...
    def populate_exit_trend(self, df: DataFrame, metadata: dict) -> DataFrame: ...

    # --- Optional overrides ---
    def custom_stoploss(self, pair, trade, current_time, current_rate, current_profit, **kw) -> float: ...
    def custom_exit(self, pair, trade, current_time, current_rate, current_profit, **kw) -> str | bool: ...
    def confirm_trade_entry(self, pair, order_type, amount, rate, ...) -> bool: ...
    def confirm_trade_exit(self, pair, trade, order_type, amount, rate, ...) -> bool: ...
    def adjust_trade_position(self, trade, current_time, current_rate, ...) -> float | None: ...
    def bot_loop_start(self, current_time, **kw): ...
    def trader_bot_start(self): ...
```

### Signal columns

`populate_entry_trend` must set:
- `df["enter_long"] = 1` / `df["exit_long"] = 1`
- `df["enter_short"] = 1` / `df["exit_short"] = 1` (only if `can_short = True`)
- `df["enter_tag"]` / `df["exit_tag"]` (optional string labels)

### HyperStrategyMixin (hyper.py)

Provides `IntParameter`, `DecimalParameter`, `CategoricalParameter`, `BooleanParameter` descriptors for hyperopt parameter declaration.

### parameters.py

`HyperoptParameter` base class and the concrete `IntParameter`, `DecimalParameter`, `CategoricalParameter`, `BooleanParameter` implementations with `optimize`, `value`, and `in_space` helpers.

### informative_decorator.py

`@informative(timeframe)` decorator — attaches informative-pair logic to a strategy method so `DataProvider` fetches and merges the pair at the specified timeframe automatically.

### strategy_helper.py

Utility functions for merging informative pairs into the main dataframe (`merge_informative_pair`).

### strategy_validation.py

Post-load strategy sanity checks (`validate_requires_candle_count`, column presence assertions, etc.).

### strategyupdater.py

Converts legacy Freqtrade v2 strategy syntax to v3 (`StrategyUpdater`).

### strategy_wrapper.py

`strategy_safe_wrapper(fn, default_retval)` wraps any strategy method call to catch exceptions and return `default_retval`, so a buggy strategy method never crashes the bot loop.

### Resolvers

`StrategyResolver.load_strategy(config)` searches `user_data/strategies/` (and `strategy_path` if set) for the class named by `config["strategy"]`, imports it, validates `INTERFACE_VERSION`, and returns an instance.

---

## config/

### Configuration(args_dict, runmode).get_config()

Merges (in order):
1. Default values from `util/config_schema.py`
2. Files listed in `args["config"]` (later files override earlier)
3. Environment variables (`FREQTRADE__` prefix; handled by `environment_vars.py`)
4. CLI overrides from `args_dict`

`config_validation.py::validate_config_consistency()` is called after the strategy is loaded (strategies may set options like `can_short`).

`config_secrets.py` strips exchange credentials from the config dict before passing to non-exchange code (`remove_exchange_credentials`).

### Key config keys

| Key | Type | Description |
|---|---|---|
| `exchange.name` | str | ccxt exchange id |
| `exchange.pair_whitelist` | list[str] | Seed list for StaticPairList |
| `stake_currency` | str | e.g. `"USDT"` |
| `stake_amount` | float \| `"unlimited"` | Per-trade stake |
| `max_open_trades` | int | Concurrent trade cap |
| `timeframe` | str | Primary OHLCV timeframe |
| `dry_run` | bool | Paper trading mode |
| `trading_mode` | str | `"spot"` / `"futures"` |
| `margin_mode` | str | `"isolated"` / `"cross"` |
| `db_url` | str | `json:///user_data/trades.live.json` — ignored by `bot.py autostart`/config-manager Start, which always derive `<config-name>_<dry_run\|live>.json` instead |
| `api_server.listen_port` | int | Web portal port (default 8060) — normally set once in `configs_system/system.json`, per-instance `--port` CLI override still wins |
| `api_server.password` | str | Bearer token password |
| `discord.webhook_url` | str | Trade notification webhook (usually shared via `configs_system/system.json`; messages tagged `[bot_name]`) |
| `discord.bot_token` | str | `!`-prefix command bot token — see "Multi-bot fleet control"; only one bot in a fleet should actually run the client |
| `is_bot` | bool | See "Multi-bot fleet control" — default true, gates Configs-table visibility and `autostart` eligibility |
| `strategy` | str | See "Multi-bot fleet control" — ignored by `backtest`/`hyperopt` |
| `auto_start` | bool | See "Multi-bot fleet control" |
| `show_terminal` | bool | See "Multi-bot fleet control" |
| `tmux` | bool | See "Multi-bot fleet control" |
| `single_process_mode` | bool | Fleet-wide, `configs_system/system.json` only (default false) — see "single_process_mode" under "Multi-bot fleet control" |
| `pairlists` | list[dict] | Ordered pairlist handler configs |
| `protections` | list[dict] | Protection handler configs |

---

## util/ highlights

| Module | Key exports | Purpose |
|---|---|---|
| `pairlistmanager.py` | `PairListManager` | Orchestrates pairlist handler pipeline |
| `protectionmanager.py` | `ProtectionManager` | Evaluates cool-down / drawdown protections |
| `discord_bot.py` | `DiscordBot` | Multi-bot `!`-prefix command interface (runs in daemon thread) — see "Multi-bot fleet control" |
| `discord_logger.py` | `log`, `log_startup`, `log_trade_chart`, … | Fire-and-forget webhook poster, tags messages `[bot_name]` |
| `proc_launch.py` | `launch_detached` | Spawns a `trader_bot.py` subprocess honouring that config's `show_terminal`/`tmux` — the `single_process_mode: false` path, see "Multi-bot fleet control" |
| `ctx_thread.py` | `start_thread_with_context` | Spawns a thread that inherits the caller's `contextvars` context — required for any thread spawned on behalf of an already-`init_db()`'d bot under `single_process_mode`, see "persistence/" |
| `trade_chart.py` | `render_trade_chart` | Matplotlib PNG chart for Discord |
| `optimize_reports.py` | `generate_backtest_stats`, `store_backtest_results` | Backtest stats aggregation + JSON write |
| `bt_progress.py` | `BTProgress` | Rich progress bar for backtest |
| `exceptions.py` | `DependencyException`, `ExchangeError`, … | Typed exception hierarchy |
| `datetime_helpers.py` | `dt_now`, `dt_from_ts` | UTC-aware datetime helpers |
| `ft_precise.py` | `FtPrecise` | Arbitrary-precision decimal arithmetic |
| `ft_ttlcache.py` | `TTLCache` | TTL-bounded dict |
| `protections.py` | `CooldownPeriod`, `MaxDrawdown`, `StoplossGuard`, … | Individual protection implementations |
| `bias_analysis.py` | `LookaheadAnalysis`, `RecursiveAnalysis` | Look-ahead / recursive bias checkers |
| `backtest_caching.py` | — | Caching helpers for backtest data |
| `migrations.py` | — | JSON database migration utilities |
| `misc.py` | — | Miscellaneous small helpers |
| `logger.py` | — | Logging setup / configuration |
| `rpc_types.py` | — | Typed dicts for RPC message payloads |
| `formatters.py` | — | Output formatting helpers |
| `liquidation_price.py` | `calc_liquidation_price` | Futures liquidation price calculation |
| `interest.py` | `calc_interest` | Margin interest calculation |

---

## enums/

All enum types live in `VulcanTrader/enums/`. Key ones:

| Enum | Values |
|---|---|
| `State` | `RUNNING`, `STOPPED`, `RELOAD_CONFIG` |
| `RunMode` | `LIVE`, `DRY_RUN`, `BACKTEST`, `HYPEROPT`, `UTIL_EXCHANGE`, `WEBSERVER` |
| `ExitType` | `STOP_LOSS`, `TRAILING_STOP_LOSS`, `ROI`, `EXIT_SIGNAL`, `CUSTOM_EXIT`, `FORCE_EXIT`, `EMERGENCY_EXIT`, `LIQUIDATION` |
| `RPCMessageType` | `ENTRY`, `ENTRY_FILL`, `ENTRY_CANCEL`, `EXIT`, `EXIT_FILL`, `EXIT_CANCEL`, `STATUS`, `WARNING`, `STARTUP`, `EXCEPTION` |
| `TradingMode` | `SPOT`, `MARGIN`, `FUTURES` |
| `MarginMode` | `ISOLATED`, `CROSS` |
| `SignalDirection` | `LONG`, `SHORT` |
| `SignalType` | `ENTER_LONG`, `EXIT_LONG`, `ENTER_SHORT`, `EXIT_SHORT` |
| `CandleType` | `SPOT`, `FUTURES`, `MARK`, `INDEX`, `PREMIUMINDEX`, `FUNDING_RATE` |
| `PriceType` | `OPEN`, `HIGH`, `LOW`, `CLOSE`, `SAME_AS_OPEN` |
| `OrderTypeValue` | `LIMIT`, `MARKET` |
| `HyperoptState` | `OPTIMIZE`, `LOSS`, `RESULTS_EXPLAIN`, `RESULTS_SAVE` |
| `BacktestState` | `STARTUP`, `RUNNING`, `RESULTS_WAIT`, `RESULTS_FINAL` |
| `MarketStateType` | `BULL`, `BEAR`, `SIDEWAYS`, `UNKNOWN` |
| `ExitCheckTuple` | Named tuple: `exit_type`, `exit_reason` |

---

## Common patterns

### Adding a new bot-level feature
1. Add instance attribute in `VulcanTraderBot.__init__`.
2. Implement logic in a private method (`_my_feature()`).
3. Call from `process()` in the correct phase (before/after exit lock as appropriate).
4. Notifications go through `self._emit({"type": RPCMessageType.STATUS, ...})`.

### Adding a new API endpoint
Edit `web_portal.py`. Add a route under the existing `app` FastAPI instance. Use the `_verify_token` dependency for authentication. Serialise with `_json_safe()`.

### Adding a new pairlist filter
Subclass `IPairList`, implement `filter_pairlist()`, place the file in `VulcanTrader/pairlist/`, and add the class name to `AVAILABLE_PAIRLISTS` in `constants.py`.

### Adding a new strategy method
Override it in your strategy class in `user_data/strategies/`. The `IStrategy` base provides safe defaults for all optional methods. Wrap risky calls in the strategy with `strategy_safe_wrapper`.

### Persistence changes
Never call `Trade.session.add()` / `Trade.commit()` from background threads without first acquiring `_exit_lock`. All persistence mutations in `process()` are already within that lock or directly after `Trade.commit()`.

---

## Run scripts

| Script | Purpose |
|---|---|
| `run-live.bat` / `run-live.sh` | Live trading (real orders) |
| `run-paper.bat` / `run-paper.sh` | Dry-run paper trading |
| `run-app.bat` / `run-app.sh` | Web portal standalone (`python -m VulcanTrader.bot webserver`) |
| `main.py` | Same as `run-app`'s underlying command, run directly: `python main.py [--port ...]`. Whether the fleet it starts spawns subprocesses or runs everything in this one process is controlled by `single_process_mode` in `configs_system/system.json`, not by which of these you use. |
| `install.bat` / `install.sh` | Create `.venv` and install deps |

Environment variables `CONFIG`, `STRATEGY`, `DB_URL` override defaults in all run scripts.

---

## Testing

`tests/` — stdlib `unittest`, no pytest in the venv (don't add it as a
dependency without checking first). Run everything:

```
python -m unittest discover -s tests -v
```

| File | Covers |
|---|---|
| `test_persistence_single_process.py` | `PersistenceContext` isolation across concurrent threads, `start_thread_with_context` propagation, same-path/same-thread reinit reuse, second-writer rejection, `release_writer_lock()` regression |
| `test_persistence_integrity.py` | All five model types isolated (not just `Trade`) across many bots, save→reload round-trip fidelity (incl. two bots "restarting" concurrently), a real bot's own trade-loop-writes/dashboard-reads concurrency pattern under load, atomic-write crash-safety (failed snapshot leaves the live file untouched, no stray tmp file), corrupt-file recovery isolation between bots, `_replace_with_retry`'s Windows sharing-violation retry |
| `test_bot_single_process_launcher.py` | `launch_bot_in_process`/`stop_in_process_bot`/`in_process_bot_running`/`stop_all_in_process_bots` registry mechanics — mocks out `run_bot_lifecycle` itself so these run fast and offline, no exchange/network needed |

None of this replaces a live check when touching `single_process_mode` or
`persistence/`: run `main.py` (or `webserver` with `single_process_mode:
true`) against two real dry-run configs and confirm via `/api/livebots` +
each bot's own `/api/dashboard` that they stay isolated, both before and
after a stop.
