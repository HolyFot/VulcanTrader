# =============================================================================
#  VulcanBot  ::  Top-level orchestrator
# =============================================================================
#
#  Purpose
#  -------
#  Single CLI entry-point that hosts and routes between the three runtime
#  components of VulcanTrader:
#
#      * :class:`Backtesting`  (src/backtesting.py)   — historical replay
#      * :class:`VulcanTraderBot` (src/trader_bot.py)    — live trading daemon
#      * :class:`WebPortal`    (src/web_portal.py)    — FastAPI dashboard
#
#  All invocation paths funnel through this file so configuration loading,
#  user_data layout and notification wiring stay consistent.
#
#  Subcommands
#  -----------
#      backtest        Run one or many strategies through the backtester.
#                      Supports asyncio fan-out so several strategy/config
#                      combinations can run concurrently.
#      download-data   Pull OHLCV history for the configured pairs/timeframes.
#      trade           Launch the live (or dry-run) trading bot, attaching
#                      the web portal for monitoring and notifications.
#      webserver       Run the web portal standalone (browse backtest
#                      results without spinning up the trader).
#
#  User-data layout
#  ----------------
#      user_data/
#        configs/             ← default location for *.json config files
#        data/                ← OHLCV cache (per exchange / timeframe)
#        strategies/          ← user strategy modules
#        backtest_results/    ← JSON output consumed by /api/backtests
#
#  Config resolution
#  -----------------
#  Any ``--config`` value passed to a subcommand is resolved as follows:
#      1. If the path exists as given, use it verbatim.
#      2. Otherwise, look it up under ``<user_data>/configs/<name>``,
#         appending ``.json`` if no extension is supplied.
#  This mirrors the layout requested by the user (configs live in
#  ``user_data/configs``) without forcing changes inside backtesting.py
#  or trader_bot.py — paths are normalised before the underlying
#  ``Configuration`` loader is invoked.
#
#  Examples
#  --------
#      python -m VulcanTrader.bot backtest -c live -s MyStrat \
#             --timeframe 5m --timerange 20240101-20240601
#
#      python -m VulcanTrader.bot backtest -c live \
#             --strategies StratA StratB StratC --jobs 3
#
#      python -m VulcanTrader.bot download-data -c live \
#             --pairs BTC/USDT ETH/USDT --timeframes 1m 5m 1h \
#             --days 90
#
#      python -m VulcanTrader.bot trade -c live --strategy MyStrat
#
#      python -m VulcanTrader.bot webserver --port 8080
# =============================================================================

"""CLI orchestrator (see header comment above for details)."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Sequence

# On Windows both asyncio loop types emit spurious connection-error tracebacks:
#   SelectorEventLoop  → ConnectionResetError   [WinError 10054] in _read_from_self
#   ProactorEventLoop  → ConnectionAbortedError [WinError  1236] on shutdown
# Neither is a real error. We keep the SelectorEventLoop (better ccxt compat)
# and suppress both via a logging filter on the 'asyncio' logger.
if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

    class _WinAsyncioNoiseFilter(logging.Filter):
        """Suppress harmless Windows asyncio self-pipe connection errors."""
        _SUPPRESSED = (ConnectionResetError, ConnectionAbortedError)

        def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
            if record.exc_info:
                exc = record.exc_info[1]
                if isinstance(exc, self._SUPPRESSED):
                    return False
            msg = record.getMessage()
            if "WinError 10054" in msg or "WinError 1236" in msg:
                return False
            return True

    logging.getLogger("asyncio").addFilter(_WinAsyncioNoiseFilter())

logger = logging.getLogger("VulcanTrader.bot")


# ---------------------------------------------------------------------------
#  Paths / config resolution
# ---------------------------------------------------------------------------

DEFAULT_USER_DATA = Path("user_data")
DEFAULT_CONFIG_SUBDIR = "configs"
DEFAULT_SYSTEM_CONFIG_SUBDIR = "configs_system"
SYSTEM_CONFIG_NAME = "system.json"


def _user_data_dir(args: argparse.Namespace) -> Path:
    """Return the active user_data directory (CLI flag wins over default)."""
    raw = getattr(args, "user_data_dir", None) or DEFAULT_USER_DATA
    return Path(raw).resolve()


def _configs_dir(args: argparse.Namespace) -> Path:
    return _user_data_dir(args) / DEFAULT_CONFIG_SUBDIR


def _configs_system_dir(args: argparse.Namespace) -> Path:
    return _user_data_dir(args) / DEFAULT_SYSTEM_CONFIG_SUBDIR


def _with_system_config(configs: list[str], args: argparse.Namespace) -> list[str]:
    """Prepend the shared system-level config, if present.

    api_server (host, port default, jwt_secret_key, username/password) plus
    discord/telegram are system-wide concerns, not per-strategy ones - they
    used to be duplicated verbatim into every config under user_data/configs,
    which meant rotating a credential meant editing N files. Loaded first
    here so a bot's own -c config(s), and later --port/--host CLI overrides,
    still win if they explicitly set something (last-file-wins / CLI-wins
    semantics unchanged).

    Note: "show_terminal"/"tmux" (see config_schema.py) are deliberately
    NOT here - those live per-bot-config instead, since different bots may
    want different terminal-visibility behavior.
    """
    sys_cfg = _configs_system_dir(args) / SYSTEM_CONFIG_NAME
    if sys_cfg.is_file():
        return [str(sys_cfg), *configs]
    return configs


def _resolve_config_path(name: str, configs_dir: Path) -> str:
    """
    Resolve a single ``--config`` argument.

    * Absolute / existing relative paths are returned unchanged.
    * Bare names (``live`` or ``live.json``) are looked up under
      ``user_data/configs``.
    """
    p = Path(name)
    if p.exists():
        return str(p)

    candidate = configs_dir / p.name
    if candidate.exists():
        return str(candidate)
    if candidate.suffix == "" and candidate.with_suffix(".json").exists():
        return str(candidate.with_suffix(".json"))

    # Fall back to original — let downstream loader raise a helpful error.
    return name


def _resolve_configs(names: Sequence[str], args: argparse.Namespace) -> list[str]:
    cfg_dir = _configs_dir(args)
    cfg_dir.mkdir(parents=True, exist_ok=True)
    return [_resolve_config_path(n, cfg_dir) for n in names]


def _build_args_dict(args: argparse.Namespace, configs: list[str]) -> dict[str, Any]:
    """Translate argparse Namespace into the ``args`` dict that
    ``Configuration(args, runmode)`` expects."""
    out: dict[str, Any] = {
        "config": configs,
        "user_data_dir": str(_user_data_dir(args)),
        "verbosity": getattr(args, "verbose", 0),
    }
    # Forward common optional overrides only when the user actually set them.
    for key in (
        "strategy",
        "strategy_path",
        "timeframe",
        "timerange",
        "pairs",
        "exchange",
        "datadir",
        "logfile",
        "db_url",
        "dry_run",
        "exportfilename",
        "engine",
    ):
        v = getattr(args, key, None)
        if v not in (None, [], ""):
            out[key] = v
    return out


# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------

def _setup_logging(verbosity: int, logfile: str | None) -> None:
    from VulcanTrader.util.logger import setup  # type: ignore

    level = logging.WARNING
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO

    # Initialise the shared logger — creates the per-run timestamped file under
    # VulcanTrader/logs/ and a stderr handler.  Must happen before any other code
    # so that ALL subsequent log calls reach the file.
    setup(level=level)

    # Also append to the persistent bot.log so the standalone web-portal viewer
    # (run-app.bat) can tail a single stable path across restarts.
    if logfile:
        root = logging.getLogger()
        fh = logging.FileHandler(logfile, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s :: %(message)s"))
        root.addHandler(fh)


# ---------------------------------------------------------------------------
#  Configuration loader bridge
# ---------------------------------------------------------------------------

def _load_configuration(args_dict: dict[str, Any], runmode: Any) -> dict[str, Any]:
    """Invoke the project Configuration loader. Lazy-imported so a missing
    sub-module never breaks ``--help``."""
    from VulcanTrader.config.configuration import Configuration  # type: ignore

    return Configuration(args_dict, runmode).get_config()


# ---------------------------------------------------------------------------
#  Subcommand: backtest
# ---------------------------------------------------------------------------

async def _run_one_backtest(args_dict: dict[str, Any]) -> dict[str, Any]:
    """Run a single Backtesting().start() in a worker thread."""
    from VulcanTrader.backtesting import Backtesting  # type: ignore
    from VulcanTrader.enums import RunMode  # type: ignore

    def _go() -> dict[str, Any]:
        config = _load_configuration(args_dict, RunMode.BACKTEST)
        # Backtest must ignore a config file's own "strategy" field (see
        # config_schema.py's "strategy" entry) - Configuration otherwise
        # inherits it whenever -s/--strategies wasn't passed, which would
        # silently pin every backtest against this config to one strategy
        # and defeat the multi-strategy fan-out in _cmd_backtest_async.
        # args_dict["strategy"] (possibly absent/None) is the only
        # authoritative source here.
        config["strategy"] = args_dict.get("strategy")
        if args_dict.get("engine"):
            config["engine"] = args_dict["engine"]
        bt = Backtesting(config)
        bt.start()
        return {
            "strategies": [s.get_strategy_name() for s in bt.strategylist],
            "results": bt.results,
        }

    return await asyncio.to_thread(_go)


async def _cmd_backtest_async(args: argparse.Namespace) -> int:
    configs = _resolve_configs(args.config or [], args)
    strategies: list[str | None] = list(args.strategies) if args.strategies else [args.strategy]

    # Build one args_dict per strategy so they can be fanned out concurrently.
    job_specs: list[dict[str, Any]] = []
    for strat in strategies:
        d = _build_args_dict(args, configs)
        if strat:
            d["strategy"] = strat
        job_specs.append(d)

    sem = asyncio.Semaphore(max(1, args.jobs))

    async def _runner(spec: dict[str, Any]) -> Any:
        async with sem:
            label = spec.get("strategy", "<config-default>")
            logger.info("backtest start :: %s", label)
            t0 = time.perf_counter()
            try:
                res = await _run_one_backtest(spec)
                logger.info("backtest done  :: %s (%.1fs)", label, time.perf_counter() - t0)
                return res
            except Exception:
                logger.exception("backtest FAILED :: %s", label)
                return None

    results = await asyncio.gather(*[_runner(s) for s in job_specs])
    failures = sum(1 for r in results if r is None)
    return 1 if failures else 0


def cmd_backtest(args: argparse.Namespace) -> int:
    return asyncio.run(_cmd_backtest_async(args))


# ---------------------------------------------------------------------------
#  Subcommand: hyperopt
# ---------------------------------------------------------------------------

def cmd_hyperopt(args: argparse.Namespace) -> int:
    from VulcanTrader.hyperopt.hyperopt.hyperopt import Hyperopt  # type: ignore
    from VulcanTrader.enums import RunMode  # type: ignore

    configs = _resolve_configs(args.config or [], args)
    args_dict = _build_args_dict(args, configs)

    # Map CLI flags → config keys expected by Hyperopt
    if args.epochs:
        args_dict["epochs"] = int(args.epochs)
    if args.spaces:
        args_dict["spaces"] = list(args.spaces)
    if args.hyperopt_loss:
        args_dict["hyperopt_loss"] = args.hyperopt_loss
    if args.timerange:
        args_dict["timerange"] = args.timerange
    if getattr(args, "jobs", None) is not None:
        args_dict["hyperopt_jobs"] = int(args.jobs)
    if getattr(args, "min_trades", None) is not None:
        args_dict["hyperopt_min_trades"] = int(args.min_trades)
    if getattr(args, "analyze_per_epoch", False):
        args_dict["analyze_per_epoch"] = True
    if getattr(args, "print_all", False):
        args_dict["print_all"] = True
    if getattr(args, "no_color", False):
        args_dict["no_color"] = True
    if getattr(args, "print_json", False):
        args_dict["print_json"] = True
    if getattr(args, "export_csv", None):
        args_dict["export_csv"] = args.export_csv

    config = _load_configuration(args_dict, RunMode.HYPEROPT)
    hyperopt = Hyperopt(config)
    hyperopt.start()
    return 0


# ---------------------------------------------------------------------------
#  Subcommand: download-data
# ---------------------------------------------------------------------------

def cmd_download_data(args: argparse.Namespace) -> int:
    from VulcanTrader.data.history import download_data_main  # type: ignore
    from VulcanTrader.enums import RunMode  # type: ignore

    configs = _resolve_configs(args.config or [], args)
    args_dict = _build_args_dict(args, configs)
    if args.timeframes:
        args_dict["timeframes"] = list(args.timeframes)
    if args.days:
        args_dict["days"] = int(args.days)
    if args.timerange:
        args_dict["timerange"] = args.timerange
    if args.pairs:
        args_dict["pairs"] = list(args.pairs)
    if args.erase:
        args_dict["erase"] = True

    config = _load_configuration(args_dict, RunMode.UTIL_EXCHANGE)
    download_data_main(config)
    return 0


# ---------------------------------------------------------------------------
#  Subcommand: trade
# ---------------------------------------------------------------------------

def _attach_portal(bot: Any, portal: Any) -> None:
    """Wire WebPortal into VulcanTraderBot in place of the ``_emit`` stub.

    Must still call the bot's own ``_dispatch_discord`` - it's a separate
    method (untouched by this monkey-patch), not part of the stub being
    replaced, and it's the only thing that forwards trade lifecycle events
    to the Discord webhook. Forgetting it here silently kills Discord
    notifications entirely, since attaching a portal is the default path.
    """
    bot.portal = portal

    def _emit(msg: dict) -> None:
        try:
            portal.send_msg(msg)
        except Exception:
            logger.exception("portal.send_msg failed")
        try:
            bot._dispatch_discord(msg)
        except Exception:
            logger.exception("discord dispatch failed")

    bot._emit = _emit  # type: ignore[assignment]


def cmd_trade(args: argparse.Namespace) -> int:
    from VulcanTrader.trader_bot import VulcanTraderBot  # type: ignore
    from VulcanTrader.web_portal import WebPortal  # type: ignore
    from VulcanTrader.enums import RunMode, State  # type: ignore

    configs = _resolve_configs(args.config or [], args)
    configs = _with_system_config(configs, args)
    args_dict = _build_args_dict(args, configs)
    if args.dry_run:
        args_dict["dry_run"] = True

    config = _load_configuration(args_dict, RunMode.DRY_RUN if args.dry_run else RunMode.LIVE)

    # Per-instance overrides so several concurrent trade instances can share
    # one base config file - vary these flags per invocation instead of
    # maintaining a separate config file per port (see run-paper.bat/.sh).
    if args.port or args.host:
        api_cfg = config.setdefault("api_server", {})
        if args.port:
            api_cfg["listen_port"] = int(args.port)
        if args.host:
            api_cfg["listen_ip_address"] = args.host
    if args.bot_name:
        config["bot_name"] = args.bot_name

    # Cross-process stop requests: the web portal (or anything else) creates
    # ``<db>.json.stop`` next to the persistence file; the loop below notices
    # within one cycle and shuts down exactly like a local Ctrl+C.
    #
    # Cross-process force-exit requests use the same pattern: ``<db>.json.forceexit``
    # holds a JSON list of pair strings, written by the multi-bot Discord
    # controller (or web portal) for a bot it isn't attached to in-process -
    # see web_portal.py's ``_request_account_force_exit``.
    stop_file: Path | None = None
    forceexit_file: Path | None = None
    try:
        import os as _os
        from VulcanTrader.persistence.models import _path_from_db_url
        _db_path = _path_from_db_url(config["db_url"])
        if str(_db_path) != _os.devnull:
            stop_file = _db_path.with_name(_db_path.name + ".stop")
            if stop_file.exists():
                stop_file.unlink()  # stale request from a previous run
                logger.info("removed stale stop file %s", stop_file)
            forceexit_file = _db_path.with_name(_db_path.name + ".forceexit")
            if forceexit_file.exists():
                forceexit_file.unlink()  # stale request from a previous run
                logger.info("removed stale forceexit file %s", forceexit_file)
    except Exception:
        logger.debug("stop-file setup failed", exc_info=True)
        stop_file = None
        forceexit_file = None

    stop_event = threading.Event()
    _watchdog: list = [None]

    def _cancel_watchdog() -> None:
        if _watchdog[0] is not None:
            _watchdog[0].cancel()
            _watchdog[0] = None

    def _signal_handler(signum, _frame) -> None:  # noqa: ARG001
        import os as _os
        if stop_event.is_set():
            # Second Ctrl+C — bail out hard immediately.
            logger.warning("second signal %s — forcing exit", signum)
            _cancel_watchdog()
            _os._exit(130)
        logger.info("signal %s received — shutting down (Ctrl+C again to force)", signum)
        stop_event.set()
        # Watchdog: if cleanup hangs (e.g. blocking network call), force-exit after 15 s.
        def _force() -> None:
            logger.warning("shutdown timed out — forcing exit")
            _os._exit(130)
        t = threading.Timer(15.0, _force)
        t.daemon = True
        t.start()
        _watchdog[0] = t

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _signal_handler)
        except (ValueError, OSError):
            # Not in main thread / unsupported platform
            pass

    # Windows only: clicking the X on this bot's own console window (the
    # normal way to "close" a show_terminal:true bot) sends CTRL_CLOSE_EVENT,
    # NOT SIGINT/SIGTERM - Python's signal module never sees it, so the
    # handlers above never fire and cleanup() never runs. Without a real
    # console-control handler, Windows force-kills the process a few seconds
    # later regardless, permanently leaving is_running=1 in the account JSON
    # (cleanup() is what flips it back to 0) - which is exactly what makes a
    # deliberately-closed bot show up as "crashed" the next time the
    # dashboard scans it, forever, until someone edits the file by hand.
    # Registering this gives the same graceful stop_event.set() path a
    # chance to run bot.cleanup() during the OS's few-second grace period
    # before it kills the process outright.
    if sys.platform == "win32":
        try:
            import ctypes
            import ctypes.wintypes

            _CTRL_CLOSE_EVENT = 2
            _CTRL_LOGOFF_EVENT = 5
            _CTRL_SHUTDOWN_EVENT = 6
            _HANDLER_TYPE = ctypes.WINFUNCTYPE(ctypes.wintypes.BOOL, ctypes.wintypes.DWORD)

            def _console_ctrl_handler(ctrl_type: int) -> bool:
                if ctrl_type in (_CTRL_CLOSE_EVENT, _CTRL_LOGOFF_EVENT, _CTRL_SHUTDOWN_EVENT):
                    _signal_handler(ctrl_type, None)
                    return True
                return False

            # ctypes keeps no reference of its own to the wrapped callback -
            # it must stay alive as a local in this still-running function's
            # frame for as long as the process runs, or the CFUNCTYPE object
            # can be garbage-collected out from under a live OS callback
            # pointer.
            _console_handler_ref = _HANDLER_TYPE(_console_ctrl_handler)
            if not ctypes.windll.kernel32.SetConsoleCtrlHandler(_console_handler_ref, True):
                logger.debug("SetConsoleCtrlHandler registration returned false")
        except Exception:
            logger.debug("Could not register Windows console-close handler", exc_info=True)

    # Started early (before the bot even exists) so a dashboard/Discord Stop
    # can interrupt a bot that's stuck retrying initialisation below, not
    # just a local Ctrl+C. ``bot`` is captured by the closures via late
    # binding - it's still None until construction below succeeds.
    bot = None
    if stop_file is not None:
        def _stop_file_watcher(sf: Path = stop_file) -> None:
            while not stop_event.wait(2.0):
                if sf.exists():
                    logger.info("stop file %s detected — shutting down gracefully", sf)
                    try:
                        sf.unlink()
                    except OSError:
                        pass
                    stop_event.set()
                    return

        threading.Thread(
            target=_stop_file_watcher, name="StopFileWatcher", daemon=True
        ).start()

    if forceexit_file is not None:
        def _forceexit_file_watcher(fe: Path = forceexit_file) -> None:
            while not stop_event.wait(2.0):
                if not fe.exists():
                    continue
                try:
                    pairs = json.loads(fe.read_text(encoding="utf-8") or "[]")
                    fe.unlink()
                except (OSError, ValueError):
                    continue
                if bot is None:
                    continue  # nothing to force-exit before the bot exists
                for pair in pairs:
                    logger.info("forceexit file requested exit for %s", pair)
                    bot._schedule_force_exit_pair(str(pair))

        threading.Thread(
            target=_forceexit_file_watcher, name="ForceExitFileWatcher", daemon=True
        ).start()

    # Per-phase timing so slow startups are easy to diagnose. Initialisation
    # (exchange market loading, initial balance/position fetch) can hit
    # transient errors - rate limits, a temporary 5xx/451 from the exchange,
    # a network blip - that have nothing to do with the strategy itself. A
    # fleet bot dying on one of these means a silent, permanently-stopped bot
    # until someone notices and restarts it by hand, so instead of letting
    # the exception escape and kill the process, log it and keep retrying
    # here: the console window/tmux pane/dashboard/Discord all stay live and
    # show the error, and the bot comes up on its own once the issue clears.
    # A stop request (Ctrl+C, or a stop file once bots are running elsewhere)
    # still breaks out of the retry loop instead of retrying forever.
    logger.info("[trade] initialising bot ...")
    t0 = time.perf_counter()
    init_retry_s = 30.0
    while bot is None and not stop_event.is_set():
        try:
            bot = VulcanTraderBot(config)
        except Exception:
            logger.exception(
                "[trade] bot initialisation failed - retrying in %.0fs "
                "(process stays up; stop this bot from the dashboard/Discord "
                "if you need to abort)", init_retry_s,
            )
            stop_event.wait(init_retry_s)
    if bot is None:
        logger.info("[trade] stop requested during initialisation - exiting")
        return 0
    logger.info("[trade] bot constructed in %.2fs", time.perf_counter() - t0)

    portal = WebPortal(bot) if not args.no_web else None
    if portal:
        _attach_portal(bot, portal)
        t1 = time.perf_counter()
        portal.start(blocking=False)
        logger.info("[trade] web portal started in %.2fs", time.perf_counter() - t1)

    # The interactive Discord (!) command client runs from `cmd_webserver()`
    # instead of here - it's a control-plane concern belonging to the one
    # persistent orchestrating process, not something tied to any individual
    # trade subprocess's lifecycle (see "Multi-bot fleet control" in
    # AGENTS.md). This process's trade events still reach Discord via the
    # webhook (util/discord_logger.py, through _dispatch_discord below) -
    # that part needs no interactive client at all.

    t2 = time.perf_counter()
    while not stop_event.is_set():
        try:
            bot.startup()
            break
        except Exception:
            logger.exception(
                "[trade] bot.startup() failed - retrying in %.0fs "
                "(process stays up; stop this bot from the dashboard/Discord "
                "if you need to abort)", init_retry_s,
            )
            stop_event.wait(init_retry_s)
    if stop_event.is_set():
        logger.info("[trade] stop requested during startup - exiting")
        try:
            bot.cleanup()
        finally:
            if portal:
                portal.cleanup()
        return 0
    logger.info("[trade] bot.startup() in %.2fs", time.perf_counter() - t2)
    logger.info("[trade] total init %.2fs — entering trade loop", time.perf_counter() - t0)

    # Allow the bot to start running immediately rather than sitting in STOPPED.
    try:
        bot.state = State.RUNNING
    except Exception:
        pass

    try:
        timeframe_s = max(1.0, float(config.get("internals", {}).get("process_throttle_secs", 5)))
        logger.info("[trade] trade loop active — cycle every %.0fs", timeframe_s)
        while not stop_event.is_set():
            t0 = time.perf_counter()
            try:
                bot.process()
            except Exception:
                logger.exception("bot.process() raised")
            elapsed = time.perf_counter() - t0
            # Interruptible sleep — wakes immediately when stop_event is set.
            stop_event.wait(timeout=max(0.0, timeframe_s - elapsed))
    finally:
        _cancel_watchdog()
        try:
            bot.cleanup()
        finally:
            if portal:
                portal.cleanup()
    return 0


# ---------------------------------------------------------------------------
#  Subcommand: lookahead-analysis
# ---------------------------------------------------------------------------

def _build_bias_args(args: argparse.Namespace) -> dict[str, Any]:
    configs = _resolve_configs(args.config or [], args)
    args_dict = _build_args_dict(args, configs)
    if args.strategy:
        args_dict["strategy"] = args.strategy
    if args.strategy_list:
        args_dict["strategy_list"] = list(args.strategy_list)
    if args.strategy_path:
        args_dict["strategy_path"] = args.strategy_path
    if args.timeframe:
        args_dict["timeframe"] = args.timeframe
    if args.timerange:
        args_dict["timerange"] = args.timerange
    if args.pairs:
        args_dict["pairs"] = list(args.pairs)
    if args.exchange:
        args_dict["exchange"] = {"name": args.exchange}
    if args.minimum_trade_amount is not None:
        args_dict["minimum_trade_amount"] = int(args.minimum_trade_amount)
    if args.targeted_trade_amount is not None:
        args_dict["targeted_trade_amount"] = int(args.targeted_trade_amount)
    if args.lookahead_analysis_exportfilename:
        args_dict["lookahead_analysis_exportfilename"] = args.lookahead_analysis_exportfilename
    if args.startup_candle:
        args_dict["startup_candle"] = list(args.startup_candle)
    args_dict.setdefault("minimum_trade_amount", 10)
    args_dict.setdefault("targeted_trade_amount", 20)
    return args_dict


def cmd_lookahead_analysis(args: argparse.Namespace) -> int:
    from VulcanTrader.util.bias_analysis import LookaheadAnalysisSubFunctions  # type: ignore
    from VulcanTrader.enums import RunMode  # type: ignore

    args_dict = _build_bias_args(args)
    config = _load_configuration(args_dict, RunMode.BACKTEST)
    LookaheadAnalysisSubFunctions.start(config)
    return 0


# ---------------------------------------------------------------------------
#  Subcommand: recursive-analysis
# ---------------------------------------------------------------------------

def cmd_recursive_analysis(args: argparse.Namespace) -> int:
    from VulcanTrader.util.bias_analysis import RecursiveAnalysisSubFunctions  # type: ignore
    from VulcanTrader.enums import RunMode  # type: ignore

    args_dict = _build_bias_args(args)
    config = _load_configuration(args_dict, RunMode.BACKTEST)
    RecursiveAnalysisSubFunctions.start(config)
    return 0


# ---------------------------------------------------------------------------
#  Subcommand: webserver
# ---------------------------------------------------------------------------

def cmd_webserver(args: argparse.Namespace) -> int:
    from VulcanTrader.web_portal import WebPortal  # type: ignore
    from VulcanTrader.enums import RunMode  # type: ignore

    config: dict[str, Any] = {}
    if args.config:
        configs = _resolve_configs(args.config, args)
        configs = _with_system_config(configs, args)
        try:
            config = _load_configuration(_build_args_dict(args, configs), RunMode.WEBSERVER)
        except Exception:
            logger.exception("config load failed — running portal without bot config")
            config = {}
    else:
        # No bot config attached (pure viewer/standalone mode) - still pull in the
        # shared system config if present, so host/port/credentials stay
        # consistent with every trade instance instead of falling back to
        # WebPortal's hardcoded defaults.
        sys_cfg = _configs_system_dir(args) / SYSTEM_CONFIG_NAME
        if sys_cfg.is_file():
            try:
                config = json.loads(sys_cfg.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                logger.warning("could not read system config %s", sys_cfg)

    api_cfg = config.setdefault("api_server", {}) if isinstance(config, dict) else {}
    if args.host:
        api_cfg["listen_ip_address"] = args.host
    if args.port:
        api_cfg["listen_port"] = int(args.port)
    if args.password:
        api_cfg["password"] = args.password

    class _Stub:
        def __init__(self, cfg: dict) -> None:
            self.config = cfg

    portal = WebPortal(_Stub(config) if config else None)

    # The interactive Discord (!) command client belongs here, not in
    # cmd_trade() - it's a control-plane concern for the one persistent
    # process that watches the whole fleet via portal's account scan, not
    # something tied to any individual trade subprocess's lifecycle (see
    # "Multi-bot fleet control" in AGENTS.md). daemon thread - doesn't block
    # the portal.start(blocking=True) call below.
    discord_conf = portal.config.get("discord") or {} if isinstance(portal.config, dict) else {}
    discord_bot = None
    if (
        discord_conf.get("bot_token")
        and discord_conf.get("enabled", True)
        and not getattr(args, "no_discord_bot", False)
    ):
        from VulcanTrader.util.discord_bot import DiscordBot
        discord_bot = DiscordBot(portal.config, trading_bot=None, portal=portal)
        discord_bot.start()
    elif discord_conf.get("webhook_url") and discord_conf.get("enabled", True):
        # No interactive (!) command client (no bot_token, disabled, or
        # --no-discord-bot) - nothing will ever fire on_ready() to post the
        # startup announcement, so send it right away over the plain webhook
        # instead of leaving it silent.
        from VulcanTrader.util import discord_logger
        discord_logger.init(discord_conf["webhook_url"])
        discord_logger.set_bot_name("Bot Manager")
        discord_logger.log_startup("VulcanTrader Bot manager started")

    if not getattr(args, "no_autostart", False):
        # Bring up every auto_start:true config as part of starting the
        # fleet-control process itself - previously this required a separate
        # `bot.py autostart` invocation every time, which meant starting the
        # dashboard/Discord bot alone did NOT actually launch any bot at all,
        # even ones explicitly marked auto_start:true.
        try:
            _launch_autostart_configs(_user_data_dir(args))
        except Exception:
            logger.exception("autostart scan failed - continuing without it")

    try:
        portal.start(blocking=True)
    finally:
        if discord_bot:
            discord_bot.stop()
    return 0


# ---------------------------------------------------------------------------
#  Subcommand: autostart
# ---------------------------------------------------------------------------

def _scan_autostart_configs(user_data_dir: Path) -> list[dict[str, Any]]:
    """Read every ``*.json`` under ``<user_data_dir>/configs`` and return a
    launch spec for each one with ``auto_start: true`` and ``is_bot`` not
    explicitly false (see config_schema.py's ``strategy``/``auto_start``/
    ``is_bot`` entries). A raw JSON read, not the full Configuration pipeline -
    this only needs a few top-level fields and must stay cheap since it may
    scan many config files."""
    cfg_dir = user_data_dir / "configs"
    specs: list[dict[str, Any]] = []
    if not cfg_dir.is_dir():
        return specs
    for p in sorted(cfg_dir.glob("*.json")):
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            logger.warning("autostart: could not parse %s - skipping", p.name)
            continue
        if not isinstance(raw, dict) or not raw.get("auto_start"):
            continue
        if raw.get("is_bot", True) is False:
            logger.warning(
                "autostart: %s has auto_start=true but is_bot=false - skipping", p.name
            )
            continue
        strategy = raw.get("strategy")
        if not strategy:
            logger.warning(
                "autostart: %s has auto_start=true but no 'strategy' field - skipping",
                p.name,
            )
            continue
        specs.append({
            "config_name": p.stem,
            "strategy": strategy,
            "dry_run": bool(raw.get("dry_run", True)),
            "bot_name": raw.get("bot_name") or strategy,
            "port": int((raw.get("api_server") or {}).get("listen_port") or 8080),
            # Per-bot-config, not system-wide (see config_schema.py) - each
            # bot may want different terminal-visibility behavior.
            "show_terminal": raw.get("show_terminal", True),
            "tmux": bool(raw.get("tmux", False)),
        })
    return specs


def _launch_autostart_configs(user_data_dir: Path) -> int:
    """Scan every config for ``auto_start: true`` and launch each as its own
    detached ``trade`` subprocess. Returns how many were launched.

    Fire-and-forget: launches the fleet and returns immediately. Each bot
    then runs as an independent process, observable/controllable the same
    way as any manually-launched ``trade`` instance (stop-file, the web
    portal's multi-bot account scan, the multi-bot Discord commands).

    ``db_url`` is always derived as
    ``user_data/accounts/<config-name>_<dry_run|live>.json`` - keyed by the
    config's own filename (unique by construction), not its ``strategy``
    (two configs could share one) - matching the web portal's config-manager
    ``_start_config_bot``. A config's own db_url is ignored here since a
    persistence account is inherently per-bot-instance, never something
    meaningfully shared across configs.
    ``api_server.listen_port`` is honoured when set and not already claimed
    by an earlier auto_start config in this same scan; otherwise the next
    free port from 8080 is assigned instead of colliding.

    Shared by both ``bot.py autostart`` (standalone CLI use) and
    ``cmd_webserver()`` (so starting the fleet-control process itself brings
    up every auto_start:true config, instead of requiring a separate manual
    ``autostart`` invocation every time - see ``--no-autostart``).
    """
    from VulcanTrader.util.proc_launch import launch_detached

    specs = _scan_autostart_configs(user_data_dir)
    if not specs:
        logger.info("autostart: no config has auto_start=true - nothing to launch.")
        return 0

    import socket

    def _port_is_free(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return True
            except OSError:
                return False

    used_ports: set[int] = set()
    launched = 0
    for spec in specs:
        # An actual bind-probe, not just "not already picked in this batch" -
        # since `cmd_webserver()` now calls this too (see --no-autostart),
        # naive same-batch-only avoidance let every auto_start config without
        # an explicit unique api_server.listen_port try to grab 8080, the
        # same port the calling webserver itself is usually already bound to
        # (confirmed directly: that collision is exactly what happened before
        # this fix - the launched bot's own dashboard failed to bind while
        # the trade loop itself kept running regardless).
        port = spec["port"]
        while port in used_ports or not _port_is_free(port):
            port += 1
        used_ports.add(port)

        mode = "dry_run" if spec["dry_run"] else "live"
        db_url = f"json:///user_data/accounts/{spec['config_name']}_{mode}.json"

        # Skip configs whose bot is already running - without this, every
        # `cmd_webserver()` restart (or a stray second `bot.py autostart`)
        # while an earlier auto_start subprocess is still alive spawns a
        # SECOND trader_bot.py against the same persistence account: two
        # processes fighting over the same account lock/db file, duplicate
        # trades, and a port collision on top. Same lock-probe web_portal.py
        # already uses for its own live-bot scan/Start-button guard
        # (`_probe_db_lock`) - reused here (via a lazy import to avoid a
        # module-load-time circular import with web_portal.py) rather than
        # reimplemented, since Windows vs POSIX file-locking differs.
        try:
            from VulcanTrader.persistence.models import _path_from_db_url
            from VulcanTrader.web_portal import WebPortal
            already_running = bool(WebPortal._probe_db_lock(_path_from_db_url(db_url)))
        except Exception:
            logger.debug("autostart: running-check failed for %s", spec["config_name"], exc_info=True)
            already_running = False
        if already_running:
            logger.info(
                "autostart: %s's bot is already running (db=%s) - skipping",
                spec["config_name"], db_url,
            )
            continue

        cmd = [
            sys.executable, "-m", "VulcanTrader.bot", "trade",
            "-v",
            "-c", spec["config_name"],
            "--strategy", spec["strategy"],
            "--db-url", db_url,
            "--port", str(port),
            "--bot-name", spec["bot_name"],
        ]
        if spec["dry_run"]:
            cmd.append("--dry-run")

        logger.info(
            "autostart: launching %s (strategy=%s, port=%d, db=%s)",
            spec["config_name"], spec["strategy"], port, db_url,
        )
        launch_detached(cmd, spec)
        launched += 1

    logger.info("autostart: launched %d bot(s).", launched)
    return launched


def cmd_autostart(args: argparse.Namespace) -> int:
    """CLI entry point for ``bot.py autostart`` - see `_launch_autostart_configs`."""
    _launch_autostart_configs(_user_data_dir(args))
    return 0


# ---------------------------------------------------------------------------
#  Argument parser
# ---------------------------------------------------------------------------

def _common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "-c", "--config",
        action="append",
        metavar="NAME_OR_PATH",
        help="Config file (relative names resolved under user_data/configs). "
             "Repeatable; later configs override earlier ones.",
    )
    p.add_argument(
        "--user-data-dir",
        dest="user_data_dir",
        metavar="PATH",
        help=f"User data directory (default: {DEFAULT_USER_DATA}).",
    )
    p.add_argument("--logfile", metavar="PATH", help="Write logs to file in addition to stderr.")
    p.add_argument("-v", "--verbose", action="count", default=0, help="-v for INFO, -vv for DEBUG.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="VulcanTrader",
        description="VulcanTrader — backtesting, data download, live trading and web portal.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # backtest -------------------------------------------------------------
    p_bt = sub.add_parser("backtest", help="Run one or many backtests (async fan-out).")
    _common_args(p_bt)
    p_bt.add_argument("-s", "--strategy", help="Strategy class name (single run).")
    p_bt.add_argument("--strategies", nargs="+", help="Multiple strategy names — run in parallel.")
    p_bt.add_argument("--strategy-path", dest="strategy_path", help="Override strategy search path.")
    p_bt.add_argument("--engine", choices=["python", "rust"], default="python",
                      help="Backtest engine: 'python' (full-fidelity) or 'rust' (fast, simplified).")
    p_bt.add_argument("--timeframe", "-i", help="Override strategy timeframe (e.g. 5m, 1h).")
    p_bt.add_argument("--timerange", help="Backtest timerange, e.g. 20240101-20240601.")
    p_bt.add_argument("--pairs", "-p", nargs="+", help="Restrict to these pairs.")
    p_bt.add_argument("--datadir", "-d", help="Override OHLCV data directory.")
    p_bt.add_argument("--exchange", help="Override exchange name.")
    p_bt.add_argument("--exportfilename", dest="exportfilename",
                      help="Write backtest result JSON to this path/dir (overrides default).")
    p_bt.add_argument("--jobs", "-j", type=int, default=1, help="Concurrent backtests (default: 1).")
    p_bt.set_defaults(func=cmd_backtest)

    # download-data --------------------------------------------------------
    p_dl = sub.add_parser("download-data", help="Download historical OHLCV.")
    _common_args(p_dl)
    p_dl.add_argument("--pairs", "-p", nargs="+", help="Pairs to download (default: from config).")
    p_dl.add_argument("--timeframes", "-t", nargs="+", help="Timeframes (default: from config).")
    p_dl.add_argument("--days", type=int, help="Number of days back to fetch.")
    p_dl.add_argument("--timerange", help="Explicit timerange (overrides --days).")
    p_dl.add_argument("--exchange", help="Override exchange name.")
    p_dl.add_argument("--datadir", "-d", help="Output data directory.")
    p_dl.add_argument("--erase", action="store_true", help="Erase existing data first.")
    p_dl.set_defaults(func=cmd_download_data)

    # trade ----------------------------------------------------------------
    p_tr = sub.add_parser("trade", help="Start the live trading bot + web portal.")
    _common_args(p_tr)
    p_tr.add_argument("-s", "--strategy", help="Strategy class name (overrides config).")
    p_tr.add_argument("--strategy-path", dest="strategy_path")
    p_tr.add_argument(
        "--db-url", dest="db_url",
        help="Override persistence URL. Examples: json:///user_data/trades.dry_run.json "
             "(human-readable JSON mirror, default), sqlite:///foo.sqlite, sqlite:// (in-memory).",
    )
    p_tr.add_argument("--dry-run", dest="dry_run", action="store_true", help="Force dry-run mode.")
    p_tr.add_argument(
        "--headless", "--no-web", dest="no_web", action="store_true",
        help="Run without the web portal (headless). Notifications fall back to log output.",
    )
    p_tr.add_argument(
        "--port", type=int,
        help="Web portal bind port (overrides api_server.listen_port). "
             "Lets several trade instances share one base config - just vary "
             "--port/--db-url/--strategy/--bot-name per invocation instead of "
             "maintaining a separate config file per port.",
    )
    p_tr.add_argument("--host", help="Web portal bind host (overrides api_server.listen_ip_address).")
    p_tr.add_argument("--bot-name", dest="bot_name", help="Override config's bot_name (display label).")
    p_tr.set_defaults(func=cmd_trade)

    # webserver ------------------------------------------------------------
    p_ws = sub.add_parser("webserver", help="Run the web portal standalone.")
    _common_args(p_ws)
    p_ws.add_argument("--host", help="Bind host (default 127.0.0.1).")
    p_ws.add_argument("--port", type=int, help="Bind port (default 8080).")
    p_ws.add_argument("--password", help="Override api_server.password.")
    p_ws.add_argument(
        "--no-discord-bot", dest="no_discord_bot", action="store_true",
        help="Disable the interactive Discord (!) command client even if discord.bot_token "
             "is set - webhook logging (discord_logger, from each trade instance) is "
             "unaffected. Useful if you run more than one webserver instance and only want "
             "one of them hosting the Discord client (running two means two gateway logins "
             "fighting over the same session).",
    )
    p_ws.add_argument(
        "--no-autostart", dest="no_autostart", action="store_true",
        help="Don't scan configs for auto_start:true on startup (see 'autostart' subcommand). "
             "Useful if you run more than one webserver instance and only want one of them "
             "responsible for launching the fleet.",
    )
    p_ws.set_defaults(func=cmd_webserver)

    # autostart --------------------------------------------------------------
    p_auto = sub.add_parser(
        "autostart",
        help="Scan configs for auto_start:true + strategy, launch each as its own trade subprocess.",
    )
    _common_args(p_auto)
    p_auto.set_defaults(func=cmd_autostart)

    # lookahead-analysis ---------------------------------------------------
    p_la = sub.add_parser(
        "lookahead-analysis",
        help="Detect look-ahead bias in strategy signals/indicators.",
    )
    _common_args(p_la)
    p_la.add_argument("-s", "--strategy", help="Strategy class name (single run).")
    p_la.add_argument(
        "--strategy-list", dest="strategy_list", nargs="+",
        help="Multiple strategies to test.",
    )
    p_la.add_argument("--strategy-path", dest="strategy_path")
    p_la.add_argument("--timeframe", "-i", help="Override strategy timeframe.")
    p_la.add_argument("--timerange", help="Required timerange, e.g. 20240101-20240601.")
    p_la.add_argument("--pairs", "-p", nargs="+", help="Pairs to analyse.")
    p_la.add_argument("--exchange", help="Override exchange name.")
    p_la.add_argument(
        "--minimum-trade-amount", dest="minimum_trade_amount", type=int,
        help="Minimum trades required (default: 10).",
    )
    p_la.add_argument(
        "--targeted-trade-amount", dest="targeted_trade_amount", type=int,
        help="Stop after this many trades have been analysed (default: 20).",
    )
    p_la.add_argument(
        "--lookahead-analysis-exportfilename", dest="lookahead_analysis_exportfilename",
        help="CSV file to append the analysis results to.",
    )
    p_la.add_argument(
        "--startup-candle", dest="startup_candle", nargs="+", type=int,
        help=argparse.SUPPRESS,
    )
    p_la.set_defaults(func=cmd_lookahead_analysis)

    # recursive-analysis ---------------------------------------------------
    p_ra = sub.add_parser(
        "recursive-analysis",
        help="Detect recursive-formula and indicator-only look-ahead bias.",
    )
    _common_args(p_ra)
    p_ra.add_argument("-s", "--strategy", help="Strategy class name (single run).")
    p_ra.add_argument(
        "--strategy-list", dest="strategy_list", nargs="+",
        help="Multiple strategies to test.",
    )
    p_ra.add_argument("--strategy-path", dest="strategy_path")
    p_ra.add_argument("--timeframe", "-i", help="Override strategy timeframe.")
    p_ra.add_argument("--timerange", help="Required timerange.")
    p_ra.add_argument("--pairs", "-p", nargs="+", help="Pairs to analyse.")
    p_ra.add_argument("--exchange", help="Override exchange name.")
    p_ra.add_argument(
        "--startup-candle", dest="startup_candle", nargs="+", type=int,
        help="Startup candle counts to compare (default: 199 399 499 999 1999).",
    )
    p_ra.add_argument(
        "--minimum-trade-amount", dest="minimum_trade_amount", type=int, help=argparse.SUPPRESS,
    )
    p_ra.add_argument(
        "--targeted-trade-amount", dest="targeted_trade_amount", type=int, help=argparse.SUPPRESS,
    )
    p_ra.add_argument(
        "--lookahead-analysis-exportfilename", dest="lookahead_analysis_exportfilename",
        help=argparse.SUPPRESS,
    )
    p_ra.set_defaults(func=cmd_recursive_analysis)

    # hyperopt -------------------------------------------------------------
    p_ho = sub.add_parser(
        "hyperopt",
        help="Optimise strategy parameters using Optuna-backed Bayesian search.",
    )
    _common_args(p_ho)
    p_ho.add_argument("-s", "--strategy", help="Strategy class name.")
    p_ho.add_argument("--strategy-path", dest="strategy_path")
    p_ho.add_argument("--timerange", help="Backtest timerange, e.g. 20240101-20240601.")
    p_ho.add_argument(
        "--epochs", "-e", type=int, default=100,
        help="Number of optimization epochs (default: 100).",
    )
    p_ho.add_argument(
        "--spaces", nargs="+",
        default=["sell"],
        choices=["buy", "sell", "roi", "stoploss", "trailing", "protection", "all"],
        help="Hyperopt spaces to search (default: sell).",
    )
    p_ho.add_argument(
        "--hyperopt-loss", dest="hyperopt_loss",
        default="SharpeHyperOptLossDaily",
        help="Loss function class name (default: SharpeHyperOptLossDaily).",
    )
    p_ho.add_argument(
        "-j", "--jobs", type=int, default=-1,
        help="Parallel jobs; -1 = all CPUs (default: -1).",
    )
    p_ho.add_argument(
        "--min-trades", dest="min_trades", type=int, default=1,
        help="Discard epochs with fewer trades than this (default: 1).",
    )
    p_ho.add_argument(
        "--analyze-per-epoch", dest="analyze_per_epoch", action="store_true",
        help="Re-run populate_indicators for every epoch (slower but more accurate).",
    )
    p_ho.add_argument(
        "--print-all", dest="print_all", action="store_true",
        help="Print all epochs, not just improvements.",
    )
    p_ho.add_argument(
        "--no-color", dest="no_color", action="store_true",
        help="Disable coloured output.",
    )
    p_ho.add_argument(
        "--print-json", dest="print_json", action="store_true",
        help="Print best result as JSON (for piping into a params file).",
    )
    p_ho.add_argument(
        "--export-csv", dest="export_csv",
        help="Export all epoch results to this CSV file.",
    )
    p_ho.set_defaults(func=cmd_hyperopt)

    return parser


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------

def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Ensure user_data layout exists *before* logging setup so the log dir is ready.
    ud = _user_data_dir(args)
    for sub in ("configs", "data", "strategies", "backtest_results", "logs"):
        (ud / sub).mkdir(parents=True, exist_ok=True)

    # Default log file so every subcommand writes persistently to user_data/logs/bot.log.
    # The web portal tails this file in viewer mode (run-app.bat) to show bot activity
    # even when the bot runs as a separate process.
    if not getattr(args, "logfile", None):
        args.logfile = str(ud / "logs" / "bot.log")

    _setup_logging(args.verbose, getattr(args, "logfile", None))

    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        logger.warning("interrupted")
        return 130
    except Exception:
        logger.exception("unhandled error in '%s'", args.command)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
