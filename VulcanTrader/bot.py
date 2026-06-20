# =============================================================================
#  IccTrader  ::  Top-level orchestrator   (bot.py)
# =============================================================================
#
#  Purpose
#  -------
#  Single CLI entry-point that hosts and routes between the three runtime
#  components of IccTrader:
#
#      * :class:`Backtesting`  (src/backtesting.py)   — historical replay
#      * :class:`IccTraderBot` (src/trader_bot.py)    — live trading daemon
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

"""IccTrader CLI orchestrator (see header comment above for details)."""

from __future__ import annotations

import argparse
import asyncio
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


def _user_data_dir(args: argparse.Namespace) -> Path:
    """Return the active user_data directory (CLI flag wins over default)."""
    raw = getattr(args, "user_data_dir", None) or DEFAULT_USER_DATA
    return Path(raw).resolve()


def _configs_dir(args: argparse.Namespace) -> Path:
    return _user_data_dir(args) / DEFAULT_CONFIG_SUBDIR


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
    """Wire WebPortal into IccTraderBot in place of the ``_emit`` stub."""
    bot.portal = portal

    def _emit(msg: dict) -> None:
        try:
            portal.send_msg(msg)
        except Exception:
            logger.exception("portal.send_msg failed")

    bot._emit = _emit  # type: ignore[assignment]


def cmd_trade(args: argparse.Namespace) -> int:
    from VulcanTrader.trader_bot import VulcanTraderBot  # type: ignore
    from VulcanTrader.web_portal import WebPortal  # type: ignore
    from VulcanTrader.enums import RunMode, State  # type: ignore

    configs = _resolve_configs(args.config or [], args)
    args_dict = _build_args_dict(args, configs)
    if args.dry_run:
        args_dict["dry_run"] = True

    config = _load_configuration(args_dict, RunMode.DRY_RUN if args.dry_run else RunMode.LIVE)

    # Per-phase timing so slow startups are easy to diagnose.
    logger.info("[trade] initialising bot ...")
    t0 = time.perf_counter()
    bot = VulcanTraderBot(config)
    logger.info("[trade] bot constructed in %.2fs", time.perf_counter() - t0)

    # Discord slash-command bot (daemon thread — no-op if bot_token not configured)
    discord_conf = config.get("discord") or {}
    discord_bot = None
    if discord_conf.get("bot_token"):
        from VulcanTrader.util.discord_bot import DiscordBot
        discord_bot = DiscordBot(config, trading_bot=bot)
        discord_bot.start()

    portal = WebPortal(bot) if not args.no_web else None
    if portal:
        _attach_portal(bot, portal)
        t1 = time.perf_counter()
        portal.start(blocking=False)
        logger.info("[trade] web portal started in %.2fs", time.perf_counter() - t1)

    t2 = time.perf_counter()
    bot.startup()
    logger.info("[trade] bot.startup() in %.2fs", time.perf_counter() - t2)
    logger.info("[trade] total init %.2fs — entering trade loop", time.perf_counter() - t0)

    # Allow the bot to start running immediately rather than sitting in STOPPED.
    try:
        bot.state = State.RUNNING
    except Exception:
        pass

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
            if discord_bot:
                discord_bot.stop()
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
        try:
            config = _load_configuration(_build_args_dict(args, configs), RunMode.WEBSERVER)
        except Exception:
            logger.exception("config load failed — running portal without bot config")
            config = {}

    api_cfg = config.setdefault("api_server", {}) if isinstance(config, dict) else {}
    if args.host:
        api_cfg["listen_ip_address"] = args.host
    if args.port:
        api_cfg["listen_port"] = int(args.port)
    if args.password:
        api_cfg["password"] = args.password

    # Stand-up portal in viewer mode (no bot attached).
    logger.warning(
        "Running in WEBSERVER (viewer-only) mode — NO trading bot is active. "
        "Use run-paper.bat / run-live.bat to start the bot with the portal."
    )

    class _Stub:
        def __init__(self, cfg: dict) -> None:
            self.config = cfg

    portal = WebPortal(_Stub(config) if config else None)
    portal.start(blocking=True)
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
        description="IccTrader — backtesting, data download, live trading and web portal.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # backtest -------------------------------------------------------------
    p_bt = sub.add_parser("backtest", help="Run one or many backtests (async fan-out).")
    _common_args(p_bt)
    p_bt.add_argument("-s", "--strategy", help="Strategy class name (single run).")
    p_bt.add_argument("--strategies", nargs="+", help="Multiple strategy names — run in parallel.")
    p_bt.add_argument("--strategy-path", dest="strategy_path", help="Override strategy search path.")
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
    p_tr.add_argument("--no-web", action="store_true", help="Do not launch the web portal.")
    p_tr.set_defaults(func=cmd_trade)

    # webserver ------------------------------------------------------------
    p_ws = sub.add_parser("webserver", help="Run the web portal standalone.")
    _common_args(p_ws)
    p_ws.add_argument("--host", help="Bind host (default 127.0.0.1).")
    p_ws.add_argument("--port", type=int, help="Bind port (default 8080).")
    p_ws.add_argument("--password", help="Override api_server.password.")
    p_ws.set_defaults(func=cmd_webserver)

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
