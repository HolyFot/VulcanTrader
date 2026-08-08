# =============================================================================
#  VulcanTrader  ::  Web portal   (FastAPI dashboard + RPC replacement)
# =============================================================================
#
#  Purpose
#  -------
#  Drop-in replacement for VulcanTrader's ``RPCManager`` (Telegram, Discord,
#  Webhook, REST API, WebSocket producer/consumer) consolidated behind a
#  single FastAPI application that also serves the Vue dashboards.
#
#  Responsibilities
#  ----------------
#    1. **Notification sink.** :meth:`WebPortal.send_msg` accepts the same
#       ``RPCMessageType``-keyed dicts the bot previously pushed to
#       ``self.rpc.send_msg``; they are buffered in a bounded ring and
#       exposed via ``GET /api/messages``.
#    2. **Read-only HTTP API** for live trades, wallets, pairlist state,
#       strategy plot config + analyzed dataframes, and on-disk backtest
#       result JSON files.
#    3. **Static asset host** for ``template/login.html``,
#       ``template/backtester.html`` and ``template/trading.html``.
#
#  Endpoints (all under ``/api/`` require ``Authorization: Bearer <token>``
#  except ``/api/login``):
#         POST /api/login                       → { token }
#         GET  /api/status                      → bot/exchange/state
#         GET  /api/livebots                    → detected trader_bot accounts
#         GET  /api/dashboard?bot=NAME          → dashboard from that account DB
#         GET  /api/messages?limit=N            → recent notifications
#         GET  /api/trades/open                 → open trades + enrichment
#         GET  /api/trades/closed?limit=N       → closed trade history
#         GET  /api/whitelist                   → current pair whitelist
#         GET  /api/locks                       → active PairLocks
#         GET  /api/backtests                   → list result files
#         GET  /api/backtests/{name}            → load result file
#         GET  /api/strategy/info               → plot_config + metadata
#         GET  /api/pair/candles?pair=&tf=&n=   → analyzed dataframe
#         GET  /api/pair/trades?pair=           → per-pair trade history
#
#  Authentication
#  --------------
#  Single bearer token derived from ``config["api_server"]["password"]``
#  (default ``"VulcanTrader"``), compared with :func:`secrets.compare_digest`.
#
#  Wiring into the bot
#  -------------------
#  In ``VulcanTraderBot.__init__``::
#
#      from VulcanTrader.web_portal import WebPortal
#      self.portal = WebPortal(self)
#      self.portal.start(blocking=False)
#
#  Repoint the ``_emit`` stub::
#
#      def _emit(self, msg: dict) -> None:
#          self.portal.send_msg(msg)
#
#  And in ``cleanup``::
#
#      self.portal.cleanup()
#
#  Run standalone (no bot attached, browses backtest results only)::
#
#      python -m VulcanTrader.web_portal
# =============================================================================

"""VulcanTrader FastAPI web portal (see header comment above for details)."""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import subprocess
import sys

import psutil
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    from fastapi import Body, Depends, FastAPI, HTTPException, Request, status
    from fastapi.responses import FileResponse, JSONResponse
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "VulcanTrader.web_portal requires `fastapi` and `uvicorn`. "
        "Install with: pip install fastapi uvicorn"
    ) from exc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = REPO_ROOT / "template"
STATIC_DIR = REPO_ROOT / "static"


def _json_safe(value: Any) -> Any:
    """Recursively coerce non-JSON-serialisable values (datetime, Decimal, Enum, etc.)."""
    from datetime import date, datetime
    from decimal import Decimal
    from enum import Enum

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "to_json") and callable(value.to_json):
        try:
            return _json_safe(value.to_json())
        except Exception:
            pass
    return value


def _iso(value: Any) -> Any:
    """Render a datetime (or ISO string) as an ISO-8601 string."""
    from datetime import datetime
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _ts_to_iso(ts: Any) -> str | None:
    """Convert a unix-second integer to an ISO-8601 UTC string."""
    from datetime import UTC, datetime
    try:
        return datetime.fromtimestamp(int(ts), tz=UTC).isoformat()
    except Exception:
        return None




def _trade_to_dict(trade: Any) -> dict:
    """Best-effort serialisation of a Trade/LocalTrade object."""
    if isinstance(trade, dict):
        return _json_safe(trade)
    if hasattr(trade, "to_json") and callable(trade.to_json):
        try:
            return _json_safe(trade.to_json())
        except Exception:
            pass
    out: dict = {}
    for attr in (
        "id", "pair", "is_open", "is_short", "leverage", "exchange",
        "stake_amount", "amount", "open_rate", "close_rate", "open_date",
        "close_date", "profit_abs", "profit_ratio", "close_profit",
        "close_profit_abs", "fee_open", "fee_close", "enter_tag",
        "exit_reason", "strategy", "timeframe", "trading_mode",
        "min_rate", "max_rate", "open_trade_value", "funding_fees",
    ):
        if hasattr(trade, attr):
            out[attr] = _json_safe(getattr(trade, attr))
    return out


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str = ""
    password: str


class LoginResponse(BaseModel):
    token: str


class BacktestRunRequest(BaseModel):
    config: str
    strategy: str | None = None
    timerange: str | None = None
    engine: str | None = "python"


class PairsFinderRequest(BaseModel):
    config: str
    strategy: str | None = None
    timerange: str | None = "20250101-"
    top_n: int = 15
    metric: str = "composite"
    workers: int = 4
    pairs: list[str] | None = None


class HyperoptRunRequest(BaseModel):
    config: str
    strategy: str | None = None
    timerange: str | None = None
    epochs: int = 100
    hyperopt_loss: str = "SharpeHyperOptLossDaily"
    spaces: list[str] = ["sell"]
    jobs: int = -1
    min_trades: int = 1
    analyze_per_epoch: bool = False
    print_all: bool = False
    no_color: bool = True


class BotConfigureRequest(BaseModel):
    dry_run: bool


class StartBotRequest(BaseModel):
    config: str | None = None
    strategy: str | None = None
    dry_run: bool = True


class ConfigRawRequest(BaseModel):
    content: str


class ConfigNameRequest(BaseModel):
    new_name: str


class DataDownloadRequest(BaseModel):
    config: str
    exchange: str | None = None
    pairs: list[str] | None = None
    timeframes: list[str] | None = None
    days: int | None = None
    timerange: str | None = None
    erase: bool = False


# ---------------------------------------------------------------------------
# Logging capture handler
# ---------------------------------------------------------------------------

class _LogCaptureHandler(logging.Handler):
    """Captures log records into a bounded in-memory deque for the web UI."""

    MAX_LINES = 500

    def __init__(self) -> None:
        super().__init__()
        self._lines: deque[dict] = deque(maxlen=self.MAX_LINES)
        self._lock = threading.Lock()
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s :: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
            with self._lock:
                self._lines.append({"ts": record.created, "level": record.levelname, "line": line})
        except Exception:
            pass

    def get_lines(self, limit: int = 200) -> list[dict]:
        with self._lock:
            return list(self._lines)[-limit:]


_log_capture = _LogCaptureHandler()


def _find_latest_log_file() -> Path | None:
    """Most recently modified per-process timestamped log file under
    user_data/logs/ (see util/logger.py's setup() - every bot/webserver/CLI
    invocation gets its own ``VulcanTrader_<timestamp>.txt`` there, size-capped
    and rotating). Best-effort proxy for "what's currently happening" in
    viewer mode: since each process now writes its own file instead of one
    shared bot.log (that file grew to 350MB/3.9M lines with no rotation before
    this was fixed - see bot.py's main()), there's no longer a single true
    "the log" to point at, so this picks whichever file was touched most
    recently. Excludes logs/data_server/ (a separate, non-trading-bot stream)
    since glob("VulcanTrader_*.txt") on the parent dir doesn't recurse into it
    anyway."""
    from VulcanTrader.util.logger import _LOG_DIR

    log_dir = _LOG_DIR
    try:
        candidates = [p for p in log_dir.glob("VulcanTrader_*.txt") if p.is_file()]
    except OSError:
        return None
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _tail_log_file(path: Path | None, limit: int) -> list[dict]:
    """Return the last *limit* lines from a bot log file as log dicts.

    Used in viewer mode (no live bot in this process) to show the persistent
    log written by a separately-running bot process.
    """
    if path is None:
        return []
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            chunk = min(size, max(limit * 250, 16_384))
            fh.seek(max(0, size - chunk))
            data = fh.read()
        lines = data.decode("utf-8", errors="replace").splitlines()
        return [{"ts": 0, "level": "INFO", "line": ln} for ln in lines[-limit:] if ln.strip()]
    except OSError:
        return []


# ---------------------------------------------------------------------------
# WebPortal
# ---------------------------------------------------------------------------

class WebPortal:
    """
    Web dashboard + notification sink.

    Parameters
    ----------
    bot:
        Optional reference to ``VulcanTraderBot``. When ``None``, the portal
        runs in "viewer" mode and only serves backtest result JSON files.
    """

    MAX_MESSAGES = 500

    def __init__(self, bot: Any | None = None, *, on_shutdown: Callable[[], None] | None = None) -> None:
        self.bot = bot
        self.config: dict = getattr(bot, "config", {}) if bot else {}
        api_cfg = self.config.get("api_server", {}) if isinstance(self.config, dict) else {}
        # Called from the blocking-mode Ctrl+C handler (see start()) before
        # the uvicorn server itself starts shutting down - lets a caller
        # hosting bots in-process (single_process_mode, see bot.py's
        # stop_all_in_process_bots) stop them too. A plain SIGINT would
        # otherwise only know to stop this portal's own server.
        self._on_shutdown = on_shutdown

        self._password: str = api_cfg.get("password", "VulcanTrader")
        self._token: str = secrets.token_urlsafe(32)
        self._host: str = api_cfg.get("listen_ip_address", "127.0.0.1")
        self._port: int = int(api_cfg.get("listen_port", 8080))

        # Viewer-mode log tail resolves the current file fresh on every request
        # (see _find_latest_log_file) rather than a fixed path stored here -
        # which process is "most recent" changes as bots restart.

        self._messages: deque[dict] = deque(maxlen=self.MAX_MESSAGES)
        self._lock = threading.Lock()
        self._server_thread: threading.Thread | None = None
        self._server: Any = None  # uvicorn.Server

        # Backtest job runner (subprocess, capped concurrency)
        self.MAX_CONCURRENT_JOBS: int = 8
        self.MAX_JOB_LOG_LINES: int = 2000
        self.MAX_JOB_HISTORY: int = 50
        self._jobs: dict[str, dict] = {}
        self._jobs_lock = threading.Lock()

        # Subprocess bot launched from webserver mode via /api/start
        self._bot_process: subprocess.Popen | None = None
        self._bot_config_name: str | None = None
        # single_process_mode counterpart of the above - set instead of
        # _bot_process when /api/start launches the bot in-process (see
        # bot.py's launch_bot_in_process).
        self._bot_account_name: str | None = None

        # CPU%/RAM per running bot (Configs table) - psutil.Process objects
        # cached by PID so cpu_percent() gets a real delta between calls
        # instead of always reading 0.0 (its first call for any given
        # Process object has no prior sample to diff against - see
        # _proc_stats).
        self._proc_stat_cache: dict[int, psutil.Process] = {}

        # Live-price cache for computing OPEN trades' unrealized PnL - keyed
        # by "<exchange>:<pair>", short TTL so a page with several open
        # positions across bots doesn't refetch the same ticker per trade,
        # but still tracks the market closely between auto-refresh polls.
        self._ticker_cache: dict[str, tuple[float, float]] = {}
        self.TICKER_CACHE_TTL: float = 5.0
        # Hard wall-clock cap on any single public ticker fetch (see
        # _get_ccxt_public_client/_run_with_deadline) - a slow or rate-
        # limited exchange must never make "click a bot to view it" wait
        # longer than this; past it we give up and serve without a live
        # price rather than block the dashboard request.
        self.TICKER_FETCH_TIMEOUT_S: float = 3.0
        # Plain public (no API key) ccxt clients, one per exchange id, used
        # only to price ANOTHER bot's open trades from its account JSON
        # snapshot (see _account_dashboard) - this project's own Exchange
        # wrapper expects a full bot config (whitelist, keys, market
        # validation) that isn't available/appropriate for read-only,
        # cross-process dashboard viewing.
        self._ccxt_public_pool: dict[str, Any] = {}

        # Install log capture AFTER logging.basicConfig(force=True) has already run.
        # Also ensure root logger level <= INFO so INFO records reach the handler
        # even when the app is started without -v.
        root_logger = logging.getLogger()
        if root_logger.level == logging.NOTSET or root_logger.level > logging.INFO:
            root_logger.setLevel(logging.INFO)
        if _log_capture not in root_logger.handlers:
            root_logger.addHandler(_log_capture)

        self.app = self._build_app()

    # ------------------------------------------------------------------
    # Public API used by VulcanTraderBot (replaces RPCManager)
    # ------------------------------------------------------------------
    def send_msg(self, msg: dict) -> None:
        """Receive a notification dict from the bot."""
        from datetime import UTC, datetime

        record = {
            "received_at": datetime.now(UTC).isoformat(),
            **_json_safe(msg),
        }
        with self._lock:
            self._messages.append(record)
        msg_type = record.get("type", "?")
        logger.debug("web_portal.send_msg: %s", msg_type)

    def startup_messages(self, config: dict, pairlists: Any, protections: Any) -> None:
        """Called once on bot startup. Stores a synthetic STATUS message."""
        try:
            pl_names = [p.name for p in getattr(pairlists, "_pairlist_handlers", [])]
        except Exception:
            pl_names = []
        try:
            pr_names = [p.name for p in getattr(protections, "_protection_handlers", [])]
        except Exception:
            pr_names = []
        self.send_msg({
            "type": "startup",
            "status": "Bot started",
            "exchange": config.get("exchange", {}).get("name"),
            "stake_currency": config.get("stake_currency"),
            "dry_run": config.get("dry_run"),
            "pairlists": pl_names,
            "protections": pr_names,
        })

    def process_msg_queue(self, queue: Any) -> None:
        """
        Drain the dataprovider message queue. ``queue`` is typically a
        ``deque`` of analyzed-dataframe broadcast messages produced by
        ``IStrategy.analyze``.
        """
        if queue is None:
            return
        try:
            while True:
                msg = queue.popleft()
                self.send_msg({"type": "analyzed_df", "data": _json_safe(msg)})
        except (IndexError, AttributeError):
            return

    def cleanup(self) -> None:
        """Stop the embedded HTTP server."""
        if self._server is not None:
            try:
                self._server.should_exit = True
            except Exception:
                logger.exception("web_portal cleanup failed")
        if self._server_thread is not None:
            self._server_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------
    def start(self, *, blocking: bool = False) -> None:
        """Start the uvicorn server (in a background thread by default)."""
        try:
            import uvicorn
        except ImportError as exc:  # pragma: no cover
            raise ImportError("uvicorn is required to run the web portal") from exc

        cfg_kwargs: dict = dict(
            host=self._host,
            port=self._port,
            log_level="info",
            log_config=None,  # Don't let uvicorn overwrite our logging handlers
            access_log=False,
        )
        # Cap graceful-shutdown wait so open browser keep-alive connections don't
        # cause Ctrl+C to appear frozen.  Introduced in uvicorn 0.20; ignore on
        # older installs.
        try:
            import inspect as _inspect
            if "timeout_graceful_shutdown" in _inspect.signature(uvicorn.Config).parameters:
                cfg_kwargs["timeout_graceful_shutdown"] = 5
        except Exception:
            pass

        cfg = uvicorn.Config(self.app, **cfg_kwargs)

        if blocking:
            import os as _os
            import signal as _signal

            # Subclass to suppress uvicorn's own signal-handler installation so
            # our handlers below stay in effect for the lifetime of the process.
            class _ManagedServer(uvicorn.Server):
                def install_signal_handlers(self) -> None:  # type: ignore[override]
                    pass

            self._server = _ManagedServer(cfg)

            _hits = [0]
            _watchdog: list = [None]

            def _cancel_watchdog() -> None:
                if _watchdog[0] is not None:
                    _watchdog[0].cancel()
                    _watchdog[0] = None

            def _on_signal(sig: int, frame: object) -> None:
                _hits[0] += 1
                if _hits[0] == 1:
                    print(
                        "\n[VulcanTrader] Shutting down… (Ctrl+C again to force)",
                        flush=True,
                    )
                    if self._on_shutdown is not None:
                        # Best-effort: e.g. bot.py's stop_all_in_process_bots -
                        # sets every in-process bot's stop_event so its own
                        # thread runs its normal cleanup path. Non-blocking:
                        # this just signals, it doesn't wait for them.
                        try:
                            self._on_shutdown()
                        except Exception:
                            logger.exception("on_shutdown callback failed")
                    if self._server is not None:
                        # force_exit skips waiting for open connections (SSE, keep-alive)
                        # to drain — without it uvicorn blocks until the browser disconnects.
                        self._server.should_exit = True
                        self._server.force_exit = True
                    # Hard-exit backstop in case asyncio cleanup itself stalls.
                    # Bumped when on_shutdown is set: real bot cleanup (closing
                    # exchange connections, a final persistence flush) races
                    # this timer, so give it a bit more room than the plain
                    # portal-only shutdown needs.
                    def _force() -> None:
                        print(
                            "\n[VulcanTrader] Shutdown timed out — forcing exit.",
                            flush=True,
                        )
                        _os._exit(0)

                    watchdog_s = 5.0 if self._on_shutdown is not None else 3.0
                    t = threading.Timer(watchdog_s, _force)
                    t.daemon = True
                    t.start()
                    _watchdog[0] = t
                else:
                    print("\n[VulcanTrader] Force exit.", flush=True)
                    _cancel_watchdog()
                    _os._exit(0)

            _signal.signal(_signal.SIGINT, _on_signal)
            if hasattr(_signal, "SIGTERM"):
                _signal.signal(_signal.SIGTERM, _on_signal)

            print(
                f"\n  VulcanTrader Web Portal  ::  http://{self._host}:{self._port}\n",
                flush=True,
            )
            try:
                self._server.run()
            finally:
                _cancel_watchdog()
            return

        self._server = uvicorn.Server(cfg)
        # start_thread_with_context (not a plain threading.Thread): this
        # server thread must see whatever persistence context (see
        # persistence/base.py) is ambient on the calling thread - the
        # attached bot's own, if any - so its REST handlers (including ones
        # FastAPI dispatches to its own sync-endpoint threadpool, which
        # AnyIO propagates contextvars into) read that bot's trade data and
        # not another bot's sharing the same process (single_process_mode).
        from VulcanTrader.util.ctx_thread import start_thread_with_context
        self._server_thread = start_thread_with_context(
            self._server.run,
            name="WebPortalServer",
            daemon=True,
        )
        url = f"http://{self._host}:{self._port}"
        banner = (
            "\n"
            "  ============================================================\n"
            f"   VulcanTrader Web Portal  ::  {url}\n"
            f"   Login page              ::  {url}/login\n"
            "   (bearer password from config['api_server']['password'])\n"
            "  ============================================================\n"
        )
        print(banner, flush=True)
        logger.info("Web portal listening on %s", url)

    # ------------------------------------------------------------------
    # FastAPI app construction
    # ------------------------------------------------------------------
    def _build_app(self) -> FastAPI:  # noqa: C901 - plain route registration
        app = FastAPI(title="VulcanTrader Web Portal", docs_url="/api/docs")

        @app.on_event("startup")
        async def _reattach_log_capture() -> None:
            # uvicorn calls configure_logging() before this event fires.
            # Re-attach _log_capture so it survives any logging reconfiguration.
            root = logging.getLogger()
            if root.level == logging.NOTSET or root.level > logging.INFO:
                root.setLevel(logging.INFO)
            if _log_capture not in root.handlers:
                root.addHandler(_log_capture)

        bearer = HTTPBearer(auto_error=False)

        def auth(creds: HTTPAuthorizationCredentials | None = Depends(bearer)) -> None:
            if creds is None or creds.credentials != self._token:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or missing token",
                )

        # --------------- static pages ---------------
        @app.get("/", include_in_schema=False)
        def root() -> Any:
            f = TEMPLATE_DIR / "login.html"
            if f.is_file():
                return FileResponse(f)
            return JSONResponse({"detail": "login.html not found"}, status_code=404)

        @app.get("/login", include_in_schema=False)
        def login_page() -> Any:
            return root()

        # No-cache headers so the dashboards always pick up fresh HTML edits
        # without the browser serving a stale cached copy.
        _no_cache_headers = {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        }

        @app.get("/backtester", include_in_schema=False)
        def backtester_page() -> Any:
            f = TEMPLATE_DIR / "backtester.html"
            if f.is_file():
                return FileResponse(f, headers=_no_cache_headers)
            return JSONResponse({"detail": "backtester.html not found"}, status_code=404)

        @app.get("/trading", include_in_schema=False)
        def trading_page() -> Any:
            f = TEMPLATE_DIR / "trading.html"
            if f.is_file():
                return FileResponse(f, headers=_no_cache_headers)
            return JSONResponse({"detail": "trading.html not found"}, status_code=404)

        @app.get("/datamanager", include_in_schema=False)
        def datamanager_page() -> Any:
            f = TEMPLATE_DIR / "datamanager.html"
            if f.is_file():
                return FileResponse(f, headers=_no_cache_headers)
            return JSONResponse({"detail": "datamanager.html not found"}, status_code=404)

        if TEMPLATE_DIR.is_dir():
            app.mount(
                "/template",
                StaticFiles(directory=str(TEMPLATE_DIR)),
                name="template",
            )

        try:
            STATIC_DIR.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        if STATIC_DIR.is_dir():
            app.mount(
                "/static",
                StaticFiles(directory=str(STATIC_DIR)),
                name="static",
            )

        # --------------- auth ---------------
        # No auth dependency - the login page itself needs this before a
        # token exists, to show the same "vX.Y" tag trading.html/backtester.html
        # display in their navbars. Version number only, nothing sensitive.
        @app.get("/api/version")
        def api_version() -> dict:
            from VulcanTrader.constants import __version__
            return {"version": __version__}

        @app.post("/api/login", response_model=LoginResponse)
        def api_login(req: LoginRequest) -> LoginResponse:
            if not secrets.compare_digest(req.password, self._password):
                raise HTTPException(status_code=401, detail="Invalid credentials")
            return LoginResponse(token=self._token)

        # --------------- bot status ---------------
        @app.get("/api/status", dependencies=[Depends(auth)])
        def api_status() -> dict:
            from VulcanTrader.constants import __version__
            if self.bot is None or not hasattr(self.bot, "process"):
                # Webserver mode: report subprocess bot state if one was launched
                proc = self._bot_process
                running = proc is not None and proc.poll() is None
                return {
                    "attached": False,
                    "version": __version__,
                    "state": "RUNNING" if running else "STOPPED",
                }
            try:
                state = getattr(self.bot, "state", None)
                strat = getattr(self.bot, "strategy", None)
                started_at = uptime_secs = None
                try:
                    from VulcanTrader.persistence.key_value_store import KeyValueStore
                    st = KeyValueStore.get_datetime_value("startup_time")
                    if st is not None:
                        started_at = st.isoformat()
                        uptime_secs = max(0.0, (datetime.now(UTC) - st).total_seconds())
                except Exception:
                    pass
                return {
                    "attached": True,
                    "version": __version__,
                    "started_at": started_at,
                    "uptime_secs": uptime_secs,
                    "state": state.name if state is not None else None,
                    "dry_run": self.config.get("dry_run"),
                    "exchange": self.config.get("exchange", {}).get("name"),
                    "stake_currency": self.config.get("stake_currency"),
                    "max_open_trades": self.config.get("max_open_trades"),
                    "trading_mode": _json_safe(getattr(self.bot, "trading_mode", None)),
                    "last_process": _json_safe(getattr(self.bot, "last_process", None)),
                    "bot_name": self.config.get("bot_name"),
                    "strategy": strat.__class__.__name__ if strat is not None else None,
                    "starting_capital": self._starting_capital_from_config(
                        self.config, bool(self.config.get("dry_run"))
                    ),
                }
            except Exception as exc:
                logger.exception("status endpoint failure")
                raise HTTPException(status_code=500, detail=str(exc)) from exc

        @app.post("/api/start", dependencies=[Depends(auth)])
        def api_start(req: StartBotRequest = Body(default_factory=StartBotRequest)) -> dict:
            if self.bot is None or not hasattr(self.bot, "process"):
                # Webserver mode: launch the bot as a subprocess (or, under
                # single_process_mode, as a thread in this process - see
                # bot.py's launch_bot_in_process).
                if self._bot_process is not None and self._bot_process.poll() is None:
                    return {"state": "RUNNING"}  # already running (subprocess mode)
                if not req.config or not req.strategy:
                    raise HTTPException(status_code=400, detail="Select a config and strategy before starting the bot.")
                if self.config.get("single_process_mode"):
                    from VulcanTrader.bot import launch_bot_in_process, in_process_bot_running
                    account_name = f"{req.config}_{'dry_run' if req.dry_run else 'live'}"
                    if in_process_bot_running(account_name):
                        return {"state": "RUNNING"}  # already running (in-process)
                    spec = {
                        "config_name": req.config, "strategy": req.strategy,
                        "dry_run": bool(req.dry_run), "bot_name": req.strategy,
                    }
                    launch_bot_in_process(spec, self._user_data_dir(), no_web=True)
                    self._bot_account_name = account_name
                    return {"state": "RUNNING"}
                import sys
                from VulcanTrader.util.proc_launch import launch_detached
                cmd = [
                    sys.executable, "-m", "VulcanTrader.bot", "trade",
                    "-v", "-c", req.config, "--strategy", req.strategy,
                    "--no-web",
                ]
                if req.dry_run:
                    cmd.append("--dry-run")
                logger.info("Launching bot subprocess: %s", " ".join(cmd))
                # show_terminal/tmux are per-bot-config settings (see
                # config_schema.py), so read them from req.config's own file.
                launch_cfg: dict = {}
                try:
                    cfg_path = self._config_path(req.config)
                    if cfg_path.is_file():
                        launch_cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    logger.debug("could not read %s for launch settings", req.config, exc_info=True)
                self._bot_process = launch_detached(cmd, launch_cfg)
                self._bot_config_name = req.config
                return {"state": "RUNNING"}
            from VulcanTrader.enums import State
            self.bot.state = State.RUNNING
            logger.info("Bot state set to RUNNING via web portal.")
            return {"state": "RUNNING"}

        @app.post("/api/stop", dependencies=[Depends(auth)])
        def api_stop() -> dict:
            if self.bot is None or not hasattr(self.bot, "process"):
                if self._bot_account_name is not None:
                    from VulcanTrader.bot import stop_in_process_bot
                    stop_in_process_bot(self._bot_account_name)
                    logger.info("In-process bot %s stop requested via web portal.", self._bot_account_name)
                    self._bot_account_name = None
                    return {"state": "STOPPED"}
                # Webserver mode: terminate the subprocess bot
                proc = self._bot_process
                if proc is not None and proc.poll() is None:
                    proc.terminate()
                    logger.info("Bot subprocess terminated via web portal.")
                self._bot_process = None
                return {"state": "STOPPED"}
            from VulcanTrader.enums import State
            self.bot.state = State.PAUSED
            logger.info("Bot state set to PAUSED via web portal.")
            return {"state": "PAUSED"}

        @app.post("/api/bot/configure", dependencies=[Depends(auth)])
        def api_bot_configure(req: BotConfigureRequest) -> dict:
            if self.bot is None or not hasattr(self.bot, "process"):
                return {"dry_run": req.dry_run}  # webserver-only mode — no-op
            self.bot.config["dry_run"] = req.dry_run
            self.config["dry_run"] = req.dry_run
            logger.info("Bot dry_run set to %s via web portal.", req.dry_run)
            return {"dry_run": req.dry_run}

        @app.get("/api/messages", dependencies=[Depends(auth)])
        def api_messages(limit: int = 100) -> dict:
            with self._lock:
                items = list(self._messages)[-limit:]
            return {"count": len(items), "messages": items}

        @app.get("/api/bot/logs", dependencies=[Depends(auth)])
        def api_bot_logs(limit: int = 200) -> dict:
            # Same-process mode (run-paper.bat / run-live.bat): the bot runs in this
            # process so _log_capture has live records.
            # Viewer mode (run-app.bat): the bot is a separate process writing to the
            # persistent log file — tail that instead.
            if self.bot is not None and hasattr(self.bot, "state"):
                lines = _log_capture.get_lines(limit)
            else:
                lines = _tail_log_file(_find_latest_log_file(), limit)
            return {"count": len(lines), "lines": lines}

        # --------------- live trade data ---------------
        @app.get("/api/trades/open", dependencies=[Depends(auth)])
        def api_open_trades() -> dict:
            trades = self._get_open_trades()
            return {"count": len(trades), "trades": [self._attach_live_pnl(_trade_to_dict(t)) for t in trades]}

        @app.get("/api/trades/closed", dependencies=[Depends(auth)])
        def api_closed_trades(limit: int = 1000) -> dict:
            trades = self._get_closed_trades(limit)
            return {"count": len(trades), "trades": [_trade_to_dict(t) for t in trades]}

        @app.get("/api/livebots", dependencies=[Depends(auth)])
        def api_livebots() -> dict:
            """Detected trader_bot persistence accounts (running + stopped)."""
            return {"bots": self._scan_live_bots()}

        @app.post("/api/livebots/{name}/stop", dependencies=[Depends(auth)])
        def api_livebot_stop(name: str) -> dict:
            """Request a graceful shutdown of the trader_bot process owning this account."""
            return self._request_account_stop(name)

        @app.post("/api/livebots/{name}/forceexit", dependencies=[Depends(auth)])
        def api_livebot_forceexit(name: str, pair: str) -> dict:
            """Request a force-exit of ``pair`` on the trader_bot process owning this account."""
            return self._request_account_force_exit(name, pair)

        @app.get("/api/dashboard", dependencies=[Depends(auth)])
        def api_dashboard(closed_limit: int = 2000, msg_limit: int = 200, log_limit: int = 300,
                          bot: str | None = None) -> dict:
            """Single endpoint combining status + trades + messages + logs for the trading page.

            ``bot=<account-name>`` (a name from /api/livebots) serves the data of that
            bot's persistence account instead of the attached/in-process bot."""
            if bot:
                return self._account_dashboard(bot, closed_limit)
            status = api_status()
            open_trades = self._get_open_trades()
            closed_trades = self._get_closed_trades(closed_limit)
            with self._lock:
                messages = list(self._messages)[-msg_limit:]
            if self.bot is not None and hasattr(self.bot, "state"):
                logs = _log_capture.get_lines(log_limit)
            else:
                logs = _tail_log_file(_find_latest_log_file(), log_limit)
            return {
                "status": status,
                "open_trades": [self._attach_live_pnl(_trade_to_dict(t)) for t in open_trades],
                "closed_trades": [_trade_to_dict(t) for t in closed_trades],
                "messages": messages,
                "logs": logs,
            }

        @app.get("/api/whitelist", dependencies=[Depends(auth)])
        def api_whitelist() -> dict:
            wl: list[str] = []
            if self.bot is not None:
                try:
                    wl = list(getattr(self.bot, "active_pair_whitelist", []) or [])
                except Exception:
                    wl = []
            return {"whitelist": wl}

        @app.get("/api/locks", dependencies=[Depends(auth)])
        def api_locks() -> dict:
            return {"locks": self._get_pair_locks()}

        # --------------- strategy / chart data ---------------
        @app.get("/api/strategy/info", dependencies=[Depends(auth)])
        def api_strategy_info() -> dict:
            return self._strategy_info()

        @app.get("/api/pair/candles", dependencies=[Depends(auth)])
        def api_pair_candles(pair: str, timeframe: str | None = None, limit: int = 500) -> dict:
            return self._pair_candles(pair, timeframe, limit)

        @app.get("/api/pair/trades", dependencies=[Depends(auth)])
        def api_pair_trades(pair: str) -> dict:
            return {"trades": self._pair_trades(pair)}

        # --------------- backtest results ---------------
        @app.get("/api/backtests", dependencies=[Depends(auth)])
        def api_backtests() -> dict:
            return {"backtests": self._list_backtests()}

        # --------------- backtest runner ---------------
        # NOTE: literal-path routes (/jobs, /run) MUST be registered before
        # the parameterised /{name} routes, otherwise FastAPI matches "jobs"
        # as a backtest name.
        @app.get("/api/configs", dependencies=[Depends(auth)])
        def api_configs(all: bool = False) -> dict:
            return {"configs": self._list_configs(include_non_bots=all)}

        @app.get("/api/strategies", dependencies=[Depends(auth)])
        def api_strategies() -> dict:
            return {"strategies": self._list_strategies()}

        # --------------- config manager (each config ~= one bot) ---------------
        @app.get("/api/configs/{name}/raw", dependencies=[Depends(auth)])
        def api_config_raw(name: str) -> dict:
            return {"content": self._read_config_raw(name)}

        @app.put("/api/configs/{name}/raw", dependencies=[Depends(auth)])
        def api_config_raw_save(name: str, req: ConfigRawRequest) -> dict:
            return self._write_config_raw(name, req.content)

        @app.post("/api/configs/{name}/duplicate", dependencies=[Depends(auth)])
        def api_config_duplicate(name: str, req: ConfigNameRequest) -> dict:
            return self._duplicate_config(name, req.new_name)

        @app.post("/api/configs/{name}/rename", dependencies=[Depends(auth)])
        def api_config_rename(name: str, req: ConfigNameRequest) -> dict:
            return self._rename_config(name, req.new_name)

        @app.delete("/api/configs/{name}", dependencies=[Depends(auth)])
        def api_config_delete(name: str) -> dict:
            return self._delete_config(name)

        @app.post("/api/configs/{name}/start", dependencies=[Depends(auth)])
        def api_config_start(name: str) -> dict:
            return self._start_config_bot(name)

        @app.post("/api/configs/{name}/stop", dependencies=[Depends(auth)])
        def api_config_stop(name: str) -> dict:
            return self._stop_config_bot(name)

        @app.post("/api/backtests/run", dependencies=[Depends(auth)])
        def api_backtest_run(req: BacktestRunRequest) -> dict:
            return self._submit_backtest_job(
                config=req.config,
                strategy=req.strategy,
                timerange=req.timerange,
                engine=req.engine,
            )

        @app.get("/api/backtests/jobs", dependencies=[Depends(auth)])
        def api_backtest_jobs() -> dict:
            return {"jobs": self._list_jobs(), "max_concurrent": self.MAX_CONCURRENT_JOBS}

        @app.get("/api/backtests/jobs/{job_id}", dependencies=[Depends(auth)])
        def api_backtest_job(job_id: str, log_offset: int = 0) -> dict:
            return self._get_job(job_id, log_offset=log_offset)

        @app.post("/api/backtests/jobs/{job_id}/cancel", dependencies=[Depends(auth)])
        def api_backtest_job_cancel(job_id: str) -> dict:
            return self._cancel_job(job_id)

        @app.post("/api/pairs_finder/run", dependencies=[Depends(auth)])
        def api_pairs_finder_run(req: PairsFinderRequest) -> dict:
            return self._submit_pairs_finder_job(req)

        @app.get("/api/pairs_finder/jobs/{job_id}", dependencies=[Depends(auth)])
        def api_pairs_finder_job(job_id: str, log_offset: int = 0) -> dict:
            return self._get_job(job_id, log_offset=log_offset)

        @app.post("/api/hyperopt/run", dependencies=[Depends(auth)])
        def api_hyperopt_run(req: HyperoptRunRequest) -> dict:
            return self._submit_hyperopt_job(req)

        @app.get("/api/hyperopt/jobs/{job_id}", dependencies=[Depends(auth)])
        def api_hyperopt_job(job_id: str, log_offset: int = 0) -> dict:
            return self._get_job(job_id, log_offset=log_offset)

        # --------------- data manager ---------------
        @app.get("/api/exchanges", dependencies=[Depends(auth)])
        def api_exchanges() -> dict:
            return {"exchanges": self._list_exchanges()}

        @app.get("/api/data/coverage", dependencies=[Depends(auth)])
        def api_data_coverage(exchange: str) -> dict:
            return self._data_coverage(exchange)

        @app.post("/api/data/download", dependencies=[Depends(auth)])
        def api_data_download(req: DataDownloadRequest) -> dict:
            return self._submit_download_job(req)

        @app.get("/api/backtests/{name}", dependencies=[Depends(auth)])
        def api_backtest(name: str) -> dict:
            return self._load_backtest(name)


        @app.get("/api/backtests/{name}/regime_analysis", dependencies=[Depends(auth)])
        def api_bt_regime_analysis(name: str, regime_pair: str = "") -> dict:
            return self._backtest_regime_analysis(name, regime_pair)

        @app.get("/api/backtests/{name}/volatility_bias", dependencies=[Depends(auth)])
        def api_bt_volatility_bias(name: str, regime_pair: str = "") -> dict:
            return self._backtest_volatility_bias(name, regime_pair)

        @app.get("/api/backtests/{name}/mae_mfe", dependencies=[Depends(auth)])
        def api_bt_mae_mfe(
            name: str,
            regime_pair: str = "__all__",
            n_clusters: int = 4,
        ) -> dict:
            return self._bt_mae_mfe_analysis(
                name, regime_pair=regime_pair, n_clusters=n_clusters
            )

        @app.get("/api/backtests/{name}/pair_candles", dependencies=[Depends(auth)])
        def api_bt_pair_candles(name: str, pair: str) -> dict:
            return self._bt_pair_candles(name, pair)

        # --------------- error handler ---------------

        # --------------- error handler ---------------
        @app.exception_handler(Exception)
        async def _all_exceptions(_: Request, exc: Exception) -> JSONResponse:
            logger.exception("web_portal unhandled error")
            return JSONResponse({"detail": str(exc)}, status_code=500)

        return app

    # ------------------------------------------------------------------
    # Data accessors
    # ------------------------------------------------------------------
    def _is_live_bot(self) -> bool:
        """True only when an actual trading bot (not a viewer-mode stub) is attached."""
        return self.bot is not None and hasattr(self.bot, "state")

    def _get_open_trades(self) -> list:
        if not self._is_live_bot():
            return self._trades_from_json(is_open=True)
        try:
            from VulcanTrader.persistence import Trade
            return list(Trade.get_open_trades())
        except Exception:
            logger.debug("get_open_trades failed", exc_info=True)
            return []

    def _get_closed_trades(self, limit: int) -> list:
        if not self._is_live_bot():
            return self._trades_from_json(is_open=False)[-limit:]
        try:
            from VulcanTrader.persistence import Trade
            try:
                return list(Trade.get_trades_proxy(is_open=False))[-limit:]
            except Exception:
                return [t for t in Trade.get_trades([]) if not t.is_open][-limit:]
        except Exception:
            logger.debug("get_closed_trades failed", exc_info=True)
            return []

    def _find_db_json(self) -> "Path | None":
        """Locate the bot's JSON trade database file for standalone/webserver mode."""
        # Try to resolve db_url from the config file used to start the subprocess
        config_name = self._bot_config_name
        if config_name:
            for cfg_path in (
                Path(config_name),
                self._configs_dir() / f"{config_name}.json",
                self._configs_dir() / config_name,
            ):
                if not cfg_path.is_file():
                    continue
                try:
                    with cfg_path.open("r", encoding="utf-8") as fh:
                        bot_cfg = json.load(fh)
                    db_url = bot_cfg.get("db_url", "")
                    if db_url.startswith("json:"):
                        from VulcanTrader.persistence.models import _json_path_from_url
                        p = _json_path_from_url(db_url)
                        if p.is_file():
                            return p
                except Exception:
                    logger.debug("_find_db_json: failed to read config %s", cfg_path, exc_info=True)
        # Fall back: common names, then recursive search under user_data/
        user_data = self._user_data_dir()
        for name in ("trades.live.json", "trades.dry_run.json"):
            p = user_data / name
            if p.is_file():
                return p
        candidates = sorted(
            user_data.rglob("trades*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return candidates[0] if candidates else None

    def _trades_from_json(self, is_open: bool) -> list:
        """Read trades from the bot's persisted JSON file (used in standalone/webserver mode)."""
        db = self._find_db_json()
        if db is None:
            return []
        try:
            with db.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            trades = data.get("trades", [])
            return [t for t in trades if bool(t.get("is_open", False)) == is_open]
        except Exception:
            logger.debug("_trades_from_json failed (%s)", db, exc_info=True)
            return []

    # ------------------------------------------------------------------
    # Multi-bot account detection (user_data/accounts/*.json)
    #
    # Every trader_bot.py process persists to one JSON "account" file and
    # holds an OS-level exclusive lock on the sibling ``<name>.json.lock``
    # for its whole lifetime (see persistence/models.py). Probing that lock
    # tells us definitively whether the owning process is alive right now.
    # The account JSON additionally carries KeyValueStore liveness markers
    # (``is_running`` / ``last_heartbeat``) written by the bot, used as a
    # fallback and to flag crashed bots (is_running=1 but lock free).
    # ------------------------------------------------------------------

    HEARTBEAT_STALE_SECS = 180.0

    @staticmethod
    def _probe_db_lock(db_path: Path) -> bool | None:
        """True → a process holds the writer lock (bot running); False → free;
        None → probe failed (fall back to heartbeat markers)."""
        lock_path = db_path.with_name(db_path.name + ".lock")
        if not lock_path.exists():
            return False
        try:
            fd = os.open(str(lock_path), os.O_RDWR)
        except OSError:
            return None
        try:
            if sys.platform == "win32":
                import msvcrt
                try:
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                    return False
                except OSError:
                    return True
            else:
                import fcntl
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return False
                except OSError:
                    return True
        except Exception:
            return None
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    def _accounts_dir(self) -> Path:
        return self._user_data_dir() / "accounts"

    def _list_account_dbs(self) -> list[Path]:
        """All candidate persistence DBs: accounts/*.json plus legacy
        user_data/trades*.json. First occurrence of a stem wins."""
        out: dict[str, Path] = {}
        acc = self._accounts_dir()
        if acc.is_dir():
            for p in sorted(acc.glob("*.json")):
                out.setdefault(p.stem, p)
        ud = self._user_data_dir()
        if ud.is_dir():
            for p in sorted(ud.glob("trades*.json")):
                out.setdefault(p.stem, p)
        return list(out.values())

    def _find_account_db(self, name: str) -> Path | None:
        """Resolve a bot name from /api/livebots back to its DB path.
        Matching by scanned stem (never by joining paths) rules out traversal."""
        for p in self._list_account_dbs():
            if p.stem == name:
                return p
        return None

    def _account_bot_info(self, path: Path, data: dict | None = None) -> dict | None:
        """Summarise one account DB: identity, liveness, uptime, trade counts."""
        if data is None:
            try:
                data = json.loads(path.read_text(encoding="utf-8") or "{}")
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                logger.debug("account scan: unreadable %s", path, exc_info=True)
                return None
        if not isinstance(data, dict):
            return None
        trades = data.get("trades") or []
        kv = {row.get("key"): row for row in data.get("KeyValueStore") or []
              if isinstance(row, dict)}

        def _kv_dt(key: str) -> datetime | None:
            raw = (kv.get(key) or {}).get("datetime_value")
            if not raw:
                return None
            try:
                dt = datetime.fromisoformat(raw)
                return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt
            except ValueError:
                return None

        now = datetime.now(UTC)
        startup = _kv_dt("startup_time")
        heartbeat = _kv_dt("last_heartbeat")
        kv_running = (kv.get("is_running") or {}).get("int_value") == 1
        hb_age = (now - heartbeat).total_seconds() if heartbeat else None
        bot_name = (kv.get("bot_name") or {}).get("string_value")

        # Prefer the in-process registry (single_process_mode) over the
        # lock-file probe when this process itself launched the account:
        # fcntl advisory locks are scoped per-PROCESS on POSIX, not per-
        # file-handle, so a probe issued from the very process that already
        # holds the lock always reports "free" there - see
        # bot.py's in_process_bot_running docstring.
        in_proc: bool | None = None
        try:
            from VulcanTrader.bot import in_process_bot_running
            in_proc = in_process_bot_running(path.stem)
        except ImportError:
            pass
        lock_held = in_proc if in_proc is not None else self._probe_db_lock(path)
        if lock_held is None:
            running = kv_running and hb_age is not None and hb_age < self.HEARTBEAT_STALE_SECS
        else:
            running = lock_held
        # Bot said it was running but nothing holds the lock → died without cleanup.
        crashed = kv_running and not running

        open_count = sum(1 for t in trades if t.get("is_open"))
        closed = [t for t in trades if not t.get("is_open")]
        last_trade = trades[-1] if trades else {}
        name_l = path.stem.lower()
        mode = "live" if "live" in name_l else ("dry_run" if "dry" in name_l else "unknown")
        try:
            mtime = _ts_to_iso(path.stat().st_mtime)
        except OSError:
            mtime = None
        return {
            "name": path.stem,
            # Human-friendly label (config "bot_name"); falls back to the
            # account/db name for accounts persisted before this field existed.
            "bot_name": bot_name or path.stem,
            "file": str(path),
            "mode": mode,
            "running": running,
            "crashed": crashed,
            "started_at": startup.isoformat() if startup else None,
            "uptime_secs": max(0.0, (now - startup).total_seconds()) if running and startup else None,
            "bot_start_time": (_kv_dt("bot_start_time") or startup).isoformat()
            if (_kv_dt("bot_start_time") or startup) else None,
            "last_heartbeat": heartbeat.isoformat() if heartbeat else None,
            "heartbeat_age_secs": hb_age,
            "strategy": last_trade.get("strategy"),
            "exchange": last_trade.get("exchange"),
            "open_trades": open_count,
            "closed_trades": len(closed),
            "closed_profit_abs": sum(float(t.get("close_profit_abs") or 0) for t in closed),
            "last_updated": mtime,
        }

    def _scan_live_bots(self) -> list[dict]:
        bots = [info for p in self._list_account_dbs()
                if (info := self._account_bot_info(p)) is not None]
        bots.sort(key=lambda b: (not b["running"], b["name"].lower()))
        return bots

    def _request_account_stop(self, name: str) -> dict:
        """Gracefully stop the external trader_bot process owning ``name``'s DB.

        Creates ``<db>.json.stop`` next to the persistence file; the bot's trade
        loop (bot.py cmd_trade) polls for it every cycle and shuts down through
        its normal cleanup path — same as a local Ctrl+C. There is no reliable
        cross-process graceful signal on Windows, so a stop-file it is."""
        path = self._find_account_db(name)
        if path is None:
            raise HTTPException(status_code=404, detail=f"No account database named {name!r}")
        info = self._account_bot_info(path)
        if info is None:
            raise HTTPException(status_code=502, detail="Account DB malformed")
        if not info["running"]:
            raise HTTPException(status_code=409, detail=f"Bot {name!r} is not running.")
        # Guard: if this account IS the attached bot's own DB, stopping it would
        # take this portal down with it — that path belongs to the Stop button.
        if self._is_live_bot() and self.config.get("db_url"):
            try:
                from VulcanTrader.persistence.models import _path_from_db_url
                if _path_from_db_url(self.config["db_url"]) == path:
                    raise HTTPException(
                        status_code=400,
                        detail="This account belongs to the portal's own bot — "
                               "deselect the account and use the regular Stop button.",
                    )
            except HTTPException:
                raise
            except Exception:
                pass
        # Instant path for a bot this process is itself hosting in-process
        # (single_process_mode, see bot.py's launch_bot_in_process) - no
        # need for the ~2s stop-file poll below when we can set its
        # stop_event directly.
        try:
            from VulcanTrader.bot import stop_in_process_bot
            if stop_in_process_bot(name):
                logger.info("Stop requested for in-process bot account %s", name)
                return {
                    "requested": True,
                    "detail": "Stop requested — this bot runs in-process and will "
                              "exit within one cycle.",
                }
        except ImportError:
            pass
        stop_file = path.with_name(path.name + ".stop")
        try:
            stop_file.touch()
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not write stop file: {exc}") from exc
        logger.info("Stop requested for bot account %s (%s)", name, stop_file)
        return {
            "requested": True,
            "detail": "Stop requested — the bot registers it within ~2 s and exits "
                      "gracefully once its current processing cycle finishes.",
        }

    def _request_account_force_exit(self, name: str, pair: str) -> dict:
        """Request a force-exit of *pair* on the trader_bot process owning ``name``'s account.

        If ``name`` happens to be the bot attached to this very process, the
        request is applied in-process immediately (no polling delay) via
        ``_schedule_force_exit_pair``. Otherwise it is handed off the same way
        :meth:`_request_account_stop` hands off stop requests: a small JSON
        file (``<db>.json.forceexit``, a list of pair strings) next to the
        account DB, which that bot's own process polls every ~2s (see
        bot.py's ``_forceexit_file_watcher``) and drains on its next cycle.
        """
        path = self._find_account_db(name)
        if path is None:
            raise HTTPException(status_code=404, detail=f"No account database named {name!r}")

        if (
            self._is_live_bot()
            and self.config.get("db_url")
            and hasattr(self.bot, "_schedule_force_exit_pair")
        ):
            try:
                from VulcanTrader.persistence.models import _path_from_db_url
                if _path_from_db_url(self.config["db_url"]) == path:
                    self.bot._schedule_force_exit_pair(pair)
                    return {
                        "requested": True,
                        "detail": f"Force-exit scheduled for {pair!r} on the attached bot "
                                  "- will execute on the next process cycle.",
                    }
            except Exception:
                logger.debug("in-process force-exit dispatch failed", exc_info=True)

        info = self._account_bot_info(path)
        if info is None:
            raise HTTPException(status_code=502, detail="Account DB malformed")
        if not info["running"]:
            raise HTTPException(status_code=409, detail=f"Bot {name!r} is not running.")

        fe_file = path.with_name(path.name + ".forceexit")
        pending: list[str] = []
        try:
            if fe_file.is_file():
                pending = json.loads(fe_file.read_text(encoding="utf-8") or "[]")
        except (OSError, ValueError):
            pending = []
        if pair not in pending:
            pending.append(pair)
        try:
            tmp = fe_file.with_name(fe_file.name + f".{uuid.uuid4().hex[:8]}.tmp")
            tmp.write_text(json.dumps(pending), encoding="utf-8")
            os.replace(tmp, fe_file)
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not write forceexit file: {exc}") from exc
        logger.info("Force-exit requested for %s on bot account %s (%s)", pair, name, fe_file)
        return {
            "requested": True,
            "detail": f"Force-exit for {pair!r} requested - the bot registers it within ~2 s "
                      "and executes it on its next processing cycle.",
        }

    @staticmethod
    def _starting_capital_from_config(cfg: dict, dry_run: bool) -> float:
        """`dry_run_wallet` for a paper account, `available_capital` for a
        live one - falls back to 1000.0 (the frontend's own hardcoded
        default) if neither is set, matching the backtest-results path's
        existing fallback (see the "Starting capital" block above)."""
        val = cfg.get("dry_run_wallet") if dry_run else cfg.get("available_capital")
        try:
            return float(val) if val is not None else 1000.0
        except (TypeError, ValueError):
            return 1000.0

    def _starting_capital_for_account(self, name: str, dry_run: bool) -> float:
        """Starting capital for a CROSS-account dashboard view: read from
        THAT bot's own config file, not `self.config` (which - if this
        portal is itself attached to a bot - belongs to a different one).
        Account names are always "<config-stem>_dry_run"/"<config-stem>_live"
        (see _list_configs), so the config stem is recovered by stripping
        that suffix back off."""
        stem = name
        for suffix in ("_dry_run", "_live"):
            if stem.endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        try:
            cfg_path = self._config_path(stem)
            if not cfg_path.is_file():
                return 1000.0
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return 1000.0
        return self._starting_capital_from_config(cfg, dry_run)

    def _account_dashboard(self, name: str, closed_limit: int) -> dict:
        """Dashboard payload built entirely from another bot's account DB."""
        path = self._find_account_db(name)
        if path is None:
            raise HTTPException(status_code=404, detail=f"No account database named {name!r}")
        try:
            data = json.loads(path.read_text(encoding="utf-8") or "{}")
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise HTTPException(status_code=502, detail=f"Account DB unreadable: {exc}") from exc
        info = self._account_bot_info(path, data)
        if info is None:
            raise HTTPException(status_code=502, detail="Account DB malformed")
        trades = data.get("trades") or []
        from VulcanTrader.constants import __version__
        status = {
            "attached": False,
            "account": name,
            "version": __version__,
            "state": "RUNNING" if info["running"] else "STOPPED",
            "crashed": info["crashed"],
            "dry_run": info["mode"] != "live",
            "mode": info["mode"],
            "exchange": info["exchange"],
            "strategy": info["strategy"],
            "bot_name": info["bot_name"],
            "started_at": info["started_at"],
            "uptime_secs": info["uptime_secs"],
            "last_heartbeat": info["last_heartbeat"],
            "starting_capital": self._starting_capital_for_account(name, info["mode"] != "live"),
        }
        open_trade_list = [t for t in trades if t.get("is_open")]
        by_exchange: dict[str, list[str]] = {}
        for t in open_trade_list:
            exch = t.get("exchange")
            if exch and t.get("pair"):
                by_exchange.setdefault(exch, []).append(t["pair"])
        for exch, pairs in by_exchange.items():
            self._prefetch_tickers_ccxt_public(exch, pairs)

        return {
            "status": status,
            "open_trades": [self._attach_live_pnl(_json_safe(t)) for t in open_trade_list],
            "closed_trades": [_json_safe(t) for t in trades if not t.get("is_open")][-closed_limit:],
            # Another process's notifications/log stream isn't visible from here.
            "messages": [],
            "logs": [],
        }

    def _get_pair_locks(self) -> list:
        try:
            from VulcanTrader.persistence import PairLocks
            locks = PairLocks.get_pair_locks(None)
            return [_json_safe(l.to_json() if hasattr(l, "to_json") else l) for l in locks]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Strategy / chart data accessors
    # ------------------------------------------------------------------
    def _strategy(self) -> Any:
        if self.bot is None:
            return None
        return getattr(self.bot, "strategy", None)

    def _strategy_info(self) -> dict:
        strat = self._strategy()
        if strat is None:
            return {
                "name": None,
                "timeframe": None,
                "plot_config": {"main_plot": {}, "subplots": {}},
                "pairs": [],
            }
        # plot_config: {"main_plot": {col: {color, type}}, "subplots": {name: {col: {...}}}}
        plot_config = getattr(strat, "plot_config", {}) or {}
        plot_config.setdefault("main_plot", {})
        plot_config.setdefault("subplots", {})

        whitelist: list[str] = []
        try:
            whitelist = list(getattr(self.bot, "active_pair_whitelist", []) or [])
        except Exception:
            pass

        return {
            "name": getattr(strat, "__class__", type(strat)).__name__,
            "timeframe": getattr(strat, "timeframe", None),
            "stoploss": getattr(strat, "stoploss", None),
            "can_short": bool(getattr(strat, "can_short", False)),
            "plot_config": _json_safe(plot_config),
            "pairs": whitelist,
        }

    def _pair_candles(self, pair: str, timeframe: str | None, limit: int) -> dict:
        """
        Return candles + every column the strategy added (indicators, signals)
        from the analyzed DataFrame cached by the DataProvider.
        """
        strat = self._strategy()
        if strat is None or self.bot is None:
            raise HTTPException(status_code=503, detail="Bot not running")
        tf = timeframe or getattr(strat, "timeframe", None)
        if not tf:
            raise HTTPException(status_code=400, detail="timeframe required")
        try:
            dp = getattr(self.bot, "dataprovider", None) or getattr(strat, "dp", None)
            if dp is None:
                raise HTTPException(status_code=503, detail="DataProvider unavailable")
            df, last_analyzed = dp.get_analyzed_dataframe(pair, tf)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"get_analyzed_dataframe: {exc}") from exc

        if df is None or len(df) == 0:
            return {"pair": pair, "timeframe": tf, "candles": [], "columns": [], "indicators": []}

        df = df.tail(max(50, min(limit, 5000))).copy()

        # Convert datetimes to ISO
        if "date" in df.columns:
            try:
                df["date"] = df["date"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                df["date"] = df["date"].astype(str)

        # Drop columns that aren't JSON friendly / are bulky
        skip = {"buy", "sell"}  # legacy aliases - keep entry/exit columns
        records: list[dict] = []
        cols = [c for c in df.columns if c not in skip]
        for row in df[cols].itertuples(index=False, name=None):
            rec = {}
            for k, v in zip(cols, row, strict=False):
                if v is None:
                    rec[k] = None
                    continue
                try:
                    f = float(v)
                    if f != f or f in (float("inf"), float("-inf")):  # NaN/Inf
                        rec[k] = None
                    else:
                        rec[k] = f
                except (TypeError, ValueError):
                    rec[k] = str(v)
            records.append(rec)

        return {
            "pair": pair,
            "timeframe": tf,
            "last_analyzed": last_analyzed.isoformat() if last_analyzed else None,
            "columns": cols,
            "indicators": [
                c for c in cols
                if c not in {"date", "open", "high", "low", "close", "volume",
                             "enter_long", "exit_long", "enter_short", "exit_short",
                             "enter_tag", "exit_tag"}
            ],
            "candles": records,
        }

    def _pair_trades(self, pair: str) -> list[dict]:
        if self.bot is None:
            return []
        try:
            from VulcanTrader.persistence import Trade
            trades: list = []
            try:
                trades = [t for t in Trade.get_trades_proxy(pair=pair)]
            except Exception:
                trades = [t for t in Trade.get_trades([]) if getattr(t, "pair", None) == pair]
            return [_trade_to_dict(t) for t in trades]
        except Exception:
            logger.debug("pair_trades failed", exc_info=True)
            return []

    # ------------------------------------------------------------------
    # Backtest result discovery
    # ------------------------------------------------------------------
    def _backtest_dir(self) -> Path:
        if self.config.get("user_data_dir"):
            return Path(self.config["user_data_dir"]) / "backtest_results"
        return REPO_ROOT / "user_data" / "backtest_results"

    def _list_backtests(self) -> list[dict]:
        d = self._backtest_dir()
        if not d.is_dir():
            return []
        out = []
        for p in sorted(d.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
            if p.name.endswith(".meta.json"):
                continue
            try:
                out.append({
                    "name": p.stem,
                    "filename": p.name,
                    "size": p.stat().st_size,
                    "modified": p.stat().st_mtime,
                })
            except Exception:
                continue
        return out

    def _load_backtest(self, name: str) -> dict:
        d = self._backtest_dir()
        # Resolve safely: prevent path traversal
        candidate = (d / f"{name}.json").resolve()
        try:
            candidate.relative_to(d.resolve())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid backtest name") from exc
        if not candidate.is_file():
            raise HTTPException(status_code=404, detail=f"Backtest '{name}' not found")
        try:
            with candidate.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to load: {exc}") from exc

    # ------------------------------------------------------------------
    # Backtest runner (subprocess job queue, capped concurrency)
    # ------------------------------------------------------------------
    _NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
    _TIMERANGE_RE = re.compile(r"^[0-9]{0,8}-[0-9]{0,8}$")

    def _user_data_dir(self) -> Path:
        if self.config.get("user_data_dir"):
            return Path(self.config["user_data_dir"])
        return REPO_ROOT / "user_data"

    def _configs_dir(self) -> Path:
        return self._user_data_dir() / "configs"

    def _strategies_dir(self) -> Path:
        return self._user_data_dir() / "strategies"

    def _data_dir(self) -> Path:
        return self._user_data_dir() / "data"

    # ------------------------------------------------------------------
    # Data manager: exchange list + per-exchange OHLCV coverage
    # ------------------------------------------------------------------
    def _list_exchanges(self) -> list[dict]:
        """Every exchange either used by a config or with existing downloaded
        data, so a newly-configured exchange with nothing downloaded yet
        still shows up (as a zero-coverage row) instead of being invisible
        until the first download. ``is_bot`` doesn't matter here - which
        configs appear in the fleet's Configs table is a different concern
        from which exchanges have data."""
        by_name: dict[str, list[str]] = {}
        cfg_dir = self._configs_dir()
        if cfg_dir.is_dir():
            for p in sorted(cfg_dir.glob("*.json")):
                try:
                    raw = json.loads(p.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                if not isinstance(raw, dict):
                    continue
                ex = (raw.get("exchange") or {}).get("name")
                if ex:
                    by_name.setdefault(ex, []).append(p.stem)

        data_dir = self._data_dir()
        if data_dir.is_dir():
            for p in sorted(data_dir.iterdir()):
                if p.is_dir() and not p.name.startswith("."):
                    by_name.setdefault(p.name, [])

        out = []
        for name in sorted(by_name):
            ex_dir = data_dir / name
            out.append({
                "name": name,
                "configs": by_name[name],
                "has_data": ex_dir.is_dir(),
                "has_futures": (ex_dir / "futures").is_dir(),
            })
        return out

    @staticmethod
    def _feather_date_min_max_count(path: Path) -> tuple[str | None, str | None, int]:
        """Read ONLY the `date` column of a feather OHLCV file - not the full
        dataframe (`FeatherDataHandler.ohlcv_data_min_max` loads every OHLCV
        column, casts dtypes, the works). Coverage only ever needs the first/
        last timestamp and a row count, and an exchange like Hyperliquid can
        have 10,000+ of these files (every pair x every timeframe x spot/
        futures/funding_rate) - full-dataframe loads made this endpoint take
        minutes; a single-column columnar read is fast enough to do live."""
        import pyarrow.feather as feather

        try:
            table = feather.read_table(path, columns=["date"])
        except Exception:
            return None, None, 0
        n = table.num_rows
        if n == 0:
            return None, None, 0
        col = table.column("date")
        start = col[0].as_py()
        end = col[-1].as_py()
        return (
            start.isoformat() if start else None,
            end.isoformat() if end else None,
            n,
        )

    def _data_coverage(self, exchange: str) -> dict:
        """Per pair+timeframe OHLCV coverage for one exchange: start/end date,
        candle count, file size, last modified. Filename/pair/timeframe
        parsing reuses the project's own ``IDataHandler``/``FeatherDataHandler``
        (see data/history/datahandlers) so this always agrees with what the
        bots/backtester consider a valid data file - only the actual date-
        range read is a lightweight custom one (see `_feather_date_min_max_count`)."""
        exchange = self._validate_name(exchange, "exchange")
        ex_dir = self._data_dir() / exchange
        empty_summary = {
            "pair_count": 0, "file_count": 0, "total_size_bytes": 0,
            "earliest": None, "latest": None,
        }
        if not ex_dir.is_dir():
            return {"exchange": exchange, "rows": [], "pairs": [], "timeframes": [], "summary": empty_summary}

        from VulcanTrader.data.history.datahandlers import get_datahandler
        from VulcanTrader.enums import TradingMode

        handler = get_datahandler(ex_dir, "feather")
        modes = [TradingMode.SPOT]
        if (ex_dir / "futures").is_dir():
            modes.append(TradingMode.FUTURES)

        specs: list[tuple[str, str, Any, Path]] = []
        for mode in modes:
            for pair, timeframe, candle_type in handler.ohlcv_get_available_data(ex_dir, mode):
                path = handler._pair_data_filename(ex_dir, pair, timeframe, candle_type)
                specs.append((pair, timeframe, candle_type, path))

        def _read_one(spec: tuple[str, str, Any, Path]) -> dict | None:
            pair, timeframe, candle_type, path = spec
            start, end, count = self._feather_date_min_max_count(path)
            try:
                st = path.stat()
                size, mtime = st.st_size, _ts_to_iso(st.st_mtime)
            except OSError:
                size, mtime = 0, None
            return {
                "pair": pair,
                "timeframe": timeframe,
                "candle_type": candle_type.value,
                "start": start,
                "end": end,
                "candles": count,
                "size_bytes": size,
                "modified": mtime,
            }

        rows: list[dict] = []
        if specs:
            # Thread pool, not sequential - thousands of small file reads are
            # I/O-bound enough (and pyarrow releases the GIL during the
            # actual read) that this matters: an exchange the size of
            # Hyperliquid's futures set is ~10k files.
            with ThreadPoolExecutor(max_workers=64) as pool:
                rows = list(pool.map(_read_one, specs))
        rows.sort(key=lambda r: (r["pair"], r["timeframe"], r["candle_type"]))

        starts = [r["start"] for r in rows if r["start"]]
        ends = [r["end"] for r in rows if r["end"]]
        summary = {
            "pair_count": len({r["pair"] for r in rows}),
            "file_count": len(rows),
            "total_size_bytes": sum(r["size_bytes"] for r in rows),
            "earliest": min(starts) if starts else None,
            "latest": max(ends) if ends else None,
        }
        return {
            "exchange": exchange,
            "rows": rows,
            "pairs": sorted({r["pair"] for r in rows}),
            "timeframes": sorted({r["timeframe"] for r in rows}),
            "summary": summary,
        }

    def _estimate_download_units(
        self, cfg_name: str, pairs_override: list[str] | None, timeframes_override: list[str] | None
    ) -> tuple[int, int]:
        """Best-effort (pairs_total, units_total) estimate for a download-data
        job's progress bar/ETA - "units" meaning (pair, timeframe, candle_type)
        combinations, matching one "Downloaded data for ..." log line each
        (see history_utils.py's `_download_pair_history`). Not exact - futures
        mode also downloads a mark-price and a funding-rate series per pair
        (see `refresh_backtest_ohlcv_data`), approximated here as +2 timeframe
        slots rather than replicating that expansion logic exactly. Good
        enough for progress display; never blocks job submission if the
        config can't be read for any reason (returns (0, 0), meaning the UI
        just shows a spinner instead of a bar)."""
        try:
            raw = json.loads((self._configs_dir() / f"{cfg_name}.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return (0, 0)
        if not isinstance(raw, dict):
            return (0, 0)

        pairs = pairs_override or (raw.get("exchange") or {}).get("pair_whitelist") or []
        timeframes = timeframes_override or raw.get("timeframes")
        if not timeframes:
            tf = raw.get("timeframe")
            # Matches history_utils.py's own download_data(): when a config
            # has no "timeframe"/"timeframes" at all, it silently downloads
            # DL_DATA_TIMEFRAMES = ["1m", "5m"] - both, not a single implied
            # one. Getting this wrong badly undercounts the total (1m history
            # needs far more paginated calls than 5m/15m), making the bar
            # look done long before the job actually finishes.
            timeframes = [tf] if tf else ["1m", "5m"]

        pairs_total = len(pairs)
        units_per_pair = len(timeframes) + (2 if raw.get("trading_mode") == "futures" else 0)
        return (pairs_total, pairs_total * max(1, units_per_pair))

    def _submit_download_job(self, req: DataDownloadRequest) -> dict:
        cfg_name = self._validate_name(req.config, "config")
        cfg_path = self._configs_dir() / f"{cfg_name}.json"
        if not cfg_path.is_file():
            raise HTTPException(status_code=404, detail=f"Config not found: {cfg_name}.json")
        timerange = self._validate_timerange(req.timerange) if req.timerange else None

        if self._running_job_count() >= self.MAX_CONCURRENT_JOBS:
            raise HTTPException(
                status_code=429,
                detail=f"Max concurrent jobs reached ({self.MAX_CONCURRENT_JOBS}). Wait for one to finish.",
            )

        pairs_total, units_total = self._estimate_download_units(cfg_name, req.pairs, req.timeframes)

        job_id = uuid.uuid4().hex[:12]
        job: dict = {
            "id": job_id,
            "kind": "download",
            "config": cfg_name,
            "exchange": req.exchange,
            "pairs": list(req.pairs) if req.pairs else None,
            "timeframes": list(req.timeframes) if req.timeframes else None,
            "days": int(req.days) if req.days else None,
            "timerange": timerange,
            "erase": bool(req.erase),
            "status": "queued",
            "exit_code": None,
            "created_at": datetime.now(UTC).isoformat(),
            "started_at": None,
            "finished_at": None,
            "log": deque(maxlen=self.MAX_JOB_LOG_LINES),
            "result_file": None,
            "error": None,
            "_proc": None,
            # Progress (see _estimate_download_units) - dl_units_done is
            # incremented by counting "Downloaded data for " log lines as
            # they stream from the subprocess.
            "dl_pairs_total": pairs_total,
            "dl_units_total": units_total,
            "dl_units_done": 0,
        }
        with self._jobs_lock:
            self._jobs[job_id] = job
            self._evict_old_jobs_locked()

        thread = threading.Thread(
            target=self._run_download_job_thread,
            args=(job_id,),
            name=f"download-job-{job_id}",
            daemon=True,
        )
        thread.start()
        return {"ok": True, "job": self._job_summary(job)}

    def _run_download_job_thread(self, job_id: str) -> None:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job["status"] = "running"
            job["started_at"] = datetime.now(UTC).isoformat()

        cmd = [
            sys.executable, "-m", "VulcanTrader.bot", "download-data",
            "-v",  # INFO level - without it, "Downloaded data for ..." (what
                   # the progress counter below greps for) is never emitted
                   # at all, since default verbosity is WARNING-only.
            "-c", job["config"],
            "--user-data-dir", str(self._user_data_dir()),
        ]
        if job.get("exchange"):
            cmd += ["--exchange", job["exchange"]]
        if job.get("pairs"):
            cmd += ["--pairs", *job["pairs"]]
        if job.get("timeframes"):
            cmd += ["--timeframes", *job["timeframes"]]
        if job.get("timerange"):
            cmd += ["--timerange", job["timerange"]]
        elif job.get("days"):
            cmd += ["--days", str(job["days"])]
        if job.get("erase"):
            cmd.append("--erase")

        job["log"].append(f"$ {' '.join(cmd)}")
        # A real, independent subprocess - not just "not the same Python
        # object": stdout is piped back for the live log viewer/progress
        # counter above, but it must survive independently of this web
        # portal process's own lifetime. Without the flags below, a plain
        # Popen's child is still tied to whatever Windows Job Object this
        # interpreter itself happens to be running in (common in sandboxed/
        # managed dev environments, CI runners, some IDE/terminal
        # supervisors) - that job's own cleanup silently kills the child the
        # moment it fires, which is exactly the bug already found and fixed
        # for the data_server master in data_server.py's ensure_master_running.
        # Same fix here: CREATE_BREAKAWAY_FROM_JOB (Windows) / a new session
        # (POSIX) detach it, while keeping stdout=PIPE so log capture is
        # unaffected - unlike launch_detached()'s CREATE_NEW_CONSOLE path
        # (used for trade bots), this can't give the download job its own
        # visible console without losing the ability to stream its output
        # into the dashboard.
        popen_kwargs: dict[str, Any] = {}
        if sys.platform == "win32":
            popen_kwargs["creationflags"] = subprocess.CREATE_BREAKAWAY_FROM_JOB
        else:
            popen_kwargs["start_new_session"] = True
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                text=True,
                encoding="utf-8",
                errors="replace",
                **popen_kwargs,
            )
        except Exception as exc:
            with self._jobs_lock:
                job["status"] = "error"
                job["error"] = str(exc)
                job["finished_at"] = datetime.now(UTC).isoformat()
            logger.exception("Failed to launch download job %s", job_id)
            return

        with self._jobs_lock:
            job["_proc"] = proc

        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                line = line.rstrip("\r\n")
                with self._jobs_lock:
                    job["log"].append(line)
                    if "Downloaded data for " in line:
                        job["dl_units_done"] += 1
        except Exception as exc:
            logger.exception("download job %s stream error", job_id)
            with self._jobs_lock:
                job["log"].append(f"[stream error] {exc}")

        rc = proc.wait()
        with self._jobs_lock:
            job["exit_code"] = rc
            job["status"] = "succeeded" if rc == 0 else ("cancelled" if rc < 0 else "failed")
            job["finished_at"] = datetime.now(UTC).isoformat()
            job["_proc"] = None

    def _scan_trade_processes(self) -> dict[str, psutil.Process]:
        """config-name -> psutil.Process for every currently-running
        ``bot.py trade`` subprocess, found by scanning OS processes rather
        than tracking PIDs at launch time - simpler, and still correct even
        for a bot started outside this portal's own launch code (a manual
        CLI ``bot.py trade``, or one from a previous portal instance)."""
        groups: dict[str, list[psutil.Process]] = {}
        for proc in psutil.process_iter(["cmdline"]):
            try:
                cmdline = proc.info["cmdline"] or []
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
            if "VulcanTrader.bot" not in cmdline or "trade" not in cmdline:
                continue
            if "-c" not in cmdline:
                continue
            idx = cmdline.index("-c")
            if idx + 1 < len(cmdline):
                groups.setdefault(cmdline[idx + 1], []).append(proc)

        out: dict[str, psutil.Process] = {}
        for config_name, procs in groups.items():
            if len(procs) == 1:
                out[config_name] = procs[0]
                continue
            # More than one OS process matches this config's cmdline - on Windows, a
            # venv "Scripts\python.exe" launcher can itself be a thin stub that
            # re-execs into the real interpreter (e.g. the machine's global
            # "Python312" install) as a CHILD process, so the SAME `-m
            # VulcanTrader.bot trade -c <config>` invocation shows up as two OS
            # processes: the near-empty wrapper and the real worker actually
            # running the bot. Picking whichever psutil.process_iter() happened
            # to enumerate last (the old behavior - dict overwrite, no
            # preference) made stats flicker between the wrapper's numbers
            # (confirmed live: ~3MB RSS, 0.0% CPU) and the real worker's
            # (hundreds of MB) depending on OS enumeration order, making a
            # genuinely healthy bot look dead in the dashboard. Prefer whichever
            # process in the group is a CHILD of another process in the same
            # group (the real worker, spawned by the wrapper) - fall back to
            # the highest-RSS process if no such parent/child relationship is
            # found (e.g. two independent matches for some other reason), since
            # that's still a better guess than an arbitrary/unstable pick.
            pids = {p.pid for p in procs}
            child = None
            for p in procs:
                try:
                    if p.ppid() in pids:
                        child = p
                        break
                except psutil.Error:
                    continue
            if child is None:
                try:
                    child = max(procs, key=lambda p: p.memory_info().rss)
                except psutil.Error:
                    child = procs[0]
            out[config_name] = child
        return out

    def _proc_stats(self, proc: psutil.Process | None) -> dict:
        """(cpu_percent, memory_mb) for one process, or (None, None) if not
        running / no longer queryable. cpu_percent is normalised to 0-100%
        of TOTAL system capacity (divided by logical core count) to match
        what Task Manager shows, rather than psutil's raw per-core-normalised
        default (which can read over 100% for a multi-threaded process).

        Caches psutil.Process objects by PID across calls (see
        __init__) because Process.cpu_percent() measures the delta since
        its OWN previous call - called fresh every time, it can only ever
        return 0.0 on the first sample. The dashboard polls this periodically
        (see trading.html's auto-refresh), so the cache naturally gets a real
        reading from the second poll onward."""
        if proc is None:
            return {"cpu_percent": None, "memory_mb": None}
        cached = self._proc_stat_cache.get(proc.pid)
        if cached is None:
            cached = proc
            try:
                cached.cpu_percent(interval=None)  # prime - discard the meaningless first 0.0
                mem = cached.memory_info().rss / (1024 * 1024)
            except psutil.Error:
                return {"cpu_percent": None, "memory_mb": None}
            self._proc_stat_cache[proc.pid] = cached
            # Memory needs no priming - only cpu_percent's first call is meaningless.
            return {"cpu_percent": None, "memory_mb": round(mem, 1)}
        try:
            cpu = cached.cpu_percent(interval=None) / (psutil.cpu_count() or 1)
            mem = cached.memory_info().rss / (1024 * 1024)
            return {"cpu_percent": round(cpu, 1), "memory_mb": round(mem, 1)}
        except psutil.Error:
            self._proc_stat_cache.pop(proc.pid, None)
            return {"cpu_percent": None, "memory_mb": None}

    def _get_ccxt_public_client(self, exchange_name: str) -> Any | None:
        """Pooled, keyless ccxt client for read-only public ticker lookups
        (see _current_price_ccxt_public/_prefetch_tickers_ccxt_public).
        `timeout` is set explicitly (ccxt's own default is 10s) - it's the
        first line of defence against a slow/degraded exchange stalling a
        dashboard request; `_run_with_deadline` below is the second, tighter
        one, since a hung DNS lookup or the rate-limiter's own pre-request
        sleep aren't always covered by ccxt's HTTP-level timeout."""
        client = self._ccxt_public_pool.get(exchange_name)
        if client is not None:
            return client
        import ccxt
        exchange_class = getattr(ccxt, exchange_name, None)
        if exchange_class is None:
            return None
        client = exchange_class({"enableRateLimit": True, "timeout": 5000})
        self._ccxt_public_pool[exchange_name] = client
        return client

    def _run_with_deadline(self, func, *args):
        """Run `func(*args)` on a worker thread and wait at most
        TICKER_FETCH_TIMEOUT_S for it. Raises on timeout/failure - callers
        catch broadly and fall back to cached/no price, same as any other
        fetch failure. Deliberately does NOT wait for the worker on timeout
        (`shutdown(wait=False)`): a slow call is left to finish on its own
        and just warms the cache for next time, rather than making THIS
        request pay for however long it actually takes."""
        import concurrent.futures as cf
        executor = cf.ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(func, *args)
            return future.result(timeout=self.TICKER_FETCH_TIMEOUT_S)
        finally:
            executor.shutdown(wait=False)

    def _current_price_ccxt_public(self, exchange_name: str, pair: str) -> float | None:
        """Cached, keyless ticker read via a plain ccxt client - used only
        for pricing another bot's open trades (see _account_dashboard),
        where there is no live, already-authenticated Exchange object to
        reuse. Never touches balances/orders/keys, so it's safe to call
        against a live-mode bot's exchange too."""
        key = f"{exchange_name}:{pair}"
        now = time.time()
        cached = self._ticker_cache.get(key)
        if cached is not None and now - cached[0] < self.TICKER_CACHE_TTL:
            return cached[1]
        try:
            client = self._get_ccxt_public_client(exchange_name)
            if client is None:
                return cached[1] if cached else None
            ticker = self._run_with_deadline(client.fetch_ticker, pair)
            price = ticker.get("last") or ticker.get("close") or ticker.get("bid")
            if price is None:
                return cached[1] if cached else None
            price = float(price)
            self._ticker_cache[key] = (now, price)
            return price
        except Exception:
            logger.debug("public ticker fetch failed for %s:%s", exchange_name, pair, exc_info=True)
            return cached[1] if cached else None

    def _prefetch_tickers_ccxt_public(self, exchange_name: str, pairs: list[str]) -> None:
        """Warm the ticker cache for every pair in one batched round trip,
        instead of leaving `_current_price_ccxt_public` to do it one pair at a
        time. Without this, viewing a bot with several open trades in
        different pairs (see _account_dashboard) paid for that many serial
        network calls - each with its own exchange latency/rate-limit risk -
        before the dashboard had anything to show. A no-op for pairs already
        warm within TICKER_CACHE_TTL, and silently skipped if the exchange
        doesn't support batch tickers (falls back to the existing per-pair path)."""
        now = time.time()
        stale = [
            p for p in dict.fromkeys(pairs)  # de-dup, keep order
            if (c := self._ticker_cache.get(f"{exchange_name}:{p}")) is None
            or now - c[0] >= self.TICKER_CACHE_TTL
        ]
        if not stale:
            return
        try:
            client = self._get_ccxt_public_client(exchange_name)
            if client is None or not client.has.get("fetchTickers"):
                return
            tickers = self._run_with_deadline(client.fetch_tickers, stale)
            for pair, ticker in tickers.items():
                price = ticker.get("last") or ticker.get("close") or ticker.get("bid")
                if price is not None:
                    self._ticker_cache[f"{exchange_name}:{pair}"] = (now, float(price))
        except Exception:
            logger.debug("batch ticker prefetch failed for %s", exchange_name, exc_info=True)

    def _current_price(self, pair: str, exchange_name: str | None) -> float | None:
        """Best-effort current price for ``pair``: reuses the attached bot's
        own live exchange connection when this portal IS that bot's process
        (cheapest, no extra client - the bot already polls this exchange
        every cycle), otherwise falls back to a cached public ccxt ticker
        (see _current_price_ccxt_public) for pricing another bot's trades
        from its account-JSON snapshot."""
        if self._is_live_bot():
            try:
                exch = getattr(self.bot, "exchange", None)
                if exch is not None and pair in getattr(exch, "markets", {}):
                    ticker = exch.fetch_ticker(pair)
                    price = ticker.get("last") or ticker.get("close") or ticker.get("bid")
                    if price is not None:
                        return float(price)
            except Exception:
                logger.debug("live exchange ticker fetch failed for %s", pair, exc_info=True)
        if not exchange_name:
            return None
        return self._current_price_ccxt_public(exchange_name, pair)

    def _attach_live_pnl(self, trade: dict) -> dict:
        """Overwrite an OPEN trade dict's profit_abs/profit_ratio with a
        live mark-to-market computed from a fresh ticker, and attach
        current_rate - both the attached-bot and cross-account dashboard
        paths otherwise only ever serve the trade's LAST-PERSISTED profit
        fields, which for an open trade means whatever was recorded at
        entry (typically 0), never updated again until the trade closes.
        Mirrors Trade.calculate_profit's own formula exactly (see
        persistence/trade_model.py) so the number matches what the trade
        will actually realize if closed at this price. No-op (returns
        ``trade`` unchanged) for closed trades or if a price can't be had."""
        if not trade.get("is_open"):
            return trade
        try:
            amount = float(trade.get("amount") or 0)
            open_rate = float(trade.get("open_rate") or 0)
            if amount <= 0 or open_rate <= 0:
                return trade
            price = self._current_price(trade.get("pair", ""), trade.get("exchange"))
            if price is None:
                return trade

            leverage = float(trade.get("leverage") or 1.0)
            is_short = bool(trade.get("is_short"))
            fee_close = float(trade.get("fee_close") or 0.0)
            trading_mode = (trade.get("trading_mode") or "spot").lower()
            funding_fees = float(trade.get("funding_fees") or 0.0)

            open_trade_value = trade.get("open_trade_value")
            if open_trade_value is None:
                fee_open = float(trade.get("fee_open") or 0.0)
                open_value = amount * open_rate
                open_fees = open_value * fee_open
                open_trade_value = open_value + open_fees if is_short else open_value - open_fees
            else:
                open_trade_value = float(open_trade_value)
            if open_trade_value == 0:
                return trade

            close_value = amount * price
            close_fees = close_value * fee_close
            close_trade_value = close_value + close_fees if is_short else close_value - close_fees
            if trading_mode == "futures":
                close_trade_value = close_trade_value - funding_fees if is_short else close_trade_value + funding_fees

            profit_abs = (open_trade_value - close_trade_value) if is_short else (close_trade_value - open_trade_value)
            profit_ratio = (
                (1 - (close_trade_value / open_trade_value)) if is_short
                else ((close_trade_value / open_trade_value) - 1)
            ) * leverage

            trade["current_rate"] = price
            trade["profit_abs"] = round(profit_abs, 8)
            trade["profit_ratio"] = round(profit_ratio, 8)
        except Exception:
            logger.debug("live PnL calc failed for %s", trade.get("pair"), exc_info=True)
        return trade

    def _list_configs(self, include_non_bots: bool = False) -> list[dict]:
        """Every config under user_data/configs with ``is_bot`` not explicitly
        false (see config_schema.py), each treated as one bot definition (see
        "strategy"/"auto_start" fields) - unless ``include_non_bots`` is set,
        in which case every config is returned regardless (the dashboard's
        "Show non-bots" toggle). ``account_name`` / ``running`` let the UI
        show whether this config's bot is currently live, by matching
        against the same account scan ``/api/livebots`` uses - the account
        name a config's bot writes to is always
        ``<config-name>_<dry_run|live>`` (see ``_start_config_bot``)."""
        d = self._configs_dir()
        if not d.is_dir():
            return []
        live = {b["name"]: b for b in self._scan_live_bots()}
        trade_procs = self._scan_trade_processes()
        out = []
        for p in sorted(d.glob("*.json")):
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                raw = {}
            if not isinstance(raw, dict):
                raw = {}
            if not include_non_bots and raw.get("is_bot", True) is False:
                continue
            dry_run = bool(raw.get("dry_run", True))
            account_name = f"{p.stem}_{'dry_run' if dry_run else 'live'}"
            bot_info = live.get(account_name)
            running = bool(bot_info and bot_info.get("running"))
            try:
                size = p.stat().st_size
            except OSError:
                size = 0
            proc_stats = self._proc_stats(trade_procs.get(p.stem) if running else None)
            out.append({
                "name": p.stem,
                "filename": p.name,
                "size": size,
                "strategy": raw.get("strategy") or None,
                "is_bot": bool(raw.get("is_bot", True)),
                "auto_start": bool(raw.get("auto_start", False)),
                "dry_run": dry_run,
                "bot_name": raw.get("bot_name") or None,
                "exchange": (raw.get("exchange") or {}).get("name"),
                "account_name": account_name,
                "running": running,
                "crashed": bool(bot_info and bot_info.get("crashed")),
                "cpu_percent": proc_stats["cpu_percent"],
                "memory_mb": proc_stats["memory_mb"],
            })
        return out

    def _config_path(self, name: str) -> Path:
        name = self._validate_name(name, "config")
        return self._configs_dir() / f"{name}.json"

    def _read_config_raw(self, name: str) -> str:
        p = self._config_path(name)
        if not p.is_file():
            raise HTTPException(status_code=404, detail=f"Config {name!r} not found")
        try:
            return p.read_text(encoding="utf-8")
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not read config: {exc}") from exc

    def _write_config_raw(self, name: str, content: str) -> dict:
        try:
            json.loads(content)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}") from exc
        p = self._config_path(name)
        if not p.is_file():
            raise HTTPException(status_code=404, detail=f"Config {name!r} not found")
        try:
            tmp = p.with_name(f".{p.name}.{uuid.uuid4().hex[:8]}.tmp")
            tmp.write_text(content, encoding="utf-8")
            os.replace(tmp, p)
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not write config: {exc}") from exc
        return {"saved": True, "name": name}

    def _duplicate_config(self, name: str, new_name: str) -> dict:
        src = self._config_path(name)
        if not src.is_file():
            raise HTTPException(status_code=404, detail=f"Config {name!r} not found")
        dst = self._config_path(new_name)
        if dst.exists():
            raise HTTPException(status_code=409, detail=f"Config {new_name!r} already exists")
        try:
            dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not duplicate config: {exc}") from exc
        return {"duplicated": True, "name": new_name}

    def _rename_config(self, name: str, new_name: str) -> dict:
        src = self._config_path(name)
        if not src.is_file():
            raise HTTPException(status_code=404, detail=f"Config {name!r} not found")
        dst = self._config_path(new_name)
        if dst.exists():
            raise HTTPException(status_code=409, detail=f"Config {new_name!r} already exists")
        try:
            src.rename(dst)
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not rename config: {exc}") from exc
        return {"renamed": True, "name": new_name}

    def _delete_config(self, name: str) -> dict:
        p = self._config_path(name)
        if not p.is_file():
            raise HTTPException(status_code=404, detail=f"Config {name!r} not found")
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raw = {}
        dry_run = bool(raw.get("dry_run", True)) if isinstance(raw, dict) else True
        account_name = f"{p.stem}_{'dry_run' if dry_run else 'live'}"
        info = next((b for b in self._scan_live_bots() if b["name"] == account_name), None)
        if info and info.get("running"):
            raise HTTPException(
                status_code=409,
                detail=f"Config {name!r}'s bot is still running - stop it first.",
            )
        try:
            p.unlink()
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not delete config: {exc}") from exc
        return {"deleted": True, "name": name}

    @staticmethod
    def _find_free_port(start: int = 8080, tries: int = 200) -> int:
        import socket

        port = start
        for _ in range(tries):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind(("127.0.0.1", port))
                    return port
                except OSError:
                    port += 1
        raise HTTPException(status_code=500, detail="No free port found")

    def _start_config_bot(self, name: str) -> dict:
        """Launch this config as its own detached trade subprocess - the
        manual, single-config equivalent of ``bot.py autostart``. Persistence
        account is always ``<config-name>_<dry_run|live>.json`` (keyed by the
        config's own filename, which is unique by construction - unlike
        ``strategy``, which two different configs could share)."""
        p = self._config_path(name)
        if not p.is_file():
            raise HTTPException(status_code=404, detail=f"Config {name!r} not found")
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=f"Config unreadable: {exc}") from exc
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="Config is not a JSON object")

        strategy = raw.get("strategy")
        if not strategy:
            raise HTTPException(
                status_code=400, detail=f"Config {name!r} has no 'strategy' set"
            )

        dry_run = bool(raw.get("dry_run", True))
        mode = "dry_run" if dry_run else "live"
        account_name = f"{name}_{mode}"
        existing = next((b for b in self._scan_live_bots() if b["name"] == account_name), None)
        if existing and existing.get("running"):
            raise HTTPException(status_code=409, detail=f"Config {name!r}'s bot is already running.")

        db_url = f"json:///user_data/accounts/{account_name}.json"
        bot_name = raw.get("bot_name") or strategy
        port = self._find_free_port(8080)

        if self.config.get("single_process_mode"):
            from VulcanTrader.bot import launch_bot_in_process
            spec = {
                "config_name": name, "strategy": strategy,
                "dry_run": dry_run, "bot_name": bot_name,
            }
            try:
                # See bot.py's _launch_autostart_configs for why fleet bots
                # skip their own WebPortal - this dashboard controls them
                # entirely through account-file scanning and stop-files.
                launch_bot_in_process(spec, self._user_data_dir(), port, no_web=True)
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Could not launch bot: {exc}") from exc
            logger.info(
                "Launched config %s in-process (strategy=%s, port=%d, db=%s)",
                name, strategy, port, db_url,
            )
            return {
                "started": True, "config": name, "strategy": strategy,
                "port": port, "db_url": db_url, "account_name": account_name,
            }

        cmd = [
            sys.executable, "-m", "VulcanTrader.bot", "trade",
            "-v",
            "-c", name,
            "--strategy", strategy,
            "--db-url", db_url,
            "--port", str(port),
            "--bot-name", bot_name,
        ]
        if dry_run:
            cmd.append("--dry-run")

        from VulcanTrader.util.proc_launch import launch_detached
        try:
            launch_detached(cmd, raw)
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not launch bot: {exc}") from exc
        logger.info("Launched config %s (strategy=%s, port=%d, db=%s)", name, strategy, port, db_url)
        return {
            "started": True, "config": name, "strategy": strategy,
            "port": port, "db_url": db_url, "account_name": account_name,
        }

    def _stop_config_bot(self, name: str) -> dict:
        """Stop the trader_bot process owned by this config, via the same
        stop-file mechanism as ``_request_account_stop``."""
        p = self._config_path(name)
        try:
            raw = json.loads(p.read_text(encoding="utf-8")) if p.is_file() else {}
        except (OSError, ValueError):
            raw = {}
        dry_run = bool(raw.get("dry_run", True)) if isinstance(raw, dict) else True
        account_name = f"{name}_{'dry_run' if dry_run else 'live'}"
        return self._request_account_stop(account_name)

    def _list_strategies(self) -> list[dict]:
        d = self._strategies_dir()
        if not d.is_dir():
            return []
        out: list[dict] = []
        cls_re = re.compile(r"^class\s+(\w+)\s*\([^)]*IStrategy[^)]*\)\s*:", re.MULTILINE)
        # Recursive so strategies in sub-folders (e.g. strategies/examples/) show up.
        for p in sorted(d.rglob("*.py")):
            rel = p.relative_to(d)
            if p.name.startswith("_") or any(part.startswith((".", "__")) for part in rel.parts[:-1]):
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for m in cls_re.finditer(text):
                out.append({"name": m.group(1), "file": rel.as_posix()})
        # de-dup preserving order
        seen: set[str] = set()
        unique = []
        for s in out:
            if s["name"] in seen:
                continue
            seen.add(s["name"])
            unique.append(s)
        return unique

    def _validate_name(self, value: str, kind: str) -> str:
        if not value or not self._NAME_RE.match(value):
            raise HTTPException(status_code=400, detail=f"Invalid {kind} name: {value!r}")
        return value

    def _validate_timerange(self, value: str | None) -> str | None:
        if value is None or value == "":
            return None
        if not self._TIMERANGE_RE.match(value):
            raise HTTPException(status_code=400, detail=f"Invalid timerange: {value!r} (expected YYYYMMDD-YYYYMMDD)")
        return value

    def _running_job_count(self) -> int:
        with self._jobs_lock:
            return sum(1 for j in self._jobs.values() if j["status"] == "running")

    def _list_jobs(self) -> list[dict]:
        with self._jobs_lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda j: j.get("created_at", ""),
                reverse=True,
            )
            return [self._job_summary(j) for j in jobs]

    def _job_summary(self, job: dict) -> dict:
        out = {
            "id": job["id"],
            "kind": job.get("kind", "backtest"),
            "config": job["config"],
            "strategy": job.get("strategy"),
            "timerange": job.get("timerange"),
            "engine": job.get("engine", "python"),
            "status": job["status"],
            "exit_code": job.get("exit_code"),
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "finished_at": job.get("finished_at"),
            "log_lines": len(job["log"]),
            "result_file": job.get("result_file"),
            "error": job.get("error"),
        }
        if job.get("kind") == "pairs_finder":
            out.update({
                "pf_metric": job.get("pf_metric"),
                "pf_top_n": job.get("pf_top_n"),
                "pf_workers": job.get("pf_workers"),
                "pf_pairs_total": job.get("pf_pairs_total", 0),
                "pf_pairs_done": job.get("pf_pairs_done", 0),
                "pf_pairs_failed": job.get("pf_pairs_failed", 0),
            })
        if job.get("type") == "hyperopt":
            out.update({
                "ho_epoch": job.get("ho_epoch"),
                "ho_total": job.get("ho_total"),
                "ho_best_loss": job.get("ho_best_loss"),
            })
        if job.get("kind") == "download":
            out.update({
                "exchange": job.get("exchange"),
                "pairs": job.get("pairs"),
                "timeframes": job.get("timeframes"),
                "days": job.get("days"),
                "erase": job.get("erase"),
                "dl_pairs_total": job.get("dl_pairs_total", 0),
                "dl_units_total": job.get("dl_units_total", 0),
                "dl_units_done": job.get("dl_units_done", 0),
            })
        return out

    def _get_job(self, job_id: str, log_offset: int = 0) -> dict:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            log = list(job["log"])
            pf_results = list(job.get("pf_results", [])) if job.get("kind") == "pairs_finder" else None
        log_offset = max(0, int(log_offset))
        out = {
            **self._job_summary(job),
            "log_offset": log_offset,
            "log": log[log_offset:],
            "log_total": len(log),
        }
        if pf_results is not None:
            out["pf_results"] = pf_results
        return out

    def _cancel_job(self, job_id: str) -> dict:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            proc: subprocess.Popen | None = job.get("_proc")
            kind = job.get("kind", "backtest")
        if job["status"] != "running":
            return {"ok": False, "detail": f"Job is {job['status']}"}
        if kind == "pairs_finder":
            with self._jobs_lock:
                job["_cancel"] = True
            return {"ok": True}
        if proc is None:
            return {"ok": False, "detail": "no process"}
        try:
            proc.terminate()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"terminate failed: {exc}") from exc
        return {"ok": True}

    def _submit_backtest_job(
        self,
        *,
        config: str,
        strategy: str | None,
        timerange: str | None,
        engine: str | None = "python",
    ) -> dict:
        cfg_name = self._validate_name(config, "config")
        strat_name = self._validate_name(strategy, "strategy") if strategy else None
        timerange = self._validate_timerange(timerange)
        engine = "rust" if str(engine).lower() == "rust" else "python"

        cfg_path = self._configs_dir() / f"{cfg_name}.json"
        if not cfg_path.is_file():
            raise HTTPException(status_code=404, detail=f"Config not found: {cfg_name}.json")

        if self._running_job_count() >= self.MAX_CONCURRENT_JOBS:
            raise HTTPException(
                status_code=429,
                detail=f"Max concurrent backtests reached ({self.MAX_CONCURRENT_JOBS}). Wait for a job to finish.",
            )

        job_id = uuid.uuid4().hex[:12]
        job: dict = {
            "id": job_id,
            "config": cfg_name,
            "strategy": strat_name,
            "timerange": timerange,
            "engine": engine,
            "status": "queued",
            "exit_code": None,
            "created_at": datetime.now(UTC).isoformat(),
            "started_at": None,
            "finished_at": None,
            "log": deque(maxlen=self.MAX_JOB_LOG_LINES),
            "result_file": None,
            "error": None,
            "_proc": None,
        }
        with self._jobs_lock:
            self._jobs[job_id] = job
            self._evict_old_jobs_locked()

        thread = threading.Thread(
            target=self._run_job_thread,
            args=(job_id,),
            name=f"backtest-job-{job_id}",
            daemon=True,
        )
        thread.start()
        return {"ok": True, "job": self._job_summary(job)}

    def _evict_old_jobs_locked(self) -> None:
        # Caller must hold self._jobs_lock.
        if len(self._jobs) <= self.MAX_JOB_HISTORY:
            return
        finished = sorted(
            (j for j in self._jobs.values() if j["status"] != "running"),
            key=lambda j: j.get("finished_at") or j.get("created_at") or "",
        )
        # Drop oldest finished until we're under the cap.
        excess = len(self._jobs) - self.MAX_JOB_HISTORY
        for old in finished[:excess]:
            self._jobs.pop(old["id"], None)

    def _run_job_thread(self, job_id: str) -> None:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job["status"] = "running"
            job["started_at"] = datetime.now(UTC).isoformat()

        cmd = [
            sys.executable,
            "-m",
            "VulcanTrader.bot",
            "backtest",
            "-c",
            job["config"],
            "--user-data-dir",
            str(self._user_data_dir()),
        ]
        if job.get("strategy"):
            cmd += ["-s", job["strategy"]]
        if job.get("timerange"):
            cmd += ["--timerange", job["timerange"]]
        if job.get("engine") == "rust":
            cmd += ["--engine", "rust"]

        job["log"].append(f"$ {' '.join(cmd)}")
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as exc:
            with self._jobs_lock:
                job["status"] = "error"
                job["error"] = str(exc)
                job["finished_at"] = datetime.now(UTC).isoformat()
            logger.exception("Failed to launch backtest job %s", job_id)
            return

        with self._jobs_lock:
            job["_proc"] = proc

        # Snapshot existing result files so we can detect the new one on success.
        bt_dir = self._backtest_dir()
        existing_files: set[str] = set()
        if bt_dir.is_dir():
            existing_files = {p.name for p in bt_dir.glob("*.json")}

        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                line = line.rstrip("\r\n")
                with self._jobs_lock:
                    job["log"].append(line)
        except Exception as exc:
            logger.exception("backtest job %s stream error", job_id)
            with self._jobs_lock:
                job["log"].append(f"[stream error] {exc}")

        rc = proc.wait()

        result_file: str | None = None
        if bt_dir.is_dir():
            new_files = sorted(
                (p for p in bt_dir.glob("*.json") if p.name not in existing_files and not p.name.endswith(".meta.json")),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if new_files:
                result_file = new_files[0].stem

        with self._jobs_lock:
            job["exit_code"] = rc
            job["status"] = "succeeded" if rc == 0 else ("cancelled" if rc < 0 else "failed")
            job["finished_at"] = datetime.now(UTC).isoformat()
            job["result_file"] = result_file
            job["_proc"] = None

    # ------------------------------------------------------------------
    # Hyperopt
    # ------------------------------------------------------------------

    _HYPEROPT_LOSS_CHOICES: frozenset[str] = frozenset({
        "SharpeHyperOptLoss", "SharpeHyperOptLossDaily",
        "SortinoHyperOptLoss", "SortinoHyperOptLossDaily",
        "CalmarHyperOptLoss",
        "MaxDrawDownHyperOptLoss", "MaxDrawDownRelativeHyperOptLoss",
        "MaxDrawDownPerPairHyperOptLoss",
        "ProfitDrawDownHyperOptLoss", "OnlyProfitHyperOptLoss",
        "ShortTradeDurHyperOptLoss", "MultiMetricHyperOptLoss",
    })
    _HYPEROPT_SPACE_CHOICES: frozenset[str] = frozenset({
        "buy", "sell", "roi", "stoploss", "trailing", "protection", "all",
    })

    def _submit_hyperopt_job(self, req: "HyperoptRunRequest") -> dict:
        cfg_name = self._validate_name(req.config, "config")
        strat_name = self._validate_name(req.strategy, "strategy") if req.strategy else None
        timerange = self._validate_timerange(req.timerange) if req.timerange else None

        cfg_path = self._configs_dir() / f"{cfg_name}.json"
        if not cfg_path.is_file():
            raise HTTPException(status_code=404, detail=f"Config not found: {cfg_name}.json")

        loss = req.hyperopt_loss or "SharpeHyperOptLossDaily"
        if loss not in self._HYPEROPT_LOSS_CHOICES:
            raise HTTPException(status_code=400, detail=f"Unknown loss function: {loss!r}")

        spaces = [s for s in (req.spaces or ["sell"]) if s in self._HYPEROPT_SPACE_CHOICES]
        if not spaces:
            raise HTTPException(status_code=400, detail="No valid spaces selected.")

        if self._running_job_count() >= self.MAX_CONCURRENT_JOBS:
            raise HTTPException(
                status_code=429,
                detail=f"Max concurrent jobs reached ({self.MAX_CONCURRENT_JOBS}).",
            )

        job_id = uuid.uuid4().hex[:12]
        job: dict = {
            "id": job_id,
            "type": "hyperopt",
            "config": cfg_name,
            "strategy": strat_name,
            "timerange": timerange,
            "epochs": max(1, int(req.epochs or 100)),
            "hyperopt_loss": loss,
            "spaces": spaces,
            "jobs": int(req.jobs) if req.jobs is not None else -1,
            "min_trades": max(1, int(req.min_trades or 1)),
            "analyze_per_epoch": bool(req.analyze_per_epoch),
            "print_all": bool(req.print_all),
            "no_color": bool(req.no_color),
            "status": "queued",
            "exit_code": None,
            "created_at": datetime.now(UTC).isoformat(),
            "started_at": None,
            "finished_at": None,
            "log": deque(maxlen=self.MAX_JOB_LOG_LINES),
            "result_file": None,
            "error": None,
            "_proc": None,
            # Live progress fields parsed from output
            "ho_epoch": None,
            "ho_total": None,
            "ho_best_loss": None,
        }
        with self._jobs_lock:
            self._jobs[job_id] = job
            self._evict_old_jobs_locked()

        thread = threading.Thread(
            target=self._run_hyperopt_job_thread,
            args=(job_id,),
            name=f"hyperopt-job-{job_id}",
            daemon=True,
        )
        thread.start()
        return {"ok": True, "job": self._job_summary(job)}

    def _run_hyperopt_job_thread(self, job_id: str) -> None:
        import re as _re

        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job["status"] = "running"
            job["started_at"] = datetime.now(UTC).isoformat()

        cmd = [
            sys.executable, "-m", "VulcanTrader.bot", "hyperopt",
            "-c", job["config"],
            "--user-data-dir", str(self._user_data_dir()),
            "--epochs", str(job["epochs"]),
            "--hyperopt-loss", job["hyperopt_loss"],
            "--spaces", *job["spaces"],
            "-j", str(job["jobs"]),
            "--min-trades", str(job["min_trades"]),
            "--no-color",  # always strip ANSI — log is plain text
        ]
        if job.get("strategy"):
            cmd += ["-s", job["strategy"]]
        if job.get("timerange"):
            cmd += ["--timerange", job["timerange"]]
        if job.get("analyze_per_epoch"):
            cmd += ["--analyze-per-epoch"]
        if job.get("print_all"):
            cmd += ["--print-all"]

        job["log"].append(f"$ {' '.join(cmd)}")
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as exc:
            with self._jobs_lock:
                job["status"] = "error"
                job["error"] = str(exc)
                job["finished_at"] = datetime.now(UTC).isoformat()
            logger.exception("Failed to launch hyperopt job %s", job_id)
            return

        with self._jobs_lock:
            job["_proc"] = proc

        # Pattern: "Epoch 42/100" or progress bars containing "42/100"
        _epoch_re = _re.compile(r"Epoch\s+(\d+)\s*/\s*(\d+)|(\d+)\s*/\s*(\d+)\s*\[")
        # Pattern for best loss: "Best loss: -0.12345"
        _loss_re = _re.compile(r"[Bb]est\s+loss[:\s]+(-?\d+\.?\d*(?:[eE][+-]?\d+)?)")

        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                line = line.rstrip("\r\n")
                # Strip ANSI escape codes
                line_clean = _re.sub(r"\x1b\[[0-9;]*[mABCDEFGHJKSTfnsu]", "", line)
                with self._jobs_lock:
                    job["log"].append(line_clean)
                    m = _epoch_re.search(line_clean)
                    if m:
                        if m.group(1):
                            job["ho_epoch"] = int(m.group(1))
                            job["ho_total"] = int(m.group(2))
                        elif m.group(3):
                            job["ho_epoch"] = int(m.group(3))
                            job["ho_total"] = int(m.group(4))
                    lm = _loss_re.search(line_clean)
                    if lm:
                        try:
                            job["ho_best_loss"] = float(lm.group(1))
                        except ValueError:
                            pass
        except Exception as exc:
            logger.exception("hyperopt job %s stream error", job_id)
            with self._jobs_lock:
                job["log"].append(f"[stream error] {exc}")

        rc = proc.wait()
        with self._jobs_lock:
            job["exit_code"] = rc
            job["status"] = "succeeded" if rc == 0 else ("cancelled" if rc < 0 else "failed")
            job["finished_at"] = datetime.now(UTC).isoformat()
            job["_proc"] = None

    # ------------------------------------------------------------------
    # Pairs Finder (per-pair backtest sweep with ranking)
    # ------------------------------------------------------------------
    _PAIR_RE = re.compile(r"^[A-Za-z0-9/:._\-]+$")
    _METRIC_CHOICES = {
        "composite", "roi", "sharpe", "sortino", "calmar",
        "expectancy", "profit_factor", "win_rate", "lowdd",
    }

    @staticmethod
    def _pf_parse_json(path: Path, pair: str) -> dict | None:
        """Read backtest result JSON written by Backtesting and extract per-pair metrics."""
        try:
            with path.open("r", encoding="utf-8") as fh:
                bt = json.load(fh)
        except Exception:
            return None
        strategies = (bt.get("strategy") or {}) if isinstance(bt, dict) else {}
        if not strategies:
            return None
        # Take the first (and usually only) strategy entry.
        st = next(iter(strategies.values()))
        trades = int(st.get("total_trades", len(st.get("trades", [])) or 0) or 0)
        if trades == 0:
            return None
        result: dict = {
            "pair": pair,
            "trades": trades,
            "total_profit_pct": float(st.get("profit_total_pct", st.get("profit_total", 0) * 100) or 0),
            "avg_profit_pct": float(st.get("profit_mean_pct", (st.get("profit_mean", 0) or 0) * 100) or 0),
            "win_rate": float((st.get("winrate") or 0) * 100),
            "sharpe": float(st.get("sharpe", 0) or 0),
            "sortino": float(st.get("sortino", 0) or 0),
            "calmar": float(st.get("calmar", 0) or 0),
            "max_drawdown": float((st.get("max_drawdown_account") or 0) * 100),
            "profit_factor": float(st.get("profit_factor", 0) or 0),
            "expectancy": float(st.get("expectancy", 0) or 0),
        }
        return result

    @staticmethod
    def _pf_parse_output(output: str, pair: str) -> dict | None:
        """Parse a backtest stdout/stderr blob into per-pair metrics (legacy fallback)."""
        result: dict = {
            "pair": pair, "trades": 0, "total_profit_pct": 0.0,
            "avg_profit_pct": 0.0, "win_rate": 0.0, "sharpe": 0.0,
            "sortino": 0.0, "calmar": 0.0, "max_drawdown": 0.0,
            "profit_factor": 0.0, "expectancy": 0.0,
        }
        sm = re.search(
            r"STRATEGY SUMMARY.*?\|\s*(\w+)\s*\|\s*(\d+)\s*\|\s*([-\d.]+)\s*\|\s*([-\d.,]+)\s*\|\s*([-\d.]+)\s*\|"
            r"\s*[\d:]+\s*\|\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s*\|",
            output, re.DOTALL,
        )
        if sm:
            result["trades"] = int(sm.group(2))
            result["avg_profit_pct"] = float(sm.group(3))
            result["total_profit_pct"] = float(sm.group(5))
            result["win_rate"] = float(sm.group(9))
        if result["trades"] == 0:
            tm = re.search(
                r"\|\s*TOTAL\s*\|\s*(\d+)\s*\|\s*([-\d.]+)\s*\|\s*([-\d.,]+)\s*\|\s*([-\d.]+)\s*\|"
                r"\s*[\d:]+\s*\|\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s*\|",
                output,
            )
            if tm:
                result["trades"] = int(tm.group(1))
                result["avg_profit_pct"] = float(tm.group(2))
                result["total_profit_pct"] = float(tm.group(4))
                result["win_rate"] = float(tm.group(8))
        if result["trades"] == 0:
            tr = re.search(r"Total/Daily Avg Trades\s+\|\s*(\d+)", output)
            if tr:
                result["trades"] = int(tr.group(1))
            pm = re.search(r"Total profit %\s+\|\s*([-\d.]+)%?", output)
            if pm:
                result["total_profit_pct"] = float(pm.group(1))
        for key, pat in (
            ("sharpe", r"Sharpe\s+\|\s*([-\d.]+)"),
            ("sortino", r"Sortino\s+\|\s*([-\d.]+)"),
            ("calmar", r"Calmar\s+\|\s*([-\d.]+)"),
            ("profit_factor", r"Profit factor\s+\|\s*([\d.]+)"),
        ):
            m = re.search(pat, output)
            if m:
                try:
                    result[key] = float(m.group(1))
                except ValueError:
                    pass
        dd = re.search(r"Absolute drawdown.*?\(([\d.]+)%\)", output)
        if dd:
            result["max_drawdown"] = float(dd.group(1))
        else:
            dd2 = re.search(r"Max % of account underwater\s+\|\s*([\d.]+)%?", output)
            if dd2:
                result["max_drawdown"] = float(dd2.group(1))
        em = re.search(r"Expectancy.*?\|\s*([-\d.]+)", output)
        if em:
            try:
                result["expectancy"] = float(em.group(1))
            except ValueError:
                pass
        if "No data found" in output or "No trades made" in output:
            return None
        if result["trades"] > 0 or result["total_profit_pct"] != 0 or result["sharpe"] != 0:
            return result
        return None

    @staticmethod
    def _pf_composite(result: dict) -> float:
        roi = result.get("total_profit_pct", 0)
        sharpe = result.get("sharpe", 0)
        trades = result.get("trades", 0)
        win_rate = result.get("win_rate", 0)
        max_dd = abs(result.get("max_drawdown", 0))
        roi_score = max(0.0, min(100.0, (roi + 50) / 150 * 100))
        sharpe_score = max(0.0, min(100.0, (sharpe + 2) / 7 * 100))
        dd_score = max(0.0, min(100.0, (50 - max_dd) / 50 * 100))
        base = roi_score * 0.35 + sharpe_score * 0.35 + dd_score * 0.30
        penalty = 1.0
        if trades < 3:
            penalty *= 0.3
        elif trades < 5:
            penalty *= 0.6
        elif trades < 10:
            penalty *= 0.8
        if win_rate < 30 and trades > 5:
            penalty *= 0.7
        return base * penalty

    @staticmethod
    def _pf_metric_value(result: dict, metric: str) -> float:
        m = metric.lower()
        if m == "lowdd":
            return -abs(result.get("max_drawdown", 100.0))
        return result.get({
            "roi": "total_profit_pct",
            "sharpe": "sharpe",
            "sortino": "sortino",
            "calmar": "calmar",
            "expectancy": "expectancy",
            "profit_factor": "profit_factor",
            "win_rate": "win_rate",
            "composite": "composite_score",
        }.get(m, "composite_score"), 0.0)

    def _submit_pairs_finder_job(self, req: "PairsFinderRequest") -> dict:
        cfg_name = self._validate_name(req.config, "config")
        strat_name = self._validate_name(req.strategy, "strategy") if req.strategy else None
        timerange = self._validate_timerange(req.timerange) if req.timerange else None
        if req.metric not in self._METRIC_CHOICES:
            raise HTTPException(status_code=400, detail=f"Invalid metric: {req.metric}")
        try:
            top_n = max(1, min(500, int(req.top_n)))
            workers = max(1, min(16, int(req.workers)))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=f"Invalid numeric param: {exc}") from exc

        cfg_path = self._configs_dir() / f"{cfg_name}.json"
        if not cfg_path.is_file():
            raise HTTPException(status_code=404, detail=f"Config not found: {cfg_name}.json")

        # Resolve pair list
        pairs: list[str]
        if req.pairs:
            pairs = list(req.pairs)
        else:
            try:
                with cfg_path.open("r", encoding="utf-8") as fh:
                    cfg_json = json.load(fh)
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Failed to read config: {exc}") from exc
            pairs = list(((cfg_json.get("exchange") or {}).get("pair_whitelist")) or [])
        # Validate + dedupe preserving order
        seen: set[str] = set()
        clean: list[str] = []
        for p in pairs:
            if not isinstance(p, str) or not self._PAIR_RE.match(p):
                raise HTTPException(status_code=400, detail=f"Invalid pair: {p!r}")
            if p in seen:
                continue
            seen.add(p)
            clean.append(p)
        if not clean:
            raise HTTPException(status_code=400, detail="No pairs to evaluate")

        if self._running_job_count() >= self.MAX_CONCURRENT_JOBS:
            raise HTTPException(
                status_code=429,
                detail=f"Max concurrent jobs reached ({self.MAX_CONCURRENT_JOBS}).",
            )

        job_id = uuid.uuid4().hex[:12]
        job: dict = {
            "id": job_id,
            "kind": "pairs_finder",
            "config": cfg_name,
            "strategy": strat_name,
            "timerange": timerange,
            "status": "queued",
            "exit_code": None,
            "created_at": datetime.now(UTC).isoformat(),
            "started_at": None,
            "finished_at": None,
            "log": deque(maxlen=self.MAX_JOB_LOG_LINES),
            "result_file": None,
            "error": None,
            "_proc": None,
            "_cancel": False,
            # pairs-finder specific
            "pf_metric": req.metric,
            "pf_top_n": top_n,
            "pf_workers": workers,
            "pf_pairs_total": len(clean),
            "pf_pairs_done": 0,
            "pf_pairs_failed": 0,
            "pf_pairs": clean,
            "pf_results": [],
        }
        with self._jobs_lock:
            self._jobs[job_id] = job
            self._evict_old_jobs_locked()

        thread = threading.Thread(
            target=self._run_pairs_finder_thread,
            args=(job_id,),
            name=f"pairs-finder-{job_id}",
            daemon=True,
        )
        thread.start()
        return {"ok": True, "job": self._job_summary(job)}

    def _run_pairs_finder_thread(self, job_id: str) -> None:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job["status"] = "running"
            job["started_at"] = datetime.now(UTC).isoformat()
            pairs: list[str] = list(job["pf_pairs"])
            workers: int = int(job["pf_workers"])
            cfg = job["config"]
            strat = job.get("strategy")
            tr = job.get("timerange")
            log = job["log"]
            log.append(f"[pairs-finder] {len(pairs)} pairs, workers={workers}, metric={job['pf_metric']}")

        # Per-job temp dir for isolated per-pair result JSONs.
        pf_tmpdir = self._backtest_dir() / f".pairs_finder_{job_id}"
        try:
            pf_tmpdir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pf_tmpdir = None

        def run_one(pair: str) -> dict | None:
            # Each pair writes to its own JSON to keep parallel runs isolated.
            safe = re.sub(r"[^A-Za-z0-9]+", "_", pair).strip("_") or "pair"
            export_path = None
            if pf_tmpdir is not None:
                export_path = pf_tmpdir / f"{safe}.json"
            cmd = [
                sys.executable, "-m", "VulcanTrader.bot", "backtest",
                "-c", cfg,
                "--user-data-dir", str(self._user_data_dir()),
                "--pairs", pair,
            ]
            if export_path is not None:
                cmd += ["--exportfilename", str(export_path)]
            if strat:
                cmd += ["-s", strat]
            if tr:
                cmd += ["--timerange", tr]
            try:
                r = subprocess.run(
                    cmd, cwd=str(REPO_ROOT),
                    capture_output=True, text=True,
                    encoding="utf-8", errors="replace",
                    timeout=600,
                )
                output = (r.stdout or "") + (r.stderr or "")
                parsed = None
                if export_path is not None and export_path.is_file():
                    try:
                        parsed = self._pf_parse_json(export_path, pair)
                    except Exception as exc:
                        logger.debug("pf_parse_json failed for %s: %s", pair, exc)
                if parsed is None:
                    # Legacy stdout fallback (kept for compatibility)
                    parsed = self._pf_parse_output(output, pair)
                if parsed is None and r.returncode != 0:
                    _out = output or ""
                    _expected_no_data = (
                        "No data found" in _out
                        or "market not found" in _out
                        or "No pair in whitelist" in _out
                    )
                    if not _expected_no_data:
                        tail = "\n".join(_out.splitlines()[-8:])
                        with self._jobs_lock:
                            job["log"].append(f"[error] {pair} rc={r.returncode}: {tail[:500]}")
                if parsed is not None:
                    parsed["composite_score"] = self._pf_composite(parsed)
                return parsed
            except subprocess.TimeoutExpired:
                return None
            except Exception as exc:
                logger.exception("pairs-finder run failed for %s", pair)
                with self._jobs_lock:
                    job["log"].append(f"[error] {pair}: {exc}")
                return None

        try:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(run_one, p): p for p in pairs}
                for fut in as_completed(futures):
                    pair = futures[fut]
                    with self._jobs_lock:
                        if job.get("_cancel"):
                            for f in futures:
                                f.cancel()
                            break
                    res = fut.result()
                    with self._jobs_lock:
                        job["pf_pairs_done"] += 1
                        if res is None:
                            job["pf_pairs_failed"] += 1
                            job["log"].append(
                                f"[{job['pf_pairs_done']}/{job['pf_pairs_total']}] {pair}: NO DATA / NO TRADES"
                            )
                        else:
                            job["pf_results"].append(res)
                            job["log"].append(
                                f"[{job['pf_pairs_done']}/{job['pf_pairs_total']}] {pair}: "
                                f"ROI {res['total_profit_pct']:.2f}% | Sharpe {res['sharpe']:.2f} | "
                                f"Trades {res['trades']} | Win {res['win_rate']:.1f}% | "
                                f"Score {res['composite_score']:.1f}"
                            )
        except Exception as exc:
            logger.exception("pairs-finder %s crashed", job_id)
            with self._jobs_lock:
                job["error"] = str(exc)

        # Sort results by chosen metric (desc)
        with self._jobs_lock:
            metric = job["pf_metric"]
            job["pf_results"].sort(key=lambda r: self._pf_metric_value(r, metric), reverse=True)

            # Persist a summary JSON
            try:
                bt_dir = self._backtest_dir()
                bt_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
                out_name = f"pair_rankings_{strat or cfg}_{ts}"
                out_path = bt_dir / f"{out_name}.json"
                with out_path.open("w", encoding="utf-8") as fh:
                    json.dump({
                        "kind": "pairs_finder",
                        "config": cfg,
                        "strategy": strat,
                        "timerange": tr,
                        "metric": metric,
                        "pairs_total": job["pf_pairs_total"],
                        "pairs_done": job["pf_pairs_done"],
                        "pairs_failed": job["pf_pairs_failed"],
                        "results": job["pf_results"],
                        "created_at": datetime.now(UTC).isoformat(),
                    }, fh, indent=2)
                job["result_file"] = out_name
            except Exception as exc:
                job["log"].append(f"[warn] failed to save summary: {exc}")

            if job.get("_cancel"):
                job["status"] = "cancelled"
                job["exit_code"] = -1
            elif job.get("error"):
                job["status"] = "error"
                job["exit_code"] = 1
            else:
                job["status"] = "succeeded"
                job["exit_code"] = 0
            job["finished_at"] = datetime.now(UTC).isoformat()

        # Best-effort cleanup of per-job temp dir.
        try:
            if pf_tmpdir is not None and pf_tmpdir.is_dir():
                import shutil
                shutil.rmtree(pf_tmpdir, ignore_errors=True)
        except Exception:
            pass

    def _datadir(self) -> Path:
        if self.config.get("datadir"):
            return Path(self.config["datadir"])
        if self.config.get("user_data_dir"):
            return Path(self.config["user_data_dir"]) / "data"
        return REPO_ROOT / "user_data" / "data"

    @staticmethod
    def _bt_extract_trades_and_meta(bt: dict) -> tuple[list[dict], str | None, str | None]:
        """
        Returns (trades, timeframe, trading_mode) from a backtest result dict.
        Supports both the wrapped ``{strategy: {<name>: {...}}}`` shape and
        a flat ``{trades: [...], timeframe: ...}`` shape.
        """
        trades: list[dict] = []
        tf: str | None = None
        tm: str | None = None
        if isinstance(bt.get("strategy"), dict):
            names = list(bt["strategy"].keys())
            if names:
                strat = bt["strategy"][names[0]]
                trades = list(strat.get("trades") or [])
                tf = strat.get("timeframe") or tf
                tm = strat.get("trading_mode") or tm
        if not trades and isinstance(bt.get("trades"), list):
            trades = list(bt["trades"])
        if not tf:
            tf = bt.get("timeframe")
        if not tm:
            tm = bt.get("trading_mode")
        return trades, tf, tm

    # ------------------------------------------------------------------
    # Pair candles for chart
    # ------------------------------------------------------------------
    def _bt_pair_candles(self, name: str, pair: str) -> dict:
        """Return OHLCV candles for a single pair from the backtest data directory."""
        bt = self._load_backtest(name)
        _, tf, trading_mode = self._bt_extract_trades_and_meta(bt)
        if not tf:
            raise HTTPException(status_code=400, detail="Backtest result missing timeframe")

        try:
            from VulcanTrader.data.history import load_pair_history
            from VulcanTrader.enums import CandleType, TradingMode
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"history loader unavailable: {exc}")

        datadir = self._datadir()
        if not datadir.is_dir():
            raise HTTPException(status_code=400, detail=f"datadir not found: {datadir}")
        data_format = self.config.get("dataformat_ohlcv", "feather")
        try:
            tm_enum = TradingMode(trading_mode) if trading_mode else TradingMode.SPOT
        except Exception:
            tm_enum = TradingMode.SPOT
        candle_type = CandleType.get_default(tm_enum.value)

        try:
            df = load_pair_history(
                pair=pair,
                timeframe=tf,
                datadir=datadir,
                data_format=data_format,
                candle_type=candle_type,
            )
        except Exception as exc:
            raise HTTPException(status_code=404, detail=f"No data for {pair}: {exc}")

        if df is None or len(df) == 0:
            raise HTTPException(status_code=404, detail=f"No candles for pair '{pair}'")

        needed = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
        df = df[needed].copy()
        # Robust epoch-seconds conversion — handles any resolution (ns/us/ms/s) and tz-aware columns.
        # pandas 2.0+ reads feather as datetime64[ms, UTC] so astype("int64") gives ms, not ns.
        date_col = df["date"]
        if hasattr(date_col.dtype, "tz") and date_col.dtype.tz is not None:
            date_col = date_col.dt.tz_convert("UTC").dt.tz_localize(None)
        df["time"] = date_col.astype("datetime64[s]").astype("int64")
        cols = ["time", "open", "high", "low", "close"] + (["volume"] if "volume" in df.columns else [])
        import json as _json
        records = _json.loads(df[cols].to_json(orient="records"))

        return {"pair": pair, "timeframe": tf, "candles": records}

    @staticmethod
    def _sim_metrics(
        pnls: list[float],
        ratios: list[float],
        close_dates_iso: list[str],
        open_dates_iso: list[str],
        starting_capital: float,
    ) -> dict:
        """Compute aggregate performance metrics for a set of (possibly adjusted) trades."""
        import math as _math
        from datetime import datetime as _datetime

        n = len(pnls)
        if n == 0:
            null_keys = [
                "num_trades", "num_wins", "win_rate", "total_pnl", "roi_pct",
                "profit_factor", "sharpe", "max_drawdown_pct", "cagr_pct", "expectancy_r",
            ]
            return {k: None for k in null_keys}

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        total_pnl = sum(pnls)
        win_rate = len(wins) / n * 100
        gw = sum(wins)
        gl = abs(sum(losses))
        pf_val = gw / gl if gl > 0 else (None if gw <= 0 else None)  # None when unbounded

        # Expectancy R = mean profit ratio (same as freqtrade's expectancy per trade)
        expectancy_r = sum(ratios) / len(ratios) if ratios else 0.0

        # Duration from sorted dates for Sharpe annualisation and CAGR
        duration_years = 0.0
        trades_per_year = float(n)
        try:
            open_dts = [
                _datetime.fromisoformat(d.replace("Z", "+00:00"))
                for d in open_dates_iso if d
            ]
            close_dts = [
                _datetime.fromisoformat(d.replace("Z", "+00:00"))
                for d in close_dates_iso if d
            ]
            all_dts = open_dts + close_dts
            if all_dts:
                span_secs = (max(all_dts) - min(all_dts)).total_seconds()
                duration_years = span_secs / (365.25 * 86400)
                if duration_years > 1 / 365.25:
                    trades_per_year = n / duration_years
        except Exception:
            pass

        # Sharpe = mean(ratio) / std(ratio) * sqrt(trades_per_year)
        sharpe = 0.0
        if len(ratios) >= 2:
            mean_r = sum(ratios) / len(ratios)
            var_r = sum((r - mean_r) ** 2 for r in ratios) / (len(ratios) - 1)
            std_r = var_r ** 0.5
            if std_r > 1e-12:
                sharpe = mean_r / std_r * (trades_per_year ** 0.5)

        # Max drawdown from equity curve sorted by close date
        max_dd_pct = 0.0
        try:
            sorted_pnls = [
                pnl for _, pnl in sorted(
                    zip(close_dates_iso, pnls), key=lambda x: x[0] or ""
                )
            ]
            equity = starting_capital
            peak = starting_capital
            for pnl in sorted_pnls:
                equity += pnl
                if equity > peak:
                    peak = equity
                dd = (peak - equity) / peak if peak > 0 else 0.0
                if dd > max_dd_pct:
                    max_dd_pct = dd
        except Exception:
            pass

        # CAGR
        cagr_pct = 0.0
        roi_pct = total_pnl / starting_capital * 100 if starting_capital else 0.0
        if duration_years > 1 / 365.25 and starting_capital > 0:
            final = starting_capital + total_pnl
            if final > 0:
                try:
                    cagr_pct = ((final / starting_capital) ** (1.0 / duration_years) - 1.0) * 100
                except Exception:
                    cagr_pct = roi_pct

        return {
            "num_trades": n,
            "num_wins": len(wins),
            "win_rate": round(win_rate, 2),
            "total_pnl": round(total_pnl, 2),
            "roi_pct": round(roi_pct, 2),
            "profit_factor": round(pf_val, 3) if pf_val is not None else None,
            "sharpe": round(sharpe, 3),
            "max_drawdown_pct": round(max_dd_pct * 100, 2),
            "cagr_pct": round(cagr_pct, 2),
            "expectancy_r": round(expectancy_r, 4),
        }



    # ------------------------------------------------------------------
    # Regime analysis
    # ------------------------------------------------------------------

    def _bt_mae_mfe_analysis(
        self,
        name: str,
        regime_pair: str = "__all__",
        n_clusters: int = 4,
    ) -> dict:
        """
        MAE / MFE analysis with K-Means clustering and per-regime excursion statistics.

        Uses min_rate / max_rate stored in each trade record — no candle data
        needed for the core analysis. Regime classification is attempted from
        candle data (same logic as _backtest_regime_analysis) and is skipped
        gracefully if data are unavailable.
        """
        try:
            import numpy as np
        except ImportError as exc:
            raise HTTPException(status_code=500, detail=f"numpy not available: {exc}")

        bt = self._load_backtest(name)
        trades, tf, trading_mode = self._bt_extract_trades_and_meta(bt)

        if not trades:
            raise HTTPException(status_code=400, detail="No trades in backtest result")

        # --- Step 1: Compute MAE / MFE per trade -------------------------
        trade_rows: list[dict] = []
        for t in trades:
            open_rate = float(t.get("open_rate") or 0)
            if open_rate <= 0:
                continue
            min_rate = t.get("min_rate")
            max_rate = t.get("max_rate")
            if min_rate is None or max_rate is None:
                continue
            min_rate = float(min_rate)
            max_rate = float(max_rate)
            is_short = bool(t.get("is_short", False))
            leverage = float(t.get("leverage") or 1.0)
            profit_ratio = float(t.get("profit_ratio") or 0)

            if is_short:
                mfe = max(0.0, (open_rate - min_rate) / open_rate * 100.0 * leverage)
                mae = max(0.0, (max_rate - open_rate) / open_rate * 100.0 * leverage)
            else:
                mfe = max(0.0, (max_rate - open_rate) / open_rate * 100.0 * leverage)
                mae = max(0.0, (open_rate - min_rate) / open_rate * 100.0 * leverage)

            trade_rows.append({
                "pair":        t.get("pair", ""),
                "open_date":   str(t.get("open_date") or "")[:19],
                "enter_tag":   str(t.get("enter_tag") or ""),
                "exit_reason": str(t.get("exit_reason") or ""),
                "mae":         round(float(mae), 3),
                "mfe":         round(float(mfe), 3),
                "pnl_pct":     round(float(profit_ratio * 100.0), 3),
                "win":         profit_ratio > 0,
                "regime":      "ALL",
                "cluster":     -1,
                # Fields used by the forward re-simulation (Step 2.5).
                "open_rate":   open_rate,
                "leverage":    leverage,
                "is_short":    is_short,
                # Forward-path running excursions (filled in if candle data loads);
                # _adv/_fav are leverage-adjusted running-max % by bar from entry.
                "_adv":        None,
                "_fav":        None,
                "_mtm":        round(float(profit_ratio * 100.0), 3),
            })

        if not trade_rows:
            raise HTTPException(
                status_code=400,
                detail="No trades contain min_rate / max_rate. "
                       "Ensure the backtest was run with freqtrade >= 2023.",
            )

        # --- Step 2: Classify regimes (optional) -------------------------
        # Regime = market state at each trade's entry (from regime_analysis).
        chosen_regime_pair = "N/A"
        try:
            import pandas as pd
            from datetime import datetime as _dt
            from VulcanTrader.regime_analysis import BacktestRegimeAnalyzer
            from VulcanTrader.data.history import load_pair_history
            from VulcanTrader.enums import CandleType, TradingMode

            if tf and self._datadir().is_dir():
                datadir = self._datadir()
                data_format = self.config.get("dataformat_ohlcv", "feather")
                try:
                    tm_enum = TradingMode(trading_mode) if trading_mode else TradingMode.SPOT
                except Exception:
                    tm_enum = TradingMode.SPOT
                candle_type = CandleType.get_default(tm_enum.value)

                def _try_load_r(pair: str):
                    try:
                        df = load_pair_history(
                            pair=pair, timeframe=tf, datadir=datadir,
                            data_format=data_format, candle_type=candle_type,
                        )
                        return df if df is not None and len(df) > 50 else None
                    except Exception:
                        return None

                def _norm_regime(df):
                    if "date" in df.columns:
                        df = df.copy()
                        df["date"] = pd.to_datetime(df["date"])
                        if hasattr(df["date"].dtype, "tz") and df["date"].dtype.tz is not None:
                            df["date"] = df["date"].dt.tz_localize(None)
                    return BacktestRegimeAnalyzer.classify_regime(df)

                def _bisect_regime(dates, labels, dt_str: str, interval) -> str:
                    # Entry is filled at the OPEN of the `open_date` candle, so
                    # that candle has not closed yet - classify from the last
                    # one that had. See BacktestRegimeAnalyzer.label_at_entry.
                    try:
                        dt = _dt.fromisoformat(str(dt_str)[:19].replace(" ", "T"))
                    except ValueError:
                        return "RANGING"
                    return BacktestRegimeAnalyzer.label_at_entry(
                        dates, labels, dt, interval, default="RANGING"
                    )

                def _build_pc(df):
                    rdf = _norm_regime(df)
                    raw = pd.to_datetime(rdf["date"])
                    if hasattr(raw.dtype, "tz") and raw.dtype.tz is not None:
                        raw = raw.dt.tz_localize(None)
                    dates = raw.to_list()
                    return (dates, rdf["regime"].to_list(),
                            BacktestRegimeAnalyzer.resolve_interval(tf, dates))

                traded_pairs = list(dict.fromkeys(r["pair"] for r in trade_rows if r["pair"]))
                rp_lower = (regime_pair or "").lower()

                if rp_lower in ("", "__all__", "all"):
                    pair_cache: dict = {}
                    for pair in traded_pairs:
                        df = _try_load_r(pair)
                        if df is not None:
                            pair_cache[pair] = _build_pc(df)
                    if pair_cache:
                        for row in trade_rows:
                            pc = pair_cache.get(row["pair"])
                            if pc:
                                row["regime"] = _bisect_regime(pc[0], pc[1], row["open_date"], pc[2])
                        chosen_regime_pair = "ALL"
                else:
                    btc_cands = ["BTC/USDT", "BTC/USDT:USDT", "BTC/USD", "BTC/BUSD"]
                    cands = ([regime_pair] if regime_pair else []) + btc_cands + traded_pairs
                    for pair in cands:
                        df = _try_load_r(pair)
                        if df is None:
                            continue
                        r_dates, r_labels, r_interval = _build_pc(df)
                        for row in trade_rows:
                            row["regime"] = _bisect_regime(
                                r_dates, r_labels, row["open_date"], r_interval
                            )
                        chosen_regime_pair = pair
                        break
        except Exception as _re:
            logger.debug(f"MAE/MFE regime classification skipped: {_re}")

        # --- Step 2.5: Forward re-simulation path -----------------------
        # For each trade, walk the candles FORWARD from entry and record the
        # leverage-adjusted running-max adverse (_adv) and favourable (_fav)
        # excursions per bar. This lets the optimiser evaluate a candidate
        # SL/TP by FIRST-TOUCH (which barrier the price reaches first) instead
        # of freezing the excursions recorded under the original exit policy —
        # removing the look-ahead bias of the old expectancy model.
        SIM_MAX_BARS = 480          # cap forward window (~5 days on 15m)
        sim_ready = False
        try:
            import bisect as _bisect2
            import numpy as _np2
            import pandas as pd
            from VulcanTrader.data.history import load_pair_history
            from VulcanTrader.enums import CandleType, TradingMode

            if tf and self._datadir().is_dir():
                datadir = self._datadir()
                data_format = self.config.get("dataformat_ohlcv", "feather")
                try:
                    tm_enum = TradingMode(trading_mode) if trading_mode else TradingMode.SPOT
                except Exception:
                    tm_enum = TradingMode.SPOT
                candle_type = CandleType.get_default(tm_enum.value)

                def _load_ohlcv(pair: str):
                    try:
                        df = load_pair_history(
                            pair=pair, timeframe=tf, datadir=datadir,
                            data_format=data_format, candle_type=candle_type,
                        )
                        if df is None or len(df) == 0:
                            return None
                        dts = pd.to_datetime(df["date"])
                        if hasattr(dts.dtype, "tz") and dts.dtype.tz is not None:
                            dts = dts.dt.tz_localize(None)
                        return (
                            dts.to_list(),
                            df["high"].to_numpy(dtype=float),
                            df["low"].to_numpy(dtype=float),
                            df["close"].to_numpy(dtype=float),
                        )
                    except Exception:
                        return None

                ohlcv_cache: dict = {}
                for pair in dict.fromkeys(r["pair"] for r in trade_rows if r["pair"]):
                    oc = _load_ohlcv(pair)
                    if oc is not None:
                        ohlcv_cache[pair] = oc

                filled = 0
                for row in trade_rows:
                    oc = ohlcv_cache.get(row["pair"])
                    if not oc:
                        continue
                    dates, high, low, close = oc
                    try:
                        odt = _dt.fromisoformat(str(row["open_date"])[:19].replace(" ", "T"))
                    except ValueError:
                        continue
                    i0 = _bisect2.bisect_left(dates, odt)
                    if i0 >= len(close):
                        continue
                    i1 = min(i0 + SIM_MAX_BARS, len(close))
                    o = row["open_rate"]
                    lev = row["leverage"]
                    if o <= 0 or i1 <= i0:
                        continue
                    h = high[i0:i1]; l = low[i0:i1]; c = close[i1 - 1]
                    if row["is_short"]:
                        adv = (h - o) / o * 100.0 * lev      # adverse = price up
                        fav = (o - l) / o * 100.0 * lev      # favourable = price down
                        mtm = (o - c) / o * 100.0 * lev
                    else:
                        adv = (o - l) / o * 100.0 * lev      # adverse = price down
                        fav = (h - o) / o * 100.0 * lev      # favourable = price up
                        mtm = (c - o) / o * 100.0 * lev
                    # Running maxima → monotonic non-decreasing, so first-touch of
                    # a level is a single searchsorted.
                    row["_adv"] = _np2.maximum.accumulate(_np2.clip(adv, 0.0, None)).astype("float32")
                    row["_fav"] = _np2.maximum.accumulate(_np2.clip(fav, 0.0, None)).astype("float32")
                    row["_mtm"] = round(float(mtm), 3)
                    filled += 1

                sim_ready = filled >= max(1, int(0.5 * len(trade_rows)))
        except Exception as _se:
            logger.debug(f"MAE/MFE forward re-simulation unavailable: {_se}")
            sim_ready = False

        # --- Step 3: K-Means clustering ----------------------------------
        mae_arr = np.array([r["mae"] for r in trade_rows])
        mfe_arr = np.array([r["mfe"] for r in trade_rows])
        pnl_arr = np.array([r["pnl_pct"] for r in trade_rows])
        def _cluster_rows(rows: list[dict], assign_labels: bool = False) -> tuple[list[dict], float | None, float | None]:
            """K-Means the (MAE, MFE) shape of `rows` into setup 'types'.

            Returns (clusters, recommended_tp, recommended_sl). Used for the
            global view AND per-regime, so a regime shows its OWN centroids
            rather than the global ones. `assign_labels` writes each row's
            cluster id back (only the global pass does this, so the scatter's
            cluster field stays globally consistent).
            """
            out: list[dict] = []
            if len(rows) < max(3, n_clusters):
                return out, None, None
            try:
                from sklearn.cluster import KMeans
                from sklearn.preprocessing import StandardScaler

                r_mae = np.array([r["mae"] for r in rows])
                r_mfe = np.array([r["mfe"] for r in rows])
                n_k = max(2, min(n_clusters, len(rows) // 10))
                if n_k > len(rows):
                    return out, None, None
                X = np.column_stack([r_mae, r_mfe])
                scaler = StandardScaler()
                X_sc = scaler.fit_transform(X)
                km = KMeans(n_clusters=n_k, n_init=10, random_state=42)
                labels = km.fit_predict(X_sc).astype(int)
                centers = scaler.inverse_transform(km.cluster_centers_)

                for ci in range(n_k):
                    mask = labels == ci
                    ct = [r for r, m in zip(rows, mask) if m]
                    if not ct:
                        continue
                    wins_c  = [r for r in ct if r["win"]]
                    loss_c  = [r for r in ct if not r["win"]]
                    wr_f    = len(wins_c) / len(ct)
                    avg_win = float(np.mean([r["pnl_pct"] for r in wins_c])) if wins_c else 0.0
                    avg_los = float(np.mean([abs(r["pnl_pct"]) for r in loss_c])) if loss_c else 0.0
                    expect  = wr_f * avg_win - (1.0 - wr_f) * avg_los
                    c_mae   = float(max(0.0, centers[ci][0]))
                    # 1R = the cluster's adverse-excursion centroid (its MAE % stop)
                    expect_r = round(expect / c_mae, 3) if c_mae > 0.01 else None
                    out.append({
                        "id":           int(ci),
                        "mae":          round(c_mae, 3),
                        "mfe":          round(float(max(0.0, centers[ci][1])), 3),
                        "size":         len(ct),
                        "win_rate":     round(wr_f * 100.0, 1),
                        "avg_pnl_pct":  round(float(np.mean([r["pnl_pct"] for r in ct])), 3),
                        "avg_win_pct":  round(avg_win, 3),
                        "avg_los_pct":  round(avg_los, 3),
                        "expectancy":   round(expect, 3),
                        "expectancy_r": expect_r,
                        "is_dominant":  False,
                    })

                if assign_labels:
                    for idx, row in enumerate(rows):
                        row["cluster"] = int(labels[idx])

                if not out:
                    return out, None, None
                # Dominant = highest expectancy × size (balances quality and frequency)
                best_c = max(out, key=lambda c: c["expectancy"] * c["size"])
                for c in out:
                    c["is_dominant"] = c["id"] == best_c["id"]
                return out, best_c["mfe"], best_c["mae"]

            except Exception as _ke:
                logger.warning(f"MAE/MFE K-means failed: {_ke}")
                return out, None, None

        cluster_results, recommended_tp, recommended_sl = _cluster_rows(trade_rows, assign_labels=True)

        # --- Expectancy model (shared by global + per-regime) ------------
        # Two implementations:
        #
        #  FROZEN (fallback, no candle data): replay each trade against a hard
        #  TP/SL using only its recorded full-hold excursions. This has a known
        #  look-ahead bias — the excursions were produced under the ORIGINAL
        #  exit policy, so a tighter SL "borrows" favourable moves / recoveries
        #  that the tighter SL would actually have truncated.
        #
        #  RESIM (preferred, candle data available): walk each trade's candles
        #  FORWARD from entry and decide the outcome by FIRST TOUCH — whichever
        #  of the SL / TP barrier the price reaches first. This is what a real
        #  sequential backtest does, so the recommendation is self-consistent
        #  with the policy it implies.
        def _expectancy_frozen(mae_a, mfe_a, pnl_a, tp: float, sl: float) -> float:
            out = np.where(mae_a >= sl, -sl, np.where(mfe_a >= tp, tp, pnl_a))
            return float(out.mean())

        def _sim_outcomes(rows: list[dict], tp: float, sl: float):
            """Per-trade outcome (%) under (tp, sl) by forward first-touch."""
            out = np.empty(len(rows), dtype=float)
            for i, r in enumerate(rows):
                adv = r["_adv"]; fav = r["_fav"]
                if adv is None or fav is None:
                    # Fall back to frozen logic for this trade.
                    if r["mae"] >= sl:      out[i] = -sl
                    elif r["mfe"] >= tp:    out[i] = tp
                    else:                   out[i] = r["pnl_pct"]
                    continue
                n = adv.shape[0]
                a = int(np.searchsorted(adv, sl, side="left"))   # first bar SL touched
                f = int(np.searchsorted(fav, tp, side="left"))   # first bar TP touched
                if a >= n and f >= n:
                    out[i] = r["_mtm"]          # neither barrier hit in the window
                elif a <= f:
                    out[i] = -sl                # SL first (ties → SL, conservative)
                else:
                    out[i] = tp                 # TP first
            return out

        def _auc(score: np.ndarray, positive: np.ndarray) -> float:
            """Probability a random positive outranks a random negative (Mann-Whitney
            U / ROC-AUC). 0.5 = the excursion carries no information about outcome."""
            n = len(score)
            n1 = int(positive.sum()); n0 = n - n1
            if n1 == 0 or n0 == 0:
                return 0.5
            order = np.argsort(score, kind="mergesort")
            ranks = np.empty(n, dtype=float); ranks[order] = np.arange(1, n + 1)
            return float((ranks[positive].sum() - n1 * (n1 + 1) / 2.0) / (n1 * n0))

        # --- Step 4: Per-regime exits from WINNER/LOSER EXCURSION SEPARATION
        # ------------------------------------------------------------------
        # Concept (Sweeney's MAE/MFE): an exit can only add expectancy when the
        # excursion is PREDICTIVE of outcome — i.e. losers reach a level winners
        # rarely do. So instead of curve-fitting a tight SL/TP to past P&L (which
        # overfits and kills winners), we:
        #   * place the stop at a HIGH percentile of WINNERS' MAE — most winners
        #     never reach it, but most losers do (cuts losers, keeps winners);
        #   * read the MFE give-back curve to set a profit-LOCK / trail trigger
        #     (the level past which trades stop giving gains back);
        #   * report AUC predictiveness so the user knows, honestly, whether an
        #     exit can help this strategy at all (AUC≈0.5 → it cannot; fix entry).
        MIN_OPT_TRADES = 30
        WINNER_KEEP = 92          # preserve ~92% of winners with the stop
        GIVEBACK_MAX = 10.0       # trail trigger where ≤10% of trades give it all back

        def _bucket_stats(rows: list[dict]) -> dict:
            mae_r = np.array([row["mae"] for row in rows])
            mfe_r = np.array([row["mfe"] for row in rows])
            win_m = np.array([bool(row["win"]) for row in rows])
            n = len(rows)
            wins_r = int(win_m.sum())

            auc_mae = round(_auc(mae_r, ~win_m), 3)   # MAE predicts a LOSER
            auc_mfe = round(_auc(mfe_r, win_m), 3)    # MFE predicts a WINNER
            predictive = auc_mae >= 0.55              # is a stop informative here?

            # Stop = high percentile of WINNERS' MAE (preserve winners, cut losers).
            if wins_r >= 5 and predictive:
                best_sl = round(float(np.percentile(mae_r[win_m], WINNER_KEEP)), 2)
            else:
                best_sl = round(float(np.percentile(mae_r, 75)), 2)
            best_sl = max(best_sl, 0.05)

            # MFE give-back trail trigger: lowest MFE level where the share of
            # trades that reach it yet end as losers drops to ≤ GIVEBACK_MAX %.
            trail_trigger = None
            for y in np.round(np.percentile(mfe_r, np.linspace(30, 90, 13)), 2):
                if y <= 0.01:
                    continue
                reached = mfe_r >= y
                if reached.sum() < max(10, 0.05 * n):
                    continue
                giveback = float((~win_m[reached]).mean() * 100.0)
                if giveback <= GIVEBACK_MAX:
                    trail_trigger = float(y)
                    break
            if trail_trigger is None:
                trail_trigger = round(float(np.percentile(mfe_r[win_m], 50)), 2) if wins_r else 0.0

            # Expectancy of the recommended STOP (no fixed TP — winners ride),
            # via forward first-touch re-sim with the TP barrier disabled.
            if sim_ready:
                oc = _sim_outcomes(rows, 1e9, best_sl)
                best_exp = round(float(oc.mean()), 3)
                sl_rate = round(float((oc < 0).mean() * 100.0), 1)
                sim_win_rate = round(float((oc > 0).mean() * 100.0), 1)
            else:
                best_exp = None; sl_rate = None; sim_win_rate = None
            best_exp_r = (round(best_exp / best_sl, 3)
                          if best_exp is not None and best_sl > 0.01 else None)

            # Share of WINNERS preserved by the stop and LOSERS it cuts.
            winners_kept = round(float((mae_r[win_m] < best_sl).mean() * 100.0), 1) if wins_r else 0.0
            losers_cut = round(float((mae_r[~win_m] >= best_sl).mean() * 100.0), 1) if (n - wins_r) else 0.0

            return {
                "count":         n,
                "win_rate":      round(wins_r / n * 100.0, 1),
                "sim_win_rate":  sim_win_rate,
                "mfe_p25":       round(float(np.percentile(mfe_r, 25)), 2),
                "mfe_median":    round(float(np.median(mfe_r)), 2),
                "mfe_p75":       round(float(np.percentile(mfe_r, 75)), 2),
                "mae_p25":       round(float(np.percentile(mae_r, 25)), 2),
                "mae_median":    round(float(np.median(mae_r)), 2),
                "mae_p75":       round(float(np.percentile(mae_r, 75)), 2),
                # Recommended exits (winner-preserving stop + MFE trail trigger).
                "best_sl":       best_sl,
                "best_tp":       round(trail_trigger, 2),    # MFE trail/lock trigger
                "best_exp":      best_exp,
                "best_exp_r":    best_exp_r,
                "tp_hit_rate":   round(float((mfe_r >= trail_trigger).mean() * 100.0), 1),
                "sl_hit_rate":   sl_rate if sl_rate is not None
                                 else round(float((mae_r >= best_sl).mean() * 100.0), 1),
                # New diagnostics: is the excursion predictive, and what does the
                # recommended stop do to winners vs losers.
                "auc_mae":       auc_mae,
                "auc_mfe":       auc_mfe,
                "predictive":    bool(predictive),
                "winners_kept":  winners_kept,
                "losers_cut":    losers_cut,
                "confident":     n >= MIN_OPT_TRADES,
            }

        per_regime: dict = {}
        for regime in sorted(set(r["regime"] for r in trade_rows)):
            rr = [row for row in trade_rows if row["regime"] == regime]
            if rr:
                stats = _bucket_stats(rr)
                # This regime's OWN cluster centroids, so selecting a regime in
                # the View dropdown shows stars for that regime rather than the
                # global ones (which sit outside the filtered scatter).
                r_clusters, r_tp, r_sl = _cluster_rows(rr)
                stats["clusters"] = r_clusters
                stats["cluster_tp"] = r_tp
                stats["cluster_sl"] = r_sl
                per_regime[regime] = stats

        # --- Global stats ------------------------------------------------
        global_stats = {
            "total":      len(trade_rows),
            "win_count":  sum(1 for r in trade_rows if r["win"]),
            "mfe_p25":    round(float(np.percentile(mfe_arr, 25)), 2),
            "mfe_median": round(float(np.median(mfe_arr)), 2),
            "mfe_p75":    round(float(np.percentile(mfe_arr, 75)), 2),
            "mfe_p90":    round(float(np.percentile(mfe_arr, 90)), 2),
            "mae_p25":    round(float(np.percentile(mae_arr, 25)), 2),
            "mae_median": round(float(np.median(mae_arr)), 2),
            "mae_p75":    round(float(np.percentile(mae_arr, 75)), 2),
            "mae_p90":    round(float(np.percentile(mae_arr, 90)), 2),
        }

        # Thin scatter to ≤5000 pts for frontend performance, and strip the
        # internal re-simulation fields (numpy arrays — not JSON serialisable).
        _DROP = {"_adv", "_fav", "_mtm", "open_rate", "leverage", "is_short"}
        rows_for_scatter = trade_rows
        if len(rows_for_scatter) > 5000:
            step = len(rows_for_scatter) // 5000 + 1
            rows_for_scatter = rows_for_scatter[::step]
        scatter = [{k: v for k, v in r.items() if k not in _DROP} for r in rows_for_scatter]

        return {
            "scatter":        scatter,
            "clusters":       cluster_results,
            "recommended_tp": recommended_tp,
            "recommended_sl": recommended_sl,
            "per_regime":     per_regime,
            "global":         global_stats,
            "regime_pair":    chosen_regime_pair,
            "timeframe":      tf or "",
            # "resim" = forward first-touch re-simulation (bias-free);
            # "frozen" = legacy excursion model (candle data unavailable).
            "method":         "resim" if sim_ready else "frozen",
        }

    # ------------------------------------------------------------------

    def _backtest_regime_analysis(self, name: str, regime_pair: str = "") -> dict:
        """
        Classify candle-level market regime for a backtest result and return
        per-regime metrics + equity series for the frontend charts.

        The regime reference pair is chosen in priority order:
          1. ``regime_pair`` query-param if provided and data exists
          2. BTC/USDT (or BTC/USDT:USDT, BTC/USD, BTC/BUSD) if data exists
          3. First traded pair that has candle data
        """
        try:
            import pandas as pd
            from VulcanTrader.regime_analysis import BacktestRegimeAnalyzer
            from VulcanTrader.data.history import load_pair_history
            from VulcanTrader.enums import CandleType, TradingMode
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Dependency unavailable: {exc}")

        bt = self._load_backtest(name)
        trades, tf, trading_mode = self._bt_extract_trades_and_meta(bt)

        if not tf:
            raise HTTPException(status_code=400, detail="Backtest result missing timeframe")

        datadir = self._datadir()
        if not datadir.is_dir():
            raise HTTPException(status_code=400, detail=f"datadir not found: {datadir}")

        data_format = self.config.get("dataformat_ohlcv", "feather")
        try:
            tm_enum = TradingMode(trading_mode) if trading_mode else TradingMode.SPOT
        except Exception:
            tm_enum = TradingMode.SPOT
        candle_type = CandleType.get_default(tm_enum.value)

        def _try_load(pair: str):
            try:
                df = load_pair_history(
                    pair=pair, timeframe=tf, datadir=datadir,
                    data_format=data_format, candle_type=candle_type,
                )
                if df is not None and len(df) > 50:
                    return df
            except Exception:
                pass
            return None

        traded_pairs = list(dict.fromkeys(
            t.get("pair", "") for t in trades if t.get("pair")
        ))

        # Starting capital
        starting_capital = 1000.0
        if isinstance(bt.get("strategy"), dict):
            for _strat_data in bt["strategy"].values():
                starting_capital = float(
                    _strat_data.get("starting_balance")
                    or _strat_data.get("dry_run_wallet")
                    or 1000.0
                )
                break

        def _norm_df(df):
            """Normalise date column and classify regime."""
            if "date" in df.columns:
                df = df.copy()
                df["date"] = pd.to_datetime(df["date"])
                if hasattr(df["date"].dtype, "tz") and df["date"].dtype.tz is not None:
                    df["date"] = df["date"].dt.tz_localize(None)
            return BacktestRegimeAnalyzer.classify_regime(df)

        # ── ALL mode: classify each trade by its own pair's regime ──────────
        if regime_pair.lower() in ("", "__all__", "all"):
            from datetime import datetime as _dt

            pair_regime_data: dict[str, tuple] = {}
            for pair in traded_pairs:
                df = _try_load(pair)
                if df is not None:
                    rdf = _norm_df(df)
                    raw = pd.to_datetime(rdf["date"])
                    if hasattr(raw.dtype, "tz") and raw.dtype.tz is not None:
                        raw = raw.dt.tz_localize(None)
                    _dates = raw.to_list()
                    pair_regime_data[pair] = (
                        _dates,
                        rdf["regime"].to_list(),
                        BacktestRegimeAnalyzer.resolve_interval(tf, _dates),
                    )

            if not pair_regime_data:
                raise HTTPException(
                    status_code=404,
                    detail="No candle data found for any traded pair. "
                           "Download OHLCV data or select a specific regime pair.",
                )

            def _lookup_per_pair(pair: str, dt_str: str) -> str:
                if pair not in pair_regime_data:
                    return "RANGING"
                dates, labels, interval = pair_regime_data[pair]
                try:
                    dt = _dt.fromisoformat(str(dt_str)[:19].replace(" ", "T"))
                except ValueError:
                    return "RANGING"
                return BacktestRegimeAnalyzer.label_at_entry(
                    dates, labels, dt, interval, default="RANGING"
                )

            pre_regimes = [
                _lookup_per_pair(t.get("pair", ""), t.get("open_date") or "")
                for t in trades
            ]

            result = BacktestRegimeAnalyzer.analyze_trades_by_regime(
                trades=trades,
                regime_df=None,
                starting_capital=starting_capital,
                trade_regimes=pre_regimes,
            )
            result["regime_pair"] = "ALL"
            result["timeframe"] = tf
            result["starting_capital"] = starting_capital
            return result

        # ── Specific pair or BTC fallback ────────────────────────────────────
        btc_candidates = ["BTC/USDT", "BTC/USDT:USDT", "BTC/USD", "BTC/BUSD"]
        candidates = [regime_pair] if regime_pair else []
        candidates.extend(btc_candidates)
        candidates.extend(traded_pairs)

        chosen_pair = None
        regime_df = None
        for pair in candidates:
            df = _try_load(pair)
            if df is not None:
                chosen_pair = pair
                regime_df = _norm_df(df)
                break

        if regime_df is None:
            raise HTTPException(
                status_code=404,
                detail="No candle data found for regime classification. "
                       "Ensure BTC/USDT data is downloaded or pass ?regime_pair=<pair>.",
            )

        result = BacktestRegimeAnalyzer.analyze_trades_by_regime(
            trades=trades,
            regime_df=regime_df,
            starting_capital=starting_capital,
            timeframe=tf,
        )
        result["regime_pair"] = chosen_pair
        result["timeframe"] = tf
        result["starting_capital"] = starting_capital
        return result

    def _backtest_volatility_bias(self, name: str, regime_pair: str = "") -> dict:
        """
        Classify AMBIENT market volatility (ATR% vs its own trailing
        baseline - see BacktestRegimeAnalyzer.classify_volatility) at each
        trade's entry and compare performance between HIGH_VOL / LOW_VOL
        entries.

        Mirrors _backtest_regime_analysis's data loading and pair-selection
        logic exactly (same reference-pair priority order), but classifies
        candles by realized volatility instead of trend direction.
        """
        try:
            import pandas as pd
            from VulcanTrader.regime_analysis import BacktestRegimeAnalyzer
            from VulcanTrader.data.history import load_pair_history
            from VulcanTrader.enums import CandleType, TradingMode
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Dependency unavailable: {exc}")

        bt = self._load_backtest(name)
        trades, tf, trading_mode = self._bt_extract_trades_and_meta(bt)

        if not tf:
            raise HTTPException(status_code=400, detail="Backtest result missing timeframe")

        datadir = self._datadir()
        if not datadir.is_dir():
            raise HTTPException(status_code=400, detail=f"datadir not found: {datadir}")

        data_format = self.config.get("dataformat_ohlcv", "feather")
        try:
            tm_enum = TradingMode(trading_mode) if trading_mode else TradingMode.SPOT
        except Exception:
            tm_enum = TradingMode.SPOT
        candle_type = CandleType.get_default(tm_enum.value)

        def _try_load(pair: str):
            try:
                df = load_pair_history(
                    pair=pair, timeframe=tf, datadir=datadir,
                    data_format=data_format, candle_type=candle_type,
                )
                if df is not None and len(df) > 50:
                    return df
            except Exception:
                pass
            return None

        traded_pairs = list(dict.fromkeys(
            t.get("pair", "") for t in trades if t.get("pair")
        ))

        starting_capital = 1000.0
        if isinstance(bt.get("strategy"), dict):
            for _strat_data in bt["strategy"].values():
                starting_capital = float(
                    _strat_data.get("starting_balance")
                    or _strat_data.get("dry_run_wallet")
                    or 1000.0
                )
                break

        def _norm_df(df):
            if "date" in df.columns:
                df = df.copy()
                df["date"] = pd.to_datetime(df["date"])
                if hasattr(df["date"].dtype, "tz") and df["date"].dtype.tz is not None:
                    df["date"] = df["date"].dt.tz_localize(None)
            return BacktestRegimeAnalyzer.classify_volatility(df)

        # ── ALL mode: classify each trade by its own pair's volatility ──────
        if regime_pair.lower() in ("", "__all__", "all"):
            from datetime import datetime as _dt

            pair_vol_data: dict[str, tuple] = {}
            for pair in traded_pairs:
                df = _try_load(pair)
                if df is not None:
                    vdf = _norm_df(df)
                    raw = pd.to_datetime(vdf["date"])
                    if hasattr(raw.dtype, "tz") and raw.dtype.tz is not None:
                        raw = raw.dt.tz_localize(None)
                    _dates = raw.to_list()
                    pair_vol_data[pair] = (
                        _dates,
                        vdf["vol_bucket"].to_list(),
                        BacktestRegimeAnalyzer.resolve_interval(tf, _dates),
                    )

            if not pair_vol_data:
                raise HTTPException(
                    status_code=404,
                    detail="No candle data found for any traded pair. "
                           "Download OHLCV data or select a specific volatility pair.",
                )

            def _lookup_per_pair(pair: str, dt_str: str):
                if pair not in pair_vol_data:
                    return None
                dates, labels, interval = pair_vol_data[pair]
                try:
                    dt = _dt.fromisoformat(str(dt_str)[:19].replace(" ", "T"))
                except ValueError:
                    return None
                return BacktestRegimeAnalyzer.label_at_entry(dates, labels, dt, interval)

            trade_vol_buckets = [
                _lookup_per_pair(t.get("pair", ""), t.get("open_date") or "")
                for t in trades
            ]

            result = BacktestRegimeAnalyzer.analyze_trades_by_volatility(
                trades=trades,
                vol_df=None,
                starting_capital=starting_capital,
                trade_vol_buckets=trade_vol_buckets,
                timeframe=tf,
            )
            result["vol_pair"] = "ALL"
            result["timeframe"] = tf
            result["starting_capital"] = starting_capital
            return result

        # ── Specific pair or BTC fallback ────────────────────────────────────
        btc_candidates = ["BTC/USDT", "BTC/USDT:USDT", "BTC/USD", "BTC/BUSD"]
        candidates = [regime_pair] if regime_pair else []
        candidates.extend(btc_candidates)
        candidates.extend(traded_pairs)

        chosen_pair = None
        vol_df = None
        for pair in candidates:
            df = _try_load(pair)
            if df is not None:
                chosen_pair = pair
                vol_df = _norm_df(df)
                break

        if vol_df is None:
            raise HTTPException(
                status_code=404,
                detail="No candle data found for volatility classification. "
                       "Ensure BTC/USDT data is downloaded or pass ?regime_pair=<pair>.",
            )

        result = BacktestRegimeAnalyzer.analyze_trades_by_volatility(
            trades=trades,
            vol_df=vol_df,
            starting_capital=starting_capital,
            timeframe=tf,
        )
        result["vol_pair"] = chosen_pair
        result["timeframe"] = tf
        result["starting_capital"] = starting_capital
        return result


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(level=logging.INFO)
    portal = WebPortal(bot=None)
    print(f"WebPortal token (Bearer): {portal._token}")
    print(f"Default password: {portal._password}")
    portal.start(blocking=True)


if __name__ == "__main__":
    main()
