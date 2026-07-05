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
import re
import secrets
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
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


def _empty_timing_summary() -> dict:
    return {
        "trades_analyzed": 0, "trades_skipped": 0,
        "improvable_entries": 0, "improvable_exits": 0,
        "earlier_exits": 0, "later_exits": 0,
        "total_entry_improvement_abs": 0.0, "total_exit_improvement_abs": 0.0,
        "avg_entry_improvement_abs": 0.0, "avg_exit_improvement_abs": 0.0,
        "sl_count": 0, "sl_recovered_count": 0, "sl_profitable_count": 0,
        "sl_actual_total_pnl_abs": 0.0, "sl_hypothetical_total_pnl_abs": 0.0,
        "sl_pnl_delta_abs": 0.0,
        "sl_profit_factor_actual": None, "sl_profit_factor_hypothetical": None,
    }


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


def _tail_log_file(path: Path, limit: int) -> list[dict]:
    """Return the last *limit* lines from a bot log file as log dicts.

    Used in viewer mode (no live bot in this process) to show the persistent
    log written by a separately-running bot process.
    """
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

    def __init__(self, bot: Any | None = None) -> None:
        self.bot = bot
        self.config: dict = getattr(bot, "config", {}) if bot else {}
        api_cfg = self.config.get("api_server", {}) if isinstance(self.config, dict) else {}

        self._password: str = api_cfg.get("password", "VulcanTrader")
        self._token: str = secrets.token_urlsafe(32)
        self._host: str = api_cfg.get("listen_ip_address", "127.0.0.1")
        self._port: int = int(api_cfg.get("listen_port", 8080))

        # Persistent log file written by bot.py (tailed in viewer mode).
        _user_data = self.config.get("user_data_dir", "user_data") if isinstance(self.config, dict) else "user_data"
        self._log_file: Path = Path(_user_data) / "logs" / "bot.log"

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
                    if self._server is not None:
                        # force_exit skips waiting for open connections (SSE, keep-alive)
                        # to drain — without it uvicorn blocks until the browser disconnects.
                        self._server.should_exit = True
                        self._server.force_exit = True
                    # Hard-exit backstop in case asyncio cleanup itself stalls.
                    def _force() -> None:
                        print(
                            "\n[VulcanTrader] Shutdown timed out — forcing exit.",
                            flush=True,
                        )
                        _os._exit(0)

                    t = threading.Timer(3.0, _force)
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
        self._server_thread = threading.Thread(
            target=self._server.run,
            name="WebPortalServer",
            daemon=True,
        )
        self._server_thread.start()
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
                return {
                    "attached": True,
                    "version": __version__,
                    "state": state.name if state is not None else None,
                    "dry_run": self.config.get("dry_run"),
                    "exchange": self.config.get("exchange", {}).get("name"),
                    "stake_currency": self.config.get("stake_currency"),
                    "max_open_trades": self.config.get("max_open_trades"),
                    "trading_mode": _json_safe(getattr(self.bot, "trading_mode", None)),
                    "last_process": _json_safe(getattr(self.bot, "last_process", None)),
                    "bot_name": self.config.get("bot_name"),
                    "strategy": strat.__class__.__name__ if strat is not None else None,
                }
            except Exception as exc:
                logger.exception("status endpoint failure")
                raise HTTPException(status_code=500, detail=str(exc)) from exc

        @app.post("/api/start", dependencies=[Depends(auth)])
        def api_start(req: StartBotRequest = Body(default_factory=StartBotRequest)) -> dict:
            if self.bot is None or not hasattr(self.bot, "process"):
                # Webserver mode: launch the bot as a subprocess
                if self._bot_process is not None and self._bot_process.poll() is None:
                    return {"state": "RUNNING"}  # already running
                if not req.config or not req.strategy:
                    raise HTTPException(status_code=400, detail="Select a config and strategy before starting the bot.")
                import sys
                cmd = [
                    sys.executable, "-m", "VulcanTrader.bot", "trade",
                    "-v", "-c", req.config, "--strategy", req.strategy,
                    "--no-web",
                ]
                if req.dry_run:
                    cmd.append("--dry-run")
                logger.info("Launching bot subprocess: %s", " ".join(cmd))
                self._bot_process = subprocess.Popen(cmd)
                self._bot_config_name = req.config
                return {"state": "RUNNING"}
            from VulcanTrader.enums import State
            self.bot.state = State.RUNNING
            logger.info("Bot state set to RUNNING via web portal.")
            return {"state": "RUNNING"}

        @app.post("/api/stop", dependencies=[Depends(auth)])
        def api_stop() -> dict:
            if self.bot is None or not hasattr(self.bot, "process"):
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
                lines = _tail_log_file(self._log_file, limit)
            return {"count": len(lines), "lines": lines}

        # --------------- live trade data ---------------
        @app.get("/api/trades/open", dependencies=[Depends(auth)])
        def api_open_trades() -> dict:
            trades = self._get_open_trades()
            return {"count": len(trades), "trades": [_trade_to_dict(t) for t in trades]}

        @app.get("/api/trades/closed", dependencies=[Depends(auth)])
        def api_closed_trades(limit: int = 1000) -> dict:
            trades = self._get_closed_trades(limit)
            return {"count": len(trades), "trades": [_trade_to_dict(t) for t in trades]}

        @app.get("/api/dashboard", dependencies=[Depends(auth)])
        def api_dashboard(closed_limit: int = 2000, msg_limit: int = 200, log_limit: int = 300) -> dict:
            """Single endpoint combining status + trades + messages + logs for the trading page."""
            status = api_status()
            open_trades = self._get_open_trades()
            closed_trades = self._get_closed_trades(closed_limit)
            with self._lock:
                messages = list(self._messages)[-msg_limit:]
            if self.bot is not None and hasattr(self.bot, "state"):
                logs = _log_capture.get_lines(log_limit)
            else:
                logs = _tail_log_file(self._log_file, log_limit)
            return {
                "status": status,
                "open_trades": [_trade_to_dict(t) for t in open_trades],
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

        # --------------- timing / counterfactual analysis ---------------
        @app.get("/api/analysis/timing", dependencies=[Depends(auth)])
        def api_analysis_timing(
            limit: int = 200,
            entry_window: int = 10,
            exit_window: int = 10,
            sl_horizon: int = 50,
        ) -> dict:
            return self._analyse_timing(
                limit=limit,
                entry_window=entry_window,
                exit_window=exit_window,
                sl_horizon=sl_horizon,
            )

        # --------------- backtest results ---------------
        @app.get("/api/backtests", dependencies=[Depends(auth)])
        def api_backtests() -> dict:
            return {"backtests": self._list_backtests()}

        # --------------- backtest runner ---------------
        # NOTE: literal-path routes (/jobs, /run) MUST be registered before
        # the parameterised /{name} routes, otherwise FastAPI matches "jobs"
        # as a backtest name.
        @app.get("/api/configs", dependencies=[Depends(auth)])
        def api_configs() -> dict:
            return {"configs": self._list_configs()}

        @app.get("/api/strategies", dependencies=[Depends(auth)])
        def api_strategies() -> dict:
            return {"strategies": self._list_strategies()}

        @app.post("/api/backtests/run", dependencies=[Depends(auth)])
        def api_backtest_run(req: BacktestRunRequest) -> dict:
            return self._submit_backtest_job(
                config=req.config,
                strategy=req.strategy,
                timerange=req.timerange,
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

        @app.get("/api/backtests/{name}", dependencies=[Depends(auth)])
        def api_backtest(name: str) -> dict:
            return self._load_backtest(name)

        @app.get("/api/backtests/{name}/timing", dependencies=[Depends(auth)])
        def api_backtest_timing(
            name: str,
            entry_window: int = 10,
            exit_window: int = 10,
            sl_horizon: int = 50,
        ) -> dict:
            return self._analyse_backtest_timing(
                name=name,
                entry_window=entry_window,
                exit_window=exit_window,
                sl_horizon=sl_horizon,
            )

        @app.get("/api/backtests/{name}/regime_analysis", dependencies=[Depends(auth)])
        def api_bt_regime_analysis(name: str, regime_pair: str = "") -> dict:
            return self._backtest_regime_analysis(name, regime_pair)

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
    # Trade-timing counterfactual analysis
    # ------------------------------------------------------------------
    def _analyse_timing(
        self,
        *,
        limit: int,
        entry_window: int,
        exit_window: int,
        sl_horizon: int,
    ) -> dict:
        """
        For each closed trade, look at the analyzed candles around the trade
        window and compute:

          * **best entry**: most favourable entry price within
            ``[open - entry_window, open + entry_window]`` candles.
          * **best exit**: most favourable exit price within
            ``[open, close + exit_window]`` candles.
          * **MFE / MAE** during the actual hold period.
          * **SL bypass**: when ``exit_reason`` is a stoploss variant,
            simulate the trade continuing for ``sl_horizon`` more candles
            (until the next strategy exit signal or horizon end). Reports
            whether the trade would have recovered to breakeven and/or
            ended profitable, plus the deepest excursion against you.

        Trades whose pair is no longer in the analyzed dataframe cache are
        skipped (counted under ``skipped``).
        """
        from datetime import datetime

        if self.bot is None:
            raise HTTPException(status_code=503, detail="Bot not running")

        strat = self._strategy()
        dp = getattr(self.bot, "dataprovider", None) or getattr(strat, "dp", None)
        if strat is None or dp is None:
            raise HTTPException(status_code=503, detail="DataProvider unavailable")
        tf = getattr(strat, "timeframe", None)
        if not tf:
            raise HTTPException(status_code=400, detail="strategy timeframe missing")

        closed = self._get_closed_trades(limit)
        if not closed:
            return {
                "trades": [],
                "summary": _empty_timing_summary(),
                "params": {
                    "entry_window": entry_window,
                    "exit_window": exit_window,
                    "sl_horizon": sl_horizon,
                    "timeframe": tf,
                },
            }

        # Cache analyzed dataframes per pair (one fetch each).
        df_cache: dict[str, Any] = {}

        def _df_for(pair: str):
            if pair in df_cache:
                return df_cache[pair]
            try:
                df, _ = dp.get_analyzed_dataframe(pair, tf)
            except Exception:
                df = None
            df_cache[pair] = df
            return df

        rows: list[dict] = []
        skipped = 0
        sl_actual_pnl_total = 0.0
        sl_hyp_pnl_total = 0.0
        sl_count = 0
        sl_recovered = 0
        sl_profitable = 0
        entry_improvement_total = 0.0
        exit_improvement_total = 0.0
        improvable_entry = 0
        improvable_exit = 0
        earlier_exits = 0
        later_exits = 0

        sl_gw = sl_gl = 0.0
        sl_hyp_gw = sl_hyp_gl = 0.0

        for t in closed:
            pair = getattr(t, "pair", None)
            open_date = getattr(t, "open_date", None)
            close_date = getattr(t, "close_date", None)
            open_rate = float(getattr(t, "open_rate", 0) or 0)
            close_rate = float(getattr(t, "close_rate", 0) or 0)
            amount = float(getattr(t, "amount", 0) or 0)
            is_short = bool(getattr(t, "is_short", False))
            leverage = float(getattr(t, "leverage", 1) or 1)
            fee_open = float(getattr(t, "fee_open", 0) or 0)
            fee_close = float(getattr(t, "fee_close", 0) or 0)
            actual_pnl = float(
                getattr(t, "close_profit_abs", None)
                or getattr(t, "profit_abs", None)
                or 0
            )
            exit_reason = (getattr(t, "exit_reason", "") or "").lower()

            if not pair or open_date is None or close_date is None or open_rate <= 0:
                skipped += 1
                continue

            df = _df_for(pair)
            if df is None or len(df) == 0 or "date" not in df.columns:
                skipped += 1
                continue

            try:
                open_ts = open_date.timestamp() if hasattr(open_date, "timestamp") else \
                    datetime.fromisoformat(str(open_date)).timestamp()
                close_ts = close_date.timestamp() if hasattr(close_date, "timestamp") else \
                    datetime.fromisoformat(str(close_date)).timestamp()
            except Exception:
                skipped += 1
                continue

            try:
                _dc = df["date"]
                if hasattr(_dc.dtype, "tz") and _dc.dtype.tz is not None:
                    _dc = _dc.dt.tz_convert("UTC").dt.tz_localize(None)
                date_ts = _dc.astype("datetime64[s]").astype("int64").to_numpy()
            except Exception:
                skipped += 1
                continue

            n = len(date_ts)
            # Locate the candle containing the open / close.
            import bisect
            i_open = max(0, bisect.bisect_right(date_ts, open_ts) - 1)
            i_close = max(0, bisect.bisect_right(date_ts, close_ts) - 1)
            if i_close < i_open:
                i_close = i_open

            highs = df["high"].to_numpy() if "high" in df.columns else None
            lows = df["low"].to_numpy() if "low" in df.columns else None
            closes = df["close"].to_numpy() if "close" in df.columns else None
            if highs is None or lows is None or closes is None:
                skipped += 1
                continue

            # ---- best entry ----
            ent_lo = max(0, i_open - entry_window)
            ent_hi = min(n, i_open + entry_window + 1)
            ent_segment_lo = lows[ent_lo:ent_hi]
            ent_segment_hi = highs[ent_lo:ent_hi]
            if is_short:
                # Better entry = sell higher
                local = ent_segment_hi.argmax() if ent_segment_hi.size else 0
                best_entry_rate = float(ent_segment_hi[local]) if ent_segment_hi.size else open_rate
                entry_diff = best_entry_rate - open_rate
            else:
                local = ent_segment_lo.argmin() if ent_segment_lo.size else 0
                best_entry_rate = float(ent_segment_lo[local]) if ent_segment_lo.size else open_rate
                entry_diff = open_rate - best_entry_rate
            best_entry_idx = ent_lo + int(local)
            best_entry_date = _ts_to_iso(date_ts[best_entry_idx]) if best_entry_idx < n else None
            entry_improvement_abs = entry_diff * amount * leverage
            if entry_improvement_abs > 1e-9:
                improvable_entry += 1
                entry_improvement_total += entry_improvement_abs

            # ---- MFE / MAE during the actual hold period ----
            hold_lo = i_open
            hold_hi = min(n, i_close + 1)
            hold_high = float(highs[hold_lo:hold_hi].max()) if hold_hi > hold_lo else open_rate
            hold_low = float(lows[hold_lo:hold_hi].min()) if hold_hi > hold_lo else open_rate
            if is_short:
                mfe_pct = (open_rate - hold_low) / open_rate * leverage
                mae_pct = (hold_high - open_rate) / open_rate * leverage
            else:
                mfe_pct = (hold_high - open_rate) / open_rate * leverage
                mae_pct = (open_rate - hold_low) / open_rate * leverage

            # ---- best exit (symmetric around actual close) ----
            xw = max(0, int(exit_window))
            exit_lo = max(i_open, i_close - xw)
            exit_hi = min(n, i_close + xw + 1)
            seg_hi = highs[exit_lo:exit_hi]
            seg_lo = lows[exit_lo:exit_hi]
            if is_short:
                local_e = seg_lo.argmin() if seg_lo.size else 0
                best_exit_rate = float(seg_lo[local_e]) if seg_lo.size else close_rate
                exit_diff = close_rate - best_exit_rate
            else:
                local_e = seg_hi.argmax() if seg_hi.size else 0
                best_exit_rate = float(seg_hi[local_e]) if seg_hi.size else close_rate
                exit_diff = best_exit_rate - close_rate
            best_exit_idx = exit_lo + int(local_e)
            best_exit_date = _ts_to_iso(date_ts[best_exit_idx]) if best_exit_idx < n else None
            exit_improvement_abs = exit_diff * amount * leverage
            exit_bars_offset = int(best_exit_idx - i_close)
            if exit_improvement_abs > 1e-9:
                improvable_exit += 1
                exit_improvement_total += exit_improvement_abs
                if exit_bars_offset < 0:
                    earlier_exits += 1
                elif exit_bars_offset > 0:
                    later_exits += 1

            # ---- SL bypass ----
            sl_info = None
            is_sl = "stop" in exit_reason  # stop_loss, stoploss_on_exchange, trailing_stop_loss
            if is_sl:
                sl_count += 1
                sl_actual_pnl_total += actual_pnl
                if actual_pnl > 0:
                    sl_gw += actual_pnl
                else:
                    sl_gl += abs(actual_pnl)

                horizon_lo = max(0, i_close + 1)
                horizon_hi = min(n, i_close + 1 + sl_horizon)
                hyp_exit_idx = None
                hyp_exit_reason = "horizon"

                # Look for a strategy-driven exit signal in the horizon.
                exit_col = "exit_short" if is_short else "exit_long"
                if exit_col in df.columns:
                    seg = df[exit_col].to_numpy()[horizon_lo:horizon_hi]
                    for k, v in enumerate(seg):
                        if v:
                            hyp_exit_idx = horizon_lo + k
                            hyp_exit_reason = "exit_signal"
                            break

                if hyp_exit_idx is None and horizon_hi > horizon_lo:
                    hyp_exit_idx = horizon_hi - 1

                if hyp_exit_idx is not None:
                    hyp_exit_rate = float(closes[hyp_exit_idx])
                    # Best favourable price reached in horizon
                    h_hi = highs[horizon_lo:horizon_hi]
                    h_lo = lows[horizon_lo:horizon_hi]
                    if is_short:
                        best_after = float(h_lo.min()) if h_lo.size else hyp_exit_rate
                        worst_after = float(h_hi.max()) if h_hi.size else hyp_exit_rate
                        hyp_pnl_per_unit = (open_rate - hyp_exit_rate)
                        max_dd_after_pct = (worst_after - open_rate) / open_rate * leverage
                    else:
                        best_after = float(h_hi.max()) if h_hi.size else hyp_exit_rate
                        worst_after = float(h_lo.min()) if h_lo.size else hyp_exit_rate
                        hyp_pnl_per_unit = (hyp_exit_rate - open_rate)
                        max_dd_after_pct = (open_rate - worst_after) / open_rate * leverage

                    fee_factor = 1.0 - (fee_open + fee_close)
                    hyp_pnl_abs = hyp_pnl_per_unit * amount * leverage * fee_factor
                    hyp_pnl_pct = (hyp_pnl_per_unit / open_rate) * leverage if open_rate else 0.0

                    # Did it ever recover to >= entry-rate (breakeven) before horizon end?
                    if is_short:
                        recovered = bool((h_lo <= open_rate).any()) if h_lo.size else False
                    else:
                        recovered = bool((h_hi >= open_rate).any()) if h_hi.size else False
                    profitable = hyp_pnl_abs > 0

                    if profitable:
                        sl_profitable += 1
                    if recovered:
                        sl_recovered += 1
                    sl_hyp_pnl_total += hyp_pnl_abs
                    if hyp_pnl_abs > 0:
                        sl_hyp_gw += hyp_pnl_abs
                    else:
                        sl_hyp_gl += abs(hyp_pnl_abs)

                    sl_info = {
                        "horizon_bars": int(horizon_hi - horizon_lo),
                        "hyp_exit_idx_offset": int(hyp_exit_idx - i_close),
                        "hyp_exit_reason": hyp_exit_reason,
                        "hyp_exit_rate": hyp_exit_rate,
                        "hyp_exit_date": _ts_to_iso(date_ts[hyp_exit_idx]),
                        "best_rate_after_sl": best_after,
                        "max_drawdown_after_sl_pct": float(max_dd_after_pct),
                        "hypothetical_pnl_abs": float(hyp_pnl_abs),
                        "hypothetical_pnl_pct": float(hyp_pnl_pct),
                        "would_have_recovered": recovered,
                        "would_have_been_profitable": profitable,
                        "pnl_delta_vs_actual": float(hyp_pnl_abs - actual_pnl),
                    }

            rows.append({
                "trade_id": getattr(t, "id", None),
                "pair": pair,
                "is_short": is_short,
                "open_date": _iso(open_date),
                "close_date": _iso(close_date),
                "open_rate": open_rate,
                "close_rate": close_rate,
                "actual_pnl_abs": actual_pnl,
                "exit_reason": getattr(t, "exit_reason", None),
                "best_entry_rate": best_entry_rate,
                "best_entry_date": best_entry_date,
                "entry_improvement_abs": float(entry_improvement_abs),
                "best_exit_rate": best_exit_rate,
                "best_exit_date": best_exit_date,
                "exit_improvement_abs": float(exit_improvement_abs),
                "exit_bars_offset": exit_bars_offset,
                "mfe_pct": float(mfe_pct),
                "mae_pct": float(mae_pct),
                "sl_bypass": sl_info,
            })

        sl_pf_actual = (sl_gw / sl_gl) if sl_gl > 0 else (float("inf") if sl_gw > 0 else 0.0)
        sl_pf_hyp = (sl_hyp_gw / sl_hyp_gl) if sl_hyp_gl > 0 else (
            float("inf") if sl_hyp_gw > 0 else 0.0
        )

        analyzed = len(rows)
        summary = {
            "trades_analyzed": analyzed,
            "trades_skipped": skipped,
            "improvable_entries": improvable_entry,
            "improvable_exits": improvable_exit,
            "total_entry_improvement_abs": float(entry_improvement_total),
            "total_exit_improvement_abs": float(exit_improvement_total),
            "avg_entry_improvement_abs": (
                entry_improvement_total / analyzed if analyzed else 0.0
            ),
            "avg_exit_improvement_abs": (
                exit_improvement_total / analyzed if analyzed else 0.0
            ),
            "earlier_exits": earlier_exits,
            "later_exits": later_exits,
            "sl_count": sl_count,
            "sl_recovered_count": sl_recovered,
            "sl_profitable_count": sl_profitable,
            "sl_actual_total_pnl_abs": float(sl_actual_pnl_total),
            "sl_hypothetical_total_pnl_abs": float(sl_hyp_pnl_total),
            "sl_pnl_delta_abs": float(sl_hyp_pnl_total - sl_actual_pnl_total),
            "sl_profit_factor_actual": (
                None if sl_pf_actual == float("inf") else float(sl_pf_actual)
            ),
            "sl_profit_factor_hypothetical": (
                None if sl_pf_hyp == float("inf") else float(sl_pf_hyp)
            ),
        }
        return {
            "trades": rows,
            "summary": summary,
            "params": {
                "entry_window": entry_window,
                "exit_window": exit_window,
                "sl_horizon": sl_horizon,
                "timeframe": tf,
            },
        }

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

    def _list_configs(self) -> list[dict]:
        d = self._configs_dir()
        if not d.is_dir():
            return []
        out = []
        for p in sorted(d.glob("*.json")):
            try:
                out.append({
                    "name": p.stem,
                    "filename": p.name,
                    "size": p.stat().st_size,
                })
            except Exception:
                continue
        return out

    def _list_strategies(self) -> list[dict]:
        d = self._strategies_dir()
        if not d.is_dir():
            return []
        out: list[dict] = []
        cls_re = re.compile(r"^class\s+(\w+)\s*\([^)]*IStrategy[^)]*\)\s*:", re.MULTILINE)
        for p in sorted(d.glob("*.py")):
            if p.name.startswith("_"):
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for m in cls_re.finditer(text):
                out.append({"name": m.group(1), "file": p.name})
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
    ) -> dict:
        cfg_name = self._validate_name(config, "config")
        strat_name = self._validate_name(strategy, "strategy") if strategy else None
        timerange = self._validate_timerange(timerange)

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

    # ------------------------------------------------------------------
    # Backtest timing counterfactual analysis
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Backtest timing — refactored prepare/compute/sweep pipeline
    # ------------------------------------------------------------------
    def _bt_prepare_timing(self, name: str):
        """
        Load a backtest result + per-pair OHLCV once, resolve every trade to
        candle indices and numpy OHLC slices, and return the prepared list
        ready for cheap repeated window evaluation.
        """
        from datetime import datetime
        import bisect

        bt = self._load_backtest(name)
        trades, tf, trading_mode = self._bt_extract_trades_and_meta(bt)
        if not tf:
            raise HTTPException(status_code=400, detail="Backtest result missing timeframe")

        try:
            from VulcanTrader.data.history import load_pair_history
            from VulcanTrader.enums import CandleType, TradingMode
        except Exception as exc:  # pragma: no cover
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

        df_cache: dict[str, Any] = {}
        arr_cache: dict[str, dict] = {}

        def _df_for(pair: str):
            if pair in df_cache:
                return df_cache[pair]
            try:
                df = load_pair_history(
                    pair=pair,
                    timeframe=tf,
                    datadir=datadir,
                    data_format=data_format,
                    candle_type=candle_type,
                )
            except Exception:
                df = None
            df_cache[pair] = df
            return df

        def _arrays_for(pair: str):
            if pair in arr_cache:
                return arr_cache[pair]
            df = _df_for(pair)
            entry: dict | None = None
            if df is not None and len(df) and "date" in df.columns:
                try:
                    # Normalise to integer seconds regardless of pandas datetime resolution
                    # (pandas 2.0+ stores feather as datetime64[ms,UTC] so .astype("int64")
                    #  gives milliseconds, not nanoseconds — dividing by 10**9 would be wrong).
                    _dc = df["date"]
                    if hasattr(_dc.dtype, "tz") and _dc.dtype.tz is not None:
                        _dc = _dc.dt.tz_convert("UTC").dt.tz_localize(None)
                    date_ts = _dc.astype("datetime64[s]").astype("int64").to_numpy()
                    entry = {
                        "date_ts": date_ts,
                        "highs": df["high"].to_numpy(),
                        "lows": df["low"].to_numpy(),
                        "closes": df["close"].to_numpy(),
                        "n": int(len(date_ts)),
                    }
                except Exception:
                    entry = None
            arr_cache[pair] = entry  # type: ignore[assignment]
            return entry

        def _to_ts(v: Any) -> float | None:
            if v is None:
                return None
            try:
                if isinstance(v, (int, float)):
                    return float(v) / 1000.0 if v > 1e12 else float(v)
                if isinstance(v, datetime):
                    return v.timestamp()
                s = str(v).replace("Z", "+00:00")
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                return None

        prepared: list[dict] = []
        skipped = 0

        for idx, t in enumerate(trades):
            pair = t.get("pair")
            open_ts = _to_ts(t.get("open_date") or t.get("open_timestamp"))
            close_ts = _to_ts(t.get("close_date") or t.get("close_timestamp"))
            open_rate = float(t.get("open_rate") or 0)
            close_rate = float(t.get("close_rate") or 0)
            amount = float(t.get("amount") or 0)
            is_short = bool(t.get("is_short", False))
            leverage = float(t.get("leverage") or 1) or 1.0
            fee_open = float(t.get("fee_open") or 0)
            fee_close = float(t.get("fee_close") or 0)
            actual_pnl = float(
                t.get("profit_abs")
                if t.get("profit_abs") is not None
                else t.get("close_profit_abs") or 0
            )
            profit_ratio = float(
                t.get("profit_ratio")
                if t.get("profit_ratio") is not None
                else t.get("close_profit") or 0
            )
            exit_reason = (t.get("exit_reason") or "").lower()

            if not pair or open_ts is None or close_ts is None or open_rate <= 0:
                skipped += 1
                continue

            arrs = _arrays_for(pair)
            if arrs is None:
                skipped += 1
                continue

            date_ts = arrs["date_ts"]
            n = arrs["n"]
            i_open = max(0, bisect.bisect_right(date_ts, open_ts) - 1)
            i_close = max(0, bisect.bisect_right(date_ts, close_ts) - 1)
            if i_close < i_open:
                i_close = i_open

            # Reject trades whose timestamps fall completely outside the candle file.
            # bisect maps out-of-range timestamps to index 0 or n-1, producing nonsense results.
            candle_period = float(date_ts[1] - date_ts[0]) if n >= 2 else 0.0
            if open_ts < date_ts[0] - candle_period or open_ts > date_ts[-1] + candle_period:
                skipped += 1
                continue

            prepared.append({
                "trade_id": t.get("trade_id", idx),
                "pair": pair,
                "is_short": is_short,
                "leverage": leverage,
                "amount": amount,
                "fee_open": fee_open,
                "fee_close": fee_close,
                "actual_pnl": actual_pnl,
                "profit_ratio": profit_ratio,
                "exit_reason": exit_reason,
                "raw_exit_reason": t.get("exit_reason"),
                "open_rate": open_rate,
                "close_rate": close_rate,
                "open_date_iso": _iso(t.get("open_date")),
                "close_date_iso": _iso(t.get("close_date")),
                "i_open": int(i_open),
                "i_close": int(i_close),
                "n": int(n),
                "highs": arrs["highs"],
                "lows": arrs["lows"],
                "closes": arrs["closes"],
                "date_ts": date_ts,
            })

        # Extract starting capital (same pattern as regime analysis)
        starting_capital = 1000.0
        if isinstance(bt.get("strategy"), dict):
            for _sd in bt["strategy"].values():
                starting_capital = float(
                    _sd.get("starting_balance") or _sd.get("dry_run_wallet") or 1000.0
                )
                break

        return prepared, skipped, tf, starting_capital

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

    def _compute_bt_timing_for(
        self,
        prepared: list[dict],
        skipped: int,
        tf: str,
        *,
        entry_window: int,
        exit_window: int,
        sl_horizon: int,
        starting_capital: float = 1000.0,
        with_rows: bool = True,
    ) -> dict:
        """Run window analysis over already-prepared trades for one combo."""
        rows: list[dict] = []
        sl_actual_pnl_total = 0.0
        sl_hyp_pnl_total = 0.0
        sl_count = 0
        sl_recovered = 0
        sl_profitable = 0
        entry_improvement_total = 0.0
        exit_improvement_total = 0.0
        improvable_entry = 0
        improvable_exit = 0
        earlier_exits = 0
        later_exits = 0
        sl_gw = sl_gl = 0.0
        sl_hyp_gw = sl_hyp_gl = 0.0
        # Scenario accumulators for simulated metrics
        _sc_pnl_actual: list[float] = []
        _sc_pnl_entry: list[float] = []
        _sc_pnl_exit: list[float] = []
        _sc_pnl_both: list[float] = []
        _sc_ratio_actual: list[float] = []
        _sc_ratio_entry: list[float] = []
        _sc_ratio_exit: list[float] = []
        _sc_ratio_both: list[float] = []
        _sc_open_dates: list[str] = []
        _sc_close_dates: list[str] = []
        # Lag curves: for each fixed delta, track [count, sum_improvement, count_positive]
        # Entry: "if I always enter delta bars after signal, avg improvement vs actual"
        # Exit:  "if I always exit delta bars vs actual close, avg improvement vs actual"
        _entry_lag: dict[int, list] = {}
        _exit_lag: dict[int, list] = {}

        for p in prepared:
            is_short = p["is_short"]
            leverage = p["leverage"]
            amount = p["amount"]
            fee_open = p["fee_open"]
            fee_close = p["fee_close"]
            actual_pnl = p["actual_pnl"]
            exit_reason = p["exit_reason"]
            open_rate = p["open_rate"]
            close_rate = p["close_rate"]
            i_open = p["i_open"]
            i_close = p["i_close"]
            n = p["n"]
            highs = p["highs"]
            lows = p["lows"]
            closes = p["closes"]
            date_ts = p["date_ts"]

            # ---- best entry (symmetric) ----
            ent_lo = max(0, i_open - entry_window)
            ent_hi = min(n, i_open + entry_window + 1)
            seg_lo = lows[ent_lo:ent_hi]
            seg_hi = highs[ent_lo:ent_hi]
            if is_short:
                local = seg_hi.argmax() if seg_hi.size else 0
                best_entry_rate = float(seg_hi[local]) if seg_hi.size else open_rate
                entry_diff = best_entry_rate - open_rate
            else:
                local = seg_lo.argmin() if seg_lo.size else 0
                best_entry_rate = float(seg_lo[local]) if seg_lo.size else open_rate
                entry_diff = open_rate - best_entry_rate
            best_entry_idx = ent_lo + int(local)
            entry_improvement_abs = entry_diff * amount * leverage
            if entry_improvement_abs > 1e-9:
                improvable_entry += 1
                entry_improvement_total += entry_improvement_abs

            # Lag curve: for each fixed FORWARD delta from 0 to +window,
            # compute the P&L improvement vs actual if we always entered at that offset.
            # Negative deltas are excluded — looking back before the signal is not actionable
            # and trivially shows higher prices for shorts / lower prices for longs because
            # the strategy enters during trends (the market was at better levels before entry).
            # Uses the candle low (longs) / high (shorts) — limit-order fill assumption.
            for _d in range(0, entry_window + 1):
                _idx = i_open + _d
                if 0 <= _idx < n:
                    _rate = float(lows[_idx]) if not is_short else float(highs[_idx])
                    _imp = (open_rate - _rate if not is_short else _rate - open_rate) * amount * leverage
                    _r = _entry_lag.setdefault(_d, [0, 0.0, 0])
                    _r[0] += 1
                    _r[1] += _imp
                    _r[2] += 1 if _imp > 0 else 0

            # ---- MFE / MAE during actual hold ----
            hold_lo = i_open
            hold_hi = min(n, i_close + 1)
            hold_high = float(highs[hold_lo:hold_hi].max()) if hold_hi > hold_lo else open_rate
            hold_low = float(lows[hold_lo:hold_hi].min()) if hold_hi > hold_lo else open_rate
            if is_short:
                mfe_pct = (open_rate - hold_low) / open_rate * leverage
                mae_pct = (hold_high - open_rate) / open_rate * leverage
            else:
                mfe_pct = (hold_high - open_rate) / open_rate * leverage
                mae_pct = (open_rate - hold_low) / open_rate * leverage

            # ---- best exit (symmetric around actual close) ----
            xw = max(0, int(exit_window))
            exit_lo = max(i_open, i_close - xw)
            exit_hi = min(n, i_close + xw + 1)
            xseg_hi = highs[exit_lo:exit_hi]
            xseg_lo = lows[exit_lo:exit_hi]
            if is_short:
                local_e = xseg_lo.argmin() if xseg_lo.size else 0
                best_exit_rate = float(xseg_lo[local_e]) if xseg_lo.size else close_rate
                exit_diff = close_rate - best_exit_rate
            else:
                local_e = xseg_hi.argmax() if xseg_hi.size else 0
                best_exit_rate = float(xseg_hi[local_e]) if xseg_hi.size else close_rate
                exit_diff = best_exit_rate - close_rate
            best_exit_idx = exit_lo + int(local_e)
            exit_improvement_abs = exit_diff * amount * leverage
            exit_bars_offset = int(best_exit_idx - i_close)
            if exit_improvement_abs > 1e-9:
                improvable_exit += 1
                exit_improvement_total += exit_improvement_abs
                if exit_bars_offset < 0:
                    earlier_exits += 1
                elif exit_bars_offset > 0:
                    later_exits += 1

            # Lag curve: for each fixed delta from -window to +window,
            # compute improvement vs actual if we always exited at that offset.
            # Uses close price — market order at that candle's close.
            for _d in range(-exit_window, exit_window + 1):
                _idx = i_close + _d
                if i_open <= _idx < n:
                    _rate = float(closes[_idx])
                    _imp = (_rate - close_rate if not is_short else close_rate - _rate) * amount * leverage
                    _r = _exit_lag.setdefault(_d, [0, 0.0, 0])
                    _r[0] += 1
                    _r[1] += _imp
                    _r[2] += 1 if _imp > 0 else 0

            # ---- SL bypass ----
            sl_info = None
            is_sl = "stop" in exit_reason
            if is_sl:
                sl_count += 1
                sl_actual_pnl_total += actual_pnl
                if actual_pnl > 0:
                    sl_gw += actual_pnl
                else:
                    sl_gl += abs(actual_pnl)

                horizon_lo = max(0, i_close + 1)
                horizon_hi = min(n, i_close + 1 + sl_horizon)
                hyp_exit_idx = horizon_hi - 1 if horizon_hi > horizon_lo else None

                if hyp_exit_idx is not None:
                    hyp_exit_rate = float(closes[hyp_exit_idx])
                    h_hi = highs[horizon_lo:horizon_hi]
                    h_lo = lows[horizon_lo:horizon_hi]
                    if is_short:
                        best_after = float(h_lo.min()) if h_lo.size else hyp_exit_rate
                        worst_after = float(h_hi.max()) if h_hi.size else hyp_exit_rate
                        hyp_pnl_per_unit = open_rate - hyp_exit_rate
                        max_dd_after_pct = (worst_after - open_rate) / open_rate * leverage
                    else:
                        best_after = float(h_hi.max()) if h_hi.size else hyp_exit_rate
                        worst_after = float(h_lo.min()) if h_lo.size else hyp_exit_rate
                        hyp_pnl_per_unit = hyp_exit_rate - open_rate
                        max_dd_after_pct = (open_rate - worst_after) / open_rate * leverage

                    fee_factor = 1.0 - (fee_open + fee_close)
                    hyp_pnl_abs = hyp_pnl_per_unit * amount * leverage * fee_factor
                    hyp_pnl_pct = (
                        (hyp_pnl_per_unit / open_rate) * leverage if open_rate else 0.0
                    )

                    if is_short:
                        recovered = bool((h_lo <= open_rate).any()) if h_lo.size else False
                    else:
                        recovered = bool((h_hi >= open_rate).any()) if h_hi.size else False
                    profitable = hyp_pnl_abs > 0

                    if profitable:
                        sl_profitable += 1
                    if recovered:
                        sl_recovered += 1
                    sl_hyp_pnl_total += hyp_pnl_abs
                    if hyp_pnl_abs > 0:
                        sl_hyp_gw += hyp_pnl_abs
                    else:
                        sl_hyp_gl += abs(hyp_pnl_abs)

                    if with_rows:
                        sl_info = {
                            "horizon_bars": int(horizon_hi - horizon_lo),
                            "hyp_exit_idx_offset": int(hyp_exit_idx - i_close),
                            "hyp_exit_reason": "horizon",
                            "hyp_exit_rate": hyp_exit_rate,
                            "hyp_exit_date": _ts_to_iso(date_ts[hyp_exit_idx]),
                            "best_rate_after_sl": best_after,
                            "max_drawdown_after_sl_pct": float(max_dd_after_pct),
                            "hypothetical_pnl_abs": float(hyp_pnl_abs),
                            "hypothetical_pnl_pct": float(hyp_pnl_pct),
                            "would_have_recovered": recovered,
                            "would_have_been_profitable": profitable,
                            "pnl_delta_vs_actual": float(hyp_pnl_abs - actual_pnl),
                        }

            # Accumulate scenario data for portfolio-level simulated metrics.
            # stake = notional position size (amount * open_rate / leverage already
            # includes leverage in the dollar P&L, so we divide it back out here to
            # get the actual capital at risk = cost of the position before leverage).
            _stake = (p["amount"] * p["open_rate"] / p["leverage"]) if p["leverage"] else 0.0
            _pnl_e = actual_pnl + entry_improvement_abs
            _pnl_x = actual_pnl + exit_improvement_abs
            _pnl_b = actual_pnl + entry_improvement_abs + exit_improvement_abs
            _sc_pnl_actual.append(actual_pnl)
            _sc_pnl_entry.append(_pnl_e)
            _sc_pnl_exit.append(_pnl_x)
            _sc_pnl_both.append(_pnl_b)
            if _stake > 0:
                _sc_ratio_actual.append(actual_pnl / _stake)
                _sc_ratio_entry.append(_pnl_e / _stake)
                _sc_ratio_exit.append(_pnl_x / _stake)
                _sc_ratio_both.append(_pnl_b / _stake)
            _sc_open_dates.append(p["open_date_iso"] or "")
            _sc_close_dates.append(p["close_date_iso"] or "")

            if with_rows:
                rows.append({
                    "trade_id": p["trade_id"],
                    "pair": p["pair"],
                    "is_short": is_short,
                    "open_date": p["open_date_iso"],
                    "close_date": p["close_date_iso"],
                    "open_rate": open_rate,
                    "close_rate": close_rate,
                    "actual_pnl_abs": actual_pnl,
                    "exit_reason": p["raw_exit_reason"],
                    "best_entry_rate": best_entry_rate,
                    "best_entry_date": _ts_to_iso(date_ts[best_entry_idx])
                        if best_entry_idx < n else None,
                    "entry_improvement_abs": float(entry_improvement_abs),
                    "best_exit_rate": best_exit_rate,
                    "best_exit_date": _ts_to_iso(date_ts[best_exit_idx])
                        if best_exit_idx < n else None,
                    "exit_improvement_abs": float(exit_improvement_abs),
                    "exit_bars_offset": exit_bars_offset,
                    "mfe_pct": float(mfe_pct),
                    "mae_pct": float(mae_pct),
                    "sl_bypass": sl_info,
                })

        sl_pf_actual = (
            (sl_gw / sl_gl) if sl_gl > 0 else (float("inf") if sl_gw > 0 else 0.0)
        )
        sl_pf_hyp = (
            (sl_hyp_gw / sl_hyp_gl)
            if sl_hyp_gl > 0
            else (float("inf") if sl_hyp_gw > 0 else 0.0)
        )

        analyzed = len(prepared)
        summary = {
            "trades_analyzed": analyzed,
            "trades_skipped": skipped,
            "improvable_entries": improvable_entry,
            "improvable_exits": improvable_exit,
            "earlier_exits": earlier_exits,
            "later_exits": later_exits,
            "total_entry_improvement_abs": float(entry_improvement_total),
            "total_exit_improvement_abs": float(exit_improvement_total),
            "avg_entry_improvement_abs": (
                entry_improvement_total / analyzed if analyzed else 0.0
            ),
            "avg_exit_improvement_abs": (
                exit_improvement_total / analyzed if analyzed else 0.0
            ),
            "sl_count": sl_count,
            "sl_recovered_count": sl_recovered,
            "sl_profitable_count": sl_profitable,
            "sl_actual_total_pnl_abs": float(sl_actual_pnl_total),
            "sl_hypothetical_total_pnl_abs": float(sl_hyp_pnl_total),
            "sl_pnl_delta_abs": float(sl_hyp_pnl_total - sl_actual_pnl_total),
            "sl_profit_factor_actual": (
                None if sl_pf_actual == float("inf") else float(sl_pf_actual)
            ),
            "sl_profit_factor_hypothetical": (
                None if sl_pf_hyp == float("inf") else float(sl_pf_hyp)
            ),
        }
        # Serialise lag curves: avg improvement at each fixed bar offset
        def _lag_to_list(d: dict) -> list[dict]:
            out = []
            for delta in sorted(d):
                cnt, total, pos = d[delta]
                out.append({
                    "delta": delta,
                    "count": cnt,
                    "avg_improvement": round(total / cnt, 2) if cnt else 0.0,
                    "pct_better": round(pos / cnt * 100, 1) if cnt else 0.0,
                })
            return out

        entry_lag_curve = _lag_to_list(_entry_lag)
        exit_lag_curve = _lag_to_list(_exit_lag)

        # Compute simulated portfolio metrics for all four timing scenarios
        _common = dict(
            open_dates_iso=_sc_open_dates,
            close_dates_iso=_sc_close_dates,
            starting_capital=starting_capital,
        )
        simulated_metrics = {
            "actual":     self._sim_metrics(_sc_pnl_actual, _sc_ratio_actual, **_common),
            "best_entry": self._sim_metrics(_sc_pnl_entry,  _sc_ratio_entry,  **_common),
            "best_exit":  self._sim_metrics(_sc_pnl_exit,   _sc_ratio_exit,   **_common),
            "best_both":  self._sim_metrics(_sc_pnl_both,   _sc_ratio_both,   **_common),
        }

        return {
            "trades": rows,
            "summary": summary,
            "simulated_metrics": simulated_metrics,
            "entry_lag_curve": entry_lag_curve,
            "exit_lag_curve": exit_lag_curve,
            "params": {
                "entry_window": int(entry_window),
                "exit_window": int(exit_window),
                "sl_horizon": int(sl_horizon),
                "timeframe": tf,
                "starting_capital": starting_capital,
            },
        }

    def _analyse_backtest_timing(
        self,
        *,
        name: str,
        entry_window: int,
        exit_window: int,
        sl_horizon: int,
    ) -> dict:
        """One-shot timing analysis for a backtest result."""
        prepared, skipped, tf, starting_capital = self._bt_prepare_timing(name)
        if not prepared:
            return {
                "trades": [],
                "summary": _empty_timing_summary(),
                "simulated_metrics": None,
                "params": {
                    "entry_window": int(entry_window),
                    "exit_window": int(exit_window),
                    "sl_horizon": int(sl_horizon),
                    "timeframe": tf,
                    "starting_capital": starting_capital,
                },
            }
        return self._compute_bt_timing_for(
            prepared, skipped, tf,
            entry_window=entry_window,
            exit_window=exit_window,
            sl_horizon=sl_horizon,
            starting_capital=starting_capital,
        )

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
            import bisect as _bisect
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

                def _bisect_regime(dates, labels, dt_str: str) -> str:
                    try:
                        dt = _dt.fromisoformat(str(dt_str)[:19].replace(" ", "T"))
                    except ValueError:
                        return "RANGING"
                    idx = _bisect.bisect_right(dates, dt) - 1
                    if idx < 0:
                        return labels[0] if labels else "RANGING"
                    if idx >= len(labels):
                        return labels[-1]
                    return labels[idx]

                def _build_pc(df):
                    rdf = _norm_regime(df)
                    raw = pd.to_datetime(rdf["date"])
                    if hasattr(raw.dtype, "tz") and raw.dtype.tz is not None:
                        raw = raw.dt.tz_localize(None)
                    return raw.to_list(), rdf["regime"].to_list()

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
                                row["regime"] = _bisect_regime(pc[0], pc[1], row["open_date"])
                        chosen_regime_pair = "ALL"
                else:
                    btc_cands = ["BTC/USDT", "BTC/USDT:USDT", "BTC/USD", "BTC/BUSD"]
                    cands = ([regime_pair] if regime_pair else []) + btc_cands + traded_pairs
                    for pair in cands:
                        df = _try_load_r(pair)
                        if df is None:
                            continue
                        r_dates, r_labels = _build_pc(df)
                        for row in trade_rows:
                            row["regime"] = _bisect_regime(r_dates, r_labels, row["open_date"])
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
        cluster_results: list[dict] = []
        recommended_tp: float | None = None
        recommended_sl: float | None = None
        km_labels_arr = np.full(len(trade_rows), -1, dtype=int)

        if len(trade_rows) >= max(3, n_clusters):
            try:
                from sklearn.cluster import KMeans
                from sklearn.preprocessing import StandardScaler

                n_k = max(2, min(n_clusters, len(trade_rows) // 10))
                X = np.column_stack([mae_arr, mfe_arr])
                scaler = StandardScaler()
                X_sc = scaler.fit_transform(X)
                km = KMeans(n_clusters=n_k, n_init=10, random_state=42)
                km_labels_arr = km.fit_predict(X_sc).astype(int)
                centers = scaler.inverse_transform(km.cluster_centers_)

                for ci in range(n_k):
                    mask = km_labels_arr == ci
                    ct = [r for r, m in zip(trade_rows, mask) if m]
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
                    cluster_results.append({
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

                for idx, row in enumerate(trade_rows):
                    row["cluster"] = int(km_labels_arr[idx])

                if cluster_results:
                    # Dominant = highest expectancy × size (balances quality and frequency)
                    best_c = max(cluster_results, key=lambda c: c["expectancy"] * c["size"])
                    for c in cluster_results:
                        c["is_dominant"] = c["id"] == best_c["id"]
                    recommended_tp = best_c["mfe"]
                    recommended_sl = best_c["mae"]

            except Exception as _ke:
                logger.warning(f"MAE/MFE K-means failed: {_ke}")

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
                per_regime[regime] = _bucket_stats(rr)

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
            import bisect as _bisect
            from datetime import datetime as _dt

            pair_regime_data: dict[str, tuple] = {}
            for pair in traded_pairs:
                df = _try_load(pair)
                if df is not None:
                    rdf = _norm_df(df)
                    raw = pd.to_datetime(rdf["date"])
                    if hasattr(raw.dtype, "tz") and raw.dtype.tz is not None:
                        raw = raw.dt.tz_localize(None)
                    pair_regime_data[pair] = (raw.to_list(), rdf["regime"].to_list())

            if not pair_regime_data:
                raise HTTPException(
                    status_code=404,
                    detail="No candle data found for any traded pair. "
                           "Download OHLCV data or select a specific regime pair.",
                )

            def _lookup_per_pair(pair: str, dt_str: str) -> str:
                if pair not in pair_regime_data:
                    return "RANGING"
                dates, labels = pair_regime_data[pair]
                try:
                    dt = _dt.fromisoformat(str(dt_str)[:19].replace(" ", "T"))
                except ValueError:
                    return "RANGING"
                idx = _bisect.bisect_right(dates, dt) - 1
                if idx < 0:
                    return labels[0] if labels else "RANGING"
                if idx >= len(labels):
                    return labels[-1]
                return labels[idx]

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
        )
        result["regime_pair"] = chosen_pair
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
