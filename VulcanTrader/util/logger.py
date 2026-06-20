"""Centralized logging setup.

Configures the root logger with:
    * a console (stderr) handler
    * a rotating file handler under ``logs/`` that flushes after every record
    * optional forwarding of WARNING/ERROR records to Discord

Import this module once at process start (e.g. from your entrypoint) and then
use ``logger.get_logger(__name__)`` everywhere else.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console

from VulcanTrader.util import discord_logger

ROOT = Path(__file__).resolve().parent.parent

# Shared rich consoles (used by progress trackers, rich tables, etc.)
error_console = Console(stderr=True, width=200)

# ── Constants ───────────────────────────────────────────────────────
_LOG_DIR = ROOT / "logs"
_LOG_DIR.mkdir(exist_ok=True)

_LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
_LOG_PREFIX = "VulcanTrader"

_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_BACKUP_COUNT = 5

_configured = False
_log_file: Optional[Path] = None


# ── Handlers ────────────────────────────────────────────────────────
class _FlushingRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """RotatingFileHandler that flushes after every record so ``tail -f`` is live."""

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        try:
            self.flush()
        except Exception:
            pass


class _DiscordHandler(logging.Handler):
    """Forwards WARNING/ERROR records to Discord (fire-and-forget)."""

    # Patterns for noisy but benign warnings that should NOT go to Discord.
    _SUPPRESS = re.compile(
        r"\[(?:init|exchange-init|reload_markets|fill_leverage_tiers|"
        r"trader_additional_exchange_init)\]"
        r".*(?:took \d|loaded \d+.*markets from cache)"
        r"|rate.limit"
        r"|is not installed, voice will NOT be supported"
        r"|Using fallback Tier \d+ pairs"
        r"|data starts at"
        r"|Backtest result caching",
        re.IGNORECASE,
    )

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if self._SUPPRESS.search(msg):
                return
            if record.levelno >= logging.ERROR:
                discord_logger.log_error(msg)
            else:
                discord_logger.log(msg, level="warning")
        except Exception:
            pass


def _log_namer(default_name: str) -> str:
    """Rewrite ``foo.txt.1`` -> ``foo.1.txt`` so the rotation index sits before the extension."""
    base, _, num = default_name.rpartition(".")
    stem, ext = os.path.splitext(base)
    return f"{stem}.{num}{ext}"


# ── Public API ──────────────────────────────────────────────────────
def setup(
    level: int = logging.INFO,
    *,
    log_to_discord: bool = False,
    quiet_libs: bool = True,
) -> Path:
    """Configure root logging. Safe to call more than once (subsequent calls are no-ops).

    Returns the path of the active log file.
    """
    global _configured, _log_file
    if _configured:
        assert _log_file is not None
        return _log_file

    formatter = logging.Formatter(_LOG_FMT, datefmt=_LOG_DATEFMT)

    root = logging.getLogger()
    root.setLevel(level)
    # Drop any handlers configured implicitly (e.g. by ``logging.basicConfig``).
    for h in list(root.handlers):
        root.removeHandler(h)

    # Console handler
    console = logging.StreamHandler(stream=sys.stderr)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Rotating file handler
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    _log_file = _LOG_DIR / f"{_LOG_PREFIX}_{timestamp}.txt"
    file_handler = _FlushingRotatingFileHandler(
        _log_file,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.namer = _log_namer
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Optional Discord handler — only WARNING and above to avoid spamming.
    if log_to_discord:
        discord_handler = _DiscordHandler(level=logging.WARNING)
        discord_handler.setFormatter(formatter)
        root.addHandler(discord_handler)

    if quiet_libs:
        for name in ("werkzeug", "urllib3", "websockets", "asyncio", "ccxt"):
            logging.getLogger(name).setLevel(logging.WARNING)

    _configured = True
    root.info("Logging to %s", _log_file)
    return _log_file


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger, configuring root logging on first use."""
    if not _configured:
        setup()
    return logging.getLogger(name)


def setup_logging(config: dict) -> Path:
    """Configure logging from a Configuration dict.

    Maps ``config['verbosity']`` (0/1/2+) onto WARNING/INFO/DEBUG and
    delegates to :func:`setup`. If ``config['discord']['webhook_url']`` is
    present, the Discord webhook logger is initialised and WARNING+ records
    are forwarded to Discord.
    """
    verbosity = int(config.get("verbosity", 0) or 0)
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.WARNING

    discord_conf = config.get("discord") or {}
    webhook_url = discord_conf.get("webhook_url")
    discord_enabled = discord_conf.get("enabled", True)
    log_to_discord = False
    if webhook_url and discord_enabled:
        discord_logger.init(webhook_url)
        log_to_discord = True

    return setup(level=level, log_to_discord=log_to_discord)


# ── Bias tester verbosity helpers ───────────────────────────────────
_bias_tester_saved_levels: dict[str, int] = {}
_BIAS_TESTER_QUIET = (
    "VulcanTrader.exchange",
    "VulcanTrader.data",
    "VulcanTrader.resolvers",
    "VulcanTrader.persistence",
    "VulcanTrader.strategy",
    "VulcanTrader.util.pairlistmanager",
)


def reduce_verbosity_for_bias_tester() -> None:
    """Silence chatty subsystems while running lookahead/recursive analysis."""
    for name in _BIAS_TESTER_QUIET:
        lg = logging.getLogger(name)
        _bias_tester_saved_levels[name] = lg.level
        lg.setLevel(logging.WARNING)


def restore_verbosity_for_bias_tester() -> None:
    """Undo :func:`reduce_verbosity_for_bias_tester`."""
    for name, level in _bias_tester_saved_levels.items():
        logging.getLogger(name).setLevel(level)
    _bias_tester_saved_levels.clear()


def set_level(level: int) -> None:
    """Update the level on the root logger and all attached handlers."""
    root = logging.getLogger()
    root.setLevel(level)
    for h in root.handlers:
        h.setLevel(level)


def shutdown() -> None:
    """Flush and close all handlers. Call on clean process exit."""
    logging.shutdown()


# ── LoggingMixin ────────────────────────────────────────────────────
from collections.abc import Callable

from cachetools import cached

from VulcanTrader.util.ft_ttlcache import FtTTLCache


class LoggingMixin:
    """
    Logging Mixin

    Shows similar messages only once every ``refresh_period`` seconds.
    """

    # Disable output completely
    show_output = True

    def __init__(self, logger, refresh_period: int = 3600):
        """
        :param refresh_period: in seconds - Show identical messages in this intervals
        """
        self.logger = logger
        self.refresh_period = refresh_period
        self._log_cache: FtTTLCache = FtTTLCache(maxsize=1024, ttl=self.refresh_period)

    def log_once(self, message: str, logmethod: Callable, force_show: bool = False) -> None:
        """
        Log ``message`` not more often than ``refresh_period`` to avoid spamming.

        :param message: String containing the message to be sent to the function.
        :param logmethod: Function that will be called. Typically ``logger.info``.
        :param force_show: If True, sends the message regardless of ``show_output``.
        """

        @cached(cache=self._log_cache)
        def _log_once(message: str):
            logmethod(message)

        # Log as debug first
        self.logger.debug(message)

        if self.show_output or force_show:
            _log_once(message)

# --- MeasureTime ----------------------------------------------------
import time as _time


class MeasureTime:
    """
    Context manager that measures execution time and invokes a callback
    when the elapsed duration exceeds `time_limit`.

    Used as either a one-shot context manager or a reusable measurement object.
    """

    def __init__(self, callback, time_limit: float):
        self.callback = callback
        self.time_limit = time_limit
        self._start: float = 0.0

    def __enter__(self):
        self._start = _time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = _time.perf_counter() - self._start
        if duration > self.time_limit:
            try:
                self.callback(duration, self.time_limit)
            except Exception:
                pass
        return False
