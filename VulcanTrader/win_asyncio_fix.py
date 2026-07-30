"""Windows asyncio setup shared by every long-lived VulcanTrader process
(bot.py, data_server.py, ...).

On Windows, ccxt's aiohttp-based async transport is unreliable on the default
ProactorEventLoop, so every entrypoint forces WindowsSelectorEventLoopPolicy.
That in turn makes SelectorEventLoop's internal self-pipe wakeup socket the
active mechanism, which occasionally logs a harmless but noisy traceback when
something (AV/firewall/idle-connection cleanup) resets it:

    SelectorEventLoop  -> ConnectionResetError   [WinError 10054] in _read_from_self
    ProactorEventLoop  -> ConnectionAbortedError [WinError 1236] on shutdown

Neither indicates a real problem - call :func:`apply` once, early, in every
process that touches asyncio on Windows, so both the policy and the matching
noise filter travel together and can't drift out of sync (as they did when
data_server.py copied only the policy half of bot.py's fix).

Deliberately a top-level module (not under VulcanTrader/util/, whose
__init__.py eagerly imports rich/humanize/etc.) and stdlib-only, so it can run
as the very first thing bot.py/data_server.py do, before any dependency is
guaranteed to be installed.
"""

from __future__ import annotations

import asyncio
import logging
import sys

_applied = False


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


def apply() -> None:
    """Force WindowsSelectorEventLoopPolicy (better ccxt compat) and suppress
    the resulting harmless self-pipe noise. No-op on non-Windows or if already
    applied in this process. Must run before any code creates an event loop
    (e.g. before Exchange is imported/used), since the policy only affects
    loops created after it's set."""
    global _applied
    if _applied or sys.platform != "win32":
        return
    _applied = True

    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

    logging.getLogger("asyncio").addFilter(_WinAsyncioNoiseFilter())
