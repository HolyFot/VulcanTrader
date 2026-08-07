"""Spawn a thread that inherits the calling thread's contextvars context.

``contextvars`` isolate by OS thread by default: a plain ``threading.Thread``
starts with a *blank* context, it does not inherit its parent thread's. That
is exactly the isolation ``persistence/base.py``'s ``PersistenceContext``
relies on to keep several bots' trade ledgers apart when they share one
process (``single_process_mode``) - but it also means any thread spawned ON
BEHALF of an already-bound bot (its own embedded web portal's server thread,
its stop-file/forceexit-file watcher threads) needs that bot's context
carried over explicitly, or it silently sees no bot's Trade/Order/PairLock
data at all.
"""

from __future__ import annotations

import contextvars
import threading
from collections.abc import Callable
from typing import Any


def start_thread_with_context(
    target: Callable[..., Any],
    *,
    name: str | None = None,
    daemon: bool = True,
    args: tuple = (),
    kwargs: dict | None = None,
) -> threading.Thread:
    """Like ``threading.Thread(...).start()``, but the new thread runs
    inside a copy of the calling thread's contextvars context, so it sees
    whatever ``PersistenceContext`` (or anything else contextvar-bound) is
    ambient on the caller right now."""
    ctx = contextvars.copy_context()

    def _runner() -> None:
        ctx.run(target, *args, **(kwargs or {}))

    t = threading.Thread(target=_runner, name=name, daemon=daemon)
    t.start()
    return t
