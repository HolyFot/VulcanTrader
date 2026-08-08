"""Spawn a thread that inherits the calling thread's contextvars context.

A plain ``threading.Thread`` starts with a *blank* context - the isolation
``PersistenceContext`` uses to keep bots' ledgers apart in
``single_process_mode``. Threads spawned on behalf of a bound bot (web
portal, stop-file watchers) need that context carried over explicitly.
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
