"""
Pure-Python persistence base.

Replaces the prior SQLAlchemy ``DeclarativeBase``. Storage lives in a
:class:`PersistenceContext` bound via ``contextvars`` rather than as
class-level attributes, so multiple bots can share one process
(``single_process_mode``) without their trade ledgers clobbering each
other -- each bot's own thread (and anything it spawns via
``util/ctx_thread.py::start_thread_with_context``) sees only its own
context. A single bot per process is just the degenerate case of exactly
one context ever being bound.

The :class:`_FakeSession` exposed via ``Model.session`` provides the small
subset of the SQLAlchemy ``Session`` API that callers in this codebase
actually use (``add``/``delete``/``commit``/``refresh``/``rollback``/``remove``).

Persistence to disk is handled by :mod:`VulcanTrader.persistence.models` --
``commit()`` triggers a JSON dump of the current context's models.
"""

from __future__ import annotations

import threading
from contextvars import ContextVar
from pathlib import Path
from typing import Any, ClassVar


class PersistenceContext:
    """One bot's own trade store: instance lists + id counters per model
    class, plus save/lock state. Bound to ``_current_db`` via contextvars
    so every thread/task descending from the thread that called
    ``init_db()`` sees this bot's data, and never another bot's sharing the
    same process."""

    def __init__(self) -> None:
        self._instances: dict[type, list] = {}
        self._next_id: dict[type, int] = {}
        self.active_path: Path | None = None
        self.writer_lock_fd: int | None = None
        self.writer_lock_path: Path | None = None
        self.save_callback: Any = None
        self.save_lock = threading.Lock()


_current_db: ContextVar["PersistenceContext | None"] = ContextVar("current_db", default=None)


def _ctx() -> PersistenceContext:
    ctx = _current_db.get()
    if ctx is None:
        raise RuntimeError(
            "No persistence context bound on this thread -- init_db() must "
            "run (on this thread, or a thread started via "
            "util/ctx_thread.py::start_thread_with_context from one that "
            "already has) before any Trade/Order/PairLock access."
        )
    return ctx


class _FakeSession:
    """Minimal subset of SQLAlchemy's ``Session`` API used by callers."""

    def add(self, obj: "ModelBase") -> None:
        type(obj)._add_instance(obj)
        # Mirror the cascade a real SQLAlchemy relationship gives for free: every
        # call site does `trade.orders.append(order_obj)` then `Trade.session.add
        # (trade)`, exactly like real freqtrade code does against a relationship-
        # mapped `orders` collection - there, appending alone stages the child for
        # the next commit via the relationship's cascade. `trade.orders` here is
        # just a plain list (see LocalTrade.orders), so without this, appended
        # Order objects were never added to `Order._instances()` at all - they
        # stayed correctly attached to `trade.orders` for this process's own
        # in-memory use (recalc_trade_from_orders() etc. worked fine), but every
        # order silently vanished from persistence: the JSON snapshot's "orders"
        # list was always empty regardless of how many orders had actually
        # executed, and any trade reloaded after a restart came back with zero
        # order history for get_latest_candle-independent things like
        # recalc_trade_from_orders() to work from. Idempotent like _add_instance
        # itself, so this runs safely every time a trade is (re-)added, including
        # when a later exit order is appended to an already-persisted trade.
        for order in getattr(obj, "orders", None) or ():
            order.ft_trade_id = obj.id
            type(order)._add_instance(order)

    def delete(self, obj: "ModelBase") -> None:
        type(obj)._delete_instance(obj)

    def commit(self) -> None:
        ctx = _current_db.get()
        if ctx is not None and ctx.save_callback is not None:
            ctx.save_callback()

    def flush(self) -> None:  # pragma: no cover
        pass

    def rollback(self) -> None:
        # In-memory store -- nothing to undo.
        pass

    def refresh(self, obj: "ModelBase") -> None:  # pragma: no cover
        pass

    def remove(self) -> None:  # pragma: no cover
        pass

    def expire_all(self) -> None:  # pragma: no cover
        pass


class ModelBase:
    """Common base for all persisted models."""

    session: ClassVar[_FakeSession] = _FakeSession()

    @classmethod
    def _instances(cls) -> list:
        return _ctx()._instances.setdefault(cls, [])

    @classmethod
    def _add_instance(cls, obj: "ModelBase") -> None:
        ctx = _ctx()
        lst = ctx._instances.setdefault(cls, [])
        next_id = ctx._next_id.get(cls, 1)
        if getattr(obj, "id", None) in (None, 0):
            obj.id = next_id  # type: ignore[attr-defined]
            ctx._next_id[cls] = next_id + 1
        else:
            ctx._next_id[cls] = max(next_id, int(obj.id) + 1)  # type: ignore[attr-defined]
        if obj not in lst:
            lst.append(obj)

    @classmethod
    def _delete_instance(cls, obj: "ModelBase") -> None:
        try:
            _ctx()._instances.setdefault(cls, []).remove(obj)
        except ValueError:
            pass

    @classmethod
    def _reset_instances(cls) -> None:
        ctx = _ctx()
        ctx._instances[cls] = []
        ctx._next_id[cls] = 1


# Backwards-compat alias (was the SQLAlchemy ``scoped_session`` type).
SessionType = _FakeSession
