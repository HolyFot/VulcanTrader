"""
Pure-Python persistence base.

Replaces the prior SQLAlchemy ``DeclarativeBase``. Each subclass keeps a
class-level ``_instances`` list of live objects plus a monotonic ``_next_id``
counter. The :class:`_FakeSession` exposed via ``Model.session`` provides the
small subset of the SQLAlchemy ``Session`` API that callers in this codebase
actually use (``add``/``delete``/``commit``/``refresh``/``rollback``/``remove``).

Persistence to disk is handled by :mod:`VulcanTrader.persistence.models` --
``commit()`` triggers a JSON dump of every registered model.
"""

from __future__ import annotations

from typing import Any, ClassVar


# Populated by :func:`VulcanTrader.persistence.models.init_db`.
_save_callback: Any = None


def _set_save_callback(cb: Any) -> None:
    global _save_callback
    _save_callback = cb


class _FakeSession:
    """Minimal subset of SQLAlchemy's ``Session`` API used by callers."""

    def add(self, obj: "ModelBase") -> None:
        type(obj)._add_instance(obj)

    def delete(self, obj: "ModelBase") -> None:
        type(obj)._delete_instance(obj)

    def commit(self) -> None:
        if _save_callback is not None:
            _save_callback()

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

    _instances: ClassVar[list["ModelBase"]]
    _next_id: ClassVar[int]
    session: ClassVar[_FakeSession] = _FakeSession()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Each concrete model gets its own list/counter (not inherited).
        cls._instances = []
        cls._next_id = 1
        cls.session = _FakeSession()

    @classmethod
    def _add_instance(cls, obj: "ModelBase") -> None:
        if getattr(obj, "id", None) in (None, 0):
            obj.id = cls._next_id  # type: ignore[attr-defined]
            cls._next_id += 1
        else:
            cls._next_id = max(cls._next_id, int(obj.id) + 1)  # type: ignore[attr-defined]
        if obj not in cls._instances:
            cls._instances.append(obj)

    @classmethod
    def _delete_instance(cls, obj: "ModelBase") -> None:
        try:
            cls._instances.remove(obj)
        except ValueError:
            pass

    @classmethod
    def _reset_instances(cls) -> None:
        cls._instances = []
        cls._next_id = 1


# Backwards-compat alias (was the SQLAlchemy ``scoped_session`` type).
SessionType = _FakeSession
