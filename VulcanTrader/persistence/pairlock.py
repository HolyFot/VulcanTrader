"""Pure-Python pair-lock model (no SQLAlchemy)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from VulcanTrader.constants import DATETIME_PRINT_FORMAT
from VulcanTrader.persistence.base import ModelBase


class PairLock(ModelBase):
    """Lock entry preventing trading on a pair until ``lock_end_time``."""

    __tablename__ = "pairlocks"

    def __init__(
        self,
        pair: str,
        lock_time: datetime,
        lock_end_time: datetime,
        side: str = "*",
        reason: str | None = None,
        active: bool = True,
        id: int | None = None,
    ) -> None:
        self.id = id or 0
        self.pair = pair
        self.side = side
        self.reason = reason
        self.lock_time = lock_time
        self.lock_end_time = lock_end_time
        self.active = active

    def __repr__(self) -> str:
        lt = self.lock_time.strftime(DATETIME_PRINT_FORMAT)
        let = self.lock_end_time.strftime(DATETIME_PRINT_FORMAT)
        return (
            f"PairLock(id={self.id}, pair={self.pair}, side={self.side}, lock_time={lt}, "
            f"lock_end_time={let}, reason={self.reason}, active={self.active})"
        )

    @property
    def lock_end_time_utc(self) -> datetime:
        """Lock end time with UTC timezoneinfo"""
        return self.lock_end_time.replace(tzinfo=UTC)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "pair": self.pair,
            "side": self.side,
            "reason": self.reason,
            "lock_time": self.lock_time.isoformat() if self.lock_time else None,
            "lock_end_time": self.lock_end_time.isoformat() if self.lock_end_time else None,
            "active": self.active,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PairLock":
        def _parse(v: Any) -> datetime | None:
            if v is None:
                return None
            return datetime.fromisoformat(v) if isinstance(v, str) else v

        return cls(
            id=data.get("id"),
            pair=data["pair"],
            side=data.get("side", "*"),
            reason=data.get("reason"),
            lock_time=_parse(data.get("lock_time")),
            lock_end_time=_parse(data.get("lock_end_time")),
            active=data.get("active", True),
        )

    @staticmethod
    def query_pair_locks(
        pair: str | None, now: datetime, side: str | None = None
    ) -> list["PairLock"]:
        """Return all currently active locks matching the filters."""
        result: list[PairLock] = []
        for lock in PairLock._instances:
            if not lock.active:
                continue
            if lock.lock_end_time is None or lock.lock_end_time <= now:
                continue
            if pair and lock.pair != pair:
                continue
            if side is not None and side != "*":
                if lock.side != side and lock.side != "*":
                    continue
            elif side is not None:  # side == "*"
                if lock.side != "*":
                    continue
            result.append(lock)  # type: ignore[arg-type]
        return result

    @staticmethod
    def get_all_locks() -> list["PairLock"]:
        return list(PairLock._instances)  # type: ignore[arg-type]

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "pair": self.pair,
            "lock_time": self.lock_time.strftime(DATETIME_PRINT_FORMAT),
            "lock_timestamp": int(self.lock_time.replace(tzinfo=UTC).timestamp() * 1000),
            "lock_end_time": self.lock_end_time.strftime(DATETIME_PRINT_FORMAT),
            "lock_end_timestamp": int(self.lock_end_time.replace(tzinfo=UTC).timestamp() * 1000),
            "reason": self.reason,
            "side": self.side,
            "active": self.active,
        }
