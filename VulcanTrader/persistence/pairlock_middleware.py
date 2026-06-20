"""Pure-Python pair-lock middleware (no SQLAlchemy)."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

from VulcanTrader.exchange import timeframe_to_next_date
from VulcanTrader.persistence.pairlock import PairLock


logger = logging.getLogger(__name__)


class PairLocks:
    """Pair-lock middleware. ``use_db`` toggles between the persistent registry
    and an in-memory ``locks`` list (used during backtesting)."""

    use_db = True
    locks: list[PairLock] = []

    timeframe: str = ""

    @staticmethod
    def reset_locks() -> None:
        if not PairLocks.use_db:
            PairLocks.locks = []

    @staticmethod
    def lock_pair(
        pair: str,
        until: datetime,
        reason: str | None = None,
        *,
        now: datetime | None = None,
        side: str = "*",
    ) -> PairLock:
        lock_end_time = timeframe_to_next_date(PairLocks.timeframe, until)
        existing_locks = PairLocks.get_pair_locks(pair, now, side=side)
        for lock in existing_locks:
            if (
                lock.reason == reason
                and lock.lock_end_time_utc == lock_end_time
                and lock.side == side
            ):
                return lock

        lock = PairLock(
            pair=pair,
            lock_time=now or datetime.now(UTC),
            lock_end_time=lock_end_time,
            reason=reason,
            side=side,
            active=True,
        )
        if PairLocks.use_db:
            PairLock.session.add(lock)
            PairLock.session.commit()
        else:
            PairLocks.locks.append(lock)
        return lock

    @staticmethod
    def get_pair_locks(
        pair: str | None, now: datetime | None = None, side: str | None = None
    ) -> Sequence[PairLock]:
        if not now:
            now = datetime.now(UTC)

        if PairLocks.use_db:
            return PairLock.query_pair_locks(pair, now, side)
        return [
            lock
            for lock in PairLocks.locks
            if lock.lock_end_time >= now
            and lock.active is True
            and (pair is None or lock.pair == pair)
            and (side is None or lock.side == "*" or lock.side == side)
        ]

    @staticmethod
    def get_pair_longest_lock(
        pair: str, now: datetime | None = None, side: str = "*"
    ) -> PairLock | None:
        locks = PairLocks.get_pair_locks(pair, now, side=side)
        locks = sorted(locks, key=lambda lock: lock.lock_end_time, reverse=True)
        return locks[0] if locks else None

    @staticmethod
    def unlock_pair(pair: str, now: datetime | None = None, side: str = "*") -> None:
        if not now:
            now = datetime.now(UTC)
        logger.info(f"Releasing all locks for {pair}.")
        for lock in PairLocks.get_pair_locks(pair, now, side=side):
            lock.active = False
        if PairLocks.use_db:
            PairLock.session.commit()

    @staticmethod
    def unlock_reason(reason: str, now: datetime | None = None) -> None:
        if not now:
            now = datetime.now(UTC)

        if PairLocks.use_db:
            logger.info(f"Releasing all locks with reason '{reason}':")
            for lock in PairLock._instances:
                if (
                    lock.lock_end_time
                    and lock.lock_end_time > now
                    and lock.active
                    and lock.reason == reason
                ):
                    logger.info(f"Releasing lock for {lock.pair} with reason '{reason}'.")
                    lock.active = False
            PairLock.session.commit()
        else:
            for lock in PairLocks.get_pair_locks(None):
                if lock.reason == reason:
                    lock.active = False

    @staticmethod
    def is_global_lock(now: datetime | None = None, side: str = "*") -> bool:
        if not now:
            now = datetime.now(UTC)
        return len(PairLocks.get_pair_locks("*", now, side)) > 0

    @staticmethod
    def is_pair_locked(pair: str, now: datetime | None = None, side: str = "*") -> bool:
        if not now:
            now = datetime.now(UTC)
        return len(PairLocks.get_pair_locks(pair, now, side)) > 0 or PairLocks.is_global_lock(
            now, side
        )

    @staticmethod
    def get_all_locks() -> Sequence[PairLock]:
        if PairLocks.use_db:
            return PairLock.get_all_locks()
        return PairLocks.locks
