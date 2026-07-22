"""Pure-Python key/value store (no SQLAlchemy)."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Literal

from VulcanTrader.persistence.base import ModelBase


ValueTypes = str | datetime | float | int


class ValueTypesEnum(str, Enum):
    STRING = "str"
    DATETIME = "datetime"
    FLOAT = "float"
    INT = "int"


KeyStoreKeys = Literal[
    "bot_start_time",
    "startup_time",
    "binance_migration",
    # Liveness markers written by trader_bot.py so anything reading the account
    # JSON (dashboards, watchdogs) can tell whether the owning bot process is
    # alive: is_running flips 1 at startup / 0 on clean shutdown, and
    # last_heartbeat is refreshed every heartbeat interval - is_running=1 with
    # a stale last_heartbeat means the bot died without cleanup (crash/kill).
    "is_running",
    "last_heartbeat",
]


class _KeyValueStoreModel(ModelBase):
    """Persistent key/value entry."""

    __tablename__ = "KeyValueStore"

    def __init__(
        self,
        key: str,
        value_type: ValueTypesEnum | None = None,
        string_value: str | None = None,
        datetime_value: datetime | None = None,
        float_value: float | None = None,
        int_value: int | None = None,
        id: int | None = None,
    ) -> None:
        self.id = id or 0
        self.key = key
        self.value_type = value_type
        self.string_value = string_value
        self.datetime_value = datetime_value
        self.float_value = float_value
        self.int_value = int_value

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "key": self.key,
            "value_type": self.value_type.value
            if isinstance(self.value_type, ValueTypesEnum)
            else self.value_type,
            "string_value": self.string_value,
            "datetime_value": self.datetime_value.isoformat() if self.datetime_value else None,
            "float_value": self.float_value,
            "int_value": self.int_value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "_KeyValueStoreModel":
        dt = data.get("datetime_value")
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        vt = data.get("value_type")
        if isinstance(vt, str):
            try:
                vt = ValueTypesEnum(vt)
            except ValueError:
                pass
        return cls(
            id=data.get("id"),
            key=data["key"],
            value_type=vt,
            string_value=data.get("string_value"),
            datetime_value=dt,
            float_value=data.get("float_value"),
            int_value=data.get("int_value"),
        )

    @staticmethod
    def _find(key: str) -> "_KeyValueStoreModel | None":
        for kv in _KeyValueStoreModel._instances:
            if kv.key == key:
                return kv  # type: ignore[return-value]
        return None


class KeyValueStore:
    """Generic bot-wide persistent key/value store."""

    @staticmethod
    def store_value(key: KeyStoreKeys, value: ValueTypes) -> None:
        kv = _KeyValueStoreModel._find(key)
        if kv is None:
            kv = _KeyValueStoreModel(key=key)
            _KeyValueStoreModel._add_instance(kv)
        # Clear all value slots first: overwriting a key with a different type
        # otherwise leaves the previous type's slot populated forever - reads
        # stayed correct (every getter keys off value_type), but the stale
        # value kept being persisted to the JSON file on every commit.
        kv.string_value = None
        kv.datetime_value = None
        kv.float_value = None
        kv.int_value = None
        if isinstance(value, str):
            kv.value_type = ValueTypesEnum.STRING
            kv.string_value = value
        elif isinstance(value, datetime):
            kv.value_type = ValueTypesEnum.DATETIME
            kv.datetime_value = value
        elif isinstance(value, float):
            kv.value_type = ValueTypesEnum.FLOAT
            kv.float_value = value
        elif isinstance(value, int):
            kv.value_type = ValueTypesEnum.INT
            kv.int_value = value
        else:
            raise ValueError(f"Unknown value type {type(value).__name__}")
        _KeyValueStoreModel.session.commit()

    @staticmethod
    def delete_value(key: KeyStoreKeys) -> None:
        kv = _KeyValueStoreModel._find(key)
        if kv is not None:
            _KeyValueStoreModel._delete_instance(kv)
            _KeyValueStoreModel.session.commit()

    @staticmethod
    def get_value(key: KeyStoreKeys) -> ValueTypes | None:
        kv = _KeyValueStoreModel._find(key)
        if kv is None:
            return None
        if kv.value_type == ValueTypesEnum.STRING:
            return kv.string_value
        if kv.value_type == ValueTypesEnum.DATETIME and kv.datetime_value is not None:
            dt = kv.datetime_value
            return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt
        if kv.value_type == ValueTypesEnum.FLOAT:
            return kv.float_value
        if kv.value_type == ValueTypesEnum.INT:
            return kv.int_value
        raise ValueError(f"Unknown value type {kv.value_type}")

    @staticmethod
    def get_string_value(key: KeyStoreKeys) -> str | None:
        kv = _KeyValueStoreModel._find(key)
        if kv is None or kv.value_type != ValueTypesEnum.STRING:
            return None
        return kv.string_value

    @staticmethod
    def get_datetime_value(key: KeyStoreKeys) -> datetime | None:
        kv = _KeyValueStoreModel._find(key)
        if kv is None or kv.value_type != ValueTypesEnum.DATETIME or kv.datetime_value is None:
            return None
        dt = kv.datetime_value
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt

    @staticmethod
    def get_float_value(key: KeyStoreKeys) -> float | None:
        kv = _KeyValueStoreModel._find(key)
        if kv is None or kv.value_type != ValueTypesEnum.FLOAT:
            return None
        return kv.float_value

    @staticmethod
    def get_int_value(key: KeyStoreKeys) -> int | None:
        kv = _KeyValueStoreModel._find(key)
        if kv is None or kv.value_type != ValueTypesEnum.INT:
            return None
        return kv.int_value


def set_startup_time() -> None:
    """
    Sets bot_start_time to the first trade open date - or "now" on new databases.
    Sets startup_time to "now".
    """
    st = KeyValueStore.get_value("bot_start_time")
    if st is None:
        from VulcanTrader.persistence import Trade

        trades = sorted(
            [t for t in Trade._instances if getattr(t, "open_date", None) is not None],
            key=lambda t: t.open_date,
        )
        if trades:
            KeyValueStore.store_value("bot_start_time", trades[0].open_date_utc)
        else:
            KeyValueStore.store_value("bot_start_time", datetime.now(UTC))
    KeyValueStore.store_value("startup_time", datetime.now(UTC))
