"""Pure-Python per-trade custom-data store (no SQLAlchemy)."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from VulcanTrader.constants import DATETIME_PRINT_FORMAT
from VulcanTrader.persistence.base import ModelBase
from VulcanTrader.util import dt_now


logger = logging.getLogger(__name__)


class _CustomData(ModelBase):
    """Per-trade key/value metadata."""

    __tablename__ = "trade_custom_data"

    def __init__(
        self,
        ft_trade_id: int,
        cd_key: str,
        cd_type: str,
        cd_value: str,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        id: int | None = None,
    ) -> None:
        self.id = id or 0
        self.ft_trade_id = ft_trade_id
        self.cd_key = cd_key
        self.cd_type = cd_type
        self.cd_value = cd_value
        self.created_at = created_at or dt_now()
        self.updated_at = updated_at
        self.value: Any = None

    def __repr__(self) -> str:
        ct = self.created_at.strftime(DATETIME_PRINT_FORMAT) if self.created_at else None
        ut = self.updated_at.strftime(DATETIME_PRINT_FORMAT) if self.updated_at else None
        return (
            f"CustomData(id={self.id}, key={self.cd_key}, type={self.cd_type}, "
            f"value={self.cd_value}, trade_id={self.ft_trade_id}, created={ct}, updated={ut})"
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "ft_trade_id": self.ft_trade_id,
            "cd_key": self.cd_key,
            "cd_type": self.cd_type,
            "cd_value": self.cd_value,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "_CustomData":
        def _parse(v: Any) -> datetime | None:
            if v is None:
                return None
            return datetime.fromisoformat(v) if isinstance(v, str) else v

        return cls(
            id=data.get("id"),
            ft_trade_id=data["ft_trade_id"],
            cd_key=data["cd_key"],
            cd_type=data["cd_type"],
            cd_value=data["cd_value"],
            created_at=_parse(data.get("created_at")),
            updated_at=_parse(data.get("updated_at")),
        )

    @classmethod
    def query_cd(
        cls, key: str | None = None, trade_id: int | None = None
    ) -> Sequence["_CustomData"]:
        result: list[_CustomData] = []
        for cd in cls._instances:
            if trade_id is not None and cd.ft_trade_id != trade_id:
                continue
            if key is not None and cd.cd_key.casefold() != key.casefold():
                continue
            result.append(cd)  # type: ignore[arg-type]
        return result


class CustomDataWrapper:
    """Middleware wrapper around :class:`_CustomData`."""

    use_db = True
    custom_data: list[_CustomData] = []
    unserialized_types = ["bool", "float", "int", "str"]

    @staticmethod
    def _convert_custom_data(data: _CustomData) -> _CustomData:
        if data.cd_type in CustomDataWrapper.unserialized_types:
            data.value = data.cd_value
            if data.cd_type == "bool":
                data.value = data.cd_value.lower() == "true"
            elif data.cd_type == "int":
                data.value = int(data.cd_value)
            elif data.cd_type == "float":
                data.value = float(data.cd_value)
        else:
            data.value = json.loads(data.cd_value)
        return data

    @staticmethod
    def reset_custom_data() -> None:
        if not CustomDataWrapper.use_db:
            CustomDataWrapper.custom_data = []

    @staticmethod
    def delete_custom_data(trade_id: int) -> None:
        to_remove = [cd for cd in _CustomData._instances if cd.ft_trade_id == trade_id]
        for cd in to_remove:
            _CustomData._delete_instance(cd)
        if to_remove:
            _CustomData.session.commit()

    @staticmethod
    def get_custom_data(*, trade_id: int, key: str | None = None) -> list[_CustomData]:
        if CustomDataWrapper.use_db:
            filtered = [cd for cd in _CustomData._instances if cd.ft_trade_id == trade_id]
            if key is not None:
                filtered = [cd for cd in filtered if cd.cd_key.casefold() == key.casefold()]
        else:
            filtered = [
                d for d in CustomDataWrapper.custom_data if d.ft_trade_id == trade_id
            ]
            if key is not None:
                filtered = [
                    d for d in filtered if d.cd_key.casefold() == key.casefold()
                ]
        return [CustomDataWrapper._convert_custom_data(d) for d in filtered]

    @staticmethod
    def set_custom_data(trade_id: int, key: str, value: Any) -> None:
        value_type = type(value).__name__

        if value_type not in CustomDataWrapper.unserialized_types:
            try:
                value_db = json.dumps(value)
            except TypeError as e:
                logger.warning(f"could not serialize {key} value due to {e}")
                return
        else:
            value_db = str(value)

        if trade_id is None:
            trade_id = 0

        existing = CustomDataWrapper.get_custom_data(trade_id=trade_id, key=key)
        if existing:
            data_entry = existing[0]
            data_entry.cd_value = value_db
            data_entry.cd_type = value_type
            data_entry.updated_at = dt_now()
        else:
            data_entry = _CustomData(
                ft_trade_id=trade_id,
                cd_key=key,
                cd_type=value_type,
                cd_value=value_db,
                created_at=dt_now(),
            )
        data_entry.value = value

        if CustomDataWrapper.use_db and value_db is not None:
            if not existing:
                _CustomData.session.add(data_entry)
            _CustomData.session.commit()
        else:
            if not existing:
                CustomDataWrapper.custom_data.append(data_entry)
