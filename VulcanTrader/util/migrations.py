"""Minimal migrations module.

Placeholder used by ``data/history/history_utils.py``. Real migration logic
can be added here later as the port progresses.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from VulcanTrader.constants import Config


if TYPE_CHECKING:
    from VulcanTrader.exchange import Exchange


def migrate_data(config: Config, exchange: "Exchange | None" = None) -> None:
    """Migrate persisted data from old formats to new formats. (no-op stub)"""
    return None


def migrate_live_content(
    config: Config, exchange: "Exchange", starting_balance: float
) -> None:
    """Migrate DB content from old formats to new formats. (no-op stub)"""
    return None
