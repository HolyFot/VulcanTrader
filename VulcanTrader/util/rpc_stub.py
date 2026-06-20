"""Stub RPCManager.

The original freqtrade ``RPCManager`` has been replaced by the FastAPI
``WebPortal`` (see :mod:`VulcanTrader.web_portal`). This stub exists so legacy
type hints in :mod:`VulcanTrader.data.dataprovider` keep importing cleanly.
"""

from __future__ import annotations

from typing import Any


class RPCManager:
    """No-op RPC manager. Kept for backwards-compatible import chains."""

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def send_msg(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def cleanup(self) -> None:
        return None
