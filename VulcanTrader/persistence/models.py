"""
JSON-backed persistence bootstrap.

``init_db(db_url)`` parses a ``json:///path/to/file.json`` URL, loads any
existing trades/orders/locks/custom-data/key-value entries into the in-memory
class-level lists, and registers a save callback so every ``Model.session.commit()``
atomically rewrites the JSON file on disk.

Legacy ``sqlite://`` URLs are accepted for backwards compatibility -- they
are silently rewritten to a sibling ``.json`` file in the same directory.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import threading
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Final

from VulcanTrader.persistence.base import ModelBase, _set_save_callback
from VulcanTrader.persistence.custom_data import _CustomData
from VulcanTrader.persistence.key_value_store import _KeyValueStoreModel
from VulcanTrader.persistence.pairlock import PairLock
from VulcanTrader.persistence.trade_model import Order, Trade


logger = logging.getLogger(__name__)


REQUEST_ID_CTX_KEY: Final[str] = "request_id"
_request_id_ctx_var: ContextVar[str | None] = ContextVar(REQUEST_ID_CTX_KEY, default=None)


def get_request_or_thread_id() -> str | None:
    """Return the FastAPI request id (if set) or a thread-id fallback."""
    request_id = _request_id_ctx_var.get()
    if request_id is None:
        request_id = str(threading.current_thread().ident)
    return request_id


# ---------------------------------------------------------------------------
#  URL parsing
# ---------------------------------------------------------------------------


def _json_path_from_url(db_url: str) -> Path:
    """Parse ``json:///abs/path.json``, ``json://relative.json`` or ``json:relative.json``."""
    if db_url.startswith("json://"):
        raw = db_url[len("json://"):]
    elif db_url.startswith("json:"):
        raw = db_url[len("json:"):]
    else:
        raw = db_url
    raw = raw.lstrip("/")
    return Path(raw).expanduser().resolve()


def _path_from_db_url(db_url: str) -> Path:
    if db_url.startswith("json:"):
        return _json_path_from_url(db_url)
    if db_url.startswith("sqlite:///"):
        # Backwards-compat: rewrite to a sibling .json file.
        sqlite_path = Path(db_url[len("sqlite:///"):]).expanduser().resolve()
        json_path = sqlite_path.with_suffix(".json")
        logger.warning(
            "Legacy sqlite:// URL detected -- using JSON file %s instead.", json_path
        )
        return json_path
    if db_url == "sqlite://":
        # In-memory only -- use a temp file we never write back.
        return Path(os.devnull)
    raise ValueError(
        f"Unsupported db_url {db_url!r}. Use json:///path/to/file.json"
    )


# ---------------------------------------------------------------------------
#  Snapshot load / save
# ---------------------------------------------------------------------------


_REGISTRY: list[tuple[str, type[ModelBase]]] = [
    ("trades", Trade),
    ("orders", Order),
    ("pairlocks", PairLock),
    ("trade_custom_data", _CustomData),
    ("KeyValueStore", _KeyValueStoreModel),
]

_save_lock = threading.Lock()
_active_path: Path | None = None


def _snapshot() -> dict[str, list[dict[str, Any]]]:
    return {
        name: [obj.to_dict() for obj in cls._instances]
        for name, cls in _REGISTRY
    }


def _save_snapshot() -> None:
    if _active_path is None or _active_path == Path(os.devnull):
        return
    with _save_lock:
        try:
            _active_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = _active_path.with_suffix(_active_path.suffix + ".tmp")
            tmp.write_text(json.dumps(_snapshot(), indent=2, default=str))
            tmp.replace(_active_path)
        except Exception:
            logger.exception("Failed to persist DB snapshot to %s", _active_path)


def _load_snapshot(path: Path) -> None:
    # Reset everything so re-initialisation in tests doesn't accumulate state.
    for _, cls in _REGISTRY:
        cls._reset_instances()

    if not path.exists() or path == Path(os.devnull):
        return
    try:
        text = path.read_text() or "{}"
        snapshot = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("JSON persistence file %s is malformed; starting empty.", path)
        return

    # Load orders first so we can wire them onto trades by ft_trade_id.
    trades_raw = snapshot.get("trades", [])
    orders_raw = snapshot.get("orders", [])
    locks_raw = snapshot.get("pairlocks", [])
    custom_raw = snapshot.get("trade_custom_data", [])
    kv_raw = snapshot.get("KeyValueStore", [])

    trades_by_id: dict[int, Trade] = {}
    for row in trades_raw:
        try:
            t = Trade.from_dict(row)
        except Exception:
            logger.exception("Skipping malformed trade row: %s", row)
            continue
        Trade._add_instance(t)
        trades_by_id[t.id] = t

    for row in orders_raw:
        try:
            o = Order.from_dict(row)
        except Exception:
            logger.exception("Skipping malformed order row: %s", row)
            continue
        Order._add_instance(o)
        parent = trades_by_id.get(o.ft_trade_id) if o.ft_trade_id is not None else None
        if parent is not None:
            o._trade_live = parent
            parent.orders.append(o)

    for row in locks_raw:
        try:
            lock = PairLock.from_dict(row)
        except Exception:
            logger.exception("Skipping malformed pairlock row: %s", row)
            continue
        PairLock._add_instance(lock)

    for row in custom_raw:
        try:
            cd = _CustomData.from_dict(row)
        except Exception:
            logger.exception("Skipping malformed custom_data row: %s", row)
            continue
        _CustomData._add_instance(cd)

    for row in kv_raw:
        try:
            kv = _KeyValueStoreModel.from_dict(row)
        except Exception:
            logger.exception("Skipping malformed kv row: %s", row)
            continue
        _KeyValueStoreModel._add_instance(kv)

    logger.info(
        "Loaded JSON snapshot: %d trades, %d orders, %d locks, %d custom-data, %d kv entries.",
        len(trades_by_id),
        len(orders_raw),
        len(locks_raw),
        len(custom_raw),
        len(kv_raw),
    )


# ---------------------------------------------------------------------------
#  init_db
# ---------------------------------------------------------------------------


def init_db(db_url: str) -> None:
    """Initialise JSON-backed persistence for the given URL."""
    global _active_path
    path = _path_from_db_url(db_url)
    _active_path = path
    logger.info("Using JSON-backed persistence: %s", path)
    _load_snapshot(path)
    _set_save_callback(_save_snapshot)


def custom_data_rpc_wrapper(func):
    """Backwards-compat decorator. With JSON persistence there is no session
    to manage -- this is now a passthrough."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
