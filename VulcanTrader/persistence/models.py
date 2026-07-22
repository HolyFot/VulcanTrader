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
import sys
import threading
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from VulcanTrader.persistence.base import ModelBase, _set_save_callback
from VulcanTrader.util.exceptions import OperationalException
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
# OS-level single-writer lock (see _acquire_writer_lock): file descriptor of the
# held ``<db>.lock`` file, kept open for the life of the process. The OS releases
# it automatically on ANY process exit, including crashes and hard kills.
_writer_lock_fd: int | None = None
_writer_lock_path: Path | None = None


def _acquire_writer_lock(path: Path) -> None:
    """Enforce exactly ONE writing process per persistence file, via an
    OS-level exclusive lock on a sibling ``<name>.lock`` file. Atomic
    write-then-rename protects against crashes *within* one process, but two
    processes pointed at the same db_url (the same bot accidentally launched
    twice, a stray duplicate in a batch script) would still race each other:
    interleaved writes and last-replace-wins can only be truly prevented by
    refusing the second writer outright, so that's what this does - the second
    process gets a clear OperationalException at startup instead of the two
    silently taking turns clobbering each other's snapshots forever.

    The lock is advisory-exclusive at the OS level (msvcrt.locking on Windows,
    fcntl.flock elsewhere), held for the entire process lifetime, and released
    by the OS itself on any kind of exit - no stale-lock cleanup needed: a
    leftover .lock FILE from a dead process is lockable again immediately.
    Re-initialising the same path in the same process (tests, restarts of the
    bot loop) reuses the already-held lock."""
    global _writer_lock_fd, _writer_lock_path
    lock_path = path.with_name(path.name + ".lock")
    if _writer_lock_fd is not None:
        if _writer_lock_path == lock_path:
            return  # same file re-initialised in-process: keep the held lock
        try:
            os.close(_writer_lock_fd)
        except OSError:
            pass
        _writer_lock_fd = None
        _writer_lock_path = None

    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise OperationalException(
            f"Another process is already using the persistence file '{path}' "
            f"(could not acquire '{lock_path}'). Two processes writing the same "
            "trade database would corrupt it - use a distinct --db-url for each "
            "bot instance."
        ) from None
    _writer_lock_fd = fd
    _writer_lock_path = lock_path


def _sync_trade_orders() -> None:
    """Register every Order attached to a live Trade into ``Order._instances``
    before snapshotting. With SQLAlchemy this was free: appending to a
    relationship-mapped ``trade.orders`` staged the child for the next commit
    via the relationship cascade. Here ``trade.orders`` is a plain list, and
    only ONE of the four call sites that append orders follows up with
    ``session.add(trade)`` (entry orders in execute_entry); exit orders,
    stoploss orders, and startup-recovered orders all just append and
    ``Trade.commit()`` - without this sweep, all three vanished from the JSON
    snapshot (its "orders" list stayed empty) and every reloaded trade came
    back with no order history. Doing it at the snapshot choke point instead
    of patching each call site covers current and future append-then-commit
    paths alike. ``_add_instance`` is idempotent, and ``Trade.delete()``
    removes a deleted trade's orders from ``_instances`` itself, so this
    never resurrects them."""
    for trade in Trade._instances:
        for order in getattr(trade, "orders", None) or ():
            order.ft_trade_id = trade.id
            Order._add_instance(order)


def _snapshot() -> dict[str, list[dict[str, Any]]]:
    _sync_trade_orders()
    return {
        name: [obj.to_dict() for obj in cls._instances]
        for name, cls in _REGISTRY
    }


def _save_snapshot() -> None:
    if _active_path is None or _active_path == Path(os.devnull):
        return
    with _save_lock:
        tmp: Path | None = None
        try:
            _active_path.parent.mkdir(parents=True, exist_ok=True)
            # Per-PID tmp name: even in the (already lock-prevented, see
            # _acquire_writer_lock) scenario of two processes on one db file,
            # they can never interleave writes inside one tmp file.
            tmp = _active_path.with_name(f"{_active_path.name}.{os.getpid()}.tmp")
            # write -> flush -> fsync -> VERIFY -> atomic replace: os.replace
            # alone is atomic in the filesystem NAMESPACE, but without fsync
            # the data itself may still be sitting in OS write-back cache when
            # a power loss / hard crash hits - leaving the (successfully
            # renamed) file truncated or zero-length on disk. fsync forces the
            # bytes down before the rename makes them the live copy.
            payload = json.dumps(_snapshot(), indent=2, default=str)
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            # Verify what actually LANDED ON DISK parses back as JSON before
            # promoting it to the live file. A short write (disk full), a disk
            # fault, or any serialization surprise is caught right here while
            # the previous good snapshot is still untouched - an unparseable
            # byte stream can never become the live file.
            with open(tmp, encoding="utf-8") as fh:
                json.loads(fh.read())
            os.replace(tmp, _active_path)
        except Exception:
            logger.exception("Failed to persist DB snapshot to %s", _active_path)
            # Leave no unverified garbage behind; the live file is untouched.
            if tmp is not None:
                try:
                    tmp.unlink(missing_ok=True)
                except OSError:
                    pass


def _load_snapshot(path: Path) -> None:
    # Reset everything so re-initialisation in tests doesn't accumulate state.
    for _, cls in _REGISTRY:
        cls._reset_instances()

    if not path.exists() or path == Path(os.devnull):
        return
    try:
        text = path.read_text(encoding="utf-8") or "{}"
        snapshot = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Preserve the damaged file before continuing: starting empty means the
        # very next commit() would OVERWRITE it with an empty snapshot,
        # permanently destroying trade history that may well be hand-recoverable
        # (atomic writes make corruption near-impossible from this code, but an
        # external editor, disk fault, or partial copy can still produce one).
        backup = path.with_name(
            f"{path.name}.corrupt-{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        )
        try:
            os.replace(path, backup)
            logger.error(
                "JSON persistence file %s is malformed; moved it to %s for manual "
                "recovery and starting empty.", path, backup,
            )
        except OSError:
            logger.exception(
                "JSON persistence file %s is malformed AND could not be backed up - "
                "starting empty; the next commit will overwrite it.", path,
            )
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
    if path != Path(os.devnull):
        # Refuse to start at all if another process is already writing this
        # file - the only way two writers can't corrupt each other is for the
        # second one to never get going (see _acquire_writer_lock).
        _acquire_writer_lock(path)
        # Sweep stale per-PID tmp files left by crashed processes (never the
        # live file itself; a tmp only becomes live via a verified os.replace).
        for stale in path.parent.glob(f"{path.name}.*.tmp"):
            try:
                stale.unlink()
                logger.info("Removed stale persistence tmp file %s", stale)
            except OSError:
                pass
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
