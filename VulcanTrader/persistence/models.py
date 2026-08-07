"""
JSON-backed persistence bootstrap.

``init_db(db_url)`` parses a ``json:///path/to/file.json`` URL, loads any
existing trades/orders/locks/custom-data/key-value entries into a
:class:`~VulcanTrader.persistence.base.PersistenceContext` bound to the
calling thread's contextvars context (see persistence/base.py), and sets
that context's save callback so every ``Model.session.commit()``
atomically rewrites its own JSON file on disk. Contextvar-scoped (rather
than the single set of process-global lists this used to be) so several
bots can share one process (``single_process_mode``) without one bot's
``init_db()`` clobbering another's trade ledger.

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
import time
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from VulcanTrader.persistence.base import ModelBase, PersistenceContext, _current_db
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


def _acquire_writer_lock(ctx: PersistenceContext, path: Path) -> None:
    """Enforce exactly ONE writing process per persistence file, via an
    OS-level exclusive lock on a sibling ``<name>.lock`` file. Atomic
    write-then-rename protects against crashes *within* one process, but two
    processes (or, now that a process can host several bots at once via
    single_process_mode, two DIFFERENT bots) pointed at the same db_url would
    still race each other: interleaved writes and last-replace-wins can only
    be truly prevented by refusing the second writer outright, so that's what
    this does - the second one gets a clear OperationalException at startup
    instead of the two silently taking turns clobbering each other's
    snapshots forever.

    The lock is advisory-exclusive at the OS level (msvcrt.locking on Windows,
    fcntl.flock elsewhere), held on ``ctx`` (this bot's own
    :class:`PersistenceContext`, see persistence/base.py) for its whole
    lifetime. A crashed or killed PROCESS releases it automatically (a
    leftover .lock FILE from a dead process is lockable again immediately)
    - but a single bot's own graceful shutdown within a still-alive process
    (single_process_mode, several bots sharing one interpreter) does NOT
    release it by itself, since fds are process- not thread-scoped; see
    release_writer_lock(), called from bot.py's run_bot_lifecycle after
    cleanup. Re-initialising the same path from the same context (tests,
    restarts of the bot loop) reuses the already-held lock - see init_db()."""
    lock_path = path.with_name(path.name + ".lock")
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
    ctx.writer_lock_fd = fd
    ctx.writer_lock_path = lock_path


def release_writer_lock() -> None:
    """Release THIS thread's ambient PersistenceContext's OS-level writer
    lock (see _acquire_writer_lock), if any, and clear the context's save
    callback so a stray post-shutdown commit() becomes a silent no-op
    instead of writing through a closed fd.

    A single bot-per-process (``trade`` subcommand) never needed this - the
    OS releases every fd a process holds on exit, automatically, the moment
    that process dies. But under single_process_mode several bots' contexts
    all live in the SAME process, so a bot's own graceful shutdown finishing
    does NOT close its fd by itself (fds are process-scoped, not
    thread-scoped) - confirmed live: without this, a stopped bot's account
    kept reporting "running" via the lock-probe (_probe_db_lock /
    in_process_bot_running) for the rest of the shared process's lifetime,
    even though its account JSON's own is_running flag had correctly
    flipped to 0. Called from bot.py's run_bot_lifecycle after
    bot.cleanup()/portal.cleanup() finish. A no-op (nothing to release) when
    called from a subprocess-per-bot context, or twice."""
    ctx = _current_db.get()
    if ctx is None or ctx.writer_lock_fd is None:
        return
    try:
        os.close(ctx.writer_lock_fd)
    except OSError:
        pass
    ctx.writer_lock_fd = None
    ctx.writer_lock_path = None
    ctx.save_callback = None


def _sync_trade_orders() -> None:
    """Register every Order attached to a live Trade into ``Order._instances()``
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
    removes a deleted trade's orders from ``_instances()`` itself, so this
    never resurrects them. Runs against whichever context is ambient (see
    _save_snapshot) - only ever called from that bot's own commit path."""
    for trade in Trade._instances():
        for order in getattr(trade, "orders", None) or ():
            order.ft_trade_id = trade.id
            Order._add_instance(order)


def _snapshot() -> dict[str, list[dict[str, Any]]]:
    _sync_trade_orders()
    return {
        name: [obj.to_dict() for obj in cls._instances()]
        for name, cls in _REGISTRY
    }


_REPLACE_RETRY_ATTEMPTS = 25
_REPLACE_RETRY_BASE_DELAY_S = 0.01


def _replace_with_retry(tmp: Path, dest: Path) -> None:
    """``os.replace()``, retrying a transient Windows sharing violation.

    Windows refuses to replace a file that's currently open elsewhere
    without FILE_SHARE_DELETE, and a concurrent READER of THIS SAME
    account file is a real, reachable scenario: web_portal.py's fleet
    account scan (``_account_bot_info``, behind ``/api/livebots``) opens
    every account JSON directly via ``path.read_text()``, including an
    in-process bot's own file while that bot's trade loop is mid-commit -
    confirmed live via a concurrent-readers stress test, which reproduced
    ``PermissionError: [WinError 5]`` on ``os.replace()`` reliably under
    load. POSIX ``rename()`` has no such restriction. The reader's
    ``open()`` here is always a single short-lived ``read_text()`` call,
    so a short bounded retry clears it without risking a stuck write -
    exponential backoff capped at ~1s total worst case, negligible next to
    the multi-second bot cycle time this runs on."""
    delay = _REPLACE_RETRY_BASE_DELAY_S
    for attempt in range(_REPLACE_RETRY_ATTEMPTS):
        try:
            os.replace(tmp, dest)
            return
        except OSError:
            if attempt == _REPLACE_RETRY_ATTEMPTS - 1:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 0.25)


def _save_snapshot(ctx: PersistenceContext) -> None:
    """Write ``ctx``'s own trade store to its own ``active_path``. Bound as
    ``ctx.save_callback`` by init_db() and invoked by _FakeSession.commit()
    -- always from that bot's own thread, so the ambient contextvar used by
    _snapshot()/Trade._instances() etc. above already resolves to this same
    ``ctx`` by the time this runs; ``ctx`` is still taken explicitly here
    (rather than re-derived) so the disk-write path for real trade records
    never depends on ambient state being correct at call time."""
    if ctx.active_path is None or ctx.active_path == Path(os.devnull):
        return
    with ctx.save_lock:
        tmp: Path | None = None
        try:
            ctx.active_path.parent.mkdir(parents=True, exist_ok=True)
            # Per-PID tmp name: even in the (already lock-prevented, see
            # _acquire_writer_lock) scenario of two writers on one db file,
            # they can never interleave writes inside one tmp file.
            tmp = ctx.active_path.with_name(f"{ctx.active_path.name}.{os.getpid()}.tmp")
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
            _replace_with_retry(tmp, ctx.active_path)
        except Exception:
            logger.exception("Failed to persist DB snapshot to %s", ctx.active_path)
            # Leave no unverified garbage behind; the live file is untouched.
            if tmp is not None:
                try:
                    tmp.unlink(missing_ok=True)
                except OSError:
                    pass


def _load_snapshot(path: Path, ctx: PersistenceContext) -> None:
    # Reset everything so re-initialisation (tests, or the same bot
    # re-running init_db for the same path) doesn't accumulate state. Must
    # run with ``ctx`` already the ambient context (see init_db) since
    # _reset_instances() resolves it that way.
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
    """Initialise JSON-backed persistence for the given URL, bound to a
    :class:`PersistenceContext` on THIS thread's contextvars context (see
    persistence/base.py). A thread that never called init_db() (or wasn't
    started via util/ctx_thread.py::start_thread_with_context from one that
    did) sees no Trade/Order/PairLock data at all - this is the isolation
    that lets several bots share one process (single_process_mode) without
    their trade ledgers clobbering each other."""
    path = _path_from_db_url(db_url)
    existing = _current_db.get()
    if existing is not None and existing.active_path == path:
        # Same bot re-initialising the same path (tests, RELOAD_CONFIG) -
        # keep its already-held writer lock and in-memory state's identity;
        # _load_snapshot below still resets+repopulates it from disk.
        ctx = existing
    else:
        ctx = PersistenceContext()
        if path != Path(os.devnull):
            # Refuse to start at all if another writer already owns this file
            # - the only way two writers can't corrupt each other is for the
            # second one to never get going (see _acquire_writer_lock).
            _acquire_writer_lock(ctx, path)
            # Sweep stale per-PID tmp files left by crashed processes (never
            # the live file itself; a tmp only becomes live via a verified
            # os.replace).
            for stale in path.parent.glob(f"{path.name}.*.tmp"):
                try:
                    stale.unlink()
                    logger.info("Removed stale persistence tmp file %s", stale)
                except OSError:
                    pass
        ctx.active_path = path
    _current_db.set(ctx)
    logger.info("Using JSON-backed persistence: %s", path)
    _load_snapshot(path, ctx)
    ctx.save_callback = functools.partial(_save_snapshot, ctx)


def custom_data_rpc_wrapper(func):
    """Backwards-compat decorator. With JSON persistence there is no session
    to manage -- this is now a passthrough."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
