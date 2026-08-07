"""Share one ``data_server`` connection across every bot in this process.

Each ``VulcanTraderBot`` used to open its own independent
``DataServerClient`` TCP socket and send its own separate
``register_interest`` call, even under ``single_process_mode`` where several
bots already share one interpreter — confirmed live (distinct client_ids,
distinct ephemeral ports per bot) and flagged as the most likely real
bottleneck at fleet scale (N sockets + N registrations against one master).

This module makes bots in the same process that target the same
``(data_server host, port, exchange)`` share exactly **one** real
``DataServerClient``, registering the *union* of their pairs under it
instead of N independent ones. Under subprocess mode this is inert: one bot
per process means the registry always starts empty and the sole bot just
creates-and-uses its own single-subscriber ``SharedDataServerClient`` — same
connection count as today, only the ``client_id`` naming changes. The
mechanism doesn't need to know about ``single_process_mode`` at all; it only
ever needs to notice "a shared client already exists for this key in this
process."

``DataServerClient.on_ohlcv_push``/``on_funding_rate_push``/
``on_trades_push`` are single-slot callables (not a list) and ``.register()``
is a full replace, both client-side and in the server's
``ClientInterestRegistry`` — so naively pointing two bots' ``DataProvider``s
at one raw ``DataServerClient`` would have the second bot's
``set_data_server_client()`` call silently clobber the first bot's push
callbacks and pair registration. ``BotDataServerHandle`` exists to present
each bot with what looks like its own private ``DataServerClient`` (same
public surface: ``register``, ``get_ohlcv``, ``get_trades``,
``get_funding_rate``, ``get_ticker``, ``connected``, ``stop``, and settable
``on_*_push`` attributes) while a ``SharedDataServerClient`` underneath
tracks each bot's own interest set, computes the union for the one real
registration, and fans out each push to only the bot(s) actually interested
in that specific pair.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pandas import DataFrame

    from VulcanTrader.data_server import DataServerClient

logger = logging.getLogger(__name__)


class BotDataServerHandle:
    """Per-bot facade over a :class:`SharedDataServerClient`. Looks exactly
    like a ``DataServerClient`` to callers (``DataProvider``,
    ``Exchange.set_data_server_client``) so neither needs any change."""

    def __init__(
        self, shared: "SharedDataServerClient", key: tuple[str, int, str], bot_id: str
    ) -> None:
        self._shared = shared
        self._key = key
        self._bot_id = bot_id
        self.on_ohlcv_push: Callable[[str, str, str, "DataFrame"], None] | None = None
        self.on_funding_rate_push: Callable[[str, dict], None] | None = None
        self.on_trades_push: Callable[[str, "DataFrame"], None] | None = None

    def register(
        self, pairs: list[str], *, want_funding_rate: bool = False, want_trades: bool = False
    ) -> None:
        self._shared._update_subscriber(self._bot_id, pairs, want_funding_rate, want_trades, self)

    def stop(self) -> None:
        # Goes through the module-level registry function (not
        # self._shared._remove_subscriber directly) so a SharedDataServerClient
        # that just became empty is also dropped from _SHARED_CLIENTS - see
        # release_bot_data_server_handle's docstring for why that can't live
        # on SharedDataServerClient itself (lock-ordering).
        release_bot_data_server_handle(*self._key, self._bot_id)

    def connected(self) -> bool:
        return self._shared._client.connected()

    def get_ohlcv(
        self, pair: str, timeframe: str, candle_type: str = "spot", limit: int | None = None
    ) -> "DataFrame | None":
        return self._shared._client.get_ohlcv(pair, timeframe, candle_type, limit)

    def get_funding_rate(self, pair: str) -> dict | None:
        return self._shared._client.get_funding_rate(pair)

    def get_trades(self, pair: str, limit: int | None = None) -> "DataFrame | None":
        return self._shared._client.get_trades(pair, limit)

    def get_ticker(self, pair: str) -> dict | None:
        return self._shared._client.get_ticker(pair)


class SharedDataServerClient:
    """One real ``DataServerClient`` connection, shared by every
    :class:`BotDataServerHandle` created for the same
    ``(host, port, exchange_name)`` key in this process."""

    def __init__(self, host: str, port: int, exchange_name: str) -> None:
        from VulcanTrader.data_server import DataServerClient

        self._key: tuple[str, int, str] = (host, port, exchange_name)
        self._lock = threading.Lock()
        # bot_id -> (pairs, want_funding_rate, want_trades, handle)
        self._subscribers: dict[str, tuple[list[str], bool, bool, BotDataServerHandle]] = {}
        client_id = f"shared-{exchange_name}-{os.getpid()}"
        self._client: "DataServerClient" = DataServerClient(host, port, exchange_name, client_id)
        self._client.on_ohlcv_push = self._dispatch_ohlcv
        self._client.on_funding_rate_push = self._dispatch_funding_rate
        self._client.on_trades_push = self._dispatch_trades
        self._client.start()
        logger.info(
            "SharedDataServerClient: connection started (client_id=%s, master=%s:%d)",
            client_id, host, port,
        )

    def handle_for(self, bot_id: str) -> BotDataServerHandle:
        return BotDataServerHandle(self, self._key, bot_id)

    def _reregister_locked(self) -> None:
        """Caller must hold self._lock."""
        pairs: set[str] = set()
        want_funding_rate = False
        want_trades = False
        for p, wf, wt, _handle in self._subscribers.values():
            pairs.update(p)
            want_funding_rate = want_funding_rate or wf
            want_trades = want_trades or wt
        self._client.register(
            sorted(pairs), want_funding_rate=want_funding_rate, want_trades=want_trades
        )

    def _update_subscriber(
        self, bot_id: str, pairs: list[str], want_funding_rate: bool, want_trades: bool,
        handle: BotDataServerHandle,
    ) -> None:
        with self._lock:
            self._subscribers[bot_id] = (list(pairs), want_funding_rate, want_trades, handle)
            self._reregister_locked()

    def _remove_subscriber(self, bot_id: str) -> bool:
        """Returns True if this was the last subscriber (client now stopped)."""
        with self._lock:
            self._subscribers.pop(bot_id, None)
            if not self._subscribers:
                self._client.stop()
                return True
            self._reregister_locked()
            return False

    def _dispatch_ohlcv(self, pair: str, timeframe: str, candle_type: str, df: "DataFrame") -> None:
        with self._lock:
            targets = [h for p, _wf, _wt, h in self._subscribers.values() if pair in p]
        for h in targets:
            if h.on_ohlcv_push is not None:
                h.on_ohlcv_push(pair, timeframe, candle_type, df)

    def _dispatch_funding_rate(self, pair: str, payload: dict) -> None:
        with self._lock:
            targets = [h for p, _wf, _wt, h in self._subscribers.values() if pair in p]
        for h in targets:
            if h.on_funding_rate_push is not None:
                h.on_funding_rate_push(pair, payload)

    def _dispatch_trades(self, pair: str, df: "DataFrame") -> None:
        with self._lock:
            targets = [h for p, _wf, _wt, h in self._subscribers.values() if pair in p]
        for h in targets:
            if h.on_trades_push is not None:
                h.on_trades_push(pair, df)


_SHARED_CLIENTS: dict[tuple[str, int, str], SharedDataServerClient] = {}
_SHARED_CLIENTS_LOCK = threading.Lock()


def get_bot_data_server_handle(
    host: str, port: int, exchange_name: str, bot_id: str
) -> BotDataServerHandle:
    """Get-or-create the process-wide :class:`SharedDataServerClient` for
    ``(host, port, exchange_name)`` and return this bot's own handle onto
    it. Safe to call concurrently from many bot threads (e.g. a whole fleet
    launching at once under ``single_process_mode``)."""
    key = (host, port, exchange_name)
    with _SHARED_CLIENTS_LOCK:
        shared = _SHARED_CLIENTS.get(key)
        if shared is None:
            shared = SharedDataServerClient(host, port, exchange_name)
            _SHARED_CLIENTS[key] = shared
        return shared.handle_for(bot_id)


def release_bot_data_server_handle(
    host: str, port: int, exchange_name: str, bot_id: str
) -> None:
    """Called from :meth:`BotDataServerHandle.stop`. Removes this bot from
    the shared subscriber map (re-registering the smaller union if others
    remain) and, if that leaves the :class:`SharedDataServerClient` empty,
    also drops it from the process-wide registry so a later bot targeting
    the same key gets a fresh connection instead of reusing a stopped one.
    Kept as a module-level function (rather than folded into
    ``SharedDataServerClient`` itself) because it needs
    ``_SHARED_CLIENTS_LOCK`` - the registry-level lock - which must always
    be acquired *before* any per-shared-client lock, never the reverse, to
    keep lock ordering consistent and deadlock-free."""
    key = (host, port, exchange_name)
    with _SHARED_CLIENTS_LOCK:
        shared = _SHARED_CLIENTS.get(key)
        if shared is None:
            return
        if shared._remove_subscriber(bot_id):
            del _SHARED_CLIENTS[key]
