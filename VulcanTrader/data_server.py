# =============================================================================
#  VulcanTrader  ::  OHLCV / funding-rate / orderflow collector & TCP cache
#  (data_server.py)
# =============================================================================
#
#  DataCollector
#  -------------
#  The collection engine. Pulls live candles (and optionally funding rates and
#  raw public trades, used to reconstruct orderflow) for a set of pairs and
#  keeps the on-disk cache (the same cache backtesting.py reads via
#  data/history/) continuously up to date, without running the full trading
#  bot (trader_bot.py). Can be used standalone (`--mode standalone`), or as
#  the engine behind either of two networked roles:
#
#      master      Runs its own DataCollector *and* listens on two TCP ports:
#                     * --port            (default 8720) - clients request
#                       cached OHLCV / funding-rate / trades (orderflow) data
#                       for an exchange+pair.
#                     * --subserver-port  (default 8721) - subservers connect
#                       here and push freshly-collected data to be merged
#                       into the master's cache.
#
#      subserver   Runs its own DataCollector (identical collection engine,
#                  identical config format) but opens no listening socket at
#                  all - clients cannot connect to it. Instead it dials out
#                  to a master's subserver port and forwards every batch of
#                  freshly-fetched data as it arrives.

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import queue
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

# On Windows, ccxt's aiohttp-based async transport is unreliable on the default
# ProactorEventLoop (spurious WinError 10054/1236 tracebacks, or the connection
# aborting outright - see VulcanTrader/bot.py for the same fix). Exchange.__init__
# creates its event loop via asyncio.new_event_loop(), which follows whatever
# policy is active at that point, so this must run before Exchange is imported/used.
if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

from pandas import DataFrame, concat, to_datetime

from VulcanTrader.constants import (
    DEFAULT_DATAFRAME_COLUMNS,
    DEFAULT_TRADES_COLUMNS,
    Config,
    ListPairsWithTimeframes,
    PairWithTimeframe,
)
from VulcanTrader.data.converter import clean_ohlcv_dataframe
from VulcanTrader.data.converter.trade_converter import (
    trades_df_remove_duplicates,
    trades_list_to_df,
)
from VulcanTrader.data.history.datahandlers import IDataHandler, get_datahandler
from VulcanTrader.enums import CandleType, TradingMode
from VulcanTrader.exchange.exchange import Exchange
from VulcanTrader.exchange.exchange_utils_timeframe import timeframe_to_seconds
from VulcanTrader.pairlist.pairlist_helpers import dynamic_expand_pairlist
from VulcanTrader.resolvers import ExchangeResolver
from VulcanTrader.util.exceptions import OperationalException


logger = logging.getLogger(__name__)

MAX_MESSAGE_BYTES = 64 * 1024 * 1024  # guard against a corrupt/hostile length header


# ---------------------------------------------------------------------------
#  Wire protocol: length-prefixed JSON frames over a plain TCP socket.
# ---------------------------------------------------------------------------


def send_msg(sock: socket.socket, obj: dict) -> None:
    payload = json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(65536, n - len(buf)))
        if not chunk:
            raise ConnectionError("peer closed connection")
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock: socket.socket) -> dict | None:
    """Read one frame. Returns None on a clean disconnect."""
    try:
        header = _recv_exact(sock, 4)
    except ConnectionError:
        return None
    (length,) = struct.unpack(">I", header)
    if length > MAX_MESSAGE_BYTES:
        raise ValueError(f"message of {length} bytes exceeds the {MAX_MESSAGE_BYTES}-byte limit")
    payload = _recv_exact(sock, length)
    return json.loads(payload.decode("utf-8"))


def df_to_wire(df: DataFrame) -> dict:
    d = df[DEFAULT_DATAFRAME_COLUMNS].copy()
    # dt.as_unit("ms") first, *then* cast to int64: pandas datetime64 columns are
    # not always nanosecond-resolution (2.x can hand back datetime64[us] or [s]
    # depending on how the column was built), so a hardcoded `.astype("int64") //
    # 1_000_000` silently assumed ns and produced garbage (seconds mislabeled as
    # ms, landing everything near the 1970 epoch) whenever the column was actually
    # microsecond-resolution. as_unit() normalizes to ms regardless of the source
    # resolution, so the int64 cast that follows is always already in ms.
    d["date"] = d["date"].dt.as_unit("ms").astype("int64")
    return {"columns": DEFAULT_DATAFRAME_COLUMNS, "rows": d.values.tolist()}


def wire_to_df(payload: dict) -> DataFrame:
    columns = payload.get("columns", DEFAULT_DATAFRAME_COLUMNS)
    df = DataFrame(payload["rows"], columns=columns)
    df["date"] = to_datetime(df["date"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df


def trades_to_wire(df: DataFrame) -> dict:
    """`timestamp` is already the epoch-ms source of truth for trades; `date` is
    purely derived from it, so it's dropped here and recomputed by wire_to_trades()."""
    return {"columns": DEFAULT_TRADES_COLUMNS, "rows": df[DEFAULT_TRADES_COLUMNS].values.tolist()}


def wire_to_trades(payload: dict) -> DataFrame:
    return trades_list_to_df(payload["rows"], convert=True)


def candle_type_value(candle_type: CandleType | str) -> str:
    """Wire representation of a CandleType. Not `str(candle_type)` - CandleType's
    __str__ returns `.name.lower()`, which doesn't round-trip (e.g. PREMIUMINDEX ->
    "premiumindex" instead of the real value "premiumIndex")."""
    if isinstance(candle_type, CandleType):
        return candle_type.value
    return CandleType.from_string(candle_type).value


# ---------------------------------------------------------------------------
#  DataCollector: the collection engine, shared by all three modes.
# ---------------------------------------------------------------------------


class DataCollector:
    """Continuously refreshes OHLCV (and optionally funding rate / trades) from
    the exchange and caches it to disk."""

    def __init__(
        self,
        config: Config,
        exchange: Exchange | None = None,
        *,
        on_ohlcv: Callable[[PairWithTimeframe, DataFrame], None] | None = None,
        on_funding_rate: Callable[[str, dict], None] | None = None,
        on_trades: Callable[[str, DataFrame], None] | None = None,
        collect_funding_rate: bool | None = None,
        funding_rate_interval: float = 300.0,
        collect_funding_rate_history: bool | None = None,
        persist_to_disk: bool = True,
        max_consecutive_failures: int = 5,
        reload_cooldown: float = 30.0,
        max_pairs_per_batch: int | None = None,
    ) -> None:
        """
        :param on_ohlcv: called with (pair_key, newly_fetched_df) whenever a refresh
            actually produced candles beyond what was already cached. Used by
            master/subserver mode to mirror updates into a shared/networked cache.
        :param on_funding_rate: called with (pair, ccxt_funding_rate_dict) after each
            funding-rate poll.
        :param on_trades: called with (pair, newly_fetched_trades_df) after each
            trades/orderflow poll (only runs when `exchange.use_public_trades` is
            set in config, matching the live bot's DataProvider.refresh_latest_trades).
        :param collect_funding_rate: poll `exchange.fetch_funding_rate` for every pair.
            Defaults to True when `trading_mode` is "futures" and `on_funding_rate`
            is set (no point polling with nobody to consume it).
        :param funding_rate_interval: minimum seconds between funding-rate polls.
        :param collect_funding_rate_history: also fetch/persist FULL funding-rate
            HISTORY via the exchange's `fetchFundingRateHistory` - added to
            `self.pairlist` as `(pair, funding_fee_timeframe, CandleType.FUNDING_RATE)`
            entries, so it flows through the exact same `collect_once`/`_persist`/
            `on_ohlcv` path as regular OHLCV (including the same feather layout:
            `datadir/futures/<PAIR>-<tf>-funding_rate.feather`). Distinct from
            `collect_funding_rate` above, which only polls the single CURRENT rate
            for live funding-fee math and never touches disk. Refetch cadence is
            throttled by the exchange's own `_now_is_time_to_refresh` per pair/
            timeframe (same mechanism regular OHLCV uses), so this is safe to poll
            every `collect_once()` tick without hammering the API. Defaults to
            `persist_to_disk and trading_mode == "futures"` (no point historizing
            something nothing will ever persist, and spot markets have no funding
            rate to fetch in the first place).
        :param persist_to_disk: write merged candles back via the datahandler. Set to
            False when a caller (e.g. DataCache) already persists via `on_ohlcv`, to
            avoid two independent writers touching the same datadir.
        :param max_consecutive_failures: after this many back-to-back failed OHLCV
            refreshes, close and recreate the Exchange connection - the underlying
            ccxt/websocket state can get wedged in ways a plain retry never recovers
            from (e.g. a dead internal event loop), so this is the fallback of last
            resort short of restarting the whole process.
        :param reload_cooldown: minimum seconds between exchange-reload attempts, so
            a persistently-down exchange doesn't get hammered with reconnects.
        :param max_pairs_per_batch: CEILING on how many PAIRS (not pair/timeframe
            combinations) a single `collect_once()` tick fetches via one
            `refresh_latest_ohlcv()` call, e.g. `EXCHANGE_MAX_SAFE_PAIRS` for
            this exchange. When the assigned pairlist has more pairs than the
            collector's current (adaptively ramped - see `_record_batch_outcome`)
            batch size, `collect_once()` fetches a rotating slice each tick
            instead of the whole list - the excess isn't dropped, it's just
            refreshed on a slower cadence. The collector starts well below this
            ceiling and only grows into it as ticks come back clean; `None`
            (default) disables batching AND ramping entirely - the full
            pairlist is always fetched in one call, matching standalone/probe
            usage.
        """
        self.config = config
        self.exchange = exchange or ExchangeResolver.load_exchange(config, validate=False)
        # validate_config() is the only normal caller of _set_startup_candle_count(),
        # and it never runs with validate=False (deliberate - full validation assumes
        # a strategy/pairlist context this collector doesn't have). Without it,
        # self._startup_candle_count is simply missing, and _process_ohlcv_df's
        # cache-merge path (hit as soon as a pair already has klines to merge
        # against, i.e. every tick after the first) raises AttributeError for the
        # *entire* refresh_latest_ohlcv() call - discovered the same way in
        # VulcanTrader/ratelimit_probe.py, fixed there the same way.
        self.exchange._set_startup_candle_count(config)

        self.datadir: Path = config["datadir"]
        self.data_format: str = config.get("dataformat_ohlcv", "feather")
        self.candle_type: CandleType = config.get("candle_type_def", CandleType.SPOT)
        self.data_handler: IDataHandler = get_datahandler(self.datadir, self.data_format)
        self.persist_to_disk = persist_to_disk

        self.on_ohlcv = on_ohlcv
        self.on_funding_rate = on_funding_rate
        self.on_trades = on_trades

        self.collect_funding_rate = (
            collect_funding_rate
            if collect_funding_rate is not None
            else (on_funding_rate is not None and config.get("trading_mode") == "futures")
        )
        self.funding_rate_interval = funding_rate_interval
        self._last_funding_fetch = 0.0
        # Index into `pairs` where the next live-funding-rate rotation batch
        # starts - see _next_funding_rate_batch. Independent of
        # _rotation_offset (OHLCV's own rotation position): the two batch
        # different things on different cadences, so tying them together
        # would make each one's rotation depend on unrelated timing.
        self._funding_rate_rotation_offset = 0
        # pair -> consecutive non-rate-limit fetch_funding_rate() failures (see
        # _collect_funding_rates) - reset to 0 on any success, never persisted
        # across a process restart (deliberately: an exchange could add support
        # for a pair later, or a genuine outage could resolve).
        self._funding_rate_unsupported: dict[str, int] = {}

        self.collect_trades = bool(config.get("exchange", {}).get("use_public_trades", False))

        self.max_consecutive_failures = max_consecutive_failures
        self.reload_cooldown = reload_cooldown
        self._consecutive_failures = 0
        self._last_reload_attempt = 0.0

        self.max_pairs_per_batch = max_pairs_per_batch
        # Adaptively-ramped current batch size (see _record_batch_outcome) -
        # starts at a conservative floor and grows toward max_pairs_per_batch,
        # rather than requesting the full ceiling from the very first tick.
        # None (not batching/ramping at all) when max_pairs_per_batch is None.
        self._batch_cap = min(_RAMP_FLOOR, max_pairs_per_batch) if max_pairs_per_batch else None
        self._ramp_cooldown_until = 0.0
        # Index into `self.pairs` where the next rotation batch starts - only
        # touched from collect_once(), which always runs on a single thread
        # (run_forever()'s loop, or one-off manual calls), so it needs no lock
        # of its own beyond the _pairs_lock snapshot collect_once() already
        # takes of self.pairs before calling _next_batch_pairlist().
        self._rotation_offset = 0
        # Counts collect_once() calls - see _FUNDING_HISTORY_TICK_INTERVAL /
        # collect_once()'s own use of it below.
        self._tick_count = 0

        timeframes = config.get("timeframes") or [config.get("timeframe", "5m")]
        self.timeframes: list[str] = list(timeframes)

        pairs = config.get("pairs")
        if not pairs:
            available = list(
                self.exchange.get_markets(tradable_only=True, active_only=True).keys()
            )
            pairs = dynamic_expand_pairlist(config, available)
        self.pairs: list[str] = pairs

        self.collect_funding_rate_history = (
            collect_funding_rate_history
            if collect_funding_rate_history is not None
            else (persist_to_disk and config.get("trading_mode") == "futures")
        )
        self.funding_rate_history_timeframe: str | None = None
        if self.collect_funding_rate_history:
            try:
                self.funding_rate_history_timeframe = self.exchange.get_option("funding_fee_timeframe")
            except Exception:
                logger.exception(
                    "Failed to resolve funding_fee_timeframe - disabling funding-rate "
                    "history collection for this exchange"
                )
                self.collect_funding_rate_history = False

        self.pairlist: ListPairsWithTimeframes = self._build_pairlist(self.pairs)
        # Guards self.pairs/self.pairlist/self._cache against concurrent access from
        # set_pairs(), which master/subserver mode call from a different thread than
        # the one running collect_once()/run_forever() (see WorkDistributor).
        self._pairs_lock = threading.Lock()

        # In-memory copy of each pair/timeframe's on-disk history, seeded once at
        # startup so every tick only has to merge the newly-fetched candles. A
        # single corrupted/unreadable cache file must not prevent startup - fall
        # back to an empty frame for that one pair/timeframe and keep going.
        self._cache: dict[PairWithTimeframe, DataFrame] = {}
        for pair_key in self.pairlist:
            pair, tf, c_type = pair_key
            try:
                self._cache[pair_key] = self.data_handler.ohlcv_load(
                    pair,
                    timeframe=tf,
                    candle_type=c_type,
                    fill_missing=False,
                    drop_incomplete=False,
                    warn_no_data=False,
                )
            except Exception:
                logger.exception(
                    "Failed to load cached OHLCV for %s - starting with an empty cache for it", pair_key
                )
                self._cache[pair_key] = DataFrame()

        self._stop = False
        # Set by set_pairs() whenever the pair assignment actually changes, so
        # run_forever()'s sleep can wake up and collect immediately instead of
        # sitting out a stale sleep interval computed back when this collector
        # had a different (e.g. empty) pairlist - see run_forever()'s sleep loop.
        self._wake_event = threading.Event()

        logger.info(
            "DataCollector initialised: %d pairs x %d timeframes -> %d combinations "
            "(ws_enabled=%s, datadir=%s, format=%s, funding_rate=%s, funding_rate_history=%s, "
            "persist_to_disk=%s, trades=%s)",
            len(self.pairs),
            len(self.timeframes),
            len(self.pairlist),
            self.exchange._exchange_ws is not None,
            self.datadir,
            self.data_format,
            self.collect_funding_rate,
            self.collect_funding_rate_history,
            self.persist_to_disk,
            self.collect_trades,
        )

    def stop(self) -> None:
        self._stop = True

    def _build_pairlist(self, pairs: list[str]) -> ListPairsWithTimeframes:
        """Standard `(pair, tf, candle_type)` combos for every configured timeframe,
        plus one `(pair, funding_fee_timeframe, CandleType.FUNDING_RATE)` entry per
        pair when funding-rate history collection is enabled (see `__init__`)."""
        pairlist: ListPairsWithTimeframes = [
            (pair, tf, self.candle_type) for pair in pairs for tf in self.timeframes
        ]
        if self.collect_funding_rate_history and self.funding_rate_history_timeframe:
            pairlist += [
                (pair, self.funding_rate_history_timeframe, CandleType.FUNDING_RATE)
                for pair in pairs
            ]
        return pairlist

    def set_pairs(self, pairs: list[str]) -> None:
        """Replace the set of pairs this collector actively refreshes, seeding cache
        for any newly-added pair/timeframe combinations from disk and dropping cache
        entries for removed ones. Used by master/subserver mode to rebalance work
        across connected subservers (see WorkDistributor) and to reflect the union
        of every registered trading-bot client's interest (see
        ClientInterestRegistry) - safe to call from any thread while
        collect_once()/run_forever() are running on another."""
        new_pairs = list(pairs)
        new_pairlist: ListPairsWithTimeframes = self._build_pairlist(new_pairs)
        with self._pairs_lock:
            if new_pairs == self.pairs:
                return
            new_keys = set(new_pairlist)
            old_keys = set(self.pairlist)
            for pair_key in new_keys - old_keys:
                pair, tf, c_type = pair_key
                try:
                    self._cache[pair_key] = self.data_handler.ohlcv_load(
                        pair,
                        timeframe=tf,
                        candle_type=c_type,
                        fill_missing=False,
                        drop_incomplete=False,
                        warn_no_data=False,
                    )
                except Exception:
                    logger.exception(
                        "Failed to load cached OHLCV for newly-assigned %s - starting empty",
                        pair_key,
                    )
                    self._cache[pair_key] = DataFrame()
            for pair_key in old_keys - new_keys:
                self._cache.pop(pair_key, None)
            self.pairs = new_pairs
            self.pairlist = new_pairlist
        logger.info(
            "DataCollector pair assignment updated: now tracking %d pair(s) (%d combination(s))",
            len(new_pairs), len(new_pairlist),
        )
        # Wake run_forever()'s sleep immediately - newly-assigned pairs (e.g. a
        # trading-bot client that just registered) shouldn't have to wait out
        # up to a full timeframe interval of a sleep computed against the OLD
        # pairlist before their first real collect_once() happens.
        self._wake_event.set()

    def _persist(self, pair_key: PairWithTimeframe, new_data: DataFrame) -> bool:
        """Merge freshly-fetched candles into the in-memory cache. Returns True if
        new candles actually landed (i.e. this wasn't a no-op refresh)."""
        if new_data is None or new_data.empty:
            return False
        pair, timeframe, candle_type = pair_key
        existing = self._cache.get(pair_key)
        if existing is None or existing.empty:
            merged = new_data
        else:
            merged = clean_ohlcv_dataframe(
                concat([existing, new_data], axis=0),
                timeframe,
                pair,
                fill_missing=False,
                drop_incomplete=False,
            )
        if existing is not None and not existing.empty and len(merged) == len(existing):
            # Nothing new landed this tick - skip the disk write and the hook.
            return False
        self._cache[pair_key] = merged
        if self.persist_to_disk:
            self.data_handler.ohlcv_store(pair, timeframe, data=merged, candle_type=candle_type)
        if self.on_ohlcv is not None:
            try:
                self.on_ohlcv(pair_key, new_data)
            except Exception:
                logger.exception("on_ohlcv hook failed for %s", pair_key)
        return True

    def _next_batch_pairlist(
        self, pairlist: ListPairsWithTimeframes, pairs: list[str]
    ) -> ListPairsWithTimeframes:
        """Return the slice of `pairlist` to fetch THIS tick, using the current
        adaptively-ramped `_batch_cap` (see `_record_batch_outcome`), not the
        static `max_pairs_per_batch` ceiling directly. When `pairs` fits within
        `_batch_cap`, that's all of `pairlist` (no rotation needed). Otherwise,
        advance a rotating window over `pairs` by `_batch_cap` each call - so
        pair N always gets picked up again within `ceil(len(pairs) /
        _batch_cap)` ticks, never left out indefinitely, while any single
        `refresh_latest_ohlcv()` call this collector makes still never exceeds
        its current (ramped) batch size."""
        if not self._batch_cap or len(pairs) <= self._batch_cap:
            self._rotation_offset = 0
            return pairlist
        n = len(pairs)
        start = self._rotation_offset % n
        end = start + self._batch_cap
        batch_pairs = set(pairs[start:end]) if end <= n else set(pairs[start:] + pairs[: end - n])
        self._rotation_offset = end % n
        return [pair_key for pair_key in pairlist if pair_key[0] in batch_pairs]

    def _record_batch_outcome(self, *, rate_limited: bool) -> None:
        """Adjust `_batch_cap` after one OHLCV fetch: cut it back hard (and pause
        ramping for `_RAMP_COOLDOWN_S`) the moment a rate-limit signal is seen,
        otherwise grow it by `_RAMP_STEP` toward `max_pairs_per_batch` once the
        cooldown has elapsed. No-op when batching/ramping is disabled
        (`max_pairs_per_batch` is None)."""
        if not self.max_pairs_per_batch:
            return
        if rate_limited:
            old = self._batch_cap
            self._batch_cap = max(_RAMP_FLOOR, (self._batch_cap or self.max_pairs_per_batch) // 2)
            self._ramp_cooldown_until = time.time() + _RAMP_COOLDOWN_S
            logger.warning(
                "Rate-limit signal detected this tick - cutting OHLCV batch size "
                "%s -> %d pair(s) and pausing ramp-up for %.0fs",
                old, self._batch_cap, _RAMP_COOLDOWN_S,
            )
        elif (
            self._batch_cap is not None
            and self._batch_cap < self.max_pairs_per_batch
            and time.time() >= self._ramp_cooldown_until
        ):
            old = self._batch_cap
            self._batch_cap = min(self.max_pairs_per_batch, self._batch_cap + _RAMP_STEP)
            if self._batch_cap != old:
                logger.info(
                    "Clean tick - growing OHLCV batch size %d -> %d pair(s) (ceiling %d)",
                    old, self._batch_cap, self.max_pairs_per_batch,
                )

    def collect_once(self) -> dict[PairWithTimeframe, DataFrame]:
        """Fetch the latest candles (and funding rate / trades, if enabled) for all
        configured pairs/timeframes and cache them. OHLCV fetching is capped at
        the current adaptively-ramped batch size per tick (rotating - see
        `_next_batch_pairlist` / `_record_batch_outcome`); funding-rate/trades
        collection always covers every assigned pair since those are fetched
        one-by-one, not in one concurrent burst, so they aren't subject to the
        same per-call concurrency limit OHLCV batching exists to respect."""
        # Snapshot under the lock so a concurrent set_pairs() (master/subserver
        # rebalancing) can't hand us a half-updated view mid-tick.
        with self._pairs_lock:
            pairlist = list(self.pairlist)
            pairs = list(self.pairs)

        self._tick_count += 1
        if self.collect_funding_rate_history and self._tick_count % _FUNDING_HISTORY_TICK_INTERVAL != 1:
            # Funding-rate-history entries (see _build_pairlist) ride along in
            # `pairlist` at the SAME (pair, funding_fee_timeframe,
            # CandleType.FUNDING_RATE) granularity as regular OHLCV, roughly
            # doubling every tick's request count when included every time
            # (confirmed live: a 10-pair/20-entry batch - regular candles +
            # funding history - took ~4.5 minutes under real exchange latency,
            # with zero rate-limit rejections logged; it was just genuinely
            # slow per request, and every extra entry is proportionally more
            # wall-clock time). Funding-rate HISTORY changes far slower than
            # live candles need to refresh, so it doesn't need every tick -
            # only include it on every _FUNDING_HISTORY_TICK_INTERVAL-th tick,
            # cutting its contribution to average batch size by that same
            # factor without slowing down its own eventual coverage by more
            # than that (still rotates through every assigned pair, just less
            # often per pair).
            pairlist = [pk for pk in pairlist if pk[2] != CandleType.FUNDING_RATE]

        batch_pairlist = self._next_batch_pairlist(pairlist, pairs)
        if len(batch_pairlist) != len(pairlist):
            logger.info(
                "Rotating OHLCV batch: fetching %d/%d pair(s) this tick (offset=%d, cap=%s)",
                len(batch_pairlist), len(pairlist), self._rotation_offset, self._batch_cap,
            )

        rate_limit_cap = _RateLimitLogCapture()
        exchange_logger = logging.getLogger("VulcanTrader.exchange")
        exchange_logger.addHandler(rate_limit_cap)
        try:
            results = self.exchange.refresh_latest_ohlcv(batch_pairlist)
            self._consecutive_failures = 0
        except Exception:
            self._consecutive_failures += 1
            logger.exception(
                "refresh_latest_ohlcv failed (%d/%d consecutive failures)",
                self._consecutive_failures,
                self.max_consecutive_failures,
            )
            results = {}
            if self._consecutive_failures >= self.max_consecutive_failures:
                now = time.time()
                if now - self._last_reload_attempt >= self.reload_cooldown:
                    self._last_reload_attempt = now
                    self._reload_exchange()
        finally:
            exchange_logger.removeHandler(rate_limit_cap)

        if batch_pairlist:
            self._record_batch_outcome(rate_limited=rate_limit_cap.hit)

        for pair_key, df in results.items():
            try:
                self._persist(pair_key, df)
            except Exception:
                logger.exception("Failed to persist %s", pair_key)

        if self.collect_funding_rate:
            now = time.time()
            if now - self._last_funding_fetch >= self.funding_rate_interval:
                self._last_funding_fetch = now
                funding_batch = self._next_funding_rate_batch(pairs)
                if len(funding_batch) != len(pairs):
                    logger.info(
                        "Rotating live funding-rate batch: fetching %d/%d pair(s) this tick",
                        len(funding_batch), len(pairs),
                    )
                self._collect_funding_rates(funding_batch)

        if self.collect_trades:
            try:
                self._collect_trades(pairlist)
            except Exception:
                logger.exception("Failed to collect trades/orderflow data")

        return results

    def _next_funding_rate_batch(self, pairs: list[str]) -> list[str]:
        """Same rotation shape as _next_batch_pairlist, applied to the LIVE
        current-funding-rate fetch (_collect_funding_rates) instead of OHLCV.

        Before this, _collect_funding_rates always fetched EVERY pair
        sequentially, uncapped, every funding_rate_interval - with no per-call
        concurrency limit protecting it the way OHLCV's max_pairs_per_batch
        does. Under real exchange congestion this meant one funding-rate
        sweep (44 pairs, each up to 5 rapid retries with NO backoff between
        them - RateLimitExceeded isn't DDosProtection, see common.py's
        retrier) could itself take upwards of ten minutes - and since
        collect_once() calls this AFTER the OHLCV fetch, in the SAME tick,
        that single slow sweep blocked every subsequent OHLCV tick from ever
        starting for its entire duration (confirmed live: zero OHLCV rotation
        activity for 10+ minutes while funding-rate fetches kept retrying).
        Capping and rotating this exactly like OHLCV fixes that: any one
        tick's funding-rate work is now bounded by the same adaptive
        _batch_cap, so it can never again starve OHLCV collection - live
        funding rate is refreshed less often per pair when capacity is
        tight, same tradeoff OHLCV already makes."""
        cap = self._batch_cap or len(pairs)
        if not pairs or len(pairs) <= cap:
            self._funding_rate_rotation_offset = 0
            return pairs
        n = len(pairs)
        start = self._funding_rate_rotation_offset % n
        end = start + cap
        batch = pairs[start:end] if end <= n else pairs[start:] + pairs[: end - n]
        self._funding_rate_rotation_offset = end % n
        return batch

    def _collect_funding_rates(self, pairs: list[str]) -> None:
        for pair in pairs:
            if self._funding_rate_unsupported.get(pair, 0) >= _FUNDING_RATE_MAX_FAILURES:
                continue
            try:
                rate = self.exchange.fetch_funding_rate(pair)
            except Exception as e:
                # Distinguish "genuinely no funding-rate data for this pair" (e.g.
                # Hyperliquid's synthetic stock-tracking markets returning
                # NullResponse, confirmed live for XYZ-SILVER/XYZ-GOOGL) from an
                # ordinary transient rate-limit hit. Retrying the former forever,
                # every funding_rate_interval, wastes request budget on a pair
                # that will never succeed - and that wasted budget competes with
                # every other exchange call this process (and every other bot
                # relying on it) makes. A rate-limit failure still gets retried
                # at the normal cadence next time - only a run of genuinely
                # unrelated failures for the SAME pair backs off.
                if not any(marker in str(e).lower() for marker in _RATE_LIMIT_LOG_MARKERS):
                    n = self._funding_rate_unsupported.get(pair, 0) + 1
                    self._funding_rate_unsupported[pair] = n
                    if n >= _FUNDING_RATE_MAX_FAILURES:
                        logger.warning(
                            "Funding rate for %s failed %d times with a non-rate-limit "
                            "error - giving up on it (exchange likely doesn't support "
                            "funding rate for this pair): %s",
                            pair, n, e,
                        )
                # A plain one-line warning, not logger.exception(): the exception's own
                # message (e.g. "Could not get funding rate due to NullResponse.
                # Message: ...") is already fully self-describing - the full traceback
                # through 6 stacked retry-wrapper frames on top of it (confirmed live -
                # a "During handling of the above exception..." chain from the ccxt
                # NullResponse through common.py's retrier) added no diagnostic value,
                # just noise, on every one of the up-to-_FUNDING_RATE_MAX_FAILURES
                # attempts a pair gets before the backoff above silences it - and that
                # counter resets on every process restart, so this replayed on every
                # single restart for the exchange's permanently-unsupported pairs.
                logger.warning("Failed to fetch funding rate for %s: %s", pair, e)
                continue
            self._funding_rate_unsupported.pop(pair, None)
            if self.on_funding_rate is not None:
                try:
                    self.on_funding_rate(pair, rate)
                except Exception:
                    logger.exception("on_funding_rate hook failed for %s", pair)

    def _collect_trades(self, pairlist: ListPairsWithTimeframes) -> None:
        # Exchange.refresh_latest_trades() maintains its own rolling "<pair>-cached"
        # trades file on disk independently of DataCollector's own persistence path.
        results = self.exchange.refresh_latest_trades(pairlist)
        for pair_key, df in results.items():
            if df is None or df.empty:
                continue
            pair = pair_key[0]
            if self.on_trades is not None:
                try:
                    self.on_trades(pair, df)
                except Exception:
                    logger.exception("on_trades hook failed for %s", pair)

    def _reload_exchange(self) -> None:
        """Last-resort fallback after repeated refresh failures: some ccxt/websocket
        failure modes (a dead internal event loop, a wedged connection) can only be
        recovered from by discarding the Exchange instance and building a fresh one."""
        logger.warning(
            "Reloading the exchange connection after %d consecutive refresh failures...",
            self._consecutive_failures,
        )
        try:
            self.exchange.close()
        except Exception:
            logger.exception("Error closing the stale exchange instance (continuing anyway)")
        try:
            self.exchange = ExchangeResolver.load_exchange(self.config, validate=False)
            self._consecutive_failures = 0
            logger.info("Exchange connection reloaded successfully.")
        except Exception:
            logger.exception(
                "Failed to reload the exchange connection - will keep retrying refreshes "
                "and re-attempt a reload after the cooldown"
            )

    def run_forever(
        self, poll_interval: float | None = None, stop_event: threading.Event | None = None
    ) -> None:
        """Poll continuously. Stops when either `self.stop()` is called or
        `stop_event` is set - the latter lets an external supervisor request a
        stop across collector instances that get recreated on restart.

        The default cadence (no explicit poll_interval) is min(60s, shortest
        timeframe), NOT one full timeframe: a fixed timeframe-length sleep is
        unaligned to candle close times, so the cache drifted up to ~2x the
        timeframe stale right before each tick (observed live: 15m timeframe,
        candles up to 36 minutes old just before the next poll - past the
        `timeframe*2 + outdated_offset` staleness limit trading bots apply, so
        every connected bot fell back to hammering the exchange directly, the
        exact thing this collector exists to prevent). Frequent polling is
        free between candle closes: Exchange.refresh_latest_ohlcv's own
        per-pair `_now_is_time_to_refresh` gate skips the API call entirely
        until a new candle is actually due, so a 60s cadence just means each
        fresh candle is picked up (and pushed to clients) within ~a minute of
        closing, at no extra API cost."""
        logger.info("Starting continuous data collection. Ctrl+C to stop.")
        shortest_tf_secs = min(timeframe_to_seconds(tf) for tf in self.timeframes)
        if not poll_interval:
            poll_interval = min(60.0, float(shortest_tf_secs))

        def _should_stop() -> bool:
            return self._stop or (stop_event is not None and stop_event.is_set())

        while not _should_stop():
            # Cleared here (not after the sleep) so a set_pairs() call that lands
            # while collect_once() is already running - after it snapshotted the
            # pairlist for this cycle - still wakes the sleep below early instead
            # of being silently absorbed by a clear() that happens afterward.
            self._wake_event.clear()
            t0 = time.time()
            try:
                self.collect_once()
            except Exception:
                logger.exception("Error while refreshing data")

            # Account for however long collect_once() itself took.
            sleep_for = max(1.0, poll_interval - (time.time() - t0))
            slept = 0.0
            while slept < sleep_for and not _should_stop():
                step = min(1.0, sleep_for - slept)
                # A pair-assignment change (e.g. a trading-bot client just
                # registered) wakes this immediately instead of leaving it to
                # sleep out an interval computed against the OLD pairlist - see
                # set_pairs()'s own _wake_event.set() call.
                if self._wake_event.wait(step):
                    break
                slept += step
        logger.info("Data collector stopped.")

    def close(self) -> None:
        self.exchange.close()


# ---------------------------------------------------------------------------
#  Shared cache: in-memory, optionally mirrored to disk via a datahandler.
# ---------------------------------------------------------------------------

OhlcvKey = tuple[str, str, str, str]  # exchange, pair, timeframe, candle_type
FundingKey = tuple[str, str]  # exchange, pair
TradesKey = tuple[str, str]  # exchange, pair
TickerKey = tuple[str, str]  # exchange, pair


class DataCache:
    """Thread-safe in-memory cache of OHLCV/funding-rate/trades data, with optional
    disk persistence via the same datahandlers the backtester reads from."""

    def __init__(
        self,
        data_handler: IDataHandler | None = None,
        trades_data_handler: IDataHandler | None = None,
        trading_mode: TradingMode = TradingMode.SPOT,
    ) -> None:
        self._lock = threading.RLock()
        self._ohlcv: dict[OhlcvKey, DataFrame] = {}
        self._funding: dict[FundingKey, dict] = {}
        self._trades: dict[TradesKey, DataFrame] = {}
        # (ticker, fetched_at monotonic-clock timestamp) - unlike OHLCV/funding/
        # trades, tickers are for LIVE entry/exit pricing (see TickerFetcher /
        # Exchange.get_rate), so staleness has to be checked explicitly by the
        # caller (get_ticker's max_age_s) rather than just "present = usable".
        self._tickers: dict[TickerKey, tuple[dict, float]] = {}
        self._data_handler = data_handler
        self._trades_data_handler = trades_data_handler
        self._trading_mode = trading_mode

    # -- OHLCV --------------------------------------------------------------

    def merge_ohlcv(
        self, exchange: str, pair: str, timeframe: str, candle_type: str, new_df: DataFrame
    ) -> bool:
        """Merge freshly-fetched candles in. Returns True if new data landed."""
        if new_df is None or new_df.empty:
            return False
        key: OhlcvKey = (exchange, pair, timeframe, candle_type)
        with self._lock:
            existing = self._ohlcv.get(key)
            if existing is None and self._data_handler is not None:
                try:
                    existing = self._data_handler.ohlcv_load(
                        pair,
                        timeframe=timeframe,
                        candle_type=CandleType(candle_type),
                        fill_missing=False,
                        drop_incomplete=False,
                        warn_no_data=False,
                    )
                except Exception:
                    logger.exception(
                        "Failed to load on-disk OHLCV for %s - merging against an empty base", key
                    )
                    existing = None
            if existing is None or existing.empty:
                merged = new_df
                changed = True
            else:
                merged = clean_ohlcv_dataframe(
                    concat([existing, new_df], axis=0),
                    timeframe,
                    pair,
                    fill_missing=False,
                    drop_incomplete=False,
                )
                changed = len(merged) != len(existing)
            # Always keep the in-memory merge, even if the disk write below fails -
            # a persistence hiccup must not lose data that's already been merged.
            self._ohlcv[key] = merged
            if changed and self._data_handler is not None:
                try:
                    self._data_handler.ohlcv_store(
                        pair, timeframe, data=merged, candle_type=CandleType(candle_type)
                    )
                except Exception:
                    logger.exception(
                        "Failed to persist OHLCV for %s to disk - still cached in memory", key
                    )
            return changed

    def get_ohlcv(
        self, exchange: str, pair: str, timeframe: str, candle_type: str, limit: int | None = None
    ) -> DataFrame | None:
        key: OhlcvKey = (exchange, pair, timeframe, candle_type)
        with self._lock:
            df = self._ohlcv.get(key)
            if df is None and self._data_handler is not None:
                try:
                    df = self._data_handler.ohlcv_load(
                        pair,
                        timeframe=timeframe,
                        candle_type=CandleType(candle_type),
                        fill_missing=False,
                        drop_incomplete=False,
                        warn_no_data=False,
                    )
                except Exception:
                    logger.exception("Failed to load on-disk OHLCV for %s", key)
                    df = None
                if df is not None and not df.empty:
                    self._ohlcv[key] = df
            if df is None or df.empty:
                return None
            return df.tail(limit) if limit else df

    # -- Funding rate ---------------------------------------------------------

    def set_funding_rate(self, exchange: str, pair: str, payload: dict) -> None:
        with self._lock:
            self._funding[(exchange, pair)] = payload

    def get_funding_rate(self, exchange: str, pair: str) -> dict | None:
        with self._lock:
            return self._funding.get((exchange, pair))

    # -- Trades / orderflow ---------------------------------------------------

    def merge_trades(self, exchange: str, pair: str, new_df: DataFrame) -> bool:
        """Merge freshly-fetched trades in (deduped on timestamp+id, same rule the
        exchange itself uses). Returns True if new data landed."""
        if new_df is None or new_df.empty:
            return False
        key: TradesKey = (exchange, pair)
        with self._lock:
            existing = self._trades.get(key)
            if existing is None and self._trades_data_handler is not None:
                try:
                    existing = self._trades_data_handler.trades_load(pair, self._trading_mode)
                except Exception:
                    logger.exception(
                        "Failed to load on-disk trades for %s - merging against an empty base", key
                    )
                    existing = None
            if existing is None or existing.empty:
                merged = new_df
                changed = True
            else:
                merged = trades_df_remove_duplicates(concat([existing, new_df], axis=0))
                merged = merged.sort_values("timestamp").reset_index(drop=True)
                changed = len(merged) != len(existing)
            # Always keep the in-memory merge, even if the disk write below fails.
            self._trades[key] = merged
            if changed and self._trades_data_handler is not None:
                try:
                    self._trades_data_handler.trades_store(
                        pair, merged[DEFAULT_TRADES_COLUMNS], self._trading_mode
                    )
                except Exception:
                    logger.exception(
                        "Failed to persist trades for %s to disk - still cached in memory", key
                    )
            return changed

    def get_trades(self, exchange: str, pair: str, limit: int | None = None) -> DataFrame | None:
        key: TradesKey = (exchange, pair)
        with self._lock:
            df = self._trades.get(key)
            if df is None and self._trades_data_handler is not None:
                try:
                    df = self._trades_data_handler.trades_load(pair, self._trading_mode)
                except Exception:
                    logger.exception("Failed to load on-disk trades for %s", key)
                    df = None
                if df is not None and not df.empty:
                    self._trades[key] = df
            if df is None or df.empty:
                return None
            return df.tail(limit) if limit else df

    # -- Ticker (live entry/exit price snapshot) -----------------------------

    def set_ticker(self, exchange: str, pair: str, ticker: dict) -> None:
        with self._lock:
            self._tickers[(exchange, pair)] = (ticker, time.time())

    def get_ticker(self, exchange: str, pair: str, max_age_s: float) -> dict | None:
        """Returns None if there's no cached ticker for this pair, or the
        cached one is older than `max_age_s` - the caller (TickerFetcher)
        treats either case identically: "not fresh enough, go fetch"."""
        with self._lock:
            entry = self._tickers.get((exchange, pair))
        if entry is None:
            return None
        ticker, fetched_at = entry
        if time.time() - fetched_at > max_age_s:
            return None
        return ticker

    def available(self) -> dict[str, list]:
        with self._lock:
            return {
                "ohlcv": [list(k) for k in self._ohlcv],
                "funding_rate": [list(k) for k in self._funding],
                "trades": [list(k) for k in self._trades],
                "tickers": [list(k) for k in self._tickers],
            }


# ---------------------------------------------------------------------------
#  TickerFetcher: on-demand, deduped, short-TTL live ticker fetching for
#  entry/exit pricing (Exchange.get_rate). Deliberately NOT proactive polling
#  like DataCollector's OHLCV/funding-rate loop - most of the 44-pair
#  whitelist has no open trade at any given moment, so polling every pair's
#  ticker on a schedule would waste far more requests than it saves. Instead:
#  serve from a short-TTL cache when fresh, otherwise fetch on demand - and
#  when several clients ask for the SAME pair around the same moment (the
#  exact scenario that was producing redundant fetch_ticker() calls across
#  the fleet - confirmed live: 188 rate-limit warnings across 6 bots in one
#  ~15 minute session), only the FIRST one triggers a real fetch; the rest
#  share its result.
# ---------------------------------------------------------------------------


class TickerFetcher:
    def __init__(
        self,
        exchange: Any,
        cache: DataCache,
        exchange_name: str,
        *,
        max_age_s: float = 2.0,
        fetch_timeout_s: float = 5.0,
    ) -> None:
        self.exchange = exchange
        self.cache = cache
        self.exchange_name = exchange_name
        self.max_age_s = max_age_s
        self.fetch_timeout_s = fetch_timeout_s
        self._lock = threading.Lock()
        self._inflight: dict[str, concurrent.futures.Future] = {}
        # Sized for real overlap (several DIFFERENT pairs' first-fetch-of-the-
        # window landing at once) - not for total client count, since same-
        # pair requests share one in-flight Future rather than each taking a
        # worker slot.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="ticker-ondemand-fetch"
        )

    def get(self, pair: str) -> dict | None:
        """Returns None on any failure/timeout - callers (the master's
        get_ticker request handler) must treat that exactly like "nothing
        cached", never raise it up as an error the requesting bot can't
        recover from; the bot's own Exchange.get_rate() always falls back to
        a direct fetch_ticker() call when this returns nothing."""
        cached = self.cache.get_ticker(self.exchange_name, pair, self.max_age_s)
        if cached is not None:
            return cached

        with self._lock:
            future = self._inflight.get(pair)
            is_new = future is None
            if is_new:
                future = self._executor.submit(self._fetch, pair)
                self._inflight[pair] = future

        try:
            return future.result(timeout=self.fetch_timeout_s)
        except concurrent.futures.TimeoutError:
            # Don't cancel - a slow-but-eventually-successful fetch still
            # warms the cache for the next request; this caller just isn't
            # going to wait for it themselves. Not logged as a warning: under
            # real exchange congestion this is an expected, routine outcome,
            # not something requiring attention every time it happens.
            return None
        except Exception:
            logger.debug("On-demand ticker fetch failed for %s", pair, exc_info=True)
            return None

    def _fetch(self, pair: str) -> dict | None:
        try:
            ticker = self.exchange.fetch_ticker(pair)
            self.cache.set_ticker(self.exchange_name, pair, ticker)
            return ticker
        except Exception:
            logger.debug("Ticker fetch failed for %s", pair, exc_info=True)
            return None
        finally:
            with self._lock:
                self._inflight.pop(pair, None)


# ---------------------------------------------------------------------------
#  Master: serves clients, accepts pushes from subservers.
# ---------------------------------------------------------------------------


def _handle_client_message(
    cache: DataCache, msg: dict, ticker_fetcher: "TickerFetcher | None" = None
) -> dict:
    mtype = msg.get("type")
    if mtype == "get_ticker":
        if ticker_fetcher is None:
            return {"type": "error", "message": "ticker fetching not available on this master"}
        ticker = ticker_fetcher.get(msg["pair"])
        if ticker is None:
            return {"type": "error", "message": "no ticker available for this pair right now"}
        return {
            "type": "ticker",
            "exchange": msg["exchange"],
            "pair": msg["pair"],
            "ticker": ticker,
        }
    if mtype == "get_ohlcv":
        df = cache.get_ohlcv(
            msg["exchange"],
            msg["pair"],
            msg["timeframe"],
            msg.get("candle_type", "spot"),
            limit=msg.get("limit"),
        )
        if df is None:
            return {"type": "error", "message": "no data cached for this exchange/pair/timeframe"}
        return {
            "type": "ohlcv",
            "exchange": msg["exchange"],
            "pair": msg["pair"],
            "timeframe": msg["timeframe"],
            "candle_type": msg.get("candle_type", "spot"),
            **df_to_wire(df),
        }
    if mtype == "get_funding_rate":
        payload = cache.get_funding_rate(msg["exchange"], msg["pair"])
        if payload is None:
            return {"type": "error", "message": "no funding rate cached for this exchange/pair"}
        return {
            "type": "funding_rate",
            "exchange": msg["exchange"],
            "pair": msg["pair"],
            "funding_rate": payload,
        }
    if mtype == "get_trades":
        df = cache.get_trades(msg["exchange"], msg["pair"], limit=msg.get("limit"))
        if df is None:
            return {"type": "error", "message": "no trades cached for this exchange/pair"}
        return {
            "type": "trades",
            "exchange": msg["exchange"],
            "pair": msg["pair"],
            **trades_to_wire(df),
        }
    if mtype == "list_available":
        return {"type": "available", **cache.available()}
    return {"type": "error", "message": f"unknown request type: {mtype!r}"}


class _ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class _ClientConnection:
    """Master-side handle to one connected trading-bot client: its live socket
    plus a lock guarding writes. Needed so the collection thread (calling
    `ClientInterestRegistry.publish_*` from inside `collect_once()`) can push
    unsolicited data down this socket from a DIFFERENT thread than the one
    blocked reading requests on it (`_ClientHandler.handle()`'s own loop) -
    same shape/reasoning as `SubserverConnection`, kept as its own class since
    conflating "client" and "subserver" under one name would be confusing."""

    def __init__(self, sock: socket.socket, client_id: str) -> None:
        self.sock = sock
        self.client_id = client_id
        self._write_lock = threading.Lock()

    def send(self, msg: dict) -> bool:
        with self._write_lock:
            try:
                send_msg(self.sock, msg)
                return True
            except OSError:
                return False
            except Exception:
                # e.g. an unserializable payload - report "send failed" to the
                # caller rather than propagate into whatever thread (collection
                # thread, handler thread) happened to be sending.
                logger.exception("Failed to serialize/send a message to client '%s'", self.client_id)
                return False


class ClientInterestRegistry:
    """Tracks each connected trading-bot client's requested pairs/data-kinds and
    recomputes the union whenever a client registers, updates, or disconnects.
    That union is what drives the master's own collection (see WorkDistributor,
    which further splits it across itself and any connected subservers) - so N
    trader_bot processes watching overlapping pairs on the same exchange collapse
    into one collection job instead of N independent ones.

    Also doubles as the push broadcaster: `publish_ohlcv`/`publish_funding_rate`/
    `publish_trades` (called from the collection thread right after a fresh tick
    lands - see `_run_master`'s `_on_ohlcv`/`_on_funding_rate`/`_on_trades` hooks)
    forward it unprompted to every currently-registered client whose interest
    covers that pair, "webhook" style, instead of making every client poll
    `get_ohlcv`/etc. on its own cycle and wait up to one poll interval to see it."""

    def __init__(
        self, on_union_changed: Callable[[list[str], bool, bool], None]
    ) -> None:
        self._on_union_changed = on_union_changed
        self._lock = threading.Lock()
        # client_id -> (pairs, want_funding_rate, want_trades)
        self._clients: dict[str, tuple[list[str], bool, bool]] = {}
        # client_id -> live connection, only present while a push target is
        # reachable - a client that registered interest before this feature
        # existed (impossible in practice, but defensively) simply never gets
        # a push, same as if it were momentarily disconnected.
        self._connections: dict[str, _ClientConnection] = {}
        # on_union_changed (WorkDistributor.update -> _rebalance ->
        # _volume_sorted_pairs) can block for real network time - a genuine
        # get_tickers() call, potentially several seconds, longer still under
        # rate-limit pressure. It used to run inline on THIS client's own
        # _ClientHandler.handle() thread, so the "registered" ack response
        # didn't go out until it finished. DataServerClient._await_response()
        # has a 10s timeout; when a rebalance ran longer than that, the client
        # gave up and dropped the connection - which reconnects and re-sends
        # its registration on connect, triggering ANOTHER rebalance, in a
        # self-sustaining ~10s reconnect storm across every connected client
        # (confirmed live: all 6 clients cycling "Client connected -> Client
        # disconnected" in lockstep every ~10-11s). A single-worker background
        # executor lets register()/unregister() ack immediately while the
        # rebalance runs independently - single-worker (not one thread per
        # call) so rebalances still apply strictly in submission order, never
        # racing a later union being overwritten by an earlier one that
        # happened to finish later.
        self._rebalance_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="registry-rebalance"
        )

    def _submit_union_changed(self, union_pairs: list[str], union_ffr: bool, union_trades: bool) -> None:
        def _run() -> None:
            try:
                self._on_union_changed(union_pairs, union_ffr, union_trades)
            except Exception:
                logger.exception("on_union_changed callback failed")

        self._rebalance_executor.submit(_run)

    def register(
        self,
        client_id: str,
        pairs: list[str],
        want_funding_rate: bool,
        want_trades: bool,
        *,
        connection: _ClientConnection | None = None,
    ) -> None:
        with self._lock:
            self._clients[client_id] = (list(pairs), want_funding_rate, want_trades)
            if connection is not None:
                self._connections[client_id] = connection
            union_pairs, union_ffr, union_trades = self._union()
            n_clients = len(self._clients)
        logger.info(
            "Client '%s' registered interest in %d pair(s) (union now %d pair(s) "
            "across %d client(s); funding_rate=%s, trades=%s)",
            client_id, len(pairs), len(union_pairs), n_clients, union_ffr, union_trades,
        )
        # Fire-and-forget (see __init__ docstring on _rebalance_executor) - the
        # registration itself (the dicts above) already succeeded and this
        # method must return immediately regardless of how long rebalancing
        # takes.
        self._submit_union_changed(union_pairs, union_ffr, union_trades)

    def unregister(self, client_id: str) -> None:
        with self._lock:
            removed = self._clients.pop(client_id, None) is not None
            self._connections.pop(client_id, None)
            union_pairs, union_ffr, union_trades = self._union()
            n_clients = len(self._clients)
        if removed:
            logger.info(
                "Client '%s' disconnected (union now %d pair(s) across %d client(s))",
                client_id, len(union_pairs), n_clients,
            )
            # Called from handler `finally:` blocks - must return promptly and
            # never raise; see _submit_union_changed.
            self._submit_union_changed(union_pairs, union_ffr, union_trades)

    def _union(self) -> tuple[list[str], bool, bool]:
        pairs: set[str] = set()
        want_funding_rate = False
        want_trades = False
        for p, wf, wt in self._clients.values():
            pairs.update(p)
            want_funding_rate = want_funding_rate or wf
            want_trades = want_trades or wt
        return sorted(pairs), want_funding_rate, want_trades

    def _targets_for_pair(self, pair: str, *, need_funding_rate: bool = False,
                          need_trades: bool = False) -> list[_ClientConnection]:
        """Every currently-connected client whose registered interest covers
        `pair` (and, if asked, also wants funding-rate / trades data)."""
        with self._lock:
            client_ids = [
                cid for cid, (pairs, wf, wt) in self._clients.items()
                if pair in pairs
                and (not need_funding_rate or wf)
                and (not need_trades or wt)
            ]
            return [self._connections[cid] for cid in client_ids if cid in self._connections]

    def _broadcast(self, targets: list[_ClientConnection], msg: dict) -> None:
        # Best-effort, fire-and-forget: a send failure here just means that one
        # client will fall back to its own next `get_*` poll (or reconnect) -
        # never let a broken connection block or drop the push for anyone else.
        for conn in targets:
            try:
                conn.send(msg)
            except Exception:
                logger.debug("Push to client '%s' failed", conn.client_id, exc_info=True)

    def publish_ohlcv(
        self, exchange: str, pair: str, timeframe: str, candle_type: str, new_df: DataFrame
    ) -> None:
        targets = self._targets_for_pair(pair)
        if not targets or new_df is None or new_df.empty:
            return
        self._broadcast(targets, {
            "type": "ohlcv_push", "exchange": exchange, "pair": pair,
            "timeframe": timeframe, "candle_type": candle_type, **df_to_wire(new_df),
        })

    def publish_funding_rate(self, exchange: str, pair: str, payload: dict) -> None:
        targets = self._targets_for_pair(pair, need_funding_rate=True)
        if not targets:
            return
        self._broadcast(targets, {
            "type": "funding_rate_push", "exchange": exchange, "pair": pair,
            "funding_rate": payload,
        })

    def publish_trades(self, exchange: str, pair: str, new_df: DataFrame) -> None:
        targets = self._targets_for_pair(pair, need_trades=True)
        if not targets or new_df is None or new_df.empty:
            return
        self._broadcast(targets, {
            "type": "trades_push", "exchange": exchange, "pair": pair,
            **trades_to_wire(new_df),
        })


class _ClientHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        cache: DataCache = self.server.cache  # type: ignore[attr-defined]
        registry: ClientInterestRegistry | None = getattr(self.server, "registry", None)
        ticker_fetcher: TickerFetcher | None = getattr(self.server, "ticker_fetcher", None)
        peer = self.client_address
        client_id = str(peer)
        registered = False
        # Created up front (not just at register_interest time) and used for
        # EVERY outgoing write on this socket, including plain query
        # responses below - not just pushes. The collection thread can call
        # `registry.publish_*` -> `conn.send()` at any moment once this
        # client is registered, completely independently of this handler's
        # own request/response loop; without routing every write through the
        # same `_ClientConnection` (and its lock), a push landing mid-write of
        # a query response could interleave bytes from both writes on the
        # wire and corrupt the frame stream for whichever read either one.
        conn = _ClientConnection(self.request, client_id)
        # Not logged at INFO here: ensure_master_running()'s `_probe()` (see below)
        # opens a plain connect-then-close TCP socket, with no frame ever sent, to
        # check whether a master is already listening - every trader_bot start-up
        # does one of these in addition to its real persistent connection. Logging
        # this unconditionally made every bot launch look like a client connecting
        # and immediately disconnecting. Instead, the INFO-level "Client connected"
        # is deferred to the first message actually received below - a probe never
        # sends one, so it never gets an INFO connect log (only the DEBUG one right
        # here); a real client sends register_interest within moments of connecting,
        # so the INFO log still lands essentially at connect time for it. The
        # `finally` block below uses the same `got_any_message` flag to log a real
        # disconnect at INFO but a probe's close at DEBUG.
        logger.debug("Client connected: %s", peer)
        got_any_message = False
        try:
            while True:
                try:
                    msg = recv_msg(self.request)
                except (ConnectionError, ValueError, json.JSONDecodeError) as e:
                    logger.warning("Client %s sent a bad frame: %s", peer, e)
                    break
                if msg is None:
                    break
                if not got_any_message:
                    logger.info("Client connected: %s", peer)
                got_any_message = True
                if not isinstance(msg, dict):
                    # Valid JSON but not an object (e.g. a bare list/number) -
                    # without this guard, msg.get() raised AttributeError and
                    # killed the connection with a traceback.
                    logger.warning("Client %s sent a non-object frame: %r", peer, type(msg).__name__)
                    if not conn.send({"type": "error", "message": "expected a JSON object"}):
                        break
                    continue
                if msg.get("type") == "register_interest" and registry is not None:
                    client_id = msg.get("client_id", client_id)
                    conn.client_id = client_id
                    pairs = msg.get("pairs", [])
                    want_funding_rate = bool(msg.get("want_funding_rate", False))
                    want_trades = bool(msg.get("want_trades", False))
                    try:
                        # Hand the registry this connection too (not just the
                        # interest list) - it's the target `publish_*` sends
                        # pushes down, from the collection thread, later.
                        registry.register(
                            client_id, pairs, want_funding_rate, want_trades, connection=conn,
                        )
                        registered = True
                        response = {"type": "registered", "pairs": len(pairs)}
                    except Exception as e:
                        logger.exception("Failed to register interest for '%s'", client_id)
                        response = {"type": "error", "message": str(e)}
                else:
                    try:
                        response = _handle_client_message(cache, msg, ticker_fetcher)
                    except Exception as e:
                        logger.exception("Error handling client request %r", msg)
                        response = {"type": "error", "message": str(e)}
                if not conn.send(response):
                    break
        finally:
            if registry is not None and registered:
                registry.unregister(client_id)
            if got_any_message:
                logger.info("Client disconnected: %s", peer)
            else:
                logger.debug(
                    "Probe connection from %s closed (no data sent - likely a "
                    "master health-check from ensure_master_running(), not a real client)",
                    peer,
                )


# ---------------------------------------------------------------------------
#  Work distribution: split one exchange's pair list across the master's own
#  local collection and any connected subservers, so no single collector ever
#  exceeds that exchange's empirically safe per-batch pair count. Thresholds
#  come from live probing (see VulcanTrader/ratelimit_probe.py) - the highest
#  pair count that stayed rate-limit-free in a single refresh_latest_ohlcv()
#  batch under this repo's standard ccxt_config (enableRateLimit + rateLimit:
#  50ms). Conservative by construction: the last known-*good* count, not the
#  count that first failed (e.g. hyperliquid failed at 40, worked at 20, so the
#  entry is 20). OKX's real threshold moved between ~90 and ~150 across separate
#  probe runs (likely load-dependent on OKX's side), so its entry is a
#  conservative floor rather than the observed ceiling. Coinbase is the
#  exception: escalating 250/350/450/550/650/750/850/928 pairs never triggered
#  a rate-limit signal even once, so its entry (928) is simply its entire
#  available spot market at probe time, not an observed ceiling - re-probe if
#  Coinbase's tradable-pair count grows meaningfully past that.
# ---------------------------------------------------------------------------

EXCHANGE_MAX_SAFE_PAIRS: dict[str, int] = {
    # Raised 20 -> 40 (still an estimate, not a hard guarantee - see the
    # adaptive ramp below): each DataCollector now grows into this ceiling
    # from a conservative floor instead of requesting it outright from the
    # first tick, since even 20 concurrent pairs still triggered 429s during
    # a startup burst under real multi-bot load (also visibly exceeding the
    # underlying HTTP connection pool's default size of 10 - see
    # `urllib3.connectionpool: Connection pool is full` warnings).
    "hyperliquid": 40,
    "okx": 80,
    "kucoin": 150,
    "bitunix": 200,
    "binance": 600,
    "bitget": 1000,
    "bitmart": 1200,
    "coinex": 1000,
    "cryptocom": 500,
    "kraken": 1200,
    "hitbtc": 1000,
    # Never rate-limited at any escalation step (250/350/450/550/650/750/850/928) -
    # 928 is coinbase's entire available spot market, pulled clean in one batch.
    "coinbase": 928,
}
# Conservative default for any exchange not covered by a probe run above.
DEFAULT_MAX_SAFE_PAIRS = 100


def max_safe_pairs_for(exchange_name: str) -> int:
    return EXCHANGE_MAX_SAFE_PAIRS.get(exchange_name.lower(), DEFAULT_MAX_SAFE_PAIRS)


# See _volume_sorted_pairs: bounds how long a rebalance waits on the
# volume-priority ticker fetch before giving up on it for THIS rebalance and
# falling back to alphabetical order. A small dedicated pool (not the
# ClientInterestRegistry rebalance pool - this fetch is called FROM that
# pool's worker thread, so blocking there means blocking on itself) - 2
# workers is enough for "one in flight, one more queued if a fresh rebalance
# fires before the first ticker fetch times out."
_TICKER_SORT_TIMEOUT_S = 10.0
_ticker_sort_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="ticker-sort-fetch"
)


# ---------------------------------------------------------------------------
#  Adaptive batch-size ramp: EXCHANGE_MAX_SAFE_PAIRS is a per-exchange
#  ESTIMATE, not a guarantee - real-world conditions (a startup burst from
#  several trader_bot processes registering at once, exchange-side congestion,
#  this machine's own HTTP connection pool) can push back well below it (see
#  the "hyperliquid: 40" comment above). Rather than a DataCollector always
#  requesting its full max_pairs_per_batch from the first tick, it starts at
#  a conservative floor and grows toward the ceiling only as long as recent
#  ticks show no rate-limit pushback, cutting back hard the moment one
#  appears - the same slow-start-then-back-off shape TCP congestion control
#  uses, applied to "how many pairs to request in one refresh_latest_ohlcv()
#  call" instead of a send window.
# ---------------------------------------------------------------------------

_RAMP_FLOOR = 10  # starting/minimum batch size - matches the observed default
# urllib3 HTTPConnectionPool size (10); also a reasonable floor generally.
_RAMP_STEP = 5  # pairs added per clean tick while ramping up
_RAMP_COOLDOWN_S = 60.0  # pause ramp-up for this long after a rate-limit cut

# See DataCollector.collect_once(): only include funding-rate-HISTORY entries
# (which roughly double a batch's request count when bundled with regular
# OHLCV every tick) on every Nth tick.
_FUNDING_HISTORY_TICK_INTERVAL = 5

# Same signal words VulcanTrader/ratelimit_probe.py looks for - kept as an
# independent, smaller copy here (rather than imported) because
# ratelimit_probe.py itself imports FROM this module, so importing back would
# be circular.
_RATE_LIMIT_LOG_MARKERS = (
    "ratelimitexceeded", "rate limit", "too many requests", "429",
    "ddosprotection", "ddos protection",
)

# See DataCollector._collect_funding_rates: consecutive non-rate-limit failures
# for the SAME pair before giving up on ever fetching its funding rate again
# this process's lifetime.
_FUNDING_RATE_MAX_FAILURES = 3


class _RateLimitLogCapture(logging.Handler):
    """Temporary handler attached around one refresh_latest_ohlcv() call to detect
    whether the exchange pushed back with a rate-limit signal. refresh_latest_ohlcv()
    only LOGS per-pair fetch exceptions internally - it never re-raises them (see
    ratelimit_probe.py's own docstring on the same point) - so a log capture is the
    only way for the caller to observe this and feed DataCollector's adaptive ramp
    (see _record_batch_outcome)."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.hit = False

    def emit(self, record: logging.LogRecord) -> None:
        if self.hit:
            return
        msg = record.getMessage().lower()
        if any(marker in msg for marker in _RATE_LIMIT_LOG_MARKERS):
            self.hit = True


def _volume_sorted_pairs(pairs: list[str], exchange: Any) -> list[str]:
    """Order `pairs` by live quote volume, highest first - same sort_key/
    direction VolumePairList uses (pairlist/VolumePairList.py, `sorted(...,
    reverse=True, key=lambda t: t[self._sort_key])`).

    _split_pairs below hands out shares in list order, and each worker's own
    DataCollector rotation (see `_next_batch_pairlist`) starts its very first
    batch from the front of its share too. When a share exceeds one
    exchange-safe batch, whatever's earliest in the list gets refreshed
    soonest after every pair assignment change; everything else waits its
    turn in the rotation. Without this, that list came from a plain
    `sorted(...)` over pair name strings - alphabetical, with zero
    relationship to which pairs actually matter - so a capacity shortfall
    would delay pairs arbitrarily (whatever sorts last alphabetically)
    instead of prioritizing the least-traded ones for delay.
    Feeding a volume-sorted list in means the master's own share and every
    subserver's share both fill from the top (highest volume) down, so any
    pairs that have to wait an extra tick or two are guaranteed to be the
    lowest-volume ones, not an alphabetical accident.

    Falls back to alphabetical order if ticker data isn't available (e.g.
    the exchange doesn't support fetchTickers, or the fetch itself fails) -
    a ticker hiccup must never block pair distribution outright. Uses the
    cached ticker snapshot (`cached=True`) so this ordering pass adds no
    extra request pressure of its own.

    Bounded to `_TICKER_SORT_TIMEOUT_S`: this ordering is a nice-to-have
    (which pairs get refreshed FIRST within the rotation), not something OHLCV
    collection should ever wait on. Hyperliquid's fetch_tickers() always
    returns EVERY market (768+) regardless of a `symbols` filter - there's no
    cheaper, scoped endpoint on their side - so under real congestion this one
    call can itself take minutes (confirmed live: ~4 minutes, TWICE, on two
    separate sessions hours apart), during which the master was assigned zero
    pairs and collected zero candles. Every second spent waiting here is a
    second not spent fetching anything. Runs the fetch on a background
    executor and waits only up to the timeout; on timeout, falls back to
    alphabetical order for THIS rebalance and lets the fetch keep running -
    Exchange.get_tickers()'s own internal cache means a slow call that
    finishes later still warms the cache for the NEXT rebalance, so nothing
    is wasted, it just doesn't block.
    """
    if not pairs:
        # Nothing to sort - and, critically, no reason to pay for a real
        # get_tickers() call to do it. This is called unconditionally both at
        # master startup (collector.pairs is always [] before any client has
        # registered - see _run_master) and on every ClientInterestRegistry
        # union change, so without this guard an empty pairlist still fired a
        # full bulk ticker fetch. Under exchange rate-limit pressure that call
        # can retry for a long time (observed live: tens of seconds), which
        # blocked the master from ever reaching MasterServer.start() within
        # ensure_master_running()'s startup_timeout - every connecting bot
        # gave up waiting on a master that was, from its own perspective,
        # never actually stuck - just needlessly fetching tickers for zero
        # pairs before it could bind its listening socket.
        return []
    future = _ticker_sort_executor.submit(exchange.get_tickers, cached=True)
    try:
        tickers = future.result(timeout=_TICKER_SORT_TIMEOUT_S)
    except concurrent.futures.TimeoutError:
        logger.info(
            "Volume-priority pair sort still waiting on get_tickers() after %.0fs - "
            "using alphabetical order for THIS rebalance so OHLCV collection isn't "
            "held up. The ticker fetch keeps running in the background and will "
            "warm the cache for the next rebalance.",
            _TICKER_SORT_TIMEOUT_S,
        )
        return sorted(pairs)
    except Exception:
        logger.warning(
            "Volume-priority pair sort failed (ticker fetch) - falling back to "
            "alphabetical order for this rebalance", exc_info=True,
        )
        return sorted(pairs)

    if not tickers:
        return sorted(pairs)

    def _volume(pair: str) -> float:
        ticker = tickers.get(pair)
        if not ticker:
            return -1.0
        vol = ticker.get("quoteVolume")
        return float(vol) if vol is not None else -1.0

    return sorted(pairs, key=_volume, reverse=True)


def _split_pairs(pairs: list[str], num_workers: int) -> list[list[str]]:
    """Split `pairs` into `num_workers` shares, balanced as evenly as possible.

    Shares are NOT capped to a per-worker pair limit here - a share bigger than
    one exchange-safe batch is expected and fine: each worker's own
    DataCollector rotates through its share across successive polling ticks
    (see DataCollector.max_pairs_per_batch / _next_batch_pairlist) rather than
    this function permanently dropping whatever doesn't fit in one tick, which
    used to leave the trailing pairs uncollected forever with too few
    subservers connected."""
    if num_workers <= 0:
        return []
    if not pairs:
        return [[] for _ in range(num_workers)]
    even_share = -(-len(pairs) // num_workers)  # ceil division
    shares = []
    idx = 0
    for _ in range(num_workers):
        shares.append(pairs[idx : idx + even_share])
        idx += even_share
    return shares


class SubserverConnection:
    """Master-side handle to one connected subserver: its live socket plus a lock
    guarding writes. Rebalancing triggered by a *different* subserver connecting or
    disconnecting can push a fresh assignment to this connection from a thread
    other than the one running its own read loop (_SubserverHandler.handle()),
    so writes need their own lock independent of that thread."""

    def __init__(self, sock: socket.socket, name: str) -> None:
        self.sock = sock
        self.name = name
        self._write_lock = threading.Lock()

    def send(self, msg: dict) -> bool:
        with self._write_lock:
            try:
                send_msg(self.sock, msg)
                return True
            except OSError:
                return False
            except Exception:
                logger.exception("Failed to serialize/send a message to subserver '%s'", self.name)
                return False


class WorkDistributor:
    """Owns the master's live view of connected subservers and (re)computes every
    worker's pair assignment - including the master's own local share - whenever
    the worker pool *or* the target pair list changes (see `update()`, called by
    ClientInterestRegistry whenever a trading-bot client registers/updates/drops
    its interest), splitting the total pair list for `exchange_name` as evenly
    as possible across every individual collector. `max_pairs_per_worker` is
    passed through to each collector as its own rotation batch size (see
    DataCollector.max_pairs_per_batch) rather than capping what a worker gets
    assigned - a worker can be assigned more pairs than that and will rotate
    through them across ticks instead of some pairs never being assigned to
    anyone. Assignment is pair-count-based only; every worker keeps using its
    own configured timeframes/candle_type."""

    def __init__(
        self,
        exchange_name: str,
        all_pairs: list[str],
        max_pairs_per_worker: int,
        on_master_pairs: Callable[[list[str]], None],
    ) -> None:
        self.exchange_name = exchange_name
        self.all_pairs = list(all_pairs)
        self.max_pairs_per_worker = max_pairs_per_worker
        self._on_master_pairs = on_master_pairs
        self._lock = threading.Lock()
        self._subservers: dict[str, SubserverConnection] = {}
        self._rebalance()

    def update(self, all_pairs: list[str]) -> None:
        """Replace the full pair list to distribute - e.g. the union of every
        registered trading-bot client's interest - and immediately rebalance."""
        self.all_pairs = list(all_pairs)
        self._rebalance()

    def register(self, conn: SubserverConnection) -> None:
        with self._lock:
            self._subservers[conn.name] = conn
            worker_count = 1 + len(self._subservers)
        logger.info(
            "%s: subserver '%s' joined the worker pool (%d worker(s) now)",
            self.exchange_name, conn.name, worker_count,
        )
        self._rebalance()

    def unregister(self, name: str) -> None:
        with self._lock:
            removed = self._subservers.pop(name, None) is not None
            worker_count = 1 + len(self._subservers)
        if removed:
            logger.info(
                "%s: subserver '%s' left the worker pool (%d worker(s) now)",
                self.exchange_name, name, worker_count,
            )
            self._rebalance()

    def _rebalance(self) -> None:
        with self._lock:
            subserver_items = list(self._subservers.items())
        workers = 1 + len(subserver_items)
        shares = _split_pairs(self.all_pairs, workers)
        oversized = [len(s) for s in shares if len(s) > self.max_pairs_per_worker]
        if oversized:
            logger.info(
                "%s: %d/%d worker(s) have more pairs (%s) than the %d-pair safe batch "
                "size across %d worker(s) (1 master + %d subserver(s)) - each rotates "
                "through its share across successive polling ticks instead of refreshing "
                "every pair every tick. Connect more subservers to shrink this gap.",
                self.exchange_name, len(oversized), workers, oversized,
                self.max_pairs_per_worker, workers, len(subserver_items),
            )
        # Runs on whatever thread triggered the rebalance (a client handler, a
        # subserver handler, the collection thread) - a failure in either the
        # local set_pairs callback or one subserver's send must never propagate
        # into that thread, nor stop the remaining subservers' assignments.
        try:
            self._on_master_pairs(shares[0])
            logger.info("%s: master keeps %d pair(s) locally", self.exchange_name, len(shares[0]))
        except Exception:
            logger.exception("%s: applying the master's own pair share failed", self.exchange_name)
        for (name, conn), share in zip(subserver_items, shares[1:]):
            try:
                ok = conn.send(
                    {"type": "assign_pairs", "exchange": self.exchange_name, "pairs": share}
                )
            except Exception:
                logger.exception("%s: assigning pairs to subserver '%s' failed", self.exchange_name, name)
                continue
            logger.info(
                "%s: assigned %d pair(s) to subserver '%s'%s",
                self.exchange_name, len(share), name, "" if ok else " (send failed)",
            )


class _SubserverHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        cache: DataCache = self.server.cache  # type: ignore[attr-defined]
        distributor: WorkDistributor | None = getattr(self.server, "distributor", None)
        peer = self.client_address
        name = str(peer)
        conn: SubserverConnection | None = None
        try:
            while True:
                try:
                    msg = recv_msg(self.request)
                except (ConnectionError, ValueError, json.JSONDecodeError) as e:
                    logger.warning("Subserver %s sent a bad frame: %s", name, e)
                    break
                if msg is None:
                    break
                if not isinstance(msg, dict):
                    logger.warning(
                        "Subserver '%s' sent a non-object frame: %r", name, type(msg).__name__
                    )
                    continue
                mtype = msg.get("type")
                if mtype == "hello":
                    name = msg.get("name", name)
                    logger.info("Subserver '%s' connected from %s", name, peer)
                    if distributor is not None:
                        conn = SubserverConnection(self.request, name)
                        distributor.register(conn)
                elif mtype == "push_ohlcv":
                    try:
                        df = wire_to_df(msg)
                        changed = cache.merge_ohlcv(
                            msg["exchange"],
                            msg["pair"],
                            msg["timeframe"],
                            msg.get("candle_type", "spot"),
                            df,
                        )
                        logger.debug(
                            "Pushed OHLCV from '%s': %s %s %s (+%d rows, changed=%s)",
                            name, msg["exchange"], msg["pair"], msg["timeframe"], len(df), changed,
                        )
                    except Exception:
                        logger.exception("Failed to merge pushed OHLCV from '%s'", name)
                elif mtype == "push_funding_rate":
                    try:
                        cache.set_funding_rate(msg["exchange"], msg["pair"], msg["funding_rate"])
                        logger.debug(
                            "Pushed funding rate from '%s': %s %s",
                            name, msg["exchange"], msg["pair"],
                        )
                    except Exception:
                        logger.exception("Failed to store pushed funding rate from '%s'", name)
                elif mtype == "push_trades":
                    try:
                        df = wire_to_trades(msg)
                        changed = cache.merge_trades(msg["exchange"], msg["pair"], df)
                        logger.debug(
                            "Pushed trades from '%s': %s %s (+%d rows, changed=%s)",
                            name, msg["exchange"], msg["pair"], len(df), changed,
                        )
                    except Exception:
                        logger.exception("Failed to merge pushed trades from '%s'", name)
                else:
                    logger.warning("Subserver '%s' sent unknown message type: %r", name, mtype)
        finally:
            if distributor is not None and conn is not None:
                distributor.unregister(name)
            logger.info("Subserver '%s' disconnected (%s)", name, peer)


class MasterServer:
    """Owns the two listening sockets: one for clients, one for subservers."""

    def __init__(
        self,
        cache: DataCache,
        host: str = "0.0.0.0",
        client_port: int = 8720,
        subserver_port: int = 8721,
        distributor: WorkDistributor | None = None,
        registry: ClientInterestRegistry | None = None,
        ticker_fetcher: TickerFetcher | None = None,
    ) -> None:
        self.cache = cache
        self.distributor = distributor
        self.registry = registry
        self.ticker_fetcher = ticker_fetcher
        self._client_srv = _ThreadingTCPServer((host, client_port), _ClientHandler)
        self._client_srv.cache = cache  # type: ignore[attr-defined]
        self._client_srv.registry = registry  # type: ignore[attr-defined]
        self._client_srv.ticker_fetcher = ticker_fetcher  # type: ignore[attr-defined]
        self._sub_srv = _ThreadingTCPServer((host, subserver_port), _SubserverHandler)
        self._sub_srv.cache = cache  # type: ignore[attr-defined]
        self._sub_srv.distributor = distributor  # type: ignore[attr-defined]
        self._threads: list[threading.Thread] = []
        self._stopping = threading.Event()

    def _serve_forever_supervised(self, srv: _ThreadingTCPServer, label: str) -> None:
        """serve_forever() only returns after shutdown() is called (an intentional
        stop). If the accept loop itself somehow dies from an unhandled exception,
        that would otherwise silently stop accepting connections for good - instead,
        log loudly and restart it, same as any other fallback in this module."""
        backoff = 1.0
        while not self._stopping.is_set():
            try:
                srv.serve_forever()
                return
            except Exception:
                logger.exception(
                    "Master '%s' listener crashed unexpectedly - restarting in %.0fs",
                    label, backoff,
                )
                self._stopping.wait(backoff)
                backoff = min(backoff * 2, 30.0)

    def start(self) -> None:
        for srv, label in ((self._client_srv, "clients"), (self._sub_srv, "subservers")):
            t = threading.Thread(
                target=self._serve_forever_supervised, args=(srv, label),
                name=f"master-{label}", daemon=True,
            )
            t.start()
            self._threads.append(t)
        logger.info(
            "Master listening: clients on %s, subservers on %s",
            self._client_srv.server_address,
            self._sub_srv.server_address,
        )

    def stop(self) -> None:
        self._stopping.set()
        self._client_srv.shutdown()
        self._sub_srv.shutdown()
        self._client_srv.server_close()
        self._sub_srv.server_close()


# ---------------------------------------------------------------------------
#  Subserver: forwards locally-collected data to a master over an outbound
#  connection, retrying through blips so nothing is lost.
# ---------------------------------------------------------------------------


class SubserverForwarder:
    """Maintains an outbound connection to a master server and forwards batches to
    it, queuing through any blip so nothing is lost. A background sender thread
    drains the queue strictly in order over whatever connection is currently live;
    while disconnected it just waits (retrying the same head-of-queue message)
    rather than dropping it. A separate reconnect thread owns (re)dialing the
    master. The queue is bounded purely as a safety valve against unbounded memory
    growth during an extended outage - only past that bound is anything dropped,
    and loudly logged when it happens."""

    def __init__(
        self,
        master_host: str,
        master_port: int,
        exchange_name: str,
        name: str | None = None,
        max_queue: int = 100_000,
        on_assign_pairs: Callable[[list[str]], None] | None = None,
    ) -> None:
        self.master_addr = (master_host, master_port)
        self.exchange_name = exchange_name
        self.name = name or f"{exchange_name}-subserver"
        # Called with the master's latest pair assignment for this subserver
        # (WorkDistributor rebalancing) - settable after construction too, since
        # the collector it usually points at (collector.set_pairs) doesn't exist
        # until after this forwarder is constructed (its own hooks need the
        # forwarder first). Must be set before start().
        self.on_assign_pairs = on_assign_pairs
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=max_queue)
        self._reconnect_thread = threading.Thread(
            target=self._reconnect_loop, name="subserver-reconnect", daemon=True
        )
        self._sender_thread = threading.Thread(
            target=self._sender_loop, name="subserver-sender", daemon=True
        )
        self._receiver_thread = threading.Thread(
            target=self._receiver_loop, name="subserver-receiver", daemon=True
        )

    def start(self) -> None:
        self._reconnect_thread.start()
        self._sender_thread.start()
        self._receiver_thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    def pending(self) -> int:
        """Number of messages queued but not yet confirmed sent."""
        return self._queue.qsize()

    def _connect(self) -> None:
        sock = socket.create_connection(self.master_addr, timeout=5)
        send_msg(sock, {"type": "hello", "role": "subserver", "name": self.name})
        with self._lock:
            self._sock = sock
        logger.info("Subserver '%s' connected to master at %s", self.name, self.master_addr)

    def _reconnect_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                with self._lock:
                    connected = self._sock is not None
                if not connected:
                    try:
                        self._connect()
                        backoff = 1.0
                    except OSError as e:
                        logger.warning(
                            "Could not connect to master %s: %s (retrying in %.0fs)",
                            self.master_addr, e, backoff,
                        )
                        self._stop.wait(backoff)
                        backoff = min(backoff * 2, 30.0)
                        continue
                self._stop.wait(1.0)
            except Exception:
                # This daemon thread is the ONLY thing that ever redials the
                # master - an unexpected exception here must never end it.
                logger.exception("Subserver reconnect loop hit an unexpected error - continuing")
                self._stop.wait(backoff)

    def _sender_loop(self) -> None:
        """Drains the queue strictly in order. A message is only removed from the
        front of the queue once it has actually been written to a live socket -
        on any failure it is put back and retried after the reconnect loop
        re-establishes a connection, so a blip delays delivery, never drops it."""
        pending: dict | None = None
        while not self._stop.is_set():
            if pending is None:
                try:
                    pending = self._queue.get(timeout=0.5)
                except queue.Empty:
                    continue
            with self._lock:
                sock = self._sock
            if sock is None:
                self._stop.wait(0.5)
                continue
            try:
                send_msg(sock, pending)
                pending = None
            except OSError as e:
                logger.warning("Send to master failed, will retry once reconnected: %s", e)
                with self._lock:
                    if self._sock is sock:
                        try:
                            sock.close()
                        except OSError:
                            pass
                        self._sock = None
                self._stop.wait(0.2)
            except Exception:
                # Non-I/O failure (e.g. an unserializable payload): retrying the
                # SAME message forever would wedge the whole queue behind it -
                # drop the poison message loudly and keep the pipeline moving.
                logger.exception(
                    "Dropping one unsendable forwarder message (type=%r) - see above",
                    pending.get("type") if isinstance(pending, dict) else type(pending).__name__,
                )
                pending = None

    def _receiver_loop(self) -> None:
        """Listens for messages the master pushes down unprompted - currently just
        `assign_pairs`, from WorkDistributor rebalancing. Shares the connection
        with the sender loop; the socket carries the 5s timeout _connect() set via
        socket.create_connection(), so a quiet master just means a TimeoutError
        every ~5s to recheck _stop, not a busy loop. A real read failure means the
        connection dropped - the sender loop will notice the same thing and the
        reconnect loop will redial, so this just clears `_sock` and waits."""
        while not self._stop.is_set():
            with self._lock:
                sock = self._sock
            if sock is None:
                self._stop.wait(0.5)
                continue
            try:
                msg = recv_msg(sock)
            except TimeoutError:
                continue
            except (OSError, ValueError) as e:
                logger.warning("Lost connection to master while listening: %s", e)
                msg = None
            if msg is None:
                with self._lock:
                    if self._sock is sock:
                        try:
                            sock.close()
                        except OSError:
                            pass
                        self._sock = None
                self._stop.wait(0.5)
                continue
            if not isinstance(msg, dict):
                logger.warning("Master sent a non-object frame: %r", type(msg).__name__)
                continue
            if msg.get("type") == "assign_pairs":
                pairs = msg.get("pairs", [])
                logger.info("Received pair assignment from master: %d pair(s)", len(pairs))
                if self.on_assign_pairs is not None:
                    try:
                        self.on_assign_pairs(pairs)
                    except Exception:
                        logger.exception("on_assign_pairs hook failed")
            else:
                logger.warning("Master sent unknown message type: %r", msg.get("type"))

    def _enqueue(self, msg: dict) -> None:
        try:
            self._queue.put_nowait(msg)
        except queue.Full:
            try:
                self._queue.get_nowait()  # drop the oldest to make room
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(msg)
            except queue.Full:
                pass
            logger.warning(
                "Forwarder queue for master %s is full (%d) - dropped the oldest pending "
                "message. The master has been unreachable for a very long time.",
                self.master_addr, self._queue.maxsize,
            )

    def forward_ohlcv(self, pair_key: PairWithTimeframe, df: DataFrame) -> None:
        pair, timeframe, candle_type = pair_key
        self._enqueue(
            {
                "type": "push_ohlcv",
                "exchange": self.exchange_name,
                "pair": pair,
                "timeframe": timeframe,
                "candle_type": candle_type_value(candle_type),
                **df_to_wire(df),
            }
        )

    def forward_funding_rate(self, pair: str, payload: dict) -> None:
        self._enqueue(
            {
                "type": "push_funding_rate",
                "exchange": self.exchange_name,
                "pair": pair,
                "funding_rate": payload,
            }
        )

    def forward_trades(self, pair: str, df: DataFrame) -> None:
        self._enqueue(
            {
                "type": "push_trades",
                "exchange": self.exchange_name,
                "pair": pair,
                **trades_to_wire(df),
            }
        )


# ---------------------------------------------------------------------------
#  DataServerClient: used by trading-bot processes (trader_bot.py) to auto-launch
#  a master if none is running, register their wanted pairs with it (deduped
#  against every other registered client - see ClientInterestRegistry), and
#  query its cache for OHLCV/funding-rate/trades data.
# ---------------------------------------------------------------------------


def _acquire_launch_lock(lock_path: Path, *, timeout: float, stale_after: float = 30.0) -> int | None:
    """Acquire an exclusive, cross-process file lock via atomic create
    (`O_CREAT | O_EXCL` fails atomically if the file already exists, on both
    Windows and POSIX - no extra dependency needed for a single-writer lock).
    Polls until `timeout` elapses. A lock file older than `stale_after`
    seconds is treated as abandoned (its holder crashed before releasing it)
    and removed so it can't wedge every future launch attempt forever.
    Returns the open file descriptor - pass it to `_release_launch_lock` - or
    None if the lock could not be acquired in time."""
    deadline = time.time() + timeout
    while True:
        try:
            return os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > stale_after:
                    logger.warning(
                        "Removing stale data server launch lock %s (age %.0fs)", lock_path, age
                    )
                    lock_path.unlink(missing_ok=True)
                    continue
            except OSError:
                pass
            if time.time() >= deadline:
                return None
            time.sleep(0.2)


def _release_launch_lock(lock_path: Path, lock_fd: int | None) -> None:
    if lock_fd is None:
        return
    try:
        os.close(lock_fd)
    except OSError:
        pass
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        pass


def ensure_master_running(
    host: str,
    port: int,
    config_path: str | Path,
    *,
    subserver_port: int = 8721,
    python_executable: str | None = None,
    extra_args: list[str] | None = None,
    startup_timeout: float = 30.0,
    show_terminal: bool = False,
    tmux: bool = False,
) -> bool:
    """Check whether a data_server master is already listening on (host, port); if
    not, launch one as a fully detached background process, so it outlives
    whichever trader_bot happened to be first to start it - other trader_bot
    processes connecting later depend on it staying up regardless of this one's
    lifetime. Returns True if a master is already running or was just launched
    and came up within `startup_timeout`; False if launching failed outright or
    it never came up (callers should fall back to direct exchange polling).

    ``show_terminal``/``tmux`` control the launched master's own visibility -
    same convention (and same `util.proc_launch.launch_detached` helper) as
    per-bot trade subprocesses use, but read from the LAUNCHING bot's own
    ``data_server`` config block (see config_schema.py), not from
    ``config_path`` - only relevant the moment this function actually spawns
    a master; a no-op if one is already running. Both default false: an
    auto-launched background master popping up a visible console/tmux window
    unasked is more surprising than welcome.

    Several trader_bot processes on the same exchange typically start within
    the same second or two of each other (e.g. a batch of paper-trading bots
    launched together). The probe-then-launch below is a classic TOCTOU race:
    without a lock, every one of them can probe the port BEFORE the first
    launched master has finished binding it, see "nothing listening", and each
    spawn its own master - N bots produced N master processes all fighting
    over the same port, exactly the redundant-polling problem this feature
    exists to prevent (confirmed directly: 5 simultaneous trader_bot launches
    produced 5 `data_server --mode master` processes, only one of which
    actually held the listening socket). The cross-process lock file below
    serializes the whole check-and-launch section so only the first caller to
    acquire it ever spawns a process; every other caller waits on the lock,
    then re-probes (the master it was racing against is now up) instead of
    launching its own."""
    connect_host = "127.0.0.1" if host in ("0.0.0.0", "") else host

    def _probe() -> bool:
        try:
            socket.create_connection((connect_host, port), timeout=1.5).close()
            return True
        except OSError:
            return False

    if _probe():
        logger.info("Data server master already running at %s:%d", connect_host, port)
        return True

    lock_path = Path(tempfile.gettempdir()) / f"vulcantrader_dataserver_master_{port}.lock"
    # Wait comfortably longer than one full launch-and-confirm cycle: the
    # lock is now held for the holder's entire startup_timeout (see below),
    # so a waiter timing out at the same threshold could give up moments
    # before the holder's master actually comes up.
    # stale_after must exceed the longest legitimate hold time (a full
    # startup_timeout while confirming a successful launch), or a lock held
    # by a still-working holder gets ripped away as "abandoned" mid-launch.
    lock_fd = _acquire_launch_lock(
        lock_path, timeout=startup_timeout + 15.0, stale_after=startup_timeout + 30.0
    )
    if lock_fd is None:
        # Someone else is holding the lock (presumably mid-launch) and didn't
        # release it within our own startup_timeout - fall through to a final
        # probe rather than piling on with yet another spawn attempt.
        logger.warning(
            "Timed out waiting for the data server master launch lock at %s:%d "
            "(another process is holding it) - probing once more before giving up",
            connect_host, port,
        )
        return _probe()

    try:
        # Re-probe now that we hold the lock: whoever launched the master we
        # were about to race against has very likely finished by now.
        if _probe():
            logger.info("Data server master already running at %s:%d", connect_host, port)
            return True

        cmd = [
            python_executable or sys.executable,
            "-m", "VulcanTrader.data_server",
            "--mode", "master",
            "-c", str(config_path),
            "--host", connect_host,
            "--port", str(port),
            "--subserver-port", str(subserver_port),
            # Without this the auto-launched master defaults to WARNING-only
            # (main()'s own --verbose default is 0), so its dedicated
            # logs/data_server/ log file would contain almost nothing useful -
            # not even its own "DataCollector initialised" / "Master listening"
            # startup confirmations, both logged at INFO.
            "-v",
        ]
        if extra_args:
            cmd.extend(extra_args)

        logger.warning(
            "No data server master found at %s:%d - launching one: %s",
            connect_host, port, " ".join(cmd),
        )
        try:
            if sys.platform == "win32":
                # CREATE_NEW_CONSOLE (only when show_terminal) instead of
                # DETACHED_PROCESS: the master gets its OWN real, visible
                # console window with live stdio, instead of running fully
                # headless - added after an earlier bot-triggered auto-launch
                # kept coming up with no visible output anywhere. Default is
                # now headless (show_terminal=False) per config_schema.py -
                # setup_logging(subdir="data_server") in main() still
                # independently writes the same records to logs/data_server/
                # *.txt via its own FileHandler regardless, so nothing is
                # lost either way.
                #
                # CREATE_BREAKAWAY_FROM_JOB: if this interpreter is itself
                # running inside a Windows Job Object (common under sandboxed/
                # managed dev environments, CI runners, some IDE/terminal
                # supervisors), a child normally inherits that job by default
                # and gets killed the moment the job's own cleanup fires -
                # observed directly here as an earlier DETACHED_PROCESS master
                # dying silently, mid-`Exchange.__init__`, moments after the
                # parent bot's own tool invocation completed. This flag is a
                # no-op (silently ignored) unless the job explicitly permits
                # breakaway, so it's always set regardless of show_terminal.
                creationflags = subprocess.CREATE_BREAKAWAY_FROM_JOB
                if show_terminal:
                    creationflags |= subprocess.CREATE_NEW_CONSOLE
                subprocess.Popen(
                    cmd, stdin=subprocess.DEVNULL, close_fds=True, creationflags=creationflags
                )
            elif tmux:
                # Reuse the same tmux window-management (stale-window kill,
                # shared "VulcanTrader" session, window naming) that per-bot
                # trade subprocesses already use - see util/proc_launch.py.
                from VulcanTrader.util.proc_launch import launch_detached
                launch_detached(cmd, {"tmux": True})
            else:
                # No portable equivalent of "open a new visible terminal
                # window" from a background process on POSIX (it depends on
                # which terminal emulator, if any, is even installed) - stay
                # headless there and rely on the structured log file, same as
                # before.
                subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                    start_new_session=True,
                )
        except Exception:
            logger.exception("Failed to launch data server master - continuing without it")
            return False

        # Hold the lock through the ENTIRE startup-confirmation wait, not just
        # the Popen call - releasing it right after spawning let a second
        # bot's re-probe run before this master had actually bound the port,
        # so it saw "nothing listening" too and launched its own (confirmed
        # directly: releasing early still produced 5 masters from 5 near-
        # simultaneous bots, just launched one-after-another instead of all
        # at once). Every other waiter now only gets the lock once THIS
        # launch has either succeeded or given up.
        deadline = time.time() + startup_timeout
        while time.time() < deadline:
            if _probe():
                logger.info("Data server master is up at %s:%d", connect_host, port)
                return True
            time.sleep(0.5)
        logger.warning(
            "Data server master at %s:%d did not come up within %.0fs of launching - "
            "continuing without it (falling back to direct exchange polling)",
            connect_host, port, startup_timeout,
        )
        return False
    finally:
        _release_launch_lock(lock_path, lock_fd)


class DataServerClient:
    """Persistent, auto-reconnecting connection from a trading-bot process to a
    data_server master's client port. Registers this process's wanted pairs and
    queries the master's cache for OHLCV/funding-rate/trades data. Every query
    method returns None while disconnected or before the master has the
    requested data cached yet - callers must treat that exactly like a cache
    miss and fall back to their own direct exchange call, never block on it.

    Also receives unsolicited "webhook" pushes: the master forwards every fresh
    OHLCV/funding-rate/trades tick straight to whichever registered clients are
    watching that pair, the moment it lands (see `ClientInterestRegistry.
    publish_*`), rather than making every client wait for its own next poll.
    Set `on_ohlcv_push` / `on_funding_rate_push` / `on_trades_push` (each
    `Callable[[pair, DataFrame|dict], None]`, matching `get_ohlcv`/`get_funding_
    rate`/`get_trades`'s own return shapes) before `start()` to receive them -
    they're invoked from the connection's own background reader thread, so
    keep them fast/non-blocking (e.g. just write into a local cache) rather
    than doing real work inline."""

    def __init__(self, host: str, port: int, exchange_name: str, client_id: str) -> None:
        self.addr = (host, port)
        self.exchange_name = exchange_name
        self.client_id = client_id
        self._sock: socket.socket | None = None
        self._conn_lock = threading.Lock()
        # Serializes the send-then-await-response round trip of _request()/
        # _send_registration() against each other - both share one connection,
        # so two overlapping requests could otherwise consume each other's
        # response off the queue below.
        self._request_lock = threading.Lock()
        # The reader thread (one per live connection - see _connect()) routes
        # every non-push frame here; _request()/_send_registration() block on
        # it instead of calling recv_msg() themselves, so an unsolicited push
        # arriving between a send and its response can never be misread as
        # that response. maxsize=1 is enough given _request_lock guarantees at
        # most one outstanding request at a time.
        self._response_queue: queue.Queue[dict | None] = queue.Queue(maxsize=1)
        self._reader_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._pairs: list[str] = []
        self._want_funding_rate = False
        self._want_trades = False
        self._has_registration = False
        self.on_ohlcv_push: Callable[[str, str, str, DataFrame], None] | None = None
        self.on_funding_rate_push: Callable[[str, dict], None] | None = None
        self.on_trades_push: Callable[[str, DataFrame], None] | None = None
        self._reconnect_thread = threading.Thread(
            target=self._reconnect_loop, name="dataserver-client-reconnect", daemon=True
        )

    def start(self) -> None:
        self._reconnect_thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._conn_lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    def connected(self) -> bool:
        with self._conn_lock:
            return self._sock is not None

    def register(
        self, pairs: list[str], *, want_funding_rate: bool = False, want_trades: bool = False
    ) -> None:
        """Update this process's wanted pairs/data-kinds. Re-sent automatically on
        every (re)connect too, so a master restart doesn't lose the registration.
        Call again (e.g. after every pairlist refresh) to update it."""
        self._pairs = list(pairs)
        self._want_funding_rate = want_funding_rate
        self._want_trades = want_trades
        self._has_registration = True
        self._send_registration()

    def _send_registration(self) -> bool:
        with self._conn_lock:
            sock = self._sock
        if sock is None:
            return False
        msg = {
            "type": "register_interest",
            "client_id": self.client_id,
            "pairs": self._pairs,
            "want_funding_rate": self._want_funding_rate,
            "want_trades": self._want_trades,
        }
        with self._request_lock:
            try:
                send_msg(sock, msg)
            except OSError:
                self._drop(sock)
                return False
            return self._await_response(sock) is not None  # ack - otherwise fire-and-forget

    def _drop(self, sock: socket.socket) -> None:
        with self._conn_lock:
            if self._sock is sock:
                try:
                    sock.close()
                except OSError:
                    pass
                self._sock = None

    def _is_push(self, msg: dict) -> bool:
        return isinstance(msg.get("type"), str) and msg["type"].endswith("_push")

    def _dispatch_push(self, msg: dict) -> None:
        mtype = msg.get("type")
        try:
            if mtype == "ohlcv_push" and self.on_ohlcv_push is not None:
                self.on_ohlcv_push(msg["pair"], msg["timeframe"], msg["candle_type"], wire_to_df(msg))
            elif mtype == "funding_rate_push" and self.on_funding_rate_push is not None:
                self.on_funding_rate_push(msg["pair"], msg["funding_rate"])
            elif mtype == "trades_push" and self.on_trades_push is not None:
                self.on_trades_push(msg["pair"], wire_to_trades(msg))
        except Exception:
            logger.exception("DataServerClient '%s' push handler failed for %r", self.client_id, mtype)

    def _reader_loop(self, sock: socket.socket) -> None:
        """One instance per connection (see _connect()); exits (and drops the
        connection so _reconnect_loop picks it up) the moment this socket
        stops producing valid frames."""
        try:
            while not self._stop.is_set():
                try:
                    msg = recv_msg(sock)
                except (ConnectionError, ValueError, json.JSONDecodeError, OSError):
                    break
                if msg is None:
                    break
                if not isinstance(msg, dict):
                    logger.warning(
                        "DataServerClient '%s' received a non-object frame: %r",
                        self.client_id, type(msg).__name__,
                    )
                    continue
                if self._is_push(msg):
                    self._dispatch_push(msg)
                else:
                    try:
                        self._response_queue.put_nowait(msg)
                    except queue.Full:
                        # A response arrived with nobody waiting (the requester
                        # already gave up / timed out) - drop it rather than
                        # block the reader or let it desync a future request.
                        try:
                            self._response_queue.get_nowait()
                        except queue.Empty:
                            pass
                        self._response_queue.put_nowait(msg)
        finally:
            self._drop(sock)

    def _await_response(self, sock: socket.socket, timeout: float = 30.0) -> dict | None:
        """Raised from 10.0s -> 30.0s after observing a live multi-bot run:
        even with register_interest's rebalance work moved off the handler
        thread (see ClientInterestRegistry.__init__), a master juggling
        several concurrent client connections AND its own OHLCV collection
        loop under real exchange congestion can still occasionally take more
        than 10s to get a response out - not stuck, just genuinely busy. That
        showed up as a one-off mass reconnect across every connected client
        at once (confirmed live: all 6 bots disconnected/reconnected within
        the same second, ~24 minutes into an otherwise clean run) - a false
        positive from a timeout that was tighter than this server's real
        worst-case latency under load, not an actual dead connection. 30s
        still catches a genuinely wedged connection; it just stops firing on
        ordinary load spikes."""
        try:
            return self._response_queue.get(timeout=timeout)
        except queue.Empty:
            # No response within timeout - drop the connection outright rather
            # than risk a stale response landing on some LATER request after
            # reconnecting confuses things; simplest safe recovery.
            self._drop(sock)
            return None

    def _connect(self) -> None:
        sock = socket.create_connection(self.addr, timeout=5)
        # create_connection's `timeout` only bounds the connect handshake, but
        # it leaves the SOCKET's timeout set to that same 5s for every
        # subsequent operation too - including the reader thread's recv_msg()
        # below, which is meant to block indefinitely waiting for the next
        # push or response. Any idle gap on the wire >= 5s (routine: cycles
        # run every `process_throttle_secs`, commonly also 5s) made recv()
        # raise a timeout (a plain OSError subclass), which the reader loop's
        # error handling correctly-per-its-own-logic treated as a dead
        # connection and dropped - a perfectly healthy, just momentarily
        # quiet, connection torn down and reconnected every cycle. Confirmed
        # directly: a bot's client disconnected/reconnected in lockstep with
        # its own 5s process_throttle_secs. Request-level timeouts are
        # already enforced separately by _await_response()'s queue.get(
        # timeout=...), which is the correct layer for that - this socket
        # itself should just block until the connection actually breaks.
        sock.settimeout(None)
        with self._conn_lock:
            self._sock = sock
        # Drain anything left over from a prior connection's response queue -
        # only relevant if a previous _await_response() timed out without
        # consuming what eventually arrived.
        while not self._response_queue.empty():
            try:
                self._response_queue.get_nowait()
            except queue.Empty:
                break
        self._reader_thread = threading.Thread(
            target=self._reader_loop, args=(sock,), name="dataserver-client-reader", daemon=True
        )
        self._reader_thread.start()
        logger.info("DataServerClient '%s' connected to %s", self.client_id, self.addr)
        if self._has_registration:
            self._send_registration()

    def _reconnect_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                with self._conn_lock:
                    connected = self._sock is not None
                if not connected:
                    try:
                        self._connect()
                        backoff = 1.0
                    except OSError as e:
                        logger.warning(
                            "DataServerClient '%s' could not connect to %s: %s (retrying in %.0fs)",
                            self.client_id, self.addr, e, backoff,
                        )
                        self._stop.wait(backoff)
                        backoff = min(backoff * 2, 30.0)
                        continue
                self._stop.wait(1.0)
            except Exception:
                # The only thread that ever redials the master; if it died on an
                # unexpected exception (e.g. from _send_registration inside
                # _connect), the bot would silently never reconnect.
                logger.exception(
                    "DataServerClient '%s' reconnect loop hit an unexpected error - continuing",
                    self.client_id,
                )
                self._stop.wait(backoff)

    def _request(self, msg: dict) -> dict | None:
        """One-shot request/response over the persistent connection. Returns None
        if not connected or on any I/O error (including a timeout) - callers must
        treat that exactly like a cache miss and fall back."""
        with self._conn_lock:
            sock = self._sock
        if sock is None:
            return None
        with self._request_lock:
            try:
                send_msg(sock, msg)
            except OSError:
                self._drop(sock)
                return None
            return self._await_response(sock)

    def get_ohlcv(
        self, pair: str, timeframe: str, candle_type: str = "spot", limit: int | None = None
    ) -> DataFrame | None:
        resp = self._request(
            {
                "type": "get_ohlcv", "exchange": self.exchange_name, "pair": pair,
                "timeframe": timeframe, "candle_type": candle_type, "limit": limit,
            }
        )
        if resp is None or resp.get("type") != "ohlcv":
            return None
        return wire_to_df(resp)

    def get_funding_rate(self, pair: str) -> dict | None:
        resp = self._request(
            {"type": "get_funding_rate", "exchange": self.exchange_name, "pair": pair}
        )
        if resp is None or resp.get("type") != "funding_rate":
            return None
        return resp.get("funding_rate")

    def get_trades(self, pair: str, limit: int | None = None) -> DataFrame | None:
        resp = self._request(
            {"type": "get_trades", "exchange": self.exchange_name, "pair": pair, "limit": limit}
        )
        if resp is None or resp.get("type") != "trades":
            return None
        return wire_to_trades(resp)

    def get_ticker(self, pair: str) -> dict | None:
        """Live entry/exit-pricing ticker, deduped across every bot watching
        this pair (see TickerFetcher) - returns None on any failure/timeout/
        disconnect, exactly like every other get_* method here; callers
        (Exchange.get_rate) always fall back to a direct fetch_ticker() call
        when this returns nothing, so a data_server hiccup degrades pricing
        back to today's behavior rather than blocking it."""
        resp = self._request(
            {"type": "get_ticker", "exchange": self.exchange_name, "pair": pair}
        )
        if resp is None or resp.get("type") != "ticker":
            return None
        return resp.get("ticker")


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------


#  Minimal data_server config format
# ---------------------------------------------------------------------------
#
# data_server has its own tiny, dedicated JSON schema - NOT a VulcanTrader
# trading-bot config (no key/secret, no stake_amount, no minimal_roi), and NOT
# one file per exchange - ONE general config per machine, with no exchange or
# timeframe baked into it at all:
#
#   {
#     "format": "feather",               // "feather" | "parquet", default "feather"
#     "funding_rate": false,             // collect funding-rate history (futures only)
#     "persist_to_disk": true,           // write OHLCV/funding to datadir feather files
#     "orderflow": false,                // collect raw public trades
#     "is_subserver": false,             // false = master role, true = subserver role
#     "master_host": "0.0.0.0",          // THE master server's address, one field
#                                        // regardless of role: bind address when
#                                        // is_subserver=false (this IS the master -
#                                        // "0.0.0.0" listens on every interface),
#                                        // or the real IP/hostname to dial when
#                                        // is_subserver=true (this is a subserver
#                                        // on a different machine, reaching out to
#                                        // wherever the master actually runs).
#     "master_port": 8720,               // master role only: client-facing bind port
#     "subserver_port": 8721,            // BOTH roles: master's subserver-facing bind
#                                        // port, or the port a subserver dials on
#                                        // master_host
#     "name": "kraken-subserver-1"       // subserver role only: self-identification
#                                        // to the master, default "<exchange>-subserver"
#     "show_terminal": false,            // Windows only, default false. Read by
#                                        // ensure_master_running() at the moment it
#                                        // actually launches a master (no-op if one
#                                        // is already up) - true opens a new console
#                                        // window for it, false stays headless.
#     "tmux": false                      // Linux/macOS only, default false. Same
#                                        // one-shot-at-launch caveat as show_terminal -
#                                        // true opens the master in its own tmux
#                                        // window (session "VulcanTrader") instead of
#                                        // a plain headless background process.
#   }
#
# show_terminal/tmux here are just this file's copy of whatever the launching
# bot's own "data_server" config block had (see config_schema.py) - written by
# trader_bot.py's `_write_data_server_config` fresh on every launch, same as
# every other field above. Set them on the BOT config, not by hand-editing
# this auto-written file - it gets fully overwritten the next time any bot
# on this machine starts.
#
# No "pairs" field: which pairs get collected is always dynamic, never
# config-driven - a master's pairlist is the union of every trading-bot
# client's registered interest, and a subserver's is whatever the master
# assigns it. Both start with zero pairs at boot and grow from there.
#
# No "exchange"/"timeframe" fields either, for the same reason: which
# exchange/timeframe a master serves is dynamic, sourced from whichever
# trader_bot happens to launch it - trader_bot.py's `_setup_data_server_
# client` passes them as `--exchange`/`--timeframes` CLI args to `ensure_
# master_running` (see its `extra_args`), not written into this file. A
# manually-run master/subserver (no trader_bot involved, e.g. run-subserver.
# bat/.sh) supplies them the same way, on the command line, or --exchange/
# --timeframes can be omitted in favor of setting "exchange"/"timeframe"
# directly in the config as a fallback for that manual case.
#
# Exchange/ExchangeResolver (exchange/exchange.py) are deeply coupled to the
# FULL freqtrade-shaped config dict - dozens of direct `config["..."]`
# accesses (dry_run, exchange, stake_currency, entry_pricing/exit_pricing,
# runmode, datadir, dataformat_trades, orderflow{max_candles,...}, ...) that
# aren't practical to audit/special-case one by one. Rather than expose that
# complexity to whoever writes this file, `_expand_minimal_config` fills in a
# complete, sanely-defaulted version of it internally and lets the user only
# ever see/edit the handful of fields above. This expanded dict is never
# written back to disk.

_DEFAULT_TIMEFRAME = "15m"


def _load_minimal_config(path: str) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    try:
        minimal = json.loads(text)
    except json.JSONDecodeError as e:
        raise OperationalException(f'Config file "{path}" is not valid JSON: {e}') from e
    if not isinstance(minimal, dict):
        raise OperationalException(f'Config file "{path}" must contain a JSON object.')
    return minimal


def _expand_minimal_config(minimal: dict[str, Any], args: Any) -> Config:
    from VulcanTrader.config.directory_operations import create_datadir, create_userdata_dir
    from VulcanTrader.enums import RunMode

    # exchange has no home in the config file itself (see schema comment
    # above) - always supplied dynamically, either via --exchange (the normal
    # path: trader_bot.py passes it when auto-launching a master) or, for a
    # manually-run master/subserver with no bot involved, an "exchange" field
    # in the config as a fallback.
    exchange_name = (args.exchange or minimal.get("exchange") or "").lower()
    if not exchange_name:
        raise OperationalException(
            "No exchange specified - pass --exchange (trader_bot.py does this "
            'automatically) or set "exchange" in the config file.'
        )
    data_format = minimal.get("format", "feather")
    if data_format not in ("feather", "parquet"):
        raise OperationalException(f'"format" must be "feather" or "parquet", got {data_format!r}.')

    user_data_dir = create_userdata_dir(
        args.user_data_dir or minimal.get("user_data_dir") or "user_data", create_dir=True
    )
    # Deliberately no "pairs" from the config (see schema comment above) - only
    # an explicit `--pairs` CLI override (standalone/debugging use) seeds a
    # starting pairlist; otherwise both master and subserver boot with zero
    # pairs and grow only via client registration / master assignment.
    starting_pairs = list(args.pairs or [])
    config: Config = {
        "dry_run": True,  # data_server never places orders - always a read-only collector
        "stake_currency": "",
        "fee": 0.0,
        "trading_mode": "futures" if minimal.get("funding_rate") else "spot",
        "margin_mode": "isolated" if minimal.get("funding_rate") else "",
        "cancel_open_orders_on_exit": False,
        "unfilledtimeout": {"entry": 10, "exit": 10, "exit_timeout_count": 0, "unit": "minutes"},
        "entry_pricing": {"price_side": "same", "use_order_book": True, "order_book_top": 1},
        "exit_pricing": {"price_side": "same", "use_order_book": True, "order_book_top": 1},
        "orderflow": {
            "cache_size": 1500, "max_candles": 1500, "scale": 0.0,
            "stacked_imbalance_range": 3, "imbalance_volume": 1, "imbalance_ratio": 3.0,
        },
        "exchange": {
            "name": exchange_name,
            "key": "",
            "secret": "",
            "ccxt_config": {"enableRateLimit": True, "rateLimit": 1000},
            "ccxt_async_config": {},
            "use_public_trades": bool(minimal.get("orderflow", False)),
            "pair_whitelist": starting_pairs,
            "pair_blacklist": [],
        },
        "pairlists": [{"method": "StaticPairList"}],
        # dynamic_expand_pairlist (used by DataCollector to fall back to the
        # exchange's tradable markets when a caller passes no explicit
        # `pairs`) reads this TOP-LEVEL key directly, not exchange.
        # pair_whitelist - normally populated by Configuration._resolve_pairs_
        # list(), which this minimal-config path doesn't go through.
        "pairs": starting_pairs,
        "timeframe": args.timeframes[0] if args.timeframes else minimal.get("timeframe", _DEFAULT_TIMEFRAME),
        "timeframes": args.timeframes or [minimal.get("timeframe", _DEFAULT_TIMEFRAME)],
        "dataformat_ohlcv": data_format,
        "dataformat_trades": data_format,
        "data_server": {"persist_to_disk": bool(minimal.get("persist_to_disk", True))},
        "user_data_dir": user_data_dir,
        "runmode": RunMode.UTIL_EXCHANGE,
    }
    if args.exchange:
        config["exchange"]["name"] = args.exchange.lower()
    config["datadir"] = create_datadir(config, args.datadir or minimal.get("datadir"))

    from VulcanTrader.exchange.check_exchange import check_exchange
    check_exchange(config, check_for_bad=False)

    return config


def _supervised_run(
    build_and_run: Callable[[threading.Event], None], stop_event: threading.Event, label: str
) -> None:
    """Keep retrying `build_and_run` (with capped exponential backoff) until
    `stop_event` is set, so a transient failure - the exchange unreachable at
    startup, a port bind race right after a restart, a disk hiccup, or any bug
    that raises past the per-tick safety nets inside DataCollector - never takes
    the whole daemon down. Only an explicit stop (SIGINT/SIGTERM) does. Each
    retry rebuilds everything (exchange, collector, server/forwarder) from
    scratch, since whatever wedged state caused the failure lives on those
    objects, not here."""
    backoff = 2.0
    while not stop_event.is_set():
        try:
            build_and_run(stop_event)
            return  # build_and_run only returns after an intentional stop
        except Exception:
            logger.exception("%s crashed unexpectedly - restarting in %.0fs", label, backoff)
            stop_event.wait(backoff)
            backoff = min(backoff * 2, 60.0)


def _run_standalone(config: Config, args: Any, stop_event: threading.Event) -> None:
    persist_to_disk = bool(config.get("data_server", {}).get("persist_to_disk", True))
    collector = DataCollector(config, persist_to_disk=persist_to_disk)
    try:
        collector.run_forever(poll_interval=args.poll_interval, stop_event=stop_event)
    finally:
        collector.close()


def _run_master(config: Config, args: Any, stop_event: threading.Event) -> None:
    # Single switch for BOTH halves of what this master persists: regular OHLCV
    # for every pair (already unconditional before this flag existed) and
    # funding-rate history on futures pairs (new - see DataCollector's
    # `collect_funding_rate_history`). False skips constructing real
    # datahandlers at all, so DataCache's own "handler is None -> memory-only"
    # branch (merge_ohlcv/merge_trades) takes care of disabling persistence -
    # no separate on/off branch needed here.
    persist_to_disk = bool(config.get("data_server", {}).get("persist_to_disk", True))
    data_handler = (
        get_datahandler(config["datadir"], config.get("dataformat_ohlcv", "feather"))
        if persist_to_disk else None
    )
    trades_data_handler = (
        get_datahandler(config["datadir"], config.get("dataformat_trades", "feather"))
        if persist_to_disk else None
    )
    trading_mode = config.get("trading_mode", TradingMode.SPOT)
    cache = DataCache(
        data_handler=data_handler,
        trades_data_handler=trades_data_handler,
        trading_mode=trading_mode,
    )
    exchange_name = config["exchange"]["name"]

    # `registry` (constructed below, after `collector`/`distributor` it
    # depends on) is referenced here by name only - these hooks aren't
    # actually CALLED until `collector.run_forever()` starts at the very
    # bottom of this function, well after `registry` is assigned, so the
    # normal Python closure late-binding rule applies cleanly.
    def _on_ohlcv(pair_key: PairWithTimeframe, df: DataFrame) -> None:
        pair, timeframe, candle_type = pair_key
        ct_value = candle_type_value(candle_type)
        cache.merge_ohlcv(exchange_name, pair, timeframe, ct_value, df)
        # Push the fresh tick straight to every client watching this pair -
        # "webhook" style - instead of making each of them wait for their own
        # next poll to notice it landed. Publishes the full MERGED series (a
        # cheap in-memory read-back, cache.merge_ohlcv already updated it),
        # not just the raw new rows in `df` - so a receiving client can
        # replace its local copy outright instead of needing its own merge
        # logic to reconstruct what the master already computed once.
        merged = cache.get_ohlcv(exchange_name, pair, timeframe, ct_value)
        registry.publish_ohlcv(exchange_name, pair, timeframe, ct_value, merged if merged is not None else df)

    def _on_funding_rate(pair: str, payload: dict) -> None:
        cache.set_funding_rate(exchange_name, pair, payload)
        registry.publish_funding_rate(exchange_name, pair, payload)

    def _on_trades(pair: str, df: DataFrame) -> None:
        cache.merge_trades(exchange_name, pair, df)
        merged = cache.get_trades(exchange_name, pair)
        registry.publish_trades(exchange_name, pair, merged if merged is not None else df)

    # The master's own local collection shares its datadir with `cache`'s
    # data_handlers, so disk persistence is left entirely to the cache to avoid
    # two independent in-memory copies racing to write the same files.
    # `collect_funding_rate_history` is set explicitly (not left to the
    # constructor's own default) because that default reads `persist_to_disk`
    # to decide - which is unconditionally False right here for the reason
    # above, even though `cache` above may well be persisting for real.
    # Offload work to connected subservers automatically: split the target pair
    # list across the master itself + however many subservers are currently
    # connected, and rebalance every time a subserver joins or leaves. Each
    # worker's own DataCollector (below) rotates through its share in batches
    # capped at this exchange's empirically safe pair count (see
    # EXCHANGE_MAX_SAFE_PAIRS / VulcanTrader/ratelimit_probe.py) - with zero
    # subservers connected, the master just rotates through the whole list
    # itself on a slower cadence rather than dropping the excess.
    max_pairs = args.max_pairs_per_worker or max_safe_pairs_for(exchange_name)

    collector = DataCollector(
        config,
        on_ohlcv=_on_ohlcv,
        on_funding_rate=_on_funding_rate,
        on_trades=_on_trades,
        persist_to_disk=False,
        collect_funding_rate_history=(persist_to_disk and trading_mode == "futures"),
        max_pairs_per_batch=max_pairs,
    )
    distributor = WorkDistributor(
        exchange_name,
        _volume_sorted_pairs(collector.pairs, collector.exchange),
        max_pairs,
        on_master_pairs=collector.set_pairs,
    )

    # Trading-bot clients (trader_bot.py) register which pairs they want watched;
    # the registry unions every currently-registered client's interest with the
    # master's own static config pairs (baseline, possibly empty) and feeds that
    # into the distributor above - so N trader_bot processes watching overlapping
    # pairs collapse into one deduped collection job instead of N independent ones.
    static_pairs = list(collector.pairs)

    def _on_interest_changed(client_pairs: list[str], want_funding_rate: bool, want_trades: bool) -> None:
        union = set(static_pairs) | set(client_pairs)
        distributor.update(_volume_sorted_pairs(list(union), collector.exchange))
        if want_funding_rate:
            collector.collect_funding_rate = True
        if want_trades:
            collector.collect_trades = True

    registry = ClientInterestRegistry(on_union_changed=_on_interest_changed)

    # Reuses the collector's own Exchange instance (already initialised,
    # already the exchange this whole master is for) rather than creating a
    # second connection just for on-demand ticker fetches - fetch_ticker()
    # uses the SYNC ccxt client (Exchange._api), a separate object from the
    # ASYNC one (_api_async) the OHLCV collection loop drives, so the two
    # never contend over the same in-flight request.
    ticker_fetcher = TickerFetcher(collector.exchange, cache, exchange_name)

    server = MasterServer(
        cache, host=args.host, client_port=args.port, subserver_port=args.subserver_port,
        distributor=distributor, registry=registry, ticker_fetcher=ticker_fetcher,
    )
    server.start()

    try:
        collector.run_forever(poll_interval=args.poll_interval, stop_event=stop_event)
    finally:
        collector.close()
        server.stop()


def _run_subserver(config: Config, args: Any, stop_event: threading.Event) -> None:
    exchange_name = config["exchange"]["name"]
    # on_assign_pairs is wired in below, once `collector` exists - the forwarder
    # has to exist first since collector's own hooks (forward_ohlcv etc.) need it.
    forwarder = SubserverForwarder(
        args.master_host, args.subserver_port, exchange_name, name=args.name
    )

    collector = DataCollector(
        config,
        on_ohlcv=forwarder.forward_ohlcv,
        on_funding_rate=forwarder.forward_funding_rate,
        on_trades=forwarder.forward_trades,
        # Same rotation cap the master applies to its own local share (see
        # _run_master) - a subserver's assigned share can also exceed one
        # exchange-safe batch, and needs the same tick-by-tick rotation
        # instead of trying to fetch it all in a single refresh_latest_ohlcv().
        max_pairs_per_batch=args.max_pairs_per_worker or max_safe_pairs_for(exchange_name),
    )
    forwarder.on_assign_pairs = collector.set_pairs
    forwarder.start()

    try:
        collector.run_forever(poll_interval=args.poll_interval, stop_event=stop_event)
    finally:
        collector.close()
        forwarder.stop()


def main(argv: list[str] | None = None) -> int:
    import argparse

    from VulcanTrader.util.logger import setup as setup_logging

    parser = argparse.ArgumentParser(
        description="Collect OHLCV/funding-rate/orderflow data standalone, or as a "
        "networked master or subserver."
    )
    parser.add_argument(
        "--mode", choices=["standalone", "master", "subserver"], default=None,
        help="Defaults to the config file's \"is_subserver\" (true -> subserver, "
        "false/absent -> master). \"standalone\" (no networking) must be given explicitly.",
    )
    parser.add_argument("-c", "--config", required=True, help="Path to the data_server config file.")
    parser.add_argument("--user-data-dir", dest="user_data_dir", help="user_data directory.")
    parser.add_argument("-p", "--pairs", nargs="+", help="Pairs to collect (default: from config).")
    parser.add_argument(
        "-t", "--timeframes", nargs="+", help="Timeframes to collect (default: config timeframe)."
    )
    parser.add_argument("-d", "--datadir", help="Override OHLCV data directory.")
    parser.add_argument("--exchange", help="Override exchange name.")
    parser.add_argument(
        "--once", action="store_true", help="Collect a single round and exit (standalone mode)."
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        help="Fixed poll interval in seconds (default: align to the shortest timeframe).",
    )
    parser.add_argument("--name", help="Identify this subserver to the master (subserver mode).")
    # master-only - fall back to the config file's "host"/"port"/"subserver_port"
    # when not given on the CLI; None here (not a hardcoded default) so that
    # fallback can actually take effect. Resolved below, once the raw config
    # dict is loaded.
    parser.add_argument("--host", default=None, help="Bind host for master listeners.")
    parser.add_argument("--port", type=int, default=None, help="Client-facing port (master mode).")
    parser.add_argument(
        "--subserver-port", type=int, default=None, help="Subserver-facing port (master mode)."
    )
    parser.add_argument(
        "--max-pairs-per-worker",
        type=int,
        default=None,
        help="Override the per-worker pair cap used to auto-distribute pairs across "
        "connected subservers (master mode). Defaults to the empirically safe count "
        "for this exchange from VulcanTrader/ratelimit_probe.py (EXCHANGE_MAX_SAFE_PAIRS), "
        f"or {DEFAULT_MAX_SAFE_PAIRS} if the exchange isn't in that table.",
    )
    # subserver-only - falls back to the config file's "master_host" (see
    # run-subserver.bat/.sh) when not given on the CLI; None here (not a
    # hardcoded default) so that fallback can take effect. The port to dial
    # is --subserver-port above (shared with master mode's bind port - see
    # the config schema comment: one field, bind-vs-dial depending on role).
    parser.add_argument("--master-host", help="Master server host (subserver mode).")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)

    setup_logging(
        level=logging.DEBUG
        if args.verbose >= 2
        else (logging.INFO if args.verbose else logging.WARNING),
        subdir="data_server",
    )

    raw = _load_minimal_config(args.config)
    config = _expand_minimal_config(raw, args)

    if args.mode is None:
        args.mode = "subserver" if raw.get("is_subserver") else "master"

    if args.mode == "subserver":
        # A subserver dials (master_host, subserver_port) - the SAME port a
        # master binds for incoming subserver connections - never master_port
        # (that's the master's separate client/bot-facing port, irrelevant
        # here). One `--subserver-port` CLI flag/config field serves both
        # roles, matching the config's own single `master_host` field doing
        # double duty as bind-vs-dial address depending on `is_subserver`.
        if not args.master_host:
            args.master_host = raw.get("master_host")
        args.subserver_port = args.subserver_port or raw.get("subserver_port") or 8721
        if not args.name:
            args.name = raw.get("name")
        if not args.master_host:
            parser.error(
                'is_subserver is true but no master_host is set - add "master_host" '
                "to the config file (or pass --master-host) - see run-subserver.bat/.sh"
            )
    elif args.mode == "master":
        args.host = args.host or raw.get("master_host") or "0.0.0.0"
        args.port = args.port or raw.get("master_port") or 8720
        args.subserver_port = args.subserver_port or raw.get("subserver_port") or 8721

    # A one-shot standalone pass is meant to run once and exit - surface any
    # failure directly rather than retrying, so callers (cron, a script) see it.
    if args.mode == "standalone" and args.once:
        collector = DataCollector(config)
        try:
            collector.collect_once()
        finally:
            collector.close()
        return 0

    # Every long-running mode shares one stop_event: signal handlers just flip it,
    # regardless of which collector/server/forwarder instance currently exists, so
    # it keeps working across supervised restarts.
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    runners: dict[str, Callable[[threading.Event], None]] = {
        "standalone": lambda se: _run_standalone(config, args, se),
        "master": lambda se: _run_master(config, args, se),
        "subserver": lambda se: _run_subserver(config, args, se),
    }
    _supervised_run(runners[args.mode], stop_event, args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
