"""
Polymarket exchange integration for VulcanTrader.

Polymarket is a prediction-market platform, not a ccxt-supported exchange. Each
supported binary market (Yes/No, or Up/Down for the recurring price-direction
markets) is exposed here as two independently tradable synthetic pairs, one
per outcome token, e.g. "SOMESLUG-YES/USDC" and "SOMESLUG-NO/USDC", or
"SOMESLUG-UP/USDC" and "SOMESLUG-DOWN/USDC".
Prices are bounded [0, 1] (probability), always collateralized in USDC.

This module covers market discovery (Polymarket's public Gamma API),
historical/live price data (Polymarket's public CLOB API), dry-run paper
trading, and - since real order placement needs wallet-based EIP-712 signing
- real LIVE trading via Polymarket's official `polymarket-client` SDK
(imports as `polymarket`, PyPI package `polymarket-client`).

NOT `py-clob-client`. That was this project's original SDK; Polymarket
archived it 2026-05-25 ("no longer functional and should not be used for new
or existing integrations"). It hardcodes USDC.e collateral with no pUSD
support, so every order it signs comes back `400 invalid order version,
please use the latest clob-client` - there is no upgrade, 0.34.6 is the last
release and the repo (github.com/Polymarket/py-sdk) is dead. `polymarket`'s
`SecureClient` also fixes the pUSD-under-reporting bug that used to force an
on-chain balance workaround here (see git history for `_pusd_balance_of` if
that's ever needed again for some other collateral-migration surprise).

Live trading is opt-in and gated hard, because this is the one place in this
module a bug moves real money:
  - `dry_run: false` alone is not enough - `exchange.polymarket_enable_live_trading`
    must ALSO be explicitly `true` in the config, so a stray/forgotten
    `dry_run` flip can't silently start placing real orders.
  - The signing key is read from the `POLYMARKET_PRIVATE_KEY` environment
    variable only - never from a config file, never logged - see
    `Polymarket._get_secure_client`.
  - `additional_exchange_init` refuses to start live trading at all unless
    Polymarket already shows a non-zero collateral balance AND a non-zero
    allowance for this wallet (read-only check - this bot never submits the
    allowance-approval transaction itself; see that method's docstring for
    how to set it).
See `PolymarketCCXTAdapter`'s `create_order`/`cancel_order`/`fetch_order`/
`fetch_balance` and `Polymarket.additional_exchange_init`/`_get_secure_client`.

MARKET-ORDER SIZING IS SIDE-ASYMMETRIC - the bug that motivated writing this
note. `SecureClient.place_market_order` takes `amount` (USDC to spend) for
BUY and `shares` (shares to sell) for SELL - passing the wrong one for a
given side is rejected client-side (`UserInputError: amount is required for
BUY market orders.`) before any network call. See `create_order` below for
the exact mapping, and use `making_amount`/`taking_amount` off the response
for actual fill price/cost - never the requested/quoted price, since a
marketable FAK order routinely fills better than its `max_price`/`min_price`
ceiling/floor.

KNOWN LIVE-TRADING CAVEAT - orphaned orders. If `place_market_order` succeeds
on Polymarket's side but the HTTP response is lost (timeout/connection
drop), this bot raises and never records the trade, while the order may
really be resting or already filled on the CLOB. freqtrade's recovery path
for that (`trader_bot.handle_onexchange_order` -> `Exchange.fetch_orders`) is
a no-op here: `SecureClient` exposes open orders (`list_open_orders`) and a
single order by id (`get_order`), but no closed/historical-order-history
endpoint to *discover* an order whose id was never recorded, so
`_fetch_orders_emulate` finds nothing and returns []. Nothing is silently
mis-recorded, but such an order/position is invisible to the bot until
cancelled or closed manually on Polymarket's own UI - worth checking there
after any run that logged order-placement errors.

REDEMPTION IS SEPARATE FROM THE CCXT ADAPTER. Once a market resolves, shares
of the winning outcome are worth $1 each but sit inert until redeemed for
USDC - ccxt has no concept of this, so it isn't wired into the normal
trade-exit path. Call `Polymarket.redeem_position(condition_id=...)`
directly (from a strategy's `custom_exit`/a maintenance script/the console)
for a resolved market's position; see that method's docstring.

VERIFICATION STATUS: this migration (from `py-clob-client` to
`polymarket-client`) has NOT been run against a live funded wallet by
whoever last edited this file - every signature, field name, and exception
type below was checked against the installed `polymarket-client==0.6.0`
package directly (`inspect.signature`/`model_fields`/reading
`polymarket/_internal/actions/orders/market.py`'s validation source, not
just its docs), and the BUY-vs-SELL sizing and `making_amount`/
`taking_amount` mapping were cross-checked against that source's own
maker/taker construction - but none of create_order/cancel_order/
fetch_order/fetch_balance/redeem_position have executed a real order or
redemption. Verify each with a small real order/redemption before relying on
them for size.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
import ccxt
import pandas as pd
import requests
from ccxt import TICK_SIZE
from pandas import DataFrame

if TYPE_CHECKING:
    from polymarket import SecureClient

from VulcanTrader.constants import DEFAULT_DATAFRAME_COLUMNS
from VulcanTrader.data.converter import ohlcv_to_dataframe
from VulcanTrader.enums import CandleType, MarginMode, TradingMode
from VulcanTrader.exchange.common import retrier, retrier_async
from VulcanTrader.exchange.exchange import Exchange
from VulcanTrader.exchange.exchange_types import OrderBook, Tickers, TraderHas
from VulcanTrader.exchange.exchange_utils_timeframe import (
    timeframe_to_minutes,
    timeframe_to_resample_freq,
)
from VulcanTrader.util import FtTTLCache
from VulcanTrader.util.datetime_helpers import dt_ts
from VulcanTrader.util.exceptions import DDosProtection, OperationalException, TemporaryError

# Default cap on concurrent CLOB HTTP requests in flight at once, across all
# pairs (config-tunable via exchange.polymarket_max_concurrent). The base
# Exchange class fans out one coroutine per pair and batches up to 100 of
# them concurrently (VulcanTrader/exchange/exchange.py's chunks(..., 100) in
# _async_get_historic_ohlcv) with zero awareness of CLOB's real per-IP rate
# limit - verified live that firing ~100 concurrent /prices-history requests
# triggers immediate, sustained 429s. This semaphore is the actual throttle;
# the 100-wide batching above it is effectively just an upper bound.
_DEFAULT_CLOB_MAX_CONCURRENT = 8


logger = logging.getLogger(__name__)

_GAMMA_API_BASE = "https://gamma-api.polymarket.com"
_CLOB_API_BASE = "https://clob.polymarket.com"
_QUOTE_CURRENCY = "USDC"
# Gamma's /markets endpoint silently caps its real page size at 100 regardless
# of the `limit` value requested (verified live - `limit=500` still returns
# only 100 rows per page), so this must match that real cap or pagination
# stops after page 1 (a page shorter than _GAMMA_PAGE_LIMIT is treated as the
# last page).
_GAMMA_PAGE_LIMIT = 100
# Default bulk-discovery depth (config-overridable via
# exchange.polymarket_max_gamma_pages) - see _fetch_gamma_markets_paginated.
_GAMMA_DEFAULT_MAX_PAGES = 20  # 20 * 100 = 2k most-liquid markets
_GAMMA_MAX_PAGES = 200  # hard absolute ceiling regardless of config: 200 * 100 = 20k markets
_GAMMA_MEM_CACHE_TTL = timedelta(minutes=5)
_HTTP_TIMEOUT_S = 15

# CLOB /prices-history rejects a request whose startTs..endTs span is "too
# long" with a 400 (verified live: fidelity=60min fails at 30d but succeeds at
# 14d; fidelity=1440min fails at 21d but succeeds at 14d - the real boundary
# isn't documented and doesn't cleanly track fidelity or point count, so
# requests are chunked into windows safely under the smallest observed
# failure point rather than relying on a single request.
_CLOB_HISTORY_MAX_SPAN_S = 13 * 86400

# Real per-trade sizes for volume. NOT clob.polymarket.com/trades - that one
# requires an authenticated API key (L1/L2, wallet-derived - verified live:
# 401 "Unauthorized/Invalid api key" even for a public GET), i.e. exactly the
# signing infrastructure this module deliberately doesn't carry (see the
# module docstring). data-api.polymarket.com/trades is a SEPARATE, public,
# unauthenticated endpoint with the same information (verified live). It's
# scoped by `market` (the conditionId, NOT a token id - covers both outcome
# tokens' trades together, filtered client-side by `asset`==token_id below),
# paginated newest-first via `limit`/`offset` (limit up to at least 5000
# verified live), and `after=<unix_s>` genuinely filters server-side
# (verified). There's a hard offset ceiling somewhere around 15-20k
# (verified live: offset=15000 returned HTTP 400 on a ~15k-trade market) -
# _TRADES_MAX_PAGES stays comfortably above what any single fetch needs to
# reach that ceiling, and hitting it is treated as "that's everything
# reachable", not an error worth retrying.
_TRADES_API_BASE = "https://data-api.polymarket.com"
_TRADES_PAGE_LIMIT = 5000
_TRADES_MAX_PAGES = 6

# -- live trading (real order placement) --------------------------------
# Deliberately env-var-only (never a config key) - see the module docstring
# and Polymarket._get_secure_client.
_POLYMARKET_PRIVATE_KEY_ENV = "POLYMARKET_PRIVATE_KEY"
_ALLOWANCE_HELP_URL = "https://docs.polymarket.com/developers/CLOB/orders"
# Polymarket's collateral is pUSD; balances/amounts move over the SDK in
# raw base units (6dp) - see PolymarketCCXTAdapter.fetch_balance and
# create_order.
_PUSD_DECIMALS = 1_000_000
# Two floors bind on every order at once: Polymarket's own per-market
# minimum share count (meta.min_order_size, usually 5) and this bot's own
# minimum USDC notional - a 5-share order on a $0.05 token is a $0.25 spend,
# which the CLOB rejects outright ("invalid amount for a marketable BUY
# order ($0.05), min size: 1") even though it clears the share-count floor.
# See create_order.
_MIN_ORDER_NOTIONAL_USDC = 1.00


# --------------------------------------------------------------------------
# Pure helpers - no network, independently testable.
# --------------------------------------------------------------------------


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_json_field(raw: Any) -> list:
    """Gamma encodes `outcomes`/`outcomePrices`/`clobTokenIds` as JSON strings."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _slugify_pair(slug: str, outcome_label: str) -> str:
    base = f"{slug}-{outcome_label}".upper()
    return f"{base}/{_QUOTE_CURRENCY}"


def _iso_to_expiry_ms(iso_str: str | None) -> int | None:
    """Gamma's `endDate` (e.g. "2026-12-31T00:00:00Z") -> unix ms, matching
    ccxt's `market["expiry"]` convention for expiring instruments."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return None


def _pair_to_slug_guess(pair: str) -> tuple[str, str] | None:
    """Best-effort reverse of `_slugify_pair`, used only as a last resort to
    re-resolve a pair whose market has dropped out of the active listing."""
    base = pair.split("/")[0] if "/" in pair else pair
    for label in ("YES", "NO", "UP", "DOWN"):
        suffix = f"-{label}"
        if base.upper().endswith(suffix):
            slug = base[: -len(suffix)].lower()
            return (slug, label) if slug else None
    return None


# The two outcome-label pairs v1 knows how to expose as tradable pairs.
# Yes/No covers ordinary prediction markets; Up/Down covers Polymarket's
# recurring price-direction markets (e.g. "Bitcoin Up or Down - ... 3PM ET").
# Both are simple complementary binaries - see `_market_to_metas`.
_SUPPORTED_OUTCOME_SETS = ({"yes", "no"}, {"up", "down"})

# Polymarket's fast-rotating crypto Up/Down markets (5m/15m/1h/4h) each get a
# fresh, uniquely-timestamped slug every interval, e.g.
# "btc-updown-5m-1786824600" - a market open right now is closed and gone by
# the next refresh. A `pair_whitelist` entry naming one specific instance
# would go stale within minutes, so these are whitelisted by *series base*
# instead - e.g. "btc-updown-5m" - and re-resolved to whichever instance is
# currently open on every `reload_markets()` call. See
# `_resolve_recurring_series`.
_RECURRING_SERIES_RE = re.compile(r"^(?P<coin>[a-z0-9]+)-updown-(?P<interval>\d+[mhd])$")


def _is_supported_binary_market(m: dict[str, Any]) -> bool:
    """v1 only supports plain complementary-binary markets (Yes/No, Up/Down)
    with an active order book - categorical/multi-outcome markets don't fit
    the one-pair-per-outcome scheme."""
    if m.get("archived"):
        return False
    if not m.get("enableOrderBook", True):
        return False
    if not m.get("slug"):
        return False
    outcomes = [str(o).strip().lower() for o in _parse_json_field(m.get("outcomes"))]
    token_ids = _parse_json_field(m.get("clobTokenIds"))
    return (
        len(outcomes) == 2 and set(outcomes) in _SUPPORTED_OUTCOME_SETS and len(token_ids) == 2
    )


def _aggregate_trades_to_volume(trades: list[dict[str, Any]], timeframe: str) -> pd.Series:
    """Resample raw Data API trades (`{"timestamp": unix_s, "size": float, ...}`)
    into per-bucket volume (sum of trade sizes), using the exact same bucket
    boundaries `_resample_clob_history_to_ohlcv` uses for price - the two are
    joined by bucket timestamp there. Returns an empty Series (not an error)
    for no trades, so a trades-fetch failure just means volume=0 for that
    stretch rather than breaking the price candles."""
    if not trades:
        return pd.Series(dtype=float)
    try:
        timestamps = [int(t["timestamp"]) for t in trades]
        sizes = [float(t["size"]) for t in trades]
    except (KeyError, TypeError, ValueError):
        return pd.Series(dtype=float)
    if not timestamps:
        return pd.Series(dtype=float)

    index = pd.to_datetime(timestamps, unit="s", utc=True)
    series = pd.Series(sizes, index=index).sort_index()
    freq = timeframe_to_resample_freq(timeframe)
    return series.resample(freq).sum()


def _resample_clob_history_to_ohlcv(
    history: list[dict[str, Any]],
    timeframe: str,
    volume_by_bucket: pd.Series | None = None,
) -> list[list[float]]:
    """Resample CLOB `/prices-history` tick data (`{"t": unix_s, "p": price}`)
    into OHLCV candle rows `[ts_ms, open, high, low, close, volume]`.

    `volume_by_bucket` (see `_aggregate_trades_to_volume`) supplies real
    per-bucket trade volume, looked up by the same bucket timestamp; a bucket
    missing from it (no trades, or volume wasn't fetched at all) gets 0.0 -
    CLOB's price-history endpoint itself carries no trade-size data, so
    volume is only ever real when the caller separately fetched trades.
    Buckets with no ticks are carried forward flat (open=high=low=close=prior
    close) so gaps in a thinly-traded market don't produce NaN candles - the
    first bucket before any tick has occurred is dropped instead of guessed.
    """
    if not history:
        return []
    try:
        timestamps = [int(pt["t"]) for pt in history]
        prices = [float(pt["p"]) for pt in history]
    except (KeyError, TypeError, ValueError):
        return []
    if not timestamps:
        return []

    index = pd.to_datetime(timestamps, unit="s", utc=True)
    series = pd.Series(prices, index=index).sort_index()
    freq = timeframe_to_resample_freq(timeframe)
    ohlc = series.resample(freq).ohlc()
    ohlc["close"] = ohlc["close"].ffill()
    for col in ("open", "high", "low"):
        ohlc[col] = ohlc[col].fillna(ohlc["close"])
    ohlc = ohlc.dropna(subset=["close"])

    rows: list[list[float]] = []
    for row_ts, row in ohlc.iterrows():
        volume = 0.0
        if volume_by_bucket is not None and row_ts in volume_by_bucket.index:
            vol_val = volume_by_bucket.loc[row_ts]
            if pd.notna(vol_val):
                volume = float(vol_val)
        rows.append(
            [
                int(row_ts.timestamp() * 1000),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                volume,
            ]
        )
    return rows


@dataclass
class PolymarketMarketMeta:
    pair: str
    token_id: str
    other_side_token_id: str
    condition_id: str
    slug: str
    outcome_label: str  # "YES"/"NO" or "UP"/"DOWN"
    tick_size: float
    min_order_size: float
    active: bool
    end_date: str | None  # raw ISO8601 string from Gamma's "endDate" - kept as
    # a plain string (not datetime) so the dataclass stays trivially
    # JSON-serializable for the durable pair registry (see _merge_registry).
    raw: dict[str, Any]
    # True for the outcome Gamma lists first (outcomes[0] - "Yes"/"Up"), whose
    # raw bestBid/bestAsk/lastTradePrice apply directly; False for the
    # complementary side, whose pricing is derived as 1-x in get_tickers().
    # Defaulted (rather than required) so PolymarketMarketMeta(**entry) keeps
    # loading pair-registry rows written before this field existed - see
    # _resolve_pair_meta's entry.setdefault() backfill.
    is_primary: bool = True


# --------------------------------------------------------------------------
# ccxt-shaped adapter - self._api / self._api_async for Polymarket.
# --------------------------------------------------------------------------


class PolymarketCCXTAdapter:
    """Minimal ccxt-like adapter for Polymarket, used as `self._api`/
    `self._api_async` on the `Polymarket` exchange.

    Read-only market data (`fetch_ohlcv`, `fetch_ticker`, `calculate_fee`)
    talks to Polymarket's public Gamma/CLOB endpoints exactly as it always
    has - unauthenticated, no key required. Market discovery, tickers and the
    order book are implemented directly on `Polymarket` instead (see below),
    bypassing this adapter entirely.

    Order-management methods (`create_order`/`cancel_order`/`fetch_order`/
    `fetch_balance`) are real, but in dry-run they are still never reached:
    the base Exchange class's `create_dry_run_order`/`fetch_dry_run_order`
    short-circuit before ever touching `self._api`. They only fire when
    `dry_run: false` AND `exchange.polymarket_enable_live_trading: true` -
    see `Polymarket.additional_exchange_init`/`_get_secure_client` and the
    module docstring for the live-trading gate.
    """

    def __init__(self, parent: "Polymarket", *, is_async: bool) -> None:
        self._parent = parent
        self._is_async = is_async

        self.id = "polymarket"
        self.name = "Polymarket"
        self.markets: dict[str, Any] = {}
        self.symbols: list[str] = []
        self.precisionMode = TICK_SIZE
        self.timeframes = {
            "1m": 60,
            "3m": 180,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "2h": 7200,
            "4h": 14400,
            "6h": 21600,
            "12h": 43200,
            "1d": 86400,
            "1w": 604800,
        }
        self.options = {"timeframes": {"spot": self.timeframes}}
        self.has = {
            "fetchOHLCV": True,
            "fetchTicker": True,
            "fetchTickers": True,
            "fetchL2OrderBook": True,
            "fetchTrades": False,
            # Real (live) order management - see create_order/cancel_order/
            # fetch_order/fetch_balance below. Never reached in dry-run: the
            # base Exchange class's create_dry_run_order/fetch_dry_run_order
            # short-circuit before touching self._api regardless of these
            # flags. createMarketOrder is deliberately absent/False - the
            # CLOB is a pure limit order book, there is no market order.
            "createOrder": True,
            "createLimitOrder": True,
            "cancelOrder": True,
            "fetchOrder": True,
            "fetchBalance": True,
        }
        self.features = {"spot": {"fetchOHLCV": {"limit": 1000}}}
        self.session: aiohttp.ClientSession | None = None

    def set_markets_from_exchange(self, other: Any) -> None:
        self.markets = getattr(other, "markets", {}) or {}
        self.symbols = list(self.markets.keys())

    def load_markets(self, *args, **kwargs) -> dict[str, Any]:
        self.markets = self._parent.markets
        self.symbols = list(self.markets.keys())
        return self.markets

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_S)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    @retrier_async
    async def _fetch_prices_history_chunk(
        self,
        session: aiohttp.ClientSession,
        token_id: str,
        start_ts: int,
        end_ts: int,
        fidelity: int,
    ) -> list[dict[str, Any]]:
        params_ = {"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": fidelity}
        async with self._parent._clob_semaphore:
            try:
                async with session.get(
                    f"{_CLOB_API_BASE}/prices-history", params=params_
                ) as resp:
                    if resp.status == 429:
                        # Raising (rather than logging+returning []) lets the base
                        # Exchange class's @retrier_async on _async_get_candle_history
                        # (which calls this via fetch_ohlcv) actually back off and
                        # retry, instead of silently persisting an empty/incomplete
                        # candle set for this chunk.
                        raise DDosProtection(
                            f"Polymarket CLOB rate-limited (429) for token {token_id}"
                        )
                    if resp.status != 200:
                        text = await resp.text()
                        raise TemporaryError(
                            f"Polymarket CLOB /prices-history error {resp.status} for token "
                            f"{token_id}: {text[:200]}"
                        )
                    data = await resp.json()
            except TimeoutError as e:
                # A request timeout raises plain asyncio.TimeoutError (a
                # TimeoutError alias on Python 3.11+), NOT aiohttp.ClientError -
                # verified live: this previously escaped the except clause
                # below entirely, propagated up through fetch_ohlcv into the
                # base Exchange class's asyncio.gather(..., return_exceptions=True)
                # (VulcanTrader/exchange/exchange.py:_async_get_historic_ohlcv),
                # and was silently dropped there with zero retry - producing
                # incomplete candle data with no error surfaced. Converting it
                # to DDosProtection routes it through the same retry/backoff
                # path as a 429.
                raise DDosProtection(
                    f"Polymarket CLOB request timed out for token {token_id}: {e}"
                ) from e
            except aiohttp.ClientError as e:
                raise TemporaryError(
                    f"Polymarket CLOB request failed for token {token_id}: {e}"
                ) from e
        history = data.get("history") if isinstance(data, dict) else None
        return history if isinstance(history, list) else []

    async def _fetch_trades(
        self,
        session: aiohttp.ClientSession,
        condition_id: str,
        token_id: str,
        start_ts: int,
        end_ts: int,
    ) -> list[dict[str, Any]]:
        """Async counterpart of `Polymarket._fetch_trades_sync` - see that
        method's docstring and the `_TRADES_API_BASE` module comment for the
        endpoint's shape/limits. Best-effort: any failure here should not
        take down `fetch_ohlcv`, so errors are swallowed by the caller."""
        all_trades: list[dict[str, Any]] = []
        offset = 0
        for _page in range(_TRADES_MAX_PAGES):
            async with session.get(
                f"{_TRADES_API_BASE}/trades",
                params={
                    "market": condition_id,
                    "limit": _TRADES_PAGE_LIMIT,
                    "after": start_ts,
                    "offset": offset,
                },
            ) as resp:
                if resp.status != 200:
                    break
                page = await resp.json()
            if not isinstance(page, list) or not page:
                break
            all_trades.extend(
                t
                for t in page
                if t.get("asset") == token_id and start_ts <= int(t.get("timestamp", 0)) <= end_ts
            )
            if len(page) < _TRADES_PAGE_LIMIT:
                break
            offset += _TRADES_PAGE_LIMIT
        return all_trades

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[list]:
        meta = self._parent._resolve_pair_meta(symbol)
        if meta is None:
            return []

        fidelity = max(1, timeframe_to_minutes(timeframe))
        now_s = int(datetime.now(tz=UTC).timestamp())
        candle_limit = int(limit or 500)
        span_s = candle_limit * fidelity * 60
        if since is not None:
            start_ts = int(since / 1000)
            end_ts = min(start_ts + span_s, now_s)
        else:
            end_ts = now_s
            start_ts = now_s - span_s

        session = await self._ensure_session()
        history: list[dict[str, Any]] = []
        chunk_start = start_ts
        while chunk_start < end_ts:
            chunk_end = min(chunk_start + _CLOB_HISTORY_MAX_SPAN_S, end_ts)
            history.extend(
                await self._fetch_prices_history_chunk(
                    session, meta.token_id, chunk_start, chunk_end, fidelity
                )
            )
            chunk_start = chunk_end

        volume_by_bucket = None
        try:
            trades = await self._fetch_trades(session, meta.condition_id, meta.token_id, start_ts, end_ts)
            volume_by_bucket = _aggregate_trades_to_volume(trades, timeframe)
        except Exception:
            logger.debug("Polymarket trades fetch failed for %s, volume will be 0", symbol, exc_info=True)

        return _resample_clob_history_to_ohlcv(history, timeframe, volume_by_bucket)

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return self._parent._fetch_ticker_sync(symbol)

    def calculate_fee(
        self,
        symbol,
        type,
        side,
        amount,
        price,
        takerOrMaker="taker",
        params=None,
    ) -> dict[str, Any]:
        # Polymarket charges no explicit per-trade fee on the vast majority of
        # markets (v1 approximation - some markets carry a small maker/taker
        # fee that isn't exposed on the public Gamma/CLOB read endpoints).
        return {"type": takerOrMaker, "currency": _QUOTE_CURRENCY, "rate": 0.0, "cost": 0.0}

    def create_order(self, symbol, type, side, amount, price=None, params=None):
        """Real order placement. Only reached in live mode - see this class's
        docstring.

        `amount`/`price` arrive already rounded to this pair's precision by
        the base Exchange class (amount_to_precision/price_to_precision).
        `amount` is always SHARES (ccxt's base-currency convention) and
        `price` the limit ccxt asked for - Polymarket's own market-order
        primitive (`place_market_order`, a FAK sweep) is side-asymmetric
        though, so this maps ccxt's uniform (amount, price) into whichever
        shape each side actually needs:
          - BUY: `amount` (USDC to spend) = shares * price, `max_price` = the
            ccxt price as a slippage ceiling. Passing `shares` on a BUY is
            rejected client-side (`UserInputError: amount is required for
            BUY market orders.`) before any network call - this bit the
            previous migration attempt and would have failed every live buy.
          - SELL: `shares` = `amount` directly, `min_price` = the ccxt price
            as a slippage floor.
        Two minimums bind on a BUY at once and neither implies the other:
        Polymarket's own per-market share-count floor (`meta.min_order_size`,
        checked by the CLOB) and this bot's own `_MIN_ORDER_NOTIONAL_USDC`
        dollar floor (5 shares of a $0.05 token is a $0.25 spend, which the
        CLOB rejects outright even though it clears the share-count floor).
        The dollar floor is enforced here by rounding the spend UP to it,
        never by silently trading more shares than ccxt asked for.
        """
        if type != "limit":
            # Enforced upstream too (Polymarket.__init__ defaults every
            # order_types key to "limit", and validate_ordertypes rejects
            # "market" since we never declare createMarketOrder) - this is a
            # last-resort guard, not the primary gate.
            raise ccxt.InvalidOrder(
                f"Polymarket has no market-order primitive - only 'limit' orders "
                f"are supported (got '{type}' for {symbol})."
            )
        if price is None:
            raise ccxt.InvalidOrder(f"Polymarket limit orders require a price ({symbol}).")
        if amount is None or amount <= 0:
            raise ccxt.InvalidOrder(f"Polymarket order amount must be positive ({symbol}).")
        meta = self._parent._resolve_pair_meta(symbol)
        if meta is None:
            raise ccxt.InvalidOrder(f"Unknown Polymarket pair {symbol}")

        import polymarket as pm

        # The CLOB hard-rejects any price outside [tick, 1 - tick], and
        # Polymarket markets genuinely trade at those extremes - a
        # near-certain outcome sits at 0.99+, where rounding to precision can
        # land exactly on 1.0. Clamping keeps the order placeable at the best
        # valid price instead of throwing; it can only ever move the
        # max_price/min_price slippage bound in the conservative direction (a
        # buy's ceiling drops, a sell's floor rises), so it can't fill worse
        # than intended.
        tick = meta.tick_size or 0.001
        clamped = min(max(price, tick), 1 - tick)
        if clamped != price:
            logger.info(
                "Polymarket %s: clamped order price %s to %s (tick size %s).",
                symbol, price, clamped, tick,
            )
            price = clamped

        client = self._parent._get_secure_client()
        side_u = "BUY" if side == "buy" else "SELL"
        kwargs: dict[str, Any] = {
            "token_id": meta.token_id,
            "side": side_u,
            "order_type": "FAK",
        }
        if side_u == "BUY":
            spend = round(amount * price, 2)
            if spend < _MIN_ORDER_NOTIONAL_USDC:
                logger.info(
                    "Polymarket %s: raised BUY spend from $%.4f to the $%.2f minimum "
                    "(%s shares @ %s) - the order will therefore buy slightly more "
                    "than %s shares if it fills.",
                    symbol, spend, _MIN_ORDER_NOTIONAL_USDC, amount, price, amount,
                )
                spend = _MIN_ORDER_NOTIONAL_USDC
            kwargs["amount"] = spend
            kwargs["max_price"] = price
        else:
            kwargs["shares"] = amount
            kwargs["min_price"] = price

        try:
            result = client.place_market_order(**kwargs)
        except pm.InsufficientLiquidityError as e:
            # Normal outcome for a FAK sweep, not a bug - the book couldn't
            # fill it within the price bound. Nothing was placed or moved.
            raise ccxt.InvalidOrder(
                f"Polymarket: insufficient liquidity to fill {side} {amount} {symbol} "
                f"within the price bound: {e}"
            ) from e
        except pm.InsufficientAllowanceError as e:
            raise ccxt.InsufficientFunds(
                f"Polymarket: insufficient balance/allowance for {side} {amount} {symbol}: {e}"
            ) from e
        except pm.UserInputError as e:
            # A bug in the kwargs built above, not a market condition -
            # retrying the identical call will fail identically.
            raise ccxt.InvalidOrder(
                f"Polymarket rejected order parameters for {symbol}: {e}"
            ) from e
        except pm.PolymarketError as e:
            raise ccxt.ExchangeError(
                f"Polymarket order placement failed for {symbol}: {e}"
            ) from e

        if not result.ok:
            raise ccxt.InvalidOrder(
                f"Polymarket rejected the order for {symbol}: {result.code} - {result.message}"
            )

        # FAK never rests - "matched" (fully or partially filled, remainder
        # killed) is the only outcome with a real fill; "live"/"delayed" are
        # treated conservatively as not-yet-filled rather than assumed closed.
        # "making"/"taking" are the order's own two legs, not "buy"/"sell" -
        # for a BUY this order is making USDC and taking shares; for a SELL
        # it's the reverse. Using these (the SDK's own settled figures)
        # instead of the requested price matters: a marketable order fills at
        # whatever the book actually offers, which is routinely better (or
        # worse) than the max_price/min_price bound - pricing the fill at the
        # bound instead can misstate realized cost/proceeds substantially.
        filled_now = result.status == "matched"
        if filled_now:
            if side_u == "BUY":
                filled_shares = float(result.taking_amount)
                usdc_amount = float(result.making_amount)
            else:
                filled_shares = float(result.making_amount)
                usdc_amount = float(result.taking_amount)
            avg_price = usdc_amount / filled_shares if filled_shares else price
        else:
            filled_shares = 0.0
            usdc_amount = 0.0
            avg_price = price

        now = datetime.now(tz=UTC)
        return {
            "id": result.order_id,
            "symbol": symbol,
            "price": price,
            "average": avg_price if filled_now else price,
            "amount": amount,
            "cost": usdc_amount if filled_now else 0.0,
            "type": "limit",
            "side": side,
            "filled": filled_shares,
            "remaining": max(amount - filled_shares, 0.0),
            "datetime": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "timestamp": dt_ts(now),
            "status": "closed" if filled_now else "open",
            "fee": {"currency": _QUOTE_CURRENCY, "cost": 0.0, "rate": 0.0},
            "info": {
                "order_id": result.order_id,
                "status": result.status,
                "making_amount": str(result.making_amount),
                "taking_amount": str(result.taking_amount),
                "trade_ids": list(result.trade_ids),
            },
        }

    def cancel_order(self, id, symbol=None, params=None):
        import polymarket as pm

        client = self._parent._get_secure_client()
        try:
            resp = client.cancel_order(order_id=id)
        except pm.PolymarketError as e:
            raise ccxt.ExchangeError(f"Polymarket order cancel failed for {id}: {e}") from e

        if id in resp.not_canceled:
            raise ccxt.InvalidOrder(
                f"Polymarket did not cancel order {id}: {resp.not_canceled[id]}"
            )

        return {
            "id": id,
            "symbol": symbol,
            "status": "canceled",
            "filled": 0.0,
            "remaining": None,
            "fee": None,
            "info": {"canceled": list(resp.canceled), "not_canceled": dict(resp.not_canceled)},
        }

    def fetch_order(self, id, symbol=None, params=None):
        import polymarket as pm

        client = self._parent._get_secure_client()
        try:
            resp = client.get_order(order_id=id)
        except pm.PolymarketError as e:
            raise ccxt.OrderNotFound(f"Polymarket order {id} not found: {e}") from e

        # Field names/casing per the installed polymarket-client's OpenOrder
        # model (polymarket/models/clob/account.py) - NOT verified against a
        # live funded wallet (none was available while building this).
        # Verify carefully against a real small order before relying on this.
        status_raw = resp.status.lower()
        if status_raw == "live":
            status = "open"
        elif status_raw == "matched":
            status = "closed"
        elif status_raw in ("canceled", "cancelled", "expired"):
            status = "canceled"
        else:
            status = "open"

        original_size = float(resp.original_size)
        size_matched = float(resp.size_matched)
        price = float(resp.price)
        # A MATCHED order is fully filled by definition. Trusting a missing or
        # not-yet-propagated size_matched here would report a completed entry
        # as a partial fill, leaving freqtrade's trade amount permanently
        # short of the shares actually held.
        if status == "closed" and size_matched <= 0 and original_size > 0:
            size_matched = original_size

        return {
            "id": resp.id,
            "symbol": symbol,
            "price": price,
            "average": price,
            "amount": original_size,
            "cost": size_matched * price,
            "type": "limit",
            "side": resp.side.lower(),
            "filled": size_matched,
            "remaining": max(original_size - size_matched, 0.0),
            "datetime": resp.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "timestamp": dt_ts(resp.created_at),
            "status": status,
            "fee": {"currency": _QUOTE_CURRENCY, "cost": 0.0, "rate": 0.0},
            "info": {
                "id": resp.id,
                "status": resp.status,
                "side": resp.side,
                "price": str(resp.price),
                "original_size": str(resp.original_size),
                "size_matched": str(resp.size_matched),
                "token_id": resp.token_id,
            },
        }

    def fetch_balance(self, params=None):
        """Wallet balances, in the ccxt shape `Wallets._update_live` expects:
        USDC collateral plus one entry per outcome token currently held.

        The per-token (share) entries are NOT optional bookkeeping - freqtrade
        exits a spot position by looking up the BASE currency's wallet balance
        (`trader_bot._safe_exit_amount` -> `wallets.get_free(base) +
        get_used(base)`), and raises DependencyException("Not enough amount to
        exit trade") when that is 0. Reporting only USDC here would make every
        live exit fail.

        Polymarket's CLOB is non-custodial and checks balance/allowance at
        MATCH time rather than locking funds when an order is posted, so a
        resting order is not reflected in either balance source. `used` is
        therefore derived here from this wallet's own resting orders - USDC
        committed by resting BUYs, shares committed by resting SELLs - so
        `free` is genuinely uncommitted and several concurrent trades can't
        all be sized against the same collateral.

        Note this makes position sizing transiently conservative rather than
        loose: while an entry order is still resting it is counted both in
        `Trade.total_open_trades_stakes()` and in `used` here, and
        `Wallets.get_available_stake_amount()` clamps to `min(..., free)`. The
        double-count disappears the moment the order fills (it stops resting,
        and the USDC has genuinely left the wallet), and erring toward
        under-allocation is the correct direction with real funds.
        """
        client = self._parent._get_secure_client()
        funder = self._parent._funder_address()

        import polymarket as pm

        try:
            resp = client.get_balance_allowance(asset_type="COLLATERAL")
        except pm.PolymarketError as e:
            raise ccxt.ExchangeError(f"Polymarket balance lookup failed: {e}") from e
        usdc_total = resp.balance / _PUSD_DECIMALS
        info: Any = {"funder": funder, "balance_raw": resp.balance}

        usdc_used, shares_used = self._resting_order_commitments(client)
        positions = self._parent._fetch_positions_sync(funder)

        balances: dict[str, Any] = {}
        for token_id, size in positions.items():
            pair = self._parent._pair_for_token_id(token_id)
            if pair is None:
                # A position in something outside this bot's configured
                # whitelist (hand-placed on Polymarket's own UI, or a market
                # that has since dropped out of discovery). Not ours to
                # manage - and mapping it to a base currency name we can't
                # resolve would be meaningless to freqtrade anyway.
                continue
            base = pair.split("/")[0]
            used = min(shares_used.get(token_id, 0.0), size)
            balances[base] = {"free": size - used, "used": used, "total": size}

        usdc_used = min(usdc_used, usdc_total)
        balances[_QUOTE_CURRENCY] = {
            "free": usdc_total - usdc_used,
            "used": usdc_used,
            "total": usdc_total,
        }

        # `free`/`used`/`total` roll-ups are part of the ccxt shape;
        # Exchange.get_balances() pops them and keeps the per-currency dicts.
        return {
            "free": {c: v["free"] for c, v in balances.items()},
            "used": {c: v["used"] for c, v in balances.items()},
            "total": {c: v["total"] for c, v in balances.items()},
            **balances,
            "info": info,
        }

    def _resting_order_commitments(
        self, client: "SecureClient"
    ) -> tuple[float, dict[str, float]]:
        """This wallet's currently-resting orders, as (USDC committed by BUYs,
        {token_id: shares committed by SELLs}) - see `fetch_balance` for why
        this has to be derived rather than read off a balance."""
        import polymarket as pm

        try:
            open_orders = list(client.list_open_orders().iter_items())
        except pm.PolymarketError as e:
            raise ccxt.ExchangeError(f"Polymarket open-order lookup failed: {e}") from e

        usdc_used = 0.0
        shares_used: dict[str, float] = {}
        for order in open_orders:
            remaining = float(order.original_size) - float(order.size_matched)
            if remaining <= 0:
                continue
            if order.side == "BUY":
                usdc_used += remaining * float(order.price)
            else:
                shares_used[order.token_id] = shares_used.get(order.token_id, 0.0) + remaining
        return usdc_used, shares_used

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None


# --------------------------------------------------------------------------
# The exchange itself.
# --------------------------------------------------------------------------


class Polymarket(Exchange):
    """
    Polymarket prediction-market integration. Spot-only (no margin/leverage);
    every outcome token is bought outright with USDC. Data, dry-run, and
    (gated, opt-in) real live trading - see module docstring.
    """

    trader_has: TraderHas = {
        "stoploss_on_exchange": False,
        "order_time_in_force": ["GTC"],
        "trades_pagination": "time",
        "trades_pagination_arg": "since",
        "trades_has_history": False,
        "l2_limit_range": None,
        "l2_limit_range_required": False,
        "ohlcv_has_history": True,
        "ohlcv_partial_candle": True,
        "ohlcv_require_since": False,
        "ohlcv_candle_limit": 500,
        "download_data_parallel_quick": True,
        "tickers_have_quoteVolume": True,
        "tickers_have_percentage": False,
        "tickers_have_bid_ask": True,
        "tickers_have_price": True,
        "ws_enabled": False,
        "needs_trading_fees": False,
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
    ]

    def __init__(self, config, *args, **kwargs) -> None:
        # NOTE: Exchange.__init__ calls reload_markets() -> Polymarket.load_markets()
        # synchronously during construction, so any attribute load_markets() (or
        # anything it calls) touches must exist *before* super().__init__().
        self._pair_meta: dict[str, PolymarketMarketMeta] = {}
        self._gamma_cache: list[dict[str, Any]] | None = None
        self._gamma_cache_ts: datetime | None = None
        self._registry_cache: dict[str, dict[str, Any]] | None = None
        # Shared connection pool for all sync Gamma/CLOB calls (market
        # discovery, order book, ticker fallback, historic OHLCV) - avoids a
        # fresh TCP+TLS handshake per request, which matters a lot once a
        # config whitelists hundreds of pairs.
        self._http_session: requests.Session = requests.Session()
        # Snapshotted once, before _merge_whitelisted_markets ever rewrites
        # exchange.pair_whitelist in place (see that method) - this is what
        # every later _merge_whitelisted_markets() call re-resolves from, so
        # a bare slug/recurring series keeps resolving correctly on every
        # refresh instead of only the first.
        self._original_pair_whitelist: list[str] = list(
            config.get("exchange", {}).get("pair_whitelist") or []
        )
        # Must exist before super().__init__() too: additional_exchange_init()
        # (which reads/sets this via _get_secure_client) runs synchronously
        # from INSIDE super().__init__() (trader_additional_exchange_init),
        # not after it returns.
        self._secure_client: "SecureClient | None" = None

        # Polymarket's CLOB is a pure limit order book - there is no market-
        # order primitive to fall back on, ever. `Exchange.__init__` validates
        # `config["order_types"]` against `exchange_has("createMarketOrder")`
        # (which we never declare) before a strategy gets a chance to
        # override anything, and the JSON-schema default for
        # `order_types.emergency_exit` alone is "market" - so any config that
        # doesn't spell out `order_types` at all (the common case) would
        # otherwise fail construction outright with a ConfigurationError, in
        # BOTH dry-run and live. `setdefault` so an explicit user override
        # (of any individual key) still wins.
        order_types = config.setdefault("order_types", {})
        for _key in ("entry", "exit", "force_entry", "force_exit", "emergency_exit", "stoploss"):
            order_types.setdefault(_key, "limit")
        # REQUIRED_ORDERTYPES (VulcanTrader/constants.py) demands this key be
        # present too - config-level `order_types` fully replaces the
        # strategy's own default dict rather than merging into it, so leaving
        # it out here fails strategy sanity validation with a much less
        # obvious "Order-types mapping is incomplete" error.
        order_types.setdefault("stoploss_on_exchange", False)

        super().__init__(config, *args, **kwargs)

        max_conc = int(
            self._config.get("exchange", {}).get(
                "polymarket_max_concurrent", _DEFAULT_CLOB_MAX_CONCURRENT
            )
            or _DEFAULT_CLOB_MAX_CONCURRENT
        )
        # Bounds concurrent CLOB requests fired by the async adapter's
        # fetch_ohlcv (see _fetch_prices_history_chunk) - the base Exchange
        # class fans out one coroutine per pair in batches of up to 100 with
        # no awareness of CLOB's real rate limit.
        self._clob_semaphore = asyncio.Semaphore(max(1, max_conc))

    # -- lifecycle -----------------------------------------------------

    def _init_ccxt(
        self, exchange_config: dict, sync: bool, ccxt_kwargs: dict
    ) -> PolymarketCCXTAdapter:
        return PolymarketCCXTAdapter(self, is_async=not sync)

    def additional_exchange_init(self) -> None:
        self._config.setdefault("dataformat_ohlcv", "feather")
        self._config.setdefault("dataformat_trades", "feather")
        if self._config["dry_run"]:
            logger.info("Polymarket exchange initialized (data + dry-run only)")
            return

        # dry_run: false alone is deliberately not enough to go live - see
        # the module docstring for why this second, Polymarket-specific flag
        # exists.
        if not self._config.get("exchange", {}).get("polymarket_enable_live_trading", False):
            raise OperationalException(
                'Polymarket live trading requires BOTH "dry_run": false AND '
                '"exchange.polymarket_enable_live_trading": true - the second '
                "flag is a deliberate extra safeguard against starting live "
                'trading by accident. Set "dry_run": true instead to use '
                "Polymarket in paper-trading mode."
            )

        client = self._get_secure_client()
        self._check_live_trading_allowance(client)
        logger.warning(
            "Polymarket LIVE trading enabled - wallet %s (%s) will place REAL "
            "orders with REAL funds.",
            client.wallet,
            client.wallet_type,
        )

    def _get_secure_client(self) -> "SecureClient":
        """Lazily construct (and cache) the signing client used for real
        order placement. Only ever reached from live order-management code
        paths - dry-run never calls this (see PolymarketCCXTAdapter's
        docstring: the base Exchange class's dry-run order machinery
        short-circuits before touching self._api at all).

        The private key is read fresh from the POLYMARKET_PRIVATE_KEY
        environment variable and handed straight to polymarket-client's own
        `SecureClient.create()`, which signs and also derives/exposes the CLOB
        API credentials. It is deliberately never written into self._config
        (which has shown up whole, via config-validation error tracebacks, in
        this bot's own logs during development) and never logged.
        """
        if self._secure_client is not None:
            return self._secure_client

        try:
            from polymarket import SecureClient
        except ImportError as e:
            raise OperationalException(
                "Polymarket live trading requires the 'polymarket-client' package "
                "(pip install polymarket-client) - it's in requirements.txt but "
                "doesn't appear to be installed in this environment."
            ) from e

        private_key = os.environ.get(_POLYMARKET_PRIVATE_KEY_ENV)
        if not private_key:
            raise OperationalException(
                f"Polymarket live trading requires the wallet private key in the "
                f"{_POLYMARKET_PRIVATE_KEY_ENV} environment variable (never in a "
                f"config file, 0x-prefixed hex). It is not set."
            )

        # Proxy/deposit-wallet accounts (the common Polymarket setup) sign
        # with the EOA but fund/hold from a separate wallet address - `wallet`
        # tells the SDK which one to act for, and it infers the signature
        # type from whether this is set at all - unlike the old py-clob-client
        # integration there is no separate signature_type to get wrong here.
        # No `polymarket_proxy_address` configured means plain EOA.
        proxy = str(self._config.get("exchange", {}).get("polymarket_proxy_address") or "").strip()
        try:
            client = SecureClient.create(private_key=private_key, wallet=proxy or None)
        except Exception as e:
            raise OperationalException(
                f"Polymarket live trading: client creation/credential derivation "
                f"failed: {e}"
            ) from e

        self._secure_client = client
        logger.info(
            "Polymarket live trading: connected as wallet %s (%s)",
            client.wallet, client.wallet_type,
        )
        return client

    def _funder_address(self) -> str:
        """Address actually holding tradeable collateral and positions - the
        SDK's own `wallet` property, which is already the deposit/proxy
        wallet when `polymarket_proxy_address` is configured, or the signer
        EOA otherwise (see `_get_secure_client`)."""
        return self._get_secure_client().wallet

    def _pair_for_token_id(self, token_id: str) -> str | None:
        """Reverse of `PolymarketMarketMeta.token_id` - used to key live share
        balances by the pair's base currency (see the adapter's
        `fetch_balance`). Falls back to the durable pair registry so a
        position in a market that has dropped out of active discovery still
        resolves."""
        for pair, meta in self._pair_meta.items():
            if meta.token_id == token_id:
                return pair
        for pair, entry in (self._load_registry() or {}).items():
            if entry.get("token_id") == token_id:
                return pair
        return None

    def _fetch_positions_sync(self, wallet_address: str) -> dict[str, float]:
        """Outcome-token (share) balances for `wallet_address`, as
        {token_id: size}.

        Uses the same public, unauthenticated Data API this module already
        uses for volume (see `_TRADES_API_BASE`) - its `/positions` endpoint
        returns every non-zero position in ONE request, whereas the CLOB's own
        balance endpoint is per-token (`BalanceAllowanceParams` takes a single
        `token_id`) and would cost one authenticated round-trip per whitelisted
        pair on every wallet refresh.

        Note this is indexed data, so it can lag a just-filled order by a
        short window. That direction is safe: a position not yet visible makes
        `_safe_exit_amount` refuse to exit this cycle and retry the next one,
        rather than over-exiting something that isn't there.
        """
        try:
            resp = self._http_session.get(
                f"{_TRADES_API_BASE}/positions",
                params={"user": wallet_address, "limit": 500},
                timeout=_HTTP_TIMEOUT_S,
            )
        except requests.RequestException as e:
            raise TemporaryError(f"Polymarket positions request failed: {e}") from e
        if resp.status_code != 200:
            raise TemporaryError(
                f"Polymarket positions error {resp.status_code}: {resp.text[:200]}"
            )

        positions: dict[str, float] = {}
        for row in resp.json() or []:
            if not isinstance(row, dict):
                continue
            token_id = str(row.get("asset", ""))
            size = _safe_float(row.get("size")) or 0.0
            if token_id and size > 0:
                positions[token_id] = positions.get(token_id, 0.0) + size
        return positions

    def _check_live_trading_allowance(self, client: "SecureClient") -> None:
        """Read-only balance/allowance check, run once at startup.

        This bot deliberately never submits the allowance-approval
        transaction itself - that first on-chain approval stays under the
        user's direct control. Approve collateral spending once via
        Polymarket's own UI (placing any order there prompts it
        automatically) or a one-off script, then restart this bot.
        """
        import polymarket as pm

        try:
            resp = client.get_balance_allowance(asset_type="COLLATERAL")
        except pm.PolymarketError as e:
            raise OperationalException(
                f"Polymarket live trading: couldn't read collateral balance/allowance "
                f"for wallet {client.wallet}: {e}"
            ) from e

        if resp.balance <= 0:
            raise OperationalException(
                f"Polymarket live trading: wallet {client.wallet} has zero collateral "
                f"balance - fund it before starting live trading."
            )
        if not any(v > 0 for v in resp.allowances.values()):
            raise OperationalException(
                f"Polymarket live trading: wallet {client.wallet} has no collateral "
                f"allowance set for the CLOB exchange contract - orders would be "
                f"rejected at match time. Approve spending once via Polymarket's "
                f"own UI (placing any order there prompts it), then restart. "
                f"See {_ALLOWANCE_HELP_URL}."
            )

        logger.warning(
            "Polymarket live trading: wallet=%s (%s), allowance confirmed set, "
            "tradeable balance %.6f pUSD.",
            client.wallet,
            client.wallet_type,
            resp.balance / _PUSD_DECIMALS,
        )

    def redeem_position(
        self, *, condition_id: str | None = None, market_id: str | None = None
    ) -> str:
        """Redeem a resolved market's winning shares for collateral. Provide
        exactly one of `condition_id` (see `PolymarketMarketMeta.condition_id`
        for where to get it from a whitelisted pair) or `market_id`.

        Deliberately separate from the ccxt adapter and from freqtrade's own
        trade-exit path - ccxt has no concept of redemption, and nothing here
        calls this automatically. Once a market resolves, the winning
        outcome's shares are worth $1 each but sit inert (not collateral)
        until redeemed; call this directly - from a strategy's
        `custom_exit`, a maintenance script, or the console - for a resolved
        market this bot (or a human, same wallet) holds a position in.

        Gasless via Polymarket's relayer (no POL needed for gas). Blocks
        until the redemption transaction reaches a terminal on-chain outcome
        (`handle.wait()` takes no timeout - a stuck redemption blocks the
        caller until Polymarket's relayer resolves it).
        """
        if not condition_id and not market_id:
            raise OperationalException("redeem_position requires condition_id or market_id.")
        client = self._get_secure_client()

        import polymarket as pm

        try:
            handle = client.redeem_positions(condition_id=condition_id, market_id=market_id)
            handle.wait()
        except pm.PolymarketError as e:
            raise OperationalException(
                f"Polymarket redemption failed for condition_id={condition_id} "
                f"market_id={market_id}: {e}"
            ) from e

        logger.warning(
            "Polymarket: redeemed condition_id=%s market_id=%s - tx %s",
            condition_id, market_id, handle.transaction_hash,
        )
        return handle.transaction_hash

    def close(self) -> None:
        try:
            self._http_session.close()
        except Exception:
            pass
        super().close()

    @property
    def name(self) -> str:
        return "polymarket"

    @property
    def id(self) -> str:
        return "polymarket"

    @property
    def precisionMode(self) -> int:
        return TICK_SIZE

    # -- market discovery ------------------------------------------------

    def load_markets(self) -> dict[str, Any]:
        raw_markets = self._fetch_gamma_markets()
        self._gamma_markets_to_pairs(raw_markets)
        self._merge_whitelisted_markets()
        return self._markets

    def _merge_whitelisted_markets(self) -> None:
        """Bulk discovery (`_fetch_gamma_markets_paginated`) only pulls the
        top-N most-liquid markets for startup-time reasons - a config
        explicitly naming a market should work regardless of where that
        market ranks by volume. Each `_original_pair_whitelist` entry (the
        config's `exchange.pair_whitelist` as originally written - see
        `__init__`) is one of:
          - a bare market name/slug (no "/") - resolved and expanded into
            BOTH outcome pairs, e.g. "will-trump-win-2024" or
            "WILL-TRUMP-WIN-2024" becomes both `...-YES/USDC` and
            `...-NO/USDC`. This is the recommended way to whitelist a
            specific market: name it once, the token ids are looked up here.
          - a recurring Up/Down series base, e.g. "btc-updown-5m" - resolved
            to whichever timestamped instance of that series is currently
            open (see `_resolve_recurring_series`), re-resolved fresh on
            every call since the specific instance rotates out from under it.
          - an already-fully-qualified pair (`...-YES/USDC` / `...-NO/USDC`)
            - resolved directly if not already discovered.
          - anything else (regex metacharacters - wildcard patterns some
            users write for other exchanges) - left untouched, to be matched
            by the pairlist manager against whatever `self._markets` contains
            exactly like any ccxt exchange.

        `exchange.pair_whitelist` is then overwritten **in place** (`[:] =`,
        not `=`) with the *resolved* pair symbols (e.g.
        "WILL-TRUMP-WIN-2024-YES/USDC"), not left as the bare names above.
        This matters twice over:
        1. `StaticPairList` and the base `verify_whitelist`/`expand_pairlist`
           machinery treat whitelist entries as literal-or-regex matches
           against `exchange.markets` keys - a bare slug or series base
           never equals a "...-YES/USDC"-suffixed key, so left unresolved it
           is silently dropped from the trading whitelist by
           `_whitelist_for_active_markets` (verified live: `exchange.markets`
           had both resolved pairs, but `PairListManager.whitelist` came
           back empty).
        2. `Configuration._load_configuration()`'s `download-data` path takes
           `config["pairs"] = config["exchange"]["pair_whitelist"]` - a
           reference to the list *object* - before this exchange is ever
           constructed (`VulcanTrader/config/configuration.py`). Rebinding
           the key (`self._config["exchange"]["pair_whitelist"] = resolved`)
           would leave that earlier reference pointing at the stale,
           unresolved list, so `download-data` would see zero pairs and fail
           with "No pairs available for download" even though this exchange
           resolved them correctly - verified live, the exact failure mode
           before this used `[:] =`. Backtesting/live are unaffected either
           way since `StaticPairList` re-reads
           `config["exchange"]["pair_whitelist"]` fresh, after construction.

        Resolving from `_original_pair_whitelist` - not from this
        now-being-overwritten field - on every call is what makes
        re-resolution keep working after the first pass (critical for
        recurring series, which must re-target a fresh instance every
        refresh)."""
        resolved: list[str] = []
        for entry in self._original_pair_whitelist:
            if not entry:
                continue
            if "/" in entry:
                # Already a fully-qualified pair name - it's already in its
                # final form for the whitelist either way; resolve it only
                # for the side effect of populating self._markets/_pair_meta
                # when it isn't already known.
                if entry not in self._markets:
                    self._resolve_pair_meta(entry)
                resolved.append(entry)
                continue

            series_match = _RECURRING_SERIES_RE.fullmatch(entry.lower())
            if series_match:
                metas = self._resolve_recurring_series(
                    series_match.group("coin"), series_match.group("interval")
                )
                if metas:
                    resolved.extend(m.pair for m in metas)
                else:
                    resolved.append(entry)  # nothing open right now - keep, retry next refresh
            elif re.fullmatch(r"[A-Za-z0-9_-]+", entry):
                metas = self._resolve_slug_cached(entry.lower())
                if metas:
                    resolved.extend(m.pair for m in metas)
                else:
                    resolved.append(entry)  # unresolved - kept so verify_whitelist logs it
            else:
                resolved.append(entry)  # wildcard/regex - pass through unresolved

        # [:] = , not = : see this method's docstring point 2 - config["pairs"]
        # (download-data's pair source) is a reference to this SAME list
        # object taken before this exchange was constructed. Rebinding the
        # dict key would leave that reference stale; mutating the list's
        # contents in place keeps it valid. setdefault guards a config that
        # never set exchange.pair_whitelist at all (there's then no prior
        # object to preserve identity with, so a fresh list is fine).
        self._config["exchange"].setdefault("pair_whitelist", [])[:] = resolved

    def reload_markets(self, force: bool = False, *, load_leverage_tiers: bool = True) -> None:
        """Base `Exchange.reload_markets()` is deeply ccxt-specific (pickle
        cache keyed off `self._api.id`, calls real ccxt methods our adapter
        doesn't implement) - reimplemented here using the same throttle
        fields (`_last_markets_refresh`/`markets_refresh_interval`)."""
        if (
            not force
            and self._last_markets_refresh > 0
            and (self._last_markets_refresh + self.markets_refresh_interval > dt_ts())
        ):
            return None
        self.load_markets()
        self._last_markets_refresh = dt_ts()

    def _load_async_markets(self, reload: bool = False) -> None:
        if reload:
            self.load_markets()

    def _market_to_metas(self, m: dict[str, Any]) -> list[PolymarketMarketMeta]:
        if not _is_supported_binary_market(m):
            return []
        outcomes = _parse_json_field(m.get("outcomes"))
        token_ids = _parse_json_field(m.get("clobTokenIds"))
        slug = str(m.get("slug"))
        active = bool(m.get("active")) and not m.get("closed") and not m.get("archived")
        tick_size = _safe_float(m.get("orderPriceMinTickSize")) or 0.01
        min_order_size = _safe_float(m.get("orderMinSize")) or 5.0
        condition_id = str(m.get("conditionId", ""))
        end_date = m.get("endDate") or None

        metas: list[PolymarketMarketMeta] = []
        labels = [str(o).strip().upper() for o in outcomes]
        for i, label in enumerate(labels):
            other_index = 1 - i
            metas.append(
                PolymarketMarketMeta(
                    pair=_slugify_pair(slug, label),
                    token_id=str(token_ids[i]),
                    other_side_token_id=str(token_ids[other_index]),
                    condition_id=condition_id,
                    slug=slug,
                    outcome_label=label,
                    tick_size=tick_size,
                    min_order_size=min_order_size,
                    active=active,
                    end_date=end_date,
                    raw=m,
                    is_primary=(i == 0),
                )
            )
        return metas

    def _gamma_markets_to_pairs(self, raw_markets: list[dict[str, Any]]) -> None:
        pair_meta: dict[str, PolymarketMarketMeta] = {}
        markets: dict[str, Any] = {}
        excluded = 0
        for m in raw_markets:
            metas = self._market_to_metas(m)
            if not metas:
                excluded += 1
                continue
            for meta in metas:
                pair_meta[meta.pair] = meta
                markets[meta.pair] = self._build_market_dict(meta)

        self._pair_meta = pair_meta
        self._markets = markets
        self._merge_registry(list(pair_meta.values()))
        logger.info(
            "Polymarket: loaded %d tradable pairs from %d markets (%d excluded - "
            "categorical/non-binary/order-book-disabled/archived)",
            len(markets),
            len(raw_markets),
            excluded,
        )

    def _build_market_dict(self, meta: PolymarketMarketMeta) -> dict[str, Any]:
        return {
            "id": meta.token_id,
            "symbol": meta.pair,
            "base": meta.pair.split("/")[0],
            "quote": _QUOTE_CURRENCY,
            "active": meta.active,
            "type": "spot",
            "spot": True,
            "margin": False,
            "future": False,
            "swap": False,
            "option": False,
            "contract": False,
            "contractSize": 1.0,
            "precision": {"amount": 0.01, "price": meta.tick_size},
            "limits": {
                "amount": {"min": 0.0, "max": None},
                "price": {"min": 0.0, "max": 1.0},
                "cost": {"min": meta.min_order_size, "max": None},
            },
            "taker": 0.0,
            "maker": 0.0,
            # Not a real ccxt "future"/"option" (still spot: {"true", "false"}
            # outright ownership, no leverage) - but every Polymarket market
            # does resolve at a scheduled time, so it's exposed the same way
            # ccxt exposes contract expiry. IStrategy.minutes_to_market_end()
            # (and its confirm_trade_entry/custom_exit defaults gated by
            # market_end_buffer_minutes) read this to stop trading and
            # force-exit ahead of resolution.
            "expiry": _iso_to_expiry_ms(meta.end_date),
            "expiryDatetime": meta.end_date,
            "info": meta.raw,
        }

    # -- Gamma API (market metadata) -------------------------------------

    def _fetch_gamma_markets(self) -> list[dict[str, Any]]:
        now = datetime.now(tz=UTC)
        if (
            self._gamma_cache is not None
            and self._gamma_cache_ts is not None
            and (now - self._gamma_cache_ts) < _GAMMA_MEM_CACHE_TTL
        ):
            return self._gamma_cache

        try:
            markets = self._fetch_gamma_markets_paginated()
        except Exception as e:
            logger.warning("Polymarket Gamma API market fetch failed: %s", e)
            disk = self._load_gamma_disk_cache()
            if disk is not None:
                logger.warning(
                    "Polymarket: using disk-cached market list (%d markets) as fallback",
                    len(disk),
                )
                self._gamma_cache = disk
                self._gamma_cache_ts = now
                return disk
            raise OperationalException(
                f"Polymarket Gamma API market list unavailable and no disk cache "
                f"exists: {e}"
            ) from e

        self._gamma_cache = markets
        self._gamma_cache_ts = now
        self._save_gamma_disk_cache(markets)
        return markets

    def _fetch_gamma_markets_paginated(self) -> list[dict[str, Any]]:
        """Gamma's plain offset-based `/markets?...&offset=N` hard-caps around
        offset ~2000-2500 (`422 offset too large, use /markets/keyset for deeper
        pagination` - verified live against the real API). `/markets/keyset` is
        Gamma's cursor-based alternative for full-catalog traversal: each page
        carries a `next_cursor` token that must be echoed back as `after_cursor`
        on the following request to continue past it.

        There are 10,000+ currently active markets in total (the vast majority
        near-zero-volume long-tail prop questions), and each page costs one
        HTTP round trip, so pulling the *entire* catalog on every
        `reload_markets()` would make bot startup impractically slow. Sorted by
        `volume24hr` descending (verified live to work on this endpoint) and
        capped at `polymarket_max_gamma_pages` (config, default
        `_GAMMA_DEFAULT_MAX_PAGES`), this discovers every market with
        meaningful trading interest while keeping startup bounded. A specific
        market outside this window is still reachable by exact pair name -
        `_resolve_pair_meta()`'s slug-lookup fallback resolves it directly,
        uncapped, on first use (e.g. from `pair_whitelist`)."""
        max_pages = int(
            self._config.get("exchange", {}).get(
                "polymarket_max_gamma_pages", _GAMMA_DEFAULT_MAX_PAGES
            )
            or _GAMMA_DEFAULT_MAX_PAGES
        )
        max_pages = min(max_pages, _GAMMA_MAX_PAGES)

        all_markets: list[dict[str, Any]] = []
        after_cursor: str | None = None
        for _page in range(max_pages):
            params: dict[str, Any] = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": _GAMMA_PAGE_LIMIT,
                "order": "volume24hr",
                "ascending": "false",
            }
            if after_cursor:
                params["after_cursor"] = after_cursor
            data = self._get_gamma_json("/markets/keyset", params)
            page = data.get("markets") if isinstance(data, dict) else None
            if not isinstance(page, list) or not page:
                break
            all_markets.extend(page)
            after_cursor = data.get("next_cursor") if isinstance(data, dict) else None
            if not after_cursor:
                break
        return all_markets

    def _get_gamma_json(self, path: str, params: dict[str, Any]) -> Any:
        for attempt in range(3):
            try:
                resp = self._http_session.get(
                    f"{_GAMMA_API_BASE}{path}", params=params, timeout=_HTTP_TIMEOUT_S
                )
            except requests.RequestException as e:
                if attempt == 2:
                    raise TemporaryError(f"Polymarket Gamma API request failed: {e}") from e
                time.sleep(1 + attempt)
                continue
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    sleep_s = max(1, min(30, int(float(retry_after))))
                except (TypeError, ValueError):
                    sleep_s = 5
                logger.warning("Polymarket Gamma API rate-limited (429), sleeping %ss", sleep_s)
                time.sleep(sleep_s)
                continue
            if resp.status_code != 200:
                raise TemporaryError(
                    f"Polymarket Gamma API error {resp.status_code} for {path}: "
                    f"{resp.text[:200]}"
                )
            return resp.json()
        raise TemporaryError(f"Polymarket Gamma API request to {path} failed after retries")

    @property
    def _gamma_disk_cache_path(self) -> Path:
        datadir = Path(self._config.get("datadir") or "user_data/data/polymarket")
        datadir.mkdir(parents=True, exist_ok=True)
        return datadir / "gamma_markets_cache.json"

    def _load_gamma_disk_cache(self) -> list[dict[str, Any]] | None:
        path = self._gamma_disk_cache_path
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            markets = raw.get("markets")
            return markets if isinstance(markets, list) else None
        except Exception as e:
            logger.warning("Failed to read Polymarket Gamma disk cache %s: %s", path, e)
            return None

    def _save_gamma_disk_cache(self, markets: list[dict[str, Any]]) -> None:
        try:
            payload = {"timestamp": datetime.now(tz=UTC).isoformat(), "markets": markets}
            self._gamma_disk_cache_path.write_text(json.dumps(payload), encoding="utf-8")
        except Exception as e:
            logger.warning("Failed to write Polymarket Gamma disk cache: %s", e)

    # -- durable pair -> token_id registry (survives markets closing) ---

    @property
    def _pair_registry_path(self) -> Path:
        datadir = Path(self._config.get("datadir") or "user_data/data/polymarket")
        datadir.mkdir(parents=True, exist_ok=True)
        return datadir / "polymarket_pair_registry.json"

    def _load_registry(self) -> dict[str, dict[str, Any]]:
        if self._registry_cache is not None:
            return self._registry_cache
        path = self._pair_registry_path
        if not path.exists():
            self._registry_cache = {}
            return self._registry_cache
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            self._registry_cache = loaded if isinstance(loaded, dict) else {}
        except Exception as e:
            logger.warning("Failed to read Polymarket pair registry %s: %s", path, e)
            self._registry_cache = {}
        return self._registry_cache

    def _merge_registry(self, metas: list[PolymarketMarketMeta]) -> None:
        """Append-only: every pair -> token_id mapping ever resolved is kept
        here, even after the market closes, so historical data stays fetchable
        for pairs that have since dropped out of the active Gamma listing."""
        if not metas:
            return
        registry = self._load_registry()
        for meta in metas:
            registry[meta.pair] = asdict(meta)
        self._registry_cache = registry
        try:
            self._pair_registry_path.write_text(json.dumps(registry), encoding="utf-8")
        except Exception as e:
            logger.warning("Failed to write Polymarket pair registry: %s", e)

    def _meta_from_registry_entry(self, pair: str, entry: dict[str, Any]) -> PolymarketMarketMeta:
        """Build a `PolymarketMarketMeta` from a raw registry row and register
        it into `self._pair_meta`/`self._markets`. `entry.setdefault` below
        tolerates registry files written before a `PolymarketMarketMeta` field
        was added (e.g. `end_date`, `is_primary`) - avoids a hard TypeError on
        an older on-disk registry."""
        entry = dict(entry)
        entry.setdefault("end_date", None)
        entry.setdefault("is_primary", entry.get("outcome_label") in ("YES", "UP"))
        meta = PolymarketMarketMeta(**entry)
        self._pair_meta[pair] = meta
        self._markets.setdefault(pair, self._build_market_dict(meta))
        return meta

    def _resolve_pair_meta(self, pair: str) -> PolymarketMarketMeta | None:
        """3-tier lookup: current active load -> durable registry -> last-resort
        slug re-query, so historical data keeps working after a market resolves
        and drops out of `load_markets()`'s active-only listing (and so a
        specific pair the config names works even if it's outside the bulk
        top-N-by-volume discovery window - see `_merge_whitelisted_markets`)."""
        meta = self._pair_meta.get(pair)
        if meta is not None:
            return meta

        entry = self._load_registry().get(pair)
        if entry is not None:
            return self._meta_from_registry_entry(pair, entry)

        parsed = _pair_to_slug_guess(pair)
        if parsed is None:
            return None
        slug, _outcome_label = parsed
        metas = self._resolve_slug(slug)
        return next((m for m in metas if m.pair == pair), None)

    # The two outcome-label pairs a bare slug might have been registered
    # under - see `_slugify_pair`/`_market_to_metas`.
    _SLUG_OUTCOME_LABEL_PAIRS = (("YES", "NO"), ("UP", "DOWN"))

    def _resolve_slug_cached(self, slug: str) -> list[PolymarketMarketMeta]:
        """Like `_resolve_slug`, but checks the durable registry first (an
        O(1) local lookup) before falling back to a live Gamma HTTP call.
        Matters a lot once a whitelist runs into the hundreds of bare slugs
        not covered by bulk discovery (see `_merge_whitelisted_markets`) -
        once a market's been resolved once, by any previous run, every later
        `reload_markets()` reuses the cached token ids for it instead of
        re-querying Gamma on every `markets_refresh_interval` tick."""
        registry = self._load_registry()
        for yes_label, no_label in self._SLUG_OUTCOME_LABEL_PAIRS:
            yes_pair = _slugify_pair(slug, yes_label)
            no_pair = _slugify_pair(slug, no_label)
            yes_entry = registry.get(yes_pair)
            no_entry = registry.get(no_pair)
            if yes_entry is not None and no_entry is not None:
                return [
                    self._meta_from_registry_entry(yes_pair, yes_entry),
                    self._meta_from_registry_entry(no_pair, no_entry),
                ]
        return self._resolve_slug(slug)

    def _resolve_slug(self, slug: str) -> list[PolymarketMarketMeta]:
        """Query Gamma directly by slug (bypasses the bulk top-N-by-volume
        discovery cutoff entirely - this is a single targeted lookup, not
        pagination) and merge any resulting outcome pairs into
        `self._markets`/`self._pair_meta`/the durable registry. Returns the
        metas found (empty if the slug doesn't exist or isn't a supported
        binary market)."""
        try:
            results = self._get_gamma_json("/markets", {"slug": slug})
        except Exception as e:
            logger.warning("Polymarket slug lookup failed for %s: %s", slug, e)
            return []
        if isinstance(results, dict):
            results = [results]
        if not isinstance(results, list):
            return []

        for m in results:
            if str(m.get("slug", "")).lower() != slug:
                continue
            metas = self._market_to_metas(m)
            if not metas:
                logger.warning(
                    "Polymarket: '%s' exists but isn't a supported binary market "
                    "(categorical, order book disabled, or archived) - skipping.",
                    slug,
                )
                return []
            for built in metas:
                self._pair_meta[built.pair] = built
                self._markets[built.pair] = self._build_market_dict(built)
            self._merge_registry(metas)
            return metas
        logger.warning("Polymarket: no market found for slug '%s'", slug)
        return []

    # Recurring-series lookup scans this many keyset pages (worst case) before
    # giving up - each page is 100 markets ordered newest-first, and even the
    # busiest interval (5m, ~8 coins) puts a coin's next instance within the
    # first page or two, so this is a generous ceiling, not a tuned budget.
    _RECURRING_SERIES_MAX_PAGES = 5

    def _resolve_recurring_series(self, coin: str, interval: str) -> list[PolymarketMarketMeta]:
        """Resolve a recurring Up/Down series (e.g. coin='btc', interval='5m')
        to its currently-open instance. These roll to a new, differently
        timestamped slug (e.g. "btc-updown-5m-1786824600") every interval, so
        unlike `_resolve_slug`'s exact match, this scans the newest markets
        (Gamma keyset ordered by `startDate` descending) for the first
        `active`/not-`closed` slug starting with "{coin}-updown-{interval}-"
        - i.e. the most recently created instance, which is the one currently
        accepting orders. Gamma's `endDate` on these markets reflects a
        far-future dispute deadline, not the few-minutes-long trading window,
        so it isn't a usable signal here - see the module's manual
        verification notes."""
        prefix = f"{coin}-updown-{interval}-"
        after_cursor: str | None = None
        for _page in range(self._RECURRING_SERIES_MAX_PAGES):
            params: dict[str, Any] = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": _GAMMA_PAGE_LIMIT,
                "order": "startDate",
                "ascending": "false",
            }
            if after_cursor:
                params["after_cursor"] = after_cursor
            try:
                data = self._get_gamma_json("/markets/keyset", params)
            except Exception as e:
                logger.warning(
                    "Polymarket recurring-series lookup failed for %s*: %s", prefix, e
                )
                return []
            page = data.get("markets") if isinstance(data, dict) else None
            if not isinstance(page, list) or not page:
                break

            for m in page:
                slug = str(m.get("slug", "")).lower()
                if not slug.startswith(prefix):
                    continue
                metas = self._market_to_metas(m)
                if not metas:
                    logger.warning(
                        "Polymarket: newest '%s*' instance ('%s') isn't a supported "
                        "binary market - skipping.",
                        prefix,
                        slug,
                    )
                    return []
                for built in metas:
                    self._pair_meta[built.pair] = built
                    self._markets[built.pair] = self._build_market_dict(built)
                self._merge_registry(metas)
                return metas

            after_cursor = data.get("next_cursor") if isinstance(data, dict) else None
            if not after_cursor:
                break

        logger.warning(
            "Polymarket: no currently-open instance found for recurring series '%s*'", prefix
        )
        return []

    # -- tickers / order book / pricing ----------------------------------

    def get_tickers(
        self,
        symbols: list[str] | None = None,
        *,
        cached: bool = False,
        market_type: TradingMode | None = None,
    ) -> Tickers:
        """Built entirely from the cached Gamma snapshot - zero extra HTTP
        calls. The complementary side's pricing (NO given YES, DOWN given UP)
        is derived from the primary side (1 - x); Polymarket doesn't expose
        per-outcome-token volume, so `quoteVolume` is copied identically onto
        both synthetic pairs (documented v1 limitation)."""
        tickers: Tickers = {}
        for pair, meta in self._pair_meta.items():
            if symbols and pair not in symbols:
                continue
            raw = meta.raw
            primary_bid = _safe_float(raw.get("bestBid"))
            primary_ask = _safe_float(raw.get("bestAsk"))
            primary_last = _safe_float(raw.get("lastTradePrice"))
            quote_volume = _safe_float(raw.get("volume24hr"))
            if quote_volume is None:
                quote_volume = _safe_float(raw.get("volume"))

            if meta.is_primary:
                bid, ask, last = primary_bid, primary_ask, primary_last
            else:
                bid = 1 - primary_ask if primary_ask is not None else None
                ask = 1 - primary_bid if primary_bid is not None else None
                last = 1 - primary_last if primary_last is not None else None

            tickers[pair] = {
                "symbol": pair,
                "bid": bid,
                "ask": ask,
                "last": last,
                "bidVolume": None,
                "askVolume": None,
                "quoteVolume": quote_volume,
                "baseVolume": None,
                "percentage": None,
                "info": raw,
            }
        return tickers

    def _fetch_ticker_sync(self, pair: str) -> dict[str, Any]:
        meta = self._resolve_pair_meta(pair)
        if meta is None:
            raise TemporaryError(f"Unknown Polymarket pair {pair}")

        mid: float | None = None
        try:
            resp = self._http_session.get(
                f"{_CLOB_API_BASE}/midpoint",
                params={"token_id": meta.token_id},
                timeout=_HTTP_TIMEOUT_S,
            )
            if resp.status_code == 200:
                mid = _safe_float(resp.json().get("mid"))
        except requests.RequestException as e:
            logger.debug("Polymarket midpoint lookup failed for %s: %s", pair, e)

        cached_ticker = self.get_tickers(symbols=[pair]).get(pair, {})
        last = mid if mid is not None else cached_ticker.get("last")
        return {
            "symbol": pair,
            "last": last,
            "bid": cached_ticker.get("bid"),
            "ask": cached_ticker.get("ask"),
            "high": None,
            "low": None,
            "quoteVolume": cached_ticker.get("quoteVolume"),
            "baseVolume": None,
            "percentage": None,
            "info": cached_ticker.get("info"),
        }

    def fetch_l2_order_book(self, pair: str, limit: int = 100) -> OrderBook:
        if not hasattr(self, "_orderbook_cache"):
            self._orderbook_cache: FtTTLCache = FtTTLCache(maxsize=256, ttl=5)
        cached = self._orderbook_cache.get(pair)
        if cached is not None:
            return cached

        meta = self._resolve_pair_meta(pair)
        if meta is None:
            raise TemporaryError(f"Unknown Polymarket pair {pair}")

        try:
            resp = self._http_session.get(
                f"{_CLOB_API_BASE}/book",
                params={"token_id": meta.token_id},
                timeout=_HTTP_TIMEOUT_S,
            )
        except requests.RequestException as e:
            raise TemporaryError(f"Polymarket CLOB /book request failed for {pair}: {e}") from e
        if resp.status_code != 200:
            raise TemporaryError(
                f"Polymarket CLOB /book error {resp.status_code} for {pair}: {resp.text[:200]}"
            )
        data = resp.json()

        # CLOB returns {"price": "..", "size": ".."} objects with no guaranteed
        # ordering - sort explicitly to match ccxt convention (bids desc, asks asc).
        bids = sorted(
            (
                (float(level["price"]), float(level["size"]))
                for level in data.get("bids", [])
            ),
            key=lambda x: x[0],
            reverse=True,
        )
        asks = sorted(
            (
                (float(level["price"]), float(level["size"]))
                for level in data.get("asks", [])
            ),
            key=lambda x: x[0],
        )

        book: OrderBook = {
            "symbol": pair,
            "bids": [[p, s] for p, s in bids],
            "asks": [[p, s] for p, s in asks],
            "timestamp": None,
            "datetime": None,
            "nonce": None,
        }
        self._orderbook_cache[pair] = book
        return book

    # -- historic OHLCV (download-data / backtesting) ---------------------

    def get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int | None = None,
        is_new_pair: bool = False,
        raise_: bool = False,
        candle_type: CandleType = CandleType.SPOT,
        until_ms: int | None = None,
    ) -> DataFrame:
        meta = self._resolve_pair_meta(pair)
        if meta is None:
            if raise_:
                raise TemporaryError(f"Unknown Polymarket pair {pair}")
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

        now_s = int(datetime.now(tz=UTC).timestamp())
        end_ts = int(until_ms / 1000) if until_ms else now_s
        if since_ms:
            start_ts = int(since_ms / 1000)
        else:
            lookback_days = 30 if is_new_pair else 7
            start_ts = now_s - lookback_days * 86400
        end_ts = min(end_ts, now_s)
        start_ts = min(start_ts, end_ts)

        fidelity = max(1, timeframe_to_minutes(timeframe))
        try:
            history = self._fetch_prices_history_sync(meta.token_id, start_ts, end_ts, fidelity)
        except TemporaryError as e:
            if raise_:
                raise
            logger.warning(str(e))
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

        if not history:
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

        # Real volume is a nice-to-have, not load-bearing for the candles
        # themselves - never let a trades-fetch problem take down a price
        # history that otherwise fetched fine.
        volume_by_bucket = None
        try:
            trades = self._fetch_trades_sync(meta.condition_id, meta.token_id, start_ts, end_ts)
            volume_by_bucket = _aggregate_trades_to_volume(trades, timeframe)
        except Exception:
            logger.debug("Polymarket trades fetch failed for %s, volume will be 0", pair, exc_info=True)

        rows = _resample_clob_history_to_ohlcv(history, timeframe, volume_by_bucket)
        if not rows:
            return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)
        return ohlcv_to_dataframe(rows, timeframe, pair, fill_missing=False, drop_incomplete=True)

    def _fetch_trades_sync(
        self, condition_id: str, token_id: str, start_ts: int, end_ts: int
    ) -> list[dict[str, Any]]:
        """Real per-trade sizes from the public Data API - see the
        `_TRADES_API_BASE` module comment for the endpoint's shape/limits.
        `market=` here is the CONDITION id (covers both outcome tokens'
        trades together), so results are filtered client-side to just this
        token, and to the requested window (the endpoint's own `after=` is
        honored server-side, but there's no verified upper-bound param, so
        the window's upper edge is enforced client-side too)."""
        all_trades: list[dict[str, Any]] = []
        offset = 0
        for _page in range(_TRADES_MAX_PAGES):
            try:
                resp = self._http_session.get(
                    f"{_TRADES_API_BASE}/trades",
                    params={
                        "market": condition_id,
                        "limit": _TRADES_PAGE_LIMIT,
                        "after": start_ts,
                        "offset": offset,
                    },
                    timeout=_HTTP_TIMEOUT_S,
                )
            except requests.RequestException as e:
                logger.debug("Polymarket trades request failed for %s: %s", condition_id, e)
                break
            if resp.status_code != 200:
                # Most commonly the pagination offset ceiling on a
                # high-volume market (verified live) - best-effort, stop
                # rather than treat as fatal.
                break
            page = resp.json()
            if not isinstance(page, list) or not page:
                break
            all_trades.extend(
                t
                for t in page
                if t.get("asset") == token_id and start_ts <= int(t.get("timestamp", 0)) <= end_ts
            )
            if len(page) < _TRADES_PAGE_LIMIT:
                break
            offset += _TRADES_PAGE_LIMIT
        return all_trades

    def _fetch_prices_history_sync(
        self, token_id: str, start_ts: int, end_ts: int, fidelity: int
    ) -> list[dict[str, Any]]:
        """Chunked to stay under CLOB's undocumented startTs/endTs span limit -
        see `_CLOB_HISTORY_MAX_SPAN_S`."""
        history: list[dict[str, Any]] = []
        chunk_start = start_ts
        while chunk_start < end_ts:
            chunk_end = min(chunk_start + _CLOB_HISTORY_MAX_SPAN_S, end_ts)
            chunk_history = self._fetch_prices_history_chunk_sync(
                token_id, chunk_start, chunk_end, fidelity
            )
            history.extend(chunk_history)
            chunk_start = chunk_end
        return history

    @retrier
    def _fetch_prices_history_chunk_sync(
        self, token_id: str, start_ts: int, end_ts: int, fidelity: int
    ) -> list[dict[str, Any]]:
        try:
            resp = self._http_session.get(
                f"{_CLOB_API_BASE}/prices-history",
                params={
                    "market": token_id,
                    "startTs": start_ts,
                    "endTs": end_ts,
                    "fidelity": fidelity,
                },
                timeout=_HTTP_TIMEOUT_S,
            )
        except requests.RequestException as e:
            raise TemporaryError(
                f"Polymarket CLOB /prices-history request failed for token "
                f"{token_id}: {e}"
            ) from e
        if resp.status_code == 429:
            # DDosProtection (not plain TemporaryError) so @retrier applies its
            # exponential backoff sleep instead of retrying immediately.
            raise DDosProtection(f"Polymarket CLOB rate-limited (429) for token {token_id}")
        if resp.status_code != 200:
            raise TemporaryError(
                f"Polymarket CLOB /prices-history error {resp.status_code} for token "
                f"{token_id}: {resp.text[:200]}"
            )
        data = resp.json()
        chunk_history = data.get("history") if isinstance(data, dict) else None
        return chunk_history if isinstance(chunk_history, list) else []
