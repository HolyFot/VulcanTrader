from __future__ import annotations

import logging
import random
from typing import Any, Callable

import numpy as np
import pandas as pd

from VulcanTrader.pairlist.IPairList import SupportsBacktesting
from VulcanTrader.pairlist.pairlist_helpers import expand_pairlist

logger = logging.getLogger(__name__)

# Mirrors each real handler's own `supports_backtesting` class attribute.
_SUPPORT: dict[str, SupportsBacktesting] = {
    "StaticPairList": SupportsBacktesting.YES,
    "VolumePairList": SupportsBacktesting.NO,
    "ShuffleFilter": SupportsBacktesting.YES,
    "VolatilityFilter": SupportsBacktesting.NO,
    "PairInformationFilter": SupportsBacktesting.BIASED,
    "AgeFilter": SupportsBacktesting.NO,
    "PriceFilter": SupportsBacktesting.BIASED,
    "SpreadFilter": SupportsBacktesting.NO,
    "RangeStabilityFilter": SupportsBacktesting.NO,
    "PrecisionFilter": SupportsBacktesting.BIASED,
    "PerformanceFilter": SupportsBacktesting.NO_ACTION,
    "PercentChangePairList": SupportsBacktesting.NO,
    "MarketCapPairList": SupportsBacktesting.BIASED,
    "OffsetFilter": SupportsBacktesting.YES,
    "DelistFilter": SupportsBacktesting.NO,
    "FullTradesFilter": SupportsBacktesting.NO_ACTION,
    "RemotePairList": SupportsBacktesting.BIASED,
    "ProducerPairList": SupportsBacktesting.NO,
}

_GENERATORS = {
    "StaticPairList", "VolumePairList", "PercentChangePairList",
    "MarketCapPairList", "RemotePairList", "ProducerPairList",
}


# ---------------------------------------------------------------------------
# Individual handlers
# ---------------------------------------------------------------------------

def static_pairlist(config: dict) -> list[str]:
    """``StaticPairList.gen_pairlist``'s backtest branch: the configured
    whitelist, verbatim — no exchange validation, matching
    ``StaticPairList.gen_pairlist``'s own ``runmode in (BACKTEST, HYPEROPT)``
    path exactly."""
    return list(config.get("exchange", {}).get("pair_whitelist") or [])


def shuffle_filter(pairlist: list[str], pairlistconfig: dict, is_backtest: bool) -> list[str]:
    """``ShuffleFilter.filter_pairlist``: a seeded (backtest, for
    reproducible results) or unseeded (live) shuffle."""
    seed = pairlistconfig.get("seed") if is_backtest else None
    rnd = random.Random(seed)  # noqa: S311
    pl = list(pairlist)
    rnd.shuffle(pl)
    return pl


def _nested_value(d: dict, dotted_key: str, default: Any = "") -> Any:
    cur: Any = d
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def pair_information_filter(pairlist: list[str], exchange: Any, pairlistconfig: dict) -> list[str]:
    """``PairInformationFilter.filter_pairlist``: keep/drop pairs by
    comparing a key in the exchange's OWN market metadata
    (``exchange.markets[pair]``) — no live ticker involved, this is the same
    cached data the leverage/price-precision lookups elsewhere in the Rust
    engine already use, so it's exact, not an approximation."""
    info_key = pairlistconfig.get("info_key", "")
    compare_value = pairlistconfig.get("info_compare_value", "")
    mode = pairlistconfig.get("selection_mode", "whitelist")
    if not info_key or not compare_value:
        raise ValueError("PairInformationFilter requires info_key and info_compare_value")
    want_whitelist = mode == "whitelist"

    keep, drop = [], []
    markets = getattr(exchange, "markets", {}) or {}
    for pair in pairlist:
        market = markets.get(pair, {})
        matched = _nested_value(market, info_key, "") == compare_value
        (keep if matched else drop).append(pair)
    return keep if want_whitelist else drop


def price_filter(
    pairlist: list[str], exchange: Any, pairlistconfig: dict,
    last_price_fn: Callable[[str], float | None],
) -> list[str]:
    """``PriceFilter._validate_pair``, using ``last_price_fn(pair) ->
    float | None`` in place of a live ticker's ``ticker['last']``. freqtrade's
    own handler is marked BIASED for exactly this reason (it reads a
    "right now" snapshot rather than a point-in-time price), so using the
    most recent close available to the Rust engine is a faithful stand-in
    for that snapshot, not an extra approximation layered on top."""
    low_price_ratio = pairlistconfig.get("low_price_ratio", 0)
    min_price = pairlistconfig.get("min_price", 0)
    max_price = pairlistconfig.get("max_price", 0)
    max_value = pairlistconfig.get("max_value", 0)
    if not (low_price_ratio > 0 or min_price > 0 or max_price > 0 or max_value > 0):
        return list(pairlist)  # matches the real filter's `_enabled` gate

    markets = getattr(exchange, "markets", {}) or {}
    out = []
    for pair in pairlist:
        price = last_price_fn(pair)
        if not price:
            continue

        if low_price_ratio != 0:
            pip = exchange.price_get_one_pip(pair, price)
            if (pip / price) > low_price_ratio:
                continue

        if max_value != 0:
            market = markets.get(pair, {})
            limits = market.get("limits", {}) or {}
            min_amount = (limits.get("amount") or {}).get("min")
            if min_amount is not None:
                min_precision = market.get("precision", {}).get("amount")
                min_value = min_amount * price
                if getattr(exchange, "precisionMode", None) == 4:  # ccxt TICK_SIZE
                    next_value = (min_amount + min_precision) * price
                else:
                    next_value = (min_amount + pow(0.1, min_precision)) * price
                if (next_value - min_value) > max_value:
                    continue

        if min_price != 0 and price < min_price:
            continue
        if max_price != 0 and price > max_price:
            continue

        out.append(pair)
    return out


def volume_pairlist(
    candidates: list[str], ohlcv_by_pair: dict[str, pd.DataFrame], pairlistconfig: dict,
    exchange: Any = None,
) -> list[str]:
    """``VolumePairList``'s "range" mode (candle-summed volume — the mode
    that doesn't need a live ticker fetch). ``ohlcv_by_pair`` maps
    pair -> a DataFrame with ``high``/``low``/``close``/``volume`` (or an
    already-computed ``quoteVolume``) covering at least ``lookback_period``
    candles.

    NOTE: real freqtrade NEVER runs this during backtest
    (``supports_backtesting = NO`` triggers the whole-chain replacement in
    ``resolve_rust_pairlist``) — this exists so the handler is complete and
    usable for live/dry-run pairlist resolution, not because the default
    backtest path calls it.
    """
    lookback_period = pairlistconfig.get("lookback_period", 0)
    lookback_days = pairlistconfig.get("lookback_days", 0)
    if lookback_days > 0:
        lookback_period = lookback_days  # implies the 1d timeframe
    min_value = pairlistconfig.get("min_value", 0)
    max_value = pairlistconfig.get("max_value", None)
    number_assets = pairlistconfig.get("number_assets", 30)

    markets = getattr(exchange, "markets", {}) or {} if exchange is not None else {}
    scored: list[tuple[str, float]] = []
    for pair in candidates:
        df = ohlcv_by_pair.get(pair)
        if df is None or df.empty:
            continue
        contract_size = markets.get(pair, {}).get("contractSize", 1.0) or 1.0
        if "quoteVolume" in df.columns:
            qv = df["quoteVolume"]
        else:
            typical = (df["high"] + df["low"] + df["close"]) / 3.0
            qv = df["volume"] * typical * contract_size
        window = qv.rolling(max(lookback_period, 1)).sum().fillna(0)
        vol = float(window.iloc[-1]) if len(window) else 0.0
        scored.append((pair, vol))

    if min_value > 0:
        scored = [(p, v) for p, v in scored if v > min_value]
    if max_value is not None:
        scored = [(p, v) for p, v in scored if v < max_value]
    scored.sort(key=lambda pv: pv[1], reverse=True)
    return [p for p, _ in scored[:number_assets]]


def volatility_filter(
    pairlist: list[str], ohlcv_daily_by_pair: dict[str, pd.DataFrame], pairlistconfig: dict,
) -> list[str]:
    """``VolatilityFilter``, using pre-fetched daily candles instead of a
    live ``refresh_ohlcv_with_cache`` call. Real freqtrade NEVER runs this
    during backtest either (``NO`` support) — provided for live/dry-run
    completeness."""
    days = pairlistconfig.get("lookback_days", 10)
    min_v = pairlistconfig.get("min_volatility", 0)
    max_v = pairlistconfig.get("max_volatility", float("inf"))
    sort_dir = pairlistconfig.get("sort_direction")

    kept: list[str] = []
    vol_by_pair: dict[str, float] = {}
    for pair in pairlist:
        daily = ohlcv_daily_by_pair.get(pair)
        if daily is None or daily.empty:
            continue
        returns = np.log(daily["close"].shift(1) / daily["close"]).fillna(0)
        vol = float(returns.rolling(window=days).std().mean() * np.sqrt(days))
        if min_v <= vol <= max_v:
            kept.append(pair)
            vol_by_pair[pair] = 0.0 if np.isnan(vol) else vol
    if sort_dir:
        kept.sort(key=lambda p: vol_by_pair[p], reverse=(sort_dir == "desc"))
    return kept


def age_filter(
    pairlist: list[str], ohlcv_daily_by_pair: dict[str, pd.DataFrame], pairlistconfig: dict,
) -> list[str]:
    """``AgeFilter``, using pre-fetched daily candle counts in place of a
    live ``refresh_latest_ohlcv`` call. Real freqtrade NEVER runs this during
    backtest either (``NO`` support) — provided for live/dry-run
    completeness."""
    min_days = pairlistconfig.get("min_days_listed", 10)
    max_days = pairlistconfig.get("max_days_listed")
    out = []
    for pair in pairlist:
        daily = ohlcv_daily_by_pair.get(pair)
        n = 0 if daily is None else len(daily)
        if n >= min_days and (not max_days or n <= max_days):
            out.append(pair)
    return out


def spread_filter(
    pairlist: list[str], bid_ask_fn: Callable[[str], tuple[float, float] | None],
    pairlistconfig: dict,
) -> list[str]:
    """``SpreadFilter._validate_pair``, using
    ``bid_ask_fn(pair) -> (bid, ask) | None`` in place of a live ticker's
    ``bid``/``ask``. Real freqtrade NEVER runs this during backtest
    (``NO`` support) — provided for live/dry-run completeness."""
    max_spread_ratio = pairlistconfig.get("max_spread_ratio", 0.005)
    if max_spread_ratio == 0:
        return list(pairlist)  # matches the real filter's `_enabled` gate
    out = []
    for pair in pairlist:
        ba = bid_ask_fn(pair)
        if not ba or not ba[0] or not ba[1]:
            continue
        bid, ask = ba
        spread = 1 - bid / ask
        if spread <= max_spread_ratio:
            out.append(pair)
    return out


def range_stability_filter(
    pairlist: list[str], ohlcv_daily_by_pair: dict[str, pd.DataFrame], pairlistconfig: dict,
) -> list[str]:
    """``RangeStabilityFilter``, using pre-fetched daily candles instead of a
    live ``refresh_ohlcv_with_cache`` call. Real freqtrade NEVER runs this
    during backtest either (``NO`` support) — provided for live/dry-run
    completeness."""
    min_roc = pairlistconfig.get("min_rate_of_change", 0.01)
    max_roc = pairlistconfig.get("max_rate_of_change")
    sort_dir = pairlistconfig.get("sort_direction")

    kept: list[str] = []
    roc_by_pair: dict[str, float] = {}
    for pair in pairlist:
        daily = ohlcv_daily_by_pair.get(pair)
        if daily is None or daily.empty:
            continue
        highest_high = daily["high"].max()
        lowest_low = daily["low"].min()
        pct_change = ((highest_high - lowest_low) / lowest_low) if lowest_low > 0 else 0.0
        if pct_change < min_roc:
            continue
        if max_roc and pct_change > max_roc:
            continue
        kept.append(pair)
        roc_by_pair[pair] = pct_change
    if sort_dir:
        kept.sort(key=lambda p: roc_by_pair[p], reverse=(sort_dir == "desc"))
    return kept


def precision_filter(
    pairlist: list[str], exchange: Any, config: dict,
    last_price_fn: Callable[[str], float | None],
) -> list[str]:
    """``PrecisionFilter._validate_pair``: drop pairs so low-priced/coarse
    that the configured stoploss would round to the same (or a looser) price
    as a 1%-tighter "stop gap" check — i.e. there isn't enough price
    resolution to place a meaningfully distinct stoploss. Uses
    ``last_price_fn`` as the BIASED "right now" stand-in, same rationale as
    ``price_filter``."""
    stoploss = config.get("stoploss")
    if stoploss is None:
        raise ValueError("PrecisionFilter can only work with `stoploss` defined in config.")
    if stoploss == 0:
        return list(pairlist)  # matches the real filter's `_enabled` gate
    sanitized_stoploss = 1 - abs(stoploss)

    out = []
    for pair in pairlist:
        price = last_price_fn(pair)
        if not price:
            continue
        stop_price = price * sanitized_stoploss
        sp = exchange.price_to_precision(pair, stop_price, rounding_mode=4)  # ROUND_UP
        stop_gap_price = exchange.price_to_precision(pair, stop_price * 0.99, rounding_mode=4)
        if sp <= stop_gap_price:
            continue
        out.append(pair)
    return out


def performance_filter(
    pairlist: list[str], performance_by_pair: dict[str, dict] | None = None,
    pairlistconfig: dict | None = None,
) -> list[str]:
    """``PerformanceFilter.filter_pairlist``: sort pairs by historical
    profit_ratio (desc), then trade count (asc), then original order;
    optionally drop pairs below ``min_profit``. ``performance_by_pair`` maps
    pair -> ``{"profit_ratio": float, "count": int}``.

    Real freqtrade's own handler is ``NO_ACTION`` in backtest — it tries to
    read ``Trade.get_overall_performance()`` from the live trade database,
    which doesn't exist yet during a backtest run (caught as an
    ``AttributeError`` and passed through unchanged with a warning). Since
    pairlist resolution here is likewise a ONE-TIME event before any trade
    has happened, calling this with ``performance_by_pair=None`` (the
    backtest-mode default) is the exact same no-op — pass real performance
    data only for live/dry-run reuse.
    """
    pairlistconfig = pairlistconfig or {}
    if not performance_by_pair:
        return list(pairlist)
    min_profit = pairlistconfig.get("min_profit")

    scored = [
        (p, performance_by_pair.get(p, {}).get("profit_ratio", 0.0),
         performance_by_pair.get(p, {}).get("count", 0), i)
        for i, p in enumerate(pairlist)
    ]
    scored.sort(key=lambda t: (-t[1], t[2], t[3]))
    if min_profit is not None:
        scored = [t for t in scored if t[1] >= min_profit]
    return [t[0] for t in scored]


def percent_change_pairlist(
    candidates: list[str], ohlcv_by_pair: dict[str, pd.DataFrame], pairlistconfig: dict,
) -> list[str]:
    """``PercentChangePairList``'s range mode (candle-to-candle percentage
    change over a lookback, no live ticker needed). Real freqtrade NEVER
    runs this during backtest (``NO`` support) — provided for live/dry-run
    completeness."""
    lookback_period = pairlistconfig.get("lookback_period", 0)
    lookback_days = pairlistconfig.get("lookback_days", 0)
    if lookback_days > 0:
        lookback_period = lookback_days
    min_value = pairlistconfig.get("min_value")
    max_value = pairlistconfig.get("max_value")
    sort_dir = pairlistconfig.get("sort_direction", "desc")
    number_assets = pairlistconfig.get("number_assets", 30)

    scored: list[tuple[str, float]] = []
    for pair in candidates:
        df = ohlcv_by_pair.get(pair)
        if df is None or df.empty or len(df) <= lookback_period:
            scored.append((pair, 0.0))
            continue
        current_close = df["close"].iloc[-1]
        previous_close = df["close"].shift(lookback_period).iloc[-1]
        pct = ((current_close - previous_close) / previous_close) * 100 if previous_close > 0 else 0.0
        scored.append((pair, float(pct)))

    if min_value is not None:
        scored = [(p, v) for p, v in scored if v > min_value]
    if max_value is not None:
        scored = [(p, v) for p, v in scored if v < max_value]
    scored.sort(key=lambda pv: pv[1], reverse=(sort_dir == "desc"))
    return [p for p, _ in scored[:number_assets]]


def market_cap_pairlist(
    candidates: list[str], marketcap_ranking: list[str], pairlistconfig: dict,
    stake_currency: str, trading_mode: str = "futures",
) -> list[str]:
    """``MarketCapPairList``, using an already-fetched ``marketcap_ranking``
    (a list of base-asset symbols, e.g. ``["BTC", "ETH", ...]``, in
    market-cap-descending order — what a CoinGecko ``/coins/markets`` call
    would return) instead of calling CoinGecko directly. This module never
    makes that HTTP call itself; supply the ranking from wherever you'd
    otherwise fetch it.

    Mirrors the real handler's ``1000``/``K`` prefix resolution (for pairs
    like ``KPEPE/USDC:USDC`` on Hyperliquid or ``1000PEPE/USDT:USDT`` on
    Binance) and whitelist/blacklist ``mode``.
    """
    mode = pairlistconfig.get("mode", "whitelist")
    number_assets = pairlistconfig.get("number_assets", 30)
    max_rank = pairlistconfig.get("max_rank", 30)
    is_whitelist_mode = mode == "whitelist"
    quote = stake_currency.upper()
    pair_format = f"{quote}" + (f":{quote}" if trading_mode == "futures" else "")

    candidates = list(candidates)
    filtered: list[str] = []
    for symbol in marketcap_ranking[:max_rank]:
        pair = f"{symbol.upper()}/{pair_format}"
        resolved = None
        if pair in filtered:
            continue
        if pair in candidates:
            resolved = pair
        else:
            for prefix in ("1000", "K"):
                test = f"{prefix}{pair}"
                if test in candidates:
                    resolved = test
                    break
        if resolved is None:
            continue
        if not is_whitelist_mode:
            candidates.remove(resolved)
            continue
        filtered.append(resolved)
        if len(filtered) == number_assets:
            break

    return candidates if not is_whitelist_mode else filtered


def offset_filter(pairlist: list[str], pairlistconfig: dict) -> list[str]:
    """``OffsetFilter.filter_pairlist``: slice ``[offset : offset+number_assets]``.
    Fully supported during backtest (``YES``) — no snapshot/live data
    involved at all, it's pure list slicing."""
    offset = pairlistconfig.get("offset", 0)
    number_assets = pairlistconfig.get("number_assets", 0)
    if offset < 0:
        raise ValueError("OffsetFilter requires offset to be >= 0")
    if offset > len(pairlist):
        logger.warning("Offset of %s is larger than pair count of %s", offset, len(pairlist))
    pairs = pairlist[offset:]
    if number_assets:
        pairs = pairs[:number_assets]
    return pairs


def delist_filter(
    pairlist: list[str], delist_dates: dict[str, Any], pairlistconfig: dict,
) -> list[str]:
    """``DelistFilter._validate_pair``, using a pre-supplied ``delist_dates``
    map (pair -> delisting ``datetime``, or absent/``None`` if not
    scheduled for delisting) instead of a live
    ``exchange.check_delisting_time`` call. Real freqtrade NEVER runs this
    during backtest either (``NO`` support) — provided for live/dry-run
    completeness."""
    from datetime import UTC, datetime, timedelta

    max_days = pairlistconfig.get("max_days_from_now", 0)
    out = []
    for pair in pairlist:
        delist_date = delist_dates.get(pair)
        if delist_date is None:
            out.append(pair)
            continue
        remove = max_days == 0
        if max_days > 0:
            remove = delist_date <= (datetime.now(UTC) + timedelta(days=max_days))
        if not remove:
            out.append(pair)
    return out


def full_trades_filter(
    pairlist: list[str], open_trade_count: int, max_open_trades: int,
) -> list[str]:
    """``FullTradesFilter.filter_pairlist``: return an empty list once all
    trade slots are full, else pass through unchanged.

    Real freqtrade's own handler is ``NO_ACTION`` in backtest: pairlist
    resolution is a ONE-TIME event before any candle has been processed, so
    ``open_trade_count`` is always 0 at that moment — ``0 >= max_open_trades``
    is only ever true if ``max_open_trades <= 0``, which isn't a real cap.
    Calling this with ``open_trade_count=0`` (the backtest-mode default) is
    the exact same no-op; pass the real, live count only for live/dry-run
    reuse where the filter is actually re-evaluated every iteration.
    """
    if max_open_trades > 0 and open_trade_count >= max_open_trades:
        return []
    return list(pairlist)


def remote_pairlist(
    candidates: list[str], fetched_pairlist: list[str], pairlistconfig: dict,
) -> list[str]:
    """``RemotePairList``, using an already-fetched ``fetched_pairlist``
    (whatever the configured ``pairlist_url`` would have returned) instead of
    making the HTTP GET itself — this module never performs that network
    call. ``mode`` is ``whitelist`` (intersect) or ``blacklist`` (exclude);
    result is capped at ``number_assets`` in whitelist mode."""
    mode = pairlistconfig.get("mode", "whitelist")
    number_assets = pairlistconfig.get("number_assets", 0)
    fetched = set(fetched_pairlist)
    if mode == "blacklist":
        return [p for p in candidates if p not in fetched]
    result = [p for p in candidates if p in fetched] if candidates else list(fetched_pairlist)
    if number_assets:
        result = result[:number_assets]
    return result


def producer_pairlist(
    candidates: list[str] | None, producer_pairs: list[str], pairlistconfig: dict,
) -> list[str]:
    """``ProducerPairList``, using a pre-supplied ``producer_pairs`` list
    (whatever an ``external_message_consumer`` leader connection would have
    provided) instead of reading from a live producer/leader bot connection
    — which has no backtest equivalent at all. Real freqtrade NEVER runs
    this during backtest either (``NO`` support) — provided for live/dry-run
    completeness."""
    number_assets = pairlistconfig.get("number_assets", 0)
    base = candidates if candidates is not None else []
    pairs = list(dict.fromkeys(base + producer_pairs))
    if number_assets:
        pairs = pairs[:number_assets]
    return pairs


# ---------------------------------------------------------------------------
# Chain resolution
# ---------------------------------------------------------------------------

def _run_generator(handler_cfg: dict, config: dict, injected: dict[str, Any]) -> list[str]:
    method = handler_cfg["method"]
    if method not in _GENERATORS:
        raise ValueError(
            f"{method} cannot be used as the first (generator) pairlist handler."
        )
    if method == "MarketCapPairList" and injected.get("marketcap_ranking"):
        # BIASED, so it DOES run in backtest — but only produces a real
        # ranking if the caller injected one; without it, fall back to the
        # static whitelist (there is no live CoinGecko call from here).
        stake = config.get("stake_currency", "USDC")
        trading_mode = str(config.get("trading_mode", "futures"))
        candidates = static_pairlist(config)
        return market_cap_pairlist(
            candidates, injected["marketcap_ranking"], handler_cfg, stake, trading_mode,
        )
    if method == "RemotePairList" and injected.get("fetched_pairlist") is not None:
        # BIASED, runs in backtest — but only does something if a
        # pre-fetched remote list was injected (no live HTTP call here).
        candidates = static_pairlist(config)
        return remote_pairlist(candidates, injected["fetched_pairlist"], handler_cfg)
    # VolumePairList / PercentChangePairList / ProducerPairList are
    # generators too, but (like StaticPairList) are only ever reached here
    # outside backtest mode — see the NO-support handling in
    # resolve_rust_pairlist. MarketCapPairList/RemotePairList without
    # injected data fall through to the same safe default. Callers needing
    # the real behavior should call `volume_pairlist` /
    # `percent_change_pairlist` / `producer_pairlist` themselves with
    # fetched data.
    return static_pairlist(config)


def _run_filter(
    pairlist: list[str], handler_cfg: dict, exchange: Any, config: dict,
    last_price_fn: Callable[[str], float | None] | None, is_backtest: bool,
    injected: dict[str, Any],
) -> list[str]:
    method = handler_cfg["method"]
    if method == "ShuffleFilter":
        return shuffle_filter(pairlist, handler_cfg, is_backtest)
    if method == "OffsetFilter":
        return offset_filter(pairlist, handler_cfg)
    if method == "PairInformationFilter":
        if exchange is None:
            logger.debug("PairInformationFilter skipped: no exchange available.")
            return pairlist
        return pair_information_filter(pairlist, exchange, handler_cfg)
    if method == "PriceFilter":
        if exchange is None or last_price_fn is None:
            logger.debug("PriceFilter skipped: no exchange/last_price_fn available.")
            return pairlist
        return price_filter(pairlist, exchange, handler_cfg, last_price_fn)
    if method == "PrecisionFilter":
        if exchange is None or last_price_fn is None:
            logger.debug("PrecisionFilter skipped: no exchange/last_price_fn available.")
            return pairlist
        return precision_filter(pairlist, exchange, config, last_price_fn)
    if method == "PerformanceFilter":
        # NO_ACTION: a real no-op in backtest (see performance_filter's own
        # docstring) unless the caller injected real performance data.
        return performance_filter(pairlist, injected.get("performance_by_pair"), handler_cfg)
    if method == "FullTradesFilter":
        # NO_ACTION: a real no-op in backtest — 0 open trades at the moment
        # pairlist resolution runs, unless the caller injected a live count.
        return full_trades_filter(
            pairlist, injected.get("open_trade_count", 0),
            int(config.get("max_open_trades", 0) or 0),
        )
    # VolatilityFilter / AgeFilter / VolumePairList / SpreadFilter /
    # RangeStabilityFilter / DelistFilter / PercentChangePairList /
    # ProducerPairList never reach here during backtest (NO support triggers
    # the whole-chain replacement before this is called). Left as a
    # pass-through for any other/unknown handler.
    return pairlist


def resolve_rust_pairlist(
    config: dict,
    exchange: Any = None,
    available_pairs: list[str] | None = None,
    last_price_fn: Callable[[str], float | None] | None = None,
    is_backtest: bool = True,
    *,
    bid_ask_fn: Callable[[str], tuple[float, float] | None] | None = None,
    performance_by_pair: dict[str, dict] | None = None,
    open_trade_count: int = 0,
    marketcap_ranking: list[str] | None = None,
    fetched_pairlist: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Resolve the pair whitelist for the Rust backtest engine, replicating
    ``Backtesting.__init__``'s ``PairListManager(...).refresh_pairlist()`` +
    ``_check_backtest()`` sequence as an independent implementation.

    :param config: the VulcanTrader config dict.
    :param exchange: the loaded ``Exchange`` instance (for market metadata —
        the same object ``Backtesting.exchange`` already is; no live/network
        calls are made through it here).
    :param available_pairs: pairs the caller already has OHLCV for (e.g.
        ``processed.keys()``). If given, the result is intersected with it —
        the Rust engine can SELECT a subset of already-loaded pairs, not
        load new ones (see module docstring).
    :param last_price_fn: ``pair -> float | None``, used by PriceFilter and
        PrecisionFilter (both BIASED) as a stand-in for a live ticker's
        ``last`` price.
    :param is_backtest: selects ShuffleFilter's seeded-vs-live behavior and
        whether NO/NO_ACTION-support handlers trigger the StaticPairList
        fallback / stay a no-op; True for the Rust backtest driver's own use.
    :param bid_ask_fn: ``pair -> (bid, ask) | None``, only consulted if you
        call ``spread_filter`` directly — SpreadFilter is NO-support and
        never reached by this resolver in backtest mode.
    :param performance_by_pair: injected data for PerformanceFilter
        (NO_ACTION) — leave ``None`` for real backtest-mode no-op behavior.
    :param open_trade_count: injected data for FullTradesFilter (NO_ACTION)
        — leave at 0 for real backtest-mode no-op behavior (see
        ``full_trades_filter``'s docstring for why 0 is exact, not a
        placeholder).
    :param marketcap_ranking: injected data for MarketCapPairList (BIASED,
        generator) in place of a live CoinGecko call — a list of base-asset
        symbols in market-cap-descending order. Without it, MarketCapPairList
        falls back to the static whitelist.
    :param fetched_pairlist: injected data for RemotePairList (BIASED,
        generator) in place of a live HTTP fetch. Without it, RemotePairList
        falls back to the static whitelist.
    :return: ``(whitelist, warnings)`` — ``warnings`` mirrors the log lines
        ``_check_backtest``/``refresh_pairlist`` would have emitted.
    """
    handlers_cfg = config.get("pairlists") or [{"method": "StaticPairList"}]
    warnings: list[str] = []
    injected = {
        "performance_by_pair": performance_by_pair,
        "open_trade_count": open_trade_count,
        "marketcap_ranking": marketcap_ranking,
        "fetched_pairlist": fetched_pairlist,
    }

    no_support = [
        h["method"] for h in handlers_cfg
        if _SUPPORT.get(h["method"], SupportsBacktesting.YES) == SupportsBacktesting.NO
    ]
    no_action = [
        h["method"] for h in handlers_cfg
        if _SUPPORT.get(h["method"], SupportsBacktesting.YES) == SupportsBacktesting.NO_ACTION
    ]
    biased = [
        h["method"] for h in handlers_cfg
        if _SUPPORT.get(h["method"], SupportsBacktesting.YES) == SupportsBacktesting.BIASED
    ]

    if is_backtest and no_support:
        # freqtrade's own `_check_backtest`: ANY NO-support handler in the
        # configured chain replaces the ENTIRE chain with StaticPairList.
        warnings.append(
            f"Pairlist handlers {', '.join(no_support)} do not support backtesting; "
            f"falling back to StaticPairList with exchange.pair_whitelist."
        )
        pairlist = static_pairlist(config)
    else:
        if is_backtest and no_action:
            warnings.append(
                f"Pairlist handlers {', '.join(no_action)} do not generate any changes "
                f"during backtesting. Safe to leave enabled, but they won't behave like "
                f"in dry/live modes."
            )
        if is_backtest and biased:
            warnings.append(
                f"Pairlist handlers {', '.join(biased)} introduce lookahead bias "
                f"during backtesting (they read a 'right now' snapshot)."
            )
        pairlist = _run_generator(handlers_cfg[0], config, injected)
        for handler_cfg in handlers_cfg[1:]:
            pairlist = _run_filter(
                pairlist, handler_cfg, exchange, config, last_price_fn, is_backtest, injected,
            )

    # Blacklist, verified AFTER the full chain — matches refresh_pairlist's
    # own ordering exactly.
    blacklist = config.get("exchange", {}).get("pair_blacklist") or []
    if blacklist:
        if exchange is not None and getattr(exchange, "markets", None):
            try:
                expanded = expand_pairlist(blacklist, list(exchange.markets.keys()))
            except ValueError as err:
                logger.warning("Pair blacklist contains an invalid wildcard: %s", err)
                expanded = []
        else:
            expanded = blacklist
        pairlist = [p for p in pairlist if p not in expanded]

    # Safety net mirroring PairListManager.refresh_pairlist: an empty result
    # after the whole chain falls back to the raw config whitelist so the
    # backtest can still proceed against whatever data is available.
    if not pairlist:
        fallback = static_pairlist(config)
        if fallback:
            warnings.append(
                "Pairlist empty after filters; using exchange.pair_whitelist directly."
            )
            pairlist = fallback

    if available_pairs is not None:
        avail = set(available_pairs)
        pairlist = [p for p in pairlist if p in avail]

    for w in warnings:
        logger.warning(w)

    return pairlist, warnings
