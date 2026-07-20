"""
Rust-engine backtest driver.

Runs a strategy's *already-computed* entry/exit signals through a fast,
pure-Python joint multi-pair simulator (``_joint_multi_pair_backtest``) that
mirrors freqtrade's own ``time_pair_generator`` / ``backtest_loop`` bar-by-bar,
cross-pair coordination exactly — including the portfolio-wide
``max_open_trades`` slot cap, one-position-per-pair with same-candle reversal
support, and the precise pair-processing order within a candle (pairs with
open trades first, in entry order; then flat pairs in whitelist order).

The strategy is still Python: ``populate_indicators`` / ``populate_entry_trend``
/ ``populate_exit_trend`` run exactly as normal to produce the ``enter_long`` /
``enter_short`` / ``exit_long`` / ``exit_short`` columns. This driver only
replaces the per-candle *simulation* (normally ``Backtesting.backtest_loop``)
with a lighter-weight but behaviorally-equivalent Python loop, then formats the
result into the same trades DataFrame that ``generate_backtest_stats`` consumes
— so the web portal reads it unchanged.

Every IStrategy callback the python engine can invoke is wired up here too —
``leverage``, ``custom_stake_amount``, ``custom_entry_price``,
``confirm_trade_entry``, ``bot_loop_start``, ``custom_stoploss`` (gated on
``use_custom_stoploss``), ``custom_exit``, ``custom_exit_price``,
``confirm_trade_exit``, ``order_filled`` — with ONE deliberate exception:
``adjust_order_price``. That callback exists to reprice a still-PENDING,
partially-or-unfilled LIMIT order across multiple candles while it sits on the
book. Both this engine and the default python engine's core loop model every
entry/exit as an instant fill at a single resolved price on a single bar —
neither has a pending-order object that persists and gets revisited candle
after candle. Wiring `adjust_order_price` in would require building that
multi-candle order lifecycle from scratch in both engines, which is a real
architecture change, not a missing-callback gap — out of scope here and left
unimplemented.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd
from ccxt import TICK_SIZE, ROUND_DOWN, ROUND_UP

from VulcanTrader.data.btanalysis.historic_precision import get_tick_size_over_time
from VulcanTrader.exchange.exchange_utils import price_to_precision

from VulcanTrader.data.btanalysis.bt_fileutils import BT_DATA_COLUMNS

logger = logging.getLogger(__name__)

# Standard-indicator indices from fast_indicators::calculate_standard_indicators.
_RSI, _MACD_HIST, _BB_POS, _ATR, _CCI = 0, 9, 10, 14, 17

# Internal exit-reason labels -> freqtrade exit_reason strings.
_EXIT_REASON_MAP = {
    "Signal": "exit_signal",
    "CustomExit": "custom_exit",
    "RoiTarget": "roi",
    "Stoploss": "stop_loss",
    "TrailingStop": "trailing_stop_loss",
    "MaxHoldPeriod": "force_exit",
    "CciExit": "exit_signal",
    "RsiExit": "exit_signal",
    "MacdExit": "exit_signal",
    "ForceExit": "force_exit",
}


def _parse_timeframe_minutes(tf: str) -> int:
    tf = (tf or "15m").strip()
    unit = tf[-1]
    try:
        amount = int(tf[:-1])
    except ValueError:
        return 15
    mult = {"m": 1, "h": 60, "d": 60 * 24, "w": 60 * 24 * 7}.get(unit.lower(), 1)
    return max(1, amount * mult)


def _build_engine_config(strategy: Any, config: dict) -> dict:
    """Translate the freqtrade strategy/config into the joint simulator's keys."""
    tf = getattr(strategy, "timeframe", None) or config.get("timeframe", "15m")
    tf_min = _parse_timeframe_minutes(tf)

    # minimal_roi {minute_str: roi} -> 4 ROI tiers (bar-indexed).
    roi = getattr(strategy, "minimal_roi", None) or {"0": 0.10}
    tiers = sorted(((int(k), float(v)) for k, v in roi.items()), key=lambda x: x[0])
    tiers = tiers[:4]
    roi_vals = [99.0, 99.0, 99.0, 99.0]
    roi_periods = [10**9, 10**9, 10**9, 10**9]
    for i, (minute, val) in enumerate(tiers):
        roi_vals[i] = val
        roi_periods[i] = math.ceil(minute / tf_min) if tf_min else 0

    fee = float(config.get("fee", 0.0005) or 0.0005)
    trade_type = "spot" if str(config.get("trading_mode", "futures")) == "spot" else "futures"

    # --- Stake sizing: mirror freqtrade's fixed stake_amount ----------------
    stake = config.get("stake_amount")
    wallet = float(config.get("dry_run_wallet", 10000.0) or 10000.0)
    ratio = float(config.get("tradable_balance_ratio", 1.0) or 1.0)
    if isinstance(stake, (int, float)) and stake > 0:
        fixed_stake, compounding = float(stake), False
    else:  # "unlimited" -> wallet split across max_open_trades, then compounds
        slots = int(config.get("max_open_trades", 1) or 1)
        fixed_stake = wallet * ratio / max(slots, 1)
        compounding = True

    # --- ATR stoploss: only when the strategy actually enables it -----------
    # freqtrade ONLY calls custom_stoploss when `use_custom_stoploss = True`;
    # defining the method is not enough (it defaults to False on IStrategy).
    atr_mult = 0.0
    _p = getattr(strategy, "atr_stop_mult", None)
    if _p is not None and getattr(strategy, "use_custom_stoploss", False):
        try:
            atr_mult = float(getattr(_p, "value", _p))
        except Exception:
            atr_mult = 0.0

    return {
        "timeframe": tf,
        "timeframe_minutes": tf_min,
        "trade_type": "Spot" if trade_type == "spot" else "Futures",
        "atr_stop_enabled": atr_mult > 0.0,
        "atr_stop_multiplier": atr_mult,
        "fee_taker": fee,
        "roi_enabled": True,
        "roi_vals": roi_vals,
        "roi_periods": roi_periods,
        # `max_hold_period` isn't a real IStrategy attribute — no strategy in
        # this codebase defines it, and freqtrade's own backtesting.py has no
        # such concept (a trade is held until signal/stop/ROI/trailing says
        # otherwise, however long that takes). Use a large sentinel unless a
        # strategy actually sets the attribute, so it's effectively a no-op.
        "max_hold_period": int(getattr(strategy, "max_hold_period", 0) or 10**9),
        "base_stoploss": float(getattr(strategy, "stoploss", -0.10) or -0.10),
        "trailing_enabled": bool(getattr(strategy, "trailing_stop", False)),
        "trailing_trigger": float(getattr(strategy, "trailing_stop_positive_offset", 0.0) or 0.0),
        "trailing_offset": float(getattr(strategy, "trailing_stop_positive", 0.0) or 0.0),
        "compounding_enabled": compounding,
        "starting_balance": wallet,
        "tradable_balance_ratio": ratio,
        "max_trade_amount": fixed_stake,
    }


def _pair_exchange_max_leverage(exchange: Any, pair: str, stake: float,
                                fallback_max: float) -> float:
    """Each pair has its OWN exchange max leverage (e.g. on Hyperliquid,
    BTC=40x, SPX=5x, VVV=3x — not a global constant); resolved once per pair
    since it's a static market limit, unlike `strategy.leverage()` itself
    (called fresh per TRADE in `do_enter` — see 2026-07-19 history note in
    `_joint_multi_pair_backtest`'s docstring; an earlier version sampled
    `strategy.leverage()` once per pair too, which silently froze whatever
    ECS/ANY time-varying equity-curve state the strategy's leverage() reads at
    whatever moment pair-setup happened to run, instead of the real value at
    each trade's own entry time).
    """
    max_lev = fallback_max
    if exchange is not None:
        try:
            max_lev = float(exchange.get_max_leverage(pair, stake))
        except Exception:
            logger.debug("exchange.get_max_leverage(%s) failed; using fallback", pair, exc_info=True)
    return max_lev


def _resolve_leverage(strategy: Any, pair: str, current_time: Any, current_rate: float,
                      max_leverage: float, entry_tag: str | None, side: str) -> float:
    """`strategy.leverage(...)` called fresh at entry time, clamped to
    [1.0, max_leverage] exactly like `Backtesting.get_valid_entry_price_and_stake`."""
    leverage = 1.0
    try:
        lev = strategy.leverage(
            pair=pair, current_time=current_time, current_rate=current_rate,
            proposed_leverage=1.0, max_leverage=max_leverage, entry_tag=entry_tag, side=side,
        )
        if lev and float(lev) > 0:
            leverage = float(lev)
    except Exception:
        logger.debug("strategy.leverage(%s) not usable; defaulting to 1.0", pair, exc_info=True)
    return min(max(leverage, 1.0), max_leverage)


def _load_funding_mark_data(config: dict, exchange: Any, pairs: list[str]) -> dict[str, pd.DataFrame]:
    """Load and combine funding-rate + mark-price series per pair, mirroring
    `Backtesting._load_bt_data_detail`'s FUTURES path exactly, so
    `exchange.calculate_funding_fees` can be reused unmodified. For futures
    trades, freqtrade adds/subtracts accrued funding directly into
    `profit_abs` (via `calc_close_trade_value`) — skipping it, as an earlier
    version of this engine did, biases every trade that happens to span a
    funding accrual boundary (every `funding_fee_timeframe`, e.g. hourly on
    Hyperliquid) and compounds into the wallet balance over thousands of
    trades.
    """
    from VulcanTrader.data import history
    from VulcanTrader.enums import CandleType

    datadir = config["datadir"]
    data_format = config.get("dataformat_ohlcv", "feather")
    funding_fee_timeframe = exchange.get_option("funding_fee_timeframe")
    mark_timeframe = exchange.get_option("mark_ohlcv_timeframe")

    funding_rates = history.load_data(
        datadir=datadir, pairs=pairs, timeframe=funding_fee_timeframe,
        fill_up_missing=False, data_format=data_format,
        candle_type=CandleType.FUNDING_RATE,
    )
    mark_rates = history.load_data(
        datadir=datadir, pairs=pairs, timeframe=mark_timeframe,
        data_format=data_format,
        candle_type=CandleType.from_string(exchange.get_option("mark_ohlcv_price")),
    )

    combined: dict[str, pd.DataFrame] = {}
    for pair in pairs:
        if pair not in funding_rates or pair not in mark_rates:
            continue
        try:
            combined[pair] = exchange.combine_funding_and_mark(
                funding_rates=funding_rates[pair],
                mark_rates=mark_rates[pair],
                futures_funding_rate=config.get("futures_funding_rate", None),
            )
        except Exception:
            logger.debug("combine_funding_and_mark(%s) failed", pair, exc_info=True)
    return combined


def _u8(series: pd.Series | None, n: int) -> np.ndarray:
    if series is None:
        return np.zeros(n, dtype=np.uint8)
    return (series.fillna(0).to_numpy() != 0).astype(np.uint8)


def _f64(series: pd.Series, n: int) -> np.ndarray:
    return np.nan_to_num(series.to_numpy(dtype=np.float64), nan=0.0)


def _shift1(a: np.ndarray) -> np.ndarray:
    """freqtrade's `_get_ohlcv_as_lists` `.shift(1)`: a signal computed from
    bar i's close is only acted on starting at bar i+1's row."""
    return np.concatenate(([0], a[:-1])).astype(np.uint8)


def _precision_per_bar(df: pd.DataFrame, dates: np.ndarray, exchange: Any, pair: str) -> np.ndarray:
    """Per-bar exchange tick size, matching `Backtesting.get_pair_precision`
    (data-inferred, monthly-varying — NOT a fixed exchange constant).
    `get_tick_size_over_time` returns a Series indexed by month-start; this
    broadcasts it across every bar so the hot loop can do an O(1) array
    lookup instead of a pandas `.asof()` call per stop-loss update.

    Months where no precision could be inferred (e.g. BTC: every price in
    this dataset is a whole dollar, so there are no fractional digits to
    infer a tick size FROM) come back as NaN from `get_tick_size_over_time`.
    `get_pair_precision` does NOT skip rounding in that case — it falls back
    to the exchange's OWN declared price precision (`markets[pair]
    ['precision']['price']`, e.g. BTC's real $1 tick on Hyperliquid). Treating
    NaN as "don't round" left every BTC stop-loss/trailing fill at raw float
    precision instead of the real $1-rounded price, a small but ALWAYS
    same-signed bias (rounding is one-directional: ROUND_DOWN for shorts,
    ROUND_UP for longs) that showed up as 93/234 BTC trades all missing
    profit by the same sign — not random noise, a systematic gap.
    """
    prec_series = get_tick_size_over_time(df)
    fallback = np.nan
    if exchange is not None:
        try:
            fallback = float(exchange.get_precision_price(pair))
        except Exception:
            logger.debug("exchange.get_precision_price(%s) failed", pair, exc_info=True)

    if prec_series.empty:
        return np.full(len(dates), fallback)
    month_starts = prec_series.index.values.astype("datetime64[ns]")
    idx = np.searchsorted(month_starts, dates, side="right") - 1
    idx = np.clip(idx, 0, len(prec_series) - 1)
    per_bar = prec_series.to_numpy()[idx]
    return np.where(np.isnan(per_bar), fallback, per_bar)


def _round_stop(price: float, precision: float, is_short: bool) -> float:
    """freqtrade's `Trade.adjust_stop_loss`: every stop-loss update is rounded
    to the pair's tick size, ROUND_DOWN for shorts / ROUND_UP for longs (a
    deliberately conservative direction). Skipped when precision is unknown
    (NaN) rather than rounding to a made-up tick size."""
    if np.isnan(precision):
        return price
    return price_to_precision(
        price, precision, TICK_SIZE,
        rounding_mode=ROUND_DOWN if is_short else ROUND_UP,
    )


class _PairState:
    """Per-pair simulation state, mutated in place as the joint loop advances."""

    __slots__ = (
        "dates", "open", "high", "low", "atr", "el", "xl", "es", "xs", "n",
        "row_idx", "in_position", "is_short", "entry_price", "base_price",
        "entry_row", "trade_leverage", "stop_loss_price", "stop_is_trailing",
        "trailing_activated", "exchange_max_leverage", "precision", "trade_stake",
        "trade_amount", "local_trade",
    )

    def __init__(self, dates, openp, high, low, atr, el, xl, es, xs, exchange_max_leverage, precision):
        self.dates = dates
        self.open = openp
        self.high = high
        self.low = low
        self.atr = atr
        self.el, self.xl, self.es, self.xs = el, xl, es, xs
        self.precision = precision
        self.n = len(dates)
        self.row_idx = 0
        self.in_position = False
        self.is_short = False
        self.entry_price = 0.0
        self.base_price = 0.0
        self.entry_row = -1
        # Resolved once per pair (freqtrade's exchange max leverage is a
        # per-pair market limit, e.g. Hyperliquid: BTC=40x, VVV=3x — never a
        # global constant); see `_pair_exchange_max_leverage`. The STRATEGY's
        # own leverage() is resolved fresh per-trade in `do_enter` instead.
        self.exchange_max_leverage = exchange_max_leverage
        self.trade_leverage = 1.0
        self.stop_loss_price = 0.0
        self.stop_is_trailing = False
        self.trailing_activated = False
        self.trade_stake = 0.0
        self.trade_amount = 0.0
        self.local_trade = None  # the registered LocalTrade while a position is open


def _joint_multi_pair_backtest(
    processed: dict[str, pd.DataFrame], strategy: Any, config: dict, exchange: Any = None,
    full_signals: dict[str, pd.DataFrame] | None = None,
    required_startup: int = 0,
    dataprovider: Any = None,
) -> tuple[list[dict], int]:
    """Bar-by-bar joint simulation across all pairs, sharing one global
    `max_open_trades` slot count and freqtrade's exact per-candle pair order.

    Invokes `leverage()`, `custom_stake_amount()`, and `confirm_trade_entry()`
    per-trade, mirroring `Backtesting._enter_trade` / `get_valid_entry_price_
    and_stake`'s exact call order and semantics (2026-07-19 addition — these
    were previously NOT invoked at all; see the module docstring history).
    `custom_exit()` / `custom_stoploss()` remain unimplemented: `custom_
    stoploss` is a no-op on every strategy in this codebase anyway (none set
    `use_custom_stoploss = True`, the flag freqtrade itself gates it on — see
    `IStrategy.stop_loss_reached`), and no strategy here defines `custom_exit`.

    Returns (trades, rejected_count). Each trade dict carries raw fields (row
    indices, base/fee-adjusted prices) that `run_rust_backtest` formats into
    the standard trades DataFrame afterward.
    """
    eng = _build_engine_config(strategy, config)
    can_short = bool(getattr(strategy, "can_short", False)) and \
        str(config.get("trading_mode", "futures")) != "spot"
    max_open_trades = int(config.get("max_open_trades", 0) or 0)
    fallback_max_leverage = float(config.get("rust_leverage_max", 10.0))

    # IStrategy's own DEFAULT order_types is "limit" for both entry and exit
    # (order_time_in_force default "GTC") — NOT "market". Read the strategy's
    # actual values rather than assuming, since confirm_trade_entry/confirm_
    # trade_exit/custom_entry_price/custom_exit_price all receive/gate on this.
    _order_types = getattr(strategy, "order_types", {}) or {}
    _tif = getattr(strategy, "order_time_in_force", {}) or {}
    entry_order_type = _order_types.get("entry", "limit")
    exit_order_type = _order_types.get("exit", "limit")
    entry_tif = _tif.get("entry", "GTC")
    exit_tif = _tif.get("exit", "GTC")

    fee = eng["fee_taker"]
    base_stoploss = eng["base_stoploss"]
    trailing_enabled = eng["trailing_enabled"]
    trailing_trigger = eng["trailing_trigger"]
    trailing_offset = eng["trailing_offset"]
    roi_enabled = eng["roi_enabled"]
    roi_vals = eng["roi_vals"]
    roi_periods = eng["roi_periods"]
    max_hold_period = eng["max_hold_period"]
    # NOTE: atr_stop_enabled/atr_stop_multiplier/the precomputed `atr` array
    # below are now VESTIGIAL — they were a hardcoded reimplementation of ONE
    # specific custom_stoploss pattern (before this engine could call the real
    # method). do_exit_check now calls strategy.custom_stoploss() directly
    # (gated on use_custom_stoploss, exactly like the real engine), which
    # covers every implementation, not just the ATR-anchored one. Left
    # in place (harmless, unread by do_exit_check) rather than ripped out, to
    # avoid touching _PairState's constructor/slots for a low-risk cleanup.
    atr_stop_enabled = eng["atr_stop_enabled"]
    atr_stop_multiplier = eng["atr_stop_multiplier"]
    use_custom_stoploss = bool(getattr(strategy, "use_custom_stoploss", False))
    compounding = eng["compounding_enabled"]
    starting_balance = eng["starting_balance"]
    tradable_ratio = eng["tradable_balance_ratio"]
    max_trade_amount = eng["max_trade_amount"]

    import vulcan_rust_indicators as vri

    from VulcanTrader.enums import CandleType, TradingMode as _TradingModeEnum
    from VulcanTrader.persistence.trade_model import LocalTrade, Order

    # --- Callback wiring setup (2026-07-19) ----------------------------------
    # Fresh trade registry so leverage()/custom_stake_amount()/confirm_trade_
    # entry() — which read Trade.get_trades_proxy() (portfolio-risk vetoes in
    # new_risk_management.py) — see only THIS run's positions, not anything
    # left over from a prior run in the same process.
    LocalTrade.reset_trades()

    # Cache each pair's FULL (untrimmed) analyzed dataframe into the strategy's
    # real DataProvider, exactly as the Python engine's own backtest_loop does
    # (Backtesting._set_cached_df, called once per pair before the loop starts)
    # — so self.dp.get_analyzed_dataframe(pair, timeframe) inside any callback
    # returns real, causally-correct data instead of empty/stale results. The
    # per-bar slice index (_set_dataframe_max_index) is updated as each pair's
    # row_idx advances in the main loop below, mirroring backtesting.py's
    # `required_startup + row_index` exactly.
    _dp_wired = dataprovider is not None and full_signals is not None
    if _dp_wired:
        _candle_type = config.get("candle_type_def", CandleType.SPOT)
        _tf = eng["timeframe"]
        for _pair, _full_df in full_signals.items():
            try:
                dataprovider._set_cached_df(_pair, _tf, _full_df, _candle_type)
            except Exception:
                logger.debug("DataProvider._set_cached_df(%s) failed", _pair, exc_info=True)

    pair_states: dict[str, _PairState] = {}
    for pair, df in processed.items():
        if df is None or df.empty:
            continue
        n = len(df)
        dates = pd.to_datetime(df["date"]).to_numpy(dtype="datetime64[ns]")
        openp, high, low = _f64(df["open"], n), _f64(df["high"], n), _f64(df["low"], n)
        close, vol = _f64(df["close"], n), _f64(df["volume"], n)

        atr = None
        if atr_stop_enabled:
            ind = vri.calculate_standard_indicators(close, high, low, vol)
            atr = np.ascontiguousarray(ind[_ATR], dtype=np.float64)

        el = _shift1(_u8(df.get("enter_long"), n))
        xl = _shift1(_u8(df.get("exit_long"), n))
        es = _shift1(_u8(df.get("enter_short"), n)) if can_short else np.zeros(n, dtype=np.uint8)
        xs = _shift1(_u8(df.get("exit_short"), n)) if can_short else np.zeros(n, dtype=np.uint8)

        exch_max_lev = _pair_exchange_max_leverage(exchange, pair, max_trade_amount, fallback_max_leverage)
        precision = _precision_per_bar(df, dates, exchange, pair)
        pair_states[pair] = _PairState(dates, openp, high, low, atr, el, xl, es, xs, exch_max_lev, precision)

    if not pair_states:
        return [], 0

    pair_whitelist = list(pair_states.keys())  # dict preserves the caller's insertion order
    all_dates = np.unique(np.concatenate([p.dates for p in pair_states.values()]))
    all_dates.sort()

    is_futures = str(config.get("trading_mode", "futures")) != "spot"
    funding_mark: dict[str, pd.DataFrame] = {}
    if is_futures and exchange is not None:
        try:
            funding_mark = _load_funding_mark_data(config, exchange, pair_whitelist)
        except Exception:
            logger.exception("Failed to load funding/mark data; funding fees will be skipped")

    def funding_fee_for(pair: str, amount: float, is_short: bool,
                        open_date: pd.Timestamp, close_date: pd.Timestamp) -> float:
        df = funding_mark.get(pair)
        if df is None or df.empty:
            return 0.0
        try:
            return float(exchange.calculate_funding_fees(df, amount, is_short, open_date, close_date))
        except Exception:
            logger.debug("calculate_funding_fees(%s) failed", pair, exc_info=True)
            return 0.0

    trades: list[dict] = []
    rejected = 0
    open_count = 0
    open_order: list[str] = []  # pairs currently open, in entry order

    fixed_stake = min(starting_balance * tradable_ratio, max_trade_amount)
    running_balance = starting_balance
    tied_up_capital = 0.0  # sum of stakes of currently-open trades

    def current_stake() -> float:
        if not compounding:
            return fixed_stake
        return min(running_balance * tradable_ratio, max_trade_amount)

    def capital_available(stake: float) -> bool:
        """freqtrade's `Wallets.get_trade_stake_amount` /
        `_check_available_stake_amount`: a fixed stake_amount is NOT always
        fillable just because a max_open_trades slot is free — the wallet's
        available capital also has to cover it. In `get_total_stake_amount`,
        `(tied_up + free) * ratio` algebraically reduces to
        `(starting_balance + realized_profit) * ratio` (tied_up cancels), so
        available = that minus the capital already tied up in open trades.
        As a losing run's realized balance shrinks, this constraint starts
        rejecting entries even while slots remain nominally open — exactly
        the kind of rejection a naive "just check open_count" model misses,
        and which showed up as this engine trading noticeably more than
        Python everywhere once trading in negative territory.
        """
        total_stake_amount = running_balance * tradable_ratio
        return (total_stake_amount - tied_up_capital) >= stake

    class _RustWalletsProxy:
        """Minimal stand-in for the real `Wallets` object, monkey-patched onto
        `strategy.wallets` for the duration of this run. `custom_stake_amount`
        implementations in this codebase call `self.wallets.get_total_stake_
        amount()` to feed their ECS engine's `.update(equity)` — the REAL
        `Wallets` object is never touched by this engine's own loop (it
        bypasses freqtrade's order/wallet pipeline entirely), so leaving it
        unpatched would return a frozen `dry_run_wallet` figure all run,
        silently disabling ECS's equity-curve adaptation. `get_total_stake_
        amount` algebraically mirrors the real formula (see
        `capital_available`'s docstring) using this loop's own running_balance.
        """
        def get_total_stake_amount(self) -> float:
            return running_balance * tradable_ratio

        def get_available_stake_amount(self) -> float:
            return max(running_balance * tradable_ratio - tied_up_capital, 0.0)

    def _resolve_stake_and_leverage(pair: str, p: "_PairState", i: int, direction: str,
                                    proposed_stake: float) -> tuple[float, float]:
        """leverage() then custom_stake_amount(), in that exact order — the
        real engine resolves leverage first and passes it INTO custom_stake_
        amount (see Backtesting.get_valid_entry_price_and_stake)."""
        current_time = pd.Timestamp(p.dates[i]).tz_localize("UTC")
        current_rate = p.open[i]

        leverage = _resolve_leverage(
            strategy, pair, current_time, current_rate, p.exchange_max_leverage, None, direction,
        )

        stake = proposed_stake
        try:
            result = strategy.custom_stake_amount(
                pair=pair, current_time=current_time, current_rate=current_rate,
                proposed_stake=proposed_stake, min_stake=0.0,
                max_stake=max(running_balance * tradable_ratio - tied_up_capital, 0.0),
                leverage=leverage, entry_tag=None, side=direction,
            )
            if result is not None and float(result) > 0:
                stake = float(result)
        except Exception:
            logger.debug("strategy.custom_stake_amount(%s) not usable; using proposed stake", pair, exc_info=True)

        return stake, leverage

    def _confirm_entry(pair: str, current_time, amount: float, rate: float, direction: str) -> bool:
        try:
            return bool(strategy.confirm_trade_entry(
                pair=pair, order_type=entry_order_type, amount=amount, rate=rate,
                time_in_force=entry_tif, current_time=current_time, entry_tag=None, side=direction,
            ))
        except Exception:
            logger.debug("strategy.confirm_trade_entry(%s) raised; allowing entry", pair, exc_info=True)
            return True

    def _resolve_entry_price(pair: str, current_time, proposed_rate: float, direction: str) -> float:
        """custom_entry_price(), gated on order_type=='limit' exactly like the
        real engine's get_valid_entry_price_and_stake — the default
        implementation returns proposed_rate unchanged, so this is a genuine
        no-op for every strategy that doesn't override it (none currently do),
        and correct for any that eventually will."""
        if entry_order_type != "limit":
            return proposed_rate
        try:
            new_rate = strategy.custom_entry_price(
                pair=pair, trade=None, current_time=current_time,
                proposed_rate=proposed_rate, entry_tag=None, side=direction,
            )
            if new_rate is not None:
                return float(new_rate)
        except Exception:
            logger.debug("strategy.custom_entry_price(%s) not usable; using proposed rate", pair, exc_info=True)
        return proposed_rate

    def _resolve_exit_price(pair: str, trade, current_time, proposed_rate: float,
                            current_profit: float, exit_tag: str | None, precision: float) -> float:
        """custom_exit_price(), gated on order_type=='limit' AND only for
        Signal/CustomExit exits — mirrors `_get_exit_for_signal` exactly:
        ROI/Stoploss/Trailing exits never call this."""
        if exit_order_type != "limit" or not _dp_wired or trade is None:
            return proposed_rate
        try:
            new_rate = strategy.custom_exit_price(
                pair=pair, trade=trade, current_time=current_time,
                proposed_rate=proposed_rate, current_profit=current_profit, exit_tag=exit_tag,
            )
        except Exception:
            logger.debug("strategy.custom_exit_price(%s) not usable; using proposed rate", pair, exc_info=True)
            return proposed_rate
        if new_rate is None or float(new_rate) == proposed_rate:
            return proposed_rate
        if np.isnan(precision):
            return float(new_rate)
        return price_to_precision(float(new_rate), precision, TICK_SIZE)

    def _confirm_exit(pair: str, trade, current_time, amount: float, rate: float, exit_reason: str) -> bool:
        if not _dp_wired or trade is None:
            return True
        try:
            return bool(strategy.confirm_trade_exit(
                pair=pair, trade=trade, order_type=exit_order_type, amount=amount, rate=rate,
                time_in_force=exit_tif, sell_reason=exit_reason, exit_reason=exit_reason,
                current_time=current_time,
            ))
        except Exception:
            logger.debug("strategy.confirm_trade_exit(%s) raised; allowing exit", pair, exc_info=True)
            return True

    _order_id_counter = 0

    def _notify_order_filled(pair: str, trade, side: str, order_type: str, amount: float,
                             price: float, current_time) -> None:
        """order_filled(): dormant callback (no current strategy overrides it),
        called best-effort right after an entry/exit instant-fills — matches
        `_try_close_open_order`'s call site, minus the pending-order lifecycle
        neither engine's instant-fill core models (see `adjust_order_price`)."""
        nonlocal _order_id_counter
        if not _dp_wired or trade is None:
            return
        _order_id_counter += 1
        try:
            order = Order(
                id=_order_id_counter, order_id=str(_order_id_counter), ft_trade_id=0,
                ft_order_side=side, ft_pair=pair, ft_amount=amount, ft_price=price,
                ft_is_open=False, status="closed", symbol=pair, order_type=order_type,
                side=side, price=price, average=price, amount=amount, filled=amount,
                remaining=0.0, cost=amount * price, order_date=current_time,
                order_filled_date=current_time, order_update_date=current_time,
            )
            strategy.order_filled(pair=pair, trade=trade, order=order, current_time=current_time)
        except Exception:
            logger.debug("strategy.order_filled(%s) raised", pair, exc_info=True)

    _orig_wallets = getattr(strategy, "wallets", None)
    if _dp_wired:
        strategy.wallets = _RustWalletsProxy()

    def entry_dir(p: _PairState, i: int) -> str | None:
        enter_long, exit_long = p.el[i] == 1, p.xl[i] == 1
        enter_short = can_short and p.es[i] == 1
        exit_short = can_short and p.xs[i] == 1
        if enter_long and not (exit_long or enter_short):
            return "long"
        if enter_short and not (exit_short or enter_long):
            return "short"
        return None

    def do_enter(pair: str, p: _PairState, i: int, direction: str, proposed_stake: float) -> bool:
        """Returns True if a position was actually opened. Mirrors
        `Backtesting._enter_trade` / `get_valid_entry_price_and_stake`'s exact
        order: resolve leverage -> resolve stake (custom_stake_amount, given
        that leverage) -> round amount to lot size -> back-calculate the real
        stake from the rounded amount -> confirm_trade_entry with the FINAL
        amount/rate -> only THEN commit any state or register a trade."""
        nonlocal open_count, tied_up_capital, rejected
        is_short = direction == "short"
        current_time = pd.Timestamp(p.dates[i]).tz_localize("UTC")
        base_price = p.open[i]

        if _dp_wired:
            base_price = _resolve_entry_price(pair, current_time, base_price, direction)
            # "We can't place orders higher than current high" / lower than
            # current low (get_valid_entry_price_and_stake) — a no-op given
            # custom_entry_price's own no-op default, kept for when it isn't.
            base_price = max(base_price, p.low[i]) if is_short else min(base_price, p.high[i])
            stake, lev = _resolve_stake_and_leverage(pair, p, i, direction, proposed_stake)
        else:
            stake, lev = proposed_stake, p.exchange_max_leverage
        lev = max(lev, 0.1)

        # Mirror `wallets.validate_stake_amount`: custom_stake_amount() can
        # inflate the proposed stake (e.g. ECS pressing size on a hot streak);
        # clamp to what capital is actually still available rather than
        # blindly trusting the strategy's number.
        available = max(running_balance * tradable_ratio - tied_up_capital, 0.0)
        stake = min(stake, available)
        if stake <= 0:
            rejected += 1
            return False

        # freqtrade's `_enter_trade`: amount is derived from the NOMINAL stake
        # (`stake*leverage/propose_rate`), then rounded to the exchange's lot
        # size (`amount_to_contract_precision`), and the stake is then
        # BACK-CALCULATED from that rounded amount — so the capital actually
        # tied up (and hence every downstream profit_abs figure) is the
        # post-rounding value, not the nominal $10k. For coarse lot sizes
        # (e.g. XRP/KPEPE round to whole units) this shifts the realized
        # stake by a small but real amount each trade, compounding over
        # thousands of trades into a wallet-balance drift that can flip a
        # capital-availability decision under heavy contention.
        actual_stake = stake
        amount = stake * lev / base_price
        if exchange is not None:
            try:
                amount_rounded = exchange.amount_to_contract_precision(pair, amount)
                if amount_rounded > 0:
                    amount = amount_rounded
                    actual_stake = amount * base_price / lev
            except Exception:
                logger.debug("amount_to_contract_precision(%s) failed", pair, exc_info=True)

        if _dp_wired and not _confirm_entry(pair, current_time, amount, base_price, direction):
            rejected += 1
            return False

        p.in_position = True
        p.is_short = is_short
        p.base_price = base_price
        p.entry_price = base_price * ((1.0 + fee) if not is_short else (1.0 - fee))
        p.entry_row = i
        p.trade_leverage = lev
        p.trade_stake = actual_stake
        p.trade_amount = amount
        p.trailing_activated = False
        p.stop_is_trailing = False
        raw_stop = base_price * (1.0 + (base_stoploss / lev if not is_short else -base_stoploss / lev))
        p.stop_loss_price = _round_stop(raw_stop, p.precision[i], is_short)
        open_count += 1
        tied_up_capital += actual_stake
        open_order.append(pair)

        if _dp_wired:
            try:
                trade = LocalTrade(
                    pair=pair, is_short=is_short, amount=amount, open_rate=base_price,
                    fee_open=fee, fee_close=fee, leverage=lev,
                    trading_mode=_TradingModeEnum.FUTURES if is_futures else _TradingModeEnum.SPOT,
                    open_date=current_time, is_open=True,
                    stake_amount=actual_stake, max_stake_amount=actual_stake,
                )
                LocalTrade.add_bt_trade(trade)
                p.local_trade = trade
                _notify_order_filled(
                    pair, trade, "sell" if is_short else "buy", entry_order_type,
                    amount, base_price, current_time,
                )
            except Exception:
                logger.debug("LocalTrade registration failed for %s", pair, exc_info=True)
                p.local_trade = None

        return True

    def do_exit_check(pair: str, p: _PairState, i: int) -> bool:
        """Full priority-ordered exit check + fill-price computation for the
        given already-open pair at bar i. Returns True if it closed."""
        nonlocal open_count, running_balance, tied_up_capital
        is_long = not p.is_short
        time_in_position = i - p.entry_row
        high_i, low_i, open_i = p.high[i], p.low[i], p.open[i]
        base_price, entry_price, lev = p.base_price, p.entry_price, p.trade_leverage
        amount = p.trade_amount
        bound = high_i if is_long else low_i

        # freqtrade's `should_exit` calls `trade.adjust_min_max_rates(high or
        # current_rate, low or current_rate)` unconditionally, BEFORE any
        # stoploss/ROI/exit-signal/custom_exit logic runs — updating
        # `trade.max_rate`/`trade.min_rate` every single bar a position is
        # open. Skipping this left both fields permanently None on every
        # LocalTrade this engine creates: any strategy callback that reads
        # them (Strat2's custom_exit trailing-profit-pullback checks,
        # AlphaHunterV5's MFE/pullback logic) threw a silent TypeError
        # (`None - float`), caught by this engine's own blanket
        # exception-to-False handling around custom_exit/custom_stoploss —
        # so the callback always looked like "no exit"/"no adjustment" with
        # no visible error.
        if p.local_trade is not None:
            p.local_trade.adjust_min_max_rates(high_i, low_i)

        # freqtrade's `_run_funding_fees` re-accrues `trade.funding_fees` on
        # EVERY bar the trade is open (open_date -> current bar), and
        # `calc_profit_ratio`/`calc_close_trade_value` always read that
        # CURRENT, progressively-growing value — not just at final exit. So
        # `bound_profit` (the trailing/ROI trigger check) already has
        # whatever funding has accrued so far baked in. Missing this meant a
        # trade could sit just below a 2% trailing-activation threshold by
        # our count while Python's real bound_profit — nudged over by
        # already-accrued funding on a pair with a large funding rate — was
        # just above it, activating trailing a full bar early and producing
        # a completely different (and much more favorable) exit price.
        funding_so_far = 0.0
        if is_futures:
            funding_so_far = funding_fee_for(
                pair, amount, p.is_short,
                pd.Timestamp(p.dates[p.entry_row]).tz_localize("UTC"),
                pd.Timestamp(p.dates[i]).tz_localize("UTC"),
            )
        if is_long:
            open_value = amount * base_price * (1.0 + fee)
            close_value_bound = amount * bound * (1.0 - fee) + funding_so_far
            leveraged_bound_return = ((close_value_bound / open_value) - 1.0) * lev if open_value else 0.0
        else:
            open_value = amount * base_price * (1.0 - fee)
            close_value_bound = amount * bound * (1.0 + fee) - funding_so_far
            leveraged_bound_return = (1.0 - (close_value_bound / open_value)) * lev if open_value else 0.0

        stop_loss_price = p.stop_loss_price
        stop_is_trailing = p.stop_is_trailing
        trailing_activated = p.trailing_activated
        dir_correct = (stop_loss_price < low_i) if is_long else (stop_loss_price > high_i)

        # freqtrade's `adjust_stop_loss` rounds EVERY candidate to the pair's
        # tick size (ROUND_DOWN for shorts / ROUND_UP for longs) BEFORE
        # comparing it against the current stop — not after. Comparing
        # unrounded floats here lets hairline (sub-tick) differences flip a
        # threshold the real engine would round away, which is exactly the
        # kind of divergence that showed up under heavy multi-pair slot
        # contention (see `_round_stop`).
        precision_i = p.precision[i]
        if dir_correct:
            if use_custom_stoploss and _dp_wired and p.local_trade is not None:
                # Mirrors trader_stoploss_adjust exactly: call custom_stoploss()
                # with current_rate=bound, current_profit=bound_profit (=
                # leveraged_bound_return, already computed above at `bound`),
                # then adjust_stop_loss's own formula —
                # new_loss = bound * (1 +/- abs(stoploss_ratio)/leverage) —
                # applied through the SAME round + tighter-wins ratchet already
                # used for the trailing/ATR cases below.
                current_time_sl = pd.Timestamp(p.dates[i]).tz_localize("UTC")
                try:
                    sl_ratio = strategy.custom_stoploss(
                        pair=pair, trade=p.local_trade, current_time=current_time_sl,
                        current_rate=bound, current_profit=leveraged_bound_return,
                        after_fill=False,
                    )
                except Exception:
                    logger.debug("strategy.custom_stoploss(%s) raised", pair, exc_info=True)
                    sl_ratio = None
                if sl_ratio is not None and not (math.isnan(sl_ratio) or math.isinf(sl_ratio)):
                    dist = abs(float(sl_ratio)) / lev
                    candidate = bound * (1.0 - dist) if is_long else bound * (1.0 + dist)
                    candidate = _round_stop(candidate, precision_i, not is_long)
                    tighter = (candidate > stop_loss_price) if is_long else (candidate < stop_loss_price)
                    if tighter:
                        stop_loss_price, stop_is_trailing = candidate, False
            if trailing_enabled:
                if not trailing_activated and leveraged_bound_return >= trailing_trigger:
                    trailing_activated = True
                if trailing_activated:
                    trail_dist = trailing_offset / lev
                    candidate = bound * (1.0 - trail_dist) if is_long else bound * (1.0 + trail_dist)
                    candidate = _round_stop(candidate, precision_i, not is_long)
                    tighter = (candidate > stop_loss_price) if is_long else (candidate < stop_loss_price)
                    if tighter:
                        stop_loss_price, stop_is_trailing = candidate, True

        stop_triggered = (low_i <= stop_loss_price) if is_long else (high_i >= stop_loss_price)
        roi_triggered = roi_enabled and (
            (time_in_position >= roi_periods[0] and leveraged_bound_return >= roi_vals[0]) or
            (time_in_position >= roi_periods[1] and leveraged_bound_return >= roi_vals[1]) or
            (time_in_position >= roi_periods[2] and leveraged_bound_return >= roi_vals[2]) or
            (time_in_position >= roi_periods[3] and leveraged_bound_return >= roi_vals[3])
        )
        # freqtrade should_exit: `if exit_ and not enter` — a same-direction
        # entry signal on this bar suppresses the exit-signal reading.
        if is_long:
            signal_exit_triggered = (p.xl[i] > 0) and not (p.el[i] > 0)
        else:
            signal_exit_triggered = (p.xs[i] > 0) and not (p.es[i] > 0)
        max_hold_triggered = time_in_position >= max_hold_period

        # freqtrade's `should_exit`: custom_exit() is only evaluated in the
        # `else` branch when there's no raw exit-signal this bar, using the
        # bar's OPEN (`current_rate = rate = row[OPEN_IDX]`) — NOT the
        # high/low `bound` used above for stoploss/ROI/trailing.
        custom_exit_triggered, custom_exit_tag = False, None
        if not signal_exit_triggered and _dp_wired and p.local_trade is not None:
            current_time_ce = pd.Timestamp(p.dates[i]).tz_localize("UTC")
            if is_long:
                ov_ce = amount * base_price * (1.0 + fee)
                cv_ce = amount * open_i * (1.0 - fee) + funding_so_far
                leveraged_open_return = ((cv_ce / ov_ce) - 1.0) * lev if ov_ce else 0.0
            else:
                ov_ce = amount * base_price * (1.0 - fee)
                cv_ce = amount * open_i * (1.0 + fee) - funding_so_far
                leveraged_open_return = (1.0 - (cv_ce / ov_ce)) * lev if ov_ce else 0.0
            try:
                reason_cust = strategy.custom_exit(
                    pair=pair, trade=p.local_trade, current_time=current_time_ce,
                    current_rate=open_i, current_profit=leveraged_open_return,
                )
            except Exception:
                logger.debug("strategy.custom_exit(%s) raised", pair, exc_info=True)
                reason_cust = False
            if reason_cust:
                custom_exit_triggered = True
                custom_exit_tag = reason_cust if isinstance(reason_cust, str) else None

        # freqtrade's documented priority: Exit-signal (incl. custom_exit),
        # Stoploss, ROI, Trailing stoploss (`should_exit`'s own comment). A
        # veto from confirm_trade_exit (below) falls through to the NEXT
        # candidate in this same bar, exactly like `_check_trade_exit`'s
        # `for exit_ in exits: ... if t: return t` loop.
        candidates: list[tuple[str, str | None]] = []
        if signal_exit_triggered:
            candidates.append(("Signal", None))
        elif custom_exit_triggered:
            candidates.append(("CustomExit", custom_exit_tag))
        if stop_triggered and not stop_is_trailing:
            candidates.append(("Stoploss", None))
        if roi_triggered:
            candidates.append(("RoiTarget", None))
        if stop_triggered and stop_is_trailing:
            candidates.append(("TrailingStop", None))
        if max_hold_triggered:
            candidates.append(("MaxHoldPeriod", None))

        p.stop_loss_price, p.stop_is_trailing, p.trailing_activated = \
            stop_loss_price, stop_is_trailing, trailing_activated

        if not candidates:
            return False

        current_time_exit = pd.Timestamp(p.dates[i]).tz_localize("UTC")

        for exit_reason, custom_tag in candidates:
            # freqtrade parity: a trailing stop that arms-and-triggers within
            # the ENTRY candle is placed as an ORDER at the pessimistic price;
            # if that price is outside this candle's range the order can't
            # fill, so freqtrade DEFERS the exit to a later bar.
            if exit_reason == "TrailingStop" and time_in_position == 0:
                trail_dist = trailing_offset / lev
                pess = open_i * (1.0 + abs(trailing_trigger) - abs(trail_dist)) if is_long \
                    else open_i * (1.0 - abs(trailing_trigger) + abs(trail_dist))
                fills = (pess <= high_i) if is_long else (pess >= low_i)
                if not fills:
                    continue

            if exit_reason == "TrailingStop" and time_in_position == 0:
                trail_dist = trailing_offset / lev
                if is_long:
                    base_exit_price = max(open_i * (1.0 + abs(trailing_trigger) - abs(trail_dist)), low_i)
                else:
                    base_exit_price = min(open_i * (1.0 - abs(trailing_trigger) + abs(trail_dist)), high_i)
            elif exit_reason in ("TrailingStop", "Stoploss"):
                # freqtrade's `_get_close_rate_for_stoploss`: if the ratcheted
                # stop sits entirely outside this candle's range (never
                # actually touchable this candle), exit at the candle's open
                # instead.
                if is_long:
                    base_exit_price = open_i if stop_loss_price > high_i else stop_loss_price
                else:
                    base_exit_price = open_i if stop_loss_price < low_i else stop_loss_price
            elif exit_reason == "RoiTarget":
                if time_in_position >= roi_periods[3] and leveraged_bound_return >= roi_vals[3]:
                    roi_pct, roi_entry_period = roi_vals[3] / lev, roi_periods[3]
                elif time_in_position >= roi_periods[2] and leveraged_bound_return >= roi_vals[2]:
                    roi_pct, roi_entry_period = roi_vals[2] / lev, roi_periods[2]
                elif time_in_position >= roi_periods[1] and leveraged_bound_return >= roi_vals[1]:
                    roi_pct, roi_entry_period = roi_vals[1] / lev, roi_periods[1]
                else:
                    roi_pct, roi_entry_period = roi_vals[0] / lev, roi_periods[0]
                # Mirror freqtrade's `_get_close_rate_for_roi` fee-consistent
                # solve rather than a naive `base_price * (1 +/- roi_pct)`.
                side_1 = 1.0 if is_long else -1.0
                roi_rate = base_price * roi_pct
                open_fee_rate = side_1 * base_price * (1.0 + side_1 * fee)
                raw_close = -(roi_rate + open_fee_rate) / (fee - side_1)
                # freqtrade's `_get_close_rate_for_roi`: if this is exactly
                # the bar a new (tighter) ROI tier takes effect, AND the
                # candle's own open already exceeds that tier's computed
                # close_rate, use the open price instead — the trade was
                # already past the new target before this bar's own
                # close_rate could apply. Missing this made the engine use
                # the solved close_rate even when the real open price gapped
                # straight through it, understating profit by the full gap
                # size on the (rare, but not tiny) trades that hit it.
                if time_in_position == roi_entry_period:
                    is_new_roi = (open_i > raw_close) if is_long else (open_i < raw_close)
                    if is_new_roi:
                        raw_close = open_i
                lo, hi = min(low_i, high_i), max(low_i, high_i)
                base_exit_price = min(max(raw_close, lo), hi)
            else:  # MaxHoldPeriod / Signal / CustomExit -> candle's own open
                base_exit_price = open_i

            reason_tag = _EXIT_REASON_MAP.get(exit_reason, "exit_signal")
            if exit_reason == "CustomExit" and custom_tag:
                reason_tag = custom_tag

            if exit_reason in ("Signal", "CustomExit") and _dp_wired and p.local_trade is not None:
                # current_profit for custom_exit_price, computed at the
                # already-resolved close rate (matches `_get_exit_for_signal`:
                # `current_profit = trade.calc_profit_ratio(close_rate)`,
                # evaluated BEFORE custom_exit_price is applied).
                if is_long:
                    ov_p = amount * base_price * (1.0 + fee)
                    cv_p = amount * base_exit_price * (1.0 - fee) + funding_so_far
                    profit_at_close = ((cv_p / ov_p) - 1.0) * lev if ov_p else 0.0
                else:
                    ov_p = amount * base_price * (1.0 - fee)
                    cv_p = amount * base_exit_price * (1.0 + fee) - funding_so_far
                    profit_at_close = (1.0 - (cv_p / ov_p)) * lev if ov_p else 0.0
                base_exit_price = _resolve_exit_price(
                    pair, p.local_trade, current_time_exit, base_exit_price,
                    profit_at_close, reason_tag, precision_i,
                )
                # "We can't place orders lower than current low" (long) /
                # "higher than current high" (short) — VulcanTrader doesn't
                # support out-of-range limit exits in live either.
                base_exit_price = max(base_exit_price, low_i) if is_long else min(base_exit_price, high_i)

            if not _confirm_exit(pair, p.local_trade, current_time_exit, amount, base_exit_price, reason_tag):
                continue  # vetoed -> try the next candidate this same bar

            exit_price = base_exit_price * (1.0 - fee) if is_long else base_exit_price * (1.0 + fee)

            # Use the stake fixed AT ENTRY, not one recomputed now — matters
            # once `compounding_enabled` makes `current_stake()` time-varying.
            stake = p.trade_stake

            # freqtrade's REAL profit_abs/profit_ratio come from `Trade.
            # calc_close_trade_value` / `_calc_open_trade_value`: amount*price
            # trade VALUES, not stake*profit_ratio. profit_ratio's `amount`
            # term cancels out of that ratio (this formula IS still exactly
            # `raw_profit*leverage` when funding_fees==0 — verified
            # algebraically and empirically), which is why the simpler
            # stake-based version already matched profit_ratio exactly.
            # profit_abs is a DIFFERENCE though, so `amount` does NOT cancel
            # there, and for FUTURES trades `close_trade_value` also has
            # accrued funding fees added in (subtracted for shorts) — both
            # need the real amount-based formula. `funding_so_far` was
            # already computed above (entry_row -> i, same range) for the
            # bound_profit check — this bar's exit reuses it as-is.
            funding_fees = funding_so_far

            if is_long:
                open_value = amount * base_price * (1.0 + fee)
                close_value = amount * base_exit_price * (1.0 - fee) + funding_fees
                pnl_amount = close_value - open_value
                leveraged_profit = ((close_value / open_value) - 1.0) * lev if open_value else 0.0
            else:
                open_value = amount * base_price * (1.0 - fee)
                close_value = amount * base_exit_price * (1.0 + fee) - funding_fees
                pnl_amount = open_value - close_value
                leveraged_profit = (1.0 - (close_value / open_value)) * lev if open_value else 0.0

            running_balance += pnl_amount
            tied_up_capital -= stake

            trades.append({
                "pair": pair, "is_short": p.is_short, "entry_row": p.entry_row, "exit_row": i,
                "entry_price": entry_price, "base_exit_price": base_exit_price, "exit_price": exit_price,
                "profit_ratio": leveraged_profit, "profit_abs": pnl_amount,
                "leverage": lev, "exit_reason": exit_reason, "exit_reason_tag": reason_tag, "stake": stake,
                "funding_fees": funding_fees,
            })

            p.in_position = False
            open_count -= 1
            if pair in open_order:
                open_order.remove(pair)

            if p.local_trade is not None:
                trade = p.local_trade
                trade.close_date = current_time_exit
                trade.close_profit = leveraged_profit
                trade.close_profit_abs = pnl_amount
                trade.is_open = False
                try:
                    LocalTrade.close_bt_trade(trade)
                except Exception:
                    logger.debug("LocalTrade.close_bt_trade failed for %s", pair, exc_info=True)
                _notify_order_filled(
                    pair, trade, "sell" if is_long else "buy", exit_order_type,
                    amount, base_exit_price, current_time_exit,
                )
                p.local_trade = None
            return True

        return False  # every candidate this bar was vetoed by confirm_trade_exit

    def process_pass(pair: str, p: _PairState, i: int, direction: str | None,
                     can_enter_bar: bool) -> str | None:
        """One backtest_loop-equivalent pass. Returns the pre-call open
        direction (or None), used by the reversal-retry loop."""
        nonlocal open_count, rejected
        exiting_dir_before = ("short" if p.is_short else "long") if p.in_position else None

        # No same-bar cooldown here: freqtrade's real entry guard is only
        # "currently flat" (`len(bt_trades_open_pp[pair])==0`). A same-candle
        # reversal (opposite-direction signal closes one side and opens the
        # other within the SAME bar, via the two-pass loop below) legitimately
        # needs `i == last_exit_row` to succeed on pass 2.
        # `can_enter_bar` mirrors freqtrade's own `backtest_loop(..., can_enter=
        # not is_last_row)`: the very last candle of the backtest range never
        # opens new positions (only `handle_left_open`'s end-of-run force-close
        # applies to anything still open) — without this, the engine opened
        # AND immediately force-closed a same-candle trade on the final bar
        # that real freqtrade never took at all.
        if can_enter_bar and (not p.in_position) and direction is not None:
            if max_open_trades <= 0 or open_count < max_open_trades:
                stake = current_stake()
                if stake > 0 and capital_available(stake):
                    do_enter(pair, p, i, direction, stake)
                else:
                    rejected += 1
            else:
                rejected += 1

        if p.in_position:
            do_exit_check(pair, p, i)

        return exiting_dir_before

    last_ts = all_dates[-1] if len(all_dates) else None
    for ts in all_dates:
        can_enter_bar = ts != last_ts
        if _dp_wired:
            _ts_now = pd.Timestamp(ts).tz_localize("UTC")
            try:
                strategy.bot_loop_start(current_time=_ts_now)
            except Exception:
                logger.debug("strategy.bot_loop_start failed", exc_info=True)
            # `self.dp.get_pair_dataframe(pair, tf)` (used by strategies for
            # informative-timeframe data, e.g. HigherTimeframeDeviationReversion's
            # own 1h fetch — a DIFFERENT method from get_analyzed_dataframe,
            # with its OWN separate lookahead-safety mechanism keyed off a
            # single global cutoff DATE rather than a per-pair row index).
            # backtesting.py sets this every bar via `_set_dataframe_max_date`;
            # without it, get_pair_dataframe returns the full, untrimmed
            # historical series — a real lookahead bias, not a cosmetic gap.
            try:
                dataprovider._set_dataframe_max_date(_ts_now)
            except Exception:
                logger.debug("DataProvider._set_dataframe_max_date failed", exc_info=True)
        # freqtrade's exact per-candle order: pairs with open trades first (in
        # entry order, so a slot they free is available to later pairs this
        # same candle), then flat pairs in whitelist order.
        ordered = list(dict.fromkeys(open_order + pair_whitelist))
        for pair in ordered:
            p = pair_states[pair]
            idx = p.row_idx
            if idx >= p.n or p.dates[idx] != ts:
                continue
            p.row_idx = idx + 1
            if _dp_wired:
                # Mirrors backtesting.py's own `_set_dataframe_max_index(pair,
                # required_startup + row_index)` call — but empirically
                # verified (via a live-monkeypatched python-engine run,
                # Strat2/RESOLV 2026-06-23 13:00-14:10) that this makes
                # `get_analyzed_dataframe(pair).iloc[-1]` land on bar `idx-1`,
                # ONE BAR BEHIND the bar currently executing (`idx`) — i.e.
                # `row[OPEN_IDX]`/current_rate come from the just-opened bar
                # `idx`, but indicator lookups only ever see the LAST FULLY
                # CLOSED candle, `idx-1`. Passing `required_startup + idx`
                # (not `+ p.row_idx`, which is already post-increment to
                # `idx+1`) reproduces that exactly: slicing is exclusive-end,
                # so `iloc[-1]` = absolute row `required_startup+idx-1` = bar
                # `idx-1`. Using `p.row_idx` here (the old, wrong version)
                # let callbacks peek at the still-forming current bar's own
                # close/high/low/derived indicators — a real lookahead bug
                # that only became reachable once custom_exit/custom_stoploss/
                # confirm_trade_entry started actually calling
                # get_analyzed_dataframe this session.
                try:
                    dataprovider._set_dataframe_max_index(pair, required_startup + idx)
                except Exception:
                    logger.debug("DataProvider._set_dataframe_max_index(%s) failed", pair, exc_info=True)
            direction = entry_dir(p, idx)

            for _ in range(2):
                prior_dir = process_pass(pair, p, idx, direction, can_enter_bar)
                if not can_short or prior_dir is None or prior_dir == direction:
                    break

    # Force-close anything still open at the end (handle_left_open).
    for pair, p in pair_states.items():
        if not p.in_position:
            continue
        idx = p.n - 1
        is_long = not p.is_short
        open_i = p.open[idx]
        exit_price = open_i * (1.0 - fee) if is_long else open_i * (1.0 + fee)
        lev = p.trade_leverage
        stake = p.trade_stake
        amount = p.trade_amount

        funding_fees = 0.0
        if is_futures:
            funding_fees = funding_fee_for(
                pair, amount, p.is_short,
                pd.Timestamp(p.dates[p.entry_row]).tz_localize("UTC"),
                pd.Timestamp(p.dates[idx]).tz_localize("UTC"),
            )

        if is_long:
            open_value = amount * p.base_price * (1.0 + fee)
            close_value = amount * open_i * (1.0 - fee) + funding_fees
            pnl_amount = close_value - open_value
            leveraged_profit = ((close_value / open_value) - 1.0) * lev if open_value else 0.0
        else:
            open_value = amount * p.base_price * (1.0 - fee)
            close_value = amount * open_i * (1.0 + fee) - funding_fees
            pnl_amount = open_value - close_value
            leveraged_profit = (1.0 - (close_value / open_value)) * lev if open_value else 0.0

        running_balance += pnl_amount
        tied_up_capital -= stake
        trades.append({
            "pair": pair, "is_short": p.is_short, "entry_row": p.entry_row, "exit_row": idx,
            "entry_price": p.entry_price, "base_exit_price": open_i, "exit_price": exit_price,
            "profit_ratio": leveraged_profit, "profit_abs": pnl_amount,
            "leverage": p.trade_leverage, "exit_reason": "ForceExit", "exit_reason_tag": "force_exit", "stake": stake,
            "funding_fees": funding_fees,
        })

        if p.local_trade is not None:
            trade = p.local_trade
            trade.close_date = pd.Timestamp(p.dates[idx]).tz_localize("UTC")
            trade.close_profit = leveraged_profit
            trade.close_profit_abs = pnl_amount
            trade.is_open = False
            try:
                LocalTrade.close_bt_trade(trade)
            except Exception:
                logger.debug("LocalTrade.close_bt_trade failed for %s (force-exit)", pair, exc_info=True)
            p.local_trade = None

    # Stash the per-pair date arrays so `run_rust_backtest` can turn row
    # indices back into timestamps when formatting the trades DataFrame.
    for t in trades:
        t["_dates"] = pair_states[t["pair"]].dates

    if _dp_wired:
        # Restore the strategy's real wallets object. (An exception earlier in
        # this function already aborts the whole backtest via run_rust_
        # backtest's own try/except, so this only needs to cover the normal
        # return path — a hard failure mid-run is already visible/logged, not
        # a silently-wrong number.)
        strategy.wallets = _orig_wallets

    return trades, rejected


def run_rust_backtest(processed: dict[str, pd.DataFrame], strategy: Any,
                      config: dict, exchange: Any = None,
                      full_signals: dict[str, pd.DataFrame] | None = None,
                      required_startup: int = 0,
                      dataprovider: Any = None) -> dict:
    """Run all pairs through the joint multi-pair simulator; return a
    backtest() result dict in the same shape as the Python engine's.

    `exchange` (the already-loaded `Backtesting.exchange`) is used to resolve
    each pair's real max leverage — see `_pair_leverage`. Pass None only for
    callers without an exchange handy; leverage then falls back to a single
    config-wide constant, which will misprice any pair whose real per-pair
    exchange limit differs from that constant.

    Pairlist resolution (StaticPairList/VolumePairList/ShuffleFilter/
    VolatilityFilter/PairInformationFilter/AgeFilter/PriceFilter + whitelist/
    blacklist) is the Rust engine's OWN, independent of whatever whitelist
    Python's `PairListManager` already resolved upstream — see
    `VulcanTrader.rust_pairlist`. It can only SELECT among pairs `processed`
    already has data for, not load new ones.
    """
    from VulcanTrader.rust_pairlist import resolve_rust_pairlist

    def _last_close(pair: str) -> float | None:
        df = processed.get(pair)
        if df is None or df.empty or "close" not in df:
            return None
        val = df["close"].iloc[-1]
        return float(val) if pd.notna(val) else None

    resolved, pairlist_warnings = resolve_rust_pairlist(
        config, exchange, available_pairs=list(processed.keys()), last_price_fn=_last_close,
    )
    for w in pairlist_warnings:
        logger.warning("[rust pairlist] %s", w)
    processed = {p: processed[p] for p in resolved if p in processed}

    fee = float(config.get("fee", 0.0005) or 0.0005)
    tf_min = _parse_timeframe_minutes(
        getattr(strategy, "timeframe", None) or config.get("timeframe", "15m"))
    stoploss_ratio = float(getattr(strategy, "stoploss", -0.10) or -0.10)

    try:
        raw_trades, rejected = _joint_multi_pair_backtest(
            processed, strategy, config, exchange,
            full_signals=full_signals, required_startup=required_startup,
            dataprovider=dataprovider,
        )
    except Exception:
        logger.exception("Rust-engine joint backtest failed")
        raw_trades, rejected = [], 0

    rows: list[dict] = []
    for t in raw_trades:
        dates = t["_dates"]
        ei, xi = t["entry_row"], t["exit_row"]
        is_short = t["is_short"]
        # Undo the engine's fee-baked fill price (entry = open*(1+fee), exit =
        # open*(1-fee) for longs) — freqtrade records the RAW fill price and
        # carries fees in fee_open/fee_close separately.
        if is_short:
            open_rate = t["entry_price"] / (1.0 - fee) if fee else t["entry_price"]
            close_rate = t["exit_price"] / (1.0 + fee) if fee else t["exit_price"]
        else:
            open_rate = t["entry_price"] / (1.0 + fee) if fee else t["entry_price"]
            close_rate = t["exit_price"] / (1.0 - fee) if fee else t["exit_price"]

        profit_ratio = t["profit_ratio"]
        profit_abs = t["profit_abs"]
        leverage = t["leverage"] or 1.0
        stake = t["stake"]
        amount = (stake * leverage / open_rate) if open_rate > 0 else 0.0
        reason = t.get("exit_reason_tag") or _EXIT_REASON_MAP.get(t["exit_reason"], "exit_signal")
        # `dates` is a naive-UTC numpy datetime64 array (tz stripped for fast
        # array ops in the joint sim) — restore UTC tz-awareness to match the
        # Python engine's own trade timestamps.
        open_date = pd.Timestamp(dates[ei]).tz_localize("UTC")
        close_date = pd.Timestamp(dates[xi]).tz_localize("UTC")

        rows.append({
            "pair": t["pair"],
            "stake_amount": stake,
            "max_stake_amount": stake,
            "amount": amount,
            "open_date": open_date,
            "close_date": close_date,
            "open_rate": open_rate,
            "close_rate": close_rate,
            "fee_open": fee,
            "fee_close": fee,
            "trade_duration": int(xi - ei) * tf_min,
            "profit_ratio": profit_ratio,
            "profit_abs": profit_abs,
            "exit_reason": reason,
            "initial_stop_loss_abs": 0.0,
            "initial_stop_loss_ratio": stoploss_ratio,
            "stop_loss_abs": 0.0,
            "stop_loss_ratio": stoploss_ratio,
            "min_rate": min(open_rate, close_rate),
            "max_rate": max(open_rate, close_rate),
            "is_open": False,
            "enter_tag": None,
            "leverage": leverage,
            "is_short": is_short,
            "open_timestamp": int(pd.Timestamp(open_date).timestamp() * 1000),
            "close_timestamp": int(pd.Timestamp(close_date).timestamp() * 1000),
            "orders": [],
            "funding_fees": t.get("funding_fees", 0.0),
        })

    results = pd.DataFrame(rows, columns=BT_DATA_COLUMNS)
    if not results.empty:
        results = results.sort_values("open_date").reset_index(drop=True)

    start_bal = float(config.get("dry_run_wallet", 10000.0) or 10000.0)
    final_bal = start_bal + (results["profit_abs"].sum() if not results.empty else 0.0)
    return {
        "results": results,
        "config": strategy.config,
        "locks": [],
        "rejected_signals": rejected,
        "timedout_entry_orders": 0,
        "timedout_exit_orders": 0,
        "canceled_trade_entries": 0,
        "canceled_entry_orders": 0,
        "replaced_entry_orders": 0,
        "final_balance": final_bal,
    }
