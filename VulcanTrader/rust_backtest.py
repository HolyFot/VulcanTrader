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

Fidelity notes:
  - One position per pair (long OR short, never both), with same-candle
    reversal support (closing one direction and opening the other within the
    same candle), matching freqtrade's own two-pass ``backtest_loop`` call.
  - The portfolio-wide ``max_open_trades`` cap is enforced AS a live
    constraint during simulation (checked at the moment each entry is
    attempted, in the same pair order freqtrade uses), not as a post-hoc
    filter — so a rejected entry correctly leaves the pair flat to retry on
    the next candle, exactly like the real engine.
  - ``leverage()`` / ``custom_stake_amount()`` / ``custom_exit()`` /
    ``custom_stoploss()`` callbacks are NOT invoked — sizing is fixed (or
    simply compounding) and stops come from the static config (base
    stoploss, ROI, trailing, ATR-stop).
  - No protections, no position adjustment/DCA, no informative pairs.
  - RSI/CCI/MACD-based auto-exits and ATR stoploss are supported but only
    exercised if the strategy config actually enables them.
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


def _pair_leverage(strategy: Any, exchange: Any, pair: str, stake: float,
                    fallback_max: float) -> float:
    """Mirror freqtrade's real per-pair leverage resolution
    (`get_valid_entry_price_and_stake`): each pair has its OWN exchange max
    leverage (e.g. on Hyperliquid, BTC=40x, SPX=5x, VVV=3x — not a global
    constant), and `strategy.leverage()` is called with THAT pair's own max.
    Sampling one leverage value for the whole run (as an earlier version did)
    silently used the wrong leverage for every pair whose real max is below
    the strategy's requested cap, corrupting every leverage-scaled
    calculation (trailing distance, ROI-adjusted %, stop ratchet) for that
    pair specifically.
    """
    max_lev = fallback_max
    if exchange is not None:
        try:
            max_lev = float(exchange.get_max_leverage(pair, stake))
        except Exception:
            logger.debug("exchange.get_max_leverage(%s) failed; using fallback", pair, exc_info=True)

    leverage = 1.0
    try:
        lev = strategy.leverage(
            pair=pair, current_time=None, current_rate=0.0,
            proposed_leverage=1.0, max_leverage=max_lev, entry_tag=None, side="long",
        )
        if lev and float(lev) > 0:
            leverage = float(lev)
    except Exception:
        logger.debug("strategy.leverage(%s) not usable; defaulting to 1.0", pair, exc_info=True)

    return min(max(leverage, 1.0), max_lev)


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
        "trailing_activated", "pair_leverage", "precision", "trade_stake",
        "trade_amount",
    )

    def __init__(self, dates, openp, high, low, atr, el, xl, es, xs, pair_leverage, precision):
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
        # global constant); see `_pair_leverage`.
        self.pair_leverage = pair_leverage
        self.trade_leverage = pair_leverage
        self.stop_loss_price = 0.0
        self.stop_is_trailing = False
        self.trailing_activated = False
        self.trade_stake = 0.0
        self.trade_amount = 0.0


def _joint_multi_pair_backtest(
    processed: dict[str, pd.DataFrame], strategy: Any, config: dict, exchange: Any = None,
) -> tuple[list[dict], int]:
    """Bar-by-bar joint simulation across all pairs, sharing one global
    `max_open_trades` slot count and freqtrade's exact per-candle pair order.

    Returns (trades, rejected_count). Each trade dict carries raw fields (row
    indices, base/fee-adjusted prices) that `run_rust_backtest` formats into
    the standard trades DataFrame afterward.
    """
    eng = _build_engine_config(strategy, config)
    can_short = bool(getattr(strategy, "can_short", False)) and \
        str(config.get("trading_mode", "futures")) != "spot"
    max_open_trades = int(config.get("max_open_trades", 0) or 0)
    fallback_max_leverage = float(config.get("rust_leverage_max", 10.0))

    fee = eng["fee_taker"]
    base_stoploss = eng["base_stoploss"]
    trailing_enabled = eng["trailing_enabled"]
    trailing_trigger = eng["trailing_trigger"]
    trailing_offset = eng["trailing_offset"]
    roi_enabled = eng["roi_enabled"]
    roi_vals = eng["roi_vals"]
    roi_periods = eng["roi_periods"]
    max_hold_period = eng["max_hold_period"]
    atr_stop_enabled = eng["atr_stop_enabled"]
    atr_stop_multiplier = eng["atr_stop_multiplier"]
    compounding = eng["compounding_enabled"]
    starting_balance = eng["starting_balance"]
    tradable_ratio = eng["tradable_balance_ratio"]
    max_trade_amount = eng["max_trade_amount"]

    import vulcan_rust_indicators as vri

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

        pair_lev = _pair_leverage(strategy, exchange, pair, max_trade_amount, fallback_max_leverage)
        precision = _precision_per_bar(df, dates, exchange, pair)
        pair_states[pair] = _PairState(dates, openp, high, low, atr, el, xl, es, xs, pair_lev, precision)

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

    def entry_dir(p: _PairState, i: int) -> str | None:
        enter_long, exit_long = p.el[i] == 1, p.xl[i] == 1
        enter_short = can_short and p.es[i] == 1
        exit_short = can_short and p.xs[i] == 1
        if enter_long and not (exit_long or enter_short):
            return "long"
        if enter_short and not (exit_short or enter_long):
            return "short"
        return None

    def do_enter(pair: str, p: _PairState, i: int, direction: str, stake: float) -> None:
        nonlocal open_count, tied_up_capital
        is_short = direction == "short"
        base_price = p.open[i]
        lev = max(p.pair_leverage, 0.1)

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
            if atr_stop_enabled and p.atr is not None:
                atr_i = p.atr[i]
                stop_price_raw = (base_price - atr_i * atr_stop_multiplier) if is_long \
                    else (base_price + atr_i * atr_stop_multiplier)
                candidate = (bound - (bound - stop_price_raw) / lev) if is_long \
                    else (bound + (stop_price_raw - bound) / lev)
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

        should_exit, exit_reason = False, None
        if not should_exit and signal_exit_triggered: should_exit, exit_reason = True, "Signal"
        if not should_exit and stop_triggered and not stop_is_trailing: should_exit, exit_reason = True, "Stoploss"
        if not should_exit and roi_triggered: should_exit, exit_reason = True, "RoiTarget"
        if not should_exit and stop_triggered and stop_is_trailing: should_exit, exit_reason = True, "TrailingStop"
        if not should_exit and max_hold_triggered: should_exit, exit_reason = True, "MaxHoldPeriod"

        # freqtrade parity: a trailing stop that arms-and-triggers within the
        # ENTRY candle is placed as an ORDER at the pessimistic price; if that
        # price is outside this candle's range the order can't fill, so
        # freqtrade DEFERS the exit to a later bar.
        if should_exit and exit_reason == "TrailingStop" and time_in_position == 0:
            trail_dist = trailing_offset / lev
            pess = open_i * (1.0 + abs(trailing_trigger) - abs(trail_dist)) if is_long \
                else open_i * (1.0 - abs(trailing_trigger) + abs(trail_dist))
            fills = (pess <= high_i) if is_long else (pess >= low_i)
            if not fills:
                should_exit = False

        p.stop_loss_price, p.stop_is_trailing, p.trailing_activated = \
            stop_loss_price, stop_is_trailing, trailing_activated

        if not should_exit:
            return False

        if exit_reason == "TrailingStop" and time_in_position == 0:
            trail_dist = trailing_offset / lev
            if is_long:
                base_exit_price = max(open_i * (1.0 + abs(trailing_trigger) - abs(trail_dist)), low_i)
            else:
                base_exit_price = min(open_i * (1.0 - abs(trailing_trigger) + abs(trail_dist)), high_i)
        elif exit_reason in ("TrailingStop", "Stoploss"):
            # freqtrade's `_get_close_rate_for_stoploss`: if the ratcheted stop
            # sits entirely outside this candle's range (never actually
            # touchable this candle), exit at the candle's open instead.
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
            # Mirror freqtrade's `_get_close_rate_for_roi` fee-consistent solve
            # rather than a naive `base_price * (1 ± roi_pct)`.
            side_1 = 1.0 if is_long else -1.0
            roi_rate = base_price * roi_pct
            open_fee_rate = side_1 * base_price * (1.0 + side_1 * fee)
            raw_close = -(roi_rate + open_fee_rate) / (fee - side_1)
            # freqtrade's `_get_close_rate_for_roi`: if this is exactly the bar
            # a new (tighter) ROI tier takes effect, AND the candle's own open
            # already exceeds that tier's computed close_rate, use the open
            # price instead — the trade was already past the new target
            # before this bar's own close_rate could apply. Missing this made
            # the engine use the solved close_rate even when the real open
            # price gapped straight through it, understating profit by the
            # full gap size on the (rare, but not tiny) trades that hit it.
            if time_in_position == roi_entry_period:
                is_new_roi = (open_i > raw_close) if is_long else (open_i < raw_close)
                if is_new_roi:
                    raw_close = open_i
            lo, hi = min(low_i, high_i), max(low_i, high_i)
            base_exit_price = min(max(raw_close, lo), hi)
        else:  # MaxHoldPeriod / Signal -> candle's own open
            base_exit_price = open_i

        exit_price = base_exit_price * (1.0 - fee) if is_long else base_exit_price * (1.0 + fee)

        # Use the stake fixed AT ENTRY, not one recomputed now — matters once
        # `compounding_enabled` makes `current_stake()` time-varying.
        stake = p.trade_stake

        # freqtrade's REAL profit_abs/profit_ratio come from `Trade.
        # calc_close_trade_value` / `_calc_open_trade_value`: amount*price
        # trade VALUES, not stake*profit_ratio. profit_ratio's `amount` term
        # cancels out of that ratio (this formula IS still exactly
        # `raw_profit*leverage` when funding_fees==0 — verified algebraically
        # and empirically), which is why the simpler stake-based version
        # already matched profit_ratio exactly. profit_abs is a DIFFERENCE
        # though, so `amount` does NOT cancel there, and for FUTURES trades
        # `close_trade_value` also has accrued funding fees added in
        # (subtracted for shorts) — both need the real amount-based formula.
        # `funding_so_far` was already computed above (entry_row -> i, same
        # range) for the bound_profit check — this bar's exit reuses it as-is.
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
            "leverage": lev, "exit_reason": exit_reason, "stake": stake,
            "funding_fees": funding_fees,
        })

        p.in_position = False
        open_count -= 1
        if pair in open_order:
            open_order.remove(pair)
        return True

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
            "leverage": p.trade_leverage, "exit_reason": "ForceExit", "stake": stake,
            "funding_fees": funding_fees,
        })

    # Stash the per-pair date arrays so `run_rust_backtest` can turn row
    # indices back into timestamps when formatting the trades DataFrame.
    for t in trades:
        t["_dates"] = pair_states[t["pair"]].dates

    return trades, rejected


def run_rust_backtest(processed: dict[str, pd.DataFrame], strategy: Any,
                      config: dict, exchange: Any = None) -> dict:
    """Run all pairs through the joint multi-pair simulator; return a
    backtest() result dict in the same shape as the Python engine's.

    `exchange` (the already-loaded `Backtesting.exchange`) is used to resolve
    each pair's real max leverage — see `_pair_leverage`. Pass None only for
    callers without an exchange handy; leverage then falls back to a single
    config-wide constant, which will misprice any pair whose real per-pair
    exchange limit differs from that constant.
    """
    fee = float(config.get("fee", 0.0005) or 0.0005)
    tf_min = _parse_timeframe_minutes(
        getattr(strategy, "timeframe", None) or config.get("timeframe", "15m"))
    stoploss_ratio = float(getattr(strategy, "stoploss", -0.10) or -0.10)

    try:
        raw_trades, rejected = _joint_multi_pair_backtest(processed, strategy, config, exchange)
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
        reason = _EXIT_REASON_MAP.get(t["exit_reason"], "exit_signal")
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
