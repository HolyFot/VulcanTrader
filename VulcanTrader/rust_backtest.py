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
``confirm_trade_exit``, ``order_filled``, ``adjust_order_price``,
``check_entry_timeout`` / ``check_exit_timeout``.

LIMIT ORDER SIMULATION
----------------------
Entries and signal-driven exits are modelled as real orders with a placement
bar, a price, and a fill test — freqtrade's own ``_get_order_filled``
(``low <= price <= high``). By default the price IS the candle's open, which
always fills instantly, so the engine behaves exactly as it always has.

A strategy drives its own orders through the standard IStrategy callbacks:

* **place** — ``custom_entry_price`` / ``custom_exit_price`` name the limit
  price. A strategy that prices its own order owns that price outright; the
  config's ``limit_order_simulation`` offset (below) is only the default for
  strategies that don't.
* **track** — while an order rests unfilled, ``adjust_order_price`` is called
  every bar with the real, still-open ``Order`` object attached to a real
  ``LocalTrade``: return the same rate to leave it alone, a different rate to
  cancel-and-replace it (which restarts its timeout clock), or ``None`` to
  cancel it outright.
* **cancel** — ``trader_check_timed_out`` runs first each bar, applying both
  the config ``unfilledtimeout`` and the strategy's own
  ``check_entry_timeout`` / ``check_exit_timeout``.

Without a strategy pricing them, ``limit_order_simulation.entry_offset`` /
``exit_offset`` (see ``_build_engine_config``) rest every order that far on
the favourable side of the open. Either way a resting order fills only on a
candle that trades through it and pays ``fee_maker`` when it does. A cancelled
entry order means the trade never happened (its slot and capital are released);
a cancelled exit order leaves the position open for a later exit condition.
With no timeout configured at all an order rests until it fills or the
strategy cancels it — freqtrade's own behaviour.

Stoploss, trailing-stop, ROI and max-hold exits are deliberately NOT modelled
as orders — they are protective exits that already resolve to their own fill
price, and making them "maybe fill" would leave positions open through exactly
the moves a stop exists to escape.
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

    # --- Limit-order simulation --------------------------------------------
    # freqtrade supplies the order TYPE (`order_types`) and the unfilled
    # TIMEOUT (`unfilledtimeout`), but not the resting OFFSET: live, a limit
    # price comes off the order book via entry_pricing/exit_pricing, which a
    # candle-level backtest has no access to. `limit_order_simulation` in the
    # bot config supplies it (strategy attributes win, like every other
    # IStrategy setting), and it is what turns this from a no-op into a real
    # model — at offset 0 the limit price IS the bar's open, so every order
    # fills instantly exactly as before.
    lim = config.get("limit_order_simulation", {}) or {}

    def _offset(cfg_key: str, attr: str) -> float:
        raw = getattr(strategy, attr, None)
        if raw is None:
            raw = lim.get(cfg_key, 0.0)
        try:
            return max(float(getattr(raw, "value", raw)), 0.0)
        except (TypeError, ValueError):
            return 0.0

    unfilled = getattr(strategy, "unfilledtimeout", None) or config.get("unfilledtimeout", {}) or {}
    _unit_div = 60.0 if str(unfilled.get("unit", "minutes")).lower() == "seconds" else 1.0

    def _timeout_bars(side: str) -> int | None:
        """Bars an unfilled order survives past its placement bar, or None for
        no timeout at all. An explicit bar count wins; otherwise freqtrade's
        own minute-based `unfilledtimeout` is converted with this timeframe.
        Sub-bar timeouts floor to 0 — fill-or-kill on the placement bar, the
        honest answer at candle resolution.

        None (nothing configured) matches freqtrade: with no `unfilledtimeout`
        its `trader_check_timed_out` finds no timeout and `check_entry_timeout`
        defaults to False, so the order rests until it fills or the strategy
        cancels it."""
        if f"{side}_timeout_bars" in lim:
            try:
                return max(int(lim[f"{side}_timeout_bars"]), 0)
            except (TypeError, ValueError):
                return None
        raw = unfilled.get(side)
        if raw is None:
            return None
        try:
            return max(int((float(raw) / _unit_div) // tf_min), 0)
        except (TypeError, ValueError):
            return None

    # A resting order that gets touched is a maker fill. With no explicit maker
    # rate the single `fee` is used for both, which keeps results unchanged
    # from a pure-taker run.
    fee_maker = lim.get("fee_maker", config.get("fee_maker"))
    try:
        fee_maker = float(fee_maker) if fee_maker is not None else fee
    except (TypeError, ValueError):
        fee_maker = fee

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
        "fee_maker": fee_maker,
        "entry_limit_offset": _offset("entry_offset", "entry_limit_offset"),
        "exit_limit_offset": _offset("exit_offset", "exit_limit_offset"),
        "entry_fill_timeout_bars": _timeout_bars("entry"),
        "exit_fill_timeout_bars": _timeout_bars("exit"),
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
        "trade_amount", "local_trade", "entry_fee",
        # Resting-order state (all inert unless a limit offset is configured).
        # An entry order holds its already-resolved stake/leverage/amount: those
        # are fixed when the order is PLACED (that's when freqtrade calls
        # leverage()/custom_stake_amount()/confirm_trade_entry()), not when it
        # eventually fills.
        "pending_entry", "pending_entry_price", "pending_entry_bar",
        "pending_entry_short", "pending_entry_stake", "pending_entry_lev",
        "pending_entry_amount", "pending_entry_order",
        "pending_exit", "pending_exit_price", "pending_exit_bar",
        "pending_exit_reason", "pending_exit_tag", "pending_exit_order",
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
        self.entry_fee = 0.0  # this trade's own entry fee (maker if it rested)
        self.local_trade = None  # the registered LocalTrade while a position is open
        self.pending_entry = False
        self.pending_entry_price = 0.0
        self.pending_entry_bar = -1
        self.pending_entry_short = False
        self.pending_entry_stake = 0.0
        self.pending_entry_lev = 1.0
        self.pending_entry_amount = 0.0
        # The real `Order` object handed to adjust_order_price() /
        # check_*_timeout() while the order sits on the book.
        self.pending_entry_order = None
        self.pending_exit = False
        self.pending_exit_price = 0.0
        self.pending_exit_bar = -1
        self.pending_exit_reason = None
        self.pending_exit_tag = None
        self.pending_exit_order = None


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

    Returns (trades, rejected_count, order_stats), where `order_stats` carries
    the limit-order counters freqtrade reports as `timedout_entry_orders` /
    `timedout_exit_orders` (both 0 unless resting limit orders are configured
    — see `limit_order_simulation` in `_build_engine_config`). Each trade dict
    carries raw fields (row indices, base/fee-adjusted prices) that
    `run_rust_backtest` formats into the standard trades DataFrame afterward.
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
    # --- Limit-order simulation -------------------------------------------
    # An order RESTS only when its side is a limit order with a non-zero
    # offset; otherwise the limit price is the bar's own open, which is
    # trivially inside [low, high], so it fills on its placement bar at the
    # same price and fee the engine has always used. Everything below is
    # gated on these two flags so the default path is untouched.
    entry_offset = float(eng.get("entry_limit_offset", 0.0) or 0.0)
    exit_offset = float(eng.get("exit_limit_offset", 0.0) or 0.0)
    entry_rests = entry_order_type == "limit" and entry_offset > 0.0
    exit_rests = exit_order_type == "limit" and exit_offset > 0.0
    # None = no engine-level timeout; the order rests until it fills, or until
    # the strategy cancels it via check_*_timeout / adjust_order_price.
    entry_timeout_bars = eng.get("entry_fill_timeout_bars")
    exit_timeout_bars = eng.get("exit_fill_timeout_bars")
    fee_maker = float(eng.get("fee_maker", fee) or fee)
    timedout_entry_orders = 0
    timedout_exit_orders = 0
    canceled_entry_orders = 0
    canceled_exit_orders = 0
    replaced_entry_orders = 0
    replaced_exit_orders = 0
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

    # `do_exit_check` re-accrues funding every bar a trade is open (matching
    # freqtrade's `_run_funding_fees`, which re-derives it from open_date to the
    # CURRENT bar every time) - calling `exchange.calculate_funding_fees` as-is
    # for that would re-sum a slice that grows every bar, i.e. O(hold_time) work
    # PER BAR -> O(hold_time^2) per trade. Precompute a prefix sum of
    # open_fund*open_mark per pair once here so each call becomes an O(log n)
    # searchsorted + O(1) array diff instead - mathematically exact (same
    # searchsorted "left"/"right" row range as `calculate_funding_fees`, just
    # summed via cumsum lookup rather than re-summing the slice), not an
    # incremental/cached approximation, so it's correct for any (open_date,
    # close_date) pair regardless of call order.
    _funding_cumsum: dict[str, tuple[pd.Series, np.ndarray]] = {}
    for _pair, _df in funding_mark.items():
        if _df.empty:
            continue
        _vals = (_df["open_fund"].to_numpy(dtype=np.float64)
                 * _df["open_mark"].to_numpy(dtype=np.float64))
        _funding_cumsum[_pair] = (_df["date"], np.concatenate(([0.0], np.cumsum(_vals))))

    def funding_fee_for(pair: str, amount: float, is_short: bool,
                        open_date: pd.Timestamp, close_date: pd.Timestamp) -> float:
        cached = _funding_cumsum.get(pair)
        if cached is None:
            return 0.0
        try:
            dates, cumsum = cached
            unit = dates.dtype.unit
            lo = pd.Timestamp(open_date).ceil(unit).as_unit(unit)
            hi = pd.Timestamp(close_date).floor(unit).as_unit(unit)
            lo_idx = dates.searchsorted(lo, "left")
            hi_idx = dates.searchsorted(hi, "right")
            fees = float(cumsum[hi_idx] - cumsum[lo_idx]) * amount
            if math.isnan(fees):
                fees = 0.0
            return fees if is_short else -fees
        except Exception:
            logger.debug("funding_fee_for(%s) failed", pair, exc_info=True)
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

        def get_starting_balance(self) -> float:
            """Mirrors the real `Wallets.get_starting_balance` — algebraically
            `(available - realized_profit + open_stakes) * ratio` reduces to
            the run's fixed initial capital scaled by ratio, i.e. this loop's
            `starting_balance` (not the fluctuating `running_balance`)."""
            return starting_balance * tradable_ratio

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

    def _make_order(pair: str, trade, side: str, order_type: str, amount: float,
                    price: float, current_time, is_open: bool, tag: str | None = None):
        """Build a real `Order`. An OPEN one is what the strategy gets handed
        by `adjust_order_price()` / `check_entry_timeout()` /
        `check_exit_timeout()` while it rests on the book; a closed one is the
        record `order_filled()` receives."""
        nonlocal _order_id_counter
        _order_id_counter += 1
        return Order(
            id=_order_id_counter, order_id=str(_order_id_counter),
            ft_trade_id=getattr(trade, "id", 0) or 0,
            ft_order_side=side, ft_pair=pair, ft_amount=amount, ft_price=price,
            ft_is_open=is_open, status="open" if is_open else "closed",
            symbol=pair, order_type=order_type, side=side, price=price, average=price,
            amount=amount, filled=0.0 if is_open else amount,
            remaining=amount if is_open else 0.0, cost=amount * price,
            order_date=current_time, order_update_date=current_time,
            order_filled_date=None if is_open else current_time,
            ft_order_tag=tag,
        )

    def _notify_order_filled(pair: str, trade, side: str, order_type: str, amount: float,
                             price: float, current_time, order=None) -> None:
        """order_filled(): called right after an entry/exit order fills, with
        the now-closed order — `_try_close_open_order`'s own call site. For a
        resting order this is the SAME object the strategy has been adjusting
        on previous bars, marked filled, rather than a fresh stand-in."""
        if not _dp_wired or trade is None:
            return
        try:
            if order is None:
                order = _make_order(pair, trade, side, order_type, amount, price,
                                    current_time, is_open=False)
            strategy.order_filled(pair=pair, trade=trade, order=order, current_time=current_time)
        except Exception:
            logger.debug("strategy.order_filled(%s) raised", pair, exc_info=True)

    def _close_order_object(order, price: float, current_time) -> None:
        """Mark a resting order filled, mirroring `Order.close_bt_order`'s
        field updates without its `recalc_trade_from_orders` side effects —
        this engine does its own trade accounting (see `close_position`) and
        must stay the single source of truth for it."""
        if order is None:
            return
        order.ft_is_open = False
        order.status = "closed"
        order.price = price
        order.ft_price = price
        order.average = price
        order.filled = order.amount
        order.remaining = 0.0
        order.order_filled_date = current_time

    def _detach_order(trade, order) -> None:
        if trade is None or order is None:
            return
        try:
            if order in trade.orders:
                trade.orders.remove(order)
        except Exception:
            logger.debug("detaching order failed", exc_info=True)

    def manage_open_order(pair: str, p: _PairState, i: int, is_entry: bool) -> tuple[str, float]:
        """freqtrade's `manage_open_orders` for one resting order: cancel it if
        it timed out (`check_order_cancel` -> `trader_check_timed_out`, which
        applies BOTH the config `unfilledtimeout` and the strategy's own
        `check_entry_timeout` / `check_exit_timeout`), otherwise let the
        strategy reprice it (`check_order_replace` -> `adjust_order_price`:
        the same rate keeps the order, a different rate cancels-and-replaces
        it, None cancels it outright).

        Returns (action, price) with action in {"keep", "timeout", "cancel",
        "replace"} — "timeout" and "cancel" both drop the order, but freqtrade
        counts them separately (`timedout_*_orders` vs `canceled_*_orders`).
        The engine's own bar-count timeout stays in force alongside the
        callbacks — it is the only rule available when this run has no
        DataProvider wired (and hence no LocalTrade/Order to hand over).
        """
        order = p.pending_entry_order if is_entry else p.pending_exit_order
        trade = p.local_trade
        price = p.pending_entry_price if is_entry else p.pending_exit_price
        placed_bar = p.pending_entry_bar if is_entry else p.pending_exit_bar
        timeout_bars = entry_timeout_bars if is_entry else exit_timeout_bars
        current_time = pd.Timestamp(p.dates[i]).tz_localize("UTC")

        if _dp_wired and trade is not None and order is not None:
            try:
                if strategy.trader_check_timed_out(trade, order, current_time):
                    return "timeout", price
            except Exception:
                logger.debug("trader_check_timed_out(%s) raised", pair, exc_info=True)

        if timeout_bars is not None and i - placed_bar > timeout_bars:
            return "timeout", price

        if _dp_wired and trade is not None and order is not None:
            side = "short" if (p.pending_entry_short if is_entry else p.is_short) else "long"
            try:
                requested = strategy.adjust_order_price(
                    trade=trade, order=order, pair=pair, current_time=current_time,
                    proposed_rate=p.open[i], current_order_rate=price,
                    entry_tag=None, side=side, is_entry=is_entry,
                )
            except Exception:
                logger.debug("strategy.adjust_order_price(%s) raised", pair, exc_info=True)
                return "keep", price
            if requested is None:
                return "cancel", price
            try:
                requested = float(requested)
            except (TypeError, ValueError):
                return "keep", price
            if requested != price:
                return "replace", requested

        return "keep", price

    def _reprice_order(p: _PairState, i: int, is_entry: bool, new_price: float) -> None:
        """Cancel-and-replace at `new_price`. freqtrade places a brand-new
        order, so its timeout clock restarts from this bar — and the new order
        goes through the same "can't place it outside this candle" clamp as the
        original (`get_valid_entry_price_and_stake` for entries,
        `_get_exit_for_signal` for exits); an order beyond the candle's range
        would be a stop-limit, which VulcanTrader doesn't support live."""
        order = p.pending_entry_order if is_entry else p.pending_exit_order
        current_time = pd.Timestamp(p.dates[i]).tz_localize("UTC")
        is_short = p.pending_entry_short if is_entry else p.is_short
        if is_entry:
            new_price = max(new_price, p.low[i]) if is_short else min(new_price, p.high[i])
        else:
            new_price = min(new_price, p.high[i]) if is_short else max(new_price, p.low[i])
        if order is not None:
            order.price = new_price
            order.ft_price = new_price
            order.average = new_price
            order.order_date = current_time
            order.order_update_date = current_time
        if is_entry:
            p.pending_entry_price = new_price
            p.pending_entry_bar = i
            # Repricing can move the order across the market, so re-classify
            # the fill it would now get.
            p.entry_fee = fill_fee(new_price, p.open[i], is_buy=not p.pending_entry_short)
            # The amount was sized off the original price; keep the stake
            # constant (freqtrade re-derives the amount from the new rate).
            if new_price > 0:
                p.pending_entry_amount = p.pending_entry_stake * p.pending_entry_lev / new_price
                if order is not None:
                    order.amount = p.pending_entry_amount
                    order.ft_amount = p.pending_entry_amount
                    order.remaining = p.pending_entry_amount
        else:
            p.pending_exit_price = new_price
            p.pending_exit_bar = i

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
        """Place an entry order (and fill it, if this bar's range reaches it).

        Returns True if an order was placed at all — with instant fills (the
        default) that is the same thing as "a position was opened", but a
        resting limit order can be placed here and fill on a later bar via
        `process_pending_entry`.

        Mirrors `Backtesting._enter_trade` / `get_valid_entry_price_and_stake`'s
        exact order: resolve leverage -> resolve stake (custom_stake_amount,
        given that leverage) -> round amount to lot size -> back-calculate the
        real stake from the rounded amount -> confirm_trade_entry with the
        FINAL amount/rate -> only THEN commit any state or register a trade.
        All of that happens when the order is PLACED, exactly as in freqtrade,
        where `_enter_trade` runs in full before the order can fill.
        """
        nonlocal open_count, tied_up_capital, rejected
        is_short = direction == "short"
        current_time = pd.Timestamp(p.dates[i]).tz_localize("UTC")
        base_price = p.open[i]

        strategy_priced = False
        if _dp_wired:
            resolved = _resolve_entry_price(pair, current_time, base_price, direction)
            # A strategy that actually prices its own limit order OWNS that
            # price: the config's blanket offset is a default for strategies
            # that don't, never something layered on top of an explicit
            # custom_entry_price.
            strategy_priced = resolved != base_price
            base_price = resolved
            # "We can't place orders higher than current high" / lower than
            # current low (get_valid_entry_price_and_stake).
            base_price = max(base_price, p.low[i]) if is_short else min(base_price, p.high[i])
            stake, lev = _resolve_stake_and_leverage(pair, p, i, direction, proposed_stake)
        else:
            stake, lev = proposed_stake, p.exchange_max_leverage
        lev = max(lev, 0.1)

        # A resting entry sits `entry_offset` on the favourable side of the
        # price above: below it for a long, above it for a short.
        if entry_rests and not strategy_priced:
            base_price = base_price * ((1.0 + entry_offset) if is_short else (1.0 - entry_offset))

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

        # The order now exists. It reserves its slot and its capital from this
        # moment on — in freqtrade the LocalTrade object is created here, at
        # placement, and only removed again if the order times out unfilled.
        open_count += 1
        tied_up_capital += actual_stake
        open_order.append(pair)

        p.pending_entry = True
        p.pending_entry_price = base_price
        p.pending_entry_bar = i
        p.pending_entry_short = is_short
        p.pending_entry_stake = actual_stake
        p.pending_entry_lev = lev
        p.pending_entry_amount = amount
        # Decided by where the order was PLACED relative to the market, so it
        # is already correct for the LocalTrade registered below and for every
        # profit calculation once the order fills.
        p.entry_fee = fill_fee(base_price, p.open[i], is_buy=not is_short)

        # Register the trade and its OPEN entry order now, at placement — the
        # same point freqtrade's `_enter_trade` does, and what makes the order
        # visible to `adjust_order_price()` / `check_entry_timeout()` on every
        # bar it stays unfilled. `amount=0` until it fills, so the trade reads
        # as "ordered, not yet in position" to any callback that inspects it.
        if _dp_wired:
            try:
                trade = LocalTrade(
                    pair=pair, is_short=is_short, amount=0.0, amount_requested=amount,
                    open_rate=base_price, open_rate_requested=base_price,
                    fee_open=p.entry_fee, fee_close=fee, leverage=lev,
                    trading_mode=_TradingModeEnum.FUTURES if is_futures else _TradingModeEnum.SPOT,
                    open_date=current_time, is_open=True,
                    stake_amount=actual_stake, max_stake_amount=actual_stake,
                    orders=[],
                )
                LocalTrade.add_bt_trade(trade)
                p.local_trade = trade
                order = _make_order(pair, trade, trade.entry_side, entry_order_type,
                                    amount, base_price, current_time, is_open=True)
                trade.orders.append(order)
                p.pending_entry_order = order
            except Exception:
                logger.debug("LocalTrade registration failed for %s", pair, exc_info=True)
                p.local_trade = None
                p.pending_entry_order = None

        # freqtrade fills the order it just created on this same candle when
        # the price is reachable (backtest_loop step 3 runs right after step 2).
        if order_filled_at(p, i, base_price):
            fill_entry(pair, p, i, base_price, is_short, actual_stake, lev, amount)
        return True

    def order_filled_at(p: _PairState, i: int, price: float) -> bool:
        """freqtrade's `_get_order_filled`: an order fills on any candle whose
        own range contains its price. A marketable price (the bar's open, i.e.
        every order when no offset is configured) always satisfies this."""
        return p.low[i] <= price <= p.high[i]

    def fill_fee(price: float, reference: float, is_buy: bool) -> float:
        """A limit order priced on the favourable side of the market rests on
        the book and ADDS liquidity when it is finally touched -> maker. One
        priced at (or through) the market takes liquidity -> taker. Comparing
        against the bar open covers config-driven offsets and strategy-priced
        orders (custom_entry_price / custom_exit_price / adjust_order_price)
        with a single rule."""
        rests = price < reference if is_buy else price > reference
        return fee_maker if rests else fee

    def cancel_entry_order(pair: str, p: _PairState, reason: str) -> None:
        """freqtrade's `check_order_cancel` for an entry with nothing filled:
        the whole trade is deleted, releasing its slot and its capital."""
        nonlocal open_count, tied_up_capital
        p.pending_entry = False
        _detach_order(p.local_trade, p.pending_entry_order)
        p.pending_entry_order = None
        if p.local_trade is not None:
            try:
                LocalTrade.remove_bt_trade(p.local_trade)
            except Exception:
                logger.debug("remove_bt_trade(%s) failed on %s", pair, reason, exc_info=True)
            p.local_trade = None
        open_count -= 1
        tied_up_capital -= p.pending_entry_stake
        if pair in open_order:
            open_order.remove(pair)

    def fill_entry(pair: str, p: _PairState, i: int, base_price: float, is_short: bool,
                   actual_stake: float, lev: float, amount: float) -> None:
        """Commit an entry order's fill: the position opens HERE, on the bar
        the order was actually touched, so its stoploss anchor, ROI clock and
        duration all start from this bar rather than from the placement bar."""
        current_time = pd.Timestamp(p.dates[i]).tz_localize("UTC")
        p.pending_entry = False
        p.in_position = True
        p.is_short = is_short
        p.base_price = base_price
        p.entry_price = base_price * ((1.0 + p.entry_fee) if not is_short else (1.0 - p.entry_fee))
        p.entry_row = i
        p.trade_leverage = lev
        p.trade_stake = actual_stake
        p.trade_amount = amount
        p.trailing_activated = False
        p.stop_is_trailing = False
        p.pending_exit = False
        raw_stop = base_price * (1.0 + (base_stoploss / lev if not is_short else -base_stoploss / lev))
        p.stop_loss_price = _round_stop(raw_stop, p.precision[i], is_short)

        # The trade and its order already exist (created at placement); the
        # fill just completes them.
        trade = p.local_trade
        if trade is not None:
            try:
                trade.amount = amount
                trade.open_rate = base_price
                trade.open_date = current_time
                trade.stake_amount = actual_stake
                trade.max_stake_amount = actual_stake
                trade.fee_open = p.entry_fee
            except Exception:
                logger.debug("LocalTrade fill update failed for %s", pair, exc_info=True)
            order = p.pending_entry_order
            _close_order_object(order, base_price, current_time)
            p.pending_entry_order = None
            _notify_order_filled(
                pair, trade, trade.entry_side, entry_order_type,
                amount, base_price, current_time, order=order,
            )

    def process_pending_entry(pair: str, p: _PairState, i: int) -> None:
        """freqtrade's `backtest_loop` step 1 (`manage_open_orders`) followed by
        step 3 (`_try_close_open_order`), for an entry order placed on an
        earlier bar: let the strategy cancel or reprice it, then fill it if
        this bar reaches its price."""
        nonlocal timedout_entry_orders, canceled_entry_orders, replaced_entry_orders
        if not p.pending_entry:
            return
        action, new_price = manage_open_order(pair, p, i, is_entry=True)
        if action in ("timeout", "cancel"):
            if action == "timeout":
                timedout_entry_orders += 1
            canceled_entry_orders += 1
            cancel_entry_order(pair, p, action)
            return
        if action == "replace":
            _reprice_order(p, i, True, new_price)
            replaced_entry_orders += 1
        if order_filled_at(p, i, p.pending_entry_price):
            fill_entry(pair, p, i, p.pending_entry_price, p.pending_entry_short,
                       p.pending_entry_stake, p.pending_entry_lev, p.pending_entry_amount)

    def do_exit_check(pair: str, p: _PairState, i: int) -> bool:
        """Full priority-ordered exit check + fill-price computation for the
        given already-open pair at bar i. Returns True if it closed."""
        nonlocal open_count, running_balance, tied_up_capital, timedout_exit_orders
        nonlocal canceled_exit_orders, replaced_exit_orders
        is_long = not p.is_short
        time_in_position = i - p.entry_row
        high_i, low_i, open_i = p.high[i], p.low[i], p.open[i]
        base_price, entry_price, lev = p.base_price, p.entry_price, p.trade_leverage
        amount = p.trade_amount
        bound = high_i if is_long else low_i
        # This trade's OWN entry fee (maker if the entry rested). The close
        # leg of every trigger check below stays on the taker rate: trailing
        # activation and ROI are protective/market exits, which pay taker.
        fee_open = p.entry_fee

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
            open_value = amount * base_price * (1.0 + fee_open)
            close_value_bound = amount * bound * (1.0 - fee) + funding_so_far
            leveraged_bound_return = ((close_value_bound / open_value) - 1.0) * lev if open_value else 0.0
        else:
            open_value = amount * base_price * (1.0 - fee_open)
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
                ov_ce = amount * base_price * (1.0 + fee_open)
                cv_ce = amount * open_i * (1.0 - fee) + funding_so_far
                leveraged_open_return = ((cv_ce / ov_ce) - 1.0) * lev if ov_ce else 0.0
            else:
                ov_ce = amount * base_price * (1.0 - fee_open)
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
        # Each candidate is (reason, custom_tag, resting_price). A non-None
        # resting_price means "an exit order placed on an EARLIER bar, which
        # this bar's range has now reached" — it fills at its own price, not
        # at a price recomputed from this bar.
        candidates: list[tuple[str, str | None, float | None]] = []
        if signal_exit_triggered:
            candidates.append(("Signal", None, None))
        elif custom_exit_triggered:
            candidates.append(("CustomExit", custom_exit_tag, None))
        if stop_triggered and not stop_is_trailing:
            candidates.append(("Stoploss", None, None))
        if roi_triggered:
            candidates.append(("RoiTarget", None, None))
        if stop_triggered and stop_is_trailing:
            candidates.append(("TrailingStop", None, None))
        if max_hold_triggered:
            candidates.append(("MaxHoldPeriod", None, None))

        p.stop_loss_price, p.stop_is_trailing, p.trailing_activated = \
            stop_loss_price, stop_is_trailing, trailing_activated

        # A resting exit order from an earlier bar. freqtrade re-runs
        # `_check_trade_exit` every bar and `handle_similar_order` cancels-and-
        # replaces the resting order whenever this bar decides on a different
        # price — so anything decided above supersedes it, and the resting
        # order only gets its chance (appended LAST) if nothing else closes the
        # trade. A protective exit therefore always wins over a stale limit.
        if p.pending_exit:
            action, new_price = manage_open_order(pair, p, i, is_entry=False)
            if action in ("timeout", "cancel"):
                # "Close exit order and retry exiting on next signal" — the
                # POSITION stays open; only the order is cancelled.
                p.pending_exit = False
                _detach_order(p.local_trade, p.pending_exit_order)
                p.pending_exit_order = None
                if action == "timeout":
                    timedout_exit_orders += 1
                canceled_exit_orders += 1
            else:
                if action == "replace":
                    _reprice_order(p, i, False, new_price)
                    replaced_exit_orders += 1
                if order_filled_at(p, i, p.pending_exit_price):
                    candidates.append(
                        (p.pending_exit_reason, p.pending_exit_tag, p.pending_exit_price)
                    )

        if not candidates:
            return False

        current_time_exit = pd.Timestamp(p.dates[i]).tz_localize("UTC")

        for exit_reason, custom_tag, resting_price in candidates:
            # An already-resting order fills at its own price; none of the
            # per-reason price resolution below applies to it a second time.
            # confirm_trade_exit was already asked when the order was PLACED
            # (freqtrade only calls it in `_get_exit_for_signal`, never again
            # at fill time), so it is deliberately not re-asked here.
            if resting_price is not None:
                reason_tag = _EXIT_REASON_MAP.get(exit_reason, "exit_signal")
                if exit_reason == "CustomExit" and custom_tag:
                    reason_tag = custom_tag
                close_position(pair, p, i, resting_price, exit_reason, reason_tag,
                               current_time_exit, funding_so_far,
                               fee_close=fill_fee(resting_price, open_i, not is_long))
                return True

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

            strategy_priced_exit = False
            if exit_reason in ("Signal", "CustomExit") and _dp_wired and p.local_trade is not None:
                # current_profit for custom_exit_price, computed at the
                # already-resolved close rate (matches `_get_exit_for_signal`:
                # `current_profit = trade.calc_profit_ratio(close_rate)`,
                # evaluated BEFORE custom_exit_price is applied).
                if is_long:
                    ov_p = amount * base_price * (1.0 + fee_open)
                    cv_p = amount * base_exit_price * (1.0 - fee) + funding_so_far
                    profit_at_close = ((cv_p / ov_p) - 1.0) * lev if ov_p else 0.0
                else:
                    ov_p = amount * base_price * (1.0 - fee_open)
                    cv_p = amount * base_exit_price * (1.0 + fee) - funding_so_far
                    profit_at_close = (1.0 - (cv_p / ov_p)) * lev if ov_p else 0.0
                resolved_exit = _resolve_exit_price(
                    pair, p.local_trade, current_time_exit, base_exit_price,
                    profit_at_close, reason_tag, precision_i,
                )
                # A strategy that prices its own exit limit OWNS that price —
                # the config's blanket offset is only a default for strategies
                # that don't (same rule as custom_entry_price on the way in).
                strategy_priced_exit = resolved_exit != base_exit_price
                base_exit_price = resolved_exit
                # "We can't place orders lower than current low" (long) /
                # "higher than current high" (short) — VulcanTrader doesn't
                # support out-of-range limit exits in live either.
                base_exit_price = max(base_exit_price, low_i) if is_long else min(base_exit_price, high_i)

            # A signal-driven exit becomes an ORDER whenever the exit side is a
            # limit order that rests: priced `exit_offset` on the favourable
            # side of the rate resolved above (or exactly where the strategy
            # asked, if it priced its own), it only closes the trade on a bar
            # whose range reaches it. Protective exits (stoploss/trailing/ROI/
            # max-hold) are deliberately left alone — see the Rust engine's
            # `is_signal_exit` for why.
            if (exit_rests or strategy_priced_exit) and exit_reason in ("Signal", "CustomExit"):
                if exit_rests and not strategy_priced_exit:
                    base_exit_price = base_exit_price * ((1.0 + exit_offset) if is_long
                                                         else (1.0 - exit_offset))
                    base_exit_price = max(base_exit_price, low_i) if is_long \
                        else min(base_exit_price, high_i)
                if not _confirm_exit(pair, p.local_trade, current_time_exit, amount,
                                     base_exit_price, reason_tag):
                    continue  # vetoed -> try the next candidate this same bar
                # This order replaces whatever was resting (freqtrade's
                # handle_similar_order), so nothing else can fill this bar.
                if p.pending_exit and p.pending_exit_price != base_exit_price:
                    _detach_order(p.local_trade, p.pending_exit_order)
                    p.pending_exit_order = None
                    canceled_exit_orders += 1
                if not p.pending_exit or p.pending_exit_price != base_exit_price:
                    p.pending_exit = True
                    p.pending_exit_price = base_exit_price
                    p.pending_exit_bar = i
                    p.pending_exit_reason = exit_reason
                    p.pending_exit_tag = custom_tag
                    if _dp_wired and p.local_trade is not None and p.pending_exit_order is None:
                        try:
                            order = _make_order(
                                pair, p.local_trade, p.local_trade.exit_side, exit_order_type,
                                amount, base_exit_price, current_time_exit, is_open=True,
                                tag=reason_tag,
                            )
                            p.local_trade.orders.append(order)
                            p.pending_exit_order = order
                        except Exception:
                            logger.debug("exit order registration failed for %s", pair, exc_info=True)
                            p.pending_exit_order = None
                if not order_filled_at(p, i, base_exit_price):
                    return False
                close_position(pair, p, i, base_exit_price, exit_reason, reason_tag,
                               current_time_exit, funding_so_far,
                               fee_close=fill_fee(base_exit_price, open_i, not is_long))
                return True

            if not _confirm_exit(pair, p.local_trade, current_time_exit, amount, base_exit_price, reason_tag):
                continue  # vetoed -> try the next candidate this same bar

            close_position(pair, p, i, base_exit_price, exit_reason, reason_tag,
                           current_time_exit, funding_so_far, fee_close=fee)
            return True

        return False  # every candidate this bar was vetoed by confirm_trade_exit

    def close_position(pair: str, p: _PairState, i: int, base_exit_price: float,
                       exit_reason: str, reason_tag: str, current_time_exit,
                       funding_so_far: float, fee_close: float) -> None:
        """Book the close of an open position at an already-resolved,
        already-confirmed fill price, paying `fee_close` on the exit leg
        (maker when the order rested — see `fill_fee`). Shared by the
        immediate-fill path and by a resting exit order that a later bar
        finally touched."""
        nonlocal open_count, running_balance, tied_up_capital
        is_long = not p.is_short
        lev, amount = p.trade_leverage, p.trade_amount
        base_price, entry_price = p.base_price, p.entry_price
        fee_open = p.entry_fee

        exit_price = base_exit_price * (1.0 - fee_close) if is_long else base_exit_price * (1.0 + fee_close)

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
        # already computed by the caller (entry_row -> i, same range) for the
        # bound_profit check — this bar's exit reuses it as-is.
        funding_fees = funding_so_far

        if is_long:
            open_value = amount * base_price * (1.0 + fee_open)
            close_value = amount * base_exit_price * (1.0 - fee_close) + funding_fees
            pnl_amount = close_value - open_value
            leveraged_profit = ((close_value / open_value) - 1.0) * lev if open_value else 0.0
        else:
            open_value = amount * base_price * (1.0 - fee_open)
            close_value = amount * base_exit_price * (1.0 + fee_close) - funding_fees
            pnl_amount = open_value - close_value
            leveraged_profit = (1.0 - (close_value / open_value)) * lev if open_value else 0.0

        running_balance += pnl_amount
        tied_up_capital -= stake

        trades.append({
            "pair": pair, "is_short": p.is_short, "entry_row": p.entry_row, "exit_row": i,
            "entry_price": entry_price, "base_exit_price": base_exit_price, "exit_price": exit_price,
            "base_entry_price": base_price, "fee_open": fee_open, "fee_close": fee_close,
            "profit_ratio": leveraged_profit, "profit_abs": pnl_amount,
            "leverage": lev, "exit_reason": exit_reason, "exit_reason_tag": reason_tag, "stake": stake,
            "funding_fees": funding_fees,
        })

        p.in_position = False
        p.pending_exit = False
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

    def process_pass(pair: str, p: _PairState, i: int, direction: str | None,
                     can_enter_bar: bool) -> str | None:
        """One backtest_loop-equivalent pass. Returns the pre-call open
        direction (or None), used by the reversal-retry loop."""
        nonlocal open_count, rejected
        exiting_dir_before = ("short" if p.is_short else "long") if p.in_position else None

        # freqtrade's backtest_loop step 1 (`manage_open_orders`, which deletes
        # a trade whose entry order timed out) and step 3 (`_try_close_open_
        # order`, which fills it if this candle reaches its price), for an
        # order placed on an EARLIER bar. Orders placed on THIS bar get their
        # own fill attempt inside `do_enter`, exactly as in freqtrade, where
        # step 2 creates the order and step 3 immediately tries to close it.
        # Inert unless a resting entry offset is configured.
        process_pending_entry(pair, p, i)

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
        # A still-resting entry order already holds this pair's slot and its
        # capital (freqtrade: "assumption: there can't be multiple open entry
        # orders at any given time"), so no second one is placed on top of it.
        if can_enter_bar and (not p.in_position) and (not p.pending_entry) and direction is not None:
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
            # An order still resting when the data runs out never filled;
            # count it with the timeouts rather than dropping it silently.
            if p.pending_entry:
                timedout_entry_orders += 1
            continue
        if p.pending_exit:
            timedout_exit_orders += 1
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
            open_value = amount * p.base_price * (1.0 + p.entry_fee)
            close_value = amount * open_i * (1.0 - fee) + funding_fees
            pnl_amount = close_value - open_value
            leveraged_profit = ((close_value / open_value) - 1.0) * lev if open_value else 0.0
        else:
            open_value = amount * p.base_price * (1.0 - p.entry_fee)
            close_value = amount * open_i * (1.0 + fee) - funding_fees
            pnl_amount = open_value - close_value
            leveraged_profit = (1.0 - (close_value / open_value)) * lev if open_value else 0.0

        running_balance += pnl_amount
        tied_up_capital -= stake
        trades.append({
            "pair": pair, "is_short": p.is_short, "entry_row": p.entry_row, "exit_row": idx,
            "entry_price": p.entry_price, "base_exit_price": open_i, "exit_price": exit_price,
            "base_entry_price": p.base_price, "fee_open": p.entry_fee, "fee_close": fee,
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

    return trades, rejected, {
        "timedout_entry_orders": timedout_entry_orders,
        "timedout_exit_orders": timedout_exit_orders,
        "canceled_entry_orders": canceled_entry_orders,
        "canceled_exit_orders": canceled_exit_orders,
        "replaced_entry_orders": replaced_entry_orders,
        "replaced_exit_orders": replaced_exit_orders,
    }


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
        raw_trades, rejected, order_stats = _joint_multi_pair_backtest(
            processed, strategy, config, exchange,
            full_signals=full_signals, required_startup=required_startup,
            dataprovider=dataprovider,
        )
    except Exception:
        logger.exception("Rust-engine joint backtest failed")
        raw_trades, rejected, order_stats = [], 0, {}

    rows: list[dict] = []
    for t in raw_trades:
        dates = t["_dates"]
        ei, xi = t["entry_row"], t["exit_row"]
        is_short = t["is_short"]
        # freqtrade records the RAW fill price and carries fees in fee_open/
        # fee_close separately, so use the raw prices the simulator already
        # tracked rather than dividing the fee back out of the fee-baked ones
        # (which silently assumes both sides paid the SAME fee — untrue as
        # soon as one side fills as a resting maker order).
        fee_open_t = t.get("fee_open", fee)
        fee_close_t = t.get("fee_close", fee)
        open_rate = t.get("base_entry_price") or t["entry_price"]
        close_rate = t.get("base_exit_price") or t["exit_price"]

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
            "fee_open": fee_open_t,
            "fee_close": fee_close_t,
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
        "timedout_entry_orders": order_stats.get("timedout_entry_orders", 0),
        "timedout_exit_orders": order_stats.get("timedout_exit_orders", 0),
        "canceled_trade_entries": order_stats.get("canceled_entry_orders", 0),
        "canceled_entry_orders": order_stats.get("canceled_entry_orders", 0),
        "replaced_entry_orders": order_stats.get("replaced_entry_orders", 0),
        "final_balance": final_bal,
    }
