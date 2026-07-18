"""
Rust-engine backtest driver.

Runs a strategy's *already-computed* entry/exit signals through the fast Rust
backtest engine (``backtester::cpu_engine`` via the ``vulcan_rust_indicators``
extension's ``run_backtest``) instead of the Python ``backtest_loop``.

The strategy is still Python: ``populate_indicators`` / ``populate_entry_trend``
/ ``populate_exit_trend`` run exactly as normal to produce the ``enter_long`` /
``enter_short`` / ``exit_long`` / ``exit_short`` columns. This driver only
replaces the per-candle *simulation* with the Rust engine, then formats the
result into the same trades DataFrame that ``generate_backtest_stats`` consumes
— so the web portal reads it unchanged.

Fidelity notes (the Rust engine is a fast, simplified single-pair simulator):
  - One position per pair; long and short books are simulated independently and
    merged (they can overlap, unlike the Python engine's single-slot model).
  - ``leverage()`` / ``custom_stake_amount()`` / ``custom_exit()`` /
    ``custom_stoploss()`` callbacks are NOT invoked — sizing is fixed and stops
    come from the static config (base stoploss, ROI, trailing, ATR-stop).
  - No protections, no position adjustment/DCA, no informative pairs.
Use it for fast first-pass screening; use the Python engine for final parity.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

import numpy as np
import pandas as pd

from VulcanTrader.data.btanalysis.bt_fileutils import BT_DATA_COLUMNS

logger = logging.getLogger(__name__)

# Standard-indicator indices from fast_indicators::calculate_standard_indicators.
_RSI, _MACD_HIST, _BB_POS, _ATR, _CCI = 0, 9, 10, 14, 17

# Rust ExitReason Debug labels -> freqtrade exit_reason strings.
_EXIT_REASON_MAP = {
    "Signal": "exit_signal",
    "RoiTarget": "roi",
    "Stoploss": "stop_loss",
    "TrailingStop": "trailing_stop_loss",
    "MaxHoldPeriod": "force_exit",
    "CciExit": "exit_signal",
    "RsiExit": "exit_signal",
    "MacdExit": "exit_signal",
    "CustomExit": "custom_exit",
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
    """Translate the freqtrade strategy/config into UnifiedBacktestConfig keys."""
    tf = getattr(strategy, "timeframe", None) or config.get("timeframe", "15m")
    tf_min = _parse_timeframe_minutes(tf)

    # minimal_roi {minute_str: roi} -> the engine's 4 ROI tiers (bar-indexed).
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

    eng = {
        "timeframe": tf,
        "timeframe_minutes": tf_min,
        "trade_type": "Spot" if trade_type == "spot" else "Futures",
        "startup_candle_count": int(getattr(strategy, "startup_candle_count", 50) or 50),
        # Sizing (fixed — callbacks not honored). leverage default 1.0 unless set.
        "leverage_default": float(config.get("rust_leverage", 1.0)),
        "leverage_max": float(config.get("rust_leverage_max", 10.0)),
        "fee_taker": fee,
        "fee_maker": float(config.get("fee_maker", fee)),
        # ROI
        "roi_enabled": True,
        "roi_6": roi_vals[0], "roi_3": roi_vals[1], "roi_15": roi_vals[2], "roi_720": roi_vals[3],
        "roi_period_0": roi_periods[0], "roi_period_10": roi_periods[1],
        "roi_period_30": roi_periods[2], "roi_period_720": roi_periods[3],
        "max_hold_period": int(getattr(strategy, "max_hold_period", 0) or roi_periods[-1]),
        # Stoploss
        "base_stoploss": float(getattr(strategy, "stoploss", -0.10) or -0.10),
        # Trailing (freqtrade: offset = activation, positive = trail distance)
        "trailing_enabled": bool(getattr(strategy, "trailing_stop", False)),
        "trailing_trigger": float(getattr(strategy, "trailing_stop_positive_offset", 0.0) or 0.0),
        "trailing_offset": float(getattr(strategy, "trailing_stop_positive", 0.0) or 0.0),
        # Compounding off the wallet so PnL is in real currency.
        "compounding_enabled": True,
        "starting_balance": float(config.get("dry_run_wallet", 10000.0) or 10000.0),
        "tradable_balance_ratio": float(config.get("tradable_balance_ratio", 1.0) or 1.0),
    }
    return eng


def _u8(series: pd.Series | None, n: int) -> np.ndarray:
    if series is None:
        return np.zeros(n, dtype=np.uint8)
    return (series.fillna(0).to_numpy() != 0).astype(np.uint8)


def _f64(series: pd.Series, n: int) -> np.ndarray:
    return np.nan_to_num(series.to_numpy(dtype=np.float64), nan=0.0)


def _trades_for_pair(pair: str, df: pd.DataFrame, strategy: Any, config: dict,
                     can_short: bool) -> list[dict]:
    import vulcan_rust_indicators as vri

    n = len(df)
    if n == 0:
        return []
    close = _f64(df["close"], n)
    high = _f64(df["high"], n)
    low = _f64(df["low"], n)
    openp = _f64(df["open"], n)
    vol = _f64(df["volume"], n)

    ind = vri.calculate_standard_indicators(close, high, low, vol)
    rsi, macd_hist = np.array(ind[_RSI]), np.array(ind[_MACD_HIST])
    bb_pos, atr, cci = np.array(ind[_BB_POS]), np.array(ind[_ATR]), np.array(ind[_CCI])

    dates = pd.to_datetime(df["date"]).to_list()
    eng_cfg = json.dumps(_build_engine_config(strategy, config))
    fee = float(config.get("fee", 0.0005) or 0.0005)

    rows: list[dict] = []
    directions = [("long", False)]
    if can_short:
        directions.append(("short", True))

    for direction, is_short in directions:
        entries = _u8(df.get(f"enter_{direction}"), n)
        exits = _u8(df.get(f"exit_{direction}"), n)
        if not entries.any():
            continue
        out = json.loads(vri.run_backtest(
            openp, high, low, close, vol,
            rsi, macd_hist, bb_pos, atr, cci,
            entries, exits, direction, eng_cfg,
        ))
        for k in range(len(out["profits"])):
            ei, xi = int(out["entry_indices"][k]), int(out["exit_indices"][k])
            if ei >= n or xi >= n:
                continue
            open_rate = float(out["entry_prices"][k])
            close_rate = float(out["exit_prices"][k])
            profit_ratio = float(out["profits"][k])
            profit_abs = float(out["pnl_amounts"][k])
            leverage = float(out["leverages"][k]) or 1.0
            # Stake implied from PnL so downstream stats are self-consistent.
            stake = abs(profit_abs / profit_ratio) if abs(profit_ratio) > 1e-9 else 0.0
            amount = (stake * leverage / open_rate) if open_rate > 0 else 0.0
            reason = _EXIT_REASON_MAP.get(out["exit_reasons"][k], "exit_signal")
            rows.append({
                "pair": pair,
                "stake_amount": stake,
                "max_stake_amount": stake,
                "amount": amount,
                "open_date": dates[ei],
                "close_date": dates[xi],
                "open_rate": open_rate,
                "close_rate": close_rate,
                "fee_open": fee,
                "fee_close": fee,
                "trade_duration": int(out["durations"][k]) * _parse_timeframe_minutes(
                    getattr(strategy, "timeframe", None) or config.get("timeframe", "15m")),
                "profit_ratio": profit_ratio,
                "profit_abs": profit_abs,
                "exit_reason": reason,
                "initial_stop_loss_abs": 0.0,
                "initial_stop_loss_ratio": float(getattr(strategy, "stoploss", -0.10) or -0.10),
                "stop_loss_abs": 0.0,
                "stop_loss_ratio": float(getattr(strategy, "stoploss", -0.10) or -0.10),
                "min_rate": min(open_rate, close_rate),
                "max_rate": max(open_rate, close_rate),
                "is_open": False,
                "enter_tag": None,
                "leverage": leverage,
                "is_short": is_short,
                "open_timestamp": int(pd.Timestamp(dates[ei]).timestamp() * 1000),
                "close_timestamp": int(pd.Timestamp(dates[xi]).timestamp() * 1000),
                "orders": [],
                "funding_fees": 0.0,
            })
    return rows


def run_rust_backtest(processed: dict[str, pd.DataFrame], strategy: Any,
                      config: dict) -> dict:
    """Run all pairs through the Rust engine; return a backtest() result dict."""
    can_short = bool(getattr(strategy, "can_short", False)) and \
        str(config.get("trading_mode", "futures")) != "spot"

    all_rows: list[dict] = []
    for pair, df in processed.items():
        if df is None or df.empty:
            continue
        try:
            all_rows.extend(_trades_for_pair(pair, df, strategy, config, can_short))
        except Exception:
            logger.exception("Rust-engine backtest failed for %s", pair)

    results = pd.DataFrame(all_rows, columns=BT_DATA_COLUMNS)
    if not results.empty:
        results = results.sort_values("open_date").reset_index(drop=True)

    start_bal = float(config.get("dry_run_wallet", 10000.0) or 10000.0)
    final_bal = start_bal + (results["profit_abs"].sum() if not results.empty else 0.0)
    return {
        "results": results,
        "config": strategy.config,
        "locks": [],
        "rejected_signals": 0,
        "timedout_entry_orders": 0,
        "timedout_exit_orders": 0,
        "canceled_trade_entries": 0,
        "canceled_entry_orders": 0,
        "replaced_entry_orders": 0,
        "final_balance": final_bal,
    }
