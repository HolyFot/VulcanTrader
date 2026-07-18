"""
AllIndicatorsDemoStrategy — a reference/teaching strategy, NOT a real
trading strategy. It exists to show how a Python strategy can call
straight into the Rust backtester's own indicator implementations via the
`vulcan_rust_indicators` extension module (built from
`VulcanTrader/backtester_py`, a PyO3 wrapper around
`backtester::fast_indicators` — the exact same Rust code the Rust engine
itself uses), instead of recomputing the same indicators a second time in
pandas/TA-Lib.

Build/install the bridge once per environment (installs directly into the
active venv as an editable extension module):

    cd VulcanTrader/VulcanTrader/backtester_py
    maturin develop --release

Every other strategy in this folder is a bespoke statistical construction
computed by hand in pandas/numpy (jackknife variance, Gini mean
difference, Qn robust scale, etc.) — none of them use this bridge, or even
much of TA-Lib (ATR is the only TA-Lib call any of them make, for their
dead custom_stoploss). This file is the one place showing the Rust-backed
path.

## The standard indicator map

`vulcan_rust_indicators.calculate_standard_indicators(close, high, low,
volume)` returns a dict of `{index: numpy.ndarray}` — the exact same 23
series and fixed-index convention `fast_indicators::calculate_standard_
indicators` computes for every Rust strategy (see the identical table in
`backtester/src/strategies/all_indicators_demo.rs`):

  idx  name             range/units          meaning
  ---  ---------------  -------------------  ------------------------------------------------
   0   rsi              0-100                RSI(14)
   1   sma10            price                Simple moving average, 10-bar
   2   sma20            price                Simple moving average, 20-bar
   3   sma50            price                Simple moving average, 50-bar
   4   ema9             price                Exponential moving average, 9-bar
   5   ema21            price                Exponential moving average, 21-bar
   6   ema55            price                Exponential moving average, 55-bar
   7   macd             price units          EMA(12) - EMA(26)
   8   macdsignal       price units          EMA(9) of the MACD line
   9   macdhist         price units          MACD line minus signal line
  10   bb_pos           0-1 (can exceed)     0 = at lower band, 1 = at upper band, 0.5 = middle
  11   bb_upper         price                Bollinger upper band (SMA20, 2 stdev)
  12   bb_mid           price                Bollinger middle band (= SMA20)
  13   bb_lower         price                Bollinger lower band
  14   atr              price units          Average True Range(14)
  15   roc              percent              Rate of change over 10 bars
  16   mfi              0-100                Money Flow Index (volume-weighted RSI analogue)
  17   cci              ~-200..200           Commodity Channel Index(20)
  18   adx              0-100+               Trend *strength* (direction-agnostic; >25 = trending)
  19   fvg              {-1, 0, +1}          Fair-value-gap flag: +1 bullish, -1 bearish
  20   vwap             price                **Cumulative since bar 0**, not session-relative
  21   chop             0-100                Choppiness Index(14); >61.8 "choppy", <38.2 "trending"
  22   trend_eff        0-1                  Kaufman trend efficiency; magnitude only, no direction

Two gotchas worth knowing before using indices 18/19/20/22 for real (same
ones documented on the Rust side):
  - ADX(18) and trend efficiency(22) are direction-agnostic — pair them
    with something directional (an EMA crossover, MACD sign, etc.) to pick
    a side, as this file does below.
  - VWAP(20) is cumulative from the start of the fed history, not a
    rolling or session-anchored VWAP.

## Custom indicators

Anything the Rust standard set doesn't cover is still just plain pandas,
same as every other strategy here — this file adds one: a rolling 20-bar
z-score of returns, computed in Python since it isn't part of the bridge.

Entry/exit logic below is a simple additive "confluence count" purely to
show each indicator being read and compared — it is NOT tuned and should
not be expected to be profitable.
"""

import logging

import numpy as np
import vulcan_rust_indicators as vri
from pandas import DataFrame

from VulcanTrader.strategy import IStrategy

logger = logging.getLogger(__name__)

ZSCORE_WINDOW = 20
ADX_TREND_THRESHOLD = 25.0
CONFLUENCE_THRESHOLD = 6  # out of ~9 possible points below

# Index -> column name, matching fast_indicators::calculate_standard_indicators
# and the Rust-side all_indicators_demo.rs table exactly.
_INDICATOR_COLUMNS = {
    0: "rsi", 1: "sma10", 2: "sma20", 3: "sma50",
    4: "ema9", 5: "ema21", 6: "ema55",
    7: "macd", 8: "macdsignal", 9: "macdhist",
    10: "bb_pos", 11: "bb_upper", 12: "bb_mid", 13: "bb_lower",
    14: "atr", 15: "roc", 16: "mfi", 17: "cci", 18: "adx",
    19: "fvg", 20: "vwap", 21: "chop", 22: "trend_eff",
}


def _rolling_zscore(returns: np.ndarray) -> float:
    if len(returns) < ZSCORE_WINDOW:
        return 0.0
    mean = returns.mean()
    std = returns.std()
    if std <= 1e-9:
        return 0.0
    return float((returns[-1] - mean) / std)


class AllIndicatorsDemoStrategy(IStrategy):
    """Reference strategy bridging into the Rust backtester's own indicators. Not tuned."""

    INTERFACE_VERSION = 3

    timeframe = "15m"
    can_short = True
    startup_candle_count = 60  # covers SMA(50), the longest built-in lookback below
    process_only_new_candles = True

    minimal_roi = {
        "0": 0.05,
        "60": 0.03,
        "180": 0.015,
        "360": 0.0,
    }

    stoploss = -0.07
    trailing_stop = True
    trailing_stop_positive = 0.008
    trailing_stop_positive_offset = 0.018
    trailing_only_offset_is_reached = True

    use_exit_signal = True
    exit_profit_only = False

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # One call into the Rust engine's own indicator code, returning
        # every standard series at once by its fixed index.
        rust_indicators = vri.calculate_standard_indicators(
            dataframe["close"].to_numpy(dtype=np.float64),
            dataframe["high"].to_numpy(dtype=np.float64),
            dataframe["low"].to_numpy(dtype=np.float64),
            dataframe["volume"].to_numpy(dtype=np.float64),
        )
        for idx, column_name in _INDICATOR_COLUMNS.items():
            dataframe[column_name] = rust_indicators[idx]

        # --- Custom (non-Rust, non-TA-Lib) indicator: rolling z-score of returns ---
        returns = dataframe["close"].pct_change()
        dataframe["zscore"] = returns.rolling(ZSCORE_WINDOW).apply(
            lambda w: _rolling_zscore(w.to_numpy()), raw=False
        )

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        zeros = np.zeros(len(dataframe), dtype=int)
        long_score = zeros.copy()
        short_score = zeros.copy()

        # Momentum: RSI oversold/overbought.
        long_score = long_score + (dataframe["rsi"] < 30).astype(int)
        short_score = short_score + (dataframe["rsi"] > 70).astype(int)

        # Trend direction: fast EMA vs slow EMA crossover.
        long_score = long_score + (dataframe["ema9"] > dataframe["ema21"]).astype(int)
        short_score = short_score + (dataframe["ema9"] < dataframe["ema21"]).astype(int)

        # Trend direction: SMA20 vs SMA50.
        long_score = long_score + (dataframe["sma20"] > dataframe["sma50"]).astype(int)
        short_score = short_score + (dataframe["sma20"] < dataframe["sma50"]).astype(int)

        # Momentum: MACD histogram sign.
        long_score = long_score + (dataframe["macdhist"] > 0).astype(int)
        short_score = short_score + (dataframe["macdhist"] < 0).astype(int)

        # Mean reversion: price near a Bollinger Band extreme.
        long_score = long_score + (dataframe["bb_pos"] < 0.1).astype(int)
        short_score = short_score + (dataframe["bb_pos"] > 0.9).astype(int)

        # Mean reversion: CCI past its conventional +-100 band.
        long_score = long_score + (dataframe["cci"] < -100).astype(int)
        short_score = short_score + (dataframe["cci"] > 100).astype(int)

        # Trend-following gate: ADX is direction-agnostic, so pair it with
        # the EMA crossover's direction to decide which side gets the point.
        strong_trend = dataframe["adx"] > ADX_TREND_THRESHOLD
        long_score = long_score + (strong_trend & (dataframe["ema9"] > dataframe["ema21"])).astype(int)
        short_score = short_score + (strong_trend & (dataframe["ema9"] < dataframe["ema21"])).astype(int)

        # Volume-weighted mean reversion: price vs (cumulative) VWAP.
        vwap_dev = (dataframe["close"] - dataframe["vwap"]) / dataframe["vwap"]
        long_score = long_score + (vwap_dev < -0.01).astype(int)
        short_score = short_score + (vwap_dev > 0.01).astype(int)

        # Money flow: MFI oversold/overbought.
        long_score = long_score + (dataframe["mfi"] < 20).astype(int)
        short_score = short_score + (dataframe["mfi"] > 80).astype(int)

        # Fair value gap: already signed (+1 bullish / -1 bearish / 0 none).
        long_score = long_score + (dataframe["fvg"] > 0).astype(int)
        short_score = short_score + (dataframe["fvg"] < 0).astype(int)

        # Momentum: rate of change direction.
        long_score = long_score + (dataframe["roc"] > 0).astype(int)
        short_score = short_score + (dataframe["roc"] < 0).astype(int)

        # Custom indicator: statistically extreme recent return.
        long_score = long_score + (dataframe["zscore"] < -2.0).astype(int)
        short_score = short_score + (dataframe["zscore"] > 2.0).astype(int)

        # Regime gate (not scored): skip choppy, rangebound conditions.
        not_choppy = dataframe["chop"] <= 61.8
        # Volatility gate (not scored): skip near-zero-ATR, illiquid bars.
        has_volatility = dataframe["atr"] > 0

        dataframe.loc[
            not_choppy & has_volatility & (long_score >= CONFLUENCE_THRESHOLD) & (dataframe["volume"] > 0),
            ["enter_long", "enter_tag"],
        ] = [1, "confluence_long"]

        dataframe.loc[
            not_choppy & has_volatility & (short_score >= CONFLUENCE_THRESHOLD) & (dataframe["volume"] > 0),
            ["enter_short", "enter_tag"],
        ] = [1, "confluence_short"]

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Exit when the same mean-reversion indicators that could have
        # triggered entry have swung back toward, or past, the opposite
        # extreme.
        long_exit = (dataframe["rsi"] > 60) | (dataframe["bb_pos"] > 0.7) | (dataframe["cci"] > 50)
        short_exit = (dataframe["rsi"] < 40) | (dataframe["bb_pos"] < 0.3) | (dataframe["cci"] < -50)

        dataframe.loc[long_exit, "exit_long"] = 1
        dataframe.loc[short_exit, "exit_short"] = 1
        return dataframe
