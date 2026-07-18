"""
FisherStatReversion — statistical mean-reversion driven by custom indicators.

This strategy is the counterpart to AllIndicatorsDemoStrategy: where that one
shows reading the Rust engine's *standard* indicators through the
`vulcan_rust_indicators` bridge, this one shows the other half of the pattern —
building your **own** indicators, by hand in pandas/numpy, for anything the
standard set doesn't cover. None of the three signal indicators here exist in
`fast_indicators::calculate_standard_indicators`; only ATR (for the stop) is
pulled from the bridge.

Custom indicators (all computed below, none from the bridge):
  - Fisher Transform  — Ehlers' transform of the N-bar price position. It
    Gaussian-izes price so turning points become sharp, near-unbounded spikes
    instead of the compressed 0-100 an oscillator like RSI gives. Recursive, so
    it's built in an explicit loop.
  - Return z-score    — how many standard deviations the latest bar's return is
    from its own rolling mean; flags a statistically extreme move.
  - Linreg slope      — slope of the least-squares line over the last N bars,
    normalized by price (fractional drift per bar). Used only as a regime gate:
    don't fade a move that's part of a strong trend.

Logic (mean reversion — fade stretched moves, not breakouts):
  Entry Long:  Fisher deeply negative AND turning up, return z-score < -z_entry,
               and no strong down-trend (slope not below -slope_max).
  Entry Short: mirror image.
  Exit:        Fisher reverts back through zero (move has mean-reverted).
  Stop:        ATR-anchored fixed stop (ATR from the Rust bridge).
  TF:          15m.

Not tuned for profit — it's a template for the custom-indicator path.
"""

import logging

import numpy as np
import pandas as pd
import vulcan_rust_indicators as vri
from pandas import DataFrame

from VulcanTrader.strategy import DecimalParameter, IntParameter, IStrategy

logger = logging.getLogger(__name__)

# ATR index in fast_indicators::calculate_standard_indicators (see the table in
# AllIndicatorsDemoStrategy). The only standard series this strategy consumes.
_ATR_IDX = 14


def _fisher_transform(high: np.ndarray, low: np.ndarray, period: int) -> np.ndarray:
    """Ehlers Fisher Transform of the N-bar position of the median price.

    Recursive (each bar smooths the previous value and the previous fisher), so
    it can't be vectorized cleanly — an explicit loop is the honest form. NaN
    until the rolling window is full.
    """
    hl2 = (high + low) / 2.0
    hh = pd.Series(hl2).rolling(period).max().to_numpy()
    ll = pd.Series(hl2).rolling(period).min().to_numpy()

    n = hl2.shape[0]
    fisher = np.full(n, np.nan)
    value = 0.0
    fish = 0.0
    for i in range(n):
        rng = hh[i] - ll[i]
        if np.isnan(rng) or rng <= 0.0:
            continue  # warmup / flat range: hold state, emit NaN
        # Normalize price position to 0..1, rescale to -1..1, smooth.
        raw = (hl2[i] - ll[i]) / rng
        value = 0.66 * (2.0 * raw - 1.0) + 0.67 * value
        value = min(max(value, -0.999), 0.999)  # keep the log finite
        fish = 0.5 * np.log((1.0 + value) / (1.0 - value)) + 0.5 * fish
        fisher[i] = fish
    return fisher


def _rolling_zscore(close: pd.Series, window: int) -> pd.Series:
    """Z-score of the latest 1-bar return vs its own rolling distribution."""
    returns = close.pct_change()
    mean = returns.rolling(window).mean()
    std = returns.rolling(window).std()
    return (returns - mean) / std.replace(0.0, np.nan)


def _rolling_linreg_slope(close: pd.Series, period: int) -> np.ndarray:
    """Least-squares slope over each trailing `period`-bar window, normalized by
    the current price (fractional drift per bar). Vectorized via a sliding
    window; NaN until the first full window."""
    y = close.to_numpy(dtype=np.float64)
    n = y.shape[0]
    slope = np.full(n, np.nan)
    if n < period:
        return slope

    x = np.arange(period, dtype=np.float64)
    x_dev = x - x.mean()
    denom = (x_dev ** 2).sum()

    windows = np.lib.stride_tricks.sliding_window_view(y, period)
    y_dev = windows - windows.mean(axis=1, keepdims=True)
    raw_slope = (x_dev * y_dev).sum(axis=1) / denom
    # Normalize by price so the threshold is asset-agnostic.
    slope[period - 1:] = raw_slope / y[period - 1:]
    return slope


class FisherStatReversion(IStrategy):
    """15m statistical mean-reversion on custom Fisher / z-score / slope indicators."""

    INTERFACE_VERSION = 3

    timeframe = "15m"
    can_short = True
    startup_candle_count = 120  # covers the longest custom lookback + ATR warmup
    process_only_new_candles = True

    # Reversion trades are quick — take profit fast, don't ride.
    minimal_roi = {
        "0":   0.04,
        "30":  0.025,
        "90":  0.01,
        "180": 0.0,
    }

    stoploss = -0.06
    trailing_stop = True
    trailing_stop_positive = 0.01
    trailing_stop_positive_offset = 0.02
    trailing_only_offset_is_reached = True

    use_exit_signal = True
    exit_profit_only = False

    # ── Hyperopt parameters (all custom-indicator knobs) ─────────────────────

    # Fisher Transform
    fisher_period_p = IntParameter(6, 20, default=10, space="buy", optimize=True)
    fisher_entry_p  = DecimalParameter(1.0, 3.0, default=1.5, decimals=1,
                                       space="buy", optimize=True)

    # Return z-score
    zscore_win_p   = IntParameter(10, 40, default=20, space="buy", optimize=True)
    zscore_entry_p = DecimalParameter(1.5, 3.0, default=2.0, decimals=1,
                                      space="buy", optimize=True)

    # Linreg-slope regime gate: reject entries when the trend is strongly
    # against the reversion. Units are fractional price drift per bar.
    slope_period_p = IntParameter(10, 40, default=20, space="buy", optimize=True)
    slope_max_p    = DecimalParameter(0.0005, 0.004, default=0.0015, decimals=4,
                                      space="buy", optimize=True)

    # ATR stop multiplier (ATR itself is period-14, from the bridge)
    atr_stop_mult = DecimalParameter(1.5, 4.0, default=2.5, decimals=1,
                                     space="buy", optimize=True)

    # -----------------------------------------------------------------------
    # Indicators
    # -----------------------------------------------------------------------

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # ── Standard: ATR only, straight from the Rust engine's own code ───────
        rust_indicators = vri.calculate_standard_indicators(
            dataframe["close"].to_numpy(dtype=np.float64),
            dataframe["high"].to_numpy(dtype=np.float64),
            dataframe["low"].to_numpy(dtype=np.float64),
            dataframe["volume"].to_numpy(dtype=np.float64),
        )
        dataframe["atr"] = rust_indicators[_ATR_IDX]

        # ── Custom indicators (hand-built in pandas/numpy) ────────────────────
        dataframe["fisher"] = _fisher_transform(
            dataframe["high"].to_numpy(dtype=np.float64),
            dataframe["low"].to_numpy(dtype=np.float64),
            self.fisher_period_p.value,
        )
        dataframe["fisher_prev"] = dataframe["fisher"].shift(1)

        dataframe["zscore"] = _rolling_zscore(dataframe["close"], self.zscore_win_p.value)

        dataframe["slope"] = _rolling_linreg_slope(dataframe["close"], self.slope_period_p.value)

        return dataframe

    # -----------------------------------------------------------------------
    # Entry signals
    # -----------------------------------------------------------------------

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        fisher_entry = self.fisher_entry_p.value
        z_entry = self.zscore_entry_p.value
        slope_max = self.slope_max_p.value

        # Long: Fisher deeply negative and turning up, return statistically
        # stretched to the downside, and not inside a strong down-trend.
        dataframe.loc[
            (dataframe["fisher"] < -fisher_entry)
            & (dataframe["fisher"] > dataframe["fisher_prev"])
            & (dataframe["zscore"] < -z_entry)
            & (dataframe["slope"] > -slope_max)
            & (dataframe["volume"] > 0),
            ["enter_long", "enter_tag"],
        ] = [1, "fisher_reversion_long"]

        # Short: mirror image.
        dataframe.loc[
            (dataframe["fisher"] > fisher_entry)
            & (dataframe["fisher"] < dataframe["fisher_prev"])
            & (dataframe["zscore"] > z_entry)
            & (dataframe["slope"] < slope_max)
            & (dataframe["volume"] > 0),
            ["enter_short", "enter_tag"],
        ] = [1, "fisher_reversion_short"]

        return dataframe

    # -----------------------------------------------------------------------
    # Exit signals
    # -----------------------------------------------------------------------

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Exit once the Fisher transform has reverted back across the mean.
        dataframe.loc[dataframe["fisher"] > 0, "exit_long"] = 1
        dataframe.loc[dataframe["fisher"] < 0, "exit_short"] = 1
        return dataframe

    # -----------------------------------------------------------------------
    # Leverage
    # -----------------------------------------------------------------------

    def leverage(self, pair, current_time, current_rate, proposed_leverage,
                 max_leverage, entry_tag, side, **kwargs):
        return min(4.0, max_leverage)

    # -----------------------------------------------------------------------
    # ATR stop anchored to entry price (ATR from the Rust bridge)
    # -----------------------------------------------------------------------

    def custom_stoploss(self, pair, trade, current_time, current_rate,
                        current_profit, after_fill, **kwargs):
        try:
            dataframe, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
            if dataframe is None or len(dataframe) < 2:
                return None

            atr = float(dataframe.iloc[-1].get("atr", np.nan))
            if np.isnan(atr) or atr <= 0 or trade.open_rate <= 0:
                return None

            mult = float(self.atr_stop_mult.value)

            if not trade.is_short:
                stop_price = trade.open_rate - mult * atr
                sl = (stop_price / current_rate) - 1.0
            else:
                stop_price = trade.open_rate + mult * atr
                sl = 1.0 - (stop_price / current_rate)

            if sl >= 0:
                return None

            return max(self.stoploss, sl)

        except Exception as exc:
            logger.debug(f"FisherStatReversion stoploss error for {pair}: {exc}")
            return None
