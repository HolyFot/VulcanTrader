"""
DonchianBreakout — Trend-following breakout strategy using Donchian Channels.

Donchian channels define the highest high and lowest low over N periods. A close
above the prior upper band signals a bullish breakout; below the prior lower band
signals a bearish breakout. This is the systematic foundation of the original
Turtle Trading rules, adapted here for crypto futures on a 15m timeframe.

  Entry Long:  close > previous Donchian upper + RSI > rsi_long_min + volume spike
  Entry Short: close < previous Donchian lower + RSI < rsi_short_max + volume spike
  Exit Long:   price falls back below mid-channel OR RSI reaches overbought
  Exit Short:  price rises back above mid-channel OR RSI reaches oversold
  Stop:        ATR-based fixed stop anchored to entry price
  TF:          15m (breakouts need slightly more candle resolution than 5m scalps)

## Indicator sourcing

RSI(14) and ATR(14) come straight from the Rust backtester's own indicator
code via the `vulcan_rust_indicators` bridge (built from
`VulcanTrader/backtester_py`, a PyO3 wrapper around
`backtester::fast_indicators`) — the exact same series the Rust engine uses,
rather than a second TA-Lib recompute. See `AllIndicatorsDemoStrategy` for the
full standard-indicator index table and build instructions
(`cd VulcanTrader/backtester_py && maturin develop --release`).

The bridge's standard set is fixed-period, so RSI and ATR are locked at 14
here (both previously defaulted to 14, so default behaviour is unchanged).
The Donchian channels and the volume ratio aren't part of the standard set,
so they stay as plain pandas below.
"""

import logging

import numpy as np
import vulcan_rust_indicators as vri
from pandas import DataFrame

from VulcanTrader.strategy import DecimalParameter, IntParameter, IStrategy

logger = logging.getLogger(__name__)

# Standard-indicator indices consumed from the Rust bridge, matching
# fast_indicators::calculate_standard_indicators (see the table in
# AllIndicatorsDemoStrategy). RSI(14) and ATR(14) are fixed-period.
_RSI_IDX = 0
_ATR_IDX = 14


class DonchianBreakout(IStrategy):
    """
    15m trend-following breakout using Donchian Channels + RSI + volume confirmation.
    Enters on channel breaks, exits when price returns to mid-channel or RSI reverses.
    """

    INTERFACE_VERSION = 3

    timeframe = "15m"
    can_short = True
    startup_candle_count = 100
    process_only_new_candles = True

    # Breakout strategies ride longer moves than scalpers
    minimal_roi = {
        "0":   0.08,
        "60":  0.05,
        "180": 0.03,
        "360": 0.01,
    }

    stoploss = -0.05
    trailing_stop = True
    trailing_stop_positive = 0.01
    trailing_stop_positive_offset = 0.02
    trailing_only_offset_is_reached = True

    use_exit_signal = True
    exit_profit_only = False

    # ── Hyperopt parameters ──────────────────────────────────────────────────
    # Note: RSI and ATR *periods* are not optimizable — the Rust bridge computes
    # them at a fixed 14. Only the threshold/multiplier params below are tuned.

    # Donchian period
    don_period_p = IntParameter(15, 40, default=20, space="buy", optimize=True)

    # RSI thresholds (period fixed at 14 by the bridge)
    rsi_long_min_p  = IntParameter(45, 60, default=50, space="buy",  optimize=True)
    rsi_short_max_p = IntParameter(40, 55, default=50, space="sell", optimize=True)
    rsi_ob_p        = IntParameter(65, 80, default=70, space="sell", optimize=True)
    rsi_os_p        = IntParameter(20, 35, default=30, space="buy",  optimize=True)

    # Volume confirmation
    vol_factor_p = DecimalParameter(1.0, 2.5, default=1.5, decimals=1,
                                    space="buy", optimize=True)

    # ATR stop (ATR period fixed at 14 by the bridge; only the multiplier is tuned)
    atr_stop_mult = DecimalParameter(1.5, 4.0, default=2.0, decimals=1,
                                     space="buy", optimize=True)

    # -----------------------------------------------------------------------
    # Indicators
    # -----------------------------------------------------------------------

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # ── Standard indicators from the Rust engine's own code ────────────────
        # One call returns every standard series by its fixed index; we read the
        # two this strategy uses (RSI, ATR) instead of recomputing them in TA-Lib.
        rust_indicators = vri.calculate_standard_indicators(
            dataframe["close"].to_numpy(dtype=np.float64),
            dataframe["high"].to_numpy(dtype=np.float64),
            dataframe["low"].to_numpy(dtype=np.float64),
            dataframe["volume"].to_numpy(dtype=np.float64),
        )
        dataframe["rsi"] = rust_indicators[_RSI_IDX]
        dataframe["atr"] = rust_indicators[_ATR_IDX]

        # ── Custom indicators (not in the Rust standard set) — plain pandas ────
        period = self.don_period_p.value

        # Donchian Channels
        dataframe["don_upper"] = dataframe["high"].rolling(period).max()
        dataframe["don_lower"] = dataframe["low"].rolling(period).min()
        dataframe["don_mid"]   = (dataframe["don_upper"] + dataframe["don_lower"]) / 2.0

        # Volume ratio (current vs 20-bar average)
        dataframe["vol_ma"]    = dataframe["volume"].rolling(20).mean()
        dataframe["vol_ratio"] = (
            dataframe["volume"] / dataframe["vol_ma"].replace(0, np.nan)
        ).fillna(1.0)

        return dataframe

    # -----------------------------------------------------------------------
    # Entry signals
    # -----------------------------------------------------------------------

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        vol_ok = dataframe["vol_ratio"] >= self.vol_factor_p.value

        # Long: close breaks above the PREVIOUS bar's upper band
        # (using shift(1) avoids the trivial case where the bar itself creates the band)
        dataframe.loc[
            vol_ok
            & (dataframe["close"] > dataframe["don_upper"].shift(1))
            & (dataframe["rsi"] > self.rsi_long_min_p.value)
            & (dataframe["volume"] > 0),
            ["enter_long", "enter_tag"],
        ] = [1, "don_upper_break"]

        # Short: close breaks below the PREVIOUS bar's lower band
        dataframe.loc[
            vol_ok
            & (dataframe["close"] < dataframe["don_lower"].shift(1))
            & (dataframe["rsi"] < self.rsi_short_max_p.value)
            & (dataframe["volume"] > 0),
            ["enter_short", "enter_tag"],
        ] = [1, "don_lower_break"]

        return dataframe

    # -----------------------------------------------------------------------
    # Exit signals
    # -----------------------------------------------------------------------

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Long exit: price retreats to mid-channel (momentum stalled) OR RSI overbought
        dataframe.loc[
            (dataframe["close"] < dataframe["don_mid"])
            | (dataframe["rsi"] > self.rsi_ob_p.value),
            "exit_long",
        ] = 1

        # Short exit: price recovers to mid-channel OR RSI oversold
        dataframe.loc[
            (dataframe["close"] > dataframe["don_mid"])
            | (dataframe["rsi"] < self.rsi_os_p.value),
            "exit_short",
        ] = 1

        return dataframe

    # -----------------------------------------------------------------------
    # Leverage
    # -----------------------------------------------------------------------

    def leverage(self, pair, current_time, current_rate, proposed_leverage,
                 max_leverage, entry_tag, side, **kwargs):
        return min(5.0, max_leverage)

    # -----------------------------------------------------------------------
    # ATR stop anchored to entry price
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

            # Stop is a fixed price level: entry ± mult * ATR
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
            logger.debug(f"DonchianBreakout stoploss error for {pair}: {exc}")
            return None
