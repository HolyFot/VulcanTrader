"""
IchimokuCloud — trend-following on a hand-built Ichimoku Kinko Hyo system.

Another custom-indicator strategy (cf. FisherStatReversion): the entire Ichimoku
system is computed here in pandas — none of its five lines exist in
`fast_indicators::calculate_standard_indicators`. Only ATR (for the stop) comes
from the `vulcan_rust_indicators` bridge.

The Ichimoku lines (all custom):
  - Tenkan-sen  (conversion): midpoint of the last `tenkan` bars' high/low.
  - Kijun-sen   (base):       midpoint of the last `kijun` bars' high/low.
  - Senkou A    (lead span A): (Tenkan + Kijun) / 2, projected `displacement`
                               bars forward.
  - Senkou B    (lead span B): midpoint of the last `senkou_b` bars, projected
                               `displacement` bars forward.
  - The cloud (kumo) at the current bar is the band between Senkou A and B, and
    is read via a FORWARD shift of past values — so it uses only data from
    `displacement` bars ago (no look-ahead).
  - Chikou (lagging) confirmation is expressed look-ahead-safely as
    "current close vs the close `displacement` bars ago".

Logic (trend-following — trade with the cloud, not against it):
  Entry Long:  Tenkan crosses above Kijun, price is above the cloud, and the
               close is above the close `displacement` bars back (Chikou-style
               confirmation).
  Entry Short: mirror image (TK cross down, price below the cloud).
  Exit Long:   Tenkan crosses back below Kijun, or close drops below Kijun.
  Exit Short:  mirror image.
  Stop:        ATR-anchored fixed stop (ATR from the Rust bridge).
  TF:          15m.

Not tuned for profit — it's a template for the custom-indicator path.
"""

import logging

import numpy as np
import vulcan_rust_indicators as vri
from pandas import DataFrame

from VulcanTrader.strategy import IntParameter, DecimalParameter, IStrategy

logger = logging.getLogger(__name__)

# ATR index in fast_indicators::calculate_standard_indicators (see the table in
# AllIndicatorsDemoStrategy). The only standard series this strategy consumes.
_ATR_IDX = 14


def _donchian_mid(dataframe: DataFrame, period: int):
    """Ichimoku's building block: midpoint of the rolling high/low over `period`."""
    high = dataframe["high"].rolling(period).max()
    low = dataframe["low"].rolling(period).min()
    return (high + low) / 2.0


class IchimokuCloud(IStrategy):
    """15m Ichimoku trend-following on hand-built Tenkan/Kijun/Kumo lines."""

    INTERFACE_VERSION = 3

    timeframe = "15m"
    can_short = True
    startup_candle_count = 130  # senkou_b (52) + displacement (26) + ATR warmup
    process_only_new_candles = True

    # Trend trades ride longer than reversion trades.
    minimal_roi = {
        "0":   0.08,
        "120": 0.04,
        "300": 0.02,
        "600": 0.0,
    }

    stoploss = -0.06
    trailing_stop = True
    trailing_stop_positive = 0.015
    trailing_stop_positive_offset = 0.03
    trailing_only_offset_is_reached = True

    use_exit_signal = True
    exit_profit_only = False

    # ── Hyperopt parameters (classic Ichimoku periods) ───────────────────────
    tenkan_period_p   = IntParameter(6, 14, default=9, space="buy", optimize=True)
    kijun_period_p    = IntParameter(20, 34, default=26, space="buy", optimize=True)
    senkou_b_period_p = IntParameter(40, 64, default=52, space="buy", optimize=True)
    displacement_p    = IntParameter(20, 30, default=26, space="buy", optimize=True)

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

        # ── Custom: the Ichimoku system (all pandas) ──────────────────────────
        disp = self.displacement_p.value

        dataframe["tenkan"] = _donchian_mid(dataframe, self.tenkan_period_p.value)
        dataframe["kijun"] = _donchian_mid(dataframe, self.kijun_period_p.value)

        # Leading spans, projected `disp` bars FORWARD (shift on past values,
        # so the value sitting at the current bar was derived `disp` bars ago).
        senkou_a = (dataframe["tenkan"] + dataframe["kijun"]) / 2.0
        senkou_b = _donchian_mid(dataframe, self.senkou_b_period_p.value)
        dataframe["senkou_a"] = senkou_a.shift(disp)
        dataframe["senkou_b"] = senkou_b.shift(disp)

        # The cloud band at the current bar.
        dataframe["cloud_top"] = dataframe[["senkou_a", "senkou_b"]].max(axis=1)
        dataframe["cloud_bottom"] = dataframe[["senkou_a", "senkou_b"]].min(axis=1)

        # Chikou-style confirmation reference: the close `disp` bars ago.
        dataframe["close_lag"] = dataframe["close"].shift(disp)

        return dataframe

    # -----------------------------------------------------------------------
    # Entry signals
    # -----------------------------------------------------------------------

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        tenkan = dataframe["tenkan"]
        kijun = dataframe["kijun"]

        tk_cross_up = (tenkan > kijun) & (tenkan.shift(1) <= kijun.shift(1))
        tk_cross_down = (tenkan < kijun) & (tenkan.shift(1) >= kijun.shift(1))

        # Long: bullish TK cross, price above the cloud, Chikou confirmation.
        dataframe.loc[
            tk_cross_up
            & (dataframe["close"] > dataframe["cloud_top"])
            & (dataframe["close"] > dataframe["close_lag"])
            & (dataframe["volume"] > 0),
            ["enter_long", "enter_tag"],
        ] = [1, "ichimoku_long"]

        # Short: bearish TK cross, price below the cloud, Chikou confirmation.
        dataframe.loc[
            tk_cross_down
            & (dataframe["close"] < dataframe["cloud_bottom"])
            & (dataframe["close"] < dataframe["close_lag"])
            & (dataframe["volume"] > 0),
            ["enter_short", "enter_tag"],
        ] = [1, "ichimoku_short"]

        return dataframe

    # -----------------------------------------------------------------------
    # Exit signals
    # -----------------------------------------------------------------------

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        tenkan = dataframe["tenkan"]
        kijun = dataframe["kijun"]

        tk_cross_up = (tenkan > kijun) & (tenkan.shift(1) <= kijun.shift(1))
        tk_cross_down = (tenkan < kijun) & (tenkan.shift(1) >= kijun.shift(1))

        # Long exit: momentum flips (TK cross down) or price loses the base line.
        dataframe.loc[
            tk_cross_down | (dataframe["close"] < kijun),
            "exit_long",
        ] = 1

        # Short exit: mirror image.
        dataframe.loc[
            tk_cross_up | (dataframe["close"] > kijun),
            "exit_short",
        ] = 1

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
            logger.debug(f"IchimokuCloud stoploss error for {pair}: {exc}")
            return None
