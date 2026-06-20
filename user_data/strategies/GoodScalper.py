"""
VulcanScalper — Regime-filtered scalping strategy with Van Tharp position sizing.

Entry:   EMA ribbon alignment (5/13/21) + RSI momentum + volume spike + BB position
Exit:    RSI reversal, BB touch, or EMA flip
Regime:  1h BacktestRegimeAnalyzer gate — sizes up in trending regimes, blocks
         counter-trend entries (e.g. no longs in EXTREME_BEAR)
Sizing:  Van Tharp PositionSizer: 1% base risk, scaled by equity curve state,
         streak state (RMultipleTracker), and regime multiplier
Stop:    Chandelier ATR trailing stop from best price (same as AlphaHunterV5)
"""

import logging
from collections import deque
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import talib.abstract as ta
from pandas import DataFrame

from VulcanTrader.persistence.trade_model import Trade
from VulcanTrader.strategy import (
    DecimalParameter,
    IntParameter,
    IStrategy,
    merge_informative_pair,
)
from VulcanTrader.regime_analysis import BacktestRegimeAnalyzer
from user_data.strategies.risk_management import (
    AsymmetricLeverageClassifier,
    ExpectancyCalculator,
    PositionSizer,
    RMultipleTracker,
    VanTharpeLabels,
)

logger = logging.getLogger(__name__)

pd.set_option("future.no_silent_downcasting", True)


# ---------------------------------------------------------------------------
# Regime size multipliers per direction.
# 0.0 = block this side entirely in this regime.
# ---------------------------------------------------------------------------
_REGIME_MULT: dict[str, dict[str, float]] = {
    "EXTREME_BULL": {"long": 1.4, "short": 0.0},
    "BULL":         {"long": 1.1, "short": 0.3},
    "RANGING":      {"long": 0.55, "short": 0.55},
    "BEAR":         {"long": 0.3, "short": 1.1},
    "EXTREME_BEAR": {"long": 0.0, "short": 1.4},
}


class GoodScalper(IStrategy):
    """
    Scalper on 5m with 1h regime filter and Van Tharp R-based sizing.

    Position sizing hierarchy (all multiplied):
        base_risk_pct  (1% default)
        × regime_mult  (from 1h market regime, 0–1.4×)
        × ec_mult      (equity curve state, 0.10–1.30×)
        × exp_mult     (rolling expectancy + SQN, 0.05–1.40× — warm-up to 1.0 until 20 trades)
        = final_risk_pct → stake = equity × risk / stop_distance / leverage
    """

    INTERFACE_VERSION = 3

    timeframe = "5m"
    informative_timeframe = "1h"

    can_short: bool = True

    # ROI: quick scalper targets, let trailing stop handle the runner
    minimal_roi = {
        "0":   0.06,   # 6% anytime
        "30":  0.04,   # 4% after 30 min
        "90":  0.025,  # 2.5% after 90 min
        "180": 0.015,  # 1.5% after 3 h
    }

    stoploss = -0.05  # 5% hard floor (chandelier usually kicks in tighter)
    trailing_stop = True
    trailing_stop_positive = 0.005       # activate 0.5% trail once in profit
    trailing_stop_positive_offset = 0.012  # activates at +1.2%
    trailing_only_offset_is_reached = True

    use_exit_signal = True
    exit_profit_only = False
    process_only_new_candles = True
    startup_candle_count: int = 60

    # ---- Hyperopt parameters -----------------------------------------------

    # Entry / trend
    ema_fast_p = IntParameter(3, 8, default=5, space="buy", optimize=True)
    ema_mid_p  = IntParameter(8, 21, default=13, space="buy", optimize=True)
    ema_slow_p = IntParameter(15, 34, default=21, space="buy", optimize=True)

    rsi_period_p    = IntParameter(7, 14, default=9, space="buy", optimize=True)
    rsi_entry_long  = IntParameter(50, 72, default=65, space="buy", optimize=True)
    rsi_entry_short = IntParameter(28, 50, default=35, space="sell", optimize=True)

    volume_ratio_p = DecimalParameter(0.8, 2.0, default=1.2, decimals=1,
                                      space="buy", optimize=True)

    # Stop / ATR
    atr_period_p   = IntParameter(7, 21, default=14, space="buy", optimize=True)
    atr_stop_mult  = DecimalParameter(0.8, 2.5, default=1.5, decimals=1,
                                      space="buy", optimize=True)
    chandelier_mult = DecimalParameter(1.0, 3.5, default=2.0, decimals=1,
                                       space="buy", optimize=True)

    # Exit
    rsi_exit_long  = IntParameter(65, 85, default=75, space="sell", optimize=True)
    rsi_exit_short = IntParameter(15, 35, default=25, space="sell", optimize=True)

    # Van Tharp risk constants (not hyperopt — tuned separately)
    BASE_RISK_PCT = 0.01   # 1% equity per trade
    MAX_RISK_PCT  = 0.03   # 3% hard cap
    MIN_RISK_PCT  = 0.002  # 0.2% floor
    MAX_POS_PCT   = 0.25   # 25% position cap

    # Warmup: use plain fixed-fractional until this many trades complete
    WARMUP_TRADES = 20
    WARMUP_RISK_PCT = 0.005  # 0.5% during warmup

    def __init__(self, config: dict) -> None:
        super().__init__(config)

        # Van Tharp components
        self._position_sizer = PositionSizer(
            base_risk_pct=self.BASE_RISK_PCT,
            max_risk_pct=self.MAX_RISK_PCT,
            min_risk_pct=self.MIN_RISK_PCT,
            max_position_pct=self.MAX_POS_PCT,
        )
        self._r_tracker = RMultipleTracker(max_trades=100)

        # Equity curve state tracking
        self._peak_equity: float = 0.0

        # Initial stop cache: trade_id -> stop_price (for R computation on close)
        self._initial_stops: dict[str, float] = {}

    # -----------------------------------------------------------------------
    # Informative pairs
    # -----------------------------------------------------------------------

    def informative_pairs(self):
        pairs = self.dp.current_whitelist()
        return [(pair, self.informative_timeframe) for pair in pairs]

    # -----------------------------------------------------------------------
    # Indicators
    # -----------------------------------------------------------------------

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        pair = metadata["pair"]

        # ── 1h regime ──────────────────────────────────────────────────────
        informative = self.dp.get_pair_dataframe(pair=pair,
                                                  timeframe=self.informative_timeframe)
        if len(informative) >= 55:
            rdf = BacktestRegimeAnalyzer.classify_regime(informative)
            rdf = rdf[["date", "regime"]].copy().sort_values("date")
        else:
            rdf = pd.DataFrame({"date": informative["date"] if len(informative) else [],
                                 "regime": "RANGING"})

        # Forward-fill 1h regime into each 5m candle via merge_asof
        df_dates = dataframe[["date"]].copy()
        merged = pd.merge_asof(
            df_dates.sort_values("date"),
            rdf,
            on="date",
            direction="backward",
        )
        dataframe["regime"] = merged["regime"].fillna("RANGING").values

        # ── EMA ribbon ─────────────────────────────────────────────────────
        dataframe["ema_fast"] = ta.EMA(dataframe, timeperiod=self.ema_fast_p.value)
        dataframe["ema_mid"]  = ta.EMA(dataframe, timeperiod=self.ema_mid_p.value)
        dataframe["ema_slow"] = ta.EMA(dataframe, timeperiod=self.ema_slow_p.value)

        # ── RSI ────────────────────────────────────────────────────────────
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=self.rsi_period_p.value)

        # ── ATR (stop sizing + chandelier) ─────────────────────────────────
        dataframe["atr"] = ta.ATR(dataframe, timeperiod=self.atr_period_p.value)

        # ── Bollinger Bands (20, 2σ) ────────────────────────────────────────
        bb_upper, bb_mid, bb_lower = ta.BBANDS(
            dataframe["close"], timeperiod=20, nbdevup=2.0, nbdevdn=2.0
        )
        dataframe["bb_upper"] = bb_upper
        dataframe["bb_mid"]   = bb_mid
        dataframe["bb_lower"] = bb_lower
        denom = np.where(bb_upper - bb_lower == 0, np.nan, bb_upper - bb_lower)
        dataframe["bb_pct"] = (dataframe["close"] - bb_lower) / denom

        # ── Volume ratio ────────────────────────────────────────────────────
        dataframe["vol_ma"]    = dataframe["volume"].rolling(20).mean()
        dataframe["vol_ratio"] = (dataframe["volume"] /
                                   dataframe["vol_ma"].replace(0, np.nan)).fillna(1.0)

        return dataframe

    # -----------------------------------------------------------------------
    # Entry signals
    # -----------------------------------------------------------------------

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        long_regimes  = ["EXTREME_BULL", "BULL", "RANGING"]
        short_regimes = ["EXTREME_BEAR", "BEAR", "RANGING"]

        # Long: bullish EMA stack + RSI building momentum (not yet overbought) + volume + price above BB mid
        long_cond = (
            dataframe["regime"].isin(long_regimes)
            & (dataframe["ema_fast"] > dataframe["ema_mid"])
            & (dataframe["ema_mid"]  > dataframe["ema_slow"])
            & (dataframe["rsi"] > 40)
            & (dataframe["rsi"] < self.rsi_entry_long.value)
            & (dataframe["vol_ratio"] >= self.volume_ratio_p.value)
            & (dataframe["close"] > dataframe["bb_mid"])
            & (dataframe["volume"] > 0)
        )
        dataframe.loc[long_cond, ["enter_long", "enter_tag"]] = [1, "ema_bull"]

        # Short: bearish EMA stack + RSI building downward momentum (not yet oversold) + volume + price below BB mid
        short_cond = (
            dataframe["regime"].isin(short_regimes)
            & (dataframe["ema_fast"] < dataframe["ema_mid"])
            & (dataframe["ema_mid"]  < dataframe["ema_slow"])
            & (dataframe["rsi"] < 60)
            & (dataframe["rsi"] > self.rsi_entry_short.value)
            & (dataframe["vol_ratio"] >= self.volume_ratio_p.value)
            & (dataframe["close"] < dataframe["bb_mid"])
            & (dataframe["volume"] > 0)
        )
        dataframe.loc[short_cond, ["enter_short", "enter_tag"]] = [1, "ema_bear"]

        return dataframe

    # -----------------------------------------------------------------------
    # Exit signals
    # -----------------------------------------------------------------------

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Long exit: RSI overbought OR price hits BB upper (trailing stop handles EMA flip)
        dataframe.loc[
            (dataframe["rsi"] > self.rsi_exit_long.value)
            | (dataframe["close"] >= dataframe["bb_upper"]),
            "exit_long",
        ] = 1

        # Short exit: RSI oversold OR price hits BB lower (trailing stop handles EMA flip)
        dataframe.loc[
            (dataframe["rsi"] < self.rsi_exit_short.value)
            | (dataframe["close"] <= dataframe["bb_lower"]),
            "exit_short",
        ] = 1

        return dataframe

    # -----------------------------------------------------------------------
    # Leverage
    # -----------------------------------------------------------------------

    def leverage(self, pair: str, current_time: datetime, current_rate: float,
                 proposed_leverage: float, max_leverage: float,
                 entry_tag: Optional[str], side: str, **kwargs) -> float:
        return min(5.0, max_leverage)

    # -----------------------------------------------------------------------
    # Chandelier ATR stop (trailing from best price seen in trade)
    # -----------------------------------------------------------------------

    def custom_stoploss(self, pair: str, trade: Trade, current_time: datetime,
                        current_rate: float, current_profit: float,
                        after_fill: bool, **kwargs) -> Optional[float]:
        try:
            dataframe, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
            if dataframe is None or len(dataframe) < 2:
                return None

            atr = float(dataframe.iloc[-1].get("atr", np.nan))
            if np.isnan(atr) or atr <= 0 or trade.open_rate <= 0:
                return None

            mult = float(self.chandelier_mult.value)

            if not trade.is_short:
                best   = float(getattr(trade, "max_rate", trade.open_rate))
                level  = best - mult * atr
                sl     = (level / current_rate) - 1.0
            else:
                best   = float(getattr(trade, "min_rate", trade.open_rate))
                level  = best + mult * atr
                sl     = 1.0 - (level / current_rate)

            # Only provide downside protection; ignore when chandelier is above price
            if sl >= 0:
                return None

            # Never wider than hard stoploss
            return max(self.stoploss, sl)

        except Exception as exc:
            logger.debug(f"VulcanScalper custom_stoploss error for {pair}: {exc}")
            return None

    # -----------------------------------------------------------------------
    # Van Tharp position sizing
    # -----------------------------------------------------------------------

    def _ec_state(self, equity: float) -> str:
        """Classify equity-curve state and update peak."""
        if equity > self._peak_equity:
            self._peak_equity = equity
        return AsymmetricLeverageClassifier.classify_equity_curve_state(
            current_equity=equity,
            peak_equity=self._peak_equity,
        )

    def _exp_labels(self) -> tuple[str, str]:
        """Return (expectancy_label, sqn_label) from rolling R history."""
        rs = self._r_tracker.get_recent_r_list(30)
        if len(rs) < 5:
            # Too few trades — use neutral defaults
            return VanTharpeLabels.EXP_GOOD, VanTharpeLabels.SQN_AVERAGE
        exp = ExpectancyCalculator.calculate_expectancy(rs)
        sqn = ExpectancyCalculator.calculate_sqn(rs)
        return exp["expectancy_label"], sqn["sqn_label"]

    def custom_stake_amount(self, pair: str, current_time: datetime,
                            current_rate: float, proposed_stake: float,
                            min_stake: Optional[float], max_stake: float,
                            leverage: float, entry_tag: Optional[str],
                            side: str, **kwargs) -> float:
        try:
            equity = self.wallets.get_total_stake_amount()
            if equity <= 0:
                return proposed_stake

            dataframe, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
            if dataframe is None or dataframe.empty:
                return proposed_stake

            last   = dataframe.iloc[-1]
            atr    = float(last.get("atr", np.nan))
            regime = str(last.get("regime", "RANGING"))

            if np.isnan(atr) or atr <= 0:
                return proposed_stake

            # ATR-based initial stop distance
            stop_dist     = atr * float(self.atr_stop_mult.value)
            direction     = 1 if side == "long" else -1
            stop_price    = current_rate - direction * stop_dist

            # Regime multiplier for this side
            regime_mult = _REGIME_MULT.get(regime, {"long": 1.0, "short": 1.0}).get(side, 1.0)
            if regime_mult == 0.0:
                return min_stake or proposed_stake  # blocked — return minimum

            # Warmup: use simple fixed-fractional until WARMUP_TRADES completed
            n_trades = len(self._r_tracker.r_multiples)
            if n_trades < self.WARMUP_TRADES:
                notional = equity * self.WARMUP_RISK_PCT / (stop_dist / current_rate)
                stake    = notional / leverage if leverage > 0 else notional
                return max(min_stake or 0, min(stake, max_stake))

            # Full Van Tharp sizing
            ec_state      = self._ec_state(equity)
            streak_state  = self._r_tracker.get_streak_state()
            exp_lbl, sqn_lbl = self._exp_labels()

            sizing = self._position_sizer.calculate_position_size(
                equity=equity,
                entry_price=current_rate,
                stop_price=stop_price,
                direction=direction,
                regime_score={"combined_multiplier": regime_mult},
                ec_state=ec_state,
                streak_state=streak_state,
                expectancy_label=exp_lbl,
                sqn_label=sqn_lbl,
            )

            # Stake = margin required (notional / leverage for futures)
            notional = sizing["notional_position"]
            stake    = notional / leverage if leverage > 0 else notional

            return max(min_stake or 0, min(stake, max_stake))

        except Exception as exc:
            logger.warning(f"VulcanScalper stake sizing failed for {pair}: {exc}")
            return proposed_stake

    # -----------------------------------------------------------------------
    # Entry gate — block regime-blocked sides even if signal fires
    # -----------------------------------------------------------------------

    def confirm_trade_entry(self, pair: str, order_type: str, amount: float,
                            rate: float, time_in_force: str, current_time: datetime,
                            entry_tag: Optional[str], side: str, **kwargs) -> bool:
        try:
            dataframe, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
            if dataframe is None or dataframe.empty:
                return True

            last   = dataframe.iloc[-1]
            regime = str(last.get("regime", "RANGING"))
            mult   = _REGIME_MULT.get(regime, {"long": 1.0, "short": 1.0}).get(side, 1.0)

            if mult == 0.0:
                logger.debug(f"VulcanScalper: {pair} {side} blocked — regime {regime}")
                return False

            # Cache initial ATR stop for R-multiple calculation on close
            atr = float(last.get("atr", 0.0))
            if atr > 0:
                direction = 1 if side == "long" else -1
                stop = rate - direction * atr * float(self.atr_stop_mult.value)
                self._initial_stops[pair] = stop

            return True

        except Exception:
            return True

    # -----------------------------------------------------------------------
    # Exit hook — update R-tracker for position sizing calibration
    # -----------------------------------------------------------------------

    def confirm_trade_exit(self, pair: str, trade: Trade, order_type: str,
                           amount: float, rate: float, time_in_force: str,
                           exit_reason: str, current_time: datetime, **kwargs) -> bool:
        try:
            stop_price = self._initial_stops.pop(pair, None)
            if stop_price is None:
                # Fallback: approximate stop from hard stoploss
                stop_price = trade.open_rate * (1 + self.stoploss)

            direction = -1 if trade.is_short else 1
            r = RMultipleTracker.calculate_r_multiple(
                entry=float(trade.open_rate),
                exit_price=float(rate),
                stop=float(stop_price),
                direction=direction,
            )
            self._r_tracker.add_trade(r, pd.Timestamp(current_time))
            logger.debug(f"VulcanScalper: {pair} closed {exit_reason} R={r:.2f} "
                         f"(total trades={len(self._r_tracker.r_multiples)})")
        except Exception as exc:
            logger.debug(f"VulcanScalper: R-tracker update failed for {pair}: {exc}")

        return True
