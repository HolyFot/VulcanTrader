# flake8: noqa
# ruff: noqa
# isort: skip_file
"""
Price Distribution Regime & Position Sizing Module
===================================================

Universal distribution-based regime labels, classifiers, and a full
Van Tharp position-sizing stack for market analysis.

Module contents
---------------
**Label constants**
    RegimeLabels, TrendLabels, ADXDistributionLabels, SlopeDistributionLabels,
    DISpreadLabels, HTFAlignmentLabels, PerformanceRegime, VanTharpeLabels

**Classifiers**
    RegimeClassifier          – price-distribution percentile zones
    TrendClassifier           – EMA alignment / slope / HH-LL trend regimes
    DistributionRegimeClassifier – ADX, slope, DI-spread, HTF alignment regimes

**Equity Curve & MAE/MFE**
    EquityCurveSizing         – RSI-of-equity performance regime tracker
    MAEMFECalculator          – MAE/MFE percentile calculator from closed trades

**Regime-Informed Exits**
    RegimeExitCalculator      – regime-aware stop-loss / take-profit calculator

**Pair & Liquidity Filters**
    LiquidityTierManager      – per-pair stake caps by liquidity tier
    PairFilter                – multi-factor momentum pair scoring / ranking

**Risk Engine**
    HawkesRiskEngine          – self-exciting Hawkes process for event clustering

**Indicators**
    IndicatorHelpers          – ADX, ATR, RSI, EMA, distribution-regime helpers

**Van Tharp Framework**
    RMultipleTracker          – R-multiple trade logging & streak detection
    ExpectancyCalculator      – expectancy, SQN, opportunity-adjusted metrics
    AsymmetricLeverageClassifier – equity-curve & expectancy multipliers

**Position Sizing**
    PositionSizer             – regime × EC × expectancy sizing + consensus blending
    AsymmetricExpansionSizer  – asymmetric expand/contract sizing with HWM tracking
    BlendedPositionSizer      – multi-method blended sizer with ECS, MAE/MFE,
                                liquidity tiers, and trade-state tracking

Strategy-agnostic: usable by both Momentum and Mean Reversion strategies.

Usage
-----
::

    from regime_ecs_mixin import (
        # Labels
        RegimeLabels, TrendLabels, ADXDistributionLabels,
        SlopeDistributionLabels, DISpreadLabels, HTFAlignmentLabels,
        PerformanceRegime, VanTharpeLabels,
        # Classifiers
        RegimeClassifier, TrendClassifier, DistributionRegimeClassifier,
        # ECS / MAE-MFE / Exits
        EquityCurveSizing, MAEMFECalculator, RegimeExitCalculator,
        # Filters
        LiquidityTierManager, PairFilter,
        # Risk
        HawkesRiskEngine,
        # Indicators
        IndicatorHelpers,
        # Van Tharp / Sizing
        RMultipleTracker, ExpectancyCalculator,
        AsymmetricLeverageClassifier,
        PositionSizer, AsymmetricExpansionSizer, BlendedPositionSizer,
    )
"""

import logging
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional, Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd
from pandas import DataFrame

if TYPE_CHECKING:
    from freqtrade.persistence import Trade

logger = logging.getLogger(__name__)


# =============================================================================
# REGIME LABELS - Universal Distribution-Based Zones
# =============================================================================
class RegimeLabels:
    """
    Universal distribution-based regime labels.
    
    These represent where price sits within its own statistical distribution:
    
    Distribution-Based Regime Zones:
    - EXTREME_TAIL (<P3 / >P97): Structural break zone - rare events
    - TAIL (P3-P10 / P90-P97): Extended move - caution zone  
    - OUTER_SHOULDER (P10-P20 / P80-P90): Prime reversal/momentum zone
    - INNER_SHOULDER (P20-P35 / P65-P80): Early signal zone
    - CORE (P35-P65): Neutral/balanced zone
    
    Interpretation varies by strategy type:
    - Mean Reversion: OUTER_SHOULDER = entry zone, CORE = exit target
    - Momentum: OUTER_SHOULDER = momentum building, EXTREME_TAIL = strong trend
    """
    # Universal zone labels
    EXTREME_TAIL = "EXTREME_TAIL"
    TAIL = "TAIL"  
    OUTER_SHOULDER = "OUTER_SHOULDER"
    INNER_SHOULDER = "INNER_SHOULDER"
    CORE = "CORE"
    
    # Mean Reversion specific interpretations
    MR_CORE = "CORE_MR"
    MR_INNER_SHOULDER = "INNER_SHOULDER"
    MR_OUTER_SHOULDER = "OUTER_SHOULDER"
    MR_TAIL = "TAIL_EXPANSION"
    MR_EXTREME = "EXTREME_BREAK"
    
    # Momentum specific interpretations
    MOM_STRONG_TREND = "STRONG_TREND"
    MOM_BUILDING_TREND = "BUILDING_TREND"
    MOM_EARLY_TREND = "EARLY_TREND"
    MOM_CONSOLIDATION = "CONSOLIDATION"


# =============================================================================
# TREND REGIME LABELS
# =============================================================================
class TrendLabels:
    """
    Trend-based regime labels for directional classification.
    
    These represent the overall trend direction and strength:
    
    Trend Regime Zones:
    - STRONG_UPTREND: Clear bullish trend with high ADX, price above MAs
    - UPTREND: Moderate bullish bias, price above key MAs
    - WEAK_UPTREND: Early/weakening uptrend signals
    - RANGING: No clear direction, choppy price action
    - WEAK_DOWNTREND: Early/weakening downtrend signals
    - DOWNTREND: Moderate bearish bias, price below key MAs
    - STRONG_DOWNTREND: Clear bearish trend with high ADX, price below MAs
    
    Combined with distribution regimes for complete market context:
    - STRONG_UPTREND + EXTREME_TAIL = Extended rally, caution for longs
    - STRONG_DOWNTREND + EXTREME_TAIL = Capitulation, watch for reversal
    - RANGING + CORE = True consolidation, range trading ideal
    """
    # Core trend labels
    STRONG_UPTREND = "STRONG_UPTREND"
    UPTREND = "UPTREND"
    WEAK_UPTREND = "WEAK_UPTREND"
    RANGING = "RANGING"
    WEAK_DOWNTREND = "WEAK_DOWNTREND"
    DOWNTREND = "DOWNTREND"
    STRONG_DOWNTREND = "STRONG_DOWNTREND"
    
    # Transition states
    TREND_REVERSAL_UP = "TREND_REVERSAL_UP"      # Transitioning from down to up
    TREND_REVERSAL_DOWN = "TREND_REVERSAL_DOWN"  # Transitioning from up to down
    TREND_EXHAUSTION = "TREND_EXHAUSTION"        # Trend losing momentum
    
    # Volatility-adjusted trend states
    VOLATILE_UPTREND = "VOLATILE_UPTREND"        # Uptrend with high volatility
    VOLATILE_DOWNTREND = "VOLATILE_DOWNTREND"    # Downtrend with high volatility
    QUIET_TREND = "QUIET_TREND"                  # Trend with low volatility


# =============================================================================
# ADX DISTRIBUTION REGIME LABELS
# =============================================================================
class ADXDistributionLabels:
    """
    ADX (trend strength) distribution-based regime labels.
    
    ADX measures trend strength regardless of direction.
    Distribution percentiles tell us if current ADX is high/low relative to recent history.
    
    Regime Zones:
    - EXTREME_TRENDING (>P90): ADX at historical highs - very strong trend, may be exhausting
    - HIGH_TRENDING (P75-P90): Strong trend conditions - momentum strategies thrive
    - MODERATE_TRENDING (P50-P75): Normal trending - decent directional moves
    - LOW_TRENDING (P25-P50): Below average trend strength - mixed signals
    - CHOPPY (P10-P25): Low ADX - ranging/choppy market
    - EXTREME_CHOPPY (<P10): Historically low ADX - avoid trend strategies
    
    Usage:
    - Momentum: Enter on HIGH_TRENDING, avoid CHOPPY
    - Mean Reversion: Enter on CHOPPY/LOW_TRENDING, avoid EXTREME_TRENDING
    - Exit momentum trades when ADX drops from HIGH to LOW
    """
    EXTREME_TRENDING = "ADX_EXTREME_TRENDING"    # >P90 - Trend may exhaust
    HIGH_TRENDING = "ADX_HIGH_TRENDING"          # P75-P90 - Strong trend
    MODERATE_TRENDING = "ADX_MODERATE_TRENDING"  # P50-P75 - Normal trend
    LOW_TRENDING = "ADX_LOW_TRENDING"            # P25-P50 - Weak trend
    CHOPPY = "ADX_CHOPPY"                        # P10-P25 - Ranging market
    EXTREME_CHOPPY = "ADX_EXTREME_CHOPPY"        # <P10 - Very choppy


# =============================================================================
# SLOPE DISTRIBUTION REGIME LABELS
# =============================================================================
class SlopeDistributionLabels:
    """
    Price/MA slope distribution-based regime labels.
    
    Slope measures the rate of change/momentum of price or moving averages.
    Distribution percentiles tell us if current slope is steep/flat vs history.
    
    Regime Zones:
    - EXTREME_STEEP_UP (>P95): Parabolic move up - often unsustainable
    - STEEP_UP (P80-P95): Strong upward momentum
    - MODERATE_UP (P60-P80): Healthy uptrend slope
    - FLAT (P40-P60): Minimal slope - consolidation
    - MODERATE_DOWN (P20-P40): Healthy downtrend slope
    - STEEP_DOWN (P5-P20): Strong downward momentum
    - EXTREME_STEEP_DOWN (<P5): Capitulation/crash - often unsustainable
    
    Usage:
    - Momentum: Enter on MODERATE slopes, trail stops on STEEP, exit on EXTREME
    - Mean Reversion: Look for entries on EXTREME slopes (reversal candidates)
    - Combine with ADX: STEEP slope + HIGH ADX = strong trend
    """
    EXTREME_STEEP_UP = "SLOPE_EXTREME_UP"      # >P95 - Parabolic up
    STEEP_UP = "SLOPE_STEEP_UP"                # P80-P95 - Strong up
    MODERATE_UP = "SLOPE_MODERATE_UP"          # P60-P80 - Healthy up
    FLAT = "SLOPE_FLAT"                        # P40-P60 - Consolidation
    MODERATE_DOWN = "SLOPE_MODERATE_DOWN"      # P20-P40 - Healthy down
    STEEP_DOWN = "SLOPE_STEEP_DOWN"            # P5-P20 - Strong down
    EXTREME_STEEP_DOWN = "SLOPE_EXTREME_DOWN"  # <P5 - Capitulation


# =============================================================================
# DI SPREAD DISTRIBUTION REGIME LABELS
# =============================================================================
class DISpreadLabels:
    """
    Directional Index spread (+DI - -DI) distribution-based regime labels.
    
    DI Spread measures the balance between buying and selling pressure.
    Positive spread = bulls dominating, Negative spread = bears dominating.
    Distribution percentiles show if current directional pressure is extreme.
    
    Regime Zones:
    - EXTREME_BULLISH (>P95): +DI dominance at historical highs
    - STRONG_BULLISH (P80-P95): Clear bullish pressure
    - MODERATE_BULLISH (P60-P80): Mild bullish edge
    - NEUTRAL (P40-P60): Balanced pressure
    - MODERATE_BEARISH (P20-P40): Mild bearish edge
    - STRONG_BEARISH (P5-P20): Clear bearish pressure
    - EXTREME_BEARISH (<P5): -DI dominance at historical highs
    
    Usage:
    - Confirms trend direction with strength
    - EXTREME zones often precede reversals
    - Divergence: Price making highs but DI spread falling = weakness
    """
    EXTREME_BULLISH = "DI_EXTREME_BULLISH"      # >P95
    STRONG_BULLISH = "DI_STRONG_BULLISH"        # P80-P95
    MODERATE_BULLISH = "DI_MODERATE_BULLISH"    # P60-P80
    NEUTRAL = "DI_NEUTRAL"                       # P40-P60
    MODERATE_BEARISH = "DI_MODERATE_BEARISH"    # P20-P40
    STRONG_BEARISH = "DI_STRONG_BEARISH"        # P5-P20
    EXTREME_BEARISH = "DI_EXTREME_BEARISH"      # <P5


# =============================================================================
# HTF ALIGNMENT SCORE DISTRIBUTION LABELS
# =============================================================================
class HTFAlignmentLabels:
    """
    Higher Timeframe Alignment Score distribution-based regime labels.
    
    HTF Alignment measures how well current price action aligns with higher
    timeframe trends. Score combines multiple HTF signals (MA alignment, 
    trend direction, momentum).
    
    Regime Zones:
    - PERFECT_ALIGNMENT (>P90): All timeframes aligned - high conviction
    - STRONG_ALIGNMENT (P75-P90): Most timeframes agree - good setup
    - MODERATE_ALIGNMENT (P50-P75): Mixed signals - reduced conviction
    - WEAK_ALIGNMENT (P25-P50): Timeframes diverging - caution
    - CONFLICTING (P10-P25): HTF and LTF disagree - avoid or counter-trend
    - EXTREME_CONFLICT (<P10): Strong HTF opposition - counter-trend territory
    
    Usage:
    - Momentum: Only enter on STRONG+ alignment, reduce size on MODERATE
    - Mean Reversion: Can trade CONFLICTING zones for counter-trend plays
    - Use for position sizing: Full size on PERFECT, half on MODERATE
    """
    PERFECT_ALIGNMENT = "HTF_PERFECT"           # >P90 - All TFs aligned
    STRONG_ALIGNMENT = "HTF_STRONG"             # P75-P90 - Most TFs agree
    MODERATE_ALIGNMENT = "HTF_MODERATE"         # P50-P75 - Mixed
    WEAK_ALIGNMENT = "HTF_WEAK"                 # P25-P50 - Diverging
    CONFLICTING = "HTF_CONFLICTING"             # P10-P25 - HTF disagrees
    EXTREME_CONFLICT = "HTF_EXTREME_CONFLICT"   # <P10 - Strong HTF opposition




# =============================================================================
# REGIME CLASSIFIER
# =============================================================================

class RegimeClassifier:
    """
    Classifies market regime based on price distribution percentiles.
    
    Strategy-agnostic - returns universal zone labels that strategies
    can interpret according to their trading philosophy.
    """
    
    @staticmethod
    def classify_distribution_regime(
        pctl: pd.Series,
        extreme_threshold: int = 3,
        tail_lower: int = 10,
        tail_upper: int = 90,
        outer_shoulder_inner: int = 20,
        outer_shoulder_outer: int = 80,
        inner_shoulder_inner: int = 35,
        inner_shoulder_outer: int = 65,
        core_lower: int = 35,
        core_upper: int = 65
    ) -> pd.Series:
        """
        Classify regime based on percentile position in distribution.
        
        Args:
            pctl: Series of percentile values (0-100)
            extreme_threshold: Percentile for extreme tails (default P3/P97)
            tail_lower/upper: Tail zone thresholds
            outer_shoulder_inner/outer: Outer shoulder zone thresholds
            inner_shoulder_inner/outer: Inner shoulder zone thresholds
            core_lower/upper: Core zone thresholds
            
        Returns:
            Series of regime labels
        """
        conditions = [
            # EXTREME_TAIL: <P3 or >P97
            (pctl < extreme_threshold) | (pctl > (100 - extreme_threshold)),
            
            # TAIL: P3-P10 or P90-P97
            ((pctl >= extreme_threshold) & (pctl < tail_lower)) |
            ((pctl > tail_upper) & (pctl <= (100 - extreme_threshold))),
            
            # OUTER_SHOULDER: P10-P20 or P80-P90
            ((pctl >= tail_lower) & (pctl < outer_shoulder_inner)) |
            ((pctl > outer_shoulder_outer) & (pctl <= tail_upper)),
            
            # INNER_SHOULDER: P20-P35 or P65-P80
            ((pctl >= outer_shoulder_inner) & (pctl < inner_shoulder_inner)) |
            ((pctl > inner_shoulder_outer) & (pctl <= outer_shoulder_outer)),
            
            # CORE: P35-P65
            (pctl >= core_lower) & (pctl <= core_upper)
        ]
        
        choices = [
            RegimeLabels.EXTREME_TAIL,
            RegimeLabels.TAIL,
            RegimeLabels.OUTER_SHOULDER,
            RegimeLabels.INNER_SHOULDER,
            RegimeLabels.CORE
        ]
        
        return np.select(conditions, choices, default=RegimeLabels.CORE)
    
    @staticmethod
    def classify_mr_regime(
        pctl: pd.Series,
        extreme_pctl: int = 3,
        tail_pctl_lower: int = 10,
        tail_pctl_upper: int = 90,
        entry_pctl_long: int = 25,
        entry_pctl_short: int = 75,
        core_lower: int = 35,
        core_upper: int = 65
    ) -> pd.Series:
        """
        Classify Mean Reversion specific regime.
        
        Mean Reversion interpretation:
        - CORE_MR: P35-P65 - stable MR zone, exit target
        - INNER_SHOULDER: P20-P35 / P65-P80 - fading edge, early partials
        - OUTER_SHOULDER: P10-P20 / P80-P90 - prime entry zone
        - TAIL_EXPANSION: P3-P10 / P90-P97 - no new entries, aggressive TPs
        - EXTREME_BREAK: <P3 / >P97 - structural move, exit all
        """
        conditions = [
            # EXTREME_BREAK
            (pctl < extreme_pctl) | (pctl > (100 - extreme_pctl)),
            
            # TAIL_EXPANSION
            ((pctl >= extreme_pctl) & (pctl < tail_pctl_lower)) |
            ((pctl > tail_pctl_upper) & (pctl <= (100 - extreme_pctl))),
            
            # OUTER_SHOULDER
            ((pctl >= tail_pctl_lower) & (pctl < entry_pctl_long)) |
            ((pctl > entry_pctl_short) & (pctl <= tail_pctl_upper)),
            
            # INNER_SHOULDER
            ((pctl >= entry_pctl_long) & (pctl < core_lower)) |
            ((pctl > core_upper) & (pctl <= entry_pctl_short)),
            
            # CORE_MR (default)
            (pctl >= core_lower) & (pctl <= core_upper)
        ]
        
        choices = [
            RegimeLabels.MR_EXTREME,
            RegimeLabels.MR_TAIL,
            RegimeLabels.MR_OUTER_SHOULDER,
            RegimeLabels.MR_INNER_SHOULDER,
            RegimeLabels.MR_CORE
        ]
        
        return np.select(conditions, choices, default=RegimeLabels.MR_CORE)
    
    @staticmethod
    def classify_momentum_regime(
        pctl: pd.Series,
        adx: pd.Series,
        strong_upper: int = 90,
        strong_lower: int = 10,
        entry_long: int = 80,
        entry_short: int = 20,
        early_upper: int = 65,
        early_lower: int = 35,
        consolidation_lower: int = 35,
        consolidation_upper: int = 65,
        adx_trending: int = 25,
        adx_strong: int = 40
    ) -> pd.Series:
        """
        Classify Momentum specific regime.
        
        Momentum interpretation (opposite of MR):
        - STRONG_TREND: P90+ / P10- with high ADX - ride the momentum
        - BUILDING_TREND: P80-P90 / P10-P20 with trending ADX - prime entry
        - EARLY_TREND: P65-P80 / P20-P35 - confirmation needed
        - CONSOLIDATION: P35-P65 or low ADX - no momentum, avoid
        """
        conditions = [
            # STRONG_TREND (EXTREME_TAIL + high ADX)
            ((pctl >= strong_upper) | (pctl <= strong_lower)) &
            (adx >= adx_strong),
            
            # BUILDING_TREND (OUTER_SHOULDER + trending ADX)
            (((pctl >= entry_long) & (pctl < strong_upper)) |
             ((pctl <= entry_short) & (pctl > strong_lower))) &
            (adx >= adx_trending),
            
            # EARLY_TREND (INNER_SHOULDER)
            (((pctl >= early_upper) & (pctl < entry_long)) |
             ((pctl <= early_lower) & (pctl > entry_short))),
            
            # CONSOLIDATION (CORE or low ADX)
            (pctl >= consolidation_lower) & (pctl <= consolidation_upper)
        ]
        
        choices = [
            RegimeLabels.MOM_STRONG_TREND,
            RegimeLabels.MOM_BUILDING_TREND,
            RegimeLabels.MOM_EARLY_TREND,
            RegimeLabels.MOM_CONSOLIDATION
        ]
        
        return np.select(conditions, choices, default=RegimeLabels.MOM_CONSOLIDATION)


# =============================================================================
# TREND CLASSIFIER
# =============================================================================
class TrendClassifier:
    """
    Classifies market trend regime based on multiple indicators.
    
    Uses a combination of:
    - EMA alignment (fast/medium/slow)
    - Price position relative to MAs
    - ADX for trend strength
    - Slope of moving averages
    - Higher highs/lower lows pattern
    """
    
    @staticmethod
    def classify_trend_regime(
        close: pd.Series,
        ema_fast: pd.Series,
        ema_medium: pd.Series,
        ema_slow: pd.Series,
        adx: pd.Series,
        adx_strong: int = 40,
        adx_trending: int = 25,
        adx_weak: int = 15
    ) -> pd.Series:
        """
        Classify trend regime based on EMA alignment and ADX.
        
        Args:
            close: Close price series
            ema_fast: Fast EMA (e.g., 8 or 12 period)
            ema_medium: Medium EMA (e.g., 21 or 26 period)
            ema_slow: Slow EMA (e.g., 50 or 200 period)
            adx: ADX values for trend strength
            adx_strong: ADX threshold for strong trend
            adx_trending: ADX threshold for moderate trend
            adx_weak: ADX threshold for weak trend
            
        Returns:
            Series of trend regime labels
        """
        # EMA alignment checks
        bullish_alignment = (ema_fast > ema_medium) & (ema_medium > ema_slow)
        bearish_alignment = (ema_fast < ema_medium) & (ema_medium < ema_slow)
        
        # Price above/below EMAs
        price_above_all = (close > ema_fast) & (close > ema_medium) & (close > ema_slow)
        price_below_all = (close < ema_fast) & (close < ema_medium) & (close < ema_slow)
        price_above_slow = close > ema_slow
        price_below_slow = close < ema_slow
        
        conditions = [
            # STRONG_UPTREND: Full bullish alignment + high ADX + price above all
            bullish_alignment & (adx >= adx_strong) & price_above_all,
            
            # STRONG_DOWNTREND: Full bearish alignment + high ADX + price below all
            bearish_alignment & (adx >= adx_strong) & price_below_all,
            
            # UPTREND: Bullish alignment OR price above slow with trending ADX
            (bullish_alignment & (adx >= adx_trending)) |
            (price_above_slow & (ema_fast > ema_slow) & (adx >= adx_trending)),
            
            # DOWNTREND: Bearish alignment OR price below slow with trending ADX
            (bearish_alignment & (adx >= adx_trending)) |
            (price_below_slow & (ema_fast < ema_slow) & (adx >= adx_trending)),
            
            # WEAK_UPTREND: Some bullish signals but weak ADX
            (price_above_slow | (ema_fast > ema_slow)) & 
            (adx >= adx_weak) & (adx < adx_trending),
            
            # WEAK_DOWNTREND: Some bearish signals but weak ADX
            (price_below_slow | (ema_fast < ema_slow)) & 
            (adx >= adx_weak) & (adx < adx_trending),
            
            # RANGING: Low ADX, no clear direction
            adx < adx_weak
        ]
        
        choices = [
            TrendLabels.STRONG_UPTREND,
            TrendLabels.STRONG_DOWNTREND,
            TrendLabels.UPTREND,
            TrendLabels.DOWNTREND,
            TrendLabels.WEAK_UPTREND,
            TrendLabels.WEAK_DOWNTREND,
            TrendLabels.RANGING
        ]
        
        return np.select(conditions, choices, default=TrendLabels.RANGING)
    
    @staticmethod
    def classify_trend_with_slope(
        close: pd.Series,
        ema_fast: pd.Series,
        ema_slow: pd.Series,
        adx: pd.Series,
        slope_period: int = 5,
        slope_threshold: float = 0.001,
        adx_trending: int = 25
    ) -> pd.Series:
        """
        Classify trend with slope analysis for trend strength.
        
        Args:
            close: Close price series
            ema_fast: Fast EMA series
            ema_slow: Slow EMA series
            adx: ADX values
            slope_period: Period for slope calculation
            slope_threshold: Minimum slope for trending (as % of price)
            adx_trending: ADX threshold for trending
            
        Returns:
            Series of trend regime labels
        """
        # Calculate slopes (normalized by price)
        fast_slope = (ema_fast - ema_fast.shift(slope_period)) / (close + 1e-10)
        slow_slope = (ema_slow - ema_slow.shift(slope_period)) / (close + 1e-10)
        
        # Strong slopes
        strong_up_slope = (fast_slope > slope_threshold * 2) & (slow_slope > slope_threshold)
        strong_down_slope = (fast_slope < -slope_threshold * 2) & (slow_slope < -slope_threshold)
        
        # Moderate slopes
        up_slope = fast_slope > slope_threshold
        down_slope = fast_slope < -slope_threshold
        
        # Price position
        price_above = close > ema_slow
        price_below = close < ema_slow
        
        conditions = [
            # STRONG_UPTREND
            strong_up_slope & price_above & (adx >= adx_trending),
            
            # STRONG_DOWNTREND
            strong_down_slope & price_below & (adx >= adx_trending),
            
            # UPTREND
            up_slope & price_above,
            
            # DOWNTREND
            down_slope & price_below,
            
            # WEAK_UPTREND
            price_above & ~down_slope,
            
            # WEAK_DOWNTREND
            price_below & ~up_slope,
        ]
        
        choices = [
            TrendLabels.STRONG_UPTREND,
            TrendLabels.STRONG_DOWNTREND,
            TrendLabels.UPTREND,
            TrendLabels.DOWNTREND,
            TrendLabels.WEAK_UPTREND,
            TrendLabels.WEAK_DOWNTREND,
        ]
        
        return np.select(conditions, choices, default=TrendLabels.RANGING)
    
    @staticmethod
    def classify_trend_with_hh_ll(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        lookback: int = 20,
        adx: Optional[pd.Series] = None,
        adx_trending: int = 25
    ) -> pd.Series:
        """
        Classify trend based on higher highs/lower lows pattern.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            lookback: Period for HH/LL detection
            adx: Optional ADX for strength confirmation
            adx_trending: ADX threshold
            
        Returns:
            Series of trend regime labels
        """
        # Rolling max/min
        rolling_high = high.rolling(lookback).max()
        rolling_low = low.rolling(lookback).min()
        
        # Previous rolling max/min
        prev_rolling_high = rolling_high.shift(lookback)
        prev_rolling_low = rolling_low.shift(lookback)
        
        # Higher highs and higher lows (uptrend structure)
        higher_high = rolling_high > prev_rolling_high
        higher_low = rolling_low > prev_rolling_low
        
        # Lower highs and lower lows (downtrend structure)
        lower_high = rolling_high < prev_rolling_high
        lower_low = rolling_low < prev_rolling_low
        
        # Strong structure
        strong_uptrend_structure = higher_high & higher_low
        strong_downtrend_structure = lower_high & lower_low
        
        # Weak/mixed structure
        weak_uptrend_structure = higher_low & ~lower_high
        weak_downtrend_structure = lower_high & ~higher_low
        
        if adx is not None:
            trending = adx >= adx_trending
        else:
            trending = pd.Series(True, index=close.index)
        
        conditions = [
            strong_uptrend_structure & trending,
            strong_downtrend_structure & trending,
            strong_uptrend_structure & ~trending,
            strong_downtrend_structure & ~trending,
            weak_uptrend_structure,
            weak_downtrend_structure,
        ]
        
        choices = [
            TrendLabels.STRONG_UPTREND,
            TrendLabels.STRONG_DOWNTREND,
            TrendLabels.UPTREND,
            TrendLabels.DOWNTREND,
            TrendLabels.WEAK_UPTREND,
            TrendLabels.WEAK_DOWNTREND,
        ]
        
        return np.select(conditions, choices, default=TrendLabels.RANGING)
    
    @staticmethod
    def detect_trend_transitions(
        trend_regime: pd.Series,
        lookback: int = 3
    ) -> pd.Series:
        """
        Detect trend transition states (reversals, exhaustion).
        
        Args:
            trend_regime: Series of trend labels
            lookback: Periods to look back for transition detection
            
        Returns:
            Series with transition labels where applicable
        """
        result = trend_regime.copy()
        
        # Map trend labels to numeric for comparison
        trend_map = {
            TrendLabels.STRONG_UPTREND: 3,
            TrendLabels.UPTREND: 2,
            TrendLabels.WEAK_UPTREND: 1,
            TrendLabels.RANGING: 0,
            TrendLabels.WEAK_DOWNTREND: -1,
            TrendLabels.DOWNTREND: -2,
            TrendLabels.STRONG_DOWNTREND: -3,
        }
        
        numeric_trend = trend_regime.map(trend_map).fillna(0)
        
        # Detect reversals (significant change in trend direction)
        trend_change = numeric_trend - numeric_trend.shift(lookback)
        
        # Reversal up: Was bearish, now bullish
        reversal_up = (numeric_trend.shift(lookback) < 0) & (numeric_trend > 0)
        
        # Reversal down: Was bullish, now bearish  
        reversal_down = (numeric_trend.shift(lookback) > 0) & (numeric_trend < 0)
        
        # Exhaustion: Strong trend weakening significantly
        exhaustion = (
            ((numeric_trend.shift(lookback) == 3) & (numeric_trend <= 1)) |
            ((numeric_trend.shift(lookback) == -3) & (numeric_trend >= -1))
        )
        
        result = result.where(~reversal_up, TrendLabels.TREND_REVERSAL_UP)
        result = result.where(~reversal_down, TrendLabels.TREND_REVERSAL_DOWN)
        result = result.where(~exhaustion, TrendLabels.TREND_EXHAUSTION)
        
        return result
    
    @staticmethod
    def get_trend_bias(trend_regime: str) -> int:
        """
        Get numeric trend bias from trend label.
        
        Returns:
            1 for bullish, -1 for bearish, 0 for neutral
        """
        bullish = [
            TrendLabels.STRONG_UPTREND,
            TrendLabels.UPTREND,
            TrendLabels.WEAK_UPTREND,
            TrendLabels.TREND_REVERSAL_UP,
            TrendLabels.VOLATILE_UPTREND,
        ]
        bearish = [
            TrendLabels.STRONG_DOWNTREND,
            TrendLabels.DOWNTREND,
            TrendLabels.WEAK_DOWNTREND,
            TrendLabels.TREND_REVERSAL_DOWN,
            TrendLabels.VOLATILE_DOWNTREND,
        ]
        
        if trend_regime in bullish:
            return 1
        elif trend_regime in bearish:
            return -1
        return 0
    
    @staticmethod
    def get_trend_strength(trend_regime: str) -> float:
        """
        Get trend strength multiplier from trend label.
        
        Returns:
            Float from 0.0 (no trend) to 1.0 (strongest trend)
        """
        strength_map = {
            TrendLabels.STRONG_UPTREND: 1.0,
            TrendLabels.STRONG_DOWNTREND: 1.0,
            TrendLabels.UPTREND: 0.7,
            TrendLabels.DOWNTREND: 0.7,
            TrendLabels.VOLATILE_UPTREND: 0.6,
            TrendLabels.VOLATILE_DOWNTREND: 0.6,
            TrendLabels.WEAK_UPTREND: 0.4,
            TrendLabels.WEAK_DOWNTREND: 0.4,
            TrendLabels.TREND_REVERSAL_UP: 0.3,
            TrendLabels.TREND_REVERSAL_DOWN: 0.3,
            TrendLabels.TREND_EXHAUSTION: 0.2,
            TrendLabels.QUIET_TREND: 0.3,
            TrendLabels.RANGING: 0.0,
        }
        return strength_map.get(trend_regime, 0.0)


# =============================================================================
# DISTRIBUTION REGIME CLASSIFIER
# =============================================================================
class DistributionRegimeClassifier:
    """
    Classifier for rolling window distribution-based regimes.
    
    Provides classification methods for:
    - ADX Distribution: Trend strength relative to recent history
    - Slope Distribution: Price momentum relative to recent history
    - DI Spread Distribution: Directional pressure relative to history
    - HTF Alignment Distribution: Multi-timeframe agreement scoring
    
    All methods take a percentile series (0-100) computed from a rolling
    window and return regime labels based on which zone the percentile falls in.
    """
    
    @staticmethod
    def classify_adx_distribution(
        adx_pctl: pd.Series,
        extreme_trending: int = 90,
        high_trending: int = 75,
        moderate_trending: int = 50,
        low_trending: int = 25,
        choppy: int = 10
    ) -> pd.Series:
        """
        Classify ADX distribution regime based on ADX percentile position.
        
        Args:
            adx_pctl: Percentile of ADX within rolling window (0-100)
            extreme_trending: Threshold for extreme trending (default P90)
            high_trending: Threshold for high trending (default P75)
            moderate_trending: Threshold for moderate trending (default P50)
            low_trending: Threshold for low trending (default P25)
            choppy: Threshold for choppy market (default P10)
            
        Returns:
            Series of ADX distribution regime labels
        """
        conditions = [
            adx_pctl >= extreme_trending,
            (adx_pctl >= high_trending) & (adx_pctl < extreme_trending),
            (adx_pctl >= moderate_trending) & (adx_pctl < high_trending),
            (adx_pctl >= low_trending) & (adx_pctl < moderate_trending),
            (adx_pctl >= choppy) & (adx_pctl < low_trending),
            adx_pctl < choppy
        ]
        
        choices = [
            ADXDistributionLabels.EXTREME_TRENDING,
            ADXDistributionLabels.HIGH_TRENDING,
            ADXDistributionLabels.MODERATE_TRENDING,
            ADXDistributionLabels.LOW_TRENDING,
            ADXDistributionLabels.CHOPPY,
            ADXDistributionLabels.EXTREME_CHOPPY
        ]
        
        return np.select(conditions, choices, default=ADXDistributionLabels.MODERATE_TRENDING)
    
    @staticmethod
    def classify_adx_raw(
        adx: pd.Series,
        extreme_trending: float = 50,
        high_trending: float = 40,
        moderate_trending: float = 25,
        low_trending: float = 15,
        choppy: float = 10
    ) -> pd.Series:
        """
        Classify ADX regime based on raw ADX values (not normalized).
        
        Args:
            adx: Raw ADX values
            extreme_trending: ADX threshold for extreme trending (default 50)
            high_trending: ADX threshold for high trending (default 40)
            moderate_trending: ADX threshold for moderate trending (default 25)
            low_trending: ADX threshold for low trending (default 15)
            choppy: ADX threshold for choppy market (default 10)
            
        Returns:
            Series of ADX distribution regime labels
        """
        conditions = [
            adx >= extreme_trending,
            (adx >= high_trending) & (adx < extreme_trending),
            (adx >= moderate_trending) & (adx < high_trending),
            (adx >= low_trending) & (adx < moderate_trending),
            (adx >= choppy) & (adx < low_trending),
            adx < choppy
        ]
        
        choices = [
            ADXDistributionLabels.EXTREME_TRENDING,
            ADXDistributionLabels.HIGH_TRENDING,
            ADXDistributionLabels.MODERATE_TRENDING,
            ADXDistributionLabels.LOW_TRENDING,
            ADXDistributionLabels.CHOPPY,
            ADXDistributionLabels.EXTREME_CHOPPY
        ]
        
        return np.select(conditions, choices, default=ADXDistributionLabels.MODERATE_TRENDING)
    
    @staticmethod
    def classify_adx_zscore(
        adx_zscore: pd.Series,
        extreme_trending: float = 2.0,
        high_trending: float = 1.5,
        moderate_trending: float = 0.5,
        low_trending: float = -0.5,
        choppy: float = -1.0
    ) -> pd.Series:
        """
        Classify ADX regime based on z-score normalized ADX values.
        
        Args:
            adx_zscore: Z-score of ADX (value - rolling_mean) / rolling_std
            extreme_trending: Z-score threshold for extreme trending (default 2.0)
            high_trending: Z-score threshold for high trending (default 1.5)
            moderate_trending: Z-score threshold for moderate trending (default 0.5)
            low_trending: Z-score threshold for low trending (default -0.5)
            choppy: Z-score threshold for choppy market (default -1.0)
            
        Returns:
            Series of ADX distribution regime labels
        """
        conditions = [
            adx_zscore >= extreme_trending,
            (adx_zscore >= high_trending) & (adx_zscore < extreme_trending),
            (adx_zscore >= moderate_trending) & (adx_zscore < high_trending),
            (adx_zscore >= low_trending) & (adx_zscore < moderate_trending),
            (adx_zscore >= choppy) & (adx_zscore < low_trending),
            adx_zscore < choppy
        ]
        
        choices = [
            ADXDistributionLabels.EXTREME_TRENDING,
            ADXDistributionLabels.HIGH_TRENDING,
            ADXDistributionLabels.MODERATE_TRENDING,
            ADXDistributionLabels.LOW_TRENDING,
            ADXDistributionLabels.CHOPPY,
            ADXDistributionLabels.EXTREME_CHOPPY
        ]
        
        return np.select(conditions, choices, default=ADXDistributionLabels.MODERATE_TRENDING)
    
    @staticmethod
    def classify_slope(
        slope_pctl: pd.Series,
        extreme_up: int = 95,
        steep_up: int = 80,
        moderate_up: int = 60,
        flat_lower: int = 40,
        moderate_down: int = 20,
        steep_down: int = 5
    ) -> pd.Series:
        """
        Classify slope distribution regime based on slope percentile position.
        
        Args:
            slope_pctl: Percentile of slope within rolling window (0-100)
            extreme_up: Threshold for extreme upward slope (default P95)
            steep_up: Threshold for steep upward slope (default P80)
            moderate_up: Threshold for moderate upward slope (default P60)
            flat_lower: Lower threshold for flat zone (default P40)
            moderate_down: Threshold for moderate downward slope (default P20)
            steep_down: Threshold for steep downward slope (default P5)
            
        Returns:
            Series of slope distribution regime labels
        """
        conditions = [
            slope_pctl >= extreme_up,
            (slope_pctl >= steep_up) & (slope_pctl < extreme_up),
            (slope_pctl >= moderate_up) & (slope_pctl < steep_up),
            (slope_pctl >= flat_lower) & (slope_pctl < moderate_up),
            (slope_pctl >= moderate_down) & (slope_pctl < flat_lower),
            (slope_pctl >= steep_down) & (slope_pctl < moderate_down),
            slope_pctl < steep_down
        ]
        
        choices = [
            SlopeDistributionLabels.EXTREME_STEEP_UP,
            SlopeDistributionLabels.STEEP_UP,
            SlopeDistributionLabels.MODERATE_UP,
            SlopeDistributionLabels.FLAT,
            SlopeDistributionLabels.MODERATE_DOWN,
            SlopeDistributionLabels.STEEP_DOWN,
            SlopeDistributionLabels.EXTREME_STEEP_DOWN
        ]
        
        return np.select(conditions, choices, default=SlopeDistributionLabels.FLAT)
    
    @staticmethod
    def classify_slope_raw(
        slope: pd.Series,
        extreme_up: float = 0.02,
        steep_up: float = 0.01,
        moderate_up: float = 0.005,
        flat_threshold: float = 0.002,
        moderate_down: float = -0.005,
        steep_down: float = -0.01,
        extreme_down: float = -0.02
    ) -> pd.Series:
        """
        Classify slope regime based on raw normalized slope values (not percentile).
        
        Args:
            slope: Raw normalized slope values (e.g., price change / price)
            extreme_up: Threshold for extreme upward slope (default 0.02 = 2%)
            steep_up: Threshold for steep upward slope (default 0.01 = 1%)
            moderate_up: Threshold for moderate upward slope (default 0.005 = 0.5%)
            flat_threshold: Threshold for flat zone (default 0.002)
            moderate_down: Threshold for moderate downward slope (default -0.005)
            steep_down: Threshold for steep downward slope (default -0.01)
            extreme_down: Threshold for extreme downward slope (default -0.02)
            
        Returns:
            Series of slope distribution regime labels
        """
        conditions = [
            slope >= extreme_up,
            (slope >= steep_up) & (slope < extreme_up),
            (slope >= moderate_up) & (slope < steep_up),
            (slope >= -flat_threshold) & (slope < moderate_up),
            (slope >= moderate_down) & (slope < -flat_threshold),
            (slope >= steep_down) & (slope < moderate_down),
            slope < steep_down
        ]
        
        choices = [
            SlopeDistributionLabels.EXTREME_STEEP_UP,
            SlopeDistributionLabels.STEEP_UP,
            SlopeDistributionLabels.MODERATE_UP,
            SlopeDistributionLabels.FLAT,
            SlopeDistributionLabels.MODERATE_DOWN,
            SlopeDistributionLabels.STEEP_DOWN,
            SlopeDistributionLabels.EXTREME_STEEP_DOWN
        ]
        
        return np.select(conditions, choices, default=SlopeDistributionLabels.FLAT)
    
    @staticmethod
    def classify_slope_zscore(
        slope_zscore: pd.Series,
        extreme_up: float = 2.5,
        steep_up: float = 1.5,
        moderate_up: float = 0.5,
        flat_threshold: float = 0.3,
        moderate_down: float = -0.5,
        steep_down: float = -1.5,
        extreme_down: float = -2.5
    ) -> pd.Series:
        """
        Classify slope regime based on z-score normalized slope values.
        
        Args:
            slope_zscore: Z-score of slope (value - rolling_mean) / rolling_std
            extreme_up: Z-score threshold for extreme upward slope (default 2.5)
            steep_up: Z-score threshold for steep upward slope (default 1.5)
            moderate_up: Z-score threshold for moderate upward slope (default 0.5)
            flat_threshold: Z-score threshold for flat zone (default 0.3)
            moderate_down: Z-score threshold for moderate downward slope (default -0.5)
            steep_down: Z-score threshold for steep downward slope (default -1.5)
            extreme_down: Z-score threshold for extreme downward slope (default -2.5)
            
        Returns:
            Series of slope distribution regime labels
        """
        conditions = [
            slope_zscore >= extreme_up,
            (slope_zscore >= steep_up) & (slope_zscore < extreme_up),
            (slope_zscore >= moderate_up) & (slope_zscore < steep_up),
            (slope_zscore >= -flat_threshold) & (slope_zscore < moderate_up),
            (slope_zscore >= moderate_down) & (slope_zscore < -flat_threshold),
            (slope_zscore >= steep_down) & (slope_zscore < moderate_down),
            slope_zscore < steep_down
        ]
        
        choices = [
            SlopeDistributionLabels.EXTREME_STEEP_UP,
            SlopeDistributionLabels.STEEP_UP,
            SlopeDistributionLabels.MODERATE_UP,
            SlopeDistributionLabels.FLAT,
            SlopeDistributionLabels.MODERATE_DOWN,
            SlopeDistributionLabels.STEEP_DOWN,
            SlopeDistributionLabels.EXTREME_STEEP_DOWN
        ]
        
        return np.select(conditions, choices, default=SlopeDistributionLabels.FLAT)
    
    @staticmethod
    def classify_di_spread_distribution(
        di_spread_pctl: pd.Series,
        extreme_bullish: int = 95,
        strong_bullish: int = 80,
        moderate_bullish: int = 60,
        neutral_lower: int = 40,
        moderate_bearish: int = 20,
        strong_bearish: int = 5
    ) -> pd.Series:
        """
        Classify DI spread distribution regime based on spread percentile.
        
        Args:
            di_spread_pctl: Percentile of (+DI - -DI) within rolling window (0-100)
            extreme_bullish: Threshold for extreme bullish (default P95)
            strong_bullish: Threshold for strong bullish (default P80)
            moderate_bullish: Threshold for moderate bullish (default P60)
            neutral_lower: Lower threshold for neutral zone (default P40)
            moderate_bearish: Threshold for moderate bearish (default P20)
            strong_bearish: Threshold for strong bearish (default P5)
            
        Returns:
            Series of DI spread distribution regime labels
        """
        conditions = [
            di_spread_pctl >= extreme_bullish,
            (di_spread_pctl >= strong_bullish) & (di_spread_pctl < extreme_bullish),
            (di_spread_pctl >= moderate_bullish) & (di_spread_pctl < strong_bullish),
            (di_spread_pctl >= neutral_lower) & (di_spread_pctl < moderate_bullish),
            (di_spread_pctl >= moderate_bearish) & (di_spread_pctl < neutral_lower),
            (di_spread_pctl >= strong_bearish) & (di_spread_pctl < moderate_bearish),
            di_spread_pctl < strong_bearish
        ]
        
        choices = [
            DISpreadLabels.EXTREME_BULLISH,
            DISpreadLabels.STRONG_BULLISH,
            DISpreadLabels.MODERATE_BULLISH,
            DISpreadLabels.NEUTRAL,
            DISpreadLabels.MODERATE_BEARISH,
            DISpreadLabels.STRONG_BEARISH,
            DISpreadLabels.EXTREME_BEARISH
        ]
        
        return np.select(conditions, choices, default=DISpreadLabels.NEUTRAL)
    
    @staticmethod
    def classify_di_spread_raw(
        di_spread: pd.Series,
        extreme_bullish: float = 30,
        strong_bullish: float = 20,
        moderate_bullish: float = 10,
        neutral_threshold: float = 5,
        moderate_bearish: float = -10,
        strong_bearish: float = -20,
        extreme_bearish: float = -30
    ) -> pd.Series:
        """
        Classify DI spread regime based on raw (+DI - -DI) values (not percentile).
        
        Args:
            di_spread: Raw DI spread values (+DI - -DI)
            extreme_bullish: Threshold for extreme bullish (default 30)
            strong_bullish: Threshold for strong bullish (default 20)
            moderate_bullish: Threshold for moderate bullish (default 10)
            neutral_threshold: Threshold for neutral zone (default 5)
            moderate_bearish: Threshold for moderate bearish (default -10)
            strong_bearish: Threshold for strong bearish (default -20)
            extreme_bearish: Threshold for extreme bearish (default -30)
            
        Returns:
            Series of DI spread distribution regime labels
        """
        conditions = [
            di_spread >= extreme_bullish,
            (di_spread >= strong_bullish) & (di_spread < extreme_bullish),
            (di_spread >= moderate_bullish) & (di_spread < strong_bullish),
            (di_spread >= -neutral_threshold) & (di_spread < moderate_bullish),
            (di_spread >= moderate_bearish) & (di_spread < -neutral_threshold),
            (di_spread >= strong_bearish) & (di_spread < moderate_bearish),
            di_spread < strong_bearish
        ]
        
        choices = [
            DISpreadLabels.EXTREME_BULLISH,
            DISpreadLabels.STRONG_BULLISH,
            DISpreadLabels.MODERATE_BULLISH,
            DISpreadLabels.NEUTRAL,
            DISpreadLabels.MODERATE_BEARISH,
            DISpreadLabels.STRONG_BEARISH,
            DISpreadLabels.EXTREME_BEARISH
        ]
        
        return np.select(conditions, choices, default=DISpreadLabels.NEUTRAL)
    
    @staticmethod
    def classify_di_spread_zscore(
        di_spread_zscore: pd.Series,
        extreme_bullish: float = 2.5,
        strong_bullish: float = 1.5,
        moderate_bullish: float = 0.5,
        neutral_threshold: float = 0.3,
        moderate_bearish: float = -0.5,
        strong_bearish: float = -1.5,
        extreme_bearish: float = -2.5
    ) -> pd.Series:
        """
        Classify DI spread regime based on z-score normalized (+DI - -DI) values.
        
        Args:
            di_spread_zscore: Z-score of DI spread (value - rolling_mean) / rolling_std
            extreme_bullish: Z-score threshold for extreme bullish (default 2.5)
            strong_bullish: Z-score threshold for strong bullish (default 1.5)
            moderate_bullish: Z-score threshold for moderate bullish (default 0.5)
            neutral_threshold: Z-score threshold for neutral zone (default 0.3)
            moderate_bearish: Z-score threshold for moderate bearish (default -0.5)
            strong_bearish: Z-score threshold for strong bearish (default -1.5)
            extreme_bearish: Z-score threshold for extreme bearish (default -2.5)
            
        Returns:
            Series of DI spread distribution regime labels
        """
        conditions = [
            di_spread_zscore >= extreme_bullish,
            (di_spread_zscore >= strong_bullish) & (di_spread_zscore < extreme_bullish),
            (di_spread_zscore >= moderate_bullish) & (di_spread_zscore < strong_bullish),
            (di_spread_zscore >= -neutral_threshold) & (di_spread_zscore < moderate_bullish),
            (di_spread_zscore >= moderate_bearish) & (di_spread_zscore < -neutral_threshold),
            (di_spread_zscore >= strong_bearish) & (di_spread_zscore < moderate_bearish),
            di_spread_zscore < strong_bearish
        ]
        
        choices = [
            DISpreadLabels.EXTREME_BULLISH,
            DISpreadLabels.STRONG_BULLISH,
            DISpreadLabels.MODERATE_BULLISH,
            DISpreadLabels.NEUTRAL,
            DISpreadLabels.MODERATE_BEARISH,
            DISpreadLabels.STRONG_BEARISH,
            DISpreadLabels.EXTREME_BEARISH
        ]
        
        return np.select(conditions, choices, default=DISpreadLabels.NEUTRAL)
    
    @staticmethod
    def classify_htf_alignment_distribution(
        htf_alignment_pctl: pd.Series,
        perfect: int = 90,
        strong: int = 75,
        moderate: int = 50,
        weak: int = 25,
        conflicting: int = 10
    ) -> pd.Series:
        """
        Classify HTF alignment distribution regime based on alignment score percentile.
        
        Args:
            htf_alignment_pctl: Percentile of HTF alignment score within rolling window (0-100)
            perfect: Threshold for perfect alignment (default P90)
            strong: Threshold for strong alignment (default P75)
            moderate: Threshold for moderate alignment (default P50)
            weak: Threshold for weak alignment (default P25)
            conflicting: Threshold for conflicting (default P10)
            
        Returns:
            Series of HTF alignment distribution regime labels
        """
        conditions = [
            htf_alignment_pctl >= perfect,
            (htf_alignment_pctl >= strong) & (htf_alignment_pctl < perfect),
            (htf_alignment_pctl >= moderate) & (htf_alignment_pctl < strong),
            (htf_alignment_pctl >= weak) & (htf_alignment_pctl < moderate),
            (htf_alignment_pctl >= conflicting) & (htf_alignment_pctl < weak),
            htf_alignment_pctl < conflicting
        ]
        
        choices = [
            HTFAlignmentLabels.PERFECT_ALIGNMENT,
            HTFAlignmentLabels.STRONG_ALIGNMENT,
            HTFAlignmentLabels.MODERATE_ALIGNMENT,
            HTFAlignmentLabels.WEAK_ALIGNMENT,
            HTFAlignmentLabels.CONFLICTING,
            HTFAlignmentLabels.EXTREME_CONFLICT
        ]
        
        return np.select(conditions, choices, default=HTFAlignmentLabels.MODERATE_ALIGNMENT)
    
    @staticmethod
    def classify_htf_alignment_raw(
        htf_alignment_score: pd.Series,
        perfect: float = 80,
        strong: float = 50,
        moderate: float = 20,
        weak: float = -20,
        conflicting: float = -50
    ) -> pd.Series:
        """
        Classify HTF alignment regime based on raw alignment score (not percentile).
        
        The score ranges from -100 (perfect bearish) to +100 (perfect bullish).
        For regime classification, we use the absolute value for strength.
        
        Args:
            htf_alignment_score: Raw HTF alignment score (-100 to +100)
            perfect: Threshold for perfect alignment (default 80)
            strong: Threshold for strong alignment (default 50)
            moderate: Threshold for moderate alignment (default 20)
            weak: Threshold for weak alignment (default -20, meaning abs < 20)
            conflicting: Threshold for conflicting (default -50)
            
        Returns:
            Series of HTF alignment distribution regime labels
        """
        # Use absolute value for strength classification
        abs_score = htf_alignment_score.abs()
        
        conditions = [
            abs_score >= perfect,
            (abs_score >= strong) & (abs_score < perfect),
            (abs_score >= moderate) & (abs_score < strong),
            (abs_score >= 10) & (abs_score < moderate),  # weak but some direction
            abs_score < 10  # truly conflicting/neutral
        ]
        
        choices = [
            HTFAlignmentLabels.PERFECT_ALIGNMENT,
            HTFAlignmentLabels.STRONG_ALIGNMENT,
            HTFAlignmentLabels.MODERATE_ALIGNMENT,
            HTFAlignmentLabels.WEAK_ALIGNMENT,
            HTFAlignmentLabels.CONFLICTING
        ]
        
        return np.select(conditions, choices, default=HTFAlignmentLabels.MODERATE_ALIGNMENT)
    
    @staticmethod
    def classify_htf_alignment_zscore(
        htf_alignment_zscore: pd.Series,
        perfect: float = 2.0,
        strong: float = 1.0,
        moderate: float = 0.0,
        weak: float = -1.0,
        conflicting: float = -2.0
    ) -> pd.Series:
        """
        Classify HTF alignment regime based on z-score normalized alignment score.
        
        Args:
            htf_alignment_zscore: Z-score of HTF alignment score
            perfect: Z-score threshold for perfect alignment (default 2.0)
            strong: Z-score threshold for strong alignment (default 1.0)
            moderate: Z-score threshold for moderate alignment (default 0.0)
            weak: Z-score threshold for weak alignment (default -1.0)
            conflicting: Z-score threshold for conflicting (default -2.0)
            
        Returns:
            Series of HTF alignment distribution regime labels
        """
        conditions = [
            htf_alignment_zscore >= perfect,
            (htf_alignment_zscore >= strong) & (htf_alignment_zscore < perfect),
            (htf_alignment_zscore >= moderate) & (htf_alignment_zscore < strong),
            (htf_alignment_zscore >= weak) & (htf_alignment_zscore < moderate),
            (htf_alignment_zscore >= conflicting) & (htf_alignment_zscore < weak),
            htf_alignment_zscore < conflicting
        ]
        
        choices = [
            HTFAlignmentLabels.PERFECT_ALIGNMENT,
            HTFAlignmentLabels.STRONG_ALIGNMENT,
            HTFAlignmentLabels.MODERATE_ALIGNMENT,
            HTFAlignmentLabels.WEAK_ALIGNMENT,
            HTFAlignmentLabels.CONFLICTING,
            HTFAlignmentLabels.EXTREME_CONFLICT
        ]
        
        return np.select(conditions, choices, default=HTFAlignmentLabels.MODERATE_ALIGNMENT)
    
    @staticmethod
    def classify_returns_zscore(
        returns_zscore: pd.Series,
        extreme_up: float = 2.5,
        strong_up: float = 1.5,
        moderate_up: float = 0.5,
        neutral_threshold: float = 0.3,
        moderate_down: float = -0.5,
        strong_down: float = -1.5,
        extreme_down: float = -2.5
    ) -> pd.Series:
        """
        Classify returns regime based on z-score normalized cumulative returns.
        
        Args:
            returns_zscore: Z-score of cumulative returns (value - rolling_mean) / rolling_std
            extreme_up: Z-score threshold for extreme upward returns (default 2.5)
            strong_up: Z-score threshold for strong upward returns (default 1.5)
            moderate_up: Z-score threshold for moderate upward returns (default 0.5)
            neutral_threshold: Z-score threshold for neutral zone (default 0.3)
            moderate_down: Z-score threshold for moderate downward returns (default -0.5)
            strong_down: Z-score threshold for strong downward returns (default -1.5)
            extreme_down: Z-score threshold for extreme downward returns (default -2.5)
            
        Returns:
            Series of regime labels (uses SlopeDistributionLabels for consistency)
        """
        conditions = [
            returns_zscore >= extreme_up,
            (returns_zscore >= strong_up) & (returns_zscore < extreme_up),
            (returns_zscore >= moderate_up) & (returns_zscore < strong_up),
            (returns_zscore >= -neutral_threshold) & (returns_zscore < moderate_up),
            (returns_zscore >= moderate_down) & (returns_zscore < -neutral_threshold),
            (returns_zscore >= strong_down) & (returns_zscore < moderate_down),
            returns_zscore < strong_down
        ]
        
        choices = [
            SlopeDistributionLabels.EXTREME_STEEP_UP,
            SlopeDistributionLabels.STEEP_UP,
            SlopeDistributionLabels.MODERATE_UP,
            SlopeDistributionLabels.FLAT,
            SlopeDistributionLabels.MODERATE_DOWN,
            SlopeDistributionLabels.STEEP_DOWN,
            SlopeDistributionLabels.EXTREME_STEEP_DOWN
        ]
        
        return np.select(conditions, choices, default=SlopeDistributionLabels.FLAT)
    
    @staticmethod
    def classify_price_zscore(
        price_zscore: pd.Series,
        extreme_high: float = 2.5,
        high: float = 1.5,
        moderate_high: float = 0.5,
        neutral_threshold: float = 0.3,
        moderate_low: float = -0.5,
        low: float = -1.5,
        extreme_low: float = -2.5
    ) -> pd.Series:
        """
        Classify price regime based on z-score normalized price values.
        
        Args:
            price_zscore: Z-score of price (price - rolling_mean) / rolling_std
            extreme_high: Z-score threshold for extreme high price (default 2.5)
            high: Z-score threshold for high price (default 1.5)
            moderate_high: Z-score threshold for moderate high price (default 0.5)
            neutral_threshold: Z-score threshold for neutral zone (default 0.3)
            moderate_low: Z-score threshold for moderate low price (default -0.5)
            low: Z-score threshold for low price (default -1.5)
            extreme_low: Z-score threshold for extreme low price (default -2.5)
            
        Returns:
            Series of regime labels
        """
        conditions = [
            price_zscore >= extreme_high,
            (price_zscore >= high) & (price_zscore < extreme_high),
            (price_zscore >= moderate_high) & (price_zscore < high),
            (price_zscore >= -neutral_threshold) & (price_zscore < moderate_high),
            (price_zscore >= moderate_low) & (price_zscore < -neutral_threshold),
            (price_zscore >= low) & (price_zscore < moderate_low),
            price_zscore < low
        ]
        
        choices = [
            RegimeLabels.EXTREME_TAIL,      # Extreme high = overbought tail
            RegimeLabels.TAIL,              # High = overbought
            RegimeLabels.OUTER_SHOULDER,    # Moderate high
            RegimeLabels.CORE,              # Neutral
            RegimeLabels.OUTER_SHOULDER,    # Moderate low
            RegimeLabels.TAIL,              # Low = oversold
            RegimeLabels.EXTREME_TAIL       # Extreme low = oversold tail
        ]
        
        return np.select(conditions, choices, default=RegimeLabels.CORE)
    
    @staticmethod
    def get_adx_regime_multiplier(adx_regime: str, strategy_type: str = "momentum") -> float:
        """
        Get multiplier based on ADX distribution regime.
        
        Args:
            adx_regime: ADX distribution regime label
            strategy_type: 'momentum' or 'mean_reversion'
            
        Returns:
            Float multiplier for position sizing/exits
        """
        if strategy_type == "momentum":
            # Momentum loves trending markets
            multipliers = {
                ADXDistributionLabels.EXTREME_TRENDING: 0.8,   # May exhaust, reduce
                ADXDistributionLabels.HIGH_TRENDING: 1.3,      # Prime conditions
                ADXDistributionLabels.MODERATE_TRENDING: 1.1,  # Good conditions
                ADXDistributionLabels.LOW_TRENDING: 0.8,       # Weak trends
                ADXDistributionLabels.CHOPPY: 0.5,             # Avoid
                ADXDistributionLabels.EXTREME_CHOPPY: 0.3      # Strong avoid
            }
        else:  # mean_reversion
            # MR prefers non-trending markets
            multipliers = {
                ADXDistributionLabels.EXTREME_TRENDING: 0.4,   # Dangerous for MR
                ADXDistributionLabels.HIGH_TRENDING: 0.6,      # Risky
                ADXDistributionLabels.MODERATE_TRENDING: 0.9,  # Caution
                ADXDistributionLabels.LOW_TRENDING: 1.1,       # Good for MR
                ADXDistributionLabels.CHOPPY: 1.3,             # Prime MR conditions
                ADXDistributionLabels.EXTREME_CHOPPY: 1.2      # Good but watch out
            }
        return multipliers.get(adx_regime, 1.0)
    
    @staticmethod
    def get_slope_regime_multiplier(slope_regime: str, trade_direction: int = 1) -> float:
        """
        Get multiplier based on slope distribution regime.
        
        Args:
            slope_regime: Slope distribution regime label
            trade_direction: 1 for long, -1 for short
            
        Returns:
            Float multiplier for position sizing/exits
        """
        # For longs, positive slopes are good; for shorts, negative slopes are good
        if trade_direction == 1:  # Long
            multipliers = {
                SlopeDistributionLabels.EXTREME_STEEP_UP: 0.7,   # Parabolic, may reverse
                SlopeDistributionLabels.STEEP_UP: 1.2,           # Strong momentum
                SlopeDistributionLabels.MODERATE_UP: 1.3,        # Ideal
                SlopeDistributionLabels.FLAT: 0.8,               # No momentum
                SlopeDistributionLabels.MODERATE_DOWN: 0.5,      # Against trend
                SlopeDistributionLabels.STEEP_DOWN: 0.3,         # Very against
                SlopeDistributionLabels.EXTREME_STEEP_DOWN: 0.2  # Capitulation
            }
        else:  # Short
            multipliers = {
                SlopeDistributionLabels.EXTREME_STEEP_UP: 0.2,   # Against trend
                SlopeDistributionLabels.STEEP_UP: 0.3,           # Against trend
                SlopeDistributionLabels.MODERATE_UP: 0.5,        # Against trend
                SlopeDistributionLabels.FLAT: 0.8,               # No momentum
                SlopeDistributionLabels.MODERATE_DOWN: 1.3,      # Ideal
                SlopeDistributionLabels.STEEP_DOWN: 1.2,         # Strong momentum
                SlopeDistributionLabels.EXTREME_STEEP_DOWN: 0.7  # May bounce
            }
        return multipliers.get(slope_regime, 1.0)

    @staticmethod
    def get_slope_regime_multiplier_breakout(slope_regime: str, trade_direction: int = 1) -> float:
        """
        Slope multiplier tuned for breakout/trend-following strategies.

        Steep slopes in the entry direction = momentum confirmation → SIZE UP.
        Opposite slopes = against the breakout → SIZE DOWN sharply.
        Parabolic extreme is still treated as strong (trend continuation likely).
        """
        if trade_direction == 1:  # Long
            multipliers = {
                SlopeDistributionLabels.EXTREME_STEEP_UP:   1.30,  # Parabolic but confirmed
                SlopeDistributionLabels.STEEP_UP:           1.40,  # Best: strong momentum
                SlopeDistributionLabels.MODERATE_UP:        1.20,  # Good
                SlopeDistributionLabels.FLAT:               0.80,  # Stalling
                SlopeDistributionLabels.MODERATE_DOWN:      0.50,  # Against, reduce
                SlopeDistributionLabels.STEEP_DOWN:         0.30,  # Strong against
                SlopeDistributionLabels.EXTREME_STEEP_DOWN: 0.20,  # Very against
            }
        else:  # Short
            multipliers = {
                SlopeDistributionLabels.EXTREME_STEEP_DOWN: 1.30,
                SlopeDistributionLabels.STEEP_DOWN:         1.40,
                SlopeDistributionLabels.MODERATE_DOWN:      1.20,
                SlopeDistributionLabels.FLAT:               0.80,
                SlopeDistributionLabels.MODERATE_UP:        0.50,
                SlopeDistributionLabels.STEEP_UP:           0.30,
                SlopeDistributionLabels.EXTREME_STEEP_UP:   0.20,
            }
        return multipliers.get(slope_regime, 1.0)

    @staticmethod
    def get_di_spread_regime_multiplier(di_regime: str, trade_direction: int = 1) -> float:
        """
        Get multiplier based on DI spread distribution regime.
        
        Args:
            di_regime: DI spread distribution regime label
            trade_direction: 1 for long, -1 for short
            
        Returns:
            Float multiplier for position sizing/exits
        """
        if trade_direction == 1:  # Long
            multipliers = {
                DISpreadLabels.EXTREME_BULLISH: 0.8,    # May reverse, reduce
                DISpreadLabels.STRONG_BULLISH: 1.3,     # Prime long conditions
                DISpreadLabels.MODERATE_BULLISH: 1.2,   # Good for longs
                DISpreadLabels.NEUTRAL: 1.0,            # Neutral
                DISpreadLabels.MODERATE_BEARISH: 0.7,   # Against direction
                DISpreadLabels.STRONG_BEARISH: 0.4,     # Strong against
                DISpreadLabels.EXTREME_BEARISH: 0.3     # Very against
            }
        else:  # Short
            multipliers = {
                DISpreadLabels.EXTREME_BULLISH: 0.3,    # Very against
                DISpreadLabels.STRONG_BULLISH: 0.4,     # Strong against
                DISpreadLabels.MODERATE_BULLISH: 0.7,   # Against direction
                DISpreadLabels.NEUTRAL: 1.0,            # Neutral
                DISpreadLabels.MODERATE_BEARISH: 1.2,   # Good for shorts
                DISpreadLabels.STRONG_BEARISH: 1.3,     # Prime short conditions
                DISpreadLabels.EXTREME_BEARISH: 0.8     # May reverse, reduce
            }
        return multipliers.get(di_regime, 1.0)

    @staticmethod
    def get_di_spread_regime_multiplier_breakout(di_regime: str, trade_direction: int = 1) -> float:
        """
        DI-spread multiplier tuned for breakout/trend-following strategies.

        EXTREME and STRONG in the entry direction = breakout confirmed → SIZE UP.
        Strong opposing DI = breakout false or counter-trend → SIZE DOWN sharply.
        """
        if trade_direction == 1:  # Long
            multipliers = {
                DISpreadLabels.EXTREME_BULLISH:  1.30,  # +DI dominance at extremes
                DISpreadLabels.STRONG_BULLISH:   1.40,  # Best DI setup for longs
                DISpreadLabels.MODERATE_BULLISH: 1.20,
                DISpreadLabels.NEUTRAL:          0.90,
                DISpreadLabels.MODERATE_BEARISH: 0.60,
                DISpreadLabels.STRONG_BEARISH:   0.35,
                DISpreadLabels.EXTREME_BEARISH:  0.20,
            }
        else:  # Short
            multipliers = {
                DISpreadLabels.EXTREME_BEARISH:  1.30,
                DISpreadLabels.STRONG_BEARISH:   1.40,
                DISpreadLabels.MODERATE_BEARISH: 1.20,
                DISpreadLabels.NEUTRAL:          0.90,
                DISpreadLabels.MODERATE_BULLISH: 0.60,
                DISpreadLabels.STRONG_BULLISH:   0.35,
                DISpreadLabels.EXTREME_BULLISH:  0.20,
            }
        return multipliers.get(di_regime, 1.0)

    @staticmethod
    def get_htf_alignment_multiplier(htf_regime: str, strategy_type: str = "momentum") -> float:
        """
        Get multiplier based on HTF alignment distribution regime.
        
        Args:
            htf_regime: HTF alignment distribution regime label
            strategy_type: 'momentum' or 'mean_reversion'
            
        Returns:
            Float multiplier for position sizing
        """
        if strategy_type == "momentum":
            # Momentum needs HTF alignment
            multipliers = {
                HTFAlignmentLabels.PERFECT_ALIGNMENT: 1.4,    # Maximum conviction
                HTFAlignmentLabels.STRONG_ALIGNMENT: 1.2,     # High conviction
                HTFAlignmentLabels.MODERATE_ALIGNMENT: 1.0,   # Normal
                HTFAlignmentLabels.WEAK_ALIGNMENT: 0.7,       # Reduce size
                HTFAlignmentLabels.CONFLICTING: 0.4,          # Strong reduce
                HTFAlignmentLabels.EXTREME_CONFLICT: 0.2      # Avoid
            }
        else:  # mean_reversion
            # MR can trade against HTF in some cases
            multipliers = {
                HTFAlignmentLabels.PERFECT_ALIGNMENT: 0.8,    # Less MR opportunity
                HTFAlignmentLabels.STRONG_ALIGNMENT: 0.9,     # Reduce slightly
                HTFAlignmentLabels.MODERATE_ALIGNMENT: 1.0,   # Normal
                HTFAlignmentLabels.WEAK_ALIGNMENT: 1.1,       # Some opportunity
                HTFAlignmentLabels.CONFLICTING: 1.2,          # Counter-trend play
                HTFAlignmentLabels.EXTREME_CONFLICT: 1.0      # Careful, HTF strong
            }
        return multipliers.get(htf_regime, 1.0)
    
    @staticmethod
    def get_combined_distribution_score(
        adx_regime: str,
        slope_regime: str,
        di_regime: str,
        htf_regime: str,
        trade_direction: int = 1,
        strategy_type: str = "momentum",
        weights: Optional[Dict[str, float]] = None
    ) -> dict:
        """
        Calculate combined score from all distribution regimes.
        
        Args:
            adx_regime: ADX distribution regime
            slope_regime: Slope distribution regime
            di_regime: DI spread distribution regime
            htf_regime: HTF alignment distribution regime
            trade_direction: 1 for long, -1 for short
            strategy_type: 'momentum' or 'mean_reversion'
            weights: Optional dict of weights for each regime type
            
        Returns:
            dict with combined multipliers and individual scores
        """
        if weights is None:
            weights = {
                "adx": 0.25,
                "slope": 0.25,
                "di": 0.25,
                "htf": 0.25
            }
        
        # Get individual multipliers
        adx_mult = DistributionRegimeClassifier.get_adx_regime_multiplier(
            adx_regime, strategy_type
        )
        if strategy_type == "breakout":
            slope_mult = DistributionRegimeClassifier.get_slope_regime_multiplier_breakout(
                slope_regime, trade_direction
            )
            di_mult = DistributionRegimeClassifier.get_di_spread_regime_multiplier_breakout(
                di_regime, trade_direction
            )
        else:
            slope_mult = DistributionRegimeClassifier.get_slope_regime_multiplier(
                slope_regime, trade_direction
            )
            di_mult = DistributionRegimeClassifier.get_di_spread_regime_multiplier(
                di_regime, trade_direction
            )
        htf_mult = DistributionRegimeClassifier.get_htf_alignment_multiplier(
            htf_regime, strategy_type
        )
        
        # Calculate weighted combined multiplier
        combined_mult = (
            weights["adx"] * adx_mult +
            weights["slope"] * slope_mult +
            weights["di"] * di_mult +
            weights["htf"] * htf_mult
        )
        
        # Calculate conviction score (0-100)
        # Higher when all regimes agree
        conviction = min(100, max(0, (combined_mult - 0.5) * 100))
        
        return {
            "combined_multiplier": combined_mult,
            "conviction_score": conviction,
            "adx_multiplier": adx_mult,
            "slope_multiplier": slope_mult,
            "di_multiplier": di_mult,
            "htf_multiplier": htf_mult,
            "adx_regime": adx_regime,
            "slope_regime": slope_regime,
            "di_regime": di_regime,
            "htf_regime": htf_regime
        }


# =============================================================================
# BACKTEST REGIME ANALYZER
# =============================================================================
class BacktestRegimeAnalyzer:
    """
    Classify market regimes from OHLCV candle data and analyze backtest
    trade performance grouped by regime.

    Five regime buckets (derived from TrendClassifier output):
        EXTREME_BULL  – STRONG_UPTREND (high ADX + full bullish EMA alignment)
        BULL          – UPTREND / WEAK_UPTREND
        RANGING       – RANGING (low ADX, no clear direction)
        BEAR          – DOWNTREND / WEAK_DOWNTREND
        EXTREME_BEAR  – STRONG_DOWNTREND (high ADX + full bearish EMA alignment)
    """

    REGIMES: List[str] = ["EXTREME_BULL", "BULL", "RANGING", "BEAR", "EXTREME_BEAR"]

    # Colors used by the frontend for chart backgrounds / labels
    REGIME_COLORS: Dict[str, Dict[str, str]] = {
        "EXTREME_BULL": {"solid": "#10b981", "text": "#10b981", "bg": "rgba(16,185,129,0.25)"},
        "BULL":         {"solid": "#34d399", "text": "#34d399", "bg": "rgba(16,185,129,0.12)"},
        "RANGING":      {"solid": "#a0aec0", "text": "#a0aec0", "bg": "rgba(160,174,192,0.07)"},
        "BEAR":         {"solid": "#f87171", "text": "#f87171", "bg": "rgba(239,68,68,0.12)"},
        "EXTREME_BEAR": {"solid": "#ef4444", "text": "#ef4444", "bg": "rgba(239,68,68,0.25)"},
    }

    # Maps TrendLabels to our 5 buckets
    _TREND_MAP: Dict[str, str] = {
        TrendLabels.STRONG_UPTREND:       "EXTREME_BULL",
        TrendLabels.UPTREND:              "BULL",
        TrendLabels.WEAK_UPTREND:         "BULL",
        TrendLabels.TREND_REVERSAL_UP:    "BULL",
        TrendLabels.VOLATILE_UPTREND:     "BULL",
        TrendLabels.QUIET_TREND:          "RANGING",
        TrendLabels.TREND_EXHAUSTION:     "RANGING",
        TrendLabels.RANGING:              "RANGING",
        TrendLabels.TREND_REVERSAL_DOWN:  "BEAR",
        TrendLabels.WEAK_DOWNTREND:       "BEAR",
        TrendLabels.DOWNTREND:            "BEAR",
        TrendLabels.VOLATILE_DOWNTREND:   "BEAR",
        TrendLabels.STRONG_DOWNTREND:     "EXTREME_BEAR",
    }

    # ------------------------------------------------------------------ EMA / ADX

    @staticmethod
    def _ema(series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False).mean()

    @staticmethod
    def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Wilder-smoothed ADX."""
        prev_close = close.shift(1)
        prev_high  = high.shift(1)
        prev_low   = low.shift(1)

        tr = pd.concat([
            (high - low),
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)

        up_move   = high - prev_high
        down_move = prev_low - low

        plus_dm  = up_move.where((up_move > down_move)   & (up_move   > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        alpha = 1.0 / period
        atr      = tr.ewm(alpha=alpha, adjust=False).mean()
        plus_di  = 100.0 * plus_dm.ewm(alpha=alpha, adjust=False).mean() / (atr + 1e-10)
        minus_di = 100.0 * minus_dm.ewm(alpha=alpha, adjust=False).mean() / (atr + 1e-10)

        denom = plus_di + minus_di
        dx = pd.Series(0.0, index=close.index)
        mask = denom > 0
        dx[mask] = 100.0 * (plus_di - minus_di).abs()[mask] / denom[mask]

        return dx.ewm(alpha=alpha, adjust=False).mean()

    # ------------------------------------------------------------------ classify_regime

    @classmethod
    def classify_regime(
        cls,
        df: pd.DataFrame,
        fast: int = 8,
        medium: int = 21,
        slow: int = 50,
        adx_period: int = 14,
    ) -> pd.DataFrame:
        """
        Add a ``regime`` column to an OHLCV DataFrame.

        Parameters
        ----------
        df       : DataFrame with columns [open, high, low, close].
        fast     : Fast EMA period (default 8).
        medium   : Medium EMA period (default 21).
        slow     : Slow EMA period (default 50).
        adx_period: ADX smoothing period (default 14).

        Returns
        -------
        Copy of df with extra columns: ema_fast, ema_medium, ema_slow, adx, regime.
        """
        df = df.copy()
        df["ema_fast"]   = cls._ema(df["close"], fast)
        df["ema_medium"] = cls._ema(df["close"], medium)
        df["ema_slow"]   = cls._ema(df["close"], slow)
        df["adx"]        = cls._adx(df["high"], df["low"], df["close"], adx_period)

        raw = TrendClassifier.classify_trend_regime(
            close=df["close"],
            ema_fast=df["ema_fast"],
            ema_medium=df["ema_medium"],
            ema_slow=df["ema_slow"],
            adx=df["adx"],
        )
        df["regime"] = pd.Series(raw, index=df.index).map(cls._TREND_MAP).fillna("RANGING")
        return df

    # ------------------------------------------------------------------ regime_periods

    @classmethod
    def compute_regime_periods(cls, df: pd.DataFrame) -> List[Dict]:
        """
        Extract contiguous regime periods suitable for Plotly background shapes.

        Returns list of dicts: ``{start, end, regime}`` where start/end are
        UTC ISO-8601 strings.
        """
        if "regime" not in df.columns or len(df) == 0:
            return []

        def _to_iso(val) -> str:
            if hasattr(val, "isoformat"):
                s = val.isoformat()
                # strip +00:00 / Z for clean ISO
                return s.replace("+00:00", "").replace("Z", "")
            return str(val)

        date_col = df["date"] if "date" in df.columns else pd.Series(df.index)
        regimes  = df["regime"].to_list()
        dates    = date_col.to_list()

        periods: List[Dict] = []
        start_i = 0
        cur = regimes[0]

        for i in range(1, len(regimes)):
            if regimes[i] != cur:
                periods.append({"start": _to_iso(dates[start_i]), "end": _to_iso(dates[i]), "regime": cur})
                cur = regimes[i]
                start_i = i

        periods.append({"start": _to_iso(dates[start_i]), "end": _to_iso(dates[-1]), "regime": cur})
        return periods

    # ------------------------------------------------------------------ per-regime metrics

    @staticmethod
    def _metrics_for(trades: list, starting_capital: float) -> Dict:
        """Compute aggregated metrics for a list of trade dicts."""
        import math

        if not trades:
            return {
                "trades": 0, "win_rate": 0.0, "profit_factor": 0.0,
                "sharpe": 0.0, "expectancy_r": 0.0, "rr": 0.0,
                "cagr": 0.0, "total_pnl": 0.0,
            }

        def _pnl(t):
            v = t.get("profit_abs") or t.get("close_profit_abs") or 0.0
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        pnls = [_pnl(t) for t in trades]
        wins   = [p for p in pnls if p > 0]
        losses = [abs(p) for p in pnls if p < 0]
        n = len(pnls)

        gross_win  = sum(wins)
        gross_loss = sum(losses)
        avg_win    = gross_win  / len(wins)   if wins   else 0.0
        avg_loss   = gross_loss / len(losses) if losses else 0.0
        win_rate   = len(wins) / n * 100.0
        pf         = gross_win / gross_loss if gross_loss > 0 else (float("inf") if gross_win > 0 else 0.0)
        rr         = avg_win / avg_loss if avg_loss > 0 else 0.0
        win_frac   = len(wins) / n
        expect_r   = win_frac * rr - (1.0 - win_frac)
        total_pnl  = sum(pnls)

        # Daily Sharpe
        daily: Dict[str, float] = {}
        for t in trades:
            d = t.get("close_date") or t.get("open_date")
            if not d:
                continue
            key = str(d)[:10]
            daily[key] = daily.get(key, 0.0) + _pnl(t) / max(starting_capital, 1e-9)
        dr = list(daily.values())
        sharpe = 0.0
        if len(dr) >= 2:
            mean_r = sum(dr) / len(dr)
            var    = sum((r - mean_r) ** 2 for r in dr) / (len(dr) - 1)
            std_r  = math.sqrt(var)
            if std_r > 0:
                sharpe = (mean_r / std_r) * math.sqrt(252)

        # CAGR
        cagr = 0.0
        date_vals = []
        for t in trades:
            d = t.get("close_date") or t.get("open_date")
            if not d:
                continue
            s = str(d)[:19].replace(" ", "T")
            try:
                from datetime import datetime as _dt
                date_vals.append(_dt.fromisoformat(s))
            except ValueError:
                pass
        if len(date_vals) >= 2:
            days = (max(date_vals) - min(date_vals)).total_seconds() / 86400.0
            years = days / 365.25
            if years > 0 and starting_capital > 0:
                final_eq = starting_capital + total_pnl
                if final_eq > 0:
                    cagr = (math.pow(final_eq / starting_capital, 1.0 / years) - 1.0) * 100.0

        return {
            "trades":      n,
            "win_rate":    round(win_rate,  2),
            "profit_factor": round(pf, 4) if pf != float("inf") else None,
            "sharpe":      round(sharpe,   3),
            "expectancy_r": round(expect_r, 4),
            "rr":          round(rr,       3),
            "cagr":        round(cagr,     2),
            "total_pnl":   round(total_pnl, 4),
        }

    # ------------------------------------------------------------------ main entry point

    @classmethod
    def analyze_trades_by_regime(
        cls,
        trades: list,
        regime_df: "pd.DataFrame | None",
        starting_capital: float = 1000.0,
        trade_regimes: "list[str] | None" = None,
    ) -> Dict:
        """
        Group backtest trades by entry-time market regime and compute metrics.

        Parameters
        ----------
        trades           : list of trade dicts from a backtest result JSON.
        regime_df        : DataFrame with ``date`` and ``regime`` columns
                           (output of :meth:`classify_regime`).
        starting_capital : Initial capital used for ROI/CAGR/Sharpe calculations.

        Returns
        -------
        dict with keys:
            regime_periods     – list of {start, end, regime} for chart bands
            per_regime_metrics – {regime: {trades, win_rate, profit_factor, …}}
            equity_series      – [{date, equity, regime}] sorted by close_date
            regime_colors      – color definitions for the frontend
        """
        import bisect

        no_data = (regime_df is None or len(regime_df) == 0) and trade_regimes is None
        if no_data or not trades:
            return {
                "regime_periods":     [],
                "per_regime_metrics": {r: cls._metrics_for([], starting_capital) for r in cls.REGIMES},
                "equity_series":      [],
                "regime_colors":      cls.REGIME_COLORS,
            }

        if trade_regimes is None:
            # Build sorted date → regime lookup arrays from reference pair DataFrame
            raw_dates = pd.to_datetime(regime_df["date"])
            if hasattr(raw_dates.dtype, "tz") and raw_dates.dtype.tz is not None:
                raw_dates = raw_dates.dt.tz_localize(None)
            regime_dates  = raw_dates.to_list()
            regime_labels = regime_df["regime"].to_list()

            def _lookup(dt_str: str) -> str:
                s = str(dt_str)[:19].replace(" ", "T")
                try:
                    from datetime import datetime as _dt
                    dt = _dt.fromisoformat(s)
                except ValueError:
                    return "RANGING"
                idx = bisect.bisect_right(regime_dates, dt) - 1
                if idx < 0:
                    return regime_labels[0] if regime_labels else "RANGING"
                if idx >= len(regime_labels):
                    return regime_labels[-1]
                return regime_labels[idx]

            trade_regimes = [_lookup(t.get("open_date") or "") for t in trades]

        # Group by regime
        groups: Dict[str, list] = {r: [] for r in cls.REGIMES}
        for t, r in zip(trades, trade_regimes):
            if r in groups:
                groups[r].append(t)

        per_regime_metrics = {
            r: cls._metrics_for(groups[r], starting_capital)
            for r in cls.REGIMES
        }

        # Build equity series annotated with regime
        def _pnl(t) -> float:
            v = t.get("profit_abs") or t.get("close_profit_abs") or 0.0
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        sorted_pairs = sorted(
            zip(trades, trade_regimes),
            key=lambda x: str(x[0].get("close_date") or x[0].get("open_date") or ""),
        )

        equity = starting_capital
        equity_series: List[Dict] = []
        for t, r in sorted_pairs:
            p = _pnl(t)
            equity += p
            close_d = t.get("close_date") or t.get("open_date")
            equity_series.append({
                "date":   str(close_d)[:19].replace(" ", "T") if close_d else "",
                "equity": round(equity, 4),
                "pnl":    round(p, 4),
                "regime": r,
            })

        return {
            "regime_periods":     cls.compute_regime_periods(regime_df) if regime_df is not None and len(regime_df) else [],
            "per_regime_metrics": per_regime_metrics,
            "equity_series":      equity_series,
            "regime_colors":      cls.REGIME_COLORS,
        }
