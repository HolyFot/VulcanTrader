// =============================================================================
// EQUITY INDICATORS — Equity Curve Sizing (ECS)
// =============================================================================
//
// Indicator calculations run over the strategy equity curve itself, used to
// drive Equity Curve Sizing — scaling position size up or down based on the
// current performance regime (winning / losing / neutral streak).
// =============================================================================

/// ECP Mode enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EcpMode {
    Disabled,       // No ECP - always use base position size
    RsiOnly,        // RSI-based regime classification
}

impl Default for EcpMode {
    fn default() -> Self {
        EcpMode::Disabled
    }
}

impl EcpMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "disabled" | "none" | "off" => EcpMode::Disabled,
            "rsi" | "rsi_only" | "rsionly" => EcpMode::RsiOnly,
            _ => EcpMode::Disabled,
        }
    }
    
    pub fn as_str(&self) -> &'static str {
        match self {
            EcpMode::Disabled => "disabled",
            EcpMode::RsiOnly => "rsi_only",
        }
    }
}

/// Equity curve metrics structure
#[derive(Debug, Clone, Default)]
pub struct EquityMetrics {
    pub current_equity: f32,
    pub equity_rsi: f32,
    pub equity_rsi_roc: f32,
    pub regime: PerformanceRegime,
    pub streak: i32,           // positive = winning streak, negative = losing streak
    pub peak_equity: f32,
    pub drawdown: f32,         // current drawdown from peak (0.0 to 1.0)
}

impl EquityMetrics {
    pub fn regime_str(&self) -> &str {
        match self.regime {
            PerformanceRegime::Hot => "hot",
            PerformanceRegime::Warm => "warm",
            PerformanceRegime::Neutral => "neutral",
            PerformanceRegime::Cool => "cool",
            PerformanceRegime::Cold => "cold",
        }
    }
}

// =============================================================================
// PERFORMANCE REGIME
// =============================================================================

/// Performance regime for ECP-based position sizing adjustments
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerformanceRegime {
    Hot,      // RSI > 75 - strategy performing exceptionally
    Warm,     // RSI 60-75 - strategy performing well
    Neutral,  // RSI 40-60 - normal performance
    Cool,     // RSI 25-40 - underperforming
    Cold,     // RSI < 25 - in drawdown
}

impl Default for PerformanceRegime {
    fn default() -> Self {
        PerformanceRegime::Neutral
    }
}

impl PerformanceRegime {
    /// Get position size multiplier based on regime
    pub fn position_multiplier(&self) -> f32 {
        match self {
            PerformanceRegime::Hot => 2.0,
            PerformanceRegime::Warm => 1.5,
            PerformanceRegime::Neutral => 1.0,
            PerformanceRegime::Cool => 0.7,
            PerformanceRegime::Cold => 0.4,
        }
    }
    
    pub fn from_rsi(rsi: f32) -> Self {
        if rsi > 75.0 {
            PerformanceRegime::Hot
        } else if rsi > 60.0 {
            PerformanceRegime::Warm
        } else if rsi >= 40.0 {
            PerformanceRegime::Neutral
        } else if rsi >= 25.0 {
            PerformanceRegime::Cool
        } else {
            PerformanceRegime::Cold
        }
    }
}

// =============================================================================
// ECP CONFIG
// =============================================================================

/// ECP (Equity Curve Positioning) Configuration
#[derive(Debug, Clone)]
pub struct EcpConfig {
    pub mode: EcpMode,
    pub min_trades_for_ecp: usize,
    pub rsi_period: usize,
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub bb_period: usize,
    pub max_multiplier: f32,
    pub min_multiplier: f32,
}

impl Default for EcpConfig {
    fn default() -> Self {
        EcpConfig {
            mode: EcpMode::Disabled,
            min_trades_for_ecp: 5,
            rsi_period: 14,
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            bb_period: 20,
            max_multiplier: 3.0,
            min_multiplier: 0.2,
        }
    }
}

// =============================================================================
// EQUITY CURVE TRACKER
// =============================================================================

/// Tracks the equity curve and calculates ECP multiplier
#[derive(Debug, Clone)]
pub struct EquityCurveTracker {
    config: EcpConfig,
    equity_curve: Vec<f32>,
    trade_count: usize,
    current_rsi: f32,
    prev_rsi: f32,
}

impl EquityCurveTracker {
    pub fn new(config: EcpConfig) -> Self {
        EquityCurveTracker {
            config,
            equity_curve: vec![1.0],
            trade_count: 0,
            current_rsi: 50.0,
            prev_rsi: 50.0,
        }
    }
    
    /// Add a completed trade (profit as decimal, e.g., 0.05 = 5%)
    pub fn add_trade(&mut self, profit: f32) {
        self.trade_count += 1;
        let last = *self.equity_curve.last().unwrap_or(&1.0);
        self.equity_curve.push(last * (1.0 + profit));
        
        // Calculate equity RSI
        if self.equity_curve.len() >= self.config.rsi_period + 1 {
            self.prev_rsi = self.current_rsi;
            self.current_rsi = self.calculate_rsi(self.config.rsi_period);
        }
    }
    
    fn calculate_rsi(&self, period: usize) -> f32 {
        let n = self.equity_curve.len();
        if n < period + 1 {
            return 50.0;
        }
        
        let mut gains = 0.0f32;
        let mut losses = 0.0f32;
        
        for i in (n - period)..n {
            let change = self.equity_curve[i] - self.equity_curve[i - 1];
            if change > 0.0 { gains += change; } else { losses += -change; }
        }
        
        let avg_gain = gains / period as f32;
        let avg_loss = losses / period as f32;
        
        if avg_loss == 0.0 {
            if avg_gain > 0.0 { 100.0 } else { 50.0 }
        } else {
            100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
        }
    }
    
    /// Get current performance regime
    pub fn get_regime(&self) -> PerformanceRegime {
        PerformanceRegime::from_rsi(self.current_rsi)
    }
    
    /// Get position size multiplier based on ECP
    pub fn get_position_multiplier(&self) -> f32 {
        if self.config.mode == EcpMode::Disabled || self.trade_count < self.config.min_trades_for_ecp {
            return 1.0;
        }
        
        let regime = self.get_regime();
        let raw_mult = regime.position_multiplier();
        raw_mult.clamp(self.config.min_multiplier, self.config.max_multiplier)
    }
    
    /// Get equity curve metrics
    pub fn get_metrics(&self) -> EquityMetrics {
        let peak = self.equity_curve.iter().cloned().fold(0.0f32, f32::max);
        let current = *self.equity_curve.last().unwrap_or(&1.0);
        let dd = if peak > 0.0 { (peak - current) / peak } else { 0.0 };
        let regime = self.get_regime();
        let streak = self.calculate_streak();
        
        EquityMetrics {
            current_equity: current,
            equity_rsi: self.current_rsi,
            equity_rsi_roc: self.current_rsi - self.prev_rsi,
            regime,
            streak,
            peak_equity: peak,
            drawdown: dd,
        }
    }
    
    fn calculate_streak(&self) -> i32 {
        let n = self.equity_curve.len();
        if n < 2 { return 0; }
        
        let mut streak = 0i32;
        for i in (1..n).rev() {
            let change = self.equity_curve[i] - self.equity_curve[i - 1];
            if change > 0.0 {
                if streak < 0 { break; }
                streak += 1;
            } else if change < 0.0 {
                if streak > 0 { break; }
                streak -= 1;
            } else {
                break;
            }
        }
        streak
    }
}

// =============================================================================
// EQUITY CURVE SIZING (ECS) — faithful port of Python's EquityCurveSizing
// =============================================================================
//
// Distinct from the EcpMode/EquityCurveTracker above (which predates this and
// uses a simpler average-gain/average-loss RSI over a fixed window). This is
// a direct port of `user_data/strategies/risk_management.py::EquityCurveSizing`,
// used by every one of the ~20 originally-ported Python strategies'
// `leverage()`/`custom_stake_amount()` callbacks — Mode2 needs it for real
// parity, since none of those strategies ever actually run at a fixed
// leverage in the real backtest.
//
// Known approximation: Python's `_ecs_engine` lives on the STRATEGY instance,
// so its equity history is the whole PORTFOLIO's balance trajectory shared
// across every pair traded concurrently (with real freqtrade's
// `max_open_trades` cap). This engine backtests one pair at a time with
// independent capital, so equity history here is necessarily per-pair — an
// exact match only when validating against a genuine single-pair Python run.

/// pandas' `Series.ewm(span=span).mean()` with the (pandas-default) adjusted
/// formula, returning just the mean at the last point: a weighted average of
/// `values` with weight (1-alpha)^k for the k-th most recent point,
/// alpha = 2/(span+1).
fn ewm_mean_adjusted(values: &[f32], span: f32) -> f32 {
    if values.is_empty() { return 0.0; }
    let alpha = 2.0 / (span + 1.0);
    let mut weighted_sum = 0.0f64;
    let mut weight_sum = 0.0f64;
    let mut w = 1.0f64;
    for &v in values.iter().rev() {
        weighted_sum += w * v as f64;
        weight_sum += w;
        w *= (1.0 - alpha) as f64;
    }
    (weighted_sum / weight_sum) as f32
}

/// Port of `EquityCurveSizing.calculate_metrics()`'s equity_rsi + regime
/// derivation. `equity_history` is a sequence of `balance - starting_balance`
/// snapshots (Python samples this at each new trade's `custom_stake_amount`
/// call); needs >= 14 points or it returns the same NEUTRAL/50 default
/// Python does.
pub fn ecs_rsi_and_regime(equity_history: &[f32]) -> (f32, PerformanceRegime) {
    if equity_history.len() < 14 {
        return (50.0, PerformanceRegime::Neutral);
    }
    let n = equity_history.len();
    let mut gains = vec![0.0f32; n - 1];
    let mut losses = vec![0.0f32; n - 1];
    for i in 1..n {
        let delta = equity_history[i] - equity_history[i - 1];
        if delta > 0.0 { gains[i - 1] = delta; } else if delta < 0.0 { losses[i - 1] = -delta; }
    }
    let avg_gain = ewm_mean_adjusted(&gains, 14.0);
    let avg_loss = ewm_mean_adjusted(&losses, 14.0);
    let rs = avg_gain / (avg_loss + 1e-10);
    let equity_rsi = 100.0 - (100.0 / (1.0 + rs));

    // Same threshold order Python checks in (>75 before <25 etc. doesn't
    // matter here since the bands are mutually exclusive, but matched anyway).
    let regime = if equity_rsi > 75.0 { PerformanceRegime::Hot }
        else if equity_rsi < 25.0 { PerformanceRegime::Cold }
        else if equity_rsi > 60.0 { PerformanceRegime::Warm }
        else if equity_rsi < 40.0 { PerformanceRegime::Cool }
        else { PerformanceRegime::Neutral };
    (equity_rsi, regime)
}

/// Port of `EquityCurveSizing.get_leverage_multiplier` — note this is a
/// DIFFERENT table from the stake multiplier below, and the `equity_rsi > 75`
/// short-circuit means the regime-table's own HOT entry is dead code in
/// practice (Hot itself requires rsi > 75).
pub fn ecs_leverage_multiplier(regime: PerformanceRegime, equity_rsi: f32) -> f32 {
    if equity_rsi > 75.0 { return 1.3; }
    match regime {
        PerformanceRegime::Hot => 1.2,
        PerformanceRegime::Warm => 1.1,
        PerformanceRegime::Neutral => 1.0,
        PerformanceRegime::Cool => 0.7,
        PerformanceRegime::Cold => 0.5,
    }
}

/// Port of `EquityCurveSizing.get_stake_multiplier`.
pub fn ecs_stake_multiplier(regime: PerformanceRegime) -> f32 {
    match regime {
        PerformanceRegime::Hot => 2.0,
        PerformanceRegime::Warm => 1.5,
        PerformanceRegime::Neutral => 1.0,
        PerformanceRegime::Cool => 0.7,
        PerformanceRegime::Cold => 0.4,
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ecp_disabled_returns_one() {
        let config = EcpConfig { mode: EcpMode::Disabled, ..Default::default() };
        let tracker = EquityCurveTracker::new(config);
        assert_eq!(tracker.get_position_multiplier(), 1.0);
    }
    
    #[test]
    fn test_ecp_with_trades() {
        let config = EcpConfig {
            mode: EcpMode::RsiOnly,
            min_trades_for_ecp: 3,
            ..Default::default()
        };
        let mut tracker = EquityCurveTracker::new(config);
        
        // Add some winning trades
        tracker.add_trade(0.05);
        tracker.add_trade(0.03);
        tracker.add_trade(0.04);
        tracker.add_trade(0.02);
        tracker.add_trade(0.06);
        
        let mult = tracker.get_position_multiplier();
        assert!(mult > 0.0);
        assert!(mult <= 3.0);
    }
    
    #[test]
    fn test_ecp_below_min_trades() {
        let config = EcpConfig {
            mode: EcpMode::RsiOnly,
            min_trades_for_ecp: 10,
            ..Default::default()
        };
        let mut tracker = EquityCurveTracker::new(config);
        tracker.add_trade(0.05);
        assert_eq!(tracker.get_position_multiplier(), 1.0);
    }
}