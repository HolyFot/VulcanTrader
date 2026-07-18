// =============================================================================
// BACKTEST TYPES — shared data structures for the backtesting engine
// =============================================================================
//
// Defines the core types consumed by `cpu_engine` and the analytics layer:
//   - BacktestResult / TradeRecord aggregates
//   - EntryReason / ExitReason enums
//   - Strategy, Unified, and Custom-Exit configs
//   - Trailing stop, ROI, and stoploss configs
// =============================================================================

/// Backtest result structure
#[derive(Debug, Clone)]
pub struct BacktestResult {
    pub entry_indices: Vec<i32>,
    pub exit_indices: Vec<i32>,
    pub profits: Vec<f32>,
    pub pnl_amounts: Vec<f32>,
    pub entry_prices: Vec<f32>,
    pub exit_prices: Vec<f32>,
    pub exit_reasons: Vec<ExitReason>,
    pub entry_reasons: Vec<EntryReason>,
    pub leverages: Vec<f32>,
    pub durations: Vec<i32>,
}

/// Exit reason enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExitReason {
    Signal,
    RoiTarget,
    Stoploss,
    TrailingStop,
    MaxHoldPeriod,
    CciExit,
    RsiExit,
    MacdExit,
    CustomExit,
}

/// Entry reason enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryReason {
    Signal,
    RsiOversold,
    RsiOverbought,
    CciSignal,
    MacdCross,
    BollingerBand,
    CustomEntry,
}

/// Statistics for a specific exit/entry type
#[derive(Debug, Clone, Default)]
pub struct TypeStats {
    pub count: usize,
    pub total_pnl: f32,
    pub avg_profit_pct: f32,
    pub avg_duration: f32,
    pub wins: usize,
    pub draws: usize,
    pub losses: usize,
    pub win_rate: f32,
}

impl BacktestResult {
    pub fn new() -> Self {
        BacktestResult {
            entry_indices: Vec::new(),
            exit_indices: Vec::new(),
            profits: Vec::new(),
            pnl_amounts: Vec::new(),
            entry_prices: Vec::new(),
            exit_prices: Vec::new(),
            exit_reasons: Vec::new(),
            entry_reasons: Vec::new(),
            leverages: Vec::new(),
            durations: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.profits.is_empty()
    }

    pub fn trade_count(&self) -> usize {
        self.profits.len()
    }

    pub fn exits_by_reason(&self) -> std::collections::HashMap<ExitReason, usize> {
        let mut counts = std::collections::HashMap::new();
        for reason in &self.exit_reasons {
            *counts.entry(*reason).or_insert(0) += 1;
        }
        counts
    }

    pub fn exit_type_stats(&self) -> std::collections::HashMap<ExitReason, TypeStats> {
        let mut stats: std::collections::HashMap<ExitReason, TypeStats> = std::collections::HashMap::new();
        for i in 0..self.profits.len() {
            let reason = self.exit_reasons.get(i).copied().unwrap_or(ExitReason::Signal);
            let profit = self.profits[i];
            let duration = self.durations.get(i).copied().unwrap_or(0) as f32;
            let entry = stats.entry(reason).or_insert_with(TypeStats::default);
            entry.count += 1;
            entry.total_pnl += profit;
            entry.avg_duration += duration;
            if profit > 0.001 {
                entry.wins += 1;
            } else if profit < -0.001 {
                entry.losses += 1;
            } else {
                entry.draws += 1;
            }
        }
        for stat in stats.values_mut() {
            if stat.count > 0 {
                stat.avg_profit_pct = stat.total_pnl / stat.count as f32;
                stat.avg_duration = stat.avg_duration / stat.count as f32;
                stat.win_rate = stat.wins as f32 / stat.count as f32;
            }
        }
        stats
    }

    pub fn entry_type_stats(&self) -> std::collections::HashMap<EntryReason, TypeStats> {
        let mut stats: std::collections::HashMap<EntryReason, TypeStats> = std::collections::HashMap::new();
        for i in 0..self.profits.len() {
            let reason = self.entry_reasons.get(i).copied().unwrap_or(EntryReason::Signal);
            let profit = self.profits[i];
            let duration = self.durations.get(i).copied().unwrap_or(0) as f32;
            let entry = stats.entry(reason).or_insert_with(TypeStats::default);
            entry.count += 1;
            entry.total_pnl += profit;
            entry.avg_duration += duration;
            if profit > 0.001 {
                entry.wins += 1;
            } else if profit < -0.001 {
                entry.losses += 1;
            } else {
                entry.draws += 1;
            }
        }
        for stat in stats.values_mut() {
            if stat.count > 0 {
                stat.avg_profit_pct = stat.total_pnl / stat.count as f32;
                stat.avg_duration = stat.avg_duration / stat.count as f32;
                stat.win_rate = stat.wins as f32 / stat.count as f32;
            }
        }
        stats
    }
}

/// Trade type enumeration - spot or futures
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub enum TradeType {
    Spot,
    Futures,
}

impl Default for TradeType {
    fn default() -> Self {
        TradeType::Futures
    }
}

impl TradeType {
    pub fn can_short(&self) -> bool {
        matches!(self, TradeType::Futures)
    }

    pub fn effective_leverage(&self, requested: f32) -> f32 {
        match self {
            TradeType::Spot => 1.0,
            TradeType::Futures => requested,
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "spot" => TradeType::Spot,
            "futures" | "perpetual" | "perp" | "margin" => TradeType::Futures,
            _ => TradeType::Futures,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TradeType::Spot => "spot",
            TradeType::Futures => "futures",
        }
    }
}

/// Strategy configuration for backtesting
#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub name: String,
    pub timeframe: String,
    pub timeframe_minutes: usize,
    pub trade_type: TradeType,
    pub can_short: bool,
    pub startup_candle_count: usize,
    pub process_only_new_candles: bool,
    pub leverage_default: f32,
    pub leverage_max: f32,
    pub leverage_mode: LeverageMode,
    pub fee_rate: f32,
    pub fee_maker: f32,
    pub fee_taker: f32,
    // Exit settings
    pub stoploss: f32,
    pub trailing_stop: bool,
    pub trailing_stop_positive: f32,
    pub trailing_stop_positive_offset: f32,
    pub minimal_roi: std::collections::HashMap<String, f32>,
    pub max_hold_period: usize,
    // Advanced features
    pub ecp_enabled: bool,
    pub ecp_mode: String,
    pub hawkes_enabled: bool,
    // ATR-anchored-to-entry custom stoploss (mirrors the `custom_stoploss`
    // pattern used by most Python user_data/strategies: stop_price =
    // entry_price -/+ atr_stop_multiplier * ATR(current bar), floored by
    // `stoploss` so it never loosens beyond the base stop).
    pub atr_stop_enabled: bool,
    pub atr_stop_multiplier: f32,
    pub entry_edge_filter: EntryEdgeFilter,
}

/// Optional throttle on re-entry frequency, layered on top of the engine's
/// normal entry check (which by default re-enters on any bar the signal is
/// active and no position is open, matching real freqtrade). Applied
/// per-strategy, not globally — most strategies leave this at `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EntryEdgeFilter {
    #[default]
    None,
    /// Only enter on the exact bar the signal transitions 0->1. For any
    /// contiguous run of bars where the signal stays active, this allows
    /// exactly one entry attempt for the whole run — even if that trade
    /// closes early, re-entry is blocked until the signal drops back to 0
    /// and rises again.
    Full,
    /// Block re-entry for `n` bars after any exit, regardless of the
    /// signal's own state. Lighter than `Full`: it still prevents immediate
    /// re-entry into a setup that just closed (the main revenge-trading
    /// case `Full` also blocks), but doesn't require the entire signal
    /// episode to reset before trying again.
    Cooldown(usize),
}

/// Leverage calculation mode
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LeverageMode {
    Fixed,
    SignalQuality,
    Volatility,
    Custom,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        StrategyConfig {
            name: "default".into(),
            timeframe: "15m".to_string(),
            timeframe_minutes: 15,
            trade_type: TradeType::Futures,
            can_short: true,
            startup_candle_count: 50,
            process_only_new_candles: true,
            leverage_default: 1.0,
            leverage_max: 10.0,
            leverage_mode: LeverageMode::Fixed,
            fee_rate: 0.0005,
            fee_maker: 0.0002,
            fee_taker: 0.0005,
            stoploss: -0.10,
            trailing_stop: false,
            trailing_stop_positive: 0.015,
            trailing_stop_positive_offset: 0.025,
            minimal_roi: {
                let mut m = std::collections::HashMap::new();
                m.insert("0".into(), 0.06);
                m
            },
            max_hold_period: 96,
            ecp_enabled: false,
            ecp_mode: "disabled".into(),
            hawkes_enabled: false,
            atr_stop_enabled: false,
            atr_stop_multiplier: 2.3,
            entry_edge_filter: EntryEdgeFilter::None,
        }
    }
}

impl StrategyConfig {
    pub fn new(timeframe: &str) -> Self {
        let timeframe_minutes = Self::parse_timeframe(timeframe);
        StrategyConfig {
            timeframe: timeframe.to_string(),
            timeframe_minutes,
            ..Default::default()
        }
    }

    pub fn with_trade_type(mut self, trade_type: TradeType) -> Self {
        self.trade_type = trade_type;
        if trade_type == TradeType::Spot {
            self.can_short = false;
            self.leverage_default = 1.0;
            self.leverage_max = 1.0;
        }
        self
    }

    pub fn effective_leverage(&self) -> f32 {
        self.trade_type.effective_leverage(self.leverage_default)
    }

    pub fn shorting_allowed(&self) -> bool {
        self.trade_type.can_short() && self.can_short
    }
    
    /// Check if this strategy trades spot only
    pub fn is_spot(&self) -> bool {
        matches!(self.trade_type, TradeType::Spot)
    }

    pub fn parse_timeframe(tf: &str) -> usize {
        let tf_trimmed = tf.trim();
        if tf_trimmed.is_empty() {
            return 15;
        }
        let unit = tf_trimmed.chars().last().unwrap();
        let amount_str = &tf_trimmed[..tf_trimmed.len() - 1];
        let amount: usize = amount_str.parse().unwrap_or(1);
        let seconds = match unit {
            'y' => amount * 60 * 60 * 24 * 365,
            'M' => amount * 60 * 60 * 24 * 30,
            'w' => amount * 60 * 60 * 24 * 7,
            'd' => amount * 60 * 60 * 24,
            'h' => amount * 60 * 60,
            'm' => amount * 60,
            's' => amount,
            _ => match unit.to_ascii_lowercase() {
                'm' => amount * 60,
                'h' => amount * 60 * 60,
                'd' => amount * 60 * 60 * 24,
                _ => 15 * 60,
            },
        };
        std::cmp::max(1, seconds / 60)
    }

    pub fn with_leverage(mut self, default: f32, max: f32, mode: LeverageMode) -> Self {
        self.leverage_default = default;
        self.leverage_max = max;
        self.leverage_mode = mode;
        self
    }

    pub fn with_fees(mut self, maker: f32, taker: f32) -> Self {
        self.fee_maker = maker;
        self.fee_taker = taker;
        self.fee_rate = taker;
        self
    }
}

// =============================================================================
// CUSTOM EXIT AND STOPLOSS CONFIGS
// =============================================================================

/// Custom exit configuration
#[derive(Debug, Clone)]
pub struct CustomExitConfig {
    pub enabled: bool,
    pub cci_exit_enabled: bool,
    pub cci_overbought: f32,
    pub cci_oversold: f32,
    pub rsi_exit_enabled: bool,
    pub rsi_overbought: f32,
    pub rsi_oversold: f32,
    pub macd_reversal_exit: bool,
    pub custom_conditions: Vec<(i32, f32, f32)>,
}

impl Default for CustomExitConfig {
    fn default() -> Self {
        CustomExitConfig {
            enabled: false,
            cci_exit_enabled: false,
            cci_overbought: 100.0,
            cci_oversold: -100.0,
            rsi_exit_enabled: false,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,
            macd_reversal_exit: false,
            custom_conditions: Vec::new(),
        }
    }
}

impl CustomExitConfig {
    pub fn with_cci_exit(mut self, overbought: f32, oversold: f32) -> Self {
        self.enabled = true;
        self.cci_exit_enabled = true;
        self.cci_overbought = overbought;
        self.cci_oversold = oversold;
        self
    }

    pub fn with_rsi_exit(mut self, overbought: f32, oversold: f32) -> Self {
        self.enabled = true;
        self.rsi_exit_enabled = true;
        self.rsi_overbought = overbought;
        self.rsi_oversold = oversold;
        self
    }

    pub fn with_macd_reversal(mut self) -> Self {
        self.enabled = true;
        self.macd_reversal_exit = true;
        self
    }
}

/// Custom stoploss configuration
#[derive(Debug, Clone)]
pub struct CustomStoplossConfig {
    pub enabled: bool,
    pub atr_based: bool,
    pub atr_multiplier: f32,
    pub breakeven_trigger: f32,
    pub profit_lock_trigger: f32,
    pub profit_lock_ratio: f32,
    pub time_decay_enabled: bool,
    pub time_decay_start_bars: usize,
    pub time_decay_per_bar: f32,
    pub min_stoploss: f32,
    pub max_stoploss: f32,
}

impl Default for CustomStoplossConfig {
    fn default() -> Self {
        CustomStoplossConfig {
            enabled: false,
            atr_based: false,
            atr_multiplier: 2.0,
            breakeven_trigger: 0.02,
            profit_lock_trigger: 0.04,
            profit_lock_ratio: 0.5,
            time_decay_enabled: false,
            time_decay_start_bars: 20,
            time_decay_per_bar: 0.001,
            min_stoploss: -0.02,
            max_stoploss: -0.10,
        }
    }
}

impl CustomStoplossConfig {
    pub fn with_atr(mut self, multiplier: f32) -> Self {
        self.enabled = true;
        self.atr_based = true;
        self.atr_multiplier = multiplier;
        self
    }

    pub fn with_breakeven(mut self, trigger: f32) -> Self {
        self.enabled = true;
        self.breakeven_trigger = trigger;
        self
    }

    pub fn with_profit_lock(mut self, trigger: f32, ratio: f32) -> Self {
        self.enabled = true;
        self.profit_lock_trigger = trigger;
        self.profit_lock_ratio = ratio;
        self
    }

    pub fn with_time_decay(mut self, start_bars: usize, per_bar: f32) -> Self {
        self.enabled = true;
        self.time_decay_enabled = true;
        self.time_decay_start_bars = start_bars;
        self.time_decay_per_bar = per_bar;
        self
    }

    pub fn calculate_stoploss(
        &self,
        base_stoploss: f32,
        entry_price: f32,
        _current_price: f32,
        current_profit: f32,
        bars_held: usize,
        atr: f32,
        _is_long: bool,
    ) -> f32 {
        if !self.enabled {
            return base_stoploss;
        }
        let mut dynamic_sl = base_stoploss;
        if self.atr_based && entry_price > 0.0 {
            let atr_sl = -(atr * self.atr_multiplier) / entry_price;
            dynamic_sl = dynamic_sl.max(atr_sl);
        }
        if current_profit >= self.breakeven_trigger {
            dynamic_sl = dynamic_sl.max(-0.005);
        }
        if current_profit >= self.profit_lock_trigger {
            let lock_level = current_profit * self.profit_lock_ratio;
            if lock_level > 0.0 {
                dynamic_sl = dynamic_sl.max(-lock_level);
            }
        }
        if self.time_decay_enabled && bars_held > self.time_decay_start_bars {
            let time_tightening = ((bars_held - self.time_decay_start_bars) as f32 * self.time_decay_per_bar)
                .min(0.02);
            dynamic_sl = dynamic_sl.max(base_stoploss + time_tightening);
        }
        dynamic_sl = dynamic_sl.max(self.max_stoploss).min(self.min_stoploss);
        dynamic_sl
    }
}

// =============================================================================
// ROI AND TRAILING STOP CONFIGS
// =============================================================================

/// ROI configuration for ROI-based exits
#[derive(Debug, Clone, Copy)]
pub struct RoiConfig {
    pub roi_6: f32,
    pub roi_3: f32,
    pub roi_15: f32,
    pub roi_720: f32,
    pub period_0: usize,
    pub period_10: usize,
    pub period_30: usize,
    pub period_720: usize,
    pub max_hold_period: usize,
}

impl Default for RoiConfig {
    fn default() -> Self {
        RoiConfig {
            roi_6: 0.06,
            roi_3: 0.03,
            roi_15: 0.015,
            roi_720: 0.005,
            period_0: 0,
            period_10: 1,
            period_30: 2,
            period_720: 48,
            max_hold_period: 96,
        }
    }
}

/// Trailing stop configuration
#[derive(Debug, Clone, Copy)]
pub struct TrailingStopConfig {
    pub enabled: bool,
    pub trigger: f32,
    pub offset: f32,
    pub only_offset_reached: bool,
}

impl Default for TrailingStopConfig {
    fn default() -> Self {
        TrailingStopConfig {
            enabled: false,
            trigger: 0.015,
            offset: 0.025,
            only_offset_reached: true,
        }
    }
}

impl TrailingStopConfig {
    pub fn new(enabled: bool, trigger: f32, offset: f32, only_offset_reached: bool) -> Self {
        TrailingStopConfig {
            enabled,
            trigger,
            offset,
            only_offset_reached,
        }
    }
}

// =============================================================================
// UNIFIED BACKTEST CONFIGURATION
// =============================================================================

/// Unified backtest configuration combining all settings
#[derive(Debug, Clone)]
pub struct UnifiedBacktestConfig {
    // === Core Settings ===
    pub timeframe: String,
    pub timeframe_minutes: usize,
    pub trade_type: TradeType,
    pub direction: String,
    pub startup_candle_count: usize,

    // === Leverage Settings ===
    pub leverage_default: f32,
    pub leverage_max: f32,
    pub leverage_mode: LeverageMode,

    // === Fee Settings ===
    pub fee_taker: f32,
    pub fee_maker: f32,
    pub fee_mode: String,
    pub fee_dollars_taker_per_contract: f32,
    pub fee_dollars_maker_per_contract: f32,
    pub symbol: String,

    // === ROI Settings ===
    pub roi_enabled: bool,
    pub roi_6: f32,
    pub roi_3: f32,
    pub roi_15: f32,
    pub roi_720: f32,
    pub roi_period_0: usize,
    pub roi_period_10: usize,
    pub roi_period_30: usize,
    pub roi_period_720: usize,
    pub max_hold_period: usize,

    // === Stoploss Settings ===
    pub base_stoploss: f32,
    pub min_stoploss: f32,
    pub max_stoploss: f32,

    // === ATR-anchored-to-entry custom stoploss ===
    pub atr_stop_enabled: bool,
    pub atr_stop_multiplier: f32,

    // === Optional per-strategy entry throttle — see EntryEdgeFilter ===
    pub entry_edge_filter: EntryEdgeFilter,

    /// Bar index before which no trade may open, distinct from
    /// `startup_candle_count` (which only gates indicator validity). Lets a
    /// caller feed extra warmup history before a validation window — matching
    /// freqtrade's `--timerange`, where indicators see the full history but
    /// the simulation loop itself never opens a trade before the range start
    /// — without which a position opened during the "warmup" stretch can
    /// still be held into the window, silently absorbing the real first
    /// signal. 0 (default) means no additional restriction.
    pub eval_start_bar: usize,

    // === Trailing Stop Settings ===
    pub trailing_enabled: bool,
    pub trailing_trigger: f32,
    pub trailing_offset: f32,

    // === Time Decay Stoploss ===
    pub time_decay_enabled: bool,
    pub time_decay_start_bars: usize,
    pub time_decay_per_bar: f32,

    // === Profit Lock Settings ===
    pub profit_lock_enabled: bool,
    pub profit_lock_trigger: f32,
    pub profit_lock_ratio: f32,

    // === Custom Exit Settings ===
    pub cci_exit_enabled: bool,
    pub cci_overbought: f32,
    pub cci_oversold: f32,
    pub rsi_exit_enabled: bool,
    pub rsi_overbought: f32,
    pub rsi_oversold: f32,
    pub macd_reversal_exit: bool,

    // === ECP Settings ===
    pub ecp_mode: String,
    pub ecp_min_trades: usize,
    pub ecp_max_multiplier: f32,
    pub ecp_min_multiplier: f32,

    // === Compounding Settings ===
    pub compounding_enabled: bool,
    pub starting_balance: f32,
    pub tradable_balance_ratio: f32,
    pub max_trade_amount: f32,
    pub max_open_capital: f32,
}

impl Default for UnifiedBacktestConfig {
    fn default() -> Self {
        UnifiedBacktestConfig {
            // Core
            timeframe: "15m".to_string(),
            timeframe_minutes: 15,
            trade_type: TradeType::Futures,
            direction: "long".to_string(),
            startup_candle_count: 50,

            // Leverage
            leverage_default: 1.0,
            leverage_max: 10.0,
            leverage_mode: LeverageMode::Fixed,

            // Fees
            fee_taker: 0.0005,
            fee_maker: 0.0002,
            fee_mode: "percent".to_string(),
            fee_dollars_taker_per_contract: 2.25,
            fee_dollars_maker_per_contract: 2.25,
            symbol: String::new(),

            // ROI
            roi_enabled: true,
            roi_6: 0.06,
            roi_3: 0.03,
            roi_15: 0.015,
            roi_720: 0.005,
            roi_period_0: 0,
            roi_period_10: 1,
            roi_period_30: 2,
            roi_period_720: 48,
            max_hold_period: 96,

            // Stoploss
            base_stoploss: -0.10,
            min_stoploss: -0.01,
            max_stoploss: -0.15,

            // ATR-anchored-to-entry stoploss
            atr_stop_enabled: false,
            atr_stop_multiplier: 2.3,

            entry_edge_filter: EntryEdgeFilter::None,
            eval_start_bar: 0,

            // Trailing
            trailing_enabled: false,
            trailing_trigger: 0.015,
            trailing_offset: 0.025,

            // Time decay
            time_decay_enabled: false,
            time_decay_start_bars: 10,
            time_decay_per_bar: 0.001,

            // Profit lock
            profit_lock_enabled: false,
            profit_lock_trigger: 0.03,
            profit_lock_ratio: 0.5,

            // Custom exits
            cci_exit_enabled: false,
            cci_overbought: 100.0,
            cci_oversold: -100.0,
            rsi_exit_enabled: false,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,
            macd_reversal_exit: false,

            // ECP
            ecp_mode: "disabled".to_string(),
            ecp_min_trades: 3,
            ecp_max_multiplier: 3.0,
            ecp_min_multiplier: 0.2,

            // Compounding
            compounding_enabled: true,
            starting_balance: 10000.0,
            tradable_balance_ratio: 1.0,
            max_trade_amount: f32::MAX,
            max_open_capital: f32::MAX,
        }
    }
}

impl UnifiedBacktestConfig {
    pub fn to_roi_config(&self) -> RoiConfig {
        RoiConfig {
            roi_6: self.roi_6,
            roi_3: self.roi_3,
            roi_15: self.roi_15,
            roi_720: self.roi_720,
            period_0: self.roi_period_0,
            period_10: self.roi_period_10,
            period_30: self.roi_period_30,
            period_720: self.roi_period_720,
            max_hold_period: self.max_hold_period,
        }
    }

    pub fn to_trailing_config(&self) -> TrailingStopConfig {
        TrailingStopConfig {
            enabled: self.trailing_enabled,
            trigger: self.trailing_trigger,
            offset: self.trailing_offset,
            only_offset_reached: true,
        }
    }

    pub fn is_long(&self) -> bool {
        self.direction.to_lowercase() != "short"
    }

    pub fn parse_leverage_mode(mode: &str) -> LeverageMode {
        match mode.to_lowercase().as_str() {
            "fixed" => LeverageMode::Fixed,
            "signal_quality" | "signal" => LeverageMode::SignalQuality,
            "volatility" | "atr" => LeverageMode::Volatility,
            "custom" => LeverageMode::Custom,
            _ => LeverageMode::Fixed,
        }
    }
}

// =============================================================================
// STRATEGY TRAIT
// =============================================================================

/// Context provided to strategy signal functions
pub struct SignalContext {
    pub indicators: std::sync::Arc<std::collections::HashMap<usize, Vec<f32>>>,
    pub close:  std::sync::Arc<Vec<f32>>,
    pub open:   std::sync::Arc<Vec<f32>>,
    pub high:   std::sync::Arc<Vec<f32>>,
    pub low:    std::sync::Arc<Vec<f32>>,
    pub volume: std::sync::Arc<Vec<f32>>,
    pub n: usize,
}

/// Trait that every strategy must implement
pub trait Strategy: Send + Sync {
    /// Return the strategy's configuration
    fn config(&self) -> &StrategyConfig;
    
    /// Calculate any custom indicators the strategy needs.
    /// Returns a map: indicator_index -> values (len = close.len())
    fn calculate_custom_indicators(
        &self,
        close: &[f32],
        open: &[f32],
        high: &[f32],
        low: &[f32],
        volume: &[f32],
    ) -> std::collections::HashMap<usize, Vec<f32>>;
    
    /// Populate entry signals.
    /// Returns (long_entries, short_entries) where each is a Vec<u8> of length n
    ///   entry = 1 -> enter on this bar, 0 -> no entry
    fn populate_entry_trend(&self, ctx: &SignalContext) -> (Vec<u8>, Vec<u8>);
    
    /// Populate exit signals for the given direction.
    /// `entry_indices` and `entry_prices` provide context for conditional exits.
    /// Returns a Vec<u8> of length n: 1 -> exit on this bar, 0 -> hold
    fn populate_exit_trend(
        &self,
        ctx: &SignalContext,
        direction: &str,
        entry_indices: &[usize],
        entry_prices: &[f32],
    ) -> Vec<u8>;
}

// =============================================================================
// TESTS
// =============================================================================

// =============================================================================
// PARAMETER SWEEP GRID
// =============================================================================

/// Grid of parameter combinations for automated parameter sweeping.
/// `run_param_sweep` in `cpu_engine` evaluates every (stoploss × roi_target × trailing)
/// combo in parallel via rayon and returns the best-scoring result.
#[derive(Debug, Clone)]
pub struct ParamGrid {
    /// Stoploss levels to test (negative fractions, e.g. -0.05 = 5% loss).
    pub stoplosses: Vec<f32>,
    /// ROI tier configurations. Each entry is a sequence of (minute_offset, target_pct) pairs
    /// in ascending time order. Empty inner vec = disable ROI exits entirely.
    pub roi_tiers: Vec<Vec<(usize, f32)>>,
    /// Trailing stop options: (enabled, trigger_pct, offset_pct).
    pub trailing_configs: Vec<(bool, f32, f32)>,
}

/// Evenly spaced values from `min` to `max` inclusive (n points, n >= 1).
fn linspace(min: f32, max: f32, n: usize) -> Vec<f32> {
    if n <= 1 { return vec![min]; }
    let step = (max - min) / (n - 1) as f32;
    (0..n).map(|i| min + step * i as f32).collect()
}

impl Default for ParamGrid {
    fn default() -> Self {
        // Target ~300 total sweep iterations, varying only stoploss and the first
        // two ROI tiers (immediate @0m + a second tier @30m). Trailing is fixed off.
        // 7 stoplosses x (7 tier-1 values x 6 tier-2 values) x 1 trailing = 294 combos.
        let stoplosses: Vec<f32> = vec![-0.02, -0.03, -0.05, -0.07, -0.10, -0.12, -0.15];

        let roi1_vals = linspace(0.010, 0.080, 7); // tier 1 (@0m): 1%..8%, step ~1.17%
        let roi2_vals = linspace(0.005, 0.040, 6); // tier 2 (@30m): 0.5%..4%, step ~0.7%

        let mut roi_tiers = Vec::with_capacity(roi1_vals.len() * roi2_vals.len());
        for &r1 in &roi1_vals {
            for &r2 in &roi2_vals {
                roi_tiers.push(vec![(0, r1), (30, r2)]);
            }
        }

        ParamGrid {
            stoplosses,
            roi_tiers,
            trailing_configs: vec![(false, 0.0, 0.0)],
        }
    }
}

impl ParamGrid {
    pub fn combo_count(&self) -> usize {
        self.stoplosses.len() * self.roi_tiers.len() * self.trailing_configs.len()
    }
}

// =============================================================================

#[cfg(test)]
#[path = "backtest_tests.rs"]
mod tests;