// =============================================================================
// BACKTESTING ENGINE - Unified Backtesting Engine
// =============================================================================
//
// Standalone trading engine module containing the backtesting implementation:
//
// - ECP (Equity Curve Positioning) for dynamic position sizing
// - ROI Tables (time-based profit targets)
// - Trailing Stops (standard)
// - Compounding with position size caps
// - Custom indicator exits (CCI, RSI, MACD)
// - Time decay stoplosses
// - Profit lock mechanisms
// =============================================================================

use std::collections::HashMap;

use super::backtest::{
    BacktestResult, ExitReason, EntryReason,
    UnifiedBacktestConfig,
};

// =============================================================================
// TRADING ENGINE RESULT STRUCTURES
// =============================================================================

/// Result from unified backtest with comprehensive metrics
#[derive(Debug, Clone)]
pub struct TradingEngineResult {
    /// Core backtest result with all trades
    pub backtest_result: BacktestResult,
    /// ECP mode used
    pub ecp_mode: String,
    /// Number of time decay adjustments
    pub time_decay_adjustments: usize,
    /// Final equity after all trades (for compounding)
    pub final_equity: f32,
    /// Starting balance used
    pub starting_balance: f32,
    /// Whether compounding was enabled
    pub compounding_enabled: bool,
}

impl TradingEngineResult {
    pub fn new() -> Self {
        TradingEngineResult {
            backtest_result: BacktestResult::new(),
            ecp_mode: "disabled".to_string(),
            time_decay_adjustments: 0,
            final_equity: 10000.0,
            starting_balance: 10000.0,
            compounding_enabled: true,
        }
    }
    
    pub fn total_trades(&self) -> usize { self.backtest_result.profits.len() }
    pub fn total_return(&self) -> f32 { self.backtest_result.profits.iter().sum() }
    pub fn win_rate(&self) -> f32 {
        if self.total_trades() == 0 { return 0.0; }
        self.backtest_result.profits.iter().filter(|&&p| p > 0.0).count() as f32 / self.total_trades() as f32
    }
    pub fn roi(&self) -> f32 {
        if self.starting_balance > 0.0 { ((self.final_equity / self.starting_balance) - 1.0) * 100.0 } else { 0.0 }
    }
}

impl Default for TradingEngineResult {
    fn default() -> Self { Self::new() }
}

// =============================================================================
// HIGH-LEVEL STRATEGY BACKTEST
// =============================================================================

pub fn apply_strategy_overlay(cfg: &mut super::backtest::StrategyConfig, overlay: &serde_json::Value) {
    use serde_json::Value;
    macro_rules! get_f32 { ($k:expr) => { overlay.get($k).and_then(Value::as_f64).map(|v| v as f32) }; }
    macro_rules! get_f64 { ($k:expr) => { overlay.get($k).and_then(Value::as_f64) }; }
    macro_rules! _get_bool { ($k:expr) => { overlay.get($k).and_then(Value::as_bool) }; }
    if let Some(v) = get_f32!("stoploss") { cfg.stoploss = v; }
    if let Some(v) = get_f64!("leverage_default").or_else(|| get_f64!("leverage")) { cfg.leverage_default = v as f32; }
}

pub fn apply_overlay_to_ub_cfg(ub: &mut crate::backtest::UnifiedBacktestConfig, overlay: &serde_json::Value) {
    use serde_json::Value;
    macro_rules! f32_k { ($k:expr, $dst:expr) => { if let Some(v) = overlay.get($k).and_then(Value::as_f64) { $dst = v as f32; } }; }
    macro_rules! bool_k { ($k:expr, $dst:expr) => { if let Some(v) = overlay.get($k).and_then(Value::as_bool) { $dst = v; } }; }
    macro_rules! usize_k { ($k:expr, $dst:expr) => { if let Some(v) = overlay.get($k).and_then(Value::as_u64) { $dst = v as usize; } }; }
    macro_rules! str_k { ($k:expr, $dst:expr) => { if let Some(v) = overlay.get($k).and_then(Value::as_str) { $dst = v.to_string(); } }; }

    bool_k!("roi_enabled", ub.roi_enabled);
    f32_k!("min_stoploss", ub.min_stoploss);
    f32_k!("max_stoploss", ub.max_stoploss);
    bool_k!("trailing_stop", ub.trailing_enabled);
    bool_k!("trailing_enabled", ub.trailing_enabled);
    // freqtrade-named keys: trailing_stop_positive_offset is the activation
    // threshold (-> trailing_trigger), trailing_stop_positive is the trail
    // distance (-> trailing_offset). See the identical fix in
    // run_strategy_backtest's config mapping for the full rationale.
    f32_k!("trailing_stop_positive_offset", ub.trailing_trigger);
    f32_k!("trailing_trigger", ub.trailing_trigger);
    f32_k!("trailing_stop_positive", ub.trailing_offset);
    f32_k!("trailing_offset", ub.trailing_offset);
    bool_k!("profit_lock_enabled", ub.profit_lock_enabled);
    f32_k!("profit_lock_trigger", ub.profit_lock_trigger);
    f32_k!("profit_lock_ratio", ub.profit_lock_ratio);
    bool_k!("time_decay_enabled", ub.time_decay_enabled);
    usize_k!("time_decay_start_bars", ub.time_decay_start_bars);
    f32_k!("time_decay_per_bar", ub.time_decay_per_bar);
    bool_k!("atr_stop_enabled", ub.atr_stop_enabled);
    f32_k!("atr_stop_multiplier", ub.atr_stop_multiplier);
    if let Some(n) = overlay.get("entry_edge_cooldown_bars").and_then(Value::as_u64) {
        ub.entry_edge_filter = crate::backtest::EntryEdgeFilter::Cooldown(n as usize);
    } else if let Some(m) = overlay.get("entry_edge_filter").and_then(Value::as_str) {
        ub.entry_edge_filter = if m.eq_ignore_ascii_case("full") {
            crate::backtest::EntryEdgeFilter::Full
        } else {
            crate::backtest::EntryEdgeFilter::None
        };
    }
    usize_k!("eval_start_bar", ub.eval_start_bar);
    bool_k!("cci_exit_enabled", ub.cci_exit_enabled);
    f32_k!("cci_overbought", ub.cci_overbought);
    f32_k!("cci_oversold", ub.cci_oversold);
    bool_k!("rsi_exit_enabled", ub.rsi_exit_enabled);
    f32_k!("rsi_overbought", ub.rsi_overbought);
    f32_k!("rsi_oversold", ub.rsi_oversold);
    bool_k!("macd_reversal_exit", ub.macd_reversal_exit);
    str_k!("ecp_mode", ub.ecp_mode);
    usize_k!("ecp_min_trades", ub.ecp_min_trades);
    f32_k!("ecp_max_multiplier", ub.ecp_max_multiplier);
    f32_k!("ecp_min_multiplier", ub.ecp_min_multiplier);
    bool_k!("compounding_enabled", ub.compounding_enabled);
    bool_k!("compounding", ub.compounding_enabled);

    if let Some(obj) = overlay.get("minimal_roi").and_then(Value::as_object) {
        let mut roi_map = std::collections::HashMap::new();
        for (k, v) in obj { if let Some(f) = v.as_f64() { roi_map.insert(k.clone(), f as f32); } }
        assign_minimal_roi(ub, &roi_map, ub.timeframe_minutes);
    }
}

pub fn assign_minimal_roi(
    ub: &mut crate::backtest::UnifiedBacktestConfig,
    minimal_roi: &std::collections::HashMap<String, f32>,
    timeframe_minutes: usize,
) {
    let mut entries: Vec<(usize, f32)> = minimal_roi.iter()
        .filter_map(|(k, v)| k.parse::<usize>().ok().map(|m| (m, *v))).collect();
    entries.sort_by_key(|(m, _)| *m);
    ub.roi_6 = 99.0; ub.roi_3 = 99.0; ub.roi_15 = 99.0; ub.roi_720 = 99.0;
    ub.roi_period_0 = usize::MAX; ub.roi_period_10 = usize::MAX;
    ub.roi_period_30 = usize::MAX; ub.roi_period_720 = usize::MAX;
    let tf = timeframe_minutes.max(1);
    for (i, (minute, value)) in entries.iter().take(4).enumerate() {
        let period_bars = (*minute + tf - 1) / tf;
        match i {
            0 => { ub.roi_6 = *value; ub.roi_period_0 = period_bars; }
            1 => { ub.roi_3 = *value; ub.roi_period_10 = period_bars; }
            2 => { ub.roi_15 = *value; ub.roi_period_30 = period_bars; }
            3 => { ub.roi_720 = *value; ub.roi_period_720 = period_bars; }
            _ => {}
        }
    }
}

pub fn run_strategy_backtest(
    strategy: &dyn super::backtest::Strategy,
    symbol: &str,
    close: &[f32],
    open: &[f32],
    high: &[f32],
    low: &[f32],
    volume: &[f32],
    direction: &str,
    initial_capital: f32,
    fee_schedule: Option<(f32, f32)>,
    config_overlay: Option<&serde_json::Value>,
) -> serde_json::Value {
    let n = close.len();
    if n == 0 { return serde_json::json!({"error": "No data"}); }

    let mut cfg = strategy.config().clone();
    if let Some(overlay) = config_overlay { apply_strategy_overlay(&mut cfg, overlay); }

    let arc_close = std::sync::Arc::new(close.to_vec());
    let arc_open = std::sync::Arc::new(open.to_vec());
    let arc_high = std::sync::Arc::new(high.to_vec());
    let arc_low = std::sync::Arc::new(low.to_vec());
    let arc_volume = std::sync::Arc::new(volume.to_vec());

    let mut ind = crate::fast_indicators::calculate_standard_indicators(&arc_close, &arc_high, &arc_low, &arc_volume);
    for (k, v) in strategy.calculate_custom_indicators(&arc_close, &arc_open, &arc_high, &arc_low, &arc_volume) {
        ind.insert(k, v);
    }
    let ind = std::sync::Arc::new(ind);

    let empty_ind: Vec<f32> = Vec::new();
    let rsi_col       = ind.get(&0).unwrap_or(&empty_ind);
    let macd_hist_col = ind.get(&9).unwrap_or(&empty_ind);
    let bb_pos_col    = ind.get(&10).unwrap_or(&empty_ind);
    let atr_col       = ind.get(&14).unwrap_or(&empty_ind);
    let cci_col       = ind.get(&17).unwrap_or(&empty_ind);

    let ctx = super::backtest::SignalContext {
        indicators: std::sync::Arc::clone(&ind),
        close: std::sync::Arc::clone(&arc_close),
        open: std::sync::Arc::clone(&arc_open),
        high: std::sync::Arc::clone(&arc_high),
        low: std::sync::Arc::clone(&arc_low),
        volume: std::sync::Arc::clone(&arc_volume),
        n,
    };
    let (mut long_sigs, mut short_sigs) = strategy.populate_entry_trend(&ctx);

    // Guard against strategies returning wrong-length signal vectors
    if long_sigs.len() != n { long_sigs.resize(n, 0); }
    if short_sigs.len() != n { short_sigs.resize(n, 0); }

    let want_short_raw = direction == "short" || direction == "both";
    let want_short = want_short_raw && !cfg.is_spot();
    let want_long = direction == "long" || direction == "both" || (want_short_raw && cfg.is_spot());
    let effective_dir = if want_long && want_short { "both" } else if want_short { "short" } else { "long" };

    let build_exit_signals = |entry_sigs: &[u8], dir_str: &str| -> Vec<u8> {
        let entry_idx: Vec<usize> = (0..n).filter(|&i| entry_sigs[i] != 0).collect();
        let entry_px: Vec<f32> = entry_idx.iter().map(|&i| close[i]).collect();
        let mut s = strategy.populate_exit_trend(&ctx, dir_str, &entry_idx, &entry_px);
        if s.len() != n { s.resize(n, 0); }
        s
    };

    let mut ub_cfg = UnifiedBacktestConfig::default();
    let tf_min = crate::backtest::StrategyConfig::parse_timeframe(&cfg.timeframe).max(1);
    ub_cfg.timeframe = cfg.timeframe.clone();
    ub_cfg.timeframe_minutes = tf_min;
    ub_cfg.trade_type = cfg.trade_type;
    ub_cfg.direction = if want_long { "long".to_string() } else { "short".to_string() };
    ub_cfg.symbol = symbol.to_string();
    ub_cfg.startup_candle_count = cfg.startup_candle_count;
    ub_cfg.leverage_default = cfg.effective_leverage();
    ub_cfg.leverage_max = cfg.leverage_max.max(cfg.effective_leverage());
    ub_cfg.leverage_mode = cfg.leverage_mode;
    ub_cfg.fee_taker = cfg.fee_taker;
    ub_cfg.fee_maker = cfg.fee_maker;
    if let Some((maker_usd, taker_usd)) = fee_schedule {
        ub_cfg.fee_mode = "dollar".to_string();
        ub_cfg.fee_dollars_maker_per_contract = maker_usd;
        ub_cfg.fee_dollars_taker_per_contract = taker_usd;
    }
    ub_cfg.base_stoploss = cfg.stoploss;
    ub_cfg.max_hold_period = cfg.max_hold_period;
    ub_cfg.trailing_enabled = cfg.trailing_stop;
    // freqtrade semantics (confirmed in strategy/interface.py): `trailing_stop_positive_offset`
    // is the activation threshold (profit needed before trailing kicks in);
    // `trailing_stop_positive` is the trail distance once active. These were
    // previously swapped here.
    ub_cfg.trailing_trigger = cfg.trailing_stop_positive_offset;
    ub_cfg.trailing_offset = cfg.trailing_stop_positive;
    ub_cfg.atr_stop_enabled = cfg.atr_stop_enabled;
    ub_cfg.atr_stop_multiplier = cfg.atr_stop_multiplier;
    ub_cfg.entry_edge_filter = cfg.entry_edge_filter;
    assign_minimal_roi(&mut ub_cfg, &cfg.minimal_roi, tf_min);
    ub_cfg.starting_balance = initial_capital;
    if cfg.ecp_enabled { ub_cfg.ecp_mode = cfg.ecp_mode.clone(); }
    if let Some(overlay) = config_overlay { apply_overlay_to_ub_cfg(&mut ub_cfg, overlay); }

    if want_long && want_short {
        let long_entries = long_sigs.clone();
        let short_entries = short_sigs.clone();
        let long_exits = build_exit_signals(&long_entries, "long");
        let short_exits = build_exit_signals(&short_entries, "short");

        let mut long_cfg = ub_cfg.clone(); long_cfg.direction = "long".to_string();
        let mut short_cfg = ub_cfg.clone(); short_cfg.direction = "short".to_string();

        let long_res = unified_backtest(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &long_entries, &long_exits, &long_cfg);
        let short_res = unified_backtest(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &short_entries, &short_exits, &short_cfg);

        let long_json = trading_result_to_json(&long_res, &cfg, "long", initial_capital, n, tf_min);
        let short_json = trading_result_to_json(&short_res, &cfg, "short", initial_capital, n, tf_min);
        // "both" is NOT long_res+short_res merged - see unified_backtest_pythonstyle_joint's
        // doc comment for why that allows impossible simultaneous long+short exposure.
        let both_cfg = ub_cfg.clone();
        let both_res = unified_backtest_both(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &long_entries, &long_exits, &short_entries, &short_exits, &both_cfg);
        let both_json = trading_result_to_json(&both_res, &cfg, "both", initial_capital, n, tf_min);

        let variant_score = |v: &serde_json::Value| -> (i32, f64, f64) {
            let trades = v.get("total_trades").and_then(|x| x.as_u64()).unwrap_or(0);
            let score  = v.get("score").and_then(|x| x.as_f64()).unwrap_or(f64::NEG_INFINITY);
            let pf     = v.get("profit_factor").and_then(|x| x.as_f64()).unwrap_or(f64::NEG_INFINITY);
            (if trades > 0 { 1 } else { 0 }, score, pf)
        };
        let variants: Vec<(&str, &serde_json::Value)> = vec![("long", &long_json), ("short", &short_json), ("both", &both_json)];
        let (best_key, _) = variants.iter()
            .max_by(|a, b| variant_score(a.1).partial_cmp(&variant_score(b.1)).unwrap_or(std::cmp::Ordering::Equal))
            .copied().unwrap_or(("both", &both_json));

        let sides = serde_json::json!({"long": &long_json, "short": &short_json, "both": &both_json});
        let direction_scores = {
            let mk = |v: &serde_json::Value| serde_json::json!({
                "score": v.get("score").cloned().unwrap_or(serde_json::json!(0.0)),
                "profit_factor": v.get("profit_factor").cloned().unwrap_or(serde_json::json!(0.0)),
                "total_trades": v.get("total_trades").cloned().unwrap_or(serde_json::json!(0)),
                "total_return": v.get("total_return").cloned().unwrap_or(serde_json::json!(0.0)),
            });
            serde_json::json!({"long": mk(&long_json), "short": mk(&short_json), "both": mk(&both_json)})
        };
        let winner = match best_key { "long" => long_json, "short" => short_json, _ => both_json };
        let mut top = winner;
        if let Some(obj) = top.as_object_mut() {
            obj.insert("direction".into(), serde_json::json!(best_key));
            obj.insert("best_direction".into(), serde_json::json!(best_key));
            obj.insert("sides".into(), sides);
            obj.insert("direction_scores".into(), direction_scores);
        }
        top
    } else {
        let entry_signals = if want_short { short_sigs } else { long_sigs };
        let dir_str = if want_short { "short" } else { "long" };
        let exit_signals = build_exit_signals(&entry_signals, dir_str);
        let result = unified_backtest(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &entry_signals, &exit_signals, &ub_cfg);
        trading_result_to_json(&result, &cfg, effective_dir, initial_capital, n, tf_min)
    }
}

/// Sweep-optimised variant: accepts pre-built Arcs (no to_vec copies) and a
/// precomputed standard-indicator map shared across all strategies for this
/// (sym, tf) pair.  Only custom indicators are computed per-call.
pub fn run_strategy_backtest_precomputed(
    strategy: &dyn super::backtest::Strategy,
    symbol: &str,
    arc_close:  &std::sync::Arc<Vec<f32>>,
    arc_open:   &std::sync::Arc<Vec<f32>>,
    arc_high:   &std::sync::Arc<Vec<f32>>,
    arc_low:    &std::sync::Arc<Vec<f32>>,
    arc_volume: &std::sync::Arc<Vec<f32>>,
    precomputed: &std::sync::Arc<HashMap<usize, Vec<f32>>>,
    direction: &str,
    initial_capital: f32,
    fee_schedule: Option<(f32, f32)>,
    config_overlay: Option<&serde_json::Value>,
) -> serde_json::Value {
    let close  = arc_close.as_slice();
    let open   = arc_open.as_slice();
    let high   = arc_high.as_slice();
    let low    = arc_low.as_slice();
    let volume = arc_volume.as_slice();
    let n = close.len();
    if n == 0 { return serde_json::json!({"error": "No data"}); }

    let mut cfg = strategy.config().clone();
    if let Some(overlay) = config_overlay { apply_strategy_overlay(&mut cfg, overlay); }

    // Custom indicators are strategy-specific and must be computed per-call.
    // Standard indicators come from the shared precomputed map — zero recomputation.
    let custom = strategy.calculate_custom_indicators(arc_close, arc_open, arc_high, arc_low, arc_volume);
    let merged_ind: std::sync::Arc<HashMap<usize, Vec<f32>>> = if custom.is_empty() {
        std::sync::Arc::clone(precomputed)
    } else {
        let mut combined = precomputed.as_ref().clone();
        combined.extend(custom);
        std::sync::Arc::new(combined)
    };

    let empty_ind: Vec<f32> = Vec::new();
    let rsi_col       = merged_ind.get(&0).unwrap_or(&empty_ind);
    let macd_hist_col = merged_ind.get(&9).unwrap_or(&empty_ind);
    let bb_pos_col    = merged_ind.get(&10).unwrap_or(&empty_ind);
    let atr_col       = merged_ind.get(&14).unwrap_or(&empty_ind);
    let cci_col       = merged_ind.get(&17).unwrap_or(&empty_ind);

    let ctx = super::backtest::SignalContext {
        indicators: std::sync::Arc::clone(&merged_ind),
        close:  std::sync::Arc::clone(arc_close),
        open:   std::sync::Arc::clone(arc_open),
        high:   std::sync::Arc::clone(arc_high),
        low:    std::sync::Arc::clone(arc_low),
        volume: std::sync::Arc::clone(arc_volume),
        n,
    };
    let (mut long_sigs, mut short_sigs) = strategy.populate_entry_trend(&ctx);
    if long_sigs.len() != n  { long_sigs.resize(n, 0); }
    if short_sigs.len() != n { short_sigs.resize(n, 0); }

    let want_short_raw = direction == "short" || direction == "both";
    let want_short = want_short_raw && !cfg.is_spot();
    let want_long  = direction == "long" || direction == "both" || (want_short_raw && cfg.is_spot());
    let effective_dir = if want_long && want_short { "both" } else if want_short { "short" } else { "long" };

    let build_exit_signals = |entry_sigs: &[u8], dir_str: &str| -> Vec<u8> {
        let entry_idx: Vec<usize> = (0..n).filter(|&i| entry_sigs[i] != 0).collect();
        let entry_px: Vec<f32>   = entry_idx.iter().map(|&i| close[i]).collect();
        let mut s = strategy.populate_exit_trend(&ctx, dir_str, &entry_idx, &entry_px);
        if s.len() != n { s.resize(n, 0); }
        s
    };

    let mut ub_cfg = UnifiedBacktestConfig::default();
    let tf_min = crate::backtest::StrategyConfig::parse_timeframe(&cfg.timeframe).max(1);
    ub_cfg.timeframe = cfg.timeframe.clone();
    ub_cfg.timeframe_minutes = tf_min;
    ub_cfg.trade_type = cfg.trade_type;
    ub_cfg.direction = if want_long { "long".to_string() } else { "short".to_string() };
    ub_cfg.symbol = symbol.to_string();
    ub_cfg.startup_candle_count = cfg.startup_candle_count;
    ub_cfg.leverage_default = cfg.effective_leverage();
    ub_cfg.leverage_max = cfg.leverage_max.max(cfg.effective_leverage());
    ub_cfg.leverage_mode = cfg.leverage_mode;
    ub_cfg.fee_taker = cfg.fee_taker;
    ub_cfg.fee_maker = cfg.fee_maker;
    if let Some((maker_usd, taker_usd)) = fee_schedule {
        ub_cfg.fee_mode = "dollar".to_string();
        ub_cfg.fee_dollars_maker_per_contract = maker_usd;
        ub_cfg.fee_dollars_taker_per_contract = taker_usd;
    }
    ub_cfg.base_stoploss = cfg.stoploss;
    ub_cfg.max_hold_period = cfg.max_hold_period;
    ub_cfg.trailing_enabled = cfg.trailing_stop;
    // freqtrade semantics (confirmed in strategy/interface.py): `trailing_stop_positive_offset`
    // is the activation threshold (profit needed before trailing kicks in);
    // `trailing_stop_positive` is the trail distance once active. These were
    // previously swapped here.
    ub_cfg.trailing_trigger = cfg.trailing_stop_positive_offset;
    ub_cfg.trailing_offset = cfg.trailing_stop_positive;
    ub_cfg.atr_stop_enabled = cfg.atr_stop_enabled;
    ub_cfg.atr_stop_multiplier = cfg.atr_stop_multiplier;
    ub_cfg.entry_edge_filter = cfg.entry_edge_filter;
    assign_minimal_roi(&mut ub_cfg, &cfg.minimal_roi, tf_min);
    ub_cfg.starting_balance = initial_capital;
    if cfg.ecp_enabled { ub_cfg.ecp_mode = cfg.ecp_mode.clone(); }
    if let Some(overlay) = config_overlay { apply_overlay_to_ub_cfg(&mut ub_cfg, overlay); }

    if want_long && want_short {
        let long_entries  = long_sigs.clone();
        let short_entries = short_sigs.clone();
        let long_exits    = build_exit_signals(&long_entries, "long");
        let short_exits   = build_exit_signals(&short_entries, "short");
        let mut long_cfg  = ub_cfg.clone(); long_cfg.direction  = "long".to_string();
        let mut short_cfg = ub_cfg.clone(); short_cfg.direction = "short".to_string();
        let long_res  = unified_backtest(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &long_entries,  &long_exits,  &long_cfg);
        let short_res = unified_backtest(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &short_entries, &short_exits, &short_cfg);
        let long_json  = trading_result_to_json(&long_res,  &cfg, "long",  initial_capital, n, tf_min);
        let short_json = trading_result_to_json(&short_res, &cfg, "short", initial_capital, n, tf_min);
        // "both" is NOT long_res+short_res merged - see unified_backtest_pythonstyle_joint's
        // doc comment for why that allows impossible simultaneous long+short exposure.
        let both_res  = unified_backtest_both(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &long_entries, &long_exits, &short_entries, &short_exits, &ub_cfg);
        let both_json  = trading_result_to_json(&both_res, &cfg, "both", initial_capital, n, tf_min);
        let variant_score = |v: &serde_json::Value| -> (i32, f64, f64) {
            let trades = v.get("total_trades").and_then(|x| x.as_u64()).unwrap_or(0);
            let score  = v.get("score").and_then(|x| x.as_f64()).unwrap_or(f64::NEG_INFINITY);
            let pf     = v.get("profit_factor").and_then(|x| x.as_f64()).unwrap_or(f64::NEG_INFINITY);
            (if trades > 0 { 1 } else { 0 }, score, pf)
        };
        let variants: Vec<(&str, &serde_json::Value)> = vec![("long", &long_json), ("short", &short_json), ("both", &both_json)];
        let (best_key, _) = variants.iter()
            .max_by(|a, b| variant_score(a.1).partial_cmp(&variant_score(b.1)).unwrap_or(std::cmp::Ordering::Equal))
            .copied().unwrap_or(("both", &both_json));
        let sides = serde_json::json!({"long": &long_json, "short": &short_json, "both": &both_json});
        let direction_scores = {
            let mk = |v: &serde_json::Value| serde_json::json!({
                "score": v.get("score").cloned().unwrap_or(serde_json::json!(0.0)),
                "profit_factor": v.get("profit_factor").cloned().unwrap_or(serde_json::json!(0.0)),
                "total_trades": v.get("total_trades").cloned().unwrap_or(serde_json::json!(0)),
                "total_return": v.get("total_return").cloned().unwrap_or(serde_json::json!(0.0)),
            });
            serde_json::json!({"long": mk(&long_json), "short": mk(&short_json), "both": mk(&both_json)})
        };
        let winner = match best_key { "long" => long_json, "short" => short_json, _ => both_json };
        let mut top = winner;
        if let Some(obj) = top.as_object_mut() {
            obj.insert("direction".into(), serde_json::json!(best_key));
            obj.insert("best_direction".into(), serde_json::json!(best_key));
            obj.insert("sides".into(), sides);
            obj.insert("direction_scores".into(), direction_scores);
        }
        top
    } else {
        let entry_signals = if want_short { short_sigs } else { long_sigs };
        let dir_str = if want_short { "short" } else { "long" };
        let exit_signals = build_exit_signals(&entry_signals, dir_str);
        let result = unified_backtest(close, open, high, low, volume, rsi_col, macd_hist_col, bb_pos_col, atr_col, cci_col, &entry_signals, &exit_signals, &ub_cfg);
        trading_result_to_json(&result, &cfg, effective_dir, initial_capital, n, tf_min)
    }
}

fn trading_result_to_json(
    result: &TradingEngineResult, cfg: &super::backtest::StrategyConfig,
    direction: &str, initial_capital: f32, n_bars: usize, timeframe_minutes: usize,
) -> serde_json::Value {
    let br = &result.backtest_result;
    let total_trades = br.profits.len();
    if total_trades == 0 {
        return serde_json::json!({
            "strategy_name": cfg.name, "timeframe": cfg.timeframe, "trade_type": cfg.trade_type,
            "direction": direction, "total_trades": 0, "win_rate": 0.0, "winrate": 0.0,
            "profit_factor": 0.0, "total_return": 0.0, "max_drawdown": 0.0, "sharpe_ratio": 0.0, "avg_profit": 0.0,
            "sharpe": 0.0, "sortino": 0.0, "calmar": 0.0, "sqn": 0.0,
            "expectancy": 0.0, "expectancy_ratio": 100.0, "cagr": 0.0,
            "profit_mean": 0.0, "profit_median": 0.0, "profit_total": 0.0, "profit_total_abs": 0.0,
            "max_drawdown_abs": 0.0, "max_drawdown_account": 0.0,
            "max_drawdown_high": 0.0, "max_drawdown_low": 0.0, "max_relative_drawdown": 0.0,
            "max_consecutive_wins": 0, "max_consecutive_losses": 0,
            "holding_avg_minutes": 0.0, "winner_holding_avg_minutes": 0.0, "loser_holding_avg_minutes": 0.0,
            "trade_count_long": 0, "trade_count_short": 0, "backtest_days": crate::metrics::days_period(n_bars, timeframe_minutes),
        });
    }

    let wins_count = br.profits.iter().filter(|&&p| p > 0.0).count();
    let losses_count = br.profits.iter().filter(|&&p| p < 0.0).count();
    let win_rate = wins_count as f32 / total_trades as f32;
    let gross_profit: f32 = br.profits.iter().filter(|&&p| p > 0.0).sum();
    let gross_loss: f32 = br.profits.iter().filter(|&&p| p < 0.0).map(|x| x.abs()).sum();
    let profit_factor = if gross_loss == 0.0 { 99.0 } else { gross_profit / gross_loss };
    let total_return: f32 = br.profits.iter().sum();
    let avg_profit = total_return / total_trades as f32;

    let mut balance = 1.0f32; let mut peak = 1.0f32; let mut max_dd: f32 = 0.0;
    for &p in &br.profits {
        balance *= 1.0 + p; if balance > peak { peak = balance; }
        let dd = if peak > 0.0 { (peak - balance) / peak } else { 0.0 }; if dd > max_dd { max_dd = dd; }
    }

    let var: f32 = br.profits.iter().map(|p| (p - avg_profit).powi(2)).sum::<f32>() / total_trades as f32;
    let std_dev = var.sqrt();
    let tf = timeframe_minutes.max(1) as f32;
    let bars_per_year = (365.25_f32 * 1440.0) / tf;
    let trades_per_year = (total_trades as f32 / n_bars as f32).max(1.0) * bars_per_year;
    let sharpe = if std_dev == 0.0 { 0.0 } else { (avg_profit / std_dev) * trades_per_year.max(1.0).sqrt() };
    let snr = if std_dev == 0.0 { 0.0 } else { avg_profit / std_dev };
    let score = profit_factor * (total_trades as f32).sqrt() * (1.0 - max_dd) * (1.0 + sharpe.max(0.0) * 0.1);
    let total_years = n_bars as f32 / bars_per_year;
    let cagr = if total_years > 0.0 && result.final_equity > 0.0 && initial_capital > 0.0 {
        (result.final_equity / initial_capital).powf(1.0 / total_years) - 1.0
    } else { 0.0 };
    let avg_win = if wins_count > 0 { gross_profit / wins_count as f32 } else { 0.0 };
    let avg_loss = if losses_count > 0 { gross_loss / losses_count as f32 } else { 1.0 };
    let expectancy_r = if avg_loss > 0.0 { avg_win / avg_loss } else { 0.0 };

    let exit_reason_strs: Vec<String> = br.exit_reasons.iter().map(|r| match r {
        crate::backtest::ExitReason::Signal => "signal",
        crate::backtest::ExitReason::RoiTarget => "roi_target",
        crate::backtest::ExitReason::Stoploss => "stoploss",
        crate::backtest::ExitReason::TrailingStop => "trailing_stop",
        crate::backtest::ExitReason::MaxHoldPeriod => "max_hold",
        crate::backtest::ExitReason::CciExit => "cci_exit",
        crate::backtest::ExitReason::RsiExit => "rsi_exit",
        crate::backtest::ExitReason::MacdExit => "macd_exit",
        crate::backtest::ExitReason::CustomExit => "custom_exit",
    }.to_string()).collect();

    let entry_reason_strs: Vec<String> = br.entry_reasons.iter().map(|r| match r {
        crate::backtest::EntryReason::Signal => "signal",
        crate::backtest::EntryReason::RsiOversold => "rsi_oversold",
        crate::backtest::EntryReason::RsiOverbought => "rsi_overbought",
        crate::backtest::EntryReason::CciSignal => "cci_signal",
        crate::backtest::EntryReason::MacdCross => "macd_cross",
        crate::backtest::EntryReason::BollingerBand => "bollinger_band",
        crate::backtest::EntryReason::CustomEntry => "custom_entry",
    }.to_string()).collect();

    let mut exit_stats: HashMap<String, serde_json::Value> = HashMap::new();
    for (i, reason) in exit_reason_strs.iter().enumerate() {
        let entry = exit_stats.entry(reason.clone()).or_insert_with(|| serde_json::json!({"count":0,"total_pnl":0.0,"wins":0,"losses":0}));
        let obj = entry.as_object_mut().unwrap();
        *obj.get_mut("count").unwrap() = serde_json::json!(obj["count"].as_i64().unwrap() + 1);
        *obj.get_mut("total_pnl").unwrap() = serde_json::json!(obj["total_pnl"].as_f64().unwrap() + br.profits[i] as f64);
        if br.profits[i] > 0.0 { *obj.get_mut("wins").unwrap() = serde_json::json!(obj["wins"].as_i64().unwrap() + 1); }
        else if br.profits[i] < 0.0 { *obj.get_mut("losses").unwrap() = serde_json::json!(obj["losses"].as_i64().unwrap() + 1); }
    }
    for (_r, stat) in exit_stats.iter_mut() {
        let obj = stat.as_object_mut().unwrap();
        let count = obj["count"].as_i64().unwrap_or(0) as f64;
        let total = obj["total_pnl"].as_f64().unwrap_or(0.0);
        let w = obj["wins"].as_i64().unwrap_or(0) as f64;
        if count > 0.0 { obj.insert("avg_profit_pct".into(), serde_json::json!(total/count)); obj.insert("win_rate".into(), serde_json::json!(w/count)); }
        else { obj.insert("avg_profit_pct".into(), serde_json::json!(0.0)); obj.insert("win_rate".into(), serde_json::json!(0.0)); }
    }
    let mut entry_stats: HashMap<String, serde_json::Value> = HashMap::new();
    for (i, reason) in entry_reason_strs.iter().enumerate() {
        let entry = entry_stats.entry(reason.clone()).or_insert_with(|| serde_json::json!({"count":0,"total_pnl":0.0,"wins":0,"losses":0}));
        let obj = entry.as_object_mut().unwrap();
        *obj.get_mut("count").unwrap() = serde_json::json!(obj["count"].as_i64().unwrap() + 1);
        *obj.get_mut("total_pnl").unwrap() = serde_json::json!(obj["total_pnl"].as_f64().unwrap() + br.profits[i] as f64);
        if br.profits[i] > 0.0 { *obj.get_mut("wins").unwrap() = serde_json::json!(obj["wins"].as_i64().unwrap() + 1); }
        else if br.profits[i] < 0.0 { *obj.get_mut("losses").unwrap() = serde_json::json!(obj["losses"].as_i64().unwrap() + 1); }
    }
    for (_r, stat) in entry_stats.iter_mut() {
        let obj = stat.as_object_mut().unwrap();
        let count = obj["count"].as_i64().unwrap_or(0) as f64;
        let total = obj["total_pnl"].as_f64().unwrap_or(0.0);
        let w = obj["wins"].as_i64().unwrap_or(0) as f64;
        if count > 0.0 { obj.insert("avg_profit_pct".into(), serde_json::json!(total/count)); obj.insert("win_rate".into(), serde_json::json!(w/count)); }
        else { obj.insert("avg_profit_pct".into(), serde_json::json!(0.0)); obj.insert("win_rate".into(), serde_json::json!(0.0)); }
    }

    let total_pnl = total_return * initial_capital;

    let ext = crate::metrics::compute_extended_metrics(
        &br.profits, &br.pnl_amounts, &br.exit_indices, &br.durations,
        result.starting_balance, result.final_equity, n_bars, timeframe_minutes,
    );
    let (trade_count_long, trade_count_short) = match direction {
        "short" => (0, total_trades),
        _ => (total_trades, 0),
    };

    let mut out = serde_json::json!({
        "strategy_name": cfg.name, "timeframe": cfg.timeframe, "trade_type": cfg.trade_type,
        "direction": direction, "total_trades": total_trades, "wins": wins_count, "losses": losses_count,
        "win_rate": win_rate, "profit_factor": profit_factor, "total_return": total_return,
        "max_drawdown": max_dd, "sharpe_ratio": sharpe, "avg_profit": avg_profit,
        "total_pnl": total_pnl, "roi": total_return, "snr": snr, "score": score, "edge_score": score,
        "cagr": cagr, "expectancy_r": expectancy_r,
        "final_equity": result.final_equity, "starting_balance": result.starting_balance,
        "entry_indices": br.entry_indices, "exit_indices": br.exit_indices,
        "profits": br.profits, "pnl_amounts": br.pnl_amounts,
        "entry_prices": br.entry_prices, "exit_prices": br.exit_prices,
        "exit_reasons": exit_reason_strs, "entry_reasons": entry_reason_strs,
        "durations": br.durations, "leverages": br.leverages,
        "exit_type_stats": exit_stats, "entry_type_stats": entry_stats,
    });
    if let Some(obj) = out.as_object_mut() {
        obj.insert("winrate".into(), serde_json::json!(win_rate));
        obj.insert("sharpe".into(), serde_json::json!(ext.sharpe));
        obj.insert("sortino".into(), serde_json::json!(ext.sortino));
        obj.insert("calmar".into(), serde_json::json!(ext.calmar));
        obj.insert("sqn".into(), serde_json::json!(ext.sqn));
        obj.insert("expectancy".into(), serde_json::json!(ext.expectancy));
        obj.insert("expectancy_ratio".into(), serde_json::json!(ext.expectancy_ratio));
        obj.insert("cagr".into(), serde_json::json!(ext.cagr)); // overrides legacy 365.25-based value with the exact freqtrade formula
        obj.insert("profit_mean".into(), serde_json::json!(ext.profit_mean));
        obj.insert("profit_median".into(), serde_json::json!(ext.profit_median));
        obj.insert("profit_total".into(), serde_json::json!(total_return));
        obj.insert("profit_total_abs".into(), serde_json::json!(total_pnl));
        obj.insert("max_drawdown_abs".into(), serde_json::json!(ext.max_drawdown_abs));
        obj.insert("max_drawdown_account".into(), serde_json::json!(ext.max_drawdown_account));
        obj.insert("max_drawdown_high".into(), serde_json::json!(ext.max_drawdown_high));
        obj.insert("max_drawdown_low".into(), serde_json::json!(ext.max_drawdown_low));
        obj.insert("max_relative_drawdown".into(), serde_json::json!(ext.max_relative_drawdown));
        obj.insert("max_consecutive_wins".into(), serde_json::json!(ext.max_consecutive_wins));
        obj.insert("max_consecutive_losses".into(), serde_json::json!(ext.max_consecutive_losses));
        obj.insert("holding_avg_minutes".into(), serde_json::json!(ext.holding_avg_minutes));
        obj.insert("winner_holding_avg_minutes".into(), serde_json::json!(ext.winner_holding_avg_minutes));
        obj.insert("loser_holding_avg_minutes".into(), serde_json::json!(ext.loser_holding_avg_minutes));
        obj.insert("trade_count_long".into(), serde_json::json!(trade_count_long));
        obj.insert("trade_count_short".into(), serde_json::json!(trade_count_short));
        obj.insert("backtest_days".into(), serde_json::json!(ext.backtest_days));
    }
    out
}

// =============================================================================
// TRADING ENGINE - UNIFIED BACKTEST
// =============================================================================

pub fn unified_backtest(
    close_prices: &[f32], open_prices: &[f32], high_prices: &[f32], low_prices: &[f32],
    volumes: &[f32],
    rsi: &[f32], macd_hist: &[f32], bb_pos: &[f32], atr: &[f32], cci: &[f32],
    entry_signals: &[u8], exit_signals: &[u8],
    config: &UnifiedBacktestConfig,
) -> TradingEngineResult {
    // freqtrade's _get_ohlcv_as_lists: "To avoid using data from future, we
    // use entry/exit signals shifted from the previous candle" - every
    // enter_long/enter_short/exit_long/exit_short column is .shift(1)
    // before the backtest loop ever sees it, so bar i's tradeable signal is
    // really bar (i-1)'s raw indicator-derived signal (price/OHLC itself is
    // NOT shifted). Verified directly against a real freqtrade-fork run:
    // the fully advised dataframe had exit_short=1 at a given candle, but
    // the row actually consumed by the backtest loop for that same candle
    // read 0 - it only became 1 one candle later. Continuously-true signals
    // (e.g. Jackknife's RSI/CCI gates) hide this shift entirely; strategies
    // with a sharp threshold-crossing signal (like a fading-magnitude exit)
    // expose it immediately - which is also why an earlier version of this
    // shifted only exit_signals: with entries left unshifted, single-slowly-
    // changing-signal validation runs (Jackknife/BTC's first several trades)
    // still matched, masking the fact that entries needed it too. It took a
    // RAPIDLY flickering entry signal (long/short direction alternating
    // bar-to-bar around bar 1000 of that same test) to expose the gap: real
    // Python's trade opened LONG on a bar where this crate's own raw signal
    // said SHORT, but exactly matched the PRIOR bar's raw LONG signal -
    // direct proof entries get the identical .shift(1) treatment as exits.
    let shift = |sigs: &[u8]| -> Vec<u8> {
        let mut out = vec![0u8; sigs.len()];
        for i in 1..sigs.len() { out[i] = sigs[i - 1]; }
        out
    };
    let entry_shifted = shift(entry_signals);
    let exit_shifted = shift(exit_signals);
    unified_backtest_pythonstyle(close_prices, open_prices, high_prices, low_prices, volumes, rsi, macd_hist, bb_pos, atr, cci, &entry_shifted, &exit_shifted, config)
}

// =============================================================================
// MODE2 — freqtrade-parity backtest loop
// =============================================================================
//
// Ported from a direct reading of the actual `backtesting.py` /
// `persistence/trade_model.py` in this repo's Python fork, not guessed:
//
//   - backtest_loop() opens a trade and immediately calls _check_trade_exit
//     for it on that SAME candle (backtesting.py ~line 1524-1553) — exits
//     are checked from the entry bar itself, not starting the bar after.
//   - Trade.adjust_stop_loss (persistence/trade_model.py) ratchets: a new
//     stop candidate only replaces the current one if it's tighter, and it
//     divides whatever fraction it's given (custom_stoploss's return value,
//     or trailing_stop_positive) by leverage to get the raw price distance.
//   - trader_stoploss_adjust always evaluates custom_stoploss/trailing using
//     `bound = low if short else high` as `current_rate` — that candle's
//     most-favorable price, not its close.
//   - _get_close_rate_for_roi clamps the ROI target price into the candle's
//     own [low, high] range; _get_close_rate falls through to that candle's
//     OPEN for any exit type other than stoploss/trailing/liquidation/ROI.
//   - _get_close_rate_for_stoploss's documented pessimistic case: a
//     same-candle (trade_dur == 0) TRAILING trigger fills as if price had
//     *just* armed the trail (reached the offset) then dove straight down,
//     anchored to that candle's OPEN — not its high.
//
// Not replicated: freqtrade's "opening candle ROI on red candles" validation
// (a defensive edge-case check, not a pricing rule) and time-decay/
// profit-lock stoploss adjustments (none of this crate's ported strategies
// use them).
#[allow(unused_assignments)]
fn unified_backtest_pythonstyle(
    close_prices: &[f32], open_prices: &[f32], high_prices: &[f32], low_prices: &[f32],
    _volumes: &[f32],
    rsi: &[f32], macd_hist: &[f32], bb_pos: &[f32], atr: &[f32], cci: &[f32],
    entry_signals: &[u8], exit_signals: &[u8],
    config: &UnifiedBacktestConfig,
) -> TradingEngineResult {
    let n = close_prices.len();
    let is_long = config.is_long();
    let leverage = config.leverage_default;
    let use_dollar_fees = config.fee_mode.eq_ignore_ascii_case("dollar");
    let fee_rate = if use_dollar_fees { 0.0 } else { config.fee_taker };
    let contract_mult = 1.0_f32;
    // eval_start_bar (default 0) is the first bar a trade may actually FILL
    // on, separate from startup_candle_count's indicator-warmup role — see
    // the field doc comment on UnifiedBacktestConfig. Signal and fill resolve
    // on the same bar (see below), so the loop can start exactly there.
    let start_idx = config.startup_candle_count.max(config.eval_start_bar.saturating_sub(1)).min(n.saturating_sub(1));

    let est = (n / 50).max(16);
    let mut entry_indices = Vec::with_capacity(est);
    let mut exit_indices = Vec::with_capacity(est);
    let mut profits = Vec::with_capacity(est);
    let mut pnl_amounts = Vec::with_capacity(est);
    let mut entry_prices_vec = Vec::with_capacity(est);
    let mut exit_prices_vec = Vec::with_capacity(est);
    let mut exit_reasons = Vec::with_capacity(est);
    let mut entry_reasons = Vec::with_capacity(est);
    let mut leverages_used = Vec::with_capacity(est);
    let mut durations = Vec::with_capacity(est);
    let effective_max_trade = config.max_trade_amount.min(config.max_open_capital);
    let mut current_equity = config.starting_balance;
    let mut base_position_size = (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade);
    let mut trade_position_size = base_position_size;

    let mut in_position = false;
    let mut entry_price = 0.0f32; // fee-adjusted fill price
    let mut base_price = 0.0f32;  // raw fill price (freqtrade's open_rate)
    let mut entry_idx = 0usize;
    let mut trade_leverage = leverage;
    let mut current_entry_reason = EntryReason::Signal;

    // Unified ratcheting stop tracker — Mode2's core structural difference
    // from Mode1's separately-recomputed-each-bar effective_sl/trailing_stop_price.
    let mut stop_loss_price = 0.0f32;
    let mut stop_is_trailing = false;
    let mut trailing_activated = false;

    let time_decay_count = 0usize; // fixed base sizing — no dynamic ECS/ECP
    let mut last_exit_idx: usize = 0;

    // Verified against two consecutive real Python trades
    // (JackknifeVarianceEstimator on BTC): entry AND signal both resolve on
    // the SAME bar, filling at THAT bar's own open — not a bar-deferred fill.
    // (An earlier version of this deferred entry to bar i+1, reasoned from
    // "you can't act on a signal before its candle closes" — plausible, but
    // it turned out freqtrade's `backtest_loop` checks entries for bar i
    // BEFORE that same bar's own exit-processing runs, so exiting and
    // re-entering can't both resolve on one bar under a deferred model. A
    // second real trade (entering the very next bar after the first one's
    // exit, at that next bar's own open) only matches a same-bar model.)
    // No signal_edge (rising-edge) requirement either, unlike Mode1: real
    // freqtrade's entry check is just `enter_long/enter_short == 1 AND no
    // open position` — no transition/edge concept. Confirmed against a
    // signal that stayed continuously true across dozens of bars, where
    // freqtrade re-entered on literally the bar after each exit — an edge
    // requirement would have missed that entirely.
    for i in (start_idx + 1)..n {
        let signal_active = entry_signals[i] > 0;
        let no_exit_conflict = exit_signals[i] == 0;
        // Optional per-strategy throttle on top of freqtrade's real
        // (edge-free) entry check — see EntryEdgeFilter's doc comment.
        let edge_ok = match config.entry_edge_filter {
            crate::backtest::EntryEdgeFilter::None => true,
            crate::backtest::EntryEdgeFilter::Full => i == 0 || entry_signals[i - 1] == 0,
            crate::backtest::EntryEdgeFilter::Cooldown(bars) => i >= last_exit_idx + bars,
        };
        let can_enter = !in_position && signal_active && no_exit_conflict && i > last_exit_idx && edge_ok;

        if can_enter {
            in_position = true;
            current_entry_reason = determine_entry_reason(rsi, cci, macd_hist, bb_pos, i, is_long);

            trade_leverage = leverage.min(config.leverage_max).max(0.1);
            trade_position_size = base_position_size.min(effective_max_trade).max(0.0);

            base_price = open_prices[i]; // fill at this bar's own open, not its close
            entry_price = if is_long { base_price * (1.0 + fee_rate) } else { base_price * (1.0 - fee_rate) };
            entry_idx = i;
            trailing_activated = false;
            stop_is_trailing = false;
            // Initial stop is the static base stoploss (freqtrade sets this
            // at entry before any custom_stoploss call can tighten it).
            stop_loss_price = if is_long { base_price * (1.0 + config.base_stoploss / trade_leverage) }
                              else { base_price * (1.0 - config.base_stoploss / trade_leverage) };
        }

        // Deliberately not `else if`: a freshly-opened position falls straight
        // through into its own exit check on this same bar (entry_idx == i,
        // so time_in_position below is naturally 0) — see the module doc comment.
        if in_position {
            let time_in_position = i - entry_idx;
            // freqtrade's `bound = low if short else high`: the most
            // favorable price reached this candle, used for every per-candle
            // stop/trailing/ROI check (not just the entry candle's).
            let bound = if is_long { high_prices[i] } else { low_prices[i] };
            // freqtrade's `calc_profit_ratio` (used for both the trailing-trigger
            // and ROI-reached comparisons) bakes fee_open/fee_close into the
            // open/close trade values rather than comparing raw price ratios -
            // this matters right at threshold crossings, where the fee-exclusive
            // raw return can be a few bps above a target that the real,
            // fee-inclusive profit never actually reaches. Mirror it exactly
            // rather than approximating with a flat subtraction.
            let leveraged_bound_return = if is_long {
                ((bound * (1.0 - fee_rate)) / (base_price * (1.0 + fee_rate)) - 1.0) * trade_leverage
            } else {
                (1.0 - (bound * (1.0 + fee_rate)) / (base_price * (1.0 - fee_rate))) * trade_leverage
            };

            // freqtrade's `dir_correct` gate (strategy/interface.py
            // trader_stoploss_adjust): custom_stoploss/trailing are only
            // consulted to *tighten* the stop if the ALREADY-SET level hasn't
            // been breached yet by this candle's low/high. Skipping this
            // meant every bar's ATR candidate got considered unconditionally,
            // which systematically produced spurious near-instant stop-outs
            // on volatile entry candles (exactly the kind these
            // dispersion/regime-gated strategies tend to enter on) instead of
            // falling back to the plain base stoploss the way Python does.
            let dir_correct = if is_long { stop_loss_price < low_prices[i] } else { stop_loss_price > high_prices[i] };

            if dir_correct {
                // --- ATR-anchored custom stoploss candidate (ratchets tighter only) ---
                if config.atr_stop_enabled && i < atr.len() && base_price > 0.0 {
                    let stop_price_raw = if is_long { base_price - atr[i] * config.atr_stop_multiplier }
                                          else { base_price + atr[i] * config.atr_stop_multiplier };
                    let candidate = if is_long { bound - (bound - stop_price_raw) / trade_leverage }
                                    else { bound + (stop_price_raw - bound) / trade_leverage };
                    let tighter = if is_long { candidate > stop_loss_price } else { candidate < stop_loss_price };
                    if tighter {
                        stop_loss_price = candidate;
                        stop_is_trailing = false;
                    }
                }

                // --- Trailing-stop candidate (ratchets tighter only, once activated) ---
                if config.trailing_enabled {
                    if !trailing_activated && leveraged_bound_return >= config.trailing_trigger {
                        trailing_activated = true;
                    }
                    if trailing_activated {
                        let trail_dist = config.trailing_offset / trade_leverage;
                        let candidate = if is_long { bound * (1.0 - trail_dist) } else { bound * (1.0 + trail_dist) };
                        let tighter = if is_long { candidate > stop_loss_price } else { candidate < stop_loss_price };
                        if tighter {
                            stop_loss_price = candidate;
                            stop_is_trailing = true;
                        }
                    }
                }
            }

            let stop_triggered = if is_long { low_prices[i] <= stop_loss_price } else { high_prices[i] >= stop_loss_price };
            let roi_triggered = config.roi_enabled && (
                (time_in_position >= config.roi_period_0 && leveraged_bound_return >= config.roi_6) ||
                (time_in_position >= config.roi_period_10 && leveraged_bound_return >= config.roi_3) ||
                (time_in_position >= config.roi_period_30 && leveraged_bound_return >= config.roi_15) ||
                (time_in_position >= config.roi_period_720 && leveraged_bound_return >= config.roi_720)
            );
            let signal_exit_triggered = exit_signals[i] > 0;
            let cci_exit_triggered = config.cci_exit_enabled && i < cci.len() && {
                let cci_v = cci[i];
                (is_long && cci_v > config.cci_overbought) || (!is_long && cci_v < config.cci_oversold)
            };
            let rsi_exit_triggered = config.rsi_exit_enabled && i < rsi.len() && {
                let rsi_v = rsi[i];
                (is_long && rsi_v > config.rsi_overbought) || (!is_long && rsi_v < config.rsi_oversold)
            };
            let macd_exit_triggered = config.macd_reversal_exit && i > 1 && i < macd_hist.len() && {
                let hist_curr = macd_hist[i];
                let hist_prev = macd_hist[i - 1];
                (is_long && hist_curr < hist_prev && hist_prev > 0.0 && hist_curr < 0.0) ||
                (!is_long && hist_curr > hist_prev && hist_prev < 0.0 && hist_curr > 0.0)
            };
            let max_hold_triggered = time_in_position >= config.max_hold_period;

            // Priority order is freqtrade's own documented sequence (strategy/
            // interface.py, should_exit(), literally commented "Sequence:
            // Exit-signal / Stoploss / ROI / Trailing stoploss"). Plain
            // stop/ATR-based stoploss (stop_is_trailing == false) outranks
            // ROI; trailing is checked LAST, not first — getting this order
            // right matters a lot here, since a bar can satisfy multiple
            // conditions at once and the ratcheted stop_loss_price doesn't by
            // itself say which one freqtrade would have honored.
            let mut should_exit = false;
            let mut exit_reason = ExitReason::Signal;
            if !should_exit && signal_exit_triggered { should_exit = true; exit_reason = ExitReason::Signal; }
            if !should_exit && stop_triggered && !stop_is_trailing { should_exit = true; exit_reason = ExitReason::Stoploss; }
            if !should_exit && roi_triggered { should_exit = true; exit_reason = ExitReason::RoiTarget; }
            if !should_exit && stop_triggered && stop_is_trailing { should_exit = true; exit_reason = ExitReason::TrailingStop; }
            if !should_exit && cci_exit_triggered { should_exit = true; exit_reason = ExitReason::CciExit; }
            if !should_exit && rsi_exit_triggered { should_exit = true; exit_reason = ExitReason::RsiExit; }
            if !should_exit && macd_exit_triggered { should_exit = true; exit_reason = ExitReason::MacdExit; }
            if !should_exit && max_hold_triggered { should_exit = true; exit_reason = ExitReason::MaxHoldPeriod; }

            if should_exit {
                let base_exit_price = match exit_reason {
                    ExitReason::TrailingStop if time_in_position == 0 => {
                        // freqtrade's documented pessimistic same-candle case: assume
                        // price just barely armed the trail (reached the activation
                        // offset) then dove straight to the trail distance below that
                        // — anchored to this candle's OPEN, not its high.
                        let trail_dist = config.trailing_offset / trade_leverage;
                        if is_long {
                            (open_prices[i] * (1.0 + config.trailing_trigger.abs() - trail_dist.abs())).max(low_prices[i])
                        } else {
                            (open_prices[i] * (1.0 - config.trailing_trigger.abs() + trail_dist.abs())).min(high_prices[i])
                        }
                    }
                    ExitReason::TrailingStop | ExitReason::Stoploss => stop_loss_price,
                    ExitReason::RoiTarget => {
                        let roi_pct = if time_in_position >= config.roi_period_720 && leveraged_bound_return >= config.roi_720 { config.roi_720 / trade_leverage }
                        else if time_in_position >= config.roi_period_30 && leveraged_bound_return >= config.roi_15 { config.roi_15 / trade_leverage }
                        else if time_in_position >= config.roi_period_10 && leveraged_bound_return >= config.roi_3 { config.roi_3 / trade_leverage }
                        else { config.roi_6 / trade_leverage };
                        let target = if is_long { base_price * (1.0 + roi_pct) } else { base_price * (1.0 - roi_pct) };
                        // Can't fill outside the candle's own actual range.
                        target.clamp(low_prices[i].min(high_prices[i]), low_prices[i].max(high_prices[i]))
                    }
                    // Every other exit type fills at this candle's OPEN in
                    // freqtrade's real backtesting.py (_get_close_rate falls
                    // through to row[OPEN_IDX] for anything but
                    // STOP_LOSS/TRAILING_STOP_LOSS/LIQUIDATION/ROI).
                    _ => open_prices[i],
                };

                let exit_price = if is_long { base_exit_price * (1.0 - fee_rate) } else { base_exit_price * (1.0 + fee_rate) };
                let raw_profit = if is_long { (exit_price - entry_price) / entry_price } else { (entry_price - exit_price) / entry_price };
                let mut leveraged_profit = raw_profit * trade_leverage;

                let mut pnl_amount = if config.compounding_enabled { trade_position_size * leveraged_profit }
                else { (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade) * leveraged_profit };

                if use_dollar_fees && contract_mult > 0.0 && entry_price > 0.0 {
                    let notional = if config.compounding_enabled { trade_position_size }
                        else { (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade) };
                    let contracts = (notional / (entry_price * contract_mult)).ceil().max(1.0);
                    let fees = 2.0 * contracts * config.fee_dollars_taker_per_contract;
                    pnl_amount -= fees;
                    if notional > 0.0 { leveraged_profit -= fees / notional; }
                }
                current_equity += pnl_amount;
                if config.compounding_enabled { base_position_size = (current_equity * config.tradable_balance_ratio).max(0.0).min(effective_max_trade); }

                entry_indices.push(entry_idx as i32);
                exit_indices.push(i as i32);
                profits.push(leveraged_profit);
                pnl_amounts.push(pnl_amount);
                entry_prices_vec.push(entry_price);
                exit_prices_vec.push(exit_price);
                exit_reasons.push(exit_reason);
                entry_reasons.push(current_entry_reason);
                leverages_used.push(trade_leverage);
                durations.push((i - entry_idx) as i32);
                in_position = false;
                last_exit_idx = i;
            }
        }
    }

    let result = BacktestResult {
        entry_indices, exit_indices, profits, pnl_amounts,
        entry_prices: entry_prices_vec, exit_prices: exit_prices_vec,
        exit_reasons, entry_reasons, leverages: leverages_used, durations,
    };

    TradingEngineResult {
        backtest_result: result,
        ecp_mode: config.ecp_mode.clone(),
        time_decay_adjustments: time_decay_count,
        final_equity: current_equity, starting_balance: config.starting_balance,
        compounding_enabled: config.compounding_enabled,
    }
}

// Joint long+short simulation for "both" direction mode. Real freqtrade
// tracks exactly one open position per pair regardless of direction
// (`LocalTrade.bt_trades_open_pp[pair]` — entries are blocked whenever
// that list is non-empty, long or short, without position_stacking, which
// none of this crate's config profiles enable). The naive "run long and
// short as two fully independent unified_backtest calls, then merge"
// approach doesn't have that constraint, so it can (and empirically does)
// produce a long position and a short position open on the same pair at
// the same time - confirmed directly: JackknifeVarianceEstimator/BTC's
// long book had a trade open bars 999-1060 while the short book
// simultaneously had trades open 988-1002 and 1003-1074. This function is
// a straight copy of unified_backtest_pythonstyle's per-bar logic, with
// `is_long` promoted from a call-level constant to a per-trade variable
// chosen at entry time, and a single shared `in_position` gate across both
// signal arrays so only one direction can ever be open at once.
#[allow(unused_assignments)]
fn unified_backtest_pythonstyle_joint(
    close_prices: &[f32], open_prices: &[f32], high_prices: &[f32], low_prices: &[f32],
    _volumes: &[f32],
    rsi: &[f32], macd_hist: &[f32], bb_pos: &[f32], atr: &[f32], cci: &[f32],
    long_entry_signals: &[u8], long_exit_signals: &[u8],
    short_entry_signals: &[u8], short_exit_signals: &[u8],
    config: &UnifiedBacktestConfig,
) -> TradingEngineResult {
    let n = close_prices.len();
    let leverage = config.leverage_default;
    let use_dollar_fees = config.fee_mode.eq_ignore_ascii_case("dollar");
    let fee_rate = if use_dollar_fees { 0.0 } else { config.fee_taker };
    let contract_mult = 1.0_f32;
    let start_idx = config.startup_candle_count.max(config.eval_start_bar.saturating_sub(1)).min(n.saturating_sub(1));

    let est = (n / 50).max(16);
    let mut entry_indices = Vec::with_capacity(est);
    let mut exit_indices = Vec::with_capacity(est);
    let mut profits = Vec::with_capacity(est);
    let mut pnl_amounts = Vec::with_capacity(est);
    let mut entry_prices_vec = Vec::with_capacity(est);
    let mut exit_prices_vec = Vec::with_capacity(est);
    let mut exit_reasons = Vec::with_capacity(est);
    let mut entry_reasons = Vec::with_capacity(est);
    let mut leverages_used = Vec::with_capacity(est);
    let mut durations = Vec::with_capacity(est);    let mut is_long_vec: Vec<bool> = Vec::with_capacity(est);

    let effective_max_trade = config.max_trade_amount.min(config.max_open_capital);
    let mut current_equity = config.starting_balance;
    let mut base_position_size = (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade);
    let mut trade_position_size = base_position_size;

    let mut in_position = false;
    let mut is_long = true; // set at entry; value while flat is irrelevant
    let mut entry_price = 0.0f32;
    let mut base_price = 0.0f32;
    let mut entry_idx = 0usize;
    let mut trade_leverage = leverage;
    let mut current_entry_reason = EntryReason::Signal;

    let mut stop_loss_price = 0.0f32;
    let mut stop_is_trailing = false;
    let mut trailing_activated = false;

    let time_decay_count = 0usize;
    let mut last_exit_idx: usize = 0;

    for i in (start_idx + 1)..n {
        if !in_position {
            let long_active = long_entry_signals[i] > 0 && long_exit_signals[i] == 0;
            let short_active = short_entry_signals[i] > 0 && short_exit_signals[i] == 0;
            // Both signals firing on the same bar should be effectively
            // impossible by construction (entry conditions are opposite
            // threshold directions on the same underlying metric in every
            // ported strategy) - long is checked first as a deterministic
            // tie-break in that edge case.
            let (want_entry, entry_is_long) = if long_active { (true, true) }
                else if short_active { (true, false) }
                else { (false, true) };

            let edge_ok = match config.entry_edge_filter {
                crate::backtest::EntryEdgeFilter::None => true,
                crate::backtest::EntryEdgeFilter::Full => {
                    let sigs = if entry_is_long { long_entry_signals } else { short_entry_signals };
                    i == 0 || sigs[i - 1] == 0
                }
                crate::backtest::EntryEdgeFilter::Cooldown(bars) => i >= last_exit_idx + bars,
            };
            let can_enter = want_entry && i > last_exit_idx && edge_ok;

            if can_enter {
                is_long = entry_is_long;
                in_position = true;
                current_entry_reason = determine_entry_reason(rsi, cci, macd_hist, bb_pos, i, is_long);

                trade_leverage = leverage.min(config.leverage_max).max(0.1);
                trade_position_size = base_position_size.min(effective_max_trade).max(0.0);

                base_price = open_prices[i];
                entry_price = if is_long { base_price * (1.0 + fee_rate) } else { base_price * (1.0 - fee_rate) };
                entry_idx = i;
                trailing_activated = false;
                stop_is_trailing = false;
                stop_loss_price = if is_long { base_price * (1.0 + config.base_stoploss / trade_leverage) }
                                  else { base_price * (1.0 - config.base_stoploss / trade_leverage) };
            }
        }

        if in_position {
            let exit_signals = if is_long { long_exit_signals } else { short_exit_signals };
            let time_in_position = i - entry_idx;
            let bound = if is_long { high_prices[i] } else { low_prices[i] };
            let leveraged_bound_return = if is_long {
                ((bound * (1.0 - fee_rate)) / (base_price * (1.0 + fee_rate)) - 1.0) * trade_leverage
            } else {
                (1.0 - (bound * (1.0 + fee_rate)) / (base_price * (1.0 - fee_rate))) * trade_leverage
            };

            let dir_correct = if is_long { stop_loss_price < low_prices[i] } else { stop_loss_price > high_prices[i] };

            if dir_correct {
                if config.atr_stop_enabled && i < atr.len() && base_price > 0.0 {
                    let stop_price_raw = if is_long { base_price - atr[i] * config.atr_stop_multiplier }
                                          else { base_price + atr[i] * config.atr_stop_multiplier };
                    let candidate = if is_long { bound - (bound - stop_price_raw) / trade_leverage }
                                    else { bound + (stop_price_raw - bound) / trade_leverage };
                    let tighter = if is_long { candidate > stop_loss_price } else { candidate < stop_loss_price };
                    if tighter { stop_loss_price = candidate; stop_is_trailing = false; }
                }

                if config.trailing_enabled {
                    if !trailing_activated && leveraged_bound_return >= config.trailing_trigger {
                        trailing_activated = true;
                    }
                    if trailing_activated {
                        let trail_dist = config.trailing_offset / trade_leverage;
                        let candidate = if is_long { bound * (1.0 - trail_dist) } else { bound * (1.0 + trail_dist) };
                        let tighter = if is_long { candidate > stop_loss_price } else { candidate < stop_loss_price };
                        if tighter { stop_loss_price = candidate; stop_is_trailing = true; }
                    }
                }
            }

            let stop_triggered = if is_long { low_prices[i] <= stop_loss_price } else { high_prices[i] >= stop_loss_price };
            let roi_triggered = config.roi_enabled && (
                (time_in_position >= config.roi_period_0 && leveraged_bound_return >= config.roi_6) ||
                (time_in_position >= config.roi_period_10 && leveraged_bound_return >= config.roi_3) ||
                (time_in_position >= config.roi_period_30 && leveraged_bound_return >= config.roi_15) ||
                (time_in_position >= config.roi_period_720 && leveraged_bound_return >= config.roi_720)
            );
            let signal_exit_triggered = exit_signals[i] > 0;
            let cci_exit_triggered = config.cci_exit_enabled && i < cci.len() && {
                let cci_v = cci[i];
                (is_long && cci_v > config.cci_overbought) || (!is_long && cci_v < config.cci_oversold)
            };
            let rsi_exit_triggered = config.rsi_exit_enabled && i < rsi.len() && {
                let rsi_v = rsi[i];
                (is_long && rsi_v > config.rsi_overbought) || (!is_long && rsi_v < config.rsi_oversold)
            };
            let macd_exit_triggered = config.macd_reversal_exit && i > 1 && i < macd_hist.len() && {
                let hist_curr = macd_hist[i];
                let hist_prev = macd_hist[i - 1];
                (is_long && hist_curr < hist_prev && hist_prev > 0.0 && hist_curr < 0.0) ||
                (!is_long && hist_curr > hist_prev && hist_prev < 0.0 && hist_curr > 0.0)
            };
            let max_hold_triggered = time_in_position >= config.max_hold_period;

            let mut should_exit = false;
            let mut exit_reason = ExitReason::Signal;
            if !should_exit && signal_exit_triggered { should_exit = true; exit_reason = ExitReason::Signal; }
            if !should_exit && stop_triggered && !stop_is_trailing { should_exit = true; exit_reason = ExitReason::Stoploss; }
            if !should_exit && roi_triggered { should_exit = true; exit_reason = ExitReason::RoiTarget; }
            if !should_exit && stop_triggered && stop_is_trailing { should_exit = true; exit_reason = ExitReason::TrailingStop; }
            if !should_exit && cci_exit_triggered { should_exit = true; exit_reason = ExitReason::CciExit; }
            if !should_exit && rsi_exit_triggered { should_exit = true; exit_reason = ExitReason::RsiExit; }
            if !should_exit && macd_exit_triggered { should_exit = true; exit_reason = ExitReason::MacdExit; }
            if !should_exit && max_hold_triggered { should_exit = true; exit_reason = ExitReason::MaxHoldPeriod; }

            if should_exit {
                let base_exit_price = match exit_reason {
                    ExitReason::TrailingStop if time_in_position == 0 => {
                        let trail_dist = config.trailing_offset / trade_leverage;
                        if is_long {
                            (open_prices[i] * (1.0 + config.trailing_trigger.abs() - trail_dist.abs())).max(low_prices[i])
                        } else {
                            (open_prices[i] * (1.0 - config.trailing_trigger.abs() + trail_dist.abs())).min(high_prices[i])
                        }
                    }
                    ExitReason::TrailingStop | ExitReason::Stoploss => stop_loss_price,
                    ExitReason::RoiTarget => {
                        let roi_pct = if time_in_position >= config.roi_period_720 && leveraged_bound_return >= config.roi_720 { config.roi_720 / trade_leverage }
                        else if time_in_position >= config.roi_period_30 && leveraged_bound_return >= config.roi_15 { config.roi_15 / trade_leverage }
                        else if time_in_position >= config.roi_period_10 && leveraged_bound_return >= config.roi_3 { config.roi_3 / trade_leverage }
                        else { config.roi_6 / trade_leverage };
                        let target = if is_long { base_price * (1.0 + roi_pct) } else { base_price * (1.0 - roi_pct) };
                        target.clamp(low_prices[i].min(high_prices[i]), low_prices[i].max(high_prices[i]))
                    }
                    _ => open_prices[i],
                };

                let exit_price = if is_long { base_exit_price * (1.0 - fee_rate) } else { base_exit_price * (1.0 + fee_rate) };
                let raw_profit = if is_long { (exit_price - entry_price) / entry_price } else { (entry_price - exit_price) / entry_price };
                let mut leveraged_profit = raw_profit * trade_leverage;

                let mut pnl_amount = if config.compounding_enabled { trade_position_size * leveraged_profit }
                else { (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade) * leveraged_profit };

                if use_dollar_fees && contract_mult > 0.0 && entry_price > 0.0 {
                    let notional = if config.compounding_enabled { trade_position_size }
                        else { (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade) };
                    let contracts = (notional / (entry_price * contract_mult)).ceil().max(1.0);
                    let fees = 2.0 * contracts * config.fee_dollars_taker_per_contract;
                    pnl_amount -= fees;
                    if notional > 0.0 { leveraged_profit -= fees / notional; }
                }
                current_equity += pnl_amount;
                if config.compounding_enabled { base_position_size = (current_equity * config.tradable_balance_ratio).max(0.0).min(effective_max_trade); }

                entry_indices.push(entry_idx as i32);
                exit_indices.push(i as i32);
                profits.push(leveraged_profit);
                pnl_amounts.push(pnl_amount);
                entry_prices_vec.push(entry_price);
                exit_prices_vec.push(exit_price);
                exit_reasons.push(exit_reason);
                entry_reasons.push(current_entry_reason);
                leverages_used.push(trade_leverage);
                durations.push((i - entry_idx) as i32);
                is_long_vec.push(is_long);
                in_position = false;
                last_exit_idx = i;

                // freqtrade calls backtest_loop up to twice per bar
                // (`for _ in (0, 1): ... if not a or a == trade_dir: break`):
                // if a position just closed AND the OPPOSITE direction's
                // signal is active on this SAME bar, it allows an immediate
                // same-bar reversal entry rather than waiting for the next
                // bar. Verified directly against real Python:
                // JackknifeVarianceEstimator/BTC's real trade opens at the
                // exact same bar an earlier trade closes on. Only one such
                // reversal per bar, matching freqtrade's `(0, 1)` bound.
                let reversal_is_long = !is_long;
                let (rev_entry_sigs, rev_exit_sigs) = if reversal_is_long
                    { (long_entry_signals, long_exit_signals) } else { (short_entry_signals, short_exit_signals) };
                let reversal_active = rev_entry_sigs[i] > 0 && rev_exit_sigs[i] == 0;
                let reversal_edge_ok = match config.entry_edge_filter {
                    crate::backtest::EntryEdgeFilter::None => true,
                    crate::backtest::EntryEdgeFilter::Full => i == 0 || rev_entry_sigs[i - 1] == 0,
                    // last_exit_idx == i here, so `i >= last_exit_idx + bars` only holds for bars == 0.
                    crate::backtest::EntryEdgeFilter::Cooldown(bars) => bars == 0,
                };

                if reversal_active && reversal_edge_ok {
                    is_long = reversal_is_long;
                    in_position = true;
                    current_entry_reason = determine_entry_reason(rsi, cci, macd_hist, bb_pos, i, is_long);

                    trade_leverage = leverage.min(config.leverage_max).max(0.1);
                    trade_position_size = base_position_size.min(effective_max_trade).max(0.0);

                    base_price = open_prices[i];
                    entry_price = if is_long { base_price * (1.0 + fee_rate) } else { base_price * (1.0 - fee_rate) };
                    entry_idx = i;
                    trailing_activated = false;
                    stop_is_trailing = false;
                    stop_loss_price = if is_long { base_price * (1.0 + config.base_stoploss / trade_leverage) }
                                      else { base_price * (1.0 - config.base_stoploss / trade_leverage) };
                }
            }
        }
    }

    let result = BacktestResult {
        entry_indices, exit_indices, profits, pnl_amounts,
        entry_prices: entry_prices_vec, exit_prices: exit_prices_vec,
        exit_reasons, entry_reasons, leverages: leverages_used, durations,
    };
    let _ = is_long_vec; // available for a future per-trade direction field if needed

    TradingEngineResult {
        backtest_result: result,
        ecp_mode: config.ecp_mode.clone(),
        time_decay_adjustments: time_decay_count,
        final_equity: current_equity, starting_balance: config.starting_balance,
        compounding_enabled: config.compounding_enabled,
    }
}

/// Entry point for "both" direction mode - see unified_backtest_pythonstyle_joint's
/// doc comment for why this can't just run unified_backtest twice and merge.
/// Applies the same freqtrade signal-shift as unified_backtest to both
/// directions' exit-signal arrays.
pub fn unified_backtest_both(
    close_prices: &[f32], open_prices: &[f32], high_prices: &[f32], low_prices: &[f32],
    volumes: &[f32],
    rsi: &[f32], macd_hist: &[f32], bb_pos: &[f32], atr: &[f32], cci: &[f32],
    long_entry_signals: &[u8], long_exit_signals: &[u8],
    short_entry_signals: &[u8], short_exit_signals: &[u8],
    config: &UnifiedBacktestConfig,
) -> TradingEngineResult {
    let shift = |sigs: &[u8]| -> Vec<u8> {
        let mut out = vec![0u8; sigs.len()];
        for i in 1..sigs.len() { out[i] = sigs[i - 1]; }
        out
    };
    let long_exit_shifted = shift(long_exit_signals);
    let short_exit_shifted = shift(short_exit_signals);
    // Entries get the identical freqtrade .shift(1) as exits - see the
    // detailed rationale on unified_backtest's own entry_shifted line.
    let long_entry_shifted = shift(long_entry_signals);
    let short_entry_shifted = shift(short_entry_signals);
    unified_backtest_pythonstyle_joint(
        close_prices, open_prices, high_prices, low_prices, volumes,
        rsi, macd_hist, bb_pos, atr, cci,
        &long_entry_shifted, &long_exit_shifted, &short_entry_shifted, &short_exit_shifted,
        config,
    )
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

fn determine_entry_reason(rsi: &[f32], cci: &[f32], macd_hist: &[f32], bb_pos: &[f32], i: usize, is_long: bool) -> EntryReason {
    if i >= rsi.len() { return EntryReason::Signal; }
    let rsi_v  = rsi[i];
    let cci_v  = cci.get(i).copied().unwrap_or(0.0);
    let macd   = macd_hist.get(i).copied().unwrap_or(0.0);
    let bb     = bb_pos.get(i).copied().unwrap_or(0.5);
    let macd_p = if i > 0 { macd_hist.get(i - 1).copied().unwrap_or(0.0) } else { 0.0 };
    if is_long {
        if rsi_v < 30.0 { EntryReason::RsiOversold }
        else if cci_v < -100.0 { EntryReason::CciSignal }
        else if macd > 0.0 && i > 0 && macd_p <= 0.0 { EntryReason::MacdCross }
        else if bb < 0.2 { EntryReason::BollingerBand }
        else { EntryReason::Signal }
    } else {
        if rsi_v > 70.0 { EntryReason::RsiOverbought }
        else if cci_v > 100.0 { EntryReason::CciSignal }
        else if macd < 0.0 && i > 0 && macd_p >= 0.0 { EntryReason::MacdCross }
        else if bb > 0.8 { EntryReason::BollingerBand }
        else { EntryReason::Signal }
    }
}

// =============================================================================
// SWEEP FAST PATH — Scalar stats accumulation, zero per-trade allocations
// =============================================================================
//
// run_strategy_backtest_summary is the sweep-only variant of
// run_strategy_backtest_precomputed.  It runs the identical hot loop but
// accumulates wins/losses/sharpe inputs as running scalars rather than
// pushing into per-trade Vecs.  This eliminates:
//   • ~10 Vec allocations + T Vec::push calls per combo (T = trades)
//   • trading_result_to_json / merge_both_sides_json serde_json work
//   • Hundreds of MB of JSON piped through stdout and reparsed by parent
// The output JSON contains only the ~12 summary scalars that extract_bt_summary
// actually reads, so the rest of the pipeline is unchanged.

struct DirStats {
    total_trades: usize,
    wins:         usize,
    gross_profit: f32,
    gross_loss:   f32,
    sum_p:        f32,  // Σ leveraged_profit
    sum_p2:       f32,  // Σ leveraged_profit²  (for variance)
    max_dd:       f32,
    sum_dur:      i64,  // Σ (exit_bar - entry_bar)
    final_equity: f32,
}

// Scalar-accumulating twin of unified_backtest_pythonstyle (Mode2) below -
// same per-bar semantics (same-candle exit checking, ratcheting
// dir_correct-gated stop price, fee-inclusive leveraged_bound_return,
// freqtrade's exact exit-priority order, ECS dynamic leverage/stake, the
// optional entry_edge_filter throttle), but accumulates DirStats scalars
// instead of pushing into per-trade Vecs, since this runs once per
// parameter combo in a hyperopt sweep (thousands of calls). Keep any
// change here in sync with unified_backtest_pythonstyle - this function
// exists purely for performance, not because the semantics differ.
//
// Note: unlike the old edge-required version of this function, entries
// aren't restricted to signal rising-edges by default (matching Mode2), so
// the previous "jump straight to the next entry bar" pointer-chase
// optimization no longer applies in general - can_enter must be
// re-evaluated every bar. The exception is EntryEdgeFilter::Full, which
// does restore an edge requirement per-strategy, but that's a config
// choice, not something this loop can special-case cheaply without
// duplicating the whole function, so it isn't optimized for here either.
#[allow(unused_assignments)] // sentinel defaults are always overwritten at trade entry
fn sweep_dir(
    close_prices: &[f32], open_prices: &[f32], high_prices: &[f32], low_prices: &[f32],
    rsi: &[f32], macd_hist: &[f32], _bb_pos: &[f32], atr: &[f32], cci: &[f32],
    entry_signals: &[u8], exit_signals: &[u8],
    config: &UnifiedBacktestConfig,
) -> DirStats {
    let n = close_prices.len();
    let is_long = config.is_long();
    let leverage = config.leverage_default;
    let use_dollar_fees = config.fee_mode.eq_ignore_ascii_case("dollar");
    let fee_rate = if use_dollar_fees { 0.0 } else { config.fee_taker };
    let start_idx = config.startup_candle_count.max(config.eval_start_bar.saturating_sub(1)).min(n.saturating_sub(1));
    let effective_max_trade = config.max_trade_amount.min(config.max_open_capital);

    let mut current_equity = config.starting_balance;
    let mut base_position_size = (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade);
    let mut trade_position_size = base_position_size;

    let mut in_position = false;
    let mut entry_price = 0.0f32;
    let mut base_price  = 0.0f32;
    let mut entry_idx    = 0usize;
    let mut trade_leverage = leverage;

    let mut stop_loss_price   = 0.0f32;
    let mut stop_is_trailing  = false;
    let mut trailing_activated = false;

    let mut last_exit_idx: usize = 0;

    let mut ds = DirStats { total_trades: 0, wins: 0, gross_profit: 0.0, gross_loss: 0.0,
        sum_p: 0.0, sum_p2: 0.0, max_dd: 0.0, sum_dur: 0, final_equity: config.starting_balance };
    let mut bal  = 1.0f32;
    let mut peak = 1.0f32;

    let first = start_idx + 1;
    if first >= n { return ds; }

    for i in first..n {
        let signal_active = entry_signals[i] > 0;
        let no_exit_conflict = exit_signals[i] == 0;
        let edge_ok = match config.entry_edge_filter {
            crate::backtest::EntryEdgeFilter::None => true,
            crate::backtest::EntryEdgeFilter::Full => i == 0 || entry_signals[i - 1] == 0,
            crate::backtest::EntryEdgeFilter::Cooldown(bars) => i >= last_exit_idx + bars,
        };
        let can_enter = !in_position && signal_active && no_exit_conflict && i > last_exit_idx && edge_ok;

        if can_enter {
            in_position = true;

            trade_leverage = leverage.min(config.leverage_max).max(0.1);
            trade_position_size = base_position_size.min(effective_max_trade).max(0.0);

            base_price = open_prices[i];
            entry_price = if is_long { base_price * (1.0 + fee_rate) } else { base_price * (1.0 - fee_rate) };
            entry_idx = i;
            trailing_activated = false;
            stop_is_trailing = false;
            stop_loss_price = if is_long { base_price * (1.0 + config.base_stoploss / trade_leverage) }
                              else { base_price * (1.0 - config.base_stoploss / trade_leverage) };
        }

        if in_position {
            let time_in_position = i - entry_idx;
            let bound = if is_long { high_prices[i] } else { low_prices[i] };
            let leveraged_bound_return = if is_long {
                ((bound * (1.0 - fee_rate)) / (base_price * (1.0 + fee_rate)) - 1.0) * trade_leverage
            } else {
                (1.0 - (bound * (1.0 + fee_rate)) / (base_price * (1.0 - fee_rate))) * trade_leverage
            };

            let dir_correct = if is_long { stop_loss_price < low_prices[i] } else { stop_loss_price > high_prices[i] };

            if dir_correct {
                if config.atr_stop_enabled && i < atr.len() && base_price > 0.0 {
                    let stop_price_raw = if is_long { base_price - atr[i] * config.atr_stop_multiplier }
                                          else { base_price + atr[i] * config.atr_stop_multiplier };
                    let candidate = if is_long { bound - (bound - stop_price_raw) / trade_leverage }
                                    else { bound + (stop_price_raw - bound) / trade_leverage };
                    let tighter = if is_long { candidate > stop_loss_price } else { candidate < stop_loss_price };
                    if tighter { stop_loss_price = candidate; stop_is_trailing = false; }
                }

                if config.trailing_enabled {
                    if !trailing_activated && leveraged_bound_return >= config.trailing_trigger {
                        trailing_activated = true;
                    }
                    if trailing_activated {
                        let trail_dist = config.trailing_offset / trade_leverage;
                        let candidate = if is_long { bound * (1.0 - trail_dist) } else { bound * (1.0 + trail_dist) };
                        let tighter = if is_long { candidate > stop_loss_price } else { candidate < stop_loss_price };
                        if tighter { stop_loss_price = candidate; stop_is_trailing = true; }
                    }
                }
            }

            let stop_triggered = if is_long { low_prices[i] <= stop_loss_price } else { high_prices[i] >= stop_loss_price };
            let roi_triggered = config.roi_enabled && (
                (time_in_position >= config.roi_period_0 && leveraged_bound_return >= config.roi_6) ||
                (time_in_position >= config.roi_period_10 && leveraged_bound_return >= config.roi_3) ||
                (time_in_position >= config.roi_period_30 && leveraged_bound_return >= config.roi_15) ||
                (time_in_position >= config.roi_period_720 && leveraged_bound_return >= config.roi_720)
            );
            let signal_exit_triggered = exit_signals[i] > 0;
            let cci_exit_triggered = config.cci_exit_enabled && i < cci.len() && {
                let cci_v = cci[i];
                (is_long && cci_v > config.cci_overbought) || (!is_long && cci_v < config.cci_oversold)
            };
            let rsi_exit_triggered = config.rsi_exit_enabled && i < rsi.len() && {
                let rsi_v = rsi[i];
                (is_long && rsi_v > config.rsi_overbought) || (!is_long && rsi_v < config.rsi_oversold)
            };
            let macd_exit_triggered = config.macd_reversal_exit && i > 1 && i < macd_hist.len() && {
                let hist_curr = macd_hist[i];
                let hist_prev = macd_hist[i - 1];
                (is_long && hist_curr < hist_prev && hist_prev > 0.0 && hist_curr < 0.0) ||
                (!is_long && hist_curr > hist_prev && hist_prev < 0.0 && hist_curr > 0.0)
            };
            let max_hold_triggered = time_in_position >= config.max_hold_period;

            let mut should_exit = false;
            let mut exit_reason = ExitReason::Signal;
            if !should_exit && signal_exit_triggered { should_exit = true; exit_reason = ExitReason::Signal; }
            if !should_exit && stop_triggered && !stop_is_trailing { should_exit = true; exit_reason = ExitReason::Stoploss; }
            if !should_exit && roi_triggered { should_exit = true; exit_reason = ExitReason::RoiTarget; }
            if !should_exit && stop_triggered && stop_is_trailing { should_exit = true; exit_reason = ExitReason::TrailingStop; }
            if !should_exit && cci_exit_triggered { should_exit = true; exit_reason = ExitReason::CciExit; }
            if !should_exit && rsi_exit_triggered { should_exit = true; exit_reason = ExitReason::RsiExit; }
            if !should_exit && macd_exit_triggered { should_exit = true; exit_reason = ExitReason::MacdExit; }
            if !should_exit && max_hold_triggered { should_exit = true; exit_reason = ExitReason::MaxHoldPeriod; }

            if should_exit {
                let base_exit_price = match exit_reason {
                    ExitReason::TrailingStop if time_in_position == 0 => {
                        let trail_dist = config.trailing_offset / trade_leverage;
                        if is_long {
                            (open_prices[i] * (1.0 + config.trailing_trigger.abs() - trail_dist.abs())).max(low_prices[i])
                        } else {
                            (open_prices[i] * (1.0 - config.trailing_trigger.abs() + trail_dist.abs())).min(high_prices[i])
                        }
                    }
                    ExitReason::TrailingStop | ExitReason::Stoploss => stop_loss_price,
                    ExitReason::RoiTarget => {
                        let roi_pct = if time_in_position >= config.roi_period_720 && leveraged_bound_return >= config.roi_720 { config.roi_720 / trade_leverage }
                        else if time_in_position >= config.roi_period_30 && leveraged_bound_return >= config.roi_15 { config.roi_15 / trade_leverage }
                        else if time_in_position >= config.roi_period_10 && leveraged_bound_return >= config.roi_3 { config.roi_3 / trade_leverage }
                        else { config.roi_6 / trade_leverage };
                        let target = if is_long { base_price * (1.0 + roi_pct) } else { base_price * (1.0 - roi_pct) };
                        target.clamp(low_prices[i].min(high_prices[i]), low_prices[i].max(high_prices[i]))
                    }
                    _ => open_prices[i],
                };

                let exit_price = if is_long { base_exit_price * (1.0 - fee_rate) } else { base_exit_price * (1.0 + fee_rate) };
                let raw_profit = if is_long { (exit_price - entry_price) / entry_price } else { (entry_price - exit_price) / entry_price };
                let mut lp = raw_profit * trade_leverage;
                let mut pnl = if config.compounding_enabled { trade_position_size * lp }
                    else { (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade) * lp };

                if use_dollar_fees && entry_price > 0.0 {
                    let notional = if config.compounding_enabled { trade_position_size }
                        else { (config.starting_balance * config.tradable_balance_ratio).min(effective_max_trade) };
                    let contracts = (notional / entry_price).ceil().max(1.0);
                    let fees = 2.0 * contracts * config.fee_dollars_taker_per_contract;
                    pnl -= fees;
                    if notional > 0.0 { lp -= fees / notional; }
                }
                current_equity += pnl;
                if config.compounding_enabled {
                    base_position_size = (current_equity * config.tradable_balance_ratio).max(0.0).min(effective_max_trade);
                }

                ds.total_trades += 1;
                ds.sum_dur      += (i - entry_idx) as i64;
                if lp > 0.0 { ds.wins += 1; ds.gross_profit += lp; } else { ds.gross_loss += lp.abs(); }
                ds.sum_p  += lp;
                ds.sum_p2 += lp * lp;
                bal *= 1.0 + lp;
                if bal > peak { peak = bal; }
                let dd = if peak > 0.0 { (peak - bal) / peak } else { 0.0 };
                if dd > ds.max_dd { ds.max_dd = dd; }
                in_position = false;
                last_exit_idx = i;
            }
        }
    }
    ds.final_equity = current_equity;
    ds
}

fn ds_score(ds: &DirStats, n: usize, tf_min: usize) -> f32 {
    let t = ds.total_trades;
    if t == 0 { return f32::NEG_INFINITY; }
    let pf  = if ds.gross_loss == 0.0 { 99.0 } else { ds.gross_profit / ds.gross_loss };
    let avg = ds.sum_p / t as f32;
    let std = ((ds.sum_p2 / t as f32 - avg * avg).max(0.0)).sqrt();
    let tf  = tf_min.max(1) as f32;
    let bpy = (365.25_f32 * 1440.0) / tf;
    let tpy = (t as f32 / n as f32).max(1.0) * bpy;
    let sharpe = if std == 0.0 { 0.0 } else { (avg / std) * tpy.max(1.0).sqrt() };
    pf * (t as f32).sqrt() * (1.0 - ds.max_dd) * (1.0 + sharpe.max(0.0) * 0.1)
}

fn both_score(long: &DirStats, short: &DirStats, n: usize, tf_min: usize) -> f32 {
    let t = long.total_trades + short.total_trades;
    if t == 0 { return f32::NEG_INFINITY; }
    let gp  = long.gross_profit + short.gross_profit;
    let gl  = long.gross_loss   + short.gross_loss;
    let pf  = if gl == 0.0 { 99.0 } else { gp / gl };
    let sp  = long.sum_p  + short.sum_p;
    let sp2 = long.sum_p2 + short.sum_p2;
    let avg = sp / t as f32;
    let std = ((sp2 / t as f32 - avg * avg).max(0.0)).sqrt();
    let max_dd = long.max_dd.max(short.max_dd);
    let tf  = tf_min.max(1) as f32;
    let bpy = (365.25_f32 * 1440.0) / tf;
    let tpy = (t as f32 / n as f32).max(1.0) * bpy;
    let sharpe = if std == 0.0 { 0.0 } else { (avg / std) * tpy.max(1.0).sqrt() };
    pf * (t as f32).sqrt() * (1.0 - max_dd) * (1.0 + sharpe.max(0.0) * 0.1)
}

fn ds_to_json(
    ds: &DirStats, cfg: &super::backtest::StrategyConfig,
    direction: &str, initial_capital: f32, n: usize, tf_min: usize,
) -> serde_json::Value {
    let t = ds.total_trades;
    if t == 0 {
        return serde_json::json!({
            "strategy_name": cfg.name, "timeframe": cfg.timeframe, "direction": direction,
            "total_trades": 0, "win_rate": 0.0, "profit_factor": 0.0, "total_return": 0.0,
            "max_drawdown": 0.0, "sharpe_ratio": 0.0, "avg_profit": 0.0, "total_pnl": 0.0,
            "score": 0.0, "cagr": 0.0, "expectancy_r": 0.0,
            "final_equity": initial_capital, "avg_trade_duration_minutes": 0.0,
        });
    }
    let win_rate = ds.wins as f32 / t as f32;
    let pf  = if ds.gross_loss == 0.0 { 99.0f32 } else { ds.gross_profit / ds.gross_loss };
    let tr  = ds.sum_p;
    let avg = tr / t as f32;
    let std = ((ds.sum_p2 / t as f32 - avg * avg).max(0.0)).sqrt();
    let tf  = tf_min.max(1) as f32;
    let bpy = (365.25_f32 * 1440.0) / tf;
    let tpy = (t as f32 / n as f32).max(1.0) * bpy;
    let sharpe = if std == 0.0 { 0.0 } else { (avg / std) * tpy.max(1.0).sqrt() };
    let score  = pf * (t as f32).sqrt() * (1.0 - ds.max_dd) * (1.0 + sharpe.max(0.0) * 0.1);
    let years  = n as f32 / bpy;
    let cagr   = if years > 0.0 && ds.final_equity > 0.0 && initial_capital > 0.0 {
        (ds.final_equity / initial_capital).powf(1.0 / years) - 1.0
    } else { 0.0 };
    let losses   = t - ds.wins;
    let avg_win  = if ds.wins  > 0 { ds.gross_profit / ds.wins  as f32 } else { 0.0 };
    let avg_loss = if losses   > 0 { ds.gross_loss   / losses   as f32 } else { 1.0 };
    let exp_r    = if avg_loss > 0.0 { avg_win / avg_loss } else { 0.0 };
    let avg_dur  = ds.sum_dur as f32 / t as f32 * tf;
    serde_json::json!({
        "strategy_name": cfg.name, "timeframe": cfg.timeframe, "direction": direction,
        "total_trades": t, "wins": ds.wins, "losses": losses,
        "win_rate": win_rate, "profit_factor": pf, "total_return": tr,
        "max_drawdown": ds.max_dd, "sharpe_ratio": sharpe, "avg_profit": avg,
        "total_pnl": tr * initial_capital, "score": score, "cagr": cagr,
        "expectancy_r": exp_r, "final_equity": ds.final_equity,
        "avg_trade_duration_minutes": avg_dur,
    })
}

fn both_ds_to_json(
    long: &DirStats, short: &DirStats, cfg: &super::backtest::StrategyConfig,
    initial_capital: f32, n: usize, tf_min: usize,
) -> serde_json::Value {
    let t = long.total_trades + short.total_trades;
    if t == 0 {
        return serde_json::json!({
            "strategy_name": cfg.name, "timeframe": cfg.timeframe, "direction": "both",
            "total_trades": 0, "win_rate": 0.0, "profit_factor": 0.0, "total_return": 0.0,
            "max_drawdown": 0.0, "sharpe_ratio": 0.0, "avg_profit": 0.0, "total_pnl": 0.0,
            "score": 0.0, "cagr": 0.0, "expectancy_r": 0.0,
            "final_equity": initial_capital, "avg_trade_duration_minutes": 0.0,
        });
    }
    let wins         = long.wins + short.wins;
    let gross_profit = long.gross_profit + short.gross_profit;
    let gross_loss   = long.gross_loss   + short.gross_loss;
    let sp           = long.sum_p  + short.sum_p;
    let sp2          = long.sum_p2 + short.sum_p2;
    let max_dd       = long.max_dd.max(short.max_dd);
    let sum_dur      = long.sum_dur + short.sum_dur;
    let final_equity = long.final_equity + short.final_equity - initial_capital;

    let win_rate = wins as f32 / t as f32;
    let pf  = if gross_loss == 0.0 { 99.0f32 } else { gross_profit / gross_loss };
    let tr  = sp;
    let avg = tr / t as f32;
    let std = ((sp2 / t as f32 - avg * avg).max(0.0)).sqrt();
    let tf  = tf_min.max(1) as f32;
    let bpy = (365.25_f32 * 1440.0) / tf;
    let tpy = (t as f32 / n as f32).max(1.0) * bpy;
    let sharpe = if std == 0.0 { 0.0 } else { (avg / std) * tpy.max(1.0).sqrt() };
    let score  = pf * (t as f32).sqrt() * (1.0 - max_dd) * (1.0 + sharpe.max(0.0) * 0.1);
    let years  = n as f32 / bpy;
    let cagr   = if years > 0.0 && final_equity > 0.0 && initial_capital > 0.0 {
        (final_equity / initial_capital).powf(1.0 / years) - 1.0
    } else { 0.0 };
    let losses   = t - wins;
    let avg_win  = if wins   > 0 { gross_profit / wins   as f32 } else { 0.0 };
    let avg_loss = if losses > 0 { gross_loss   / losses as f32 } else { 1.0 };
    let exp_r    = if avg_loss > 0.0 { avg_win / avg_loss } else { 0.0 };
    let avg_dur  = sum_dur as f32 / t as f32 * tf;
    serde_json::json!({
        "strategy_name": cfg.name, "timeframe": cfg.timeframe, "direction": "both",
        "total_trades": t, "wins": wins, "losses": losses,
        "win_rate": win_rate, "profit_factor": pf, "total_return": tr,
        "max_drawdown": max_dd, "sharpe_ratio": sharpe, "avg_profit": avg,
        "total_pnl": tr * initial_capital, "score": score, "cagr": cagr,
        "expectancy_r": exp_r, "final_equity": final_equity,
        "avg_trade_duration_minutes": avg_dur,
    })
}

/// Compact single-symbol backtest for the subprocess path. Takes raw slices,
/// computes indicators, then uses the scalar sweep_dir path (no per-trade Vecs).
/// Replaces run_strategy_backtest for callers that only need summary metrics.
pub fn run_strategy_backtest_compact(
    strategy: &dyn super::backtest::Strategy,
    symbol: &str,
    close: &[f32], open: &[f32], high: &[f32], low: &[f32], volume: &[f32],
    direction: &str,
    initial_capital: f32,
    fee_schedule: Option<(f32, f32)>,
    config_overlay: Option<&serde_json::Value>,
) -> serde_json::Value {
    let n = close.len();
    if n == 0 { return serde_json::json!({"error": "No data"}); }
    let arc_close  = std::sync::Arc::new(close.to_vec());
    let arc_open   = std::sync::Arc::new(open.to_vec());
    let arc_high   = std::sync::Arc::new(high.to_vec());
    let arc_low    = std::sync::Arc::new(low.to_vec());
    let arc_volume = std::sync::Arc::new(volume.to_vec());
    let precomputed = std::sync::Arc::new(
        crate::fast_indicators::calculate_standard_indicators(&arc_close, &arc_high, &arc_low, &arc_volume)
    );
    run_strategy_backtest_summary(strategy, symbol, &arc_close, &arc_open, &arc_high, &arc_low, &arc_volume, &precomputed, direction, initial_capital, fee_schedule, config_overlay)
}

/// Sweep-only backtest: same logic as run_strategy_backtest_precomputed but returns
/// a compact JSON (scalars only — no per-trade arrays).  Eliminates all serde_json
/// array building and the stdout pipe overhead.
pub fn run_strategy_backtest_summary(
    strategy: &dyn super::backtest::Strategy,
    symbol: &str,
    arc_close:  &std::sync::Arc<Vec<f32>>,
    arc_open:   &std::sync::Arc<Vec<f32>>,
    arc_high:   &std::sync::Arc<Vec<f32>>,
    arc_low:    &std::sync::Arc<Vec<f32>>,
    arc_volume: &std::sync::Arc<Vec<f32>>,
    precomputed: &std::sync::Arc<HashMap<usize, Vec<f32>>>,
    direction: &str,
    initial_capital: f32,
    fee_schedule: Option<(f32, f32)>,
    config_overlay: Option<&serde_json::Value>,
) -> serde_json::Value {
    let close  = arc_close.as_slice();
    let open   = arc_open.as_slice();
    let high   = arc_high.as_slice();
    let low    = arc_low.as_slice();
    let volume = arc_volume.as_slice();
    let n = close.len();
    if n == 0 { return serde_json::json!({"error": "No data"}); }

    let mut cfg = strategy.config().clone();
    if let Some(ov) = config_overlay { apply_strategy_overlay(&mut cfg, ov); }

    let custom = strategy.calculate_custom_indicators(arc_close, arc_open, arc_high, arc_low, arc_volume);
    let merged: std::sync::Arc<HashMap<usize, Vec<f32>>> = if custom.is_empty() {
        std::sync::Arc::clone(precomputed)
    } else {
        let mut combined = precomputed.as_ref().clone();
        combined.extend(custom);
        std::sync::Arc::new(combined)
    };

    let empty: Vec<f32> = Vec::new();
    let rsi_c = merged.get(&0).unwrap_or(&empty);
    let mh_c  = merged.get(&9).unwrap_or(&empty);
    let bb_c  = merged.get(&10).unwrap_or(&empty);
    let atr_c = merged.get(&14).unwrap_or(&empty);
    let cci_c = merged.get(&17).unwrap_or(&empty);

    let ctx = super::backtest::SignalContext {
        indicators: std::sync::Arc::clone(&merged),
        close:  std::sync::Arc::clone(arc_close),
        open:   std::sync::Arc::clone(arc_open),
        high:   std::sync::Arc::clone(arc_high),
        low:    std::sync::Arc::clone(arc_low),
        volume: std::sync::Arc::clone(arc_volume),
        n,
    };
    let _ = volume; // consumed by SignalContext, not used directly
    let (mut long_sigs, mut short_sigs) = strategy.populate_entry_trend(&ctx);
    if long_sigs.len() != n  { long_sigs.resize(n, 0); }
    if short_sigs.len() != n { short_sigs.resize(n, 0); }

    let want_short_raw = direction == "short" || direction == "both";
    let want_short = want_short_raw && !cfg.is_spot();
    let want_long  = direction == "long" || direction == "both" || (want_short_raw && cfg.is_spot());

    // sweep_dir mirrors Mode2 semantics, which needs exit signals shifted
    // one bar (freqtrade's real _get_ohlcv_as_lists .shift(1) - see the
    // identical shift/rationale in unified_backtest's dispatcher). Applied
    // once here rather than per sweep_dir call, since these arrays are
    // shared across every parameter combo.
    let mk_exit = |entry_sigs: &[u8], dir_str: &str| -> Vec<u8> {
        let entry_idx: Vec<usize> = (0..n).filter(|&i| entry_sigs[i] != 0).collect();
        let entry_px: Vec<f32>   = entry_idx.iter().map(|&i| close[i]).collect();
        let mut s = strategy.populate_exit_trend(&ctx, dir_str, &entry_idx, &entry_px);
        if s.len() != n { s.resize(n, 0); }
        let mut shifted = vec![0u8; n];
        for i in 1..n { shifted[i] = s[i - 1]; }
        shifted
    };

    let mut ub = UnifiedBacktestConfig::default();
    let tf_min = crate::backtest::StrategyConfig::parse_timeframe(&cfg.timeframe).max(1);
    ub.timeframe = cfg.timeframe.clone(); ub.timeframe_minutes = tf_min;
    ub.trade_type = cfg.trade_type; ub.symbol = symbol.to_string();
    ub.startup_candle_count = cfg.startup_candle_count;
    ub.leverage_default = cfg.effective_leverage();
    ub.leverage_max = cfg.leverage_max.max(cfg.effective_leverage());
    ub.leverage_mode = cfg.leverage_mode;
    ub.fee_taker = cfg.fee_taker; ub.fee_maker = cfg.fee_maker;
    if let Some((maker, taker)) = fee_schedule {
        ub.fee_mode = "dollar".to_string();
        ub.fee_dollars_maker_per_contract = maker;
        ub.fee_dollars_taker_per_contract = taker;
    }
    ub.base_stoploss = cfg.stoploss; ub.max_hold_period = cfg.max_hold_period;
    ub.trailing_enabled = cfg.trailing_stop; ub.trailing_trigger = cfg.trailing_stop_positive_offset;
    ub.trailing_offset = cfg.trailing_stop_positive;
    ub.atr_stop_enabled = cfg.atr_stop_enabled; ub.atr_stop_multiplier = cfg.atr_stop_multiplier;
    ub.entry_edge_filter = cfg.entry_edge_filter;
    assign_minimal_roi(&mut ub, &cfg.minimal_roi, tf_min);
    ub.starting_balance = initial_capital;
    if cfg.ecp_enabled { ub.ecp_mode = cfg.ecp_mode.clone(); }
    if let Some(ov) = config_overlay { apply_overlay_to_ub_cfg(&mut ub, ov); }

    if want_long && want_short {
        let long_exits  = mk_exit(&long_sigs,  "long");
        let short_exits = mk_exit(&short_sigs, "short");
        let mut lcfg = ub.clone(); lcfg.direction = "long".to_string();
        let mut scfg = ub.clone(); scfg.direction = "short".to_string();
        let lds = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, &long_sigs,  &long_exits,  &lcfg);
        let sds = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, &short_sigs, &short_exits, &scfg);

        let ls = ds_score(&lds, n, tf_min);
        let ss = ds_score(&sds, n, tf_min);
        let bs = both_score(&lds, &sds, n, tf_min);
        let lt = if lds.total_trades > 0 { 1i32 } else { 0 };
        let st = if sds.total_trades > 0 { 1i32 } else { 0 };
        let bt = if lds.total_trades + sds.total_trades > 0 { 1i32 } else { 0 };
        let cmp = |ai: i32, af: f32, bi: i32, bf: f32| -> bool {
            (ai, af.to_bits()) >= (bi, bf.to_bits())
        };
        if cmp(bt, bs, lt, ls) && cmp(bt, bs, st, ss) {
            both_ds_to_json(&lds, &sds, &cfg, initial_capital, n, tf_min)
        } else if cmp(lt, ls, st, ss) {
            ds_to_json(&lds, &cfg, "long", initial_capital, n, tf_min)
        } else {
            ds_to_json(&sds, &cfg, "short", initial_capital, n, tf_min)
        }
    } else {
        let (entry_sigs, dir_str) = if want_short { (&short_sigs, "short") } else { (&long_sigs, "long") };
        let exit_sigs = mk_exit(entry_sigs, dir_str);
        let mut dcfg = ub; dcfg.direction = dir_str.to_string();
        let ds = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, entry_sigs, &exit_sigs, &dcfg);
        ds_to_json(&ds, &cfg, dir_str, initial_capital, n, tf_min)
    }
}

// =============================================================================
// PARAMETER SWEEP — compute signals once, rayon-parallel sweep_dir over grid
// =============================================================================
//
// run_param_sweep is a drop-in replacement for run_strategy_backtest_summary.
// It returns the same JSON shape (plus "sweep_best_params" and
// "sweep_combos_tested" fields) but internally evaluates every
// (stoploss × roi_tier_set × trailing) combination from ParamGrid in parallel,
// keeping signals and indicators computed once and shared across all combos.
//
// roi_tier_set: each entry is Vec<(minute_offset, target_pct)> pairs.
// Multi-tier example: [(0, 0.05), (30, 0.025), (60, 0.01)] = 5%@0m→2.5%@30m→1%@60m.

fn apply_roi_tiers_to_ub(ub: &mut UnifiedBacktestConfig, tiers: &[(usize, f32)], tf_min: usize) {
    ub.roi_6 = 99.0; ub.roi_period_0   = usize::MAX;
    ub.roi_3 = 99.0; ub.roi_period_10  = usize::MAX;
    ub.roi_15 = 99.0; ub.roi_period_30 = usize::MAX;
    ub.roi_720 = 99.0; ub.roi_period_720 = usize::MAX;
    if tiers.is_empty() {
        ub.roi_enabled = false;
        return;
    }
    ub.roi_enabled = true;
    let tf = tf_min.max(1);
    for (i, &(min_off, target)) in tiers.iter().take(4).enumerate() {
        let bars = if min_off == 0 { 0 } else { (min_off + tf - 1) / tf };
        match i {
            0 => { ub.roi_6   = target; ub.roi_period_0   = bars; }
            1 => { ub.roi_3   = target; ub.roi_period_10  = bars; }
            2 => { ub.roi_15  = target; ub.roi_period_30  = bars; }
            3 => { ub.roi_720 = target; ub.roi_period_720 = bars; }
            _ => {}
        }
    }
}


pub fn run_param_sweep(
    strategy: &dyn super::backtest::Strategy,
    symbol: &str,
    arc_close:   &std::sync::Arc<Vec<f32>>,
    arc_open:    &std::sync::Arc<Vec<f32>>,
    arc_high:    &std::sync::Arc<Vec<f32>>,
    arc_low:     &std::sync::Arc<Vec<f32>>,
    arc_volume:  &std::sync::Arc<Vec<f32>>,
    precomputed: &std::sync::Arc<HashMap<usize, Vec<f32>>>,
    direction: &str,
    initial_capital: f32,
    fee_schedule: Option<(f32, f32)>,
    base_overlay: Option<&serde_json::Value>,
) -> serde_json::Value {
    use rayon::prelude::*;
    use super::backtest::ParamGrid;

    let close = arc_close.as_slice();
    let open  = arc_open.as_slice();
    let high  = arc_high.as_slice();
    let low   = arc_low.as_slice();
    let n = close.len();
    if n == 0 { return serde_json::json!({"error": "No data"}); }

    let mut cfg = strategy.config().clone();
    if let Some(ov) = base_overlay { apply_strategy_overlay(&mut cfg, ov); }

    // Merge custom indicators with the shared precomputed map (zero recompute for standard ones).
    let custom = strategy.calculate_custom_indicators(arc_close, arc_open, arc_high, arc_low, arc_volume);
    let merged: std::sync::Arc<HashMap<usize, Vec<f32>>> = if custom.is_empty() {
        std::sync::Arc::clone(precomputed)
    } else {
        let mut combined = precomputed.as_ref().clone();
        combined.extend(custom);
        std::sync::Arc::new(combined)
    };

    let empty: Vec<f32> = Vec::new();
    let rsi_c = merged.get(&0).unwrap_or(&empty);
    let mh_c  = merged.get(&9).unwrap_or(&empty);
    let bb_c  = merged.get(&10).unwrap_or(&empty);
    let atr_c = merged.get(&14).unwrap_or(&empty);
    let cci_c = merged.get(&17).unwrap_or(&empty);

    let ctx = super::backtest::SignalContext {
        indicators: std::sync::Arc::clone(&merged),
        close:  std::sync::Arc::clone(arc_close),
        open:   std::sync::Arc::clone(arc_open),
        high:   std::sync::Arc::clone(arc_high),
        low:    std::sync::Arc::clone(arc_low),
        volume: std::sync::Arc::clone(arc_volume),
        n,
    };

    // Compute entry/exit signals ONCE — shared across every param combo.
    let (mut long_sigs, mut short_sigs) = strategy.populate_entry_trend(&ctx);
    if long_sigs.len() != n  { long_sigs.resize(n, 0); }
    if short_sigs.len() != n { short_sigs.resize(n, 0); }

    let want_short_raw = direction == "short" || direction == "both";
    let want_short = want_short_raw && !cfg.is_spot();
    let want_long  = direction == "long" || direction == "both" || (want_short_raw && cfg.is_spot());

    // sweep_dir mirrors Mode2 semantics, which needs exit signals shifted
    // one bar (freqtrade's real _get_ohlcv_as_lists .shift(1) - see the
    // identical shift/rationale in unified_backtest's dispatcher). Applied
    // once here rather than per sweep_dir call, since these arrays are
    // shared across every parameter combo.
    let mk_exit = |entry_sigs: &[u8], dir_str: &str| -> Vec<u8> {
        let entry_idx: Vec<usize> = (0..n).filter(|&i| entry_sigs[i] != 0).collect();
        let entry_px: Vec<f32>   = entry_idx.iter().map(|&i| close[i]).collect();
        let mut s = strategy.populate_exit_trend(&ctx, dir_str, &entry_idx, &entry_px);
        if s.len() != n { s.resize(n, 0); }
        let mut shifted = vec![0u8; n];
        for i in 1..n { shifted[i] = s[i - 1]; }
        shifted
    };
    let long_exits  = if want_long  { mk_exit(&long_sigs,  "long")  } else { vec![0u8; n] };
    let short_exits = if want_short { mk_exit(&short_sigs, "short") } else { vec![0u8; n] };

    // Pre-slice borrows — shared immutably across all rayon threads.
    let long_sigs_s   = long_sigs.as_slice();
    let short_sigs_s  = short_sigs.as_slice();
    let long_exits_s  = long_exits.as_slice();
    let short_exits_s = short_exits.as_slice();

    // Build base UnifiedBacktestConfig from strategy settings.
    let mut base_ub = UnifiedBacktestConfig::default();
    let tf_min = crate::backtest::StrategyConfig::parse_timeframe(&cfg.timeframe).max(1);
    base_ub.timeframe = cfg.timeframe.clone();
    base_ub.timeframe_minutes = tf_min;
    base_ub.trade_type = cfg.trade_type;
    base_ub.symbol = symbol.to_string();
    base_ub.startup_candle_count = cfg.startup_candle_count;
    base_ub.leverage_default = cfg.effective_leverage();
    base_ub.leverage_max = cfg.leverage_max.max(cfg.effective_leverage());
    base_ub.leverage_mode = cfg.leverage_mode;
    base_ub.fee_taker = cfg.fee_taker;
    base_ub.fee_maker = cfg.fee_maker;
    if let Some((maker, taker)) = fee_schedule {
        base_ub.fee_mode = "dollar".to_string();
        base_ub.fee_dollars_maker_per_contract = maker;
        base_ub.fee_dollars_taker_per_contract = taker;
    }
    base_ub.max_hold_period = cfg.max_hold_period;
    base_ub.atr_stop_enabled = cfg.atr_stop_enabled;
    base_ub.atr_stop_multiplier = cfg.atr_stop_multiplier;
    base_ub.entry_edge_filter = cfg.entry_edge_filter;
    base_ub.starting_balance = initial_capital;
    if cfg.ecp_enabled { base_ub.ecp_mode = cfg.ecp_mode.clone(); }
    if let Some(ov) = base_overlay { apply_overlay_to_ub_cfg(&mut base_ub, ov); }

    // Flatten param grid to index tuples: (sl_idx, roi_idx, trail_idx).
    let grid = ParamGrid::default();
    let n_sl  = grid.stoplosses.len();
    let n_roi = grid.roi_tiers.len();
    let n_tr  = grid.trailing_configs.len();
    let combos: Vec<(usize, usize, usize)> = (0..n_sl).flat_map(|si| {
        (0..n_roi).flat_map(move |ri| {
            (0..n_tr).map(move |ti| (si, ri, ti))
        })
    }).collect();
    let n_combos = combos.len();

    // Parallel sweep: clone base_ub per combo (cheap — all primitive fields), run sweep_dir.
    let sweep: Vec<(f32, usize, usize, usize)> = combos.par_iter().map(|&(si, ri, ti)| {
        let sl = grid.stoplosses[si];
        let roi_tier = &grid.roi_tiers[ri];
        let (te, tt, to) = grid.trailing_configs[ti];

        let mut ub = base_ub.clone();
        ub.base_stoploss    = sl;
        apply_roi_tiers_to_ub(&mut ub, roi_tier, tf_min);
        ub.trailing_enabled = te;
        ub.trailing_trigger = tt;
        ub.trailing_offset  = to;

        let score = if want_long && want_short {
            let mut lc = ub.clone(); lc.direction = "long".to_string();
            let mut sc = ub;         sc.direction = "short".to_string();
            let ld = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, long_sigs_s,  long_exits_s,  &lc);
            let sd = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, short_sigs_s, short_exits_s, &sc);
            both_score(&ld, &sd, n, tf_min)
        } else {
            let (ent, ext, dir_str) = if want_short {
                (short_sigs_s, short_exits_s, "short")
            } else {
                (long_sigs_s, long_exits_s, "long")
            };
            ub.direction = dir_str.to_string();
            let ds = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, ent, ext, &ub);
            ds_score(&ds, n, tf_min)
        };
        (score, si, ri, ti)
    }).collect();

    // Find best combo indices.
    let (best_score, best_si, best_ri, best_ti) = sweep.iter().copied()
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or((f32::NEG_INFINITY, 0, 0, 0));

    let best_sl  = grid.stoplosses[best_si];
    let best_roi = &grid.roi_tiers[best_ri];
    let (best_te, best_tt, best_to) = grid.trailing_configs[best_ti];

    // Re-run the winning combo to produce the output JSON.
    let mut best_ub = base_ub;
    best_ub.base_stoploss    = best_sl;
    apply_roi_tiers_to_ub(&mut best_ub, best_roi, tf_min);
    best_ub.trailing_enabled = best_te;
    best_ub.trailing_trigger = best_tt;
    best_ub.trailing_offset  = best_to;

    let mut result_json = if want_long && want_short {
        let mut lc = best_ub.clone(); lc.direction = "long".to_string();
        let mut sc = best_ub;         sc.direction = "short".to_string();
        let ld = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, long_sigs_s,  long_exits_s,  &lc);
        let sd = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, short_sigs_s, short_exits_s, &sc);
        let ls = ds_score(&ld, n, tf_min);
        let ss = ds_score(&sd, n, tf_min);
        let bs = both_score(&ld, &sd, n, tf_min);
        let cmp = |ai: i32, af: f32, bi: i32, bf: f32| (ai, af.to_bits()) >= (bi, bf.to_bits());
        let lt = if ld.total_trades > 0 { 1i32 } else { 0 };
        let st = if sd.total_trades > 0 { 1i32 } else { 0 };
        let bt = if ld.total_trades + sd.total_trades > 0 { 1i32 } else { 0 };
        if cmp(bt, bs, lt, ls) && cmp(bt, bs, st, ss) {
            both_ds_to_json(&ld, &sd, &cfg, initial_capital, n, tf_min)
        } else if cmp(lt, ls, st, ss) {
            ds_to_json(&ld, &cfg, "long", initial_capital, n, tf_min)
        } else {
            ds_to_json(&sd, &cfg, "short", initial_capital, n, tf_min)
        }
    } else {
        let (ent, ext, dir_str) = if want_short {
            (short_sigs_s, short_exits_s, "short")
        } else {
            (long_sigs_s, long_exits_s, "long")
        };
        let mut dc = best_ub; dc.direction = dir_str.to_string();
        let ds = sweep_dir(close, open, high, low, rsi_c, mh_c, bb_c, atr_c, cci_c, ent, ext, &dc);
        ds_to_json(&ds, &cfg, dir_str, initial_capital, n, tf_min)
    };

    // Encode roi_tiers as [[min_offset, pct], ...] for downstream JSON parsing.
    let roi_tiers_json: serde_json::Value = best_roi.iter()
        .map(|&(m, p)| serde_json::json!([m, p]))
        .collect::<Vec<_>>()
        .into();

    if let Some(obj) = result_json.as_object_mut() {
        obj.insert("sweep_combos_tested".into(), serde_json::json!(n_combos));
        obj.insert("sweep_best_score".into(),    serde_json::json!(best_score));
        obj.insert("sweep_best_params".into(), serde_json::json!({
            "stoploss":          best_sl,
            "roi_tiers":         roi_tiers_json,
            "trailing_enabled":  best_te,
            "trailing_trigger":  best_tt,
            "trailing_offset":   best_to,
        }));
    }
    result_json
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
#[path = "cpu_engine_tests.rs"]
mod tests;