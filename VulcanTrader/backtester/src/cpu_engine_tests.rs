use super::*;
use crate::backtest::{StrategyConfig, TradeType, UnifiedBacktestConfig};

#[test]
fn unified_backtest_no_entries_yields_no_trades() {
    let n = 100;
    let close: Vec<f32> = (0..n).map(|i| 100.0 + i as f32).collect();
    let open = close.clone();
    let high: Vec<f32> = close.iter().map(|c| c + 1.0).collect();
    let low: Vec<f32> = close.iter().map(|c| c - 1.0).collect();
    let volume = vec![1000.0f32; n];
    let empty = vec![0.0f32; n];
    let entries = vec![0u8; n];
    let exits = vec![0u8; n];

    let mut cfg = UnifiedBacktestConfig::default();
    cfg.startup_candle_count = 5;

    let result = unified_backtest(
        &close, &open, &high, &low, &volume,
        &empty, &empty, &empty, &empty, &empty,
        &entries, &exits, &cfg,
    );
    assert_eq!(result.total_trades(), 0);
}

#[test]
fn run_strategy_backtest_reports_extended_metrics_shape() {
    struct AlwaysLongOnce;
    impl super::super::backtest::Strategy for AlwaysLongOnce {
        fn config(&self) -> &StrategyConfig {
            use std::sync::OnceLock;
            static CFG: OnceLock<StrategyConfig> = OnceLock::new();
            CFG.get_or_init(|| StrategyConfig::new("15m").with_trade_type(TradeType::Futures))
        }
        fn calculate_custom_indicators(
            &self, close: &[f32], _open: &[f32], _high: &[f32], _low: &[f32], _volume: &[f32],
        ) -> std::collections::HashMap<usize, Vec<f32>> {
            let _ = close;
            std::collections::HashMap::new()
        }
        fn populate_entry_trend(&self, ctx: &super::super::backtest::SignalContext) -> (Vec<u8>, Vec<u8>) {
            let mut longs = vec![0u8; ctx.n];
            if ctx.n > 60 { longs[60] = 1; }
            (longs, vec![0u8; ctx.n])
        }
        fn populate_exit_trend(
            &self, ctx: &super::super::backtest::SignalContext, _direction: &str,
            _entry_indices: &[usize], _entry_prices: &[f32],
        ) -> Vec<u8> {
            let mut exits = vec![0u8; ctx.n];
            if ctx.n > 70 { exits[70] = 1; }
            exits
        }
    }

    let n = 200;
    let close: Vec<f32> = (0..n).map(|i| 100.0 + (i as f32 * 0.1)).collect();
    let open = close.clone();
    let high: Vec<f32> = close.iter().map(|c| c + 0.5).collect();
    let low: Vec<f32> = close.iter().map(|c| c - 0.5).collect();
    let volume = vec![1000.0f32; n];

    let strategy = AlwaysLongOnce;
    let out = run_strategy_backtest(&strategy, "TEST/USDC", &close, &open, &high, &low, &volume, "long", 10000.0, None, None);

    assert_eq!(out.get("total_trades").and_then(|v| v.as_u64()), Some(1));
    for key in [
        "sharpe", "sortino", "calmar", "sqn", "expectancy", "expectancy_ratio",
        "max_drawdown_abs", "max_drawdown_account", "max_relative_drawdown",
        "max_consecutive_wins", "max_consecutive_losses", "holding_avg_minutes",
        "trade_count_long", "trade_count_short", "backtest_days", "profit_mean", "profit_median",
    ] {
        assert!(out.get(key).is_some(), "missing field: {key}");
    }
    assert_eq!(out.get("trade_count_long").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(out.get("trade_count_short").and_then(|v| v.as_u64()), Some(0));
}
