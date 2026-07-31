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

// =============================================================================
// LIMIT-ORDER SIMULATION
// =============================================================================

/// Flat 100.00 market with a configurable per-bar low/high, so a resting
/// order's fill is decided purely by whether the range reaches it.
struct Bars {
    open: Vec<f32>,
    high: Vec<f32>,
    low: Vec<f32>,
    close: Vec<f32>,
    volume: Vec<f32>,
}

fn flat_bars(n: usize, low: f32, high: f32) -> Bars {
    Bars {
        open: vec![100.0; n],
        high: vec![high; n],
        low: vec![low; n],
        close: vec![100.0; n],
        volume: vec![1000.0; n],
    }
}

/// Signals are consumed one bar late (freqtrade's `.shift(1)`), so a signal
/// set on bar `i` is acted on at bar `i + 1`.
fn sig_at(n: usize, bars: &[usize]) -> Vec<u8> {
    let mut v = vec![0u8; n];
    for &b in bars { v[b] = 1; }
    v
}

fn limit_cfg() -> UnifiedBacktestConfig {
    let mut cfg = UnifiedBacktestConfig::default();
    cfg.startup_candle_count = 3;
    cfg.fee_taker = 0.0005;
    cfg.fee_maker = 0.0002;
    cfg.compounding_enabled = false;
    cfg
}

fn run(b: &Bars, entries: &[u8], exits: &[u8], cfg: &UnifiedBacktestConfig) -> TradingEngineResult {
    let empty = vec![0.0f32; b.close.len()];
    unified_backtest(
        &b.close, &b.open, &b.high, &b.low, &b.volume,
        &empty, &empty, &empty, &empty, &empty,
        entries, exits, cfg,
    )
}

#[test]
fn zero_offset_limit_is_identical_to_market() {
    let n = 40;
    let b = flat_bars(n, 99.0, 101.0);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut market = limit_cfg();
    market.entry_order_type = "market".into();
    market.exit_order_type = "market".into();
    let mut limit0 = limit_cfg(); // limit both sides, zero offset

    limit0.entry_limit_offset = 0.0;
    let m = run(&b, &entries, &exits, &market);
    let l = run(&b, &entries, &exits, &limit0);

    assert_eq!(m.total_trades(), 1);
    assert_eq!(l.total_trades(), m.total_trades());
    assert_eq!(l.backtest_result.entry_prices, m.backtest_result.entry_prices);
    assert_eq!(l.backtest_result.exit_prices, m.backtest_result.exit_prices);
    assert_eq!(l.backtest_result.profits, m.backtest_result.profits);
    assert_eq!(l.entry_orders_timedout, 0);
}

#[test]
fn resting_entry_fills_at_its_own_price_and_pays_maker() {
    let n = 40;
    let b = flat_bars(n, 99.0, 101.0);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = limit_cfg();
    cfg.entry_limit_offset = 0.005; // rest at 99.50, inside the 99..101 range

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    // Filled on the placement bar (bar 10 = signal bar 9 + the shift) at the
    // resting price, with the maker fee baked in.
    assert_eq!(res.backtest_result.entry_indices[0], 10);
    let expected_entry = 99.5 * (1.0 + 0.0002);
    assert!((res.backtest_result.entry_prices[0] - expected_entry).abs() < 1e-3,
            "entry {} != {expected_entry}", res.backtest_result.entry_prices[0]);
    assert_eq!(res.entry_orders_placed, 1);
    assert_eq!(res.entry_orders_timedout, 0);
}

#[test]
fn untouched_entry_order_times_out_and_the_trade_never_happens() {
    let n = 40;
    // Low never dips below 99.9, so a 99.50 resting order is never reached.
    let b = flat_bars(n, 99.9, 101.0);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = limit_cfg();
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = 3;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 0);
    assert_eq!(res.entry_orders_placed, 1);
    assert_eq!(res.entry_orders_timedout, 1);
}

#[test]
fn with_no_timeout_configured_an_entry_order_rests_until_it_fills() {
    let n = 40;
    // Out of reach until bar 25 finally trades down to the resting price.
    let mut b = flat_bars(n, 99.9, 101.0);
    b.low[25] = 99.0;
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[30]);

    let mut cfg = limit_cfg(); // default timeout = usize::MAX, i.e. none
    cfg.entry_limit_offset = 0.005;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1, "the order should still have been live at bar 25");
    assert_eq!(res.backtest_result.entry_indices[0], 25);
    assert_eq!(res.entry_orders_timedout, 0);
}

#[test]
fn resting_exit_order_waits_for_the_price_to_come_to_it() {
    let n = 40;
    let mut b = flat_bars(n, 99.0, 100.5);
    // Only bar 17 trades up to 101.2, so a 101.00 resting exit can only fill there.
    b.high[17] = 101.2;
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]); // acted on at bar 15

    let mut cfg = limit_cfg();
    cfg.exit_limit_offset = 0.01; // rest the exit at 101.00
    cfg.exit_fill_timeout_bars = 5;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    assert_eq!(res.backtest_result.exit_indices[0], 17, "exit should wait for the touch");
    assert_eq!(res.backtest_result.exit_reasons[0], crate::backtest::ExitReason::Signal);
    let expected_exit = 101.0 * (1.0 - 0.0002); // maker on the resting fill
    assert!((res.backtest_result.exit_prices[0] - expected_exit).abs() < 1e-3,
            "exit {} != {expected_exit}", res.backtest_result.exit_prices[0]);
    assert_eq!(res.exit_orders_timedout, 0);
}

#[test]
fn timed_out_exit_order_leaves_the_position_open() {
    let n = 40;
    // Never reaches 101.00, so the resting exit expires unfilled.
    let b = flat_bars(n, 99.0, 100.5);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = limit_cfg();
    cfg.exit_limit_offset = 0.01;
    cfg.exit_fill_timeout_bars = 2;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 0, "position stays open; no trade is recorded");
    assert!(res.exit_orders_placed >= 1);
    assert!(res.exit_orders_timedout >= 1);
}

#[test]
fn a_stoploss_supersedes_a_resting_exit_order() {
    let n = 40;
    let mut b = flat_bars(n, 99.0, 100.5);
    // Bar 16 collapses through the -2% stop, while the resting 101.00 exit
    // placed on bar 15 is still live.
    b.low[16] = 97.0;
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = limit_cfg();
    cfg.base_stoploss = -0.02;
    cfg.exit_limit_offset = 0.01;
    cfg.exit_fill_timeout_bars = 10;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    assert_eq!(res.backtest_result.exit_indices[0], 16);
    assert_eq!(res.backtest_result.exit_reasons[0], crate::backtest::ExitReason::Stoploss);
    // Protective exits keep paying taker even under a resting-exit config.
    let expected_exit = 98.0 * (1.0 - 0.0005);
    assert!((res.backtest_result.exit_prices[0] - expected_exit).abs() < 1e-2,
            "exit {} != {expected_exit}", res.backtest_result.exit_prices[0]);
}

#[test]
fn overlay_maps_freqtrade_order_types_and_unfilledtimeout() {
    let mut ub = UnifiedBacktestConfig::default();
    ub.timeframe_minutes = 15;
    apply_overlay_to_ub_cfg(&mut ub, &serde_json::json!({
        "order_types": {"entry": "market", "exit": "limit"},
        "unfilledtimeout": {"entry": 60, "exit": 20, "unit": "minutes"},
        "entry_limit_offset": 0.001,
    }));
    assert_eq!(ub.entry_order_type, "market");
    assert_eq!(ub.exit_order_type, "limit");
    assert_eq!(ub.entry_fill_timeout_bars, 4); // 60m / 15m
    assert_eq!(ub.exit_fill_timeout_bars, 1);  // 20m / 15m, floored
    assert!((ub.entry_limit_offset - 0.001).abs() < 1e-9);
}

// =============================================================================
// LIMIT ORDERS — SHORT SIDE
// =============================================================================

fn short_cfg() -> UnifiedBacktestConfig {
    let mut cfg = limit_cfg();
    cfg.direction = "short".into();
    cfg
}

#[test]
fn short_resting_entry_rests_above_the_open() {
    let n = 40;
    let b = flat_bars(n, 99.0, 101.0);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = short_cfg();
    cfg.entry_limit_offset = 0.005; // a short sells INTO strength: 100.50

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    let expected_entry = 100.5 * (1.0 - 0.0002); // short entry nets the fee off
    assert!((res.backtest_result.entry_prices[0] - expected_entry).abs() < 1e-3,
            "entry {} != {expected_entry}", res.backtest_result.entry_prices[0]);
}

#[test]
fn short_resting_entry_never_touched_times_out() {
    let n = 40;
    // High never reaches 100.50.
    let b = flat_bars(n, 99.0, 100.1);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = short_cfg();
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = 3;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 0);
    assert_eq!(res.entry_orders_timedout, 1);
}

#[test]
fn short_resting_exit_rests_below_the_open() {
    let n = 40;
    let mut b = flat_bars(n, 99.5, 101.0);
    b.low[17] = 98.8; // only bar 17 reaches a 99.00 buy-to-cover
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = short_cfg();
    cfg.roi_enabled = false;
    cfg.exit_limit_offset = 0.01; // cover at 99.00
    cfg.exit_fill_timeout_bars = 5;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    assert_eq!(res.backtest_result.exit_indices[0], 17);
    let expected_exit = 99.0 * (1.0 + 0.0002);
    assert!((res.backtest_result.exit_prices[0] - expected_exit).abs() < 1e-3,
            "exit {} != {expected_exit}", res.backtest_result.exit_prices[0]);
    // A short covering 1% below its entry is clearly profitable.
    assert!(res.backtest_result.profits[0] > 0.0);
}

// =============================================================================
// LIMIT ORDERS — TIMEOUT BOUNDARIES
// =============================================================================

/// Entry signal on bar 9 => order placed on bar 10. `timeout_bars = k` must
/// keep it live through bar 10+k and kill it on 10+k+1.
fn entry_fill_bar_with_timeout(timeout: usize, touch_bar: usize) -> Option<i32> {
    let n = 40;
    let mut b = flat_bars(n, 99.9, 101.0);
    b.low[touch_bar] = 99.0;
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[35]);

    let mut cfg = limit_cfg();
    cfg.roi_enabled = false;
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = timeout;

    let res = run(&b, &entries, &exits, &cfg);
    res.backtest_result.entry_indices.first().copied()
}

#[test]
fn entry_timeout_boundary_is_exact() {
    // Placed on bar 10; timeout 2 => live on bars 10, 11 and 12.
    assert_eq!(entry_fill_bar_with_timeout(2, 12), Some(12), "last live bar must still fill");
    assert_eq!(entry_fill_bar_with_timeout(2, 13), None, "one bar later must be dead");
}

#[test]
fn zero_timeout_is_fill_or_kill() {
    // Reachable only on the bar AFTER placement -> already cancelled.
    assert_eq!(entry_fill_bar_with_timeout(0, 11), None);
    // Reachable on the placement bar itself -> fills.
    assert_eq!(entry_fill_bar_with_timeout(0, 10), Some(10));
}

#[test]
fn a_cancelled_entry_does_not_block_a_later_signal() {
    let n = 60;
    let mut b = flat_bars(n, 99.9, 101.0);
    b.low[31] = 99.0; // only the SECOND signal's order is reachable
    let entries = sig_at(n, &[9, 30]);
    let exits = sig_at(n, &[40]);

    let mut cfg = limit_cfg();
    cfg.roi_enabled = false;
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = 2;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.entry_orders_timedout, 1, "the first order expires unfilled");
    assert_eq!(res.total_trades(), 1, "the second signal still trades");
    assert_eq!(res.backtest_result.entry_indices[0], 31);
}

#[test]
fn a_resting_entry_blocks_a_second_order_while_it_lives() {
    let n = 60;
    let b = flat_bars(n, 99.9, 101.0); // the 99.50 limit is never reachable
    // A signal on every bar of 9..20 would stack up orders if the engine
    // forgot it already has one on the book.
    let entries = sig_at(n, &(9..20).collect::<Vec<_>>());
    let exits = vec![0u8; n];

    let mut cfg = limit_cfg();
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = 100; // stays live for the whole run

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.entry_orders_placed, 1, "only one entry order may be live at a time");
    assert_eq!(res.total_trades(), 0);
}

// =============================================================================
// LIMIT ORDERS — FEES
// =============================================================================

#[test]
fn a_maker_fill_beats_a_taker_fill_on_identical_prices() {
    let n = 40;
    let b = flat_bars(n, 99.0, 101.0);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    // Same fills either way (the 99.50 limit is inside every candle); only
    // the fee the entry pays differs.
    let mut maker = limit_cfg();
    maker.roi_enabled = false;
    maker.entry_limit_offset = 0.005;
    maker.fee_maker = 0.0001;
    let mut taker = maker.clone();
    taker.fee_maker = taker.fee_taker; // no maker discount

    let m = run(&b, &entries, &exits, &maker);
    let t = run(&b, &entries, &exits, &taker);
    assert_eq!(m.total_trades(), 1);
    assert_eq!(t.total_trades(), 1);
    assert_eq!(m.backtest_result.entry_indices, t.backtest_result.entry_indices);
    assert_eq!(m.backtest_result.exit_indices, t.backtest_result.exit_indices);
    assert!(m.backtest_result.profits[0] > t.backtest_result.profits[0],
            "maker {} should beat taker {}",
            m.backtest_result.profits[0], t.backtest_result.profits[0]);
}

#[test]
fn dollar_fee_mode_charges_the_maker_rate_on_a_resting_side() {
    let n = 40;
    let b = flat_bars(n, 99.0, 101.0);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);

    let mut cfg = limit_cfg();
    cfg.roi_enabled = false;
    cfg.fee_mode = "dollar".into();
    cfg.fee_dollars_taker_per_contract = 2.0;
    cfg.fee_dollars_maker_per_contract = 0.5;
    cfg.entry_limit_offset = 0.005; // maker entry, taker exit
    cfg.max_trade_amount = 1000.0;

    let mut all_taker = cfg.clone();
    all_taker.fee_dollars_maker_per_contract = 2.0;

    let cheap = run(&b, &entries, &exits, &cfg);
    let dear = run(&b, &entries, &exits, &all_taker);
    assert_eq!(cheap.total_trades(), 1);
    assert!(cheap.backtest_result.pnl_amounts[0] > dear.backtest_result.pnl_amounts[0],
            "a maker entry must cost less in dollar-fee mode: {} vs {}",
            cheap.backtest_result.pnl_amounts[0], dear.backtest_result.pnl_amounts[0]);
}

// =============================================================================
// LIMIT ORDERS — PROTECTIVE EXITS ARE UNAFFECTED
// =============================================================================

#[test]
fn an_roi_exit_ignores_the_exit_offset() {
    let n = 40;
    let mut b = flat_bars(n, 99.5, 100.5);
    b.high[13] = 106.0; // +6% -> hits the default 6% ROI target
    let entries = sig_at(n, &[9]);
    let exits = vec![0u8; n];

    let mut cfg = limit_cfg();
    cfg.exit_limit_offset = 0.05; // would rest an exit at ~105 if it applied
    cfg.exit_fill_timeout_bars = 0;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    assert_eq!(res.backtest_result.exit_reasons[0], crate::backtest::ExitReason::RoiTarget);
    assert_eq!(res.backtest_result.exit_indices[0], 13);
    assert_eq!(res.exit_orders_placed, 0, "an ROI exit is never a resting order");
}

#[test]
fn a_trailing_stop_exit_ignores_the_exit_offset() {
    let n = 40;
    let mut b = flat_bars(n, 99.9, 100.1);
    b.high[12] = 103.0; // arms the trail
    b.low[14] = 100.0;  // then falls back through it
    let entries = sig_at(n, &[9]);
    let exits = vec![0u8; n];

    let mut cfg = limit_cfg();
    cfg.roi_enabled = false;
    cfg.trailing_enabled = true;
    cfg.trailing_trigger = 0.02;
    cfg.trailing_offset = 0.01;
    cfg.exit_limit_offset = 0.05;

    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    assert_eq!(res.backtest_result.exit_reasons[0], crate::backtest::ExitReason::TrailingStop);
    assert_eq!(res.exit_orders_placed, 0, "a trailing exit is never a resting order");
}

#[test]
fn an_exit_signal_that_persists_while_an_order_rests_reprices_it() {
    let n = 40;
    // Rising opens, so each bar's exit limit lands on a different price.
    let mut b = flat_bars(n, 99.0, 100.2);
    for i in 15..20 {
        b.open[i] = 100.0 + (i - 14) as f32;
        b.high[i] = b.open[i] + 0.1;
        b.low[i] = b.open[i] - 0.1;
    }
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14, 15, 16]); // exit signal on three consecutive bars

    let mut cfg = limit_cfg();
    cfg.roi_enabled = false;
    cfg.exit_limit_offset = 0.02; // 2% above each bar's open -> out of reach
    cfg.exit_fill_timeout_bars = 100;

    let res = run(&b, &entries, &exits, &cfg);
    // One order per distinct decided price: freqtrade's handle_similar_order
    // cancels-and-replaces the resting order whenever the new price differs.
    assert!(res.exit_orders_placed >= 3, "placed {}", res.exit_orders_placed);
    // The last of those (bar 17's open 103 -> 105.06) is the one left resting;
    // the market keeps rising and reaches it on bar 19, so THAT is the price
    // that fills — not any of the superseded ones, and not bar 17's open.
    assert_eq!(res.total_trades(), 1);
    assert_eq!(res.backtest_result.exit_indices[0], 19);
    let expected_exit = 105.06 * (1.0 - 0.0002);
    assert!((res.backtest_result.exit_prices[0] - expected_exit).abs() < 1e-2,
            "exit {} != {expected_exit}", res.backtest_result.exit_prices[0]);
}

// =============================================================================
// LIMIT ORDERS — JOINT (BOTH-DIRECTION) LOOP
// =============================================================================

fn run_joint(
    b: &Bars, le: &[u8], lx: &[u8], se: &[u8], sx: &[u8], cfg: &UnifiedBacktestConfig,
) -> TradingEngineResult {
    let empty = vec![0.0f32; b.close.len()];
    unified_backtest_both(
        &b.close, &b.open, &b.high, &b.low, &b.volume,
        &empty, &empty, &empty, &empty, &empty,
        le, lx, se, sx, cfg,
    )
}

#[test]
fn joint_loop_rests_a_short_entry_and_fills_it_later() {
    let n = 40;
    let mut b = flat_bars(n, 99.0, 100.1);
    b.high[14] = 100.9; // first bar that reaches a 100.50 short entry
    let zeros = vec![0u8; n];
    let short_entries = sig_at(n, &[9]);
    let short_exits = sig_at(n, &[25]);

    let mut cfg = limit_cfg();
    cfg.roi_enabled = false;
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = 20;

    let res = run_joint(&b, &zeros, &zeros, &short_entries, &short_exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    assert_eq!(res.backtest_result.entry_indices[0], 14, "filled on the touch bar, not the signal bar");
    assert_eq!(res.entry_orders_placed, 1);
}

#[test]
fn joint_loop_keeps_one_order_across_both_directions() {
    let n = 40;
    let b = flat_bars(n, 99.9, 100.1); // nothing reaches either 0.5% limit
    let long_entries = sig_at(n, &(9..15).collect::<Vec<_>>());
    let short_entries = sig_at(n, &(15..25).collect::<Vec<_>>());
    let zeros = vec![0u8; n];

    let mut cfg = limit_cfg();
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = 100;

    let res = run_joint(&b, &long_entries, &zeros, &short_entries, &zeros, &cfg);
    assert_eq!(res.entry_orders_placed, 1,
               "a live order on one side must block the other side too");
    assert_eq!(res.total_trades(), 0);
}

#[test]
fn joint_same_bar_reversal_can_rest_instead_of_filling() {
    let n = 40;
    let mut b = flat_bars(n, 99.95, 100.05);
    b.low[12] = 99.0; // long entry fills here (99.50 limit)
    b.high[16] = 101.0; // reversal short (100.50) only becomes reachable here
    let long_entries = sig_at(n, &[9]);
    let long_exits = sig_at(n, &[13]);
    let short_entries = sig_at(n, &[13, 14, 15]);
    let short_exits = sig_at(n, &[25]); // so the reversal trade actually closes
    let zeros = vec![0u8; n];
    let _ = &zeros;

    let mut cfg = limit_cfg();
    cfg.roi_enabled = false;
    cfg.entry_limit_offset = 0.005;
    cfg.entry_fill_timeout_bars = 100;

    let res = run_joint(&b, &long_entries, &long_exits, &short_entries, &short_exits, &cfg);
    assert_eq!(res.total_trades(), 2, "long, then the reversal short: {:?}",
               res.backtest_result.entry_indices);
    // The reversal order is placed on the long's exit bar (14) but only fills
    // once the market actually reaches 100.50, on bar 16.
    assert_eq!(res.backtest_result.entry_indices[1], 16);
}

#[test]
fn joint_zero_offset_matches_market_orders() {
    let n = 60;
    let b = flat_bars(n, 99.0, 101.0);
    let long_entries = sig_at(n, &[9, 30]);
    let long_exits = sig_at(n, &[14, 35]);
    let short_entries = sig_at(n, &[20]);
    let short_exits = sig_at(n, &[24]);

    let mut market = limit_cfg();
    market.entry_order_type = "market".into();
    market.exit_order_type = "market".into();
    let limit0 = limit_cfg();

    let m = run_joint(&b, &long_entries, &long_exits, &short_entries, &short_exits, &market);
    let l = run_joint(&b, &long_entries, &long_exits, &short_entries, &short_exits, &limit0);
    assert!(m.total_trades() >= 2);
    assert_eq!(l.backtest_result.entry_indices, m.backtest_result.entry_indices);
    assert_eq!(l.backtest_result.exit_indices, m.backtest_result.exit_indices);
    assert_eq!(l.backtest_result.profits, m.backtest_result.profits);
}

// =============================================================================
// LIMIT ORDERS — SWEEP FAST PATH STAYS IN SYNC
// =============================================================================

/// Strategy shim that replays fixed signal arrays, so the full-trade engine
/// and the scalar sweep path can be fed identical inputs.
struct FixedSignals {
    cfg: StrategyConfig,
    entries: Vec<u8>,
    exits: Vec<u8>,
}

impl super::super::backtest::Strategy for FixedSignals {
    fn config(&self) -> &StrategyConfig { &self.cfg }
    fn calculate_custom_indicators(
        &self, _c: &[f32], _o: &[f32], _h: &[f32], _l: &[f32], _v: &[f32],
    ) -> std::collections::HashMap<usize, Vec<f32>> {
        std::collections::HashMap::new()
    }
    fn populate_entry_trend(&self, ctx: &super::super::backtest::SignalContext) -> (Vec<u8>, Vec<u8>) {
        (self.entries.clone(), vec![0u8; ctx.n])
    }
    fn populate_exit_trend(
        &self, _ctx: &super::super::backtest::SignalContext, _dir: &str,
        _ei: &[usize], _ep: &[f32],
    ) -> Vec<u8> {
        self.exits.clone()
    }
}

/// Deterministic random walk — enough structure that resting orders sometimes
/// fill, sometimes expire.
fn walk_bars(n: usize, seed: u64) -> Bars {
    let mut state = seed;
    let mut rand = move || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((state >> 40) as f32 / 8_388_608.0) - 1.0 // ~[-1, 1)
    };
    let (mut open, mut high, mut low, mut close) =
        (Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n));
    let mut px = 100.0f32;
    for _ in 0..n {
        let o = px;
        let c = (o * (1.0 + rand() * 0.01)).max(1.0);
        let h = o.max(c) * (1.0 + rand().abs() * 0.006);
        let l = o.min(c) * (1.0 - rand().abs() * 0.006);
        open.push(o); close.push(c); high.push(h); low.push(l);
        px = c;
    }
    Bars { open, high, low, close, volume: vec![1000.0; n] }
}

fn alternating(n: usize, period: usize, offset: usize) -> Vec<u8> {
    (0..n).map(|i| if i % period == offset { 1u8 } else { 0 }).collect()
}

#[test]
fn sweep_fast_path_agrees_with_the_full_engine_under_limit_orders() {
    let n = 400;
    let b = walk_bars(n, 0xC0FFEE);
    let entries = alternating(n, 17, 3);
    let exits = alternating(n, 17, 11);

    let mut scfg = StrategyConfig::new("15m");
    scfg.startup_candle_count = 20;
    scfg.stoploss = -0.05;
    scfg.fee_taker = 0.0005;
    scfg.fee_maker = 0.0002;
    scfg = scfg.with_limit_orders(0.002, 4);
    let strat = FixedSignals { cfg: scfg, entries, exits };

    // ROI off so the two paths' (deliberately different) ROI fill-price
    // formulas can't muddy the comparison — this test is about the ORDER
    // lifecycle being identical, which is the part that must stay in sync.
    let overlay = serde_json::json!({"roi_enabled": false, "compounding_enabled": false});

    let full = run_strategy_backtest(
        &strat, "T/USDC", &b.close, &b.open, &b.high, &b.low, &b.volume,
        "long", 10000.0, None, Some(&overlay),
    );

    let arc_close = std::sync::Arc::new(b.close.clone());
    let arc_open = std::sync::Arc::new(b.open.clone());
    let arc_high = std::sync::Arc::new(b.high.clone());
    let arc_low = std::sync::Arc::new(b.low.clone());
    let arc_vol = std::sync::Arc::new(b.volume.clone());
    let pre = std::sync::Arc::new(crate::fast_indicators::calculate_standard_indicators(
        &arc_close, &arc_high, &arc_low, &arc_vol,
    ));
    let sweep = run_strategy_backtest_summary(
        &strat, "T/USDC", &arc_close, &arc_open, &arc_high, &arc_low, &arc_vol, &pre,
        "long", 10000.0, None, Some(&overlay),
    );

    let full_trades = full.get("total_trades").and_then(|v| v.as_u64()).unwrap();
    let sweep_trades = sweep.get("total_trades").and_then(|v| v.as_u64()).unwrap();
    assert!(full_trades > 5, "test data should produce real trades, got {full_trades}");
    assert_eq!(sweep_trades, full_trades, "sweep path drifted from the full engine");

    let full_ret = full.get("total_return").and_then(|v| v.as_f64()).unwrap();
    let sweep_ret = sweep.get("total_return").and_then(|v| v.as_f64()).unwrap();
    assert!((full_ret - sweep_ret).abs() < 1e-4,
            "returns diverged: full {full_ret} vs sweep {sweep_ret}");
}

#[test]
fn param_sweep_runs_with_limit_orders_configured() {
    let n = 300;
    let b = walk_bars(n, 0xBEEF);
    let entries = alternating(n, 13, 2);
    let exits = alternating(n, 13, 8);

    let mut scfg = StrategyConfig::new("15m");
    scfg.startup_candle_count = 20;
    scfg = scfg.with_limit_orders(0.001, 3);
    let strat = FixedSignals { cfg: scfg, entries, exits };

    let arc_close = std::sync::Arc::new(b.close.clone());
    let arc_open = std::sync::Arc::new(b.open.clone());
    let arc_high = std::sync::Arc::new(b.high.clone());
    let arc_low = std::sync::Arc::new(b.low.clone());
    let arc_vol = std::sync::Arc::new(b.volume.clone());
    let pre = std::sync::Arc::new(crate::fast_indicators::calculate_standard_indicators(
        &arc_close, &arc_high, &arc_low, &arc_vol,
    ));

    let out = run_param_sweep(
        &strat, "T/USDC", &arc_close, &arc_open, &arc_high, &arc_low, &arc_vol, &pre,
        "long", 10000.0, None, None,
    );
    assert!(out.get("sweep_combos_tested").and_then(|v| v.as_u64()).unwrap_or(0) > 0);
    assert!(out.get("total_trades").is_some());
}

#[test]
fn strategy_config_limit_orders_reach_the_engine() {
    let n = 300;
    let b = walk_bars(n, 0x1234);
    let entries = alternating(n, 23, 5);
    let exits = alternating(n, 23, 15);

    let mut scfg = StrategyConfig::new("15m");
    scfg.startup_candle_count = 20;
    // A 5% resting offset almost never gets touched, so the engine should be
    // placing orders and timing them out rather than trading.
    scfg = scfg.with_limit_orders(0.05, 2);
    let strat = FixedSignals { cfg: scfg, entries: entries.clone(), exits: exits.clone() };

    let out = run_strategy_backtest(
        &strat, "T/USDC", &b.close, &b.open, &b.high, &b.low, &b.volume,
        "long", 10000.0, None, None,
    );
    let placed = out.get("entry_orders_placed").and_then(|v| v.as_u64()).unwrap_or(0);
    let timedout = out.get("timedout_entry_orders").and_then(|v| v.as_u64()).unwrap_or(0);
    assert!(placed > 0, "StrategyConfig::with_limit_orders never reached the loop");
    assert!(timedout > 0, "a 5% resting offset should mostly expire unfilled");

    // Same signals with market orders trade far more often.
    let mut market_cfg = StrategyConfig::new("15m");
    market_cfg.startup_candle_count = 20;
    market_cfg.entry_order = crate::backtest::OrderSimConfig {
        order_type: crate::backtest::OrderFillType::Market, offset: 0.0, timeout_bars: 0,
    };
    market_cfg.exit_order = market_cfg.entry_order;
    let market_strat = FixedSignals { cfg: market_cfg, entries, exits };
    let market_out = run_strategy_backtest(
        &market_strat, "T/USDC", &b.close, &b.open, &b.high, &b.low, &b.volume,
        "long", 10000.0, None, None,
    );
    let market_trades = market_out.get("total_trades").and_then(|v| v.as_u64()).unwrap();
    let limit_trades = out.get("total_trades").and_then(|v| v.as_u64()).unwrap();
    assert!(market_trades > limit_trades,
            "market {market_trades} should out-trade a 5%-away limit {limit_trades}");
}

// =============================================================================
// LIMIT ORDERS — INVARIANTS OVER A RANDOM WALK
// =============================================================================

/// Every recorded fill must lie inside its own candle. Fees are zeroed so the
/// recorded (fee-adjusted) prices ARE the raw fill prices.
fn assert_fills_inside_their_candles(b: &Bars, res: &TradingEngineResult, label: &str) {
    let br = &res.backtest_result;
    for k in 0..br.profits.len() {
        let ei = br.entry_indices[k] as usize;
        let xi = br.exit_indices[k] as usize;
        assert!(xi >= ei, "{label}: exit {xi} before entry {ei}");
        assert_eq!(br.durations[k], (xi - ei) as i32, "{label}: duration mismatch");
        let ep = br.entry_prices[k];
        let xp = br.exit_prices[k];
        assert!(ep >= b.low[ei] - 1e-3 && ep <= b.high[ei] + 1e-3,
                "{label}: entry {ep} outside candle {ei} [{}, {}]", b.low[ei], b.high[ei]);
        assert!(xp >= b.low[xi] - 1e-3 && xp <= b.high[xi] + 1e-3,
                "{label}: exit {xp} outside candle {xi} [{}, {}]", b.low[xi], b.high[xi]);
        assert!(br.profits[k].is_finite(), "{label}: non-finite profit");
    }
}

#[test]
fn every_fill_lands_inside_its_own_candle() {
    for seed in [1u64, 7, 42, 1337, 90210] {
        let n = 500;
        let b = walk_bars(n, seed);
        let entries = alternating(n, 11, 1);
        let exits = alternating(n, 11, 6);

        for (label, offset, timeout) in [
            ("market-equivalent", 0.0, 0usize),
            ("tight-rest", 0.001, 3),
            ("wide-rest", 0.01, 8),
        ] {
            let mut cfg = limit_cfg();
            cfg.startup_candle_count = 20;
            cfg.fee_taker = 0.0;
            cfg.fee_maker = 0.0;
            cfg.base_stoploss = -0.04;
            cfg.entry_limit_offset = offset;
            cfg.exit_limit_offset = offset;
            cfg.entry_fill_timeout_bars = timeout;
            cfg.exit_fill_timeout_bars = timeout;

            let long = run(&b, &entries, &exits, &cfg);
            assert_fills_inside_their_candles(&b, &long, &format!("{label}/long/seed{seed}"));

            let mut short = cfg.clone();
            short.direction = "short".into();
            let s = run(&b, &entries, &exits, &short);
            assert_fills_inside_their_candles(&b, &s, &format!("{label}/short/seed{seed}"));

            let j = run_joint(&b, &entries, &exits, &exits, &entries, &cfg);
            assert_fills_inside_their_candles(&b, &j, &format!("{label}/joint/seed{seed}"));
        }
    }
}

#[test]
fn a_wider_resting_offset_never_increases_the_fill_count() {
    let n = 500;
    let b = walk_bars(n, 0xABCDEF);
    let entries = alternating(n, 9, 1);
    let exits = alternating(n, 9, 5);

    let mut last = usize::MAX;
    for offset in [0.0f32, 0.001, 0.004, 0.02, 0.10] {
        let mut cfg = limit_cfg();
        cfg.startup_candle_count = 20;
        cfg.roi_enabled = false;
        cfg.entry_limit_offset = offset;
        cfg.entry_fill_timeout_bars = 2;
        let res = run(&b, &entries, &exits, &cfg);
        let filled = res.total_trades();
        assert!(filled <= last,
                "offset {offset} filled {filled} > previous {last}: resting further away \
                 can never make an order EASIER to hit");
        // Every placed order either filled or timed out. `filled` counts
        // CLOSED trades, so a position still open when the data runs out
        // accounts for exactly one extra fill.
        let accounted = filled + res.entry_orders_timedout;
        assert!(res.entry_orders_placed == accounted || res.entry_orders_placed == accounted + 1,
                "offset {offset}: placed {} but only {accounted} accounted for",
                res.entry_orders_placed);
        last = filled;
    }
    assert!(last < 5, "a 10% resting offset should almost never fill, got {last}");
}

#[test]
fn order_counters_balance_on_both_sides() {
    let n = 400;
    let b = walk_bars(n, 0x5EED);
    let entries = alternating(n, 12, 2);
    let exits = alternating(n, 12, 7);

    let mut cfg = limit_cfg();
    cfg.startup_candle_count = 20;
    cfg.roi_enabled = false;
    cfg.entry_limit_offset = 0.003;
    cfg.exit_limit_offset = 0.003;
    cfg.entry_fill_timeout_bars = 4;
    cfg.exit_fill_timeout_bars = 4;

    let res = run(&b, &entries, &exits, &cfg);
    assert!(res.entry_orders_placed > 0 && res.exit_orders_placed > 0);
    // Placed = closed trades + timeouts (+1 if a position is still open at
    // the end of the data, which this engine simply drops).
    let accounted = res.total_trades() + res.entry_orders_timedout;
    assert!(res.entry_orders_placed == accounted || res.entry_orders_placed == accounted + 1,
            "every entry order either fills or times out: placed {} vs {accounted}",
            res.entry_orders_placed);
    assert!(res.exit_orders_timedout <= res.exit_orders_placed);
}

#[test]
fn overlay_unfilledtimeout_accepts_the_seconds_unit() {
    let mut ub = UnifiedBacktestConfig::default();
    ub.timeframe_minutes = 5;
    apply_overlay_to_ub_cfg(&mut ub, &serde_json::json!({
        "unfilledtimeout": {"entry": 1800, "exit": 120, "unit": "seconds"},
    }));
    assert_eq!(ub.entry_fill_timeout_bars, 6); // 1800s = 30m / 5m
    assert_eq!(ub.exit_fill_timeout_bars, 0);  // 120s = 2m -> sub-bar, fill-or-kill
}

#[test]
fn overlay_limit_order_offset_sets_both_sides() {
    let mut ub = UnifiedBacktestConfig::default();
    apply_overlay_to_ub_cfg(&mut ub, &serde_json::json!({"limit_order_offset": 0.0025}));
    assert!((ub.entry_limit_offset - 0.0025).abs() < 1e-9);
    assert!((ub.exit_limit_offset - 0.0025).abs() < 1e-9);
    assert!(ub.to_entry_order().is_maker());
    assert!(ub.to_exit_order().can_rest());
}

#[test]
fn market_order_type_never_rests() {
    let mut ub = UnifiedBacktestConfig::default();
    ub.entry_order_type = "market".into();
    ub.entry_limit_offset = 0.05; // ignored: a market order has no resting price
    let entry = ub.to_entry_order();
    assert!(!entry.can_rest());
    assert!(!entry.is_maker());
    assert_eq!(entry.entry_price(100.0, true), 100.0);

    let n = 40;
    let b = flat_bars(n, 99.0, 101.0);
    let entries = sig_at(n, &[9]);
    let exits = sig_at(n, &[14]);
    let mut cfg = limit_cfg();
    cfg.entry_order_type = "market".into();
    cfg.entry_limit_offset = 0.05;
    let res = run(&b, &entries, &exits, &cfg);
    assert_eq!(res.total_trades(), 1);
    let expected = 100.0 * (1.0 + 0.0005); // filled at the open, taker fee
    assert!((res.backtest_result.entry_prices[0] - expected).abs() < 1e-3,
            "entry {} != {expected}", res.backtest_result.entry_prices[0]);
}

#[test]
fn sweep_fast_path_agrees_with_the_full_engine_on_market_orders_too() {
    // Regression guard for the entry-signal shift: before it was fixed, the
    // sweep path acted on entry signals a bar earlier than the full engine,
    // which silently made every swept parameter score describe a slightly
    // different (lookahead-flavoured) strategy than the backtest that
    // followed it.
    let n = 400;
    let b = walk_bars(n, 0x5A5A);
    let entries = alternating(n, 19, 4);
    let exits = alternating(n, 19, 12);

    let mut scfg = StrategyConfig::new("15m");
    scfg.startup_candle_count = 20;
    scfg.stoploss = -0.06;
    scfg.entry_order = crate::backtest::OrderSimConfig {
        order_type: crate::backtest::OrderFillType::Market, offset: 0.0, timeout_bars: 0,
    };
    scfg.exit_order = scfg.entry_order;
    let strat = FixedSignals { cfg: scfg, entries, exits };
    let overlay = serde_json::json!({"roi_enabled": false, "compounding_enabled": false});

    let full = run_strategy_backtest(
        &strat, "T/USDC", &b.close, &b.open, &b.high, &b.low, &b.volume,
        "long", 10000.0, None, Some(&overlay),
    );
    let arc_close = std::sync::Arc::new(b.close.clone());
    let arc_open = std::sync::Arc::new(b.open.clone());
    let arc_high = std::sync::Arc::new(b.high.clone());
    let arc_low = std::sync::Arc::new(b.low.clone());
    let arc_vol = std::sync::Arc::new(b.volume.clone());
    let pre = std::sync::Arc::new(crate::fast_indicators::calculate_standard_indicators(
        &arc_close, &arc_high, &arc_low, &arc_vol,
    ));
    let sweep = run_strategy_backtest_summary(
        &strat, "T/USDC", &arc_close, &arc_open, &arc_high, &arc_low, &arc_vol, &pre,
        "long", 10000.0, None, Some(&overlay),
    );

    let ft = full.get("total_trades").and_then(|v| v.as_u64()).unwrap();
    let st = sweep.get("total_trades").and_then(|v| v.as_u64()).unwrap();
    assert!(ft > 5, "expected real trades, got {ft}");
    assert_eq!(st, ft, "sweep vs full trade count");
    let fr = full.get("total_return").and_then(|v| v.as_f64()).unwrap();
    let sr = sweep.get("total_return").and_then(|v| v.as_f64()).unwrap();
    assert!((fr - sr).abs() < 1e-4, "sweep vs full return: {fr} vs {sr}");
}

// =============================================================================
// config_json CONTRACT (the PyO3 bridge deserialises straight into this type)
// =============================================================================

#[test]
fn config_json_round_trips_the_limit_order_fields() {
    let cfg: UnifiedBacktestConfig = serde_json::from_str(r#"{
        "timeframe": "5m",
        "entry_order_type": "limit",
        "exit_order_type": "market",
        "entry_limit_offset": 0.0015,
        "exit_limit_offset": 0.002,
        "entry_fill_timeout_bars": 6,
        "exit_fill_timeout_bars": 2,
        "fee_maker": 0.0001,
        "fee_taker": 0.0006
    }"#).expect("config_json must deserialise");

    let entry = cfg.to_entry_order();
    assert!(entry.order_type.is_limit());
    assert!(entry.can_rest() && entry.is_maker());
    assert_eq!(entry.timeout_bars, 6);
    assert!((entry.entry_price(100.0, true) - 99.85).abs() < 1e-4);
    assert!((entry.entry_price(100.0, false) - 100.15).abs() < 1e-4);

    let exit = cfg.to_exit_order();
    assert!(!exit.order_type.is_limit(), "market exits never rest");
    assert!(!exit.can_rest());
    assert_eq!(exit.exit_price(100.0, true), 100.0, "a market order prices at the open");
}

#[test]
fn config_json_omitting_limit_fields_keeps_the_legacy_behaviour() {
    // What every existing caller sends: no limit-order keys at all.
    let cfg: UnifiedBacktestConfig = serde_json::from_str(r#"{"timeframe": "15m"}"#).unwrap();
    let entry = cfg.to_entry_order();
    let exit = cfg.to_exit_order();
    assert!(!entry.can_rest() && !exit.can_rest(), "no offset -> nothing rests");
    assert!(!entry.is_maker() && !exit.is_maker(), "no offset -> taker fees");
    assert_eq!(entry.timeout_bars, usize::MAX, "no timeout unless configured");
    assert_eq!(entry.entry_price(100.0, true), 100.0);
    assert_eq!(exit.exit_price(100.0, true), 100.0);
}

#[test]
fn a_negative_offset_is_clamped_rather_than_inverting_the_order() {
    let mut cfg = UnifiedBacktestConfig::default();
    cfg.entry_limit_offset = -0.01; // would otherwise chase the market
    let entry = cfg.to_entry_order();
    assert_eq!(entry.offset, 0.0);
    assert_eq!(entry.entry_price(100.0, true), 100.0);
}

#[test]
fn empty_result_json_still_reports_the_order_counters() {
    let n = 300;
    let b = walk_bars(n, 0xFEED);
    let entries = alternating(n, 31, 7);
    let exits = alternating(n, 31, 20);

    let mut scfg = StrategyConfig::new("15m");
    scfg.startup_candle_count = 20;
    scfg = scfg.with_limit_orders(0.20, 1); // 20% away: never fills
    let strat = FixedSignals { cfg: scfg, entries, exits };

    let out = run_strategy_backtest(
        &strat, "T/USDC", &b.close, &b.open, &b.high, &b.low, &b.volume,
        "long", 10000.0, None, None,
    );
    assert_eq!(out.get("total_trades").and_then(|v| v.as_u64()), Some(0));
    assert!(out.get("entry_orders_placed").and_then(|v| v.as_u64()).unwrap_or(0) > 0,
            "a zero-trade run must still explain WHY: {out}");
    assert!(out.get("timedout_entry_orders").and_then(|v| v.as_u64()).unwrap_or(0) > 0);
}
