// =============================================================================
// METRICS — freqtrade/backtesting.py-parity performance statistics
// =============================================================================
//
// Ports the formulas in `VulcanTrader/data/metrics.py` (sharpe, sortino,
// calmar, sqn, expectancy, max-drawdown, win/loss streaks) so this engine's
// summary output lines up field-for-field with the Python `backtesting.py`
// engine's `generate_strategy_stats`.
//
// Real calendar dates aren't available at this layer (only bar indices), so
// `days_period` is derived from bar count * timeframe — the elapsed
// wall-clock span of the whole backtest. This mirrors how the Python side
// feeds the full min_date..max_date backtest range (not just the trade span)
// into these same formulas.
// =============================================================================

fn population_std(xs: &[f32]) -> f32 {
    let n = xs.len();
    if n == 0 { return f32::NAN; }
    let mean = xs.iter().sum::<f32>() / n as f32;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f32>() / n as f32;
    var.sqrt()
}

fn sample_std(xs: &[f32]) -> f32 {
    let n = xs.len();
    if n < 2 { return f32::NAN; }
    let mean = xs.iter().sum::<f32>() / n as f32;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f32>() / (n - 1) as f32;
    var.sqrt()
}

fn mean(xs: &[f32]) -> f32 {
    if xs.is_empty() { 0.0 } else { xs.iter().sum::<f32>() / xs.len() as f32 }
}

pub fn median(xs: &[f32]) -> f32 {
    if xs.is_empty() { return 0.0; }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

/// Mirrors `_calculate_annualized_ratio` — used by Sharpe/Sortino/Calmar.
/// Returns -100.0 (a deliberately bad score) when the denominator is zero or
/// NaN, same convention as the Python side.
fn annualized_ratio(expected_returns_mean: f32, denominator: f32, annualization_factor: f32) -> f32 {
    if denominator != 0.0 && !denominator.is_nan() {
        (expected_returns_mean / denominator) * annualization_factor.sqrt()
    } else {
        -100.0
    }
}

/// Elapsed wall-clock days spanned by the backtest (analog of Python's
/// `(max_date - min_date).days`, floored at 1 the same way).
pub fn days_period(n_bars: usize, timeframe_minutes: usize) -> f32 {
    ((n_bars as f32 * timeframe_minutes as f32) / 1440.0).max(1.0)
}

/// Port of `calculate_cagr` in data/metrics.py.
pub fn calculate_cagr(days_passed: f32, starting_balance: f32, final_balance: f32) -> f32 {
    if final_balance < 0.0 || starting_balance <= 0.0 || days_passed <= 0.0 {
        return 0.0;
    }
    (final_balance / starting_balance).powf(1.0 / (days_passed / 365.0)) - 1.0
}

/// Port of `calculate_expectancy` in data/metrics.py. Takes `profit_abs`
/// (dollar P&L per trade). Returns (expectancy, expectancy_ratio).
pub fn calculate_expectancy(profits_abs: &[f32]) -> (f32, f32) {
    if profits_abs.is_empty() {
        return (0.0, 100.0);
    }
    let wins: Vec<f32> = profits_abs.iter().copied().filter(|&p| p > 0.0).collect();
    let losses: Vec<f32> = profits_abs.iter().copied().filter(|&p| p < 0.0).collect();
    let profit_sum: f32 = wins.iter().sum();
    let loss_sum: f32 = losses.iter().map(|x| x.abs()).sum();
    let n = profits_abs.len() as f32;
    let avg_win = if !wins.is_empty() { profit_sum / wins.len() as f32 } else { 0.0 };
    let avg_loss = if !losses.is_empty() { loss_sum / losses.len() as f32 } else { 0.0 };
    let winrate = wins.len() as f32 / n;
    let loserate = losses.len() as f32 / n;
    let expectancy = winrate * avg_win - loserate * avg_loss;
    let expectancy_ratio = if avg_loss > 0.0 {
        ((1.0 + avg_win / avg_loss) * winrate) - 1.0
    } else {
        100.0
    };
    (expectancy, expectancy_ratio)
}

/// Port of `calculate_sharpe`. `profits_abs` is dollar P&L per trade.
pub fn calculate_sharpe(profits_abs: &[f32], days: f32, starting_balance: f32) -> f32 {
    if profits_abs.is_empty() || starting_balance <= 0.0 {
        return 0.0;
    }
    let ratios: Vec<f32> = profits_abs.iter().map(|p| p / starting_balance).collect();
    let expected_returns_mean = ratios.iter().sum::<f32>() / days;
    let up_stdev = population_std(&ratios);
    annualized_ratio(expected_returns_mean, up_stdev, 365.0)
}

/// Port of `calculate_sortino`.
pub fn calculate_sortino(profits_abs: &[f32], days: f32, starting_balance: f32) -> f32 {
    if profits_abs.is_empty() || starting_balance <= 0.0 {
        return 0.0;
    }
    let total: f32 = profits_abs.iter().map(|p| p / starting_balance).sum();
    let expected_returns_mean = total / days;
    let down: Vec<f32> = profits_abs.iter()
        .filter(|&&p| p < 0.0)
        .map(|p| p / starting_balance)
        .collect();
    // population_std([]) is NaN -> annualized_ratio falls back to -100.0,
    // matching numpy's np.std behavior on an empty slice.
    let down_stdev = population_std(&down);
    annualized_ratio(expected_returns_mean, down_stdev, 365.0)
}

/// Port of `calculate_sqn` (System Quality Number). Uses sample std (ddof=1)
/// like pandas' `.std()`, unlike Sharpe/Sortino which use population std.
pub fn calculate_sqn(profits_abs: &[f32], starting_balance: f32) -> f32 {
    if profits_abs.is_empty() || starting_balance <= 0.0 {
        return 0.0;
    }
    let ratios: Vec<f32> = profits_abs.iter().map(|p| p / starting_balance).collect();
    let n = ratios.len() as f32;
    let avg = mean(&ratios);
    let std = sample_std(&ratios);
    if std != 0.0 && !std.is_nan() {
        n.sqrt() * (avg / std)
    } else {
        -100.0
    }
}

/// Result of a max-drawdown scan, mirroring `DrawDownResult` in
/// data/metrics.py (dollar fields only — no wall-clock dates at this layer).
#[derive(Debug, Clone, Copy, Default)]
pub struct DrawdownResult {
    pub drawdown_abs: f32,
    pub high_value: f32,
    pub low_value: f32,
    pub relative_account_drawdown: f32,
}

/// Port of `calculate_max_drawdown`. `profits_abs`/`exit_bar` are parallel
/// per-trade arrays; trades are walked in exit-bar (chronological close)
/// order to build the cumulative equity curve, same as Python sorting by
/// `close_date`.
///
/// When `relative` is false (freqtrade's default for the headline
/// "max_drawdown_account" figure), the point of maximum ABSOLUTE dollar
/// drawdown is selected and its relative drawdown is reported alongside —
/// this is not necessarily the point of maximum relative drawdown. Pass
/// `relative: true` to select by relative drawdown instead (freqtrade's
/// separate "max_relative_drawdown").
pub fn calculate_max_drawdown(
    profits_abs: &[f32],
    exit_bar: &[i32],
    starting_balance: f32,
    relative: bool,
) -> Option<DrawdownResult> {
    if profits_abs.is_empty() {
        return None;
    }
    let mut order: Vec<usize> = (0..profits_abs.len()).collect();
    order.sort_by_key(|&i| exit_bar.get(i).copied().unwrap_or(0));

    let mut cumulative = 0.0f32;
    let mut high_value = 0.0f32;
    let mut best_abs = 0.0f32;
    let mut best_rel = 0.0f32;
    let mut best_high = 0.0f32;
    let mut best_low = 0.0f32;

    for &i in &order {
        cumulative += profits_abs[i];
        if cumulative > high_value {
            high_value = cumulative;
        }
        let dd_abs = high_value - cumulative;
        let max_balance = starting_balance + high_value;
        let cur_balance = starting_balance + cumulative;
        let dd_rel = if max_balance > 0.0 { (max_balance - cur_balance) / max_balance } else { 0.0 };

        let is_better = if relative { dd_rel > best_rel } else { dd_abs > best_abs };
        if is_better {
            best_abs = dd_abs;
            best_rel = dd_rel;
            best_high = high_value;
            best_low = cumulative;
        }
    }

    Some(DrawdownResult {
        drawdown_abs: best_abs,
        high_value: best_high,
        low_value: best_low,
        relative_account_drawdown: best_rel,
    })
}

/// Port of `calculate_calmar`.
pub fn calculate_calmar(profits_abs: &[f32], exit_bar: &[i32], days: f32, starting_balance: f32) -> f32 {
    if profits_abs.is_empty() || starting_balance <= 0.0 {
        return 0.0;
    }
    let total_profit: f32 = profits_abs.iter().sum::<f32>() / starting_balance;
    let expected_returns_mean = total_profit / days * 100.0;
    let max_drawdown = match calculate_max_drawdown(profits_abs, exit_bar, starting_balance, false) {
        Some(d) => d.relative_account_drawdown,
        None => return 0.0,
    };
    annualized_ratio(expected_returns_mean, max_drawdown, 365.0)
}

/// Port of `calc_streak`. A "loss" bucket includes draws (profit <= 0), same
/// as `np.where(profit_ratio > 0, "win", "loss")` on the Python side.
/// `profits_ratio_sorted` must already be in chronological (exit-order).
pub fn calc_streak(profits_ratio_sorted: &[f32]) -> (usize, usize) {
    let mut max_win = 0usize;
    let mut max_loss = 0usize;
    let mut cur_win = 0usize;
    let mut cur_loss = 0usize;
    for &p in profits_ratio_sorted {
        if p > 0.0 {
            cur_win += 1;
            cur_loss = 0;
            max_win = max_win.max(cur_win);
        } else {
            cur_loss += 1;
            cur_win = 0;
            max_loss = max_loss.max(cur_loss);
        }
    }
    (max_win, max_loss)
}

/// Aggregated freqtrade-parity metrics for a single trade list. See module
/// docs for the mapping to `generate_strategy_stats` field names.
#[derive(Debug, Clone, Default)]
pub struct ExtendedMetrics {
    pub sharpe: f32,
    pub sortino: f32,
    pub calmar: f32,
    pub sqn: f32,
    pub expectancy: f32,
    pub expectancy_ratio: f32,
    pub cagr: f32,
    pub profit_mean: f32,
    pub profit_median: f32,
    pub max_drawdown_abs: f32,
    pub max_drawdown_account: f32,
    pub max_drawdown_high: f32,
    pub max_drawdown_low: f32,
    pub max_relative_drawdown: f32,
    pub max_consecutive_wins: usize,
    pub max_consecutive_losses: usize,
    pub holding_avg_minutes: f32,
    pub winner_holding_avg_minutes: f32,
    pub loser_holding_avg_minutes: f32,
    pub backtest_days: f32,
}

/// Computes the full freqtrade-parity metric set for one trade list.
///
/// - `profits_ratio`: per-trade leveraged profit as a fraction (e.g. 0.02 = +2%)
/// - `pnl_amounts`: per-trade dollar P&L (freqtrade's `profit_abs`)
/// - `exit_indices` / `durations_bars`: parallel per-trade arrays (bar index, bar count)
pub fn compute_extended_metrics(
    profits_ratio: &[f32],
    pnl_amounts: &[f32],
    exit_indices: &[i32],
    durations_bars: &[i32],
    starting_balance: f32,
    final_equity: f32,
    n_bars: usize,
    timeframe_minutes: usize,
) -> ExtendedMetrics {
    let days = days_period(n_bars, timeframe_minutes);

    if profits_ratio.is_empty() {
        return ExtendedMetrics {
            expectancy_ratio: 100.0,
            backtest_days: days,
            ..Default::default()
        };
    }

    let mut order: Vec<usize> = (0..profits_ratio.len()).collect();
    order.sort_by_key(|&i| exit_indices.get(i).copied().unwrap_or(0));
    let profits_sorted: Vec<f32> = order.iter().map(|&i| profits_ratio[i]).collect();

    let (expectancy, expectancy_ratio) = calculate_expectancy(pnl_amounts);
    let dd_abs = calculate_max_drawdown(pnl_amounts, exit_indices, starting_balance, false).unwrap_or_default();
    let dd_rel = calculate_max_drawdown(pnl_amounts, exit_indices, starting_balance, true).unwrap_or_default();
    let (max_win, max_loss) = calc_streak(&profits_sorted);

    let tfm = timeframe_minutes as f32;
    let all_durations: Vec<f32> = durations_bars.iter().map(|&d| d as f32).collect();
    let winner_durations: Vec<f32> = order.iter()
        .filter(|&&i| profits_ratio[i] > 0.0)
        .map(|&i| durations_bars.get(i).copied().unwrap_or(0) as f32)
        .collect();
    let loser_durations: Vec<f32> = order.iter()
        .filter(|&&i| profits_ratio[i] < 0.0)
        .map(|&i| durations_bars.get(i).copied().unwrap_or(0) as f32)
        .collect();

    ExtendedMetrics {
        sharpe: calculate_sharpe(pnl_amounts, days, starting_balance),
        sortino: calculate_sortino(pnl_amounts, days, starting_balance),
        calmar: calculate_calmar(pnl_amounts, exit_indices, days, starting_balance),
        sqn: calculate_sqn(pnl_amounts, starting_balance),
        expectancy,
        expectancy_ratio,
        cagr: calculate_cagr(days, starting_balance, final_equity),
        profit_mean: mean(profits_ratio),
        profit_median: median(profits_ratio),
        max_drawdown_abs: dd_abs.drawdown_abs,
        max_drawdown_account: dd_abs.relative_account_drawdown,
        max_drawdown_high: dd_abs.high_value,
        max_drawdown_low: dd_abs.low_value,
        max_relative_drawdown: dd_rel.relative_account_drawdown,
        max_consecutive_wins: max_win,
        max_consecutive_losses: max_loss,
        holding_avg_minutes: mean(&all_durations) * tfm,
        winner_holding_avg_minutes: mean(&winner_durations) * tfm,
        loser_holding_avg_minutes: mean(&loser_durations) * tfm,
        backtest_days: days,
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cagr_matches_python_formula() {
        // (11000/10000)^(1/(365/365)) - 1 == 0.10
        let cagr = calculate_cagr(365.0, 10000.0, 11000.0);
        assert!((cagr - 0.10).abs() < 1e-4);
    }

    #[test]
    fn cagr_zero_on_invalid_inputs() {
        assert_eq!(calculate_cagr(0.0, 10000.0, 11000.0), 0.0);
        assert_eq!(calculate_cagr(365.0, 0.0, 11000.0), 0.0);
        assert_eq!(calculate_cagr(365.0, 10000.0, -1.0), 0.0);
    }

    #[test]
    fn expectancy_all_wins_has_zero_loserate() {
        let (exp, ratio) = calculate_expectancy(&[10.0, 20.0, 30.0]);
        assert!((exp - 20.0).abs() < 1e-4); // avg win, no losses to subtract
        assert_eq!(ratio, 100.0); // avg_loss == 0 -> ratio defined as 100.0
    }

    #[test]
    fn expectancy_matches_hand_calc() {
        // 2 wins (avg 10), 1 loss (avg -20), winrate=2/3, loserate=1/3
        let (exp, ratio) = calculate_expectancy(&[10.0, 10.0, -20.0]);
        let expected_exp = (2.0 / 3.0) * 10.0 - (1.0 / 3.0) * 20.0;
        assert!((exp - expected_exp).abs() < 1e-3);
        let rr = 10.0 / 20.0;
        let expected_ratio = ((1.0 + rr) * (2.0 / 3.0)) - 1.0;
        assert!((ratio - expected_ratio).abs() < 1e-3);
    }

    #[test]
    fn sharpe_zero_variance_is_penalty_value() {
        let s = calculate_sharpe(&[100.0, 100.0, 100.0], 30.0, 10000.0);
        assert_eq!(s, -100.0);
    }

    #[test]
    fn sortino_no_losers_is_penalty_value() {
        // Empty downside slice -> population_std(&[]) is NaN -> -100.0.
        let s = calculate_sortino(&[10.0, 20.0, 30.0], 30.0, 10000.0);
        assert_eq!(s, -100.0);
    }

    #[test]
    fn sqn_zero_std_is_penalty_value() {
        let s = calculate_sqn(&[50.0, 50.0, 50.0], 10000.0);
        assert_eq!(s, -100.0);
    }

    #[test]
    fn max_drawdown_all_wins_is_zero() {
        let profits = [10.0, 20.0, 30.0];
        let exits = [1, 2, 3];
        let dd = calculate_max_drawdown(&profits, &exits, 1000.0, false).unwrap();
        assert_eq!(dd.drawdown_abs, 0.0);
        assert_eq!(dd.relative_account_drawdown, 0.0);
    }

    #[test]
    fn max_drawdown_picks_biggest_dollar_dip() {
        // Peak at +100 (cum 100), then drops to cum -50 (a 150 dollar dip from peak).
        let profits = [100.0, -150.0, 10.0];
        let exits = [1, 2, 3];
        let dd = calculate_max_drawdown(&profits, &exits, 1000.0, false).unwrap();
        assert!((dd.drawdown_abs - 150.0).abs() < 1e-3);
        assert!((dd.high_value - 100.0).abs() < 1e-3);
        assert!((dd.low_value - (-50.0)).abs() < 1e-3);
    }

    #[test]
    fn calc_streak_basic() {
        // win win loss win loss loss loss -> max_win=2, max_loss=3
        let profits = [1.0, 1.0, -1.0, 1.0, -1.0, -1.0, -1.0];
        let (w, l) = calc_streak(&profits);
        assert_eq!(w, 2);
        assert_eq!(l, 3);
    }

    #[test]
    fn calc_streak_draw_counts_as_loss() {
        let profits = [1.0, 0.0, 1.0];
        let (w, l) = calc_streak(&profits);
        assert_eq!(w, 1);
        assert_eq!(l, 1);
    }

    #[test]
    fn empty_extended_metrics_dont_panic() {
        let m = compute_extended_metrics(&[], &[], &[], &[], 10000.0, 10000.0, 1000, 15);
        assert_eq!(m.sharpe, 0.0);
        assert_eq!(m.expectancy_ratio, 100.0);
    }
}
