// =============================================================================
// vulcan_rust_indicators — PyO3 bridge into this crate's own indicators/engine
// =============================================================================
//
// Compiled only under the `python` / `extension-module` features (what maturin
// builds with). Exposes `fast_indicators::calculate_standard_indicators` and the
// `cpu_engine` simulator to Python.
//
// PERFORMANCE — this bridge is marshalling-bound, not compute-bound.
// Measured on a 20,000-bar pair before the current design:
//     calculate_standard_indicators   24.4 ms   (built 23 x 20k Python floats)
//     list -> numpy on the caller      4.6 ms
//     run_backtest (one direction)    18.2 ms   (12 arrays extracted per-element)
// The engine work inside those calls is a small fraction of it. So, per
// performance.txt's "borrow the signal arrays, never clone them":
//
//   * inputs are borrowed zero-copy as `PyReadonlyArray1` instead of being
//     extracted element-by-element into `Vec<f64>`;
//   * outputs are returned as numpy arrays instead of Python lists, which
//     removes ~460k PyFloat allocations per call AND the caller's np.array()
//     round-trip.
//
// The indicator MATH is untouched — `calc_std` is called exactly as before and
// returns identical values; only how the data crosses the FFI boundary changed.

use std::sync::Arc;

use numpy::{IntoPyArray, PyReadonlyArray1};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::fast_indicators::calculate_standard_indicators as calc_std;

/// Borrow a numpy f64 array and down-cast to the f32 the engine works in.
/// One tight pass over a contiguous slice, versus PyO3's per-element extraction.
fn f32_from(a: &PyReadonlyArray1<'_, f64>) -> Vec<f32> {
    match a.as_slice() {
        Ok(s) => s.iter().map(|&x| x as f32).collect(),
        // Non-contiguous input (rare): walk the strided view instead.
        Err(_) => a.as_array().iter().map(|&x| x as f32).collect(),
    }
}

fn u8_from(a: &PyReadonlyArray1<'_, u8>) -> Vec<u8> {
    match a.as_slice() {
        Ok(s) => s.to_vec(),
        Err(_) => a.as_array().iter().copied().collect(),
    }
}

/// Compute the 23 standard indicators for the given OHLCV arrays.
///
/// Inputs are float64 numpy arrays (borrowed, not copied). Returns
/// `{index: numpy.ndarray(float32)}` matching `fast_indicators::
/// calculate_standard_indicators`' fixed-index map (see the table in
/// AllIndicatorsDemoStrategy). Warmup positions are NaN, exactly as the engine
/// produces them.
#[pyfunction]
fn calculate_standard_indicators<'py>(
    py: Python<'py>,
    close: PyReadonlyArray1<'py, f64>,
    high: PyReadonlyArray1<'py, f64>,
    low: PyReadonlyArray1<'py, f64>,
    volume: PyReadonlyArray1<'py, f64>,
) -> PyResult<Bound<'py, PyDict>> {
    let c = Arc::new(f32_from(&close));
    let h = Arc::new(f32_from(&high));
    let l = Arc::new(f32_from(&low));
    let v = Arc::new(f32_from(&volume));

    // Release the GIL for the (rayon-parallel) number crunching.
    let map = py.allow_threads(|| calc_std(&c, &h, &l, &v));

    let out = PyDict::new_bound(py);
    for (idx, vals) in map {
        out.set_item(idx, vals.into_pyarray_bound(py))?;
    }
    Ok(out)
}

/// Run the Rust backtest engine on precomputed signals for ONE direction.
///
/// Python computes indicators + entry/exit signals (its normal `populate_*`),
/// then hands the raw arrays here. `config_json` is a JSON object whose keys
/// match `UnifiedBacktestConfig`'s fields (missing keys use the Rust defaults).
/// `direction` is "long" or "short".
///
/// Returns a JSON string with the per-trade arrays the caller turns into trade
/// records: entry/exit bar indices, prices, profit ratio + $ pnl, durations
/// (bars), leverages, and exit-reason labels.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn run_backtest(
    py: Python<'_>,
    open: PyReadonlyArray1<'_, f64>,
    high: PyReadonlyArray1<'_, f64>,
    low: PyReadonlyArray1<'_, f64>,
    close: PyReadonlyArray1<'_, f64>,
    volume: PyReadonlyArray1<'_, f64>,
    rsi: PyReadonlyArray1<'_, f64>,
    macd_hist: PyReadonlyArray1<'_, f64>,
    bb_pos: PyReadonlyArray1<'_, f64>,
    atr: PyReadonlyArray1<'_, f64>,
    cci: PyReadonlyArray1<'_, f64>,
    entry_signals: PyReadonlyArray1<'_, u8>,
    exit_signals: PyReadonlyArray1<'_, u8>,
    direction: String,
    config_json: String,
) -> PyResult<String> {
    let o = f32_from(&open);
    let h = f32_from(&high);
    let l = f32_from(&low);
    let c = f32_from(&close);
    let vol = f32_from(&volume);
    let rsi = f32_from(&rsi);
    let mh = f32_from(&macd_hist);
    let bb = f32_from(&bb_pos);
    let atrv = f32_from(&atr);
    let cciv = f32_from(&cci);
    let ent = u8_from(&entry_signals);
    let ext = u8_from(&exit_signals);

    let mut cfg: crate::backtest::UnifiedBacktestConfig = serde_json::from_str(&config_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad config_json: {e}")))?;
    cfg.direction = direction;

    let res = py.allow_threads(|| {
        crate::cpu_engine::unified_backtest(
            &c, &o, &h, &l, &vol,
            &rsi, &mh, &bb, &atrv, &cciv,
            &ent, &ext,
            &cfg,
        )
    });

    let br = &res.backtest_result;
    let exit_reasons: Vec<String> = br.exit_reasons.iter().map(|r| format!("{r:?}")).collect();
    let out = serde_json::json!({
        "entry_indices": br.entry_indices,
        "exit_indices": br.exit_indices,
        "entry_prices": br.entry_prices,
        "exit_prices": br.exit_prices,
        "profits": br.profits,
        "pnl_amounts": br.pnl_amounts,
        "durations": br.durations,
        "leverages": br.leverages,
        "exit_reasons": exit_reasons,
        "final_equity": res.final_equity,
        "starting_balance": res.starting_balance,
    });
    Ok(out.to_string())
}

// =============================================================================
// Backtest-stats metrics (f64)
// =============================================================================
//
// `optimize_reports._generate_result_line` runs six per-group stats helpers
// (sharpe/sortino/calmar/sqn/expectancy/max_drawdown) once per pair AND once
// per exit-tag — 86 calls on a 45-pair run, measured at ~1.75s total.
//
// These are deliberately implemented in f64 rather than reusing
// `crate::metrics` (which is f32, sized for the engine's hot loop). Stats are
// user-facing numbers; matching pandas/numpy bit-for-bit matters more here than
// the few ns f32 would save. Formula notes, all mirroring VulcanTrader/data/metrics.py:
//   * sharpe/sortino use numpy's np.std -> POPULATION std (ddof=0)
//   * sqn uses pandas' .std()          -> SAMPLE std (ddof=1)
//   * a zero/NaN denominator yields -100.0 (the Python "deliberately bad score")
//   * max_drawdown prepends a zero row, so no-drawdown cases yield 0.0

fn mean_f64(xs: &[f64]) -> f64 {
    if xs.is_empty() { return f64::NAN; }
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// numpy `np.std` (ddof=0). NaN on empty, matching numpy's behaviour.
fn pop_std(xs: &[f64]) -> f64 {
    if xs.is_empty() { return f64::NAN; }
    let m = mean_f64(xs);
    (xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / xs.len() as f64).sqrt()
}

/// pandas `.std()` (ddof=1). NaN with fewer than 2 samples.
fn sample_std(xs: &[f64]) -> f64 {
    if xs.len() < 2 { return f64::NAN; }
    let m = mean_f64(xs);
    (xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64).sqrt()
}

/// Mirror of `_calculate_annualized_ratio`.
fn annualized(mean: f64, denom: f64, factor: f64) -> f64 {
    if denom != 0.0 && !denom.is_nan() { (mean / denom) * factor.sqrt() } else { -100.0 }
}

/// Compute every metric `_generate_result_line` needs, in one pass.
///
/// `profit_abs` must already be ordered by close_date (the order
/// `calculate_max_drawdown` sorts into). Returns a dict of scalars.
#[pyfunction]
fn compute_result_metrics<'py>(
    py: Python<'py>,
    profit_abs: PyReadonlyArray1<'py, f64>,
    profit_ratio: PyReadonlyArray1<'py, f64>,
    trade_duration: PyReadonlyArray1<'py, f64>,
    days_period: f64,
    starting_balance: f64,
) -> PyResult<Bound<'py, PyDict>> {
    let pa: Vec<f64> = match profit_abs.as_slice() {
        Ok(s) => s.to_vec(),
        Err(_) => profit_abs.as_array().iter().copied().collect(),
    };
    let pr: Vec<f64> = match profit_ratio.as_slice() {
        Ok(s) => s.to_vec(),
        Err(_) => profit_ratio.as_array().iter().copied().collect(),
    };
    let td: Vec<f64> = match trade_duration.as_slice() {
        Ok(s) => s.to_vec(),
        Err(_) => trade_duration.as_array().iter().copied().collect(),
    };

    let out = PyDict::new_bound(py);
    let n = pa.len();
    if n == 0 || starting_balance <= 0.0 {
        return Ok(out); // caller keeps its own empty-result handling
    }

    let (mut wins, mut draws, mut losses) = (0usize, 0usize, 0usize);
    let (mut win_sum, mut loss_sum) = (0.0f64, 0.0f64);
    let mut total_abs = 0.0f64;
    for &p in &pa {
        total_abs += p;
        if p > 0.0 { wins += 1; win_sum += p; }
        else if p < 0.0 { losses += 1; loss_sum += -p; }
        else { draws += 1; }
    }

    // ---- expectancy (mirrors calculate_expectancy) ----
    let avg_win = if wins > 0 { win_sum / wins as f64 } else { 0.0 };
    let avg_loss = if losses > 0 { loss_sum / losses as f64 } else { 0.0 };
    let winrate = wins as f64 / n as f64;
    let loserate = losses as f64 / n as f64;
    let expectancy = winrate * avg_win - loserate * avg_loss;
    let expectancy_ratio = if avg_loss > 0.0 {
        (1.0 + avg_win / avg_loss) * winrate - 1.0
    } else { 100.0 };

    // ---- ratios: all off profit_abs / starting_balance ----
    let ratios: Vec<f64> = pa.iter().map(|p| p / starting_balance).collect();
    let expected_returns_mean = ratios.iter().sum::<f64>() / days_period;
    let sharpe = annualized(expected_returns_mean, pop_std(&ratios), 365.0);
    let downside: Vec<f64> = pa.iter().filter(|&&p| p < 0.0)
        .map(|p| p / starting_balance).collect();
    let sortino = annualized(expected_returns_mean, pop_std(&downside), 365.0);

    // ---- sqn (pandas .std -> ddof=1), rounded to 4dp like Python ----
    let s_std = sample_std(&ratios);
    let sqn_raw = if s_std != 0.0 && !s_std.is_nan() {
        (n as f64).sqrt() * (mean_f64(&ratios) / s_std)
    } else { -100.0 };
    let sqn = (sqn_raw * 10_000.0).round() / 10_000.0;

    // ---- max drawdown (cumsum with a prepended zero row) ----
    let (mut cum, mut high, mut dd_abs, mut dd_rel) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let mut min_dd = 0.0f64; // the zero row's drawdown
    for &p in &pa {
        cum += p;
        if cum > high { high = cum; }
        let hv = high.max(0.0);
        let drawdown = cum - hv;
        if drawdown < min_dd {
            min_dd = drawdown;
            dd_abs = drawdown.abs();
            let max_balance = starting_balance + hv;
            dd_rel = if max_balance != 0.0 {
                (max_balance - (starting_balance + cum)) / max_balance
            } else { 0.0 };
        }
    }

    // ---- calmar (reuses the drawdown above, like the Python caller now does) ----
    let calmar = annualized((total_abs / starting_balance) / days_period * 100.0, dd_rel, 365.0);

    let profit_factor = if loss_sum != 0.0 { win_sum / loss_sum } else { 0.0 };

    out.set_item("profit_total_abs", total_abs)?;
    out.set_item("profit_mean", mean_f64(&pr))?;
    out.set_item("duration_avg_min", mean_f64(&td))?;
    out.set_item("wins", wins)?;
    out.set_item("draws", draws)?;
    out.set_item("losses", losses)?;
    out.set_item("winrate", winrate)?;
    out.set_item("expectancy", expectancy)?;
    out.set_item("expectancy_ratio", expectancy_ratio)?;
    out.set_item("sortino", sortino)?;
    out.set_item("sharpe", sharpe)?;
    out.set_item("calmar", calmar)?;
    out.set_item("sqn", sqn)?;
    out.set_item("profit_factor", profit_factor)?;
    out.set_item("max_drawdown_account", dd_rel)?;
    out.set_item("max_drawdown_abs", dd_abs)?;
    Ok(out)
}

/// Fill gaps in an OHLCV series onto a regular `step_ns` grid.
///
/// Port of `converter.ohlcv_fill_up_missing_data`, which does this with
/// `resample().agg()` + `ffill` + a `fillna(dict)` — measured at ~1.4s across
/// 45 pairs, the dominant cost of data loading. Here it is a single O(n) pass.
///
/// Semantics mirrored exactly:
///   * bins with rows aggregate open=first, high=max, low=min, close=last,
///     volume=sum (so duplicate timestamps inside one bin collapse the same way);
///   * empty bins take the previous close for open/high/low/close and volume 0;
///   * the grid spans first..=last input timestamp inclusive.
///
/// Returns (dates_ns, open, high, low, close, volume) as numpy arrays; the
/// caller reassembles the DataFrame.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn fill_missing_candles<'py>(
    py: Python<'py>,
    dates_ns: PyReadonlyArray1<'py, i64>,
    open: PyReadonlyArray1<'py, f64>,
    high: PyReadonlyArray1<'py, f64>,
    low: PyReadonlyArray1<'py, f64>,
    close: PyReadonlyArray1<'py, f64>,
    volume: PyReadonlyArray1<'py, f64>,
    step_ns: i64,
) -> PyResult<(
    Bound<'py, numpy::PyArray1<i64>>,
    Bound<'py, numpy::PyArray1<f64>>,
    Bound<'py, numpy::PyArray1<f64>>,
    Bound<'py, numpy::PyArray1<f64>>,
    Bound<'py, numpy::PyArray1<f64>>,
    Bound<'py, numpy::PyArray1<f64>>,
)> {
    let d = dates_ns.as_slice().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("dates_ns must be contiguous")
    })?;
    let o = open.as_slice().unwrap_or(&[]);
    let h = high.as_slice().unwrap_or(&[]);
    let l = low.as_slice().unwrap_or(&[]);
    let c = close.as_slice().unwrap_or(&[]);
    let v = volume.as_slice().unwrap_or(&[]);
    let n = d.len();

    if n == 0 || step_ns <= 0 {
        let e: Vec<f64> = Vec::new();
        return Ok((
            Vec::<i64>::new().into_pyarray_bound(py),
            e.clone().into_pyarray_bound(py), e.clone().into_pyarray_bound(py),
            e.clone().into_pyarray_bound(py), e.clone().into_pyarray_bound(py),
            e.into_pyarray_bound(py),
        ));
    }

    // Bin index of each input row, relative to the first timestamp.
    let start = d[0];
    let n_bins = ((d[n - 1] - start) / step_ns + 1).max(1) as usize;

    let mut out_d = Vec::with_capacity(n_bins);
    let mut out_o = Vec::with_capacity(n_bins);
    let mut out_h = Vec::with_capacity(n_bins);
    let mut out_l = Vec::with_capacity(n_bins);
    let mut out_c = Vec::with_capacity(n_bins);
    let mut out_v = Vec::with_capacity(n_bins);

    let mut i = 0usize;
    let mut prev_close = f64::NAN;
    for b in 0..n_bins {
        let bin_start = start + (b as i64) * step_ns;
        let bin_end = bin_start + step_ns;
        if i < n && d[i] < bin_end {
            // One or more rows land in this bin — aggregate them.
            let (mut hi, mut lo, mut vol) = (f64::NEG_INFINITY, f64::INFINITY, 0.0f64);
            let op = o[i];
            let mut cl = c[i];
            while i < n && d[i] < bin_end {
                if h[i] > hi { hi = h[i]; }
                if l[i] < lo { lo = l[i]; }
                vol += v[i];
                cl = c[i];
                i += 1;
            }
            out_o.push(op); out_h.push(hi); out_l.push(lo); out_c.push(cl); out_v.push(vol);
            prev_close = cl;
        } else {
            // Empty bin: carry the previous close, zero volume (matches the
            // Python ffill + fillna(close) + sum-of-nothing behaviour).
            out_o.push(prev_close); out_h.push(prev_close);
            out_l.push(prev_close); out_c.push(prev_close); out_v.push(0.0);
        }
        out_d.push(bin_start);
    }

    Ok((
        out_d.into_pyarray_bound(py),
        out_o.into_pyarray_bound(py),
        out_h.into_pyarray_bound(py),
        out_l.into_pyarray_bound(py),
        out_c.into_pyarray_bound(py),
        out_v.into_pyarray_bound(py),
    ))
}

#[pymodule]
fn vulcan_rust_indicators(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fill_missing_candles, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_standard_indicators, m)?)?;
    m.add_function(wrap_pyfunction!(run_backtest, m)?)?;
    m.add_function(wrap_pyfunction!(compute_result_metrics, m)?)?;
    Ok(())
}
