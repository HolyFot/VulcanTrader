// =============================================================================
// vulcan_rust_indicators — PyO3 bridge into this crate's own indicators
// =============================================================================
//
// Compiled only under the `python` / `extension-module` features (what maturin
// builds with). Exposes `fast_indicators::calculate_standard_indicators` to
// Python so strategies (see user_data/strategies/AllIndicatorsDemoStrategy.py)
// pull the exact same 23 standard series the engine uses, by their fixed index,
// instead of recomputing them in pandas/TA-Lib.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::fast_indicators::calculate_standard_indicators as calc_std;

/// Compute the 23 standard indicators for the given OHLCV arrays.
///
/// Inputs are float64 (numpy arrays or plain sequences); the engine works in
/// f32, so they are down-cast on the way in. Returns `{index: [f32, ...]}`
/// matching `fast_indicators::calculate_standard_indicators`' fixed-index map
/// (see the table in AllIndicatorsDemoStrategy). Warmup positions are NaN,
/// exactly as the engine produces them.
#[pyfunction]
fn calculate_standard_indicators(
    py: Python<'_>,
    close: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    volume: Vec<f64>,
) -> PyResult<HashMap<usize, Vec<f32>>> {
    let to_arc = |v: Vec<f64>| Arc::new(v.into_iter().map(|x| x as f32).collect::<Vec<f32>>());
    let close = to_arc(close);
    let high = to_arc(high);
    let low = to_arc(low);
    let volume = to_arc(volume);

    // Release the GIL for the (rayon-parallel) number crunching.
    let map = py.allow_threads(|| calc_std(&close, &high, &low, &volume));
    Ok(map)
}

/// Run the Rust backtest engine on precomputed signals for ONE direction.
///
/// Python computes indicators + entry/exit signals (its normal
/// `populate_*`), then hands the raw arrays here. `config_json` is a JSON
/// object whose keys match `UnifiedBacktestConfig`'s fields (missing keys use
/// the Rust defaults). `direction` is "long" or "short".
///
/// Returns a JSON string with the per-trade arrays the caller turns into
/// trade records: entry/exit bar indices, prices, profit ratio + $ pnl,
/// durations (bars), leverages, and exit-reason labels.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn run_backtest(
    py: Python<'_>,
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    volume: Vec<f64>,
    rsi: Vec<f64>,
    macd_hist: Vec<f64>,
    bb_pos: Vec<f64>,
    atr: Vec<f64>,
    cci: Vec<f64>,
    entry_signals: Vec<u8>,
    exit_signals: Vec<u8>,
    direction: String,
    config_json: String,
) -> PyResult<String> {
    let f32v = |v: Vec<f64>| v.into_iter().map(|x| x as f32).collect::<Vec<f32>>();
    let (o, h, l, c, vol) = (f32v(open), f32v(high), f32v(low), f32v(close), f32v(volume));
    let (rsi, mh, bb, atrv, cciv) = (f32v(rsi), f32v(macd_hist), f32v(bb_pos), f32v(atr), f32v(cci));

    let mut cfg: crate::backtest::UnifiedBacktestConfig = serde_json::from_str(&config_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad config_json: {e}")))?;
    cfg.direction = direction;

    let res = py.allow_threads(|| {
        crate::cpu_engine::unified_backtest(
            &c, &o, &h, &l, &vol,
            &rsi, &mh, &bb, &atrv, &cciv,
            &entry_signals, &exit_signals,
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

#[pymodule]
fn vulcan_rust_indicators(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calculate_standard_indicators, m)?)?;
    m.add_function(wrap_pyfunction!(run_backtest, m)?)?;
    Ok(())
}
