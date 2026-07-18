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

#[pymodule]
fn vulcan_rust_indicators(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calculate_standard_indicators, m)?)?;
    Ok(())
}
