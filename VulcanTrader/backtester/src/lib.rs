// =============================================================================
// BACKTESTER — Core backtesting engine and indicators crate
// =============================================================================

pub mod backtest;
pub mod cpu_engine;
pub mod fast_indicators;
pub mod metrics;

// The `vulcan_rust_indicators` Python extension module — only compiled when
// building the PyO3 bridge (maturin / `--features extension-module`).
#[cfg(feature = "python")]
pub mod python;