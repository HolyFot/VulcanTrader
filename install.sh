#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - one-shot installer (Linux / macOS)
#
#  Creates a virtualenv in .venv, upgrades pip, and installs every dependency
#  listed in requirements.txt.  Override the interpreter with PYTHON=pythonX.Y
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON:-python3}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "[install] ERROR: '$PYTHON_BIN' not found. Install Python 3 first." >&2
    echo "[install]        (set PYTHON=python3.12 to pin a specific version)" >&2
    exit 1
fi

if [[ ! -d .venv ]]; then
    echo "[install] creating virtualenv .venv ($("$PYTHON_BIN" --version 2>&1)) ..."
    "$PYTHON_BIN" -m venv .venv
else
    echo "[install] reusing existing .venv"
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip wheel
python -m pip install -r requirements.txt

echo
echo "[install] building the Rust backtester + vulcan_rust_indicators bridge (VulcanTrader/backtester) ..."
echo "[install]   One crate: the backtest engine, and the PyO3 indicator module that"
echo "[install]   AllIndicatorsDemoStrategy.py and several user_data/strategies/*.py import."
if ! command -v cargo >/dev/null 2>&1; then
    echo "[install] ERROR: 'cargo' not found. Install the Rust toolchain from https://rustup.rs first." >&2
    exit 1
fi
# maturin builds the `vulcan_rust_indicators` extension (which also compiles the
# engine). PYO3_USE_ABI3_FORWARD_COMPATIBILITY allows building the abi3 module
# against a Python newer than PyO3 explicitly supports.
(cd VulcanTrader/backtester && PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin develop --release --features extension-module)

echo
echo "[install] done. Activate later with:  source .venv/bin/activate"
echo "[install] then run:                   ./run-paper.sh   (or ./run-live.sh)"
