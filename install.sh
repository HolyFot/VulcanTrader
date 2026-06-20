#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - one-shot installer (Linux / macOS)
#
#  Creates a Python 3.12 virtualenv in .venv, upgrades pip, and installs
#  every dependency listed in requirements.txt.
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON:-python3.12}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "[install] ERROR: '$PYTHON_BIN' not found. Install Python 3.12 first." >&2
    echo "[install]        (set PYTHON=python3 to override)" >&2
    exit 1
fi

if [[ ! -d .venv ]]; then
    echo "[install] creating virtualenv .venv with $PYTHON_BIN ..."
    "$PYTHON_BIN" -m venv .venv
else
    echo "[install] reusing existing .venv"
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip wheel
python -m pip install -r requirements.txt

echo
echo "[install] done. Activate later with:  source .venv/bin/activate"
echo "[install] then run:                   ./run-paper.sh   (or ./run-live.sh)"
