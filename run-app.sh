#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - web dashboard only (Linux / macOS)
#
#  Starts the FastAPI web portal WITHOUT starting live or paper trading.
#  Use this to browse backtest results, inspect closed trades, and use
#  the analysis tools when no bot session is running.
#
#  Dashboard: http://localhost:8060
#  Default password: test
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .venv/bin/activate ]]; then
    echo "[run-app] .venv not found - run ./install.sh first." >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

PORT="${PORT:-8060}"

echo "[run-app] port=$PORT"
exec python -m VulcanTrader.bot webserver \
    --port "$PORT" "$@"
