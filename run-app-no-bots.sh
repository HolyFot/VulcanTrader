#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - web dashboard only, no auto-started bots (Linux / macOS)
#
#  Starts the FastAPI web portal WITHOUT launching any auto_start: true
#  configs (passes --no-autostart to `webserver`). Use this to browse
#  backtest results, inspect closed trades, and use the analysis tools
#  without bringing up the trading fleet.
#
#  Dashboard: http://localhost:8060
#  Default password: test
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .venv/bin/activate ]]; then
    echo "[run-app-no-bots] .venv not found - run ./install.sh first." >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

PORT="${PORT:-8060}"

echo "[run-app-no-bots] port=$PORT"
exec python -m VulcanTrader.bot webserver \
    --no-autostart \
    --port "$PORT" "$@"
