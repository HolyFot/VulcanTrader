#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - LIVE trading (real money) (Linux / macOS)
#
#  Starts the trader bot WITHOUT --dry-run. The trade subcommand also
#  brings up the FastAPI web portal automatically (pass --no-web to skip).
#
#  WARNING: this will place real orders against the configured exchange.
#
#  Tip: wrap this in tmux for unattended hosting:
#       tmux new -s Bot4 './run-live.sh'
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .venv/bin/activate ]]; then
    echo "[run-live] .venv not found - run ./install.sh first." >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

CONFIG="${CONFIG:-configMyStrategy_Live}"
STRATEGY="${STRATEGY:-MyStrategy}"
DB_URL="${DB_URL:-json:///user_data/accounts/MyStrategy_live.json}"

echo
echo " ============================================================"
echo "  LIVE TRADING - real orders will be placed."
echo "  config=$CONFIG  strategy=$STRATEGY  db=$DB_URL"
echo " ============================================================"

exec python -m VulcanTrader.bot trade \
    -c "$CONFIG" \
    --strategy "$STRATEGY" \
    --db-url "$DB_URL" "$@"
