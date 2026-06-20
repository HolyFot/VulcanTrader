#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - paper / dry-run trading (Linux / macOS)
#
#  Starts the trader bot in --dry-run mode. The trade subcommand also
#  brings up the FastAPI web portal automatically (pass --no-web to skip).
#
#  Tip: wrap this in tmux for unattended hosting:
#       tmux new -s Bot4 './run-paper.sh'
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .venv/bin/activate ]]; then
    echo "[run-paper] .venv not found - run ./install.sh first." >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

CONFIG="${CONFIG:-configMyStrategy_Paper}"
STRATEGY="${STRATEGY:-MyStrategy}"
DB_URL="${DB_URL:-json:///user_data/accounts/MyStrategy_dry_run.json}"

echo "[run-paper] config=$CONFIG  strategy=$STRATEGY  db=$DB_URL"
exec python -m VulcanTrader.bot trade \
    --dry-run \
    -c "$CONFIG" \
    --strategy "$STRATEGY" \
    --db-url "$DB_URL" "$@"
