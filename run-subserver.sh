#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - data_server subserver (Linux / macOS)
#
#  Runs a data_server subserver: dials out to a data_server MASTER (see
#  run-paper.sh/run-live.sh, which auto-launch one) and takes on a share of
#  its OHLCV/funding-rate/trades collection work. The master never launches a
#  subserver itself - it's a deliberate, separately-run process, typically on
#  a different machine.
#
#  data_server has ONE general config per machine (not one per exchange) at
#  user_data/data_server_configs/config.json. On a dedicated subserver
#  machine, copy user_data/data_server_configs/config.subserver.example.json
#  to that path and set "master_host" to the real master's IP. --mode is
#  deliberately omitted below: the config's "is_subserver": true already
#  selects subserver mode on its own.
#
#  Tip: wrap this in tmux for unattended hosting:
#       tmux new -s Subserver1 './run-subserver.sh'
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .venv/bin/activate ]]; then
    echo "[run-subserver] .venv not found - run ./install.sh first." >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

CONFIG="${CONFIG:-user_data/data_server_configs/config.json}"

echo "[run-subserver] config=$CONFIG"
exec python -m VulcanTrader.data_server \
    -v \
    -c "$CONFIG" "$@"
