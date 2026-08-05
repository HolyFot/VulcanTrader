#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  VulcanTrader - data_server MASTER (Linux / macOS)
#
#  Explicitly starts the data_server master as its own standalone process,
#  instead of relying on the implicit auto-launch-from-first-bot path
#  (trader_bot.py's ensure_master_running, which spawns one using whichever
#  Python interpreter happens to be running the launching bot). Run this
#  FIRST, before starting the fleet (run-app.sh / bot.py autostart) - bots
#  that find a master already listening just connect to it instead of each
#  racing to spawn their own.
#
#  Refuses to start if something is already listening on HOST:PORT - if you
#  hit that, a master (or a stray duplicate) is already running; don't start
#  a second one. Two masters fighting over the same exchange budget is what
#  caused the fleet-wide rate-limit storm this script exists to prevent.
#
#  Tip: wrap this in tmux for unattended hosting:
#       tmux new -s DataMaster './run-datamaster.sh'
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .venv/bin/activate ]]; then
    echo "[run-datamaster] .venv not found - run ./install.sh first." >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

CONFIG="${CONFIG:-user_data/configs_system/data_server.json}"
EXCHANGE="${EXCHANGE:-hyperliquid}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8720}"
SUBSERVER_PORT="${SUBSERVER_PORT:-8721}"
TIMEFRAMES="${TIMEFRAMES:-15m}"

probe_host="$HOST"
if [[ "$probe_host" == "0.0.0.0" ]]; then
    probe_host="127.0.0.1"
fi
if python -c "
import socket, sys
s = socket.socket()
s.settimeout(1.5)
sys.exit(0 if s.connect_ex(('$probe_host', $PORT)) == 0 else 1)
"; then
    echo "[run-datamaster] Something is already listening on $HOST:$PORT - refusing to start a second master." >&2
    exit 1
fi

echo "[run-datamaster] config=$CONFIG exchange=$EXCHANGE host=$HOST port=$PORT subserver-port=$SUBSERVER_PORT timeframes=$TIMEFRAMES"
exec python -m VulcanTrader.data_server \
    --mode master \
    -c "$CONFIG" \
    --host "$HOST" \
    --port "$PORT" \
    --subserver-port "$SUBSERVER_PORT" \
    --exchange "$EXCHANGE" \
    --timeframes "$TIMEFRAMES" \
    -v "$@"
