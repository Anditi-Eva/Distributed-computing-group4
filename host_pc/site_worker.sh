#!/usr/bin/env bash
# Run one worker at a remote site (multi-site demo), restarted 10 s after any crash (RESTART_DELAY).
#
#   ./site_worker.sh start core 5002 8 2048     # name, port, CPU units, memory MB
#   ./site_worker.sh status core
#   ./site_worker.sh stop core
#
# Layout expected next to this script: worker_vms/ (worker code) and venv/ (Python env).
# The worker listens on 127.0.0.1 only; the gateway reaches it through an SSH tunnel, so no
# port is opened to the internet. It runs under `nice -n 19` so it never competes with other
# work on the host.
set -u
cmd=${1:?start|stop|status}
name=${2:?worker name}
dir=$(cd "$(dirname "$0")" && pwd)
pidfile="$dir/$name.supervisor.pid"

running() { [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; }

case "$cmd" in
start)
  port=${3:?port}; cpu=${4:-8}; mem=${5:-2048}
  if running; then echo "$name already running (supervisor $(cat "$pidfile"))"; exit 0; fi
  export NODE_ID="$name" NODE_PORT="$port" NODE_CPU="$cpu" NODE_MEMORY="$mem" \
         PEER_HOST=127.0.0.1 PEER_PORT=9 PROM_PORT=$((port + 4100)) WORKER_BIND_HOST=127.0.0.1 \
         KAFKA_BOOTSTRAP_SERVERS="" SHARED_STATE_DB="$dir/$name-state.sqlite3" \
         FILE_STORE_DB="$dir/$name-files.sqlite3" PYTHONUNBUFFERED=1 WORKER_PY="$dir/venv/bin/python" \
         WORKER_DIR="$dir/worker_vms"
  nohup setsid nice -n 19 bash -c '
    cd "$WORKER_DIR"
    while true; do
      "$WORKER_PY" worker_node.py
      echo "[supervisor] $NODE_ID exited at $(date +%T); restarting in ${RESTART_DELAY:-10} s"
      sleep "${RESTART_DELAY:-10}"
    done' > "$dir/$name.log" 2>&1 < /dev/null &
  echo $! > "$pidfile"
  echo "$name started (supervisor $(cat "$pidfile")), log $dir/$name.log"
  ;;
stop)
  if running; then
    pid=$(cat "$pidfile")
    kill -- "-$pid" 2>/dev/null || kill "$pid"
    echo "$name stopped"
  else
    echo "$name not running"
  fi
  rm -f "$pidfile"
  ;;
status)
  if running; then echo "$name running (supervisor $(cat "$pidfile"))"; else echo "$name not running"; fi
  ;;
*)
  echo "usage: $0 start|stop|status NAME [PORT CPU MEM]"; exit 2
  ;;
esac
