#!/usr/bin/env bash
# Cloud deployment of the Milestone 9/10 demo on the "core" server.
#
#   ./cloud_stack.sh start | stop | status
#
# Runs, each in a restart loop: the core worker (site_worker.sh), the proposed gateway (:5101)
# and the live dashboard (:8080, owns the SSH tunnel to the cloud site). The cloud worker runs on
# its own server under site_worker.sh. Settings come from stack.env next to this script:
#   CLOUD_SSH=ubuntu@<cloud private ip>   CLOUD_KEY=~/.ssh/<key>   DASH_TOKEN=<secret>
#   CORE_LABEL=Dublin   CLOUD_LABEL=Tokyo
# The tunnel uses -tt (an interactive session) so OpenSSH sets TCP_NODELAY: without it Nagle's
# algorithm held every small HTTP write for a round trip (811 ms -> 407 ms Dublin-Tokyo, measured).
# The cloud server pins this key to: restrict,pty,port-forwarding,permitopen="127.0.0.1:5003",command="sleep ..."
set -u
dir=$(cd "$(dirname "$0")" && pwd)
# shellcheck disable=SC1091
. "$dir/stack.env"
py="$dir/venv/bin/python"

supervise() {   # name, command (run from $dir/host_pc)
  local name=$1 cmd=$2 pidfile="$dir/$1.supervisor.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then echo "$name already running"; return; fi
  STACK_CMD="$cmd" nohup setsid nice -n 10 bash -c 'cd "'"$dir"'/host_pc"; while true; do eval "$STACK_CMD"; echo "[supervisor] exited $(date +%T), restarting in 3 s"; sleep 3; done' \
    > "$dir/$name.log" 2>&1 < /dev/null &
  echo $! > "$pidfile"; echo "$name started"
}

stop_one() {
  local pidfile="$dir/$1.supervisor.pid"
  if [ -f "$pidfile" ]; then kill -- "-$(cat "$pidfile")" 2>/dev/null; rm -f "$pidfile"; echo "$1 stopped"; fi
}

case "${1:-status}" in
start)
  "$dir/site_worker.sh" start core 5002 8 2048
  supervise gateway "TRANSPARENCY_NODES=edge=http://127.0.0.1:5001,core=http://127.0.0.1:5002,cloud1=http://127.0.0.1:5003 \
    HEARTBEAT_INTERVAL=0.5 NAMING_DB=$dir/proposed-naming.sqlite3 PORT=5101 $py proposed_gateway.py"
  supervise dashboard "DASH_TOKEN=$DASH_TOKEN GATEWAY_URL=http://127.0.0.1:5101 \
    SITES='edge=Edge: Nairobi PC@http://127.0.0.1:5001,core=${CORE_LABEL:-Core}@http://127.0.0.1:5002,cloud1=${CLOUD_LABEL:-Cloud}@http://127.0.0.1:5003' \
    LINK_SITE=cloud1 LINK_CMD='ssh -tt -i $CLOUD_KEY -o BatchMode=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=3 -o StrictHostKeyChecking=accept-new -L 5003:127.0.0.1:5003 $CLOUD_SSH' \
    $py live_dashboard.py"
  ;;
stop)
  stop_one dashboard; stop_one gateway; "$dir/site_worker.sh" stop core
  ;;
status)
  for n in gateway dashboard; do
    if [ -f "$dir/$n.supervisor.pid" ] && kill -0 "$(cat "$dir/$n.supervisor.pid")" 2>/dev/null; then echo "$n running"; else echo "$n stopped"; fi
  done
  "$dir/site_worker.sh" status core
  ;;
esac
