#!/usr/bin/env bash
# Stop the backgrounded SSM port-forward tunnel started by start-gateway-tunnel.sh.
#
# The PID file holds the PID of the parent script (the one started via
# `nohup ... &` from CI or the shell). Killing it triggers the script's
# cleanup trap which kills each per-port SSM session.

set -euo pipefail

PID_FILE="${TUNNEL_PID_FILE:-/tmp/ibkr-gateway-tunnel.pid}"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file at $PID_FILE — nothing to stop."
  exit 0
fi

PID="$(cat "$PID_FILE")"
if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
  echo "→ Stopping tunnel PID $PID"
  kill "$PID" 2>/dev/null || true
  for _ in $(seq 1 10); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
  fi
fi

rm -f "$PID_FILE"
echo "✅ Tunnel stopped."
