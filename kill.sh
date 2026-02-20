#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCK_FILE="$BASE_DIR/.bot.lock"

stopped=0

stop_pid() {
  local pid="$1"
  if [ -z "${pid}" ]; then
    return 0
  fi
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    stopped=$((stopped + 1))
  fi
}

# 1) Lock file PID
if [ -f "$LOCK_FILE" ]; then
  lock_pid="$(tr -dc '0-9' < "$LOCK_FILE" || true)"
  if [ -n "${lock_pid:-}" ]; then
    stop_pid "$lock_pid"
  fi
  rm -f "$LOCK_FILE" || true
fi

# 2) Fallback process scan
if command -v pgrep >/dev/null 2>&1; then
  pids="$(pgrep -f "node .*client\\.js" || true)"
else
  pids="$(ps -eo pid=,args= | grep -E "node .*client\\.js" | grep -v grep | awk '{print $1}' || true)"
fi

if [ -n "${pids:-}" ]; then
  for pid in $pids; do
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    case "$cmd" in
      *"$BASE_DIR/client.js"*|*"node client.js"*)
        stop_pid "$pid"
        ;;
    esac
  done
fi

echo "Cleanup done. Stopped: $stopped"
