#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: ./start.sh <code> [extra args...]"
  echo "Example: ./start.sh <code> --wx --interval 1 --friend-interval 1"
  exit 1
fi

CODE="$1"
shift || true
EXTRA_ARGS=("$@")

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$BASE_DIR/logs"
LOG_FILE="$LOG_DIR/farm.log"
LOCK_FILE="$BASE_DIR/.bot.lock"

mkdir -p "$LOG_DIR"

echo "==== $(date '+%F %T') ===="
echo "Base Dir: $BASE_DIR"
echo "Code: ${CODE:0:8}..."

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
  fi
}

# 1) Prefer lock file PID (matches client.js single-instance lock)
if [ -f "$LOCK_FILE" ]; then
  LOCK_PID="$(tr -dc '0-9' < "$LOCK_FILE" || true)"
  if [ -n "${LOCK_PID:-}" ]; then
    echo "Stopping lock PID: $LOCK_PID"
    stop_pid "$LOCK_PID"
  fi
  rm -f "$LOCK_FILE" || true
fi

# 2) Fallback cleanup for old style runs without lock
OLD_PIDS="$(pgrep -f "node .*client\\.js" || true)"
if [ -n "${OLD_PIDS:-}" ]; then
  echo "Stopping fallback PIDs: $OLD_PIDS"
  for pid in $OLD_PIDS; do
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    case "$cmd" in
      *"$BASE_DIR/client.js"*|*"node client.js"*)
        stop_pid "$pid"
        ;;
    esac
  done
fi

echo "Starting new instance..."
cd "$BASE_DIR"
echo "==== $(date '+%F %T') restart ====" >> "$LOG_FILE"
setsid -f node "$BASE_DIR/client.js" --code "$CODE" "${EXTRA_ARGS[@]}" >> "$LOG_FILE" 2>&1

sleep 1

NEW_PID=""
if [ -f "$LOCK_FILE" ]; then
  NEW_PID="$(cat "$LOCK_FILE" 2>/dev/null || true)"
fi
if [ -z "${NEW_PID:-}" ]; then
  NEW_PID="$(pgrep -n -f "node .*client\\.js" || true)"
fi

echo "Started. PID: ${NEW_PID:-unknown}"
echo "Log file: $LOG_FILE"
