#!/bin/bash
#
# start.sh — AESO CSD collector launcher
# Loads .env and runs the poller inside a screen session with auto-restart.
#
# Usage:
#   ./start.sh          — start or attach to existing session
#   ./start.sh stop     — kill the screen session
#   ./start.sh status   — show if session is running

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
LISP_FILE="$SCRIPT_DIR/aeso-csd-to-grafana.lisp"
SESSION_NAME="aeso"

case "${1:-start}" in

  stop)
    screen -S "$SESSION_NAME" -X quit
    echo "Stopped screen session: $SESSION_NAME"
    ;;

  status)
    if screen -list | grep -q "$SESSION_NAME"; then
      echo "Running — attach with: screen -r $SESSION_NAME"
    else
      echo "Not running"
    fi
    ;;

  start|*)
    # If session already exists just attach
    if screen -list | grep -q "$SESSION_NAME"; then
      echo "Session already running — attaching..."
      screen -r "$SESSION_NAME"
      exit 0
    fi

    # Load .env if it exists
    if [ -f "$ENV_FILE" ]; then
      echo "Loading $ENV_FILE"
      set -a
      source "$ENV_FILE"
      set +a
    else
      echo "Warning: $ENV_FILE not found — using existing environment"
    fi

    echo "Starting AESO collector in screen session: $SESSION_NAME"
    screen -dmS "$SESSION_NAME" bash -c "
      source \"$ENV_FILE\" 2>/dev/null
      while true; do
        sbcl --load \"$LISP_FILE\"
        echo \"[RESTART] Poller exited — restarting in 10 seconds...\"
        sleep 10
      done
    "
    echo "Started. Attach with: screen -r $SESSION_NAME"
    echo "Detach later with:    Ctrl+A then D"
    ;;
esac
