#!/bin/bash
#
# Start Stage 2 import script with process group management
#
# This script starts the import_papers_stage2_conferences.py script in the background
# and manages it as a process group, making it easy to stop all processes with one command.
#
# Usage:
#   ./start_stage2.sh [OPTIONS]
#
# Options are passed directly to the Python script:
#   --batch-size N      Batch size for processing (default: 10000)
#   --skip-rebuild      Skip index rebuild after insert
#   --keep-indexes      Keep indexes during insert (slower)
#
# Examples:
#   ./start_stage2.sh
#   ./start_stage2.sh --batch-size 20000
#   ./start_stage2.sh --skip-rebuild
#
# To stop the script:
#   ./stop_stage2.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/stage2.pid"
LOG_FILE="$SCRIPT_DIR/stage2.log"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PGID=$(cat "$PID_FILE")
    if ps -g "$PGID" > /dev/null 2>&1; then
        echo "Error: Stage 2 script is already running with process group ID: $PGID"
        echo "To stop it, run: ./stop_stage2.sh"
        exit 1
    else
        echo "Warning: Found stale PID file, removing..."
        rm -f "$PID_FILE"
    fi
fi

# Start the script in a new process group using setsid
echo "Starting Stage 2 import script..."
echo "Log file: $LOG_FILE"
echo "PID file: $PID_FILE"
echo ""

# Use setsid to create a new process group and run in background
# The process group ID will be the same as the main process PID
setsid bash -c "
    cd '$SCRIPT_DIR'
    exec uv run python scripts/import_papers_stage2_conferences.py $@ > '$LOG_FILE' 2>&1
" &

# Get the process group ID (which is the PID of the setsid process)
MAIN_PID=$!

# Wait a moment for the process to start
sleep 1

# Get the actual process group ID
PGID=$(ps -o pgid= -p $MAIN_PID 2>/dev/null | tr -d ' ')

if [ -z "$PGID" ]; then
    echo "Error: Failed to get process group ID. The script may have failed to start."
    echo "Check the log file: $LOG_FILE"
    exit 1
fi

# Save the process group ID
echo "$PGID" > "$PID_FILE"

echo "✓ Stage 2 script started successfully!"
echo "  Process Group ID: $PGID"
echo "  PID file: $PID_FILE"
echo ""
echo "Monitor progress:"
echo "  tail -f $LOG_FILE"
echo ""
echo "Stop the script:"
echo "  ./stop_stage2.sh"
echo ""
