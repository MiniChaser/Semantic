#!/bin/bash
#
# Stop Stage 2 import script and all its child processes
#
# This script stops the import_papers_stage2_conferences.py script by killing
# the entire process group, ensuring all child processes (multiprocessing workers)
# are terminated cleanly.
#
# Usage:
#   ./stop_stage2.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/stage2.pid"

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo "Error: PID file not found: $PID_FILE"
    echo "The Stage 2 script does not appear to be running."
    exit 1
fi

# Read the process group ID
PGID=$(cat "$PID_FILE")

# Check if the process group is running
if ! ps -g "$PGID" > /dev/null 2>&1; then
    echo "Warning: No processes found with process group ID: $PGID"
    echo "The script may have already stopped."
    rm -f "$PID_FILE"
    exit 0
fi

# Show the processes that will be killed
echo "Found the following processes in group $PGID:"
ps -g "$PGID" -o pid,pgid,cmd
echo ""

# Ask for confirmation
read -p "Kill all these processes? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Kill the entire process group
echo "Sending SIGTERM to process group $PGID..."
kill -- -"$PGID" 2>/dev/null || true

# Wait a moment for graceful shutdown
sleep 2

# Check if any processes are still running
if ps -g "$PGID" > /dev/null 2>&1; then
    echo "Some processes are still running, sending SIGKILL..."
    kill -9 -- -"$PGID" 2>/dev/null || true
    sleep 1
fi

# Final check
if ps -g "$PGID" > /dev/null 2>&1; then
    echo "Warning: Some processes may still be running:"
    ps -g "$PGID" -o pid,pgid,cmd
    echo ""
    echo "You may need to kill them manually."
else
    echo "✓ All processes stopped successfully!"
fi

# Remove PID file
rm -f "$PID_FILE"
echo "✓ PID file removed: $PID_FILE"
