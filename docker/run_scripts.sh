#!/bin/bash
set -e

LOG_DIR="/var/log/semantic"
LOG_FILE="$LOG_DIR/scripts_$(date +%Y%m%d_%H%M%S).log"

echo "Starting scheduled script execution at $(date)" >> "$LOG_FILE"

# Run scripts sequentially
echo "Running setup_database.py..." >> "$LOG_FILE"
uv run python scripts/setup_database.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "setup_database.py failed at $(date)" >> "$LOG_FILE"
    exit 1
fi

echo "Running run_dblp_service_once.py..." >> "$LOG_FILE"
uv run python scripts/run_dblp_service_once.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "run_dblp_service_once.py failed at $(date)" >> "$LOG_FILE"
    exit 1
fi

echo "Running run_s2_enrichment.py..." >> "$LOG_FILE"
uv run python scripts/run_s2_enrichment.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "run_s2_enrichment.py failed at $(date)" >> "$LOG_FILE"
    exit 1
fi

echo "Running run_all_steps.py..." >> "$LOG_FILE"
uv run python scripts/run_all_steps.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "run_all_steps.py failed at $(date)" >> "$LOG_FILE"
    exit 1
fi

echo "All scripts completed successfully at $(date)" >> "$LOG_FILE"