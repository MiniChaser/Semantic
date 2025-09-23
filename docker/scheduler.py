#!/usr/bin/env python3
import time
import subprocess
import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def main():
    # Get interval from environment variable, default to 7 days
    interval_days = int(os.getenv('SCHEDULE_INTERVAL_DAYS', '7'))
    interval_seconds = interval_days * 24 * 60 * 60

    log_message(f"Scheduler started - will run scripts every {interval_days} days")

    while True:
        try:
            log_message("Starting script execution cycle")

            # Run the scripts
            result = subprocess.run(['/app/docker/run_scripts.sh'],
                                  capture_output=False,
                                  text=True)

            if result.returncode == 0:
                log_message("Scripts completed successfully")
            else:
                log_message(f"Scripts failed with return code: {result.returncode}")

            # Wait for specified interval
            next_run = datetime.now() + timedelta(days=interval_days)
            log_message(f"Next execution scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            log_message(f"Waiting {interval_days} days...")

            time.sleep(interval_seconds)

        except KeyboardInterrupt:
            log_message("Scheduler stopped by user")
            break
        except Exception as e:
            log_message(f"Error in scheduler: {e}")
            log_message("Waiting 1 hour before retry...")
            time.sleep(3600)  # Wait 1 hour before retrying

if __name__ == "__main__":
    main()