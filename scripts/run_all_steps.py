#!/usr/bin/env python3
"""
Author Processing - Run All Steps
Executes all Phase 1 steps sequentially with better error handling and logging
"""

import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime


def run_step(step_script: str, step_name: str) -> bool:
    """
    Run a single step script
    
    Args:
        step_script: Path to the step script
        step_name: Display name for the step
        
    Returns:
        True if successful, False otherwise
    """
    print(f"\n🚀 Starting {step_name}")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        # Run the step script using uv
        result = subprocess.run([
            "uv", "run", "python", step_script
        ], check=True, capture_output=False)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ {step_name} completed in {duration:.1f} seconds")
        return True
        
    except subprocess.CalledProcessError as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"❌ {step_name} failed after {duration:.1f} seconds")
        print(f"Error code: {e.returncode}")
        return False


def main():
    """Execute all Phase 1 steps sequentially"""
    
    overall_start_time = datetime.now()
    print("🚀 Starting Complete Author Processing Phase 1")
    print("=" * 80)
    print(f"⏰ Started at: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Define all steps
    steps = [
        ("scripts/step1_create_authorships.py", "Step 1: Create Authorships Table"),
        ("scripts/step2_create_author_profiles.py", "Step 2: Create Author Profiles Table"),
        ("scripts/step3_calculate_metrics.py", "Step 3: Calculate Advanced Metrics"), 
        ("scripts/step4_create_final_table.py", "Step 4: Create Final Author Table"),
        ("scripts/step5_generate_reports.py", "Step 5: Generate Reports")
    ]
    
    successful_steps = 0
    failed_steps = 0
    
    # Execute each step
    for step_script, step_name in steps:
        if run_step(step_script, step_name):
            successful_steps += 1
        else:
            failed_steps += 1
            print(f"⚠️ Stopping execution due to failure in {step_name}")
            break
    
    # Final summary
    overall_end_time = datetime.now()
    total_duration = overall_end_time - overall_start_time
    
    print(f"\n🎉 EXECUTION SUMMARY")
    print("=" * 80)
    print(f"⏰ Started at: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🏁 Finished at: {overall_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⌛ Total duration: {total_duration}")
    print()
    print(f"✅ Successful steps: {successful_steps}/{len(steps)}")
    print(f"❌ Failed steps: {failed_steps}")
    
    if successful_steps == len(steps):
        print("\n🎉 All steps completed successfully!")
        print("🚀 Phase 1 implementation is complete and ready for Phase 2!")
        return 0
    else:
        print(f"\n⚠️ Phase 1 incomplete: {failed_steps} step(s) failed")
        print("🔧 Please check the logs and fix the issues before proceeding")
        return 1


if __name__ == "__main__":
    sys.exit(main())