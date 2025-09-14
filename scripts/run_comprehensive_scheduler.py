#!/usr/bin/env python3
"""
Comprehensive Pipeline Scheduler Entry Point
Main script to start the 7-day scheduled comprehensive data processing pipeline

Usage:
- Weekly mode (default): uv run python scripts/run_comprehensive_scheduler.py
- Manual execution: uv run python scripts/run_comprehensive_scheduler.py --manual
- Custom schedule: uv run python scripts/run_comprehensive_scheduler.py --custom "0 2 */3 * *"
- List jobs: uv run python scripts/run_comprehensive_scheduler.py --list
"""

import sys
import os
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.scheduler.comprehensive_scheduler import ComprehensivePipelineScheduler


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Comprehensive Data Processing Pipeline Scheduler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start weekly scheduler (every 7 days at 2 AM)
  uv run python scripts/run_comprehensive_scheduler.py
  
  # Execute pipeline immediately  
  uv run python scripts/run_comprehensive_scheduler.py --manual
  
  # Custom schedule (every 3 days at 2 AM)
  uv run python scripts/run_comprehensive_scheduler.py --custom "0 2 */3 * *"
  
  # Custom schedule (daily at 1 AM)
  uv run python scripts/run_comprehensive_scheduler.py --custom "0 1 * * *"
  
  # List all scheduled jobs
  uv run python scripts/run_comprehensive_scheduler.py --list
        """)
    
    parser.add_argument('--manual', action='store_true', 
                       help='Execute pipeline immediately (manual mode)')
    parser.add_argument('--custom', type=str, metavar='CRON_EXPR',
                       help='Use custom cron schedule (format: "minute hour day month day_of_week")')
    parser.add_argument('--list', action='store_true',
                       help='List all scheduled jobs')
    
    args = parser.parse_args()
    
    try:
        print("🚀 Comprehensive Data Processing Pipeline Scheduler")
        print("=" * 60)
        
        # Load configuration
        print("Loading configuration...")
        config = AppConfig.from_env()
        
        # Validate configuration
        if not config.validate():
            print("❌ Configuration validation failed, please check environment variables")
            return 1
        
        print("✅ Configuration loaded successfully")
        
        # Create scheduler
        print("Initializing scheduler...")
        scheduler = ComprehensivePipelineScheduler(config)
        print("✅ Scheduler initialized")
        
        # Determine execution mode
        if args.list:
            print("\n📋 Listing scheduled jobs:")
            scheduler.list_jobs()
            return 0
        
        elif args.manual:
            print("\n🔄 Starting manual execution mode...")
            print("Pipeline will execute in 30 seconds...")
            scheduler.start(mode="manual")
        
        elif args.custom:
            print(f"\n📅 Starting with custom schedule: {args.custom}")
            try:
                scheduler.start(mode=f"custom:{args.custom}")
            except ValueError as e:
                print(f"❌ Invalid cron expression: {e}")
                print("Format: \"minute hour day month day_of_week\"")
                print("Example: \"0 2 */7 * *\" (every 7 days at 2 AM)")
                return 1
        
        else:
            # Default weekly mode
            print("\n📅 Starting weekly scheduler mode...")
            print("Pipeline will run every 7 days at 2:00 AM")
            print("Next execution will be scheduled automatically")
            scheduler.start(mode="weekly")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n🛑 Received interrupt signal, stopping scheduler...")
        return 0
    except Exception as e:
        print(f"\n❌ Scheduler failed to start: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())