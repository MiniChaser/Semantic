#!/usr/bin/env python3
"""
Simple Data Processor
Single entry point for all data processing operations
Replaces complex scheduler with simple, cron-friendly script

Usage:
- Scheduled execution: uv run python scripts/simple_data_processor.py
- Force execution: uv run python scripts/simple_data_processor.py --force
- Check status: uv run python scripts/simple_data_processor.py --status
- Manual phases: uv run python scripts/simple_data_processor.py --phase dblp
"""

import sys
import os
import argparse
import asyncio
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.simple_pipeline_service import SimplePipelineService


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Simple Data Processing Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Regular scheduled execution (checks if should run)
  uv run python scripts/simple_data_processor.py

  # Force execution regardless of last run time
  uv run python scripts/simple_data_processor.py --force

  # Check pipeline status
  uv run python scripts/simple_data_processor.py --status

  # Run specific phase only (for testing)
  uv run python scripts/simple_data_processor.py --phase dblp
  uv run python scripts/simple_data_processor.py --phase s2
  uv run python scripts/simple_data_processor.py --phase authors
  uv run python scripts/simple_data_processor.py --phase pdf
        """)

    parser.add_argument('--force', action='store_true',
                       help='Force execution regardless of last run time')
    parser.add_argument('--status', action='store_true',
                       help='Show pipeline status and exit')
    parser.add_argument('--phase', choices=['dblp', 's2', 'authors', 'pdf'],
                       help='Run specific phase only (for testing)')

    args = parser.parse_args()

    try:
        print("🚀 Simple Data Processing Pipeline")
        print("=" * 60)

        # Load configuration
        print("Loading configuration...")
        config = AppConfig.from_env()

        # Validate configuration
        if not config.validate():
            print("❌ Configuration validation failed, please check environment variables")
            return 1

        print("✅ Configuration loaded successfully")

        # Get database manager
        print("Connecting to database...")
        db_manager = get_db_manager()

        if not db_manager.test_connection():
            print("❌ Database connection test failed")
            return 1

        print("✅ Database connection established")

        # Create pipeline service
        pipeline_service = SimplePipelineService(config, db_manager)

        # Handle different modes
        if args.status:
            print("\n📊 Pipeline Status:")
            print("-" * 40)
            status = pipeline_service.get_pipeline_status()

            if 'error' in status:
                print(f"❌ Error getting status: {status['error']}")
                return 1

            # Display status information
            if status.get('latest_run'):
                latest = status['latest_run']
                print(f"Latest run: {latest['created_at']} - Status: {latest['status']}")

                if latest.get('error_message'):
                    print(f"Error: {latest['error_message']}")

            if status.get('last_successful_run'):
                print(f"Last successful run: {status['last_successful_run']}")

            if status.get('next_scheduled_run'):
                print(f"Next scheduled run: {status['next_scheduled_run']}")

            stats = status.get('statistics', {})
            if stats.get('total_operations', 0) > 0:
                print(f"\nLast 24h statistics:")
                for status_name, count in stats.get('by_status', {}).items():
                    print(f"  {status_name}: {count}")

            return 0

        elif args.phase:
            print(f"\n🔄 Running single phase: {args.phase}")
            print("Note: This is for testing only, use full pipeline for production")

            # Map phase names to methods
            phase_methods = {
                'dblp': pipeline_service._run_dblp_phase,
                's2': pipeline_service._run_s2_phase,
                'authors': pipeline_service._run_author_phase,
                'pdf': pipeline_service._run_pdf_phase
            }

            method = phase_methods[args.phase]

            # Create dummy metadata ID for single phase execution
            metadata_id = 0

            if asyncio.iscoroutinefunction(method):
                success, result = asyncio.run(method(metadata_id))
            else:
                success, result = method(metadata_id)

            if success:
                print(f"✅ Phase {args.phase} completed successfully")
                if result:
                    print(f"Result: {result}")
                return 0
            else:
                print(f"❌ Phase {args.phase} failed")
                if result and 'error' in result:
                    print(f"Error: {result['error']}")
                return 1

        else:
            # Full pipeline execution
            should_run = pipeline_service.should_run_pipeline(force=args.force)

            if not should_run:
                print("\n⏭️  Pipeline execution skipped (not enough time since last run)")
                print("Use --force to override this check")
                return 0

            print(f"\n🔄 Starting full pipeline execution...")
            if args.force:
                print("(Force mode enabled)")

            # Run the pipeline
            success = asyncio.run(pipeline_service.run_full_pipeline())

            if success:
                print(f"\n🎉 Pipeline execution completed successfully!")
                return 0
            else:
                print(f"\n❌ Pipeline execution failed!")
                return 1

    except KeyboardInterrupt:
        print("\n\n🛑 Received interrupt signal, stopping...")
        return 0
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())