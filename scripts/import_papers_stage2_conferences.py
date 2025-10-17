#!/usr/bin/env python3
"""
Stage 2: Filter conference papers from all_papers to dataset_papers table

Filters papers by conference venue from the all_papers base table and populates
the dataset_papers table with only conference papers.

Features:
- SQL-based filtering for efficiency
- Processes ALL data in all_papers (regardless of release_id)
- Conference matching with aliases support
- Batch processing for memory efficiency
- UPSERT logic for handling existing records

Performance:
- SQL-based filtering is very fast
- Typically completes in a few minutes
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.database.connection import DatabaseManager
from src.semantic.database.schemas.dataset_paper import DatasetPaperSchema
from src.semantic.services.dataset_service.conference_filter_service import ConferenceFilterService


def setup_database_tables(db_manager: DatabaseManager) -> bool:
    """Setup database tables if they don't exist"""
    print("\n=== Setting up database tables ===")

    try:
        # Create dataset_papers table (conference papers)
        paper_schema = DatasetPaperSchema(db_manager)
        if not paper_schema.create_table():
            print("Error: Failed to create dataset_papers table")
            return False

        print("✓ Database tables ready")
        return True

    except Exception as e:
        print(f"Error setting up database tables: {e}")
        return False


def get_release_id_from_all_papers(db_manager: DatabaseManager) -> str:
    """
    Get a release_id from all_papers table for recording purposes
    Note: This is only used for marking new records, not for filtering
    """
    try:
        query = "SELECT release_id FROM all_papers LIMIT 1"
        result = db_manager.fetch_one(query)
        if result:
            return result['release_id']
        else:
            print("Warning: No data found in all_papers table")
            return "unknown"
    except Exception as e:
        print(f"Warning: Could not get release_id from all_papers: {e}")
        return "unknown"


def filter_conferences(args, db_manager: DatabaseManager):
    """Filter conference papers from all_papers to dataset_papers"""
    print(f"\n{'='*80}")
    print("STAGE 2: Filtering conference papers")
    print(f"{'='*80}")

    # Get release_id for recording (not for filtering!)
    release_id = get_release_id_from_all_papers(db_manager)
    print(f"Using release_id for new records: {release_id}")
    print("Note: Processing ALL data in all_papers (not filtering by release_id)")

    # Create filter service
    filter_service = ConferenceFilterService(db_manager, release_id)

    # Filter and populate
    stats = filter_service.filter_and_populate_dataset_papers(batch_size=args.batch_size)

    return stats


def print_statistics(stats: dict):
    """Print filtering statistics"""
    print("\n" + "="*80)
    print("=== Filtering Completed ===")
    print("="*80)
    print(f"Status: {stats.get('status', 'unknown')}")

    if 'total_matched' in stats:
        print(f"Papers matched: {stats['total_matched']:,}")
    if 'total_inserted' in stats:
        print(f"Papers inserted (new): {stats['total_inserted']:,}")
    if 'total_updated' in stats:
        print(f"Papers updated (existing): {stats['total_updated']:,}")

    if 'processing_time_seconds' in stats:
        time_sec = stats['processing_time_seconds']
        print(f"Processing time: {time_sec:.2f}s ({time_sec/60:.2f} minutes)")

    print("="*80)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Stage 2: Filter conference papers from all_papers to dataset_papers table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Stage 2: Filter Conference Papers

This script filters papers by conference venue from the all_papers base table
and populates the dataset_papers table with only conference papers.

IMPORTANT: This script processes ALL data in the all_papers table, regardless
of release_id. It does not perform incremental filtering - it always works with
the complete current state of the all_papers table.

The release_id stored in new records is obtained from the all_papers table and
is used only for tracking purposes, not for filtering.

Examples:
  # Run with default settings (batch size: 10,000)
  %(prog)s

  # Adjust batch size for memory efficiency
  %(prog)s --batch-size 5000

  # Larger batch size for faster processing (if you have enough RAM)
  %(prog)s --batch-size 20000
        """
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for processing papers (default: 10,000)'
    )

    args = parser.parse_args()

    # Initialize database
    db_manager = DatabaseManager()

    # Test connection
    if not db_manager.test_connection():
        print("Error: Database connection failed")
        return 1

    print("✓ Database connection successful")

    # Setup database tables
    if not setup_database_tables(db_manager):
        return 1

    try:
        # Filter conferences
        stats = filter_conferences(args, db_manager)

        if stats.get('status') != 'completed':
            print("\nError: Filtering failed")
            return 1

        print_statistics(stats)
        return 0

    except Exception as e:
        print(f"\nError: Filtering failed - {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
