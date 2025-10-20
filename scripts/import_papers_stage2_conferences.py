#!/usr/bin/env python3
"""
Stage 2: Filter conference papers from all_papers to dataset_papers table

Filters papers by conference venue from the all_papers base table and populates
the dataset_papers table with only conference papers.

Performance Optimization:
- By default, drops indexes before bulk insert and rebuilds after (5-10x faster!)
- For 17M records: ~2-3 hours total (vs 10+ hours with indexes)

Features:
- Intelligent index management for optimal performance
- SQL-based filtering for efficiency
- Processes ALL data in all_papers (regardless of release_id)
- Conference matching with aliases support
- Batch processing for memory efficiency
- Complete timing statistics and throughput metrics

Usage:
  # Normal mode (recommended - drops/rebuilds indexes)
  python import_papers_stage2_conferences.py

  # Skip index rebuild (rebuild manually later)
  python import_papers_stage2_conferences.py --skip-rebuild

  # Keep indexes during insert (slower, use for incremental updates)
  python import_papers_stage2_conferences.py --keep-indexes
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.database.connection import DatabaseManager
from src.semantic.database.schemas.dataset_paper import DatasetPaperSchema
from src.semantic.services.dataset_service.conference_filter_service import ConferenceFilterService


def setup_database_tables(db_manager: DatabaseManager, drop_indexes: bool = True) -> bool:
    """Setup database tables if they don't exist"""
    print("\n=== Setting up database tables ===")

    try:
        # Create dataset_papers table (conference papers)
        paper_schema = DatasetPaperSchema(db_manager)

        # Check if table exists and has data
        count_query = "SELECT COUNT(*) as count FROM dataset_papers"
        try:
            result = db_manager.fetch_one(count_query)
            has_data = result and result.get('count', 0) > 0
            if has_data:
                print(f"⚠️  dataset_papers table already has {result['count']:,} records")
                print("   Continuing will UPSERT (update existing, insert new)")
        except:
            has_data = False

        if not paper_schema.create_table():
            print("Error: Failed to create dataset_papers table")
            return False

        # Drop indexes if requested (for performance)
        if drop_indexes:
            print("\n=== Performance Optimization ===")
            print("Dropping indexes for faster bulk insert...")
            print("⚠️  This will make the import 5-10x faster!")
            print("⚠️  Indexes will be recreated automatically after import")

            # Check if indexes exist
            if paper_schema.check_indexes_exist():
                if not paper_schema.drop_indexes():
                    print("⚠️  Warning: Failed to drop indexes, continuing with slower import")
                else:
                    print("✓ Indexes dropped successfully")
            else:
                print("✓ No indexes to drop (table is empty or indexes don't exist)")

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


def rebuild_indexes(db_manager: DatabaseManager) -> bool:
    """Rebuild indexes after bulk insert"""
    print(f"\n{'='*80}")
    print("Rebuilding indexes...")
    print(f"{'='*80}")
    print("⏰ This will take 30-90 minutes for 17M records...")
    print("   You can:")
    print("   1. Wait for completion (recommended)")
    print("   2. Run in background and check later")
    print("   3. Skip and rebuild manually later (--skip-rebuild)")

    try:
        paper_schema = DatasetPaperSchema(db_manager)

        rebuild_start = datetime.now()
        if not paper_schema.recreate_indexes():
            print("❌ Failed to recreate indexes")
            return False

        rebuild_time = (datetime.now() - rebuild_start).total_seconds()
        print(f"\n✓ Indexes rebuilt successfully in {rebuild_time:.2f}s ({rebuild_time/60:.2f} minutes)")
        return True

    except Exception as e:
        print(f"❌ Error rebuilding indexes: {e}")
        return False


def print_statistics(stats: dict, total_time_seconds: float = None):
    """Print filtering statistics"""
    print("\n" + "="*80)
    print("=== Filtering Completed ===")
    print("="*80)
    print(f"Status: {stats.get('status', 'unknown')}")

    if 'total_matched' in stats:
        print(f"Papers matched: {stats['total_matched']:,}")
    if 'total_inserted' in stats:
        print(f"Papers upserted: {stats['total_inserted']:,}")
    if 'total_updated' in stats:
        print(f"Papers updated (existing): {stats['total_updated']:,}")

    if 'processing_time_seconds' in stats:
        time_sec = stats['processing_time_seconds']
        print(f"Insert time: {time_sec:.2f}s ({time_sec/60:.2f} minutes)")

    if total_time_seconds:
        print(f"Total execution time: {total_time_seconds:.2f}s ({total_time_seconds/60:.2f} minutes, {total_time_seconds/3600:.2f} hours)")

        # Calculate throughput
        if 'total_matched' in stats and total_time_seconds > 0:
            throughput = stats['total_matched'] / total_time_seconds
            print(f"Throughput: {throughput:.2f} records/second ({throughput*60:.2f} records/minute)")

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

Performance Optimization:
By default, this script drops indexes before bulk insert and rebuilds them after,
resulting in 5-10x faster performance:
- With index optimization:  ~2-3 hours for 17M records (recommended)
- Without optimization:     ~10-11 hours for 17M records

Process:
1. Drop all 7 indexes from dataset_papers table (if not --keep-indexes)
2. Bulk insert conference papers from all_papers (uses venue_normalized index)
3. Rebuild all indexes in one go (more efficient than per-row updates)

IMPORTANT: This script processes ALL data in the all_papers table, regardless
of release_id. It does not perform incremental filtering.

Examples:
  # Normal mode (recommended - with index optimization)
  %(prog)s

  # Adjust batch size
  %(prog)s --batch-size 20000

  # Skip index rebuild (rebuild manually later)
  %(prog)s --skip-rebuild

  # Keep indexes during insert (slower, for small incremental updates)
  %(prog)s --keep-indexes
        """
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for processing papers (default: 10,000)'
    )

    parser.add_argument(
        '--skip-rebuild',
        action='store_true',
        help='Skip index rebuild after insert (you must rebuild manually later!)'
    )

    parser.add_argument(
        '--keep-indexes',
        action='store_true',
        help='Keep indexes during insert (slower, but safer - same as original script)'
    )

    args = parser.parse_args()

    # Validate options
    if args.skip_rebuild and args.keep_indexes:
        print("Error: --skip-rebuild and --keep-indexes are mutually exclusive")
        return 1

    # Start total timer
    script_start = datetime.now()

    # Initialize database
    db_manager = DatabaseManager()

    # Test connection
    if not db_manager.test_connection():
        print("Error: Database connection failed")
        return 1

    print("✓ Database connection successful")

    # Setup database tables
    drop_indexes = not args.keep_indexes
    if not setup_database_tables(db_manager, drop_indexes=drop_indexes):
        return 1

    try:
        # Filter conferences
        stats = filter_conferences(args, db_manager)

        if stats.get('status') != 'completed':
            print("\nError: Filtering failed")
            return 1

        # Rebuild indexes if requested
        rebuild_success = True
        if drop_indexes and not args.skip_rebuild:
            rebuild_success = rebuild_indexes(db_manager)

        # Calculate total time
        total_time = (datetime.now() - script_start).total_seconds()

        # Print final statistics
        print_statistics(stats, total_time)

        # Print warnings
        if drop_indexes and args.skip_rebuild:
            print("\n⚠️  WARNING: Indexes were dropped but not rebuilt!")
            print("   Run this to rebuild indexes manually:")
            print("   python -c \"from src.semantic.database.connection import DatabaseManager; from src.semantic.database.schemas.dataset_paper import DatasetPaperSchema; dm = DatabaseManager(); ds = DatasetPaperSchema(dm); ds.recreate_indexes()\"")

        if not rebuild_success:
            print("\n⚠️  WARNING: Index rebuild failed!")
            print("   dataset_papers table will have SLOW queries until indexes are recreated")
            print("   Try running the rebuild command manually")
            return 1

        # Success
        print("\n✅ Stage 2 completed successfully!")
        if drop_indexes and not args.skip_rebuild:
            print("✅ All indexes have been recreated")
        print(f"✅ Total time: {total_time/60:.2f} minutes ({total_time/3600:.2f} hours)")

        return 0

    except Exception as e:
        print(f"\nError: Filtering failed - {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
