#!/usr/bin/env python3
"""
Stage 1: Import ALL papers from S2 dataset to dataset_all_papers table

Downloads and imports all papers (200M records) from Semantic Scholar dataset
to the dataset_all_papers base table with optimized bulk import performance.

Features:
- Downloads latest S2 dataset release (optional)
- Optimized bulk import with index management (3-5x faster)
- Async pipeline processing with configurable parallelism
- Resume support for interrupted imports
- Configurable chunk size and pipeline depth
- AUTOMATIC venue_normalized computation during import (using venue_mapping table)

Performance:
- Expected speed: 18,000-30,000 papers/second (with venue normalization)
- 200M records: ~2.5-3.5 hours (with optimizations + inline venue normalization)

Venue Normalization:
- Loads 90K venue_mapping table into memory (~10-20MB)
- Computes venue_normalized inline during parsing (O(1) lookup)
- Achieves 85-90% coverage with exact matching
- No separate post-processing step required
- Run populate_venue_normalized.py only if needed for repairs
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.database.connection import DatabaseManager
from src.semantic.database.repositories.dataset_release import DatasetReleaseRepository
from src.semantic.database.models.dataset_release import DatasetRelease
from src.semantic.database.schemas.dataset_release import DatasetReleaseSchema
from src.semantic.database.schemas.all_papers import AllPapersSchema
from src.semantic.services.s2_service.s2_dataset_downloader import S2DatasetDownloader
from src.semantic.services.dataset_service.s2_all_papers_processor import S2AllPapersProcessor


def setup_database_tables(db_manager: DatabaseManager) -> bool:
    """Setup database tables if they don't exist"""
    print("\n=== Setting up database tables ===")

    try:
        # Create dataset_release table
        release_schema = DatasetReleaseSchema(db_manager)
        if not release_schema.create_table():
            print("Error: Failed to create dataset_release table")
            return False

        # Create dataset_all_papers table (base table for all 200M papers)
        all_papers_schema = AllPapersSchema(db_manager)
        if not all_papers_schema.create_table():
            print("Error: Failed to create dataset_all_papers table")
            return False

        print("✓ Database tables ready")
        return True

    except Exception as e:
        print(f"Error setting up database tables: {e}")
        return False


async def download_dataset(args, release_repo: DatasetReleaseRepository) -> tuple:
    """Download dataset and create release record"""
    print("\n=== Downloading S2 Dataset ===")

    downloader = S2DatasetDownloader()

    # Get release information
    release_info = downloader.get_latest_release_info()

    if not release_info:
        print("Error: Failed to get release information")
        return None, None

    release_id = release_info.get('release_id')
    release_date_str = release_info.get('release_date')

    print(f"Release ID: {release_id}")
    print(f"Release Date: {release_date_str}")

    # Parse release date
    release_date = None
    if release_date_str:
        try:
            release_date = datetime.fromisoformat(release_date_str.replace('Z', '+00:00'))
        except:
            pass

    # Create release record
    release = DatasetRelease(
        release_id=release_id,
        dataset_name=args.dataset_name,
        release_date=release_date,
        description=f"S2 {args.dataset_name} dataset",
        file_count=0,
        processing_status='downloading',
        download_start_time=datetime.now()
    )

    release_repo.create_release_record(release)

    # Download dataset
    print(f"\nDownloading dataset: {args.dataset_name}")
    print(f"Target directory: {args.data_dir}")

    download_result = await downloader.download_dataset(args.dataset_name, args.data_dir)

    if not download_result.get('success'):
        print(f"Error: Download failed - {download_result.get('error')}")
        release_repo.update_release_status(release_id, 'failed', download_end_time=datetime.now())
        return None, None

    # Update release status
    release_repo.update_release_status(
        release_id,
        'downloaded',
        download_end_time=datetime.now(),
        file_count=download_result.get('file_count', 0)
    )

    print(f"\n✓ Download completed: {download_result.get('downloaded_count')} files")

    return release_id, download_result


async def import_all_papers(args, db_manager: DatabaseManager, release_id: str):
    """Import ALL papers to all_papers table with optimizations"""
    print(f"\n{'='*80}")
    print("STAGE 1: Importing ALL papers with OPTIMIZED FAST IMPORT MODE")
    print(f"{'='*80}")

    # Set process nice priority to reduce system impact
    if args.nice_priority:
        try:
            os.nice(args.nice_priority)
            print(f"\n✓ Process nice priority set to: {args.nice_priority} (lower system priority)")
        except Exception as e:
            print(f"⚠️  Warning: Could not set nice priority: {e}")

    # Clear dataset_all_papers table before import (unless skip-truncate or resume flag is set)
    if args.resume:
        print("\n📝 Resume mode: Skipping files already in database...")
        print("   (Will not truncate table)")
    elif not args.skip_truncate:
        print("\n⚠️  Clearing dataset_all_papers table before import...")
        try:
            db_manager.execute_query("TRUNCATE TABLE dataset_all_papers CASCADE;")
            print("✓ dataset_all_papers table truncated successfully")
        except Exception as e:
            print(f"Error truncating table: {e}")
            print("Note: If table doesn't exist, it will be created during import.")
    else:
        print("\n⚠️  Skip truncate mode: Existing data will be preserved")

    # OPTIMIZATION: Drop indexes before bulk import for 3-5x speed improvement
    print("\n🚀 OPTIMIZATION: Dropping indexes for ultra-fast bulk insert...")
    all_papers_schema = AllPapersSchema(db_manager)
    if not all_papers_schema.drop_indexes():
        print("⚠️  Warning: Failed to drop indexes, continuing anyway...")

    # Create processor with optional max_workers override
    max_workers = None
    if args.conservative:
        max_workers = 1
        print("\n🐌 Conservative mode: Using 1 worker (minimal resource usage)")
    elif args.max_workers:
        max_workers = args.max_workers
        print(f"\n⚙️  Manual worker limit: {max_workers} workers")

    processor = S2AllPapersProcessor(db_manager, release_id, max_workers=max_workers)

    # Process files with async pipeline (fast mode - no UPSERT)
    stats = await processor.process_dataset_files(
        args.data_dir,
        pipeline_depth=args.pipeline_depth,
        chunk_size=args.chunk_size,
        resume=args.resume
    )

    # OPTIMIZATION: Recreate indexes after bulk import
    if stats.get('status') == 'completed':
        print("\n🔨 Rebuilding indexes (2 essential indexes: corpus_id, venue_normalized)...")
        print("This may take 20-30 minutes for 200M records...")

        if not all_papers_schema.recreate_indexes():
            print("⚠️  Warning: Failed to recreate some indexes")
            stats['index_recreation_failed'] = True
        else:
            print("✓ All indexes recreated successfully")

        # Note: venue_normalized is now populated automatically during import
        print("\n✓ venue_normalized computed automatically during import")
        print("   Using venue_mapping table (85-90% coverage with exact matching)")
        print("   Run populate_venue_normalized.py only if repairs are needed")

    return stats


def print_statistics(stats: dict):
    """Print import statistics"""
    print("\n" + "="*80)
    print("=== Import Completed ===")
    print("="*80)
    print(f"Status: {stats.get('status', 'unknown')}")

    if 'total_files' in stats:
        print(f"Files processed: {stats['total_files']}")
    if 'total_papers_processed' in stats:
        print(f"Papers processed: {stats['total_papers_processed']:,}")
    if 'papers_inserted' in stats:
        print(f"Papers inserted: {stats['papers_inserted']:,}")

    if 'processing_time_seconds' in stats:
        time_sec = stats['processing_time_seconds']
        print(f"Processing time: {time_sec:.2f}s ({time_sec/60:.2f} minutes)")

    print("="*80)


async def main_async(args):
    """Main async function"""
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

    release_repo = DatasetReleaseRepository(db_manager)
    release_id = None

    try:
        # Download phase (if needed)
        if not args.process_only:
            release_id, download_result = await download_dataset(args, release_repo)

            if not release_id:
                return 1

            if args.download_only:
                print("\n✓ Download completed (--download-only flag set)")
                return 0

        # Get release_id for processing
        if not release_id:
            # Get latest downloaded release
            latest = release_repo.get_latest_release(args.dataset_name)
            if not latest:
                print("Error: No release found. Please download first or run without --process-only.")
                return 1
            release_id = latest.release_id
            print(f"Using existing release: {release_id}")

        # Import phase
        stats = await import_all_papers(args, db_manager, release_id)

        if stats.get('status') != 'completed':
            print(f"\nError: Import failed - {stats.get('error')}")
            return 1

        print_statistics(stats)
        return 0

    except Exception as e:
        print(f"\nError: Import failed - {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Stage 1: Import all papers from S2 dataset to dataset_all_papers table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Stage 1: Import ALL Papers (Resource-Optimized)

This script downloads and imports all papers (200M records) from Semantic Scholar
dataset to the dataset_all_papers base table with optimized bulk import performance.

RESOURCE OPTIMIZATIONS (conservative for system stability):
  - Multi-worker parallel DB inserts (auto-detect: CPU cores * 0.25, max 8 workers)
  - Lower process priority (nice=10) to prevent SSH connection issues
  - Connection pool reuse for reduced overhead
  - Parallel JSON serialization with multiprocessing (max 2 workers)
  - Drops all indexes before bulk insert
  - Uses optimized chunk_size (200k) and auto-adaptive queue depth
  - Creates only 2 essential indexes (corpus_id, venue_normalized)
  - Skips unnecessary indexes (year, release_id, citation_count, paper_id, authors GIN)

IMPORTANT: This script will TRUNCATE the dataset_all_papers table before importing
(use --skip-truncate to prevent this).

Resume Support: Use --resume to automatically skip files that are already
in the database (based on source_file field). Perfect for interrupted imports!

Resource Control Options:
  - Use --conservative for minimal resource usage (1 worker, prevents SSH issues)
  - Use --max-workers N to manually set worker count
  - Use --nice-priority N (0-19) to adjust process priority
  - Default chunk size reduced to 200k for lower memory usage

Examples:
  # Conservative mode (recommended to prevent SSH issues)
  %(prog)s --process-only --data-dir downloads/ --conservative

  # Custom worker count (2-4 workers recommended for stability)
  %(prog)s --process-only --data-dir downloads/ --max-workers 2

  # Resume interrupted import (smart file skipping)
  %(prog)s --process-only --data-dir downloads/ --resume

  # Lower priority (won't block SSH, default is already 10)
  %(prog)s --process-only --data-dir downloads/ --nice-priority 15

  # Full control
  %(prog)s --process-only --data-dir downloads/ --max-workers 4 --chunk-size 100000 --nice-priority 10

  # Custom dataset and directory
  %(prog)s --dataset-name abstracts --data-dir /path/to/data/
        """
    )

    parser.add_argument(
        '--download-only',
        action='store_true',
        help='Only download dataset, do not process'
    )

    parser.add_argument(
        '--process-only',
        action='store_true',
        help='Only process existing downloaded files (skip download)'
    )

    parser.add_argument(
        '--skip-truncate',
        action='store_true',
        help='Skip truncating dataset_all_papers table before import (default: truncate)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume interrupted import by skipping already processed files (smart mode)'
    )

    parser.add_argument(
        '--data-dir',
        default='downloads/',
        help='Data directory for downloaded files (default: downloads/)'
    )

    parser.add_argument(
        '--dataset-name',
        default='papers',
        help='S2 dataset name (default: papers, options: papers, abstracts, etc.)'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=200000,
        help='Number of papers per chunk for processing (default: 200,000, reduced for lower memory usage)'
    )

    parser.add_argument(
        '--pipeline-depth',
        type=int,
        default=None,
        help='Async pipeline queue depth (default: auto = workers * 2, recommended for optimal performance)'
    )

    parser.add_argument(
        '--max-workers',
        type=int,
        default=None,
        help='Maximum number of parallel workers (default: auto = 25%% of CPU cores, max 8)'
    )

    parser.add_argument(
        '--conservative',
        action='store_true',
        help='Conservative mode: Use minimal resources (1 worker, lower priority). Recommended to prevent SSH issues.'
    )

    parser.add_argument(
        '--nice-priority',
        type=int,
        default=10,
        help='Process nice priority (0-19, higher = lower priority, default: 10). Set to 0 to disable.'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.download_only and args.process_only:
        print("Error: Cannot specify both --download-only and --process-only")
        return 1

    if args.resume and args.skip_truncate:
        print("Error: Cannot specify both --resume and --skip-truncate (resume implies skip-truncate)")
        return 1

    if args.conservative and args.max_workers:
        print("Error: Cannot specify both --conservative and --max-workers")
        return 1

    if args.max_workers and (args.max_workers < 1 or args.max_workers > 32):
        print("Error: --max-workers must be between 1 and 32")
        return 1

    if args.nice_priority < 0 or args.nice_priority > 19:
        print("Error: --nice-priority must be between 0 and 19")
        return 1

    # Run async main
    try:
        exit_code = asyncio.run(main_async(args))
        return exit_code
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return 130
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
