#!/usr/bin/env python3
"""
S2 Dataset Import Script
Downloads and processes Semantic Scholar dataset papers with three-stage pipeline

Three-Stage Architecture:
1. Import ALL papers (200M) to all_papers table (no filtering)
2. Filter by conferences from all_papers to dataset_papers table
3. Extract authors and their papers to dataset_author_papers table

Features:
- Downloads latest S2 dataset release
- Three-table architecture for efficient processing
- UPSERT logic: keeps only latest release_id for each corpus_id
- SQL-based filtering (fast, no file re-parsing)
- Stage-based execution (can run stages independently)
- Tracks release version and processing statistics
"""

import argparse
import asyncio
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
from src.semantic.database.schemas.dataset_paper import DatasetPaperSchema
from src.semantic.database.schemas.all_papers import AllPapersSchema
from src.semantic.database.schemas.dataset_author_papers import DatasetAuthorPapersSchema
from src.semantic.services.s2_service.s2_dataset_downloader import S2DatasetDownloader
from src.semantic.services.dataset_service.s2_all_papers_processor import S2AllPapersProcessor
from src.semantic.services.dataset_service.conference_filter_service import ConferenceFilterService
from src.semantic.services.dataset_service.author_papers_extractor import AuthorPapersExtractor


def setup_database_tables(db_manager: DatabaseManager) -> bool:
    """Setup database tables if they don't exist"""
    print("\n=== Setting up database tables ===")

    try:
        # Create dataset_release table
        release_schema = DatasetReleaseSchema(db_manager)
        if not release_schema.create_table():
            print("Error: Failed to create dataset_release table")
            return False

        # Create all_papers table (base table for all 200M papers)
        all_papers_schema = AllPapersSchema(db_manager)
        if not all_papers_schema.create_table():
            print("Error: Failed to create all_papers table")
            return False

        # Create dataset_papers table (conference papers)
        paper_schema = DatasetPaperSchema(db_manager)
        if not paper_schema.create_table():
            print("Error: Failed to create dataset_papers table")
            return False

        # Create dataset_author_papers table (author papers)
        author_papers_schema = DatasetAuthorPapersSchema(db_manager)
        if not author_papers_schema.create_table():
            print("Error: Failed to create dataset_author_papers table")
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
    """Stage 1: Import ALL papers to all_papers table (no filtering)"""
    print(f"\n{'='*80}")
    print("STAGE 1: Importing ALL papers with OPTIMIZED FAST IMPORT MODE")
    print(f"{'='*80}")

    # Clear all_papers table before import (unless skip-truncate or resume flag is set)
    if args.resume:
        print("\n📝 Resume mode: Skipping files already in database...")
        print("   (Will not truncate table)")
    elif not args.skip_truncate:
        print("\n⚠️  Clearing all_papers table before import...")
        try:
            db_manager.execute_query("TRUNCATE TABLE all_papers CASCADE;")
            print("✓ all_papers table truncated successfully")
        except Exception as e:
            print(f"Error truncating table: {e}")
            print("Note: If table doesn't exist, it will be created during import.")
    else:
        print("\n⚠️  Skip truncate mode: Existing data will be preserved")

    # OPTIMIZATION: Drop indexes before bulk import for 5-10x speed improvement
    print("\n🚀 OPTIMIZATION: Dropping indexes for ultra-fast bulk insert...")
    all_papers_schema = AllPapersSchema(db_manager)
    if not all_papers_schema.drop_indexes():
        print("⚠️  Warning: Failed to drop indexes, continuing anyway...")

    # Create processor
    processor = S2AllPapersProcessor(db_manager, release_id)

    # Process files with async pipeline (fast mode - no UPSERT)
    # Use optimized chunk_size and pipeline_depth
    chunk_size = args.chunk_size if hasattr(args, 'chunk_size') else 500000
    pipeline_depth = args.pipeline_depth if hasattr(args, 'pipeline_depth') else 5

    stats = await processor.process_dataset_files(
        args.data_dir,
        pipeline_depth=pipeline_depth,
        chunk_size=chunk_size,
        resume=args.resume
    )

    # OPTIMIZATION: Recreate indexes after bulk import
    if stats.get('status') == 'completed':
        print("\n🔨 Rebuilding indexes (this may take 30-60 minutes for 200M records)...")
        if not all_papers_schema.recreate_indexes():
            print("⚠️  Warning: Failed to recreate some indexes")
            stats['index_recreation_failed'] = True
        else:
            print("✓ All indexes recreated successfully")

    return stats


def filter_conferences(args, db_manager: DatabaseManager, release_id: str):
    """Stage 2: Filter conference papers from all_papers to dataset_papers"""
    print(f"\n{'='*80}")
    print("STAGE 2: Filtering conference papers to dataset_papers table")
    print(f"{'='*80}")

    # Create filter service
    filter_service = ConferenceFilterService(db_manager, release_id)

    # Filter and populate
    stats = filter_service.filter_and_populate_dataset_papers()

    return stats


def extract_author_papers(args, db_manager: DatabaseManager, release_id: str):
    """Stage 3: Extract author papers from all_papers to dataset_author_papers"""
    print(f"\n{'='*80}")
    print("STAGE 3: Extracting author papers to dataset_author_papers table")
    print(f"{'='*80}")

    # Create extractor service
    extractor = AuthorPapersExtractor(db_manager, release_id)

    # Extract and populate
    stats = extractor.extract_and_populate_author_papers()

    return stats


def print_stage_statistics(stage_name: str, stats: dict):
    """Print statistics for a single stage"""
    print("\n" + "="*80)
    print(f"=== {stage_name} Completed ===")
    print("="*80)
    print(f"Status: {stats.get('status', 'unknown')}")

    if 'total_files' in stats:
        print(f"Files processed: {stats['total_files']}")
    if 'total_papers_processed' in stats:
        print(f"Papers processed: {stats['total_papers_processed']:,}")
    if 'papers_matched' in stats:
        print(f"Papers matched: {stats['papers_matched']:,}")
    if 'total_matched' in stats:
        print(f"Papers matched: {stats['total_matched']:,}")
    if 'papers_inserted' in stats:
        print(f"Papers inserted (new): {stats['papers_inserted']:,}")
    if 'total_inserted' in stats:
        print(f"Papers inserted (new): {stats['total_inserted']:,}")
    if 'papers_updated' in stats:
        print(f"Papers updated (existing): {stats['papers_updated']:,}")
    if 'total_updated' in stats:
        print(f"Papers updated (existing): {stats['total_updated']:,}")
    if 'total_authors' in stats:
        print(f"Authors processed: {stats['total_authors']:,}")
    if 'total_papers_found' in stats:
        print(f"Papers found: {stats['total_papers_found']:,}")

    if 'processing_time_seconds' in stats:
        time_sec = stats['processing_time_seconds']
        print(f"Processing time: {time_sec:.2f}s ({time_sec/60:.2f} minutes)")

    print("="*80)


def print_final_summary(all_stats: dict):
    """Print final summary of all stages"""
    print("\n" + "="*80)
    print("=== PIPELINE COMPLETED - FINAL SUMMARY ===")
    print("="*80)

    if 'stage1' in all_stats:
        print(f"\nStage 1 (Import All Papers):")
        print(f"  - Papers processed: {all_stats['stage1'].get('total_papers_processed', 0):,}")
        print(f"  - Papers inserted: {all_stats['stage1'].get('papers_inserted', 0):,}")
        print(f"  - Time: {all_stats['stage1'].get('processing_time_seconds', 0)/60:.2f} minutes")

    if 'stage2' in all_stats:
        print(f"\nStage 2 (Filter Conferences):")
        print(f"  - Papers matched: {all_stats['stage2'].get('total_matched', 0):,}")
        print(f"  - Papers inserted: {all_stats['stage2'].get('total_inserted', 0):,}")
        print(f"  - Time: {all_stats['stage2'].get('processing_time_seconds', 0)/60:.2f} minutes")

    if 'stage3' in all_stats:
        print(f"\nStage 3 (Extract Author Papers):")
        print(f"  - Authors processed: {all_stats['stage3'].get('total_authors', 0):,}")
        print(f"  - Papers found: {all_stats['stage3'].get('total_papers_found', 0):,}")
        print(f"  - Papers inserted: {all_stats['stage3'].get('total_inserted', 0):,}")
        print(f"  - Time: {all_stats['stage3'].get('processing_time_seconds', 0)/60:.2f} minutes")

    total_time = sum(
        all_stats.get(stage, {}).get('processing_time_seconds', 0)
        for stage in ['stage1', 'stage2', 'stage3']
    )
    print(f"\nTotal pipeline time: {total_time:.2f}s ({total_time/60:.2f} minutes)")
    print("="*80)


async def main_async(args):
    """Main async function with three-stage pipeline"""
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
    all_stats = {}

    # Phase 1: Download (if needed)
    if not args.process_only and not args.import_all and not args.filter_conferences and not args.extract_authors:
        release_id, download_result = await download_dataset(args, release_repo)

        if not release_id:
            return 1

        if args.download_only:
            print("\n✓ Download completed (--download-only flag set)")
            return 0

    # Get release_id for processing stages
    if not release_id:
        # Get latest downloaded release
        latest = release_repo.get_latest_release(args.dataset_name)
        if not latest:
            print("Error: No release found. Please download first or run without --process-only.")
            return 1
        release_id = latest.release_id
        print(f"Using existing release: {release_id}")

    # Determine which stages to run
    run_all_stages = not (args.import_all or args.filter_conferences or args.extract_authors)
    run_stage1 = run_all_stages or args.import_all
    run_stage2 = run_all_stages or args.filter_conferences
    run_stage3 = run_all_stages or args.extract_authors

    try:
        # Stage 1: Import all papers (async)
        if run_stage1 and not args.download_only:
            stats = await import_all_papers(args, db_manager, release_id)
            if stats.get('status') != 'completed':
                print(f"\nError: Stage 1 failed - {stats.get('error')}")
                return 1
            all_stats['stage1'] = stats
            print_stage_statistics("Stage 1: Import All Papers", stats)

        # Stage 2: Filter conferences
        if run_stage2 and not args.download_only:
            stats = filter_conferences(args, db_manager, release_id)
            if stats.get('status') != 'completed':
                print(f"\nError: Stage 2 failed")
                return 1
            all_stats['stage2'] = stats
            print_stage_statistics("Stage 2: Filter Conferences", stats)

        # Stage 3: Extract author papers
        if run_stage3 and not args.download_only:
            stats = extract_author_papers(args, db_manager, release_id)
            if stats.get('status') != 'completed':
                print(f"\nError: Stage 3 failed")
                return 1
            all_stats['stage3'] = stats
            print_stage_statistics("Stage 3: Extract Author Papers", stats)

        # Print final summary if multiple stages ran
        if len(all_stats) > 1:
            print_final_summary(all_stats)

        return 0

    except Exception as e:
        print(f"\nError: Pipeline failed - {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Import S2 dataset papers with three-stage pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Three-Stage Pipeline (Optimized for Fast First-Time Import):
  1. Import ALL papers (200M) to all_papers table (FAST MODE with index optimization)
  2. Filter by conferences from all_papers to dataset_papers table
  3. Extract authors and their papers to dataset_author_papers table

OPTIMIZATION: Stage 1 now uses index management for 3-5x faster import:
  - Drops all indexes before bulk insert
  - Uses optimized chunk_size (500k) and pipeline_depth (5)
  - Recreates indexes after import completes

IMPORTANT: This script is optimized for first-time import. It will TRUNCATE
the all_papers table before importing (use --skip-truncate to prevent this).

Resume Support: Use --resume to automatically skip files that are already
in the database (based on source_file field). Perfect for interrupted imports!

Examples:
  # Full pipeline (download + all 3 stages)
  %(prog)s

  # Download only
  %(prog)s --download-only

  # Run all 3 stages on existing files
  %(prog)s --process-only --data-dir downloads/

  # Run only Stage 1 (import all papers, with table truncation)
  %(prog)s --import-all --data-dir downloads/

  # Resume interrupted import (smart file skipping)
  %(prog)s --import-all --data-dir downloads/ --resume

  # Run only Stage 2 (filter conferences)
  %(prog)s --filter-conferences

  # Run only Stage 3 (extract author papers)
  %(prog)s --extract-authors

  # Run Stages 2 and 3 only
  %(prog)s --filter-conferences --extract-authors

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
        '--import-all',
        action='store_true',
        help='Run Stage 1 only: Import all papers to all_papers table'
    )

    parser.add_argument(
        '--filter-conferences',
        action='store_true',
        help='Run Stage 2 only: Filter conferences to dataset_papers table'
    )

    parser.add_argument(
        '--extract-authors',
        action='store_true',
        help='Run Stage 3 only: Extract author papers to dataset_author_papers table'
    )

    parser.add_argument(
        '--skip-truncate',
        action='store_true',
        help='Skip truncating all_papers table before import (default: truncate)'
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
        help='S2 dataset name (default: papers, options: papers, abstracts, authors, citations, etc.)'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=500000,
        help='Number of papers per chunk for processing (default: 500,000)'
    )

    parser.add_argument(
        '--pipeline-depth',
        type=int,
        default=5,
        help='Async pipeline queue depth (default: 5, higher = more parallelism)'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.download_only and args.process_only:
        print("Error: Cannot specify both --download-only and --process-only")
        return 1

    if args.download_only and (args.import_all or args.filter_conferences or args.extract_authors):
        print("Error: Cannot specify --download-only with stage flags")
        return 1

    if args.resume and args.skip_truncate:
        print("Error: Cannot specify both --resume and --skip-truncate (resume implies skip-truncate)")
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
