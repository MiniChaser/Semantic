#!/usr/bin/env python3
"""
S2 Dataset Import Script
Downloads and processes Semantic Scholar dataset papers, filtering by conference

Features:
- Downloads latest S2 dataset release
- Filters papers by 66 conferences from csconferences.csv
- UPSERT logic: keeps only latest release_id for each corpus_id
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
from src.semantic.services.s2_service.s2_dataset_downloader import S2DatasetDownloader
from src.semantic.services.dataset_service.s2_dataset_processor_pandas import S2DatasetProcessorPandas


def setup_database_tables(db_manager: DatabaseManager) -> bool:
    """Setup database tables if they don't exist"""
    print("\n=== Setting up database tables ===")

    try:
        # Create dataset_release table
        release_schema = DatasetReleaseSchema(db_manager)
        if not release_schema.create_table():
            print("Error: Failed to create dataset_release table")
            return False

        # Create dataset_papers table
        paper_schema = DatasetPaperSchema(db_manager)
        if not paper_schema.create_table():
            print("Error: Failed to create dataset_papers table")
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


def process_dataset(args, db_manager: DatabaseManager, release_repo: DatasetReleaseRepository, release_id: str):
    """Process dataset files"""
    print(f"\n=== Processing Dataset (Release: {release_id}) ===")

    # Create processor (using original stable processor)
    processor = S2DatasetProcessorPandas(db_manager, release_id)

    # Process files
    stats = processor.process_dataset_files(args.data_dir)

    return stats


def print_statistics(stats: dict, release_id: str):
    """Print processing statistics"""
    print("\n" + "="*80)
    print("=== Processing Completed ===")
    print("="*80)
    print(f"Release ID: {stats['release_id']}")
    print(f"Status: {stats['status']}")
    print(f"Files processed: {stats['total_files']}")
    print(f"Papers processed: {stats.get('total_papers_processed', 0):,}")
    print(f"Papers matched: {stats.get('papers_matched', 0):,}")
    print(f"Papers inserted (new): {stats.get('papers_inserted', 0):,}")
    print(f"Papers updated (existing): {stats.get('papers_updated', 0):,}")
    print(f"Processing time: {stats['processing_time_seconds']:.2f}s ({stats['processing_time_seconds']/60:.2f} minutes)")
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

    # Phase 1: Download
    if not args.process_only:
        release_id, download_result = await download_dataset(args, release_repo)

        if not release_id:
            return 1

        if args.download_only:
            print("\n✓ Download completed (--download-only flag set)")
            return 0

    # Phase 2: Process
    if not args.download_only:
        if not release_id:
            # Get latest downloaded release
            latest = release_repo.get_latest_release(args.dataset_name)
            if not latest:
                print("Error: No release found. Please download first.")
                return 1
            release_id = latest.release_id
            print(f"Using existing release: {release_id}")

        stats = process_dataset(args, db_manager, release_repo, release_id)

        if stats.get('status') == 'completed':
            print_statistics(stats, release_id)
            return 0
        else:
            print(f"\nError: Processing failed - {stats.get('error')}")
            return 1


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Import S2 dataset papers filtered by conference',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full workflow (download + process)
  %(prog)s

  # Download only
  %(prog)s --download-only

  # Process existing downloaded files
  %(prog)s --process-only --data-dir downloads/

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
        help='Only process existing downloaded files'
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

    args = parser.parse_args()

    # Validate arguments
    if args.download_only and args.process_only:
        print("Error: Cannot specify both --download-only and --process-only")
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
