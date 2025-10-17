#!/usr/bin/env python3
"""
Stage 3: Extract author papers from all_papers to dataset_author_papers table

Extracts all unique authors from dataset_papers (conference papers), then finds
all papers by these authors in the all_papers table and populates the
dataset_author_papers table.

Features:
- Extracts unique authors from conference papers (dataset_papers)
- Finds all papers by these authors in all_papers (regardless of release_id)
- Marks which papers are conference papers (is_conference_paper flag)
- Batch processing for memory efficiency
- UPSERT logic for handling existing records

Performance:
- SQL and JSONB-based queries
- Typically completes in tens of minutes
- Performance depends on number of authors and their paper counts
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.database.connection import DatabaseManager
from src.semantic.database.schemas.dataset_author_papers import DatasetAuthorPapersSchema
from src.semantic.services.dataset_service.author_papers_extractor import AuthorPapersExtractor


def setup_database_tables(db_manager: DatabaseManager) -> bool:
    """Setup database tables if they don't exist"""
    print("\n=== Setting up database tables ===")

    try:
        # Create dataset_author_papers table
        author_papers_schema = DatasetAuthorPapersSchema(db_manager)
        if not author_papers_schema.create_table():
            print("Error: Failed to create dataset_author_papers table")
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


def extract_author_papers(args, db_manager: DatabaseManager):
    """Extract author papers from all_papers to dataset_author_papers"""
    print(f"\n{'='*80}")
    print("STAGE 3: Extracting author papers")
    print(f"{'='*80}")

    # Get release_id for recording (not for filtering!)
    release_id = get_release_id_from_all_papers(db_manager)
    print(f"Using release_id for new records: {release_id}")
    print("Note: Processing ALL data in all_papers (not filtering by release_id)")

    # Create extractor service
    extractor = AuthorPapersExtractor(db_manager, release_id)

    # Extract and populate
    stats = extractor.extract_and_populate_author_papers(batch_size=args.batch_size)

    return stats


def print_statistics(stats: dict):
    """Print extraction statistics"""
    print("\n" + "="*80)
    print("=== Extraction Completed ===")
    print("="*80)
    print(f"Status: {stats.get('status', 'unknown')}")

    if 'total_authors' in stats:
        print(f"Authors processed: {stats['total_authors']:,}")
    if 'total_papers_found' in stats:
        print(f"Papers found: {stats['total_papers_found']:,}")
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
        description='Stage 3: Extract author papers from all_papers to dataset_author_papers table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Stage 3: Extract Author Papers

This script extracts all unique authors from dataset_papers (conference papers),
then finds all papers by these authors in the all_papers table and populates
the dataset_author_papers table.

IMPORTANT: This script processes ALL data in the all_papers table, regardless
of release_id. It does not perform incremental extraction - it always works with
the complete current state of the all_papers table.

The release_id stored in new records is obtained from the all_papers table and
is used only for tracking purposes, not for filtering.

Process:
1. Extract unique author IDs from dataset_papers (conference papers)
2. For each author, find ALL their papers in all_papers table
3. Mark which papers are conference papers (is_conference_paper = true/false)
4. Populate dataset_author_papers table with results

Examples:
  # Run with default settings (batch size: 100 authors)
  %(prog)s

  # Adjust batch size for memory efficiency
  %(prog)s --batch-size 50

  # Larger batch size for faster processing (if you have enough RAM)
  %(prog)s --batch-size 200
        """
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for processing authors (default: 100)'
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
        # Extract author papers
        stats = extract_author_papers(args, db_manager)

        if stats.get('status') != 'completed':
            print("\nError: Extraction failed")
            return 1

        print_statistics(stats)
        return 0

    except Exception as e:
        print(f"\nError: Extraction failed - {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
