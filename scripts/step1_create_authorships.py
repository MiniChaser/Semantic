#!/usr/bin/env python3
"""
Author Processing Step 1: Create Authorships Table
Creates and populates the authorships table with paper-author relationships

Uses pandas-optimized processing mode for performance and COMPLETE data coverage
"""

import sys
import logging
import argparse
import time
from pathlib import Path
from typing import Dict

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.author_service.authorship_pandas_service import AuthorshipPandasService


def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step1_create_authorships.log')
        ]
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Create Authorships Table (Step 1) - Pandas Mode with Incremental Updates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Uses optimized pandas-based batch processing with intelligent update detection.
By default, only processes papers that need updates (incremental mode).

Examples:
  python step1_create_authorships.py                    # Incremental mode (default)
  python step1_create_authorships.py --full-mode        # Full rebuild mode
  python step1_create_authorships.py --verbose          # Incremental with verbose logging
  python step1_create_authorships.py --full-mode -v     # Full mode with verbose logging
        """
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--full-mode',
        action='store_true',
        help='Use full rebuild mode instead of incremental updates (processes all papers)'
    )

    return parser.parse_args()




def run_pandas_mode(db_manager, incremental_mode: bool = True) -> Dict:
    """Run using optimized AuthorshipPandasService with configurable update mode"""
    mode_desc = "incremental updates" if incremental_mode else "full coverage"
    print(f"Using pandas-optimized processing mode ({mode_desc})...")

    # Initialize optimized service with specified mode
    authorship_service = AuthorshipPandasService(db_manager, incremental_mode=incremental_mode)

    # Create authorships table
    if not authorship_service.create_authorships_table():
        return {'error': 'Failed to create authorships table'}
    print("Authorships table created/verified")

    # Populate authorships table using pandas optimization
    authorship_stats = authorship_service.populate_authorships_table_pandas()
    return authorship_stats


def main():
    """Execute Step 1: Create Authorships Table"""

    args = parse_arguments()

    # Determine update mode
    incremental_mode = not args.full_mode
    mode_desc = "Incremental Updates" if incremental_mode else "Full Rebuild"
    coverage_desc = "intelligent update detection" if incremental_mode else "COMPLETE data coverage"

    print(f"Step 1: Creating Authorships Table - Pandas Mode ({mode_desc})")
    print("=" * 60)
    print(f"Using pandas-optimized processing with {coverage_desc}")
    print("=" * 60)

    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)

        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.debug("Verbose logging enabled")

        # Load configuration and initialize database
        config = AppConfig.from_env()
        db_manager = get_db_manager()
        logger.info("Database connection established")
        print("Database connection established")

        # Record processing start time
        start_time = time.time()

        # Run pandas processing mode with specified update mode
        authorship_stats = run_pandas_mode(db_manager, incremental_mode)

        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time

        # Check for errors
        if 'error' in authorship_stats:
            print(f"Failed to populate authorships table: {authorship_stats['error']}")
            return 1

        # Display results
        print("\n" + "=" * 60)
        print("Authorships table populated successfully!")
        print("=" * 60)
        print(f"Update mode: {authorship_stats.get('update_mode', 'unknown')}")
        print(f"Papers processed: {authorship_stats['processed_papers']}")
        print(f"Total authorship records: {authorship_stats['total_authorships']}")
        print(f"Matched authors: {authorship_stats['matched_authors']}")
        print(f"Unmatched authors: {authorship_stats['unmatched_authors']}")

        if 'papers_updated' in authorship_stats:
            print(f"Papers updated: {authorship_stats['papers_updated']}")

        if 'optimization_method' in authorship_stats:
            print(f"Optimization method: {authorship_stats['optimization_method']}")

        if 'data_completeness' in authorship_stats:
            print(f"Data completeness: {authorship_stats['data_completeness']}")

        print(f"Processing time: {processing_time:.2f} seconds")

        # Show performance impact in incremental mode
        if incremental_mode and authorship_stats['processed_papers'] == 0:
            print("\n✅ All authorships are up to date - no processing needed!")
        elif incremental_mode and authorship_stats['processed_papers'] > 0:
            print(f"\n⚡ Incremental mode: Only processed {authorship_stats['processed_papers']} papers that needed updates")

        if args.verbose:
            print(f"\nDetailed statistics: {authorship_stats}")

        return 0

    except Exception as e:
        mode_desc = "incremental" if not args.full_mode else "full"
        print(f"Critical error in {mode_desc} mode: {e}")
        logging.getLogger(__name__).error(f"Step 1 failed ({mode_desc} mode): {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())