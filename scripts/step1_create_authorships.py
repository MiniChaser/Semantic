#!/usr/bin/env python3
"""
Author Processing Step 1: Create Authorships Table
Creates and populates the authorships table with paper-author relationships

Now supports both regular and pandas-optimized processing modes:
- Regular mode: Compatible with original implementation (limited coverage)
- Pandas mode: Optimized for performance and COMPLETE data coverage
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
from semantic.services.author_service.author_profile_service import AuthorProfileService
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
        description="Create Authorships Table (Step 1)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Processing modes:
  regular: Uses original implementation with pagination (limited coverage ~10%)
  pandas:  Uses optimized pandas-based batch processing with FULL coverage (recommended)

Examples:
  python step1_create_authorships.py --mode pandas
  python step1_create_authorships.py --mode regular --verbose
        """
    )

    parser.add_argument(
        '--mode',
        choices=['regular', 'pandas'],
        default='pandas',
        help='Processing mode (default: pandas)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    return parser.parse_args()


def run_regular_mode(db_manager) -> Dict:
    """Run using original AuthorProfileService (limited coverage)"""
    print("Using regular processing mode (limited coverage)...")

    # Initialize service
    profile_service = AuthorProfileService(db_manager)

    # Create authorships table
    if not profile_service.create_authorships_table():
        return {'error': 'Failed to create authorships table'}
    print("Authorships table created")

    # Populate authorships table
    authorship_stats = profile_service.populate_authorships_table()
    return authorship_stats


def run_pandas_mode(db_manager) -> Dict:
    """Run using optimized AuthorshipPandasService (full coverage)"""
    print("Using pandas-optimized processing mode (full coverage)...")

    # Initialize optimized service
    authorship_service = AuthorshipPandasService(db_manager)

    # Create authorships table
    if not authorship_service.create_authorships_table():
        return {'error': 'Failed to create authorships table'}
    print("Authorships table created")

    # Populate authorships table using pandas optimization
    authorship_stats = authorship_service.populate_authorships_table_pandas()
    return authorship_stats


def main():
    """Execute Step 1: Create Authorships Table"""

    args = parse_arguments()

    print("Step 1: Creating Authorships Table")
    print("=" * 50)
    print(f"Processing mode: {args.mode.upper()}")
    if args.mode == 'pandas':
        print("Note: Pandas mode provides COMPLETE data coverage (all papers)")
    else:
        print("Note: Regular mode has limited coverage (~10% of papers)")
    print("=" * 50)

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

        # Run appropriate processing mode
        if args.mode == 'pandas':
            authorship_stats = run_pandas_mode(db_manager)
        else:
            authorship_stats = run_regular_mode(db_manager)

        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time

        # Check for errors
        if 'error' in authorship_stats:
            print(f"Failed to populate authorships table: {authorship_stats['error']}")
            return 1

        # Display results
        print("\n" + "=" * 50)
        print("Authorships table populated successfully!")
        print("=" * 50)
        print(f"Papers processed: {authorship_stats['processed_papers']}")
        print(f"Total authorship records: {authorship_stats['total_authorships']}")
        print(f"Matched authors: {authorship_stats['matched_authors']}")
        print(f"Unmatched authors: {authorship_stats['unmatched_authors']}")

        if 'optimization_method' in authorship_stats:
            print(f"Optimization method: {authorship_stats['optimization_method']}")

        if 'data_completeness' in authorship_stats:
            print(f"Data completeness: {authorship_stats['data_completeness']}")

        print(f"Processing time: {processing_time:.2f} seconds")

        if args.verbose:
            print(f"\nDetailed statistics: {authorship_stats}")

        return 0

    except Exception as e:
        print(f"Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 1 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())