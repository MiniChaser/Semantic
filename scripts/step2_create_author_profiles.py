#!/usr/bin/env python3
"""
Author Processing Step 2: Create Author Profiles Table
Creates and populates the author profiles table with unique author information

Now supports both regular and pandas-optimized processing modes:
- Regular mode: Compatible with original implementation
- Pandas mode: Optimized for performance using batch processing
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
from semantic.services.author_service.author_profile_pandas_service import AuthorProfilePandasService


def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step2_create_author_profiles.log')
        ]
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Create Author Profiles Table (Step 2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Processing modes:
  regular: Uses original implementation with multiple database queries
  pandas:  Uses optimized pandas-based batch processing (recommended)

Examples:
  python step2_create_author_profiles.py --mode pandas
  python step2_create_author_profiles.py --mode regular --verbose
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
    """Run using original AuthorProfileService"""
    print("Using regular processing mode...")

    # Initialize service
    profile_service = AuthorProfileService(db_manager)

    # Create author profiles table
    if not profile_service.create_author_profiles_table():
        return {'error': 'Failed to create author profiles table'}
    print("Author profiles table created")

    # Populate author profiles table
    profile_stats = profile_service.populate_author_profiles_table()
    return profile_stats


def run_pandas_mode(db_manager) -> Dict:
    """Run using optimized AuthorProfilePandasService"""
    print("Using pandas-optimized processing mode...")

    # Initialize optimized service
    profile_service = AuthorProfilePandasService(db_manager)

    # Create author profiles table
    if not profile_service.create_author_profiles_table():
        return {'error': 'Failed to create author profiles table'}
    print("Author profiles table created")

    # Populate author profiles table using pandas optimization
    profile_stats = profile_service.populate_author_profiles_table()
    return profile_stats


def main():
    """Execute Step 2: Create Author Profiles Table"""

    args = parse_arguments()

    print("Step 2: Creating Author Profiles Table")
    print("=" * 50)
    print(f"Processing mode: {args.mode.upper()}")
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
            profile_stats = run_pandas_mode(db_manager)
        else:
            profile_stats = run_regular_mode(db_manager)

        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time

        # Check for errors
        if 'error' in profile_stats:
            print(f"Failed to populate author profiles: {profile_stats['error']}")
            return 1

        # Display results
        print("\n" + "=" * 50)
        print("Author profiles table populated successfully!")
        print("=" * 50)
        print(f"Total unique authors: {profile_stats['total_unique_authors']}")
        print(f"Authors with S2 ID: {profile_stats['authors_with_s2_id']}")
        print(f"Authors without S2 ID: {profile_stats['authors_without_s2_id']}")

        if 'total_papers_processed' in profile_stats:
            print(f"Total papers processed: {profile_stats['total_papers_processed']}")

        if 'optimization_method' in profile_stats:
            print(f"Optimization method: {profile_stats['optimization_method']}")

        print(f"Processing time: {processing_time:.2f} seconds")

        if args.verbose:
            print(f"\nDetailed statistics: {profile_stats}")

        return 0

    except Exception as e:
        print(f"Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 2 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())