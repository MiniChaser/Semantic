#!/usr/bin/env python3
"""
Author Processing Step 3: Create Final Author Table
Creates and populates the final target table with all computed metrics

Now supports both regular and pandas-optimized processing modes:
- Regular mode: Compatible with original implementation (multiple queries)
- Pandas mode: Optimized for performance using batch processing (recommended)
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
from semantic.services.author_service.final_author_table_service import FinalAuthorTableService
from semantic.services.author_service.final_author_table_pandas_service import FinalAuthorTablePandasService


def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step3_create_final_table.log')
        ]
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Create Final Author Table (Step 3)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Processing modes:
  regular: Uses original implementation with individual queries (slower)
  pandas:  Uses optimized pandas-based batch processing (recommended)

Examples:
  python step3_create_final_table.py --mode pandas
  python step3_create_final_table.py --mode regular --verbose
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
    """Run using original FinalAuthorTableService"""
    print("Using regular processing mode (individual queries)...")

    # Initialize service
    final_table_service = FinalAuthorTableService(db_manager)

    # Create final author table
    if not final_table_service.create_final_author_table():
        return {'error': 'Failed to create final author table'}
    print("Final author table created")

    # Populate final author table
    final_stats = final_table_service.populate_final_author_table()
    return final_stats


def run_pandas_mode(db_manager) -> Dict:
    """Run using optimized FinalAuthorTablePandasService"""
    print("Using pandas-optimized processing mode (batch processing)...")

    # Initialize optimized service
    final_table_service = FinalAuthorTablePandasService(db_manager)

    # Create final author table
    if not final_table_service.create_final_author_table():
        return {'error': 'Failed to create final author table'}
    print("Final author table created")

    # Populate final author table using pandas optimization
    final_stats = final_table_service.populate_final_author_table_pandas()
    return final_stats


def main():
    """Execute Step 3: Create Final Author Table"""

    args = parse_arguments()

    print("Step 3: Creating Final Target Table")
    print("=" * 50)
    print(f"Processing mode: {args.mode.upper()}")
    if args.mode == 'pandas':
        print("Note: Pandas mode provides optimized batch processing with minimal database queries")
    else:
        print("Note: Regular mode uses individual queries (slower but compatible)")
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
            final_stats = run_pandas_mode(db_manager)
        else:
            final_stats = run_regular_mode(db_manager)

        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time

        # Check for errors
        if 'error' in final_stats:
            print(f"Failed to populate final author table: {final_stats['error']}")
            return 1

        # Display results
        print("\n" + "=" * 50)
        print("Final author table populated successfully!")
        print("=" * 50)
        print(f"Total authors processed: {final_stats['total_authors_processed']}")
        print(f"Complete data records: {final_stats['authors_with_complete_data']}")
        print(f"Partial data records: {final_stats['authors_with_partial_data']}")

        if 'optimization_method' in final_stats:
            print(f"Optimization method: {final_stats['optimization_method']}")

        if 'database_queries_eliminated' in final_stats:
            print(f"Database optimization: {final_stats['database_queries_eliminated']}")

        print(f"Processing time: {processing_time:.2f} seconds")

        # Show sample records
        print(f"\nSample Final Table Records:")
        if args.mode == 'pandas':
            service = FinalAuthorTablePandasService(db_manager)
        else:
            service = FinalAuthorTableService(db_manager)

        sample_records = service.get_sample_records(5)
        for i, record in enumerate(sample_records, 1):
            print(f"  {i}. {record['dblp_author']}")
            print(f"     Papers: {record['first_author_count']}, Career: {record['career_length']} years")
            if 'semantic_scholar_citation_count' in record:
                print(f"     Citations: {record['semantic_scholar_citation_count']}, H-index: {record.get('semantic_scholar_h_index', 'N/A')}")

        if args.verbose:
            print(f"\nDetailed statistics: {final_stats}")

        return 0

    except Exception as e:
        print(f"Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 3 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())