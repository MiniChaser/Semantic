#!/usr/bin/env python3
"""
Author Processing Step 2.5: Enrich Author Profiles with S2 Author API
Enriches existing author profiles with additional data from Semantic Scholar Author API
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.s2_service.s2_author_enrichment_service import S2AuthorEnrichmentService


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step2_5_s2_author_enrichment.log')
        ]
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Enrich Author Profiles with S2 Author API (Step 2.5)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python step2_5_enrich_author_profiles_with_s2.py
  python step2_5_enrich_author_profiles_with_s2.py --limit 100 --verbose
  python step2_5_enrich_author_profiles_with_s2.py --api-key YOUR_API_KEY
        """
    )

    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Limit the number of authors to process (for testing)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--api-key',
        type=str,
        help='Semantic Scholar API key (overrides environment variable)'
    )

    return parser.parse_args()


def main():
    """Execute Step 2.5: Enrich Author Profiles with S2 Author API"""
    args = parse_arguments()

    print("Step 2.5: Enriching Author Profiles with S2 Author API")
    print("=" * 60)
    print("This step enriches existing author profiles with additional S2 data:")
    print("- Homepage URLs")
    print("- S2 Paper Count")
    print("- S2 Citation Count")
    print("- S2 H-Index")
    print("=" * 60)

    try:
        # Setup logging
        setup_logging(args.verbose)
        logger = logging.getLogger(__name__)

        if args.verbose:
            logger.debug("Verbose logging enabled")

        # Check for API key
        api_key = args.api_key or os.getenv('SEMANTIC_SCHOLAR_API_KEY')
        if not api_key:
            logger.warning("No Semantic Scholar API key provided. Rate limits will be more restrictive.")
            print("WARNING: No S2 API key found. Consider setting SEMANTIC_SCHOLAR_API_KEY environment variable.")
        else:
            logger.info("Using Semantic Scholar API key for enhanced rate limits")

        # Load configuration and initialize database
        config = AppConfig.from_env()
        db_manager = get_db_manager()
        logger.info("Database connection established")

        # Initialize enrichment service
        enrichment_service = S2AuthorEnrichmentService(db_manager, api_key)

        # Run enrichment
        stats = enrichment_service.run_enrichment(limit=args.limit)

        # Check results
        if 'error' in stats:
            logger.error(f"Enrichment process failed: {stats['error']}")
            return 1

        logger.info("S2 Author API enrichment completed successfully")

        if args.verbose:
            print(f"\nDetailed statistics: {stats}")

        return 0

    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
        logging.getLogger(__name__).info("Process interrupted by user")
        return 1

    except Exception as e:
        print(f"Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 2.5 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())