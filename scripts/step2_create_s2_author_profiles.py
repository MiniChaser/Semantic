#!/usr/bin/env python3
"""
Author Processing Step 2.6: Batch Enrich Author Profiles with S2 Author API
High-performance batch processing version that queries up to 1000 S2 author IDs per API call
Achieves significant performance improvements over individual queries
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
            logging.FileHandler(log_dir / 'step2_6_batch_s2_author_enrichment.log')
        ]
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Batch Enrich Author Profiles with S2 Author API (Step 2.6)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Performance Benefits:
  - Batch API calls: Up to 1000 author IDs per call instead of 1
  - Timestamp-based caching: Only updates stale data
  - Separate S2 data storage: Efficient data management
  - Expected 95%+ reduction in API call overhead

Examples:
  python step2_6_batch_s2_author_enrichment.py
  python step2_6_batch_s2_author_enrichment.py --limit 5000 --verbose
  python step2_6_batch_s2_author_enrichment.py --api-key YOUR_API_KEY --force-individual
        """
    )

    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Limit the number of author IDs to process (for testing)'
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

    parser.add_argument(
        '--force-individual',
        action='store_true',
        help='Force individual API calls instead of batch processing (for debugging)'
    )

    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Only display statistics without running enrichment'
    )

    return parser.parse_args()


def display_performance_comparison():
    """Display performance comparison between old and new approaches"""
    print("\n" + "=" * 72)
    print("BATCH PROCESSING PERFORMANCE BENEFITS")
    print("=" * 72)
    print("OLD APPROACH:")
    print("  - 1 API call per author")
    print("  - 1000 authors = 1000 API calls")
    print("  - High latency due to network overhead")
    print()
    print("NEW BATCH APPROACH:")
    print("  - Up to 1000 authors per API call")
    print("  - 1000 authors = 1 API call")
    print("  - 95%+ reduction in processing time")
    print("  - Timestamp-based caching prevents redundant queries")
    print("=" * 72)


def main():
    """Execute Step 2.6: Batch Enrich Author Profiles with S2 Author API"""
    args = parse_arguments()

    print("Step 2.6: Batch Enriching Author Profiles with S2 Author API")
    print("=" * 70)
    print("BATCH PROCESSING MODE - High Performance S2 Author Enrichment")
    print("=" * 70)

    if not args.force_individual:
        display_performance_comparison()

    print("\nThis step enriches author profiles with S2 data using batch processing:")
    print("- Homepage URLs")
    print("- S2 Paper Count")
    print("- S2 Citation Count")
    print("- S2 H-Index")
    print("- S2 Affiliations")
    print("=" * 70)

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
        use_batch = not args.force_individual
        enrichment_service = S2AuthorEnrichmentService(db_manager, api_key, use_batch=use_batch)

        if args.stats_only:
            # Display statistics only
            if use_batch:
                stats = enrichment_service.batch_service.get_processing_statistics()
                print("\n" + "=" * 62)
                print("BATCH PROCESSING STATISTICS")
                print("=" * 62)
                print(f"Timestamp: {stats.get('timestamp', 'N/A')}")

                s2_stats = stats.get('s2_author_profiles', {})
                if s2_stats:
                    print(f"Total S2 profiles stored: {s2_stats.get('total_s2_profiles', 0)}")
                    print(f"Profiles with homepage: {s2_stats.get('profiles_with_homepage', 0)}")
                    print(f"Profiles with affiliations: {s2_stats.get('profiles_with_affiliations', 0)}")
                    print(f"Profiles with paper count: {s2_stats.get('profiles_with_paper_count', 0)}")
                    print(f"Profiles with citation count: {s2_stats.get('profiles_with_citation_count', 0)}")
                    print(f"Profiles with H-index: {s2_stats.get('profiles_with_h_index', 0)}")
                    print(f"Average paper count: {s2_stats.get('avg_paper_count', 0):.1f}")
                    print(f"Average citation count: {s2_stats.get('avg_citation_count', 0):.1f}")
                    print(f"Average H-index: {s2_stats.get('avg_h_index', 0):.1f}")
                    print(f"Last update: {s2_stats.get('last_update', 'N/A')}")

                sync_stats = stats.get('author_profiles_sync', {})
                if sync_stats:
                    print(f"Author profiles with S2 homepage: {sync_stats.get('profiles_with_s2_homepage', 0)}")
                    print(f"Author profiles with S2 affiliations: {sync_stats.get('profiles_with_s2_affiliations', 0)}")
                    print(f"Author profiles with S2 paper count: {sync_stats.get('profiles_with_s2_paper_count', 0)}")
                    print(f"Author profiles with S2 citation count: {sync_stats.get('profiles_with_s2_citation_count', 0)}")
                    print(f"Author profiles with S2 H-index: {sync_stats.get('profiles_with_s2_h_index', 0)}")

                print("=" * 62)
            else:
                print("Statistics not available for individual processing mode")
            return 0

        # Run enrichment
        if use_batch:
            print(f"\nStarting BATCH processing mode...")
            logger.info("Using BATCH processing for optimal performance")
        else:
            print(f"\nStarting INDIVIDUAL processing mode (debugging)...")
            logger.info("Using INDIVIDUAL processing for debugging")

        stats = enrichment_service.run_enrichment(limit=args.limit)

        # Check results
        if 'error' in stats:
            logger.error(f"Enrichment process failed: {stats['error']}")
            return 1

        # Additional success message
        if use_batch:
            batch_stats = stats.get('batch_fetch_stats', {})
            api_calls = batch_stats.get('api_calls', 0)
            processed = batch_stats.get('processed', 0)

            if processed > 0 and api_calls > 0:
                efficiency_ratio = processed / api_calls
                print(f"\nBATCH EFFICIENCY: Processed {processed} authors with {api_calls} API calls")
                print(f"   Efficiency ratio: {efficiency_ratio:.0f} authors per API call")
                print(f"   This is {efficiency_ratio:.0f}x more efficient than individual calls!")

        logger.info("S2 Author API batch enrichment completed successfully")

        if args.verbose:
            print(f"\nDetailed statistics: {stats}")

        return 0

    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
        logging.getLogger(__name__).info("Process interrupted by user")
        return 1

    except Exception as e:
        print(f"Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 2.6 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())