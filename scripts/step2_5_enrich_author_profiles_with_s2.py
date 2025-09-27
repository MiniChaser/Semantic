#!/usr/bin/env python3
"""
Author Processing Step 2.5: Sync Author Profiles with Cached S2 Data
Enriches existing author profiles with S2 data from s2_author_profiles table (no API calls)
"""

import sys
import logging
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.s2_service.s2_author_profile_sync_service import S2AuthorProfileSyncService


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
        description="Sync Author Profiles with Cached S2 Data (Step 2.5)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script syncs author profiles with cached S2 data from s2_author_profiles table.
No API calls are made - only database operations.

Examples:
  python step2_5_enrich_author_profiles_with_s2.py
  python step2_5_enrich_author_profiles_with_s2.py --limit 100 --verbose
  python step2_5_enrich_author_profiles_with_s2.py --stats-only
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
        '--stats-only',
        action='store_true',
        help='Only display statistics without running sync'
    )

    return parser.parse_args()


def main():
    """Execute Step 2.5: Sync Author Profiles with Cached S2 Data"""
    args = parse_arguments()

    print("Step 2.5: Syncing Author Profiles with Cached S2 Data")
    print("=" * 60)
    print("This step syncs author profiles with cached S2 data (NO API CALLS):")
    print("- Homepage URLs")
    print("- S2 Paper Count")
    print("- S2 Citation Count")
    print("- S2 H-Index")
    print("- S2 Affiliations")
    print("=" * 60)

    try:
        # Setup logging
        setup_logging(args.verbose)
        logger = logging.getLogger(__name__)

        if args.verbose:
            logger.debug("Verbose logging enabled")

        # Load configuration and initialize database
        config = AppConfig.from_env()
        db_manager = get_db_manager()
        logger.info("Database connection established")

        # Initialize sync service
        sync_service = S2AuthorProfileSyncService(db_manager)

        if args.stats_only:
            # Display statistics only
            print("\n" + "=" * 60)
            print("CACHED S2 DATA STATISTICS")
            print("=" * 60)

            stats = sync_service.get_sync_statistics()

            if 'error' in stats:
                print(f"Error getting statistics: {stats['error']}")
                return 1

            # Display cached S2 data statistics
            s2_data = stats.get('cached_s2_data', {})
            if s2_data:
                print(f"Total cached S2 profiles: {s2_data.get('total_s2_profiles', 0)}")
                print(f"Profiles with homepage: {s2_data.get('profiles_with_homepage', 0)}")
                print(f"Profiles with affiliations: {s2_data.get('profiles_with_affiliations', 0)}")
                print(f"Profiles with paper count: {s2_data.get('profiles_with_paper_count', 0)}")
                print(f"Profiles with citation count: {s2_data.get('profiles_with_citation_count', 0)}")
                print(f"Profiles with H-index: {s2_data.get('profiles_with_h_index', 0)}")
                print(f"Last S2 data update: {s2_data.get('last_update', 'N/A')}")

            # Display author profiles status
            ap_status = stats.get('author_profiles_status', {})
            if ap_status:
                print(f"\nTotal author profiles: {ap_status.get('total_author_profiles', 0)}")
                print(f"Profiles with S2 ID: {ap_status.get('profiles_with_s2_id', 0)}")
                print(f"Profiles with homepage: {ap_status.get('profiles_with_homepage', 0)}")
                print(f"Profiles with S2 affiliations: {ap_status.get('profiles_with_affiliations', 0)}")
                print(f"Profiles with S2 paper count: {ap_status.get('profiles_with_paper_count', 0)}")
                print(f"Profiles with S2 citation count: {ap_status.get('profiles_with_citation_count', 0)}")
                print(f"Profiles with S2 H-index: {ap_status.get('profiles_with_h_index', 0)}")

            # Display sync readiness
            sync_ready = stats.get('sync_ready', {})
            if sync_ready:
                print(f"\nAuthors ready to sync: {sync_ready.get('authors_ready_to_sync', 0)}")

            print("=" * 60)
            return 0

        # Run sync
        print(f"\nStarting sync from cached S2 data...")
        logger.info("Using cached S2 data for sync - no API calls")

        stats = sync_service.sync_author_profiles(limit=args.limit)

        # Check results
        if 'error' in stats:
            logger.error(f"Sync process failed: {stats['error']}")
            return 1

        # Display results
        print("\n" + "=" * 60)
        print("S2 AUTHOR PROFILE SYNC COMPLETED")
        print("=" * 60)
        print(f"Processing time: {stats.get('processing_time', 0):.2f} seconds")
        print(f"Total authors processed: {stats.get('total_authors_processed', 0)}")
        print(f"Authors successfully synced: {stats.get('authors_synced', 0)}")
        print(f"Errors encountered: {stats.get('errors', 0)}")

        # Calculate success rate
        total_processed = stats.get('total_authors_processed', 0)
        if total_processed > 0:
            sync_rate = (stats.get('authors_synced', 0) / total_processed) * 100
            print(f"Sync success rate: {sync_rate:.1f}%")

        print("=" * 60)

        logger.info("S2 author profile sync completed successfully")

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