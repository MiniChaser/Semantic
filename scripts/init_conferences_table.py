#!/usr/bin/env python3
"""
Initialize conferences table from GitHub API
Run once to populate conference data, then use database cache

This script:
1. Creates conferences and conference_aliases tables
2. Fetches conference list from GitHub (using existing ConferenceMatcher)
3. Imports conferences and aliases into database
4. Creates indexes for fast lookup

Usage:
  uv run python scripts/init_conferences_table.py

  Optional flags:
    --force: Drop and recreate tables if they already exist
    --test: Test connection only without making changes
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.database.connection import DatabaseManager
from src.semantic.database.schemas.conferences import ConferencesSchema
from src.semantic.services.dataset_service.conference_matcher import ConferenceMatcher


def init_conferences_table(db_manager: DatabaseManager, force: bool = False) -> bool:
    """Initialize conferences table from GitHub API"""

    print("=" * 80)
    print("Initialize Conferences Table")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Initialize schema
    schema = ConferencesSchema(db_manager)

    # Check if tables already exist
    if schema.check_tables_exist():
        if not force:
            print("⚠️  Conferences tables already exist.")
            print("   Use --force to drop and recreate tables.")

            # Show current count
            result = db_manager.fetch_one("SELECT COUNT(*) as count FROM conferences")
            conf_count = result['count'] if result else 0
            result = db_manager.fetch_one("SELECT COUNT(*) as count FROM conference_aliases")
            alias_count = result['count'] if result else 0

            print(f"\nCurrent data:")
            print(f"  Conferences: {conf_count}")
            print(f"  Aliases: {alias_count}")
            print("\nTo reinitialize, run with --force flag")
            return True
        else:
            print("⚠️  Force mode: Dropping existing tables...")
            if not schema.drop_tables():
                print("✗ Failed to drop tables")
                return False

    # Step 1: Create tables
    print("\n" + "="*80)
    print("Step 1: Creating tables")
    print("="*80)

    if not schema.create_tables():
        print("✗ Failed to create tables")
        return False

    print("✓ Tables created successfully")

    # Step 2: Fetch conferences from GitHub
    print("\n" + "="*80)
    print("Step 2: Fetching conferences from GitHub API")
    print("="*80)

    try:
        matcher = ConferenceMatcher()
        conferences = matcher.get_conferences()
        aliases = matcher.aliases

        print(f"✓ Fetched {len(conferences)} conferences from GitHub")
        print(f"✓ Found {sum(len(v) for v in aliases.values())} aliases")

    except Exception as e:
        print(f"✗ Failed to fetch conferences: {e}")
        return False

    # Step 3: Insert conferences
    print("\n" + "="*80)
    print("Step 3: Importing conferences")
    print("="*80)

    try:
        conference_inserts = [(conf,) for conf in conferences]

        insert_sql = """
            INSERT INTO conferences (conference_name)
            VALUES (%s)
            ON CONFLICT (conference_name) DO NOTHING
        """

        db_manager.execute_batch_query(insert_sql, conference_inserts)
        print(f"✓ Inserted {len(conferences)} conferences")

    except Exception as e:
        print(f"✗ Failed to insert conferences: {e}")
        return False

    # Step 4: Insert aliases
    print("\n" + "="*80)
    print("Step 4: Importing aliases")
    print("="*80)

    try:
        alias_inserts = []

        # Add conference name itself as highest priority alias
        for conf in conferences:
            alias_inserts.append((conf, conf, 100))

        # Add defined aliases with medium priority
        for conf, alias_list in aliases.items():
            for alias in alias_list:
                # Skip if alias is same as conference name (to avoid duplicates)
                if alias.lower() != conf.lower():
                    alias_inserts.append((conf, alias, 50))

        insert_sql = """
            INSERT INTO conference_aliases (conference_name, alias, priority)
            VALUES (%s, %s, %s)
            ON CONFLICT (conference_name, alias) DO NOTHING
        """

        db_manager.execute_batch_query(insert_sql, alias_inserts)
        print(f"✓ Inserted {len(alias_inserts)} aliases (including conference names)")

    except Exception as e:
        print(f"✗ Failed to insert aliases: {e}")
        return False

    # Step 5: Verify
    print("\n" + "="*80)
    print("Step 5: Verifying data")
    print("="*80)

    result = db_manager.fetch_one("SELECT COUNT(*) as count FROM conferences WHERE is_active = TRUE")
    conf_count = result['count'] if result else 0

    result = db_manager.fetch_one("SELECT COUNT(*) as count FROM conference_aliases")
    alias_count = result['count'] if result else 0

    print(f"✓ Active conferences: {conf_count}")
    print(f"✓ Total aliases: {alias_count}")

    # Show sample data
    print("\nSample conferences:")
    results = db_manager.fetch_all("""
        SELECT conference_name FROM conferences
        WHERE is_active = TRUE
        ORDER BY conference_name
        LIMIT 10
    """)
    for r in results:
        print(f"  - {r['conference_name']}")

    if conf_count > 10:
        print(f"  ... and {conf_count - 10} more")

    print("\n" + "="*80)
    print("🎉 Initialization completed successfully!")
    print("="*80)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nNext steps:")
    print("1. Run: uv run python scripts/populate_venue_normalized.py")
    print("2. This will use the database cache (no network required)")
    print("="*80)

    return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Initialize conferences table from GitHub API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script initializes the conferences and conference_aliases tables
by fetching data from GitHub API (one-time operation).

After initialization, all conference matching will use the database cache
instead of making network requests.

Examples:
  # Normal initialization
  %(prog)s

  # Force reinitialize (drop and recreate)
  %(prog)s --force

  # Test connection only
  %(prog)s --test
        """
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Drop and recreate tables if they already exist'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Test database connection only, do not make changes'
    )

    args = parser.parse_args()

    # Initialize database
    db_manager = DatabaseManager()

    # Test connection
    if not db_manager.test_connection():
        print("✗ Database connection failed")
        return 1

    print("✓ Database connection successful")

    if args.test:
        print("\n✓ Test completed successfully")
        return 0

    try:
        # Initialize tables
        if not init_conferences_table(db_manager, force=args.force):
            return 1

        return 0

    except KeyboardInterrupt:
        print("\n\n✗ Interrupted by user")
        return 130
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
