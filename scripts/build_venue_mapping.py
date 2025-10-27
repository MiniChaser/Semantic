#!/usr/bin/env python3
"""
Build venue_mapping table by matching all distinct venue values
This is a one-time operation that creates a fast lookup table

Usage:
  uv run python scripts/build_venue_mapping.py

  Optional flags:
    --rebuild: Drop and rebuild the mapping table
    --batch-size N: Batch size for processing (default: 10000)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.database.connection import DatabaseManager
from src.semantic.services.dataset_service.database_conference_matcher import DatabaseConferenceMatcher


def create_table(db_manager: DatabaseManager, rebuild: bool = False):
    """Create venue_mapping table"""
    print("\n" + "="*80)
    print("Creating venue_mapping Table")
    print("="*80)

    if rebuild:
        print("Dropping existing venue_mapping table...")
        db_manager.execute_query("DROP TABLE IF EXISTS venue_mapping CASCADE")
        print("✓ Dropped")

    # Read and execute DDL
    ddl_file = Path(__file__).parent / 'create_venue_mapping_table.sql'

    if not ddl_file.exists():
        print(f"✗ DDL file not found: {ddl_file}")
        return False

    with open(ddl_file, 'r') as f:
        ddl = f.read()

    try:
        db_manager.execute_query(ddl)
        print("✓ venue_mapping table created")
        return True
    except Exception as e:
        print(f"✗ Error creating table: {e}")
        return False


def get_distinct_venues(db_manager: DatabaseManager):
    """Get all distinct venue values from all_papers"""
    print("\n" + "="*80)
    print("Fetching Distinct Venue Values")
    print("="*80)

    # Skip COUNT - just fetch directly (saves 1-3 minutes)
    print("Fetching distinct venues (this may take 1-2 minutes)...")
    start = datetime.now()

    venues = db_manager.fetch_all("""
        SELECT DISTINCT venue
        FROM dataset_all_papers
        WHERE venue IS NOT NULL AND venue != ''
        ORDER BY venue
    """)

    elapsed = (datetime.now() - start).total_seconds()

    if not venues:
        print("✗ No venues found")
        return []

    print(f"✓ Fetched {len(venues):,} distinct venues in {elapsed:.1f}s\n")

    return [v['venue'] for v in venues]


def build_mappings(db_manager: DatabaseManager, venues: list, batch_size: int = 10000):
    """Match venues to conferences and insert into mapping table"""
    print("="*80)
    print("Building Venue Mappings")
    print("="*80)

    # Initialize matcher
    print("\nInitializing DatabaseConferenceMatcher...")
    try:
        matcher = DatabaseConferenceMatcher(db_manager)
        conf_count = matcher.get_conference_count()
        print(f"✓ Loaded {conf_count} conferences\n")
    except Exception as e:
        print(f"✗ Error initializing matcher: {e}")
        return 0

    total_venues = len(venues)
    matched_count = 0
    unmatched_count = 0

    print(f"Processing {total_venues:,} venues in batches of {batch_size:,}...\n")

    with tqdm(total=total_venues, desc="Matching venues", unit=" venues") as pbar:
        for i in range(0, total_venues, batch_size):
            batch = venues[i:i+batch_size]
            mappings = []

            # Match each venue in batch
            for venue in batch:
                conf = matcher.match_conference(venue)
                if conf:
                    mappings.append((venue, conf, 'python', 1.0))
                    matched_count += 1
                else:
                    unmatched_count += 1

            # Batch insert mappings
            if mappings:
                try:
                    with db_manager.get_cursor() as cursor:
                        # Use INSERT ... ON CONFLICT DO NOTHING for safety
                        cursor.executemany("""
                            INSERT INTO venue_mapping (venue_raw, conference_name, match_method, match_confidence)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (venue_raw) DO NOTHING
                        """, mappings)
                except Exception as e:
                    print(f"\n✗ Error inserting batch: {e}")
                    return matched_count

            pbar.update(len(batch))

    print(f"\n✓ Mapping complete!")
    print(f"  Matched: {matched_count:,} ({matched_count/total_venues*100:.1f}%)")
    print(f"  Unmatched: {unmatched_count:,} ({unmatched_count/total_venues*100:.1f}%)")

    return matched_count


def show_statistics(db_manager: DatabaseManager):
    """Show mapping table statistics"""
    print("\n" + "="*80)
    print("Mapping Table Statistics")
    print("="*80)

    # Total mappings
    result = db_manager.fetch_one("SELECT COUNT(*) as total FROM venue_mapping")
    total = result['total'] if result else 0
    print(f"\nTotal mappings: {total:,}")

    # By conference
    print("\nTop 20 conferences by venue count:")
    results = db_manager.fetch_all("""
        SELECT conference_name, COUNT(*) as venue_count
        FROM venue_mapping
        GROUP BY conference_name
        ORDER BY venue_count DESC
        LIMIT 20
    """)

    for r in results:
        print(f"  {r['conference_name']:20s}: {r['venue_count']:,} venue variations")

    # By method
    print("\nBy match method:")
    results = db_manager.fetch_all("""
        SELECT match_method, COUNT(*) as count
        FROM venue_mapping
        GROUP BY match_method
        ORDER BY count DESC
    """)

    for r in results:
        print(f"  {r['match_method']:10s}: {r['count']:,}")

    # Table size
    result = db_manager.fetch_one("""
        SELECT pg_size_pretty(pg_total_relation_size('venue_mapping')) as size
    """)

    if result:
        print(f"\nTable size: {result['size']}")


def main():
    parser = argparse.ArgumentParser(
        description='Build venue_mapping table for fast lookups',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--rebuild', action='store_true',
                       help='Drop and rebuild the mapping table')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='Batch size for processing (default: 10000)')
    parser.add_argument('--skip-build', action='store_true',
                       help='Skip building, only show statistics')

    args = parser.parse_args()

    print("="*80)
    print("Build Venue Mapping Table")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    db = DatabaseManager()

    if not db.test_connection():
        print("✗ Database connection failed")
        return 1

    print("✓ Database connected")

    overall_start = datetime.now()

    try:
        if not args.skip_build:
            # Step 1: Create table
            if not create_table(db, args.rebuild):
                return 1

            # Step 2: Get distinct venues
            venues = get_distinct_venues(db)

            if not venues:
                print("✗ No venues to process")
                return 1

            # Step 3: Build mappings
            matched_count = build_mappings(db, venues, args.batch_size)

            if matched_count == 0:
                print("✗ No mappings created")
                return 1

        # Step 4: Show statistics
        show_statistics(db)

        overall_elapsed = (datetime.now() - overall_start).total_seconds()

        print("\n" + "="*80)
        print("✅ Mapping table built successfully!")
        print("="*80)
        print(f"Total time: {overall_elapsed/60:.1f} minutes")
        print("\nNext step:")
        print("  uv run python scripts/import_papers_stage1_all.py --data-dir /path/to/data")
        print("\nNote:")
        print("  venue_normalized will be computed automatically during import")
        print("  (no separate populate step needed)")
        print("="*80)

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
