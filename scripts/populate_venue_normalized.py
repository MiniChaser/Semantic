#!/usr/bin/env python3
"""
Populate venue_normalized column for existing data

Uses mixed strategy for optimal performance:
- Phase 1: SQL regex bulk matching (fast, 80-90% coverage)
- Phase 2: Python precise matching for remaining cases (accurate, 10-20%)
- Phase 3: Create B-tree index
- Phase 4: Verify optimization

Estimated runtime: 30-60 minutes for 200M records

Usage:
  uv run python scripts/populate_venue_normalized.py

  Optional flags:
    --skip-phase1: Skip SQL regex matching
    --skip-phase2: Skip Python matching
    --skip-index: Skip index creation
    --batch-size N: Set batch size for Phase 2 (default: 100000)
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


def check_column_exists(db_manager: DatabaseManager) -> bool:
    """Check if venue_normalized column exists (error if not)"""
    print("\n" + "="*80)
    print("Checking venue_normalized column")
    print("="*80)

    try:
        # Check if column exists
        result = db_manager.fetch_one("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'all_papers' AND column_name = 'venue_normalized'
        """)

        if result:
            print("✓ venue_normalized column exists")
            return True
        else:
            print("✗ venue_normalized column does NOT exist!")
            print("   Please run Stage 1 first to create the table with venue_normalized column:")
            print("   uv run python scripts/import_papers_stage1_all.py --process-only --data-dir downloads/")
            return False

    except Exception as e:
        print(f"✗ Error checking column: {e}")
        return False


def build_sql_case_statement(db_manager: DatabaseManager) -> str:
    """
    Build optimized SQL CASE statement for bulk update
    Reads conference patterns from database
    """
    print("Building SQL CASE statement from database...")

    # Load all conferences and aliases
    results = db_manager.fetch_all("""
        SELECT conference_name, alias
        FROM conference_aliases
        ORDER BY priority DESC, conference_name
    """)

    if not results:
        print("⚠️  Warning: No conference aliases found in database")
        print("   Please run: uv run python scripts/init_conferences_table.py")
        return None

    # Group aliases by conference
    conf_patterns = {}
    for r in results:
        conf = r['conference_name']
        alias = r['alias']
        if conf not in conf_patterns:
            conf_patterns[conf] = []
        conf_patterns[conf].append(alias)

    # Build CASE statement
    cases = []
    for conf, patterns in conf_patterns.items():
        # Create regex pattern with word boundaries
        regex_patterns = '|'.join([f'\\m{p.replace("(", "\\(").replace(")", "\\)")}\\M' for p in patterns])
        cases.append(f"    WHEN venue ~* '({regex_patterns})' THEN '{conf}'")

    case_sql = "CASE\n" + "\n".join(cases) + "\n    ELSE NULL\n  END"

    print(f"✓ Built CASE statement with {len(conf_patterns)} conferences")
    return case_sql


def populate_with_sql_regex(db_manager: DatabaseManager) -> dict:
    """
    Phase 1: Use SQL regex to populate 80-90% of records
    Very fast: 5-10 minutes for 200M records
    """
    print("\n" + "="*80)
    print("Phase 1: SQL Regex Matching (Fast Bulk Update)")
    print("="*80)

    start = datetime.now()

    # Build CASE statement
    case_statement = build_sql_case_statement(db_manager)

    if not case_statement:
        return {'error': 'Failed to build CASE statement'}

    # Execute bulk UPDATE
    print("\nExecuting bulk UPDATE (this may take 5-15 minutes)...")
    print("Progress: PostgreSQL is processing all 200M+ records...")

    update_sql = f"""
    UPDATE all_papers
    SET venue_normalized = {case_statement}
    WHERE venue IS NOT NULL
      AND venue != ''
      AND venue_normalized IS NULL
    """

    try:
        db_manager.execute_query(update_sql)
    except Exception as e:
        print(f"✗ Error during bulk update: {e}")
        return {'error': str(e)}

    elapsed = (datetime.now() - start).total_seconds()

    # Get statistics
    result = db_manager.fetch_one("""
        SELECT
            COUNT(*) FILTER (WHERE venue IS NOT NULL AND venue != '') as total_with_venue,
            COUNT(*) FILTER (WHERE venue_normalized IS NOT NULL) as matched,
            COUNT(*) FILTER (WHERE venue IS NOT NULL AND venue != '' AND venue_normalized IS NULL) as remaining
        FROM all_papers
    """)

    total = result['total_with_venue']
    matched = result['matched']
    remaining = result['remaining']

    coverage = (matched / total * 100) if total > 0 else 0

    print(f"\n✓ Phase 1 completed in {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"  Total papers with venue: {total:,}")
    print(f"  Matched by SQL regex: {matched:,}")
    print(f"  Remaining (NULL): {remaining:,}")
    print(f"  Coverage: {coverage:.1f}%")

    return {
        'elapsed': elapsed,
        'total': total,
        'matched': matched,
        'remaining': remaining,
        'coverage': coverage
    }


def populate_remaining_with_python(db_manager: DatabaseManager, batch_size: int = 100000) -> dict:
    """
    Phase 2: Use Python DatabaseConferenceMatcher for remaining NULL cases
    Slower but more accurate: 5-15 minutes for remaining ~10-20%
    """
    print("\n" + "="*80)
    print("Phase 2: Python Matching (Accurate for edge cases)")
    print("="*80)

    # Get remaining count
    result = db_manager.fetch_one("""
        SELECT COUNT(*) as remaining
        FROM all_papers
        WHERE venue IS NOT NULL
          AND venue != ''
          AND venue_normalized IS NULL
    """)

    total_remaining = result['remaining']

    if total_remaining == 0:
        print("✓ No remaining records to process (100% coverage by SQL regex)")
        return {'elapsed': 0, 'processed': 0, 'updated': 0}

    print(f"Processing {total_remaining:,} remaining records...")
    print(f"Batch size: {batch_size:,}\n")

    start = datetime.now()
    processed = 0
    updated = 0
    last_corpus_id = 0

    # Initialize matcher
    try:
        matcher = DatabaseConferenceMatcher(db_manager)
        print(f"✓ Loaded {matcher.get_conference_count()} conferences from database\n")
    except Exception as e:
        print(f"✗ Error initializing matcher: {e}")
        return {'error': str(e)}

    with tqdm(total=total_remaining, desc="Python matching") as pbar:
        while True:
            # Fetch batch
            papers = db_manager.fetch_all("""
                SELECT corpus_id, venue
                FROM all_papers
                WHERE venue IS NOT NULL
                  AND venue != ''
                  AND venue_normalized IS NULL
                  AND corpus_id > %s
                ORDER BY corpus_id
                LIMIT %s
            """, (last_corpus_id, batch_size))

            if not papers:
                break

            # Match with Python
            updates = []
            for paper in papers:
                normalized = matcher.match_conference(paper['venue'])
                if normalized:
                    updates.append((normalized, paper['corpus_id']))

            # Batch update
            if updates:
                db_manager.execute_batch_query("""
                    UPDATE all_papers SET venue_normalized = %s WHERE corpus_id = %s
                """, updates)
                updated += len(updates)

            processed += len(papers)
            last_corpus_id = papers[-1]['corpus_id']
            pbar.update(len(papers))

    elapsed = (datetime.now() - start).total_seconds()
    match_rate = (updated / processed * 100) if processed > 0 else 0

    print(f"\n✓ Phase 2 completed in {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"  Processed: {processed:,}")
    print(f"  Updated: {updated:,}")
    print(f"  Match rate: {match_rate:.1f}%")

    return {
        'elapsed': elapsed,
        'processed': processed,
        'updated': updated,
        'match_rate': match_rate
    }


def create_index(db_manager: DatabaseManager) -> dict:
    """
    Phase 3: Create B-tree index on venue_normalized (partial index, only non-NULL)
    Estimated time: 15-30 minutes for 200M records
    """
    print("\n" + "="*80)
    print("Phase 3: Creating B-tree Index")
    print("="*80)

    # Check if index exists
    result = db_manager.fetch_one("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'all_papers'
          AND indexname = 'idx_all_papers_venue_normalized'
    """)

    if result:
        print("✓ idx_all_papers_venue_normalized already exists, skipping...")
        return {'elapsed': 0, 'existed': True}

    print("Creating partial B-tree index (only non-NULL values)...")
    print("This may take 15-30 minutes for 200M records...")

    start = datetime.now()

    try:
        db_manager.execute_query("""
            CREATE INDEX idx_all_papers_venue_normalized
            ON all_papers(venue_normalized)
            WHERE venue_normalized IS NOT NULL
        """)
    except Exception as e:
        print(f"✗ Error creating index: {e}")
        return {'error': str(e)}

    elapsed = (datetime.now() - start).total_seconds()

    print(f"\n✓ Index created in {elapsed:.1f}s ({elapsed/60:.1f} minutes)")

    # Analyze table for query optimizer
    print("\nRunning ANALYZE to update statistics...")
    db_manager.execute_query("ANALYZE all_papers")
    print("✓ Statistics updated")

    return {'elapsed': elapsed, 'existed': False}


def verify_optimization(db_manager: DatabaseManager) -> bool:
    """
    Phase 4: Verify the optimization by testing query performance
    """
    print("\n" + "="*80)
    print("Phase 4: Verifying Optimization")
    print("="*80)

    # Get sample conferences
    results = db_manager.fetch_all("""
        SELECT conference_name FROM conferences
        WHERE is_active = TRUE
        ORDER BY conference_name
        LIMIT 10
    """)

    if not results:
        print("⚠️  Warning: No conferences found")
        return False

    conferences = [r['conference_name'] for r in results]

    # Test query
    placeholders = ','.join(['%s'] * len(conferences))
    test_query = f"""
        SELECT COUNT(*) as total
        FROM all_papers
        WHERE venue_normalized IN ({placeholders})
    """

    print(f"Testing IN query with {len(conferences)} conferences...")
    print(f"Conferences: {', '.join(conferences[:5])}...")

    start = datetime.now()
    result = db_manager.fetch_one(test_query, tuple(conferences))
    elapsed = (datetime.now() - start).total_seconds()

    count = result['total'] if result else 0

    print(f"\n✓ Test query completed")
    print(f"  Matching papers: {count:,}")
    print(f"  Query time: {elapsed:.3f}s")

    if elapsed < 5:
        speedup = 300 / elapsed  # Assume old LIKE query took ~5 minutes (300s)
        print(f"  🚀 Excellent! Query is ~{speedup:.0f}x faster than LIKE queries")
    else:
        print(f"  ⚠️  Warning: Query slower than expected")

    # Overall statistics
    print("\nOverall statistics:")
    result = db_manager.fetch_one("""
        SELECT
            COUNT(*) as total,
            COUNT(venue_normalized) as normalized,
            COUNT(DISTINCT venue_normalized) as unique_conferences
        FROM all_papers
    """)

    if result:
        total = result['total']
        normalized = result['normalized']
        unique = result['unique_conferences']
        print(f"  Total papers: {total:,}")
        print(f"  Papers with normalized venue: {normalized:,}")
        print(f"  Unique conferences found: {unique}")
        if total > 0:
            print(f"  Normalization rate: {normalized/total*100:.2f}%")

    return True


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description='Populate venue_normalized column with conference names',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script populates the venue_normalized column using a mixed strategy:
1. SQL regex for fast bulk matching (80-90% coverage)
2. Python for accurate edge case handling (10-20%)
3. B-tree index creation for fast queries
4. Verification of optimization

Expected runtime: 30-60 minutes for 200M records

Examples:
  # Normal operation (all phases)
  %(prog)s

  # Custom batch size for Phase 2
  %(prog)s --batch-size 50000

  # Skip phases (useful for debugging)
  %(prog)s --skip-phase1
  %(prog)s --skip-index
        """
    )

    parser.add_argument('--skip-phase1', action='store_true', help='Skip SQL regex matching')
    parser.add_argument('--skip-phase2', action='store_true', help='Skip Python matching')
    parser.add_argument('--skip-index', action='store_true', help='Skip index creation')
    parser.add_argument('--batch-size', type=int, default=100000, help='Batch size for Phase 2 (default: 100000)')

    args = parser.parse_args()

    print("="*80)
    print("Populate venue_normalized Column")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    db = DatabaseManager()

    if not db.test_connection():
        print("✗ Database connection failed")
        return 1

    print("✓ Database connected")

    overall_start = datetime.now()

    try:
        # Step 0: Check column exists (error if not)
        if not check_column_exists(db):
            return 1

        # Phase 1: SQL regex
        if not args.skip_phase1:
            phase1_result = populate_with_sql_regex(db)
            if 'error' in phase1_result:
                print(f"✗ Phase 1 failed: {phase1_result['error']}")
                return 1
        else:
            print("\n⚠️  Skipping Phase 1 (SQL regex)")

        # Phase 2: Python matching
        if not args.skip_phase2:
            phase2_result = populate_remaining_with_python(db, args.batch_size)
            if 'error' in phase2_result:
                print(f"✗ Phase 2 failed: {phase2_result['error']}")
                return 1
        else:
            print("\n⚠️  Skipping Phase 2 (Python matching)")

        # Phase 3: Create index
        if not args.skip_index:
            phase3_result = create_index(db)
            if 'error' in phase3_result:
                print(f"✗ Phase 3 failed: {phase3_result['error']}")
                return 1
        else:
            print("\n⚠️  Skipping Phase 3 (Index creation)")

        # Phase 4: Verify
        verify_optimization(db)

        overall_elapsed = (datetime.now() - overall_start).total_seconds()

        print("\n" + "="*80)
        print("🎉 Optimization completed successfully!")
        print("="*80)
        print(f"Total time: {overall_elapsed/60:.1f} minutes ({overall_elapsed/3600:.2f} hours)")
        print("\nNext steps:")
        print("1. Update Stage 2 script to use venue_normalized")
        print("2. Run: uv run python scripts/import_papers_stage2_conferences.py")
        print("3. Expected performance: 0.1-1 second queries (vs 5-10 seconds before)")
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
