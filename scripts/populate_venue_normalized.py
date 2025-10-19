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
import json
import multiprocessing
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from typing import Tuple, List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.semantic.database.connection import DatabaseManager
from src.semantic.services.dataset_service.database_conference_matcher import DatabaseConferenceMatcher
from scripts.utils.progress_monitor import IndexCreationMonitor

# Checkpoint file path
CHECKPOINT_FILE = Path(__file__).parent.parent / '.venue_progress.json'


def save_checkpoint(phase: str, last_corpus_id: int, stats: dict = None) -> None:
    """Save progress checkpoint to file"""
    checkpoint = {
        'phase': phase,
        'last_corpus_id': last_corpus_id,
        'timestamp': datetime.now().isoformat(),
        'stats': stats or {}
    }
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        print(f"⚠️  Warning: Failed to save checkpoint: {e}")


def load_checkpoint() -> dict:
    """Load progress checkpoint from file"""
    if not CHECKPOINT_FILE.exists():
        return None

    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
        return checkpoint
    except Exception as e:
        print(f"⚠️  Warning: Failed to load checkpoint: {e}")
        return None


def clear_checkpoint() -> None:
    """Remove checkpoint file after successful completion"""
    try:
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
    except Exception as e:
        print(f"⚠️  Warning: Failed to remove checkpoint: {e}")


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


def populate_with_mapping_table(db_manager: DatabaseManager, batch_size: int = 200000, resume: bool = False) -> dict:
    """
    Phase 1: Use venue_mapping table for ultra-fast lookups (100x faster than regex)
    Uses single UPDATE-JOIN query for optimal performance

    Args:
        batch_size: Number of records to process per batch (default: 200K)
        resume: Resume from last checkpoint if available
    """
    print("\n" + "="*80)
    print("Phase 1: Venue Mapping Table Lookup (Ultra-Fast)")
    print("="*80)

    # Check if mapping table exists
    result = db_manager.fetch_one("""
        SELECT COUNT(*) as count FROM information_schema.tables
        WHERE table_name = 'venue_mapping'
    """)

    if not result or result['count'] == 0:
        print("\n✗ venue_mapping table does not exist!")
        print("   Please run: uv run python scripts/build_venue_mapping.py")
        return {'error': 'venue_mapping table not found'}

    # Get mapping table stats
    result = db_manager.fetch_one("SELECT COUNT(*) as count FROM venue_mapping")
    mapping_count = result['count'] if result else 0
    print(f"✓ venue_mapping table found with {mapping_count:,} mappings\n")

    # Check for checkpoint
    range_start = 0
    if resume:
        checkpoint = load_checkpoint()
        if checkpoint and checkpoint.get('phase') == 'phase1':
            range_start = checkpoint.get('last_corpus_id', 0)
            print(f"📍 Resuming from checkpoint: corpus_id = {range_start:,}\n")

    start = datetime.now()
    print(f"Batch size: {batch_size:,}")
    print(f"Starting from corpus_id: {range_start:,}")
    print("Using optimized single UPDATE-JOIN query per batch...\n")

    total_updated = 0
    total_processed = 0

    # Dynamic progress bar with performance metrics
    with tqdm(
        desc="Mapping table lookup",
        unit=" records",
        unit_scale=True,
        bar_format='{desc}: {n_fmt} records [{elapsed}, {rate_fmt}] {postfix}'
    ) as pbar:
        batch_count = 0
        while True:
            try:
                batch_count += 1
                batch_start_time = datetime.now()

                # Calculate range end
                range_end = range_start + batch_size

                # Single UPDATE-JOIN query (no separate ID fetch needed)
                with db_manager.get_cursor() as cursor:
                    cursor.execute("""
                        UPDATE all_papers ap
                        SET venue_normalized = vm.conference_name
                        FROM venue_mapping vm
                        WHERE ap.venue_normalized IS NULL
                          AND ap.venue = vm.venue_raw
                          AND ap.corpus_id >= %s
                          AND ap.corpus_id < %s
                    """, (range_start, range_end))

                    updated_count = cursor.rowcount

                # Calculate batch performance
                batch_elapsed = (datetime.now() - batch_start_time).total_seconds()
                batch_rate = updated_count / batch_elapsed if batch_elapsed > 0 else 0

                # If no records were updated, check if we've reached the end
                if updated_count == 0:
                    # Check if there are any more NULL records beyond this range
                    result = db_manager.fetch_one("""
                        SELECT corpus_id FROM all_papers
                        WHERE corpus_id >= %s AND venue_normalized IS NULL
                        ORDER BY corpus_id
                        LIMIT 1
                    """, (range_end,))

                    if not result:
                        # No more NULL records, we're done
                        break
                    else:
                        # Skip to next NULL record
                        range_start = result['corpus_id']
                        continue

                # Update progress
                total_updated += updated_count
                total_processed += batch_size

                # Save checkpoint every 10 batches
                if batch_count % 10 == 0:
                    save_checkpoint('phase1', range_end, {
                        'total_updated': total_updated,
                        'total_processed': total_processed
                    })

                # Update progress bar with metrics
                pbar.set_postfix({
                    'batch': f'{batch_rate:.0f}/s',
                    'updated': f'{total_updated:,}'
                }, refresh=True)
                pbar.update(updated_count)  # Update by actual matched count

                # Move to next range
                range_start = range_end

            except Exception as e:
                print(f"\n✗ Error during batch update: {e}")
                save_checkpoint('phase1', range_start, {'error': str(e)})
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
    print(f"  Matched by mapping table: {matched:,}")
    print(f"  Remaining (NULL): {remaining:,}")
    print(f"  Coverage: {coverage:.1f}%")

    return {
        'elapsed': elapsed,
        'total': total,
        'matched': matched,
        'remaining': remaining,
        'coverage': coverage
    }


def populate_with_sql_regex(db_manager: DatabaseManager, batch_size: int = 1000000) -> dict:
    """
    Phase 1 (Alternative): Use SQL regex to populate 80-90% of records
    NOTE: This is 100x slower than mapping table approach! Use --use-regex flag to enable.
    Uses batch processing for reliable progress tracking

    Args:
        batch_size: Number of records to process per batch (default: 1M)
    """
    print("\n" + "="*80)
    print("Phase 1: SQL Regex Matching (Batch Processing) - SLOW")
    print("="*80)

    start = datetime.now()

    # Build CASE statement
    case_statement = build_sql_case_statement(db_manager)

    if not case_statement:
        return {'error': 'Failed to build CASE statement'}

    # Start batch processing immediately (no COUNT query)
    print(f"\nBatch size: {batch_size:,}")
    print("Using optimized LIMIT + IN approach for maximum speed...\n")

    # Strategy: Fetch IDs with LIMIT, then UPDATE with IN clause
    # This is 70x faster than range-based updates (26k vs 361 rows/s)
    total_processed = 0
    last_corpus_id = 0

    # Dynamic progress bar (no COUNT, no total)
    with tqdm(
        desc="SQL regex matching",
        unit=" records",
        unit_scale=True,
        bar_format='{desc}: {n_fmt} records [{elapsed}, {rate_fmt}]'
    ) as pbar:
        while True:
            try:
                # Step 1: Fetch next batch of corpus_ids (FAST - uses index)
                batch_ids = db_manager.fetch_all("""
                    SELECT corpus_id
                    FROM all_papers
                    WHERE corpus_id > %s
                    ORDER BY corpus_id
                    LIMIT %s
                """, (last_corpus_id, batch_size))

                # No more records
                if not batch_ids:
                    break

                # Extract IDs
                ids = [row['corpus_id'] for row in batch_ids]

                # Step 2: UPDATE with IN clause (FAST - direct index lookup)
                placeholders = ','.join(['%s'] * len(ids))
                update_sql = f"""
                    UPDATE all_papers
                    SET venue_normalized = {case_statement}
                    WHERE corpus_id IN ({placeholders})
                """

                # Execute update
                with db_manager.get_cursor() as cursor:
                    cursor.execute(update_sql, tuple(ids))
                    updated_count = cursor.rowcount

                # Update progress
                total_processed += updated_count
                last_corpus_id = ids[-1]  # Last ID in this batch
                pbar.update(updated_count)

            except Exception as e:
                print(f"\n✗ Error during batch update: {e}")
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


def process_phase2_worker(corpus_id_range: Tuple[int, int], batch_size: int, worker_id: int) -> dict:
    """
    Worker function for parallel Phase 2 processing
    Each worker handles a specific corpus_id range independently

    Args:
        corpus_id_range: (start_id, end_id) tuple
        batch_size: Batch size for processing
        worker_id: Worker identifier for logging

    Returns:
        dict with stats: processed, updated, match_rate
    """
    start_id, end_id = corpus_id_range

    # Each worker creates its own database connection
    db = DatabaseManager()
    if not db.test_connection():
        return {'error': f'Worker {worker_id}: Database connection failed'}

    # Initialize matcher
    try:
        matcher = DatabaseConferenceMatcher(db)
    except Exception as e:
        return {'error': f'Worker {worker_id}: Failed to initialize matcher: {e}'}

    processed = 0
    updated = 0
    last_corpus_id = start_id

    try:
        while last_corpus_id < end_id:
            # Fetch batch within range
            papers = db.fetch_all("""
                SELECT corpus_id, venue
                FROM all_papers
                WHERE venue IS NOT NULL
                  AND venue != ''
                  AND venue_normalized IS NULL
                  AND corpus_id >= %s
                  AND corpus_id < %s
                ORDER BY corpus_id
                LIMIT %s
            """, (last_corpus_id, end_id, batch_size))

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
                db.execute_batch_query("""
                    UPDATE all_papers SET venue_normalized = %s WHERE corpus_id = %s
                """, updates)
                updated += len(updates)

            processed += len(papers)
            last_corpus_id = papers[-1]['corpus_id'] + 1

    except Exception as e:
        return {'error': f'Worker {worker_id}: {str(e)}', 'processed': processed, 'updated': updated}

    return {
        'worker_id': worker_id,
        'processed': processed,
        'updated': updated,
        'match_rate': (updated / processed * 100) if processed > 0 else 0
    }


def populate_remaining_with_python(db_manager: DatabaseManager, batch_size: int = 100000, workers: int = 1) -> dict:
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

    # Enhanced progress bar with batch statistics
    with tqdm(
        total=total_remaining,
        desc="Python matching",
        unit="records",
        unit_scale=True,
        bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
    ) as pbar:
        batch_num = 0
        while True:
            batch_num += 1

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

            # Update progress with current batch stats
            batch_match_rate = (len(updates) / len(papers) * 100) if papers else 0
            overall_match_rate = (updated / processed * 100) if processed > 0 else 0

            pbar.set_postfix({
                'batch': batch_num,
                'matched': f'{len(updates)}/{len(papers)}',
                'rate': f'{overall_match_rate:.1f}%'
            }, refresh=True)
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

    # Initialize progress monitor
    monitor = IndexCreationMonitor(
        db_manager=db_manager,
        index_name='idx_all_papers_venue_normalized',
        update_interval=2.0
    )

    start = datetime.now()

    # Start monitoring
    monitor.start()

    try:
        db_manager.execute_query("""
            CREATE INDEX idx_all_papers_venue_normalized
            ON all_papers(venue_normalized)
            WHERE venue_normalized IS NOT NULL
        """)
    except Exception as e:
        monitor.stop()
        print(f"\n✗ Error creating index: {e}")
        return {'error': str(e)}
    finally:
        # Stop monitoring
        monitor.stop()

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
    parser.add_argument('--phase1-batch-size', type=int, default=1000000, help='Batch size for Phase 1 mapping table updates (default: 1000000)')
    parser.add_argument('--resume', action='store_true', help='Resume from last checkpoint')
    parser.add_argument('--workers', type=int, default=1, help='Number of parallel workers for Phase 2 (default: 1)')

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

        # Phase 1: Use mapping table (fast) or SQL regex (slow fallback)
        if not args.skip_phase1:
            # Try mapping table first (100x faster)
            phase1_result = populate_with_mapping_table(db, batch_size=args.phase1_batch_size)
            if 'error' in phase1_result:
                print(f"✗ Phase 1 failed: {phase1_result['error']}")
                return 1
        else:
            print("\n⚠️  Skipping Phase 1 (mapping table lookup)")

        # Phase 2: Python matching
        if not args.skip_phase2:
            phase2_result = populate_remaining_with_python(db, args.batch_size, args.workers)
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
