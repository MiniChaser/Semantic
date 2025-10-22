-- ============================================================================
-- PARTITIONING QUICK REFERENCE
-- ============================================================================
-- Quick SQL commands for managing partitioned dataset_papers table
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CHECK PARTITION STATUS
-- ----------------------------------------------------------------------------

-- Check if table is partitioned
SELECT EXISTS (
    SELECT 1 FROM pg_partitioned_table
    WHERE partrelid = 'dataset_papers'::regclass
) as is_partitioned;

-- Count total partitions
SELECT count(*) as partition_count
FROM pg_inherits
WHERE inhparent = 'dataset_papers'::regclass;
-- Expected: 34

-- List all partitions with boundaries
SELECT
    child.relname AS partition_name,
    pg_get_expr(child.relpartbound, child.oid) AS partition_bounds
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'dataset_papers'
ORDER BY child.relname;

-- ----------------------------------------------------------------------------
-- 2. PARTITION SIZE AND ROW COUNT
-- ----------------------------------------------------------------------------

-- View all partitions with sizes and row counts
SELECT
    schemaname || '.' || tablename as partition,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) -
                   pg_relation_size(schemaname||'.'||tablename)) as index_size,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE tablename LIKE 'dataset_papers%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Summary by partition type
SELECT
    CASE
        WHEN tablename = 'dataset_papers' THEN 'Parent Table'
        WHEN tablename = 'dataset_papers_null' THEN 'NULL Years'
        WHEN tablename LIKE 'dataset_papers_____' THEN 'Annual (2001-2030)'
        WHEN tablename LIKE 'dataset_papers_%_%' THEN 'Historical'
        ELSE 'Future'
    END as partition_type,
    COUNT(*) as partition_count,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_size,
    SUM(n_live_tup) as total_rows
FROM pg_stat_user_tables
WHERE tablename LIKE 'dataset_papers%'
GROUP BY partition_type
ORDER BY total_rows DESC;

-- ----------------------------------------------------------------------------
-- 3. DATA DISTRIBUTION
-- ----------------------------------------------------------------------------

-- Row count per partition (active partitions only)
SELECT
    tablename,
    n_live_tup as row_count,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as size
FROM pg_stat_user_tables
WHERE tablename LIKE 'dataset_papers_%'
AND n_live_tup > 0
ORDER BY n_live_tup DESC;

-- Year distribution across partitions
SELECT
    CASE
        WHEN year IS NULL THEN 'NULL'
        WHEN year < 1970 THEN '0-1969'
        WHEN year < 1990 THEN '1970-1989'
        WHEN year < 2001 THEN '1990-2000'
        WHEN year < 2031 THEN '2001-2030'
        ELSE '2031+'
    END AS year_range,
    MIN(year) as min_year,
    MAX(year) as max_year,
    COUNT(*) as paper_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dataset_papers
GROUP BY year_range
ORDER BY min_year NULLS FIRST;

-- Top 10 years by paper count
SELECT
    year,
    COUNT(*) as paper_count,
    pg_size_pretty(pg_column_size(year) * COUNT(*)) as year_column_size
FROM dataset_papers
WHERE year IS NOT NULL
GROUP BY year
ORDER BY paper_count DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- 4. INDEX INFORMATION
-- ----------------------------------------------------------------------------

-- List all indexes on partitions
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE tablename LIKE 'dataset_papers%'
ORDER BY tablename, indexname;

-- Index size summary
SELECT
    indexname,
    COUNT(*) as partition_count,
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) as total_size
FROM pg_stat_user_indexes
WHERE tablename LIKE 'dataset_papers_%'  -- Child partitions only
GROUP BY indexname
ORDER BY SUM(pg_relation_size(indexrelid)) DESC;

-- ----------------------------------------------------------------------------
-- 5. QUERY PERFORMANCE TESTING
-- ----------------------------------------------------------------------------

-- Test query with partition pruning (should scan only 1 partition)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM dataset_papers
WHERE year = 2023 AND conference_normalized = 'ICLR';
-- Look for: "Seq Scan on dataset_papers_2023"

-- Test query without year filter (scans all partitions)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM dataset_papers
WHERE conference_normalized = 'ICLR';
-- Look for: Multiple "Seq Scan on dataset_papers_*"

-- Test range query (should scan multiple partitions)
EXPLAIN (ANALYZE, BUFFERS)
SELECT year, COUNT(*) FROM dataset_papers
WHERE year BETWEEN 2020 AND 2024
GROUP BY year;
-- Look for: Scans on dataset_papers_2020 through dataset_papers_2024

-- ----------------------------------------------------------------------------
-- 6. MAINTENANCE OPERATIONS
-- ----------------------------------------------------------------------------

-- Vacuum specific partition
VACUUM ANALYZE dataset_papers_2024;

-- Vacuum all partitions (can be parallelized)
-- Run these in separate sessions for parallel execution:
VACUUM ANALYZE dataset_papers_2020;
VACUUM ANALYZE dataset_papers_2021;
VACUUM ANALYZE dataset_papers_2022;
VACUUM ANALYZE dataset_papers_2023;
VACUUM ANALYZE dataset_papers_2024;

-- Reindex specific partition
REINDEX TABLE dataset_papers_2024;

-- Analyze statistics for all partitions
ANALYZE dataset_papers;

-- Check for dead tuples (candidates for VACUUM)
SELECT
    schemaname,
    tablename,
    n_live_tup,
    n_dead_tup,
    ROUND(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_tuple_percent,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE tablename LIKE 'dataset_papers%'
AND n_dead_tup > 0
ORDER BY dead_tuple_percent DESC;

-- ----------------------------------------------------------------------------
-- 7. DATA INTEGRITY CHECKS
-- ----------------------------------------------------------------------------

-- Check for orphaned records (should return 0)
-- Records in partitions that don't match partition bounds
SELECT
    'dataset_papers_null' as partition,
    COUNT(*) as invalid_records
FROM dataset_papers_null
WHERE year IS NOT NULL
UNION ALL
SELECT
    'dataset_papers_2023',
    COUNT(*)
FROM dataset_papers_2023
WHERE year IS NULL OR year < 2023 OR year >= 2024;
-- Add more checks as needed

-- Verify uniqueness of corpus_id across all partitions
SELECT corpus_id, COUNT(*) as duplicate_count
FROM dataset_papers
GROUP BY corpus_id
HAVING COUNT(*) > 1;
-- Should return 0 rows

-- Check for NULL required fields
SELECT
    COUNT(*) FILTER (WHERE corpus_id IS NULL) as null_corpus_id,
    COUNT(*) FILTER (WHERE title IS NULL) as null_title,
    COUNT(*) FILTER (WHERE release_id IS NULL) as null_release_id
FROM dataset_papers;
-- All should be 0

-- ----------------------------------------------------------------------------
-- 8. ADDING NEW PARTITIONS (for years beyond 2030)
-- ----------------------------------------------------------------------------

-- Step 1: Create new annual partition
CREATE TABLE dataset_papers_2031 PARTITION OF dataset_papers
    FOR VALUES FROM (2031) TO (2032);

-- Step 2: Recreate future partition with adjusted bounds
DROP TABLE dataset_papers_2031_plus;
CREATE TABLE dataset_papers_2031_plus PARTITION OF dataset_papers
    FOR VALUES FROM (2032) TO (MAXVALUE);

-- Repeat for each new year...

-- ----------------------------------------------------------------------------
-- 9. ARCHIVING OLD DATA
-- ----------------------------------------------------------------------------

-- Option 1: Detach partition (preserves data as standalone table)
ALTER TABLE dataset_papers DETACH PARTITION dataset_papers_1970_1990;
-- Now 'dataset_papers_1970_1990' is a regular table
-- Can be archived to cold storage

-- Option 2: Drop partition entirely (permanent deletion!)
DROP TABLE dataset_papers_1970_1990;
-- WARNING: This permanently deletes all data in the partition!

-- ----------------------------------------------------------------------------
-- 10. BACKUP AND RESTORE
-- ----------------------------------------------------------------------------

-- Backup specific partition
-- Using pg_dump
-- pg_dump -U postgres -d semantic_scholar -t dataset_papers_2024 -f dataset_papers_2024_backup.sql

-- Backup entire partitioned table
-- pg_dump -U postgres -d semantic_scholar -t dataset_papers -f dataset_papers_backup.sql

-- Restore partition
-- psql -U postgres -d semantic_scholar -f dataset_papers_2024_backup.sql

-- ----------------------------------------------------------------------------
-- 11. MONITORING QUERIES
-- ----------------------------------------------------------------------------

-- Recent activity per partition
SELECT
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del
FROM pg_stat_user_tables
WHERE tablename LIKE 'dataset_papers%'
ORDER BY (seq_scan + idx_scan) DESC
LIMIT 20;

-- Cache hit ratio per partition
SELECT
    schemaname,
    tablename,
    heap_blks_read,
    heap_blks_hit,
    CASE
        WHEN heap_blks_hit + heap_blks_read = 0 THEN 0
        ELSE ROUND(heap_blks_hit::numeric / (heap_blks_hit + heap_blks_read) * 100, 2)
    END as cache_hit_ratio
FROM pg_statio_user_tables
WHERE tablename LIKE 'dataset_papers%'
ORDER BY cache_hit_ratio ASC;

-- Long-running queries on partitioned table
SELECT
    pid,
    now() - query_start as duration,
    state,
    query
FROM pg_stat_activity
WHERE query LIKE '%dataset_papers%'
AND state != 'idle'
ORDER BY duration DESC;

-- ----------------------------------------------------------------------------
-- 12. TROUBLESHOOTING
-- ----------------------------------------------------------------------------

-- Find papers that would fail INSERT (outside partition bounds)
-- This query helps identify data issues before they cause INSERT failures
WITH bounds AS (
    SELECT
        COALESCE(MIN(year), 0) as min_year,
        COALESCE(MAX(year), 2100) as max_year
    FROM dataset_papers
)
SELECT
    year,
    COUNT(*) as paper_count,
    'Outside bounds' as issue
FROM dataset_papers, bounds
WHERE year < 0 OR year > 2100
GROUP BY year
UNION ALL
SELECT
    NULL as year,
    COUNT(*) as paper_count,
    'NULL year (OK - goes to null partition)' as issue
FROM dataset_papers
WHERE year IS NULL;

-- Check constraint validation on partitions
SELECT
    conrelid::regclass AS table_name,
    conname AS constraint_name,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conrelid IN (
    SELECT inhrelid FROM pg_inherits
    WHERE inhparent = 'dataset_papers'::regclass
)
ORDER BY table_name;

-- ============================================================================
-- END OF QUICK REFERENCE
-- ============================================================================
