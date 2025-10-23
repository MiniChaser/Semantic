-- ============================================================================
-- Partitioned dataset_papers table schema
-- ============================================================================
-- This schema creates a partitioned version of dataset_papers table using
-- RANGE partitioning by year for better query performance and maintenance.
--
-- Partition Strategy:
-- - NULL years (converted to 0 by application): Stored in 0-1970 partition
-- - Historical (0-1970): Single partition (includes NULL years converted to 0)
-- - Historical (1971-1990): Single partition
-- - Historical (1991-2000): Single partition
-- - Modern (2001-2030): Annual partitions (30 partitions)
-- - Future (2031+): MAXVALUE partition
-- Total: 34 partitions
--
-- NOTE: The application automatically converts NULL year values to 0 before insertion.
-- ============================================================================

-- Drop existing table if converting from non-partitioned
-- WARNING: This will delete all data. Use migration script for safe conversion.
-- DROP TABLE IF EXISTS dataset_papers CASCADE;

-- Create sequence for id column
CREATE SEQUENCE IF NOT EXISTS dataset_papers_id_seq;

-- Create parent table (partitioned by year)
CREATE TABLE IF NOT EXISTS dataset_papers (
    id INTEGER NOT NULL DEFAULT nextval('dataset_papers_id_seq'::regclass),
    corpus_id BIGINT NOT NULL,
    paper_id VARCHAR(100),
    url TEXT,
    dblp_id VARCHAR(255),
    external_ids JSONB,
    title TEXT NOT NULL,
    abstract TEXT,
    venue TEXT,
    year INTEGER,  -- PARTITION KEY
    citation_count INTEGER DEFAULT 0,
    reference_count INTEGER DEFAULT 0,
    influential_citation_count INTEGER DEFAULT 0,
    authors JSONB,
    fields_of_study JSONB,
    publication_types JSONB,
    is_open_access BOOLEAN DEFAULT FALSE,
    open_access_pdf TEXT,
    conference_normalized VARCHAR(100),
    source_file VARCHAR(255),
    release_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (year);

-- ============================================================================
-- Create partitions
-- ============================================================================

-- Invalid years partition (negative years only, should rarely be used)
CREATE TABLE IF NOT EXISTS dataset_papers_null PARTITION OF dataset_papers
    FOR VALUES FROM (MINVALUE) TO (0);

-- Historical partitions (coarse granularity)
-- NOTE: This partition includes year=0 (NULL years converted by application)
CREATE TABLE IF NOT EXISTS dataset_papers_0_1970 PARTITION OF dataset_papers
    FOR VALUES FROM (0) TO (1971);  -- 0-1970, includes converted NULL years

CREATE TABLE IF NOT EXISTS dataset_papers_1970_1990 PARTITION OF dataset_papers
    FOR VALUES FROM (1971) TO (1991);  -- 1971-1990

CREATE TABLE IF NOT EXISTS dataset_papers_1991_2001 PARTITION OF dataset_papers
    FOR VALUES FROM (1991) TO (2001);  -- 1991-2000

-- Modern era partitions (annual granularity: 2001-2030)
CREATE TABLE IF NOT EXISTS dataset_papers_2001 PARTITION OF dataset_papers
    FOR VALUES FROM (2001) TO (2002);

CREATE TABLE IF NOT EXISTS dataset_papers_2002 PARTITION OF dataset_papers
    FOR VALUES FROM (2002) TO (2003);

CREATE TABLE IF NOT EXISTS dataset_papers_2003 PARTITION OF dataset_papers
    FOR VALUES FROM (2003) TO (2004);

CREATE TABLE IF NOT EXISTS dataset_papers_2004 PARTITION OF dataset_papers
    FOR VALUES FROM (2004) TO (2005);

CREATE TABLE IF NOT EXISTS dataset_papers_2005 PARTITION OF dataset_papers
    FOR VALUES FROM (2005) TO (2006);

CREATE TABLE IF NOT EXISTS dataset_papers_2006 PARTITION OF dataset_papers
    FOR VALUES FROM (2006) TO (2007);

CREATE TABLE IF NOT EXISTS dataset_papers_2007 PARTITION OF dataset_papers
    FOR VALUES FROM (2007) TO (2008);

CREATE TABLE IF NOT EXISTS dataset_papers_2008 PARTITION OF dataset_papers
    FOR VALUES FROM (2008) TO (2009);

CREATE TABLE IF NOT EXISTS dataset_papers_2009 PARTITION OF dataset_papers
    FOR VALUES FROM (2009) TO (2010);

CREATE TABLE IF NOT EXISTS dataset_papers_2010 PARTITION OF dataset_papers
    FOR VALUES FROM (2010) TO (2011);

CREATE TABLE IF NOT EXISTS dataset_papers_2011 PARTITION OF dataset_papers
    FOR VALUES FROM (2011) TO (2012);

CREATE TABLE IF NOT EXISTS dataset_papers_2012 PARTITION OF dataset_papers
    FOR VALUES FROM (2012) TO (2013);

CREATE TABLE IF NOT EXISTS dataset_papers_2013 PARTITION OF dataset_papers
    FOR VALUES FROM (2013) TO (2014);

CREATE TABLE IF NOT EXISTS dataset_papers_2014 PARTITION OF dataset_papers
    FOR VALUES FROM (2014) TO (2015);

CREATE TABLE IF NOT EXISTS dataset_papers_2015 PARTITION OF dataset_papers
    FOR VALUES FROM (2015) TO (2016);

CREATE TABLE IF NOT EXISTS dataset_papers_2016 PARTITION OF dataset_papers
    FOR VALUES FROM (2016) TO (2017);

CREATE TABLE IF NOT EXISTS dataset_papers_2017 PARTITION OF dataset_papers
    FOR VALUES FROM (2017) TO (2018);

CREATE TABLE IF NOT EXISTS dataset_papers_2018 PARTITION OF dataset_papers
    FOR VALUES FROM (2018) TO (2019);

CREATE TABLE IF NOT EXISTS dataset_papers_2019 PARTITION OF dataset_papers
    FOR VALUES FROM (2019) TO (2020);

CREATE TABLE IF NOT EXISTS dataset_papers_2020 PARTITION OF dataset_papers
    FOR VALUES FROM (2020) TO (2021);

CREATE TABLE IF NOT EXISTS dataset_papers_2021 PARTITION OF dataset_papers
    FOR VALUES FROM (2021) TO (2022);

CREATE TABLE IF NOT EXISTS dataset_papers_2022 PARTITION OF dataset_papers
    FOR VALUES FROM (2022) TO (2023);

CREATE TABLE IF NOT EXISTS dataset_papers_2023 PARTITION OF dataset_papers
    FOR VALUES FROM (2023) TO (2024);

CREATE TABLE IF NOT EXISTS dataset_papers_2024 PARTITION OF dataset_papers
    FOR VALUES FROM (2024) TO (2025);

CREATE TABLE IF NOT EXISTS dataset_papers_2025 PARTITION OF dataset_papers
    FOR VALUES FROM (2025) TO (2026);

CREATE TABLE IF NOT EXISTS dataset_papers_2026 PARTITION OF dataset_papers
    FOR VALUES FROM (2026) TO (2027);

CREATE TABLE IF NOT EXISTS dataset_papers_2027 PARTITION OF dataset_papers
    FOR VALUES FROM (2027) TO (2028);

CREATE TABLE IF NOT EXISTS dataset_papers_2028 PARTITION OF dataset_papers
    FOR VALUES FROM (2028) TO (2029);

CREATE TABLE IF NOT EXISTS dataset_papers_2029 PARTITION OF dataset_papers
    FOR VALUES FROM (2029) TO (2030);

CREATE TABLE IF NOT EXISTS dataset_papers_2030 PARTITION OF dataset_papers
    FOR VALUES FROM (2030) TO (2031);

-- Future years partition (catch-all)
CREATE TABLE IF NOT EXISTS dataset_papers_2031_plus PARTITION OF dataset_papers
    FOR VALUES FROM (2031) TO (MAXVALUE);

-- ============================================================================
-- Create indexes on parent table (automatically propagate to all partitions)
-- ============================================================================
-- Optimized index configuration (5 core indexes):
-- 1. corpus_id + year - Composite UNIQUE constraint (required for UPSERT with ON CONFLICT)
-- 2. conference_normalized - Conference filtering (core functionality)
-- 3. year - Partition key (range queries)
-- 4. dblp_id - DBLP identifier lookups (partial index for non-NULL values)
-- 5. authors (GIN) - Complex author JSONB queries
--
-- Removed indexes (low value, can be recreated later if needed):
-- - venue (replaced by conference_normalized)
-- - release_id (rarely queried)
-- - citation_count (can add back for citation-based sorting if needed)
-- ============================================================================

-- Primary identifier unique constraint (corpus_id + year)
-- Note: PostgreSQL requires partition key (year) in UNIQUE constraints on partitioned tables
-- This enables ON CONFLICT (corpus_id, year) in UPSERT operations
CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_papers_corpus_id_year ON dataset_papers(corpus_id, year);

-- Core query optimization indexes
CREATE INDEX IF NOT EXISTS idx_dataset_papers_conference ON dataset_papers(conference_normalized);
CREATE INDEX IF NOT EXISTS idx_dataset_papers_year ON dataset_papers(year);

-- DBLP identifier index (partial index for space efficiency)
CREATE INDEX IF NOT EXISTS idx_dataset_papers_dblp_id ON dataset_papers(dblp_id) WHERE dblp_id IS NOT NULL;

-- GIN index for JSONB author queries
CREATE INDEX IF NOT EXISTS idx_dataset_papers_authors ON dataset_papers USING GIN (authors);

-- ============================================================================
-- Create triggers for updated_at timestamp
-- ============================================================================

-- Trigger function to update updated_at on row updates
CREATE OR REPLACE FUNCTION update_dataset_papers_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update updated_at column
DROP TRIGGER IF EXISTS trigger_dataset_papers_updated_at ON dataset_papers;
CREATE TRIGGER trigger_dataset_papers_updated_at
    BEFORE UPDATE ON dataset_papers
    FOR EACH ROW
    EXECUTE FUNCTION update_dataset_papers_updated_at();

-- ============================================================================
-- Helpful maintenance queries
-- ============================================================================

-- View partition sizes:
-- SELECT
--     schemaname || '.' || tablename as partition,
--     pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
--     n_live_tup as row_count
-- FROM pg_stat_user_tables
-- WHERE tablename LIKE 'dataset_papers%'
-- ORDER BY tablename;

-- Check partition constraints:
-- SELECT
--     parent.relname AS parent,
--     child.relname AS child,
--     pg_get_expr(child.relpartbound, child.oid) AS bounds
-- FROM pg_inherits
-- JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
-- JOIN pg_class child ON pg_inherits.inhrelid = child.oid
-- WHERE parent.relname = 'dataset_papers'
-- ORDER BY child.relname;
