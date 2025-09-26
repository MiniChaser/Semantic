# Database Schema Documentation

This document provides comprehensive information about all database tables used in the DBLP Semantic Scholar Data Processing Pipeline, their purposes, structures, and key relationships.

## 📊 Table Overview

The system maintains 8 main tables organized into different functional categories:

### Core Data Tables (3 tables)
- `dblp_papers` - Raw DBLP paper data
- `enriched_papers` - Papers enriched with Semantic Scholar data
- `s2_processing_meta` - Semantic Scholar processing metadata

### Author Analysis Tables (3 tables)
- `authorships` - Paper-author relationships
- `author_profiles` - Consolidated author profiles
- `final_author_table` - Final unified author data

### System Tables (2 tables)
- `dblp_processing_meta` - DBLP processing metadata
- `scheduler_jobs` - APScheduler job persistence

---

## 🗃️ Core Data Tables

### 1. dblp_papers
**Purpose**: Store raw DBLP bibliographic data for target conferences

**Structure**:
```sql
CREATE TABLE dblp_papers (
    id                  SERIAL PRIMARY KEY,
    key                 VARCHAR(255) UNIQUE NOT NULL,     -- DBLP unique key (e.g., "conf/acl/2023-1234")
    title               TEXT NOT NULL,                    -- Paper title
    authors             JSONB NOT NULL,                   -- Array of author names
    author_count        INTEGER,                          -- Number of authors
    venue               VARCHAR(50),                      -- Conference venue (acl, naacl, emnlp, etc.)
    year                VARCHAR(4),                       -- Publication year
    pages               VARCHAR(50),                      -- Page numbers
    ee                  TEXT,                             -- Electronic edition URL
    booktitle           TEXT,                             -- Conference proceedings title
    doi                 VARCHAR(100),                     -- Digital Object Identifier
    create_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Key Indexes**:
- `idx_dblp_papers_venue` - Fast venue filtering
- `idx_dblp_papers_year` - Temporal queries
- `idx_dblp_papers_authors (GIN)` - Author search within JSONB
- `idx_dblp_papers_doi` - DOI-based lookups

**Sample Data**:
```json
{
    "key": "conf/acl/SmithJ23",
    "title": "Neural Language Models for Academic Writing",
    "authors": ["John Smith", "Jane Doe"],
    "venue": "acl",
    "year": "2023",
    "doi": "10.18653/v1/2023.acl-long.123"
}
```

---

### 2. enriched_papers
**Purpose**: Store papers enriched with Semantic Scholar metadata (54 fields total)

**Field Categories**:

#### DBLP Fields (12 fields)
```sql
dblp_paper_id         INTEGER REFERENCES dblp_papers(id),
dblp_id               INTEGER,
dblp_key              VARCHAR(255),
dblp_title            TEXT,
dblp_authors          JSONB,
dblp_year             VARCHAR(4),
dblp_pages            VARCHAR(50),
dblp_url              TEXT,
dblp_venue            VARCHAR(50),
dblp_created_at       TIMESTAMP,
dblp_first_author     TEXT,
dblp_last_author      TEXT,
first_author_dblp_id  TEXT,
```

#### Semantic Scholar Basic (9 fields)
```sql
semantic_paper_id     VARCHAR(50),        -- S2 unique paper ID
semantic_title        TEXT,               -- S2 paper title
semantic_year         INTEGER,            -- S2 publication year
semantic_venue        TEXT,               -- S2 venue information
semantic_abstract     TEXT,               -- Paper abstract
semantic_url          TEXT,               -- S2 paper URL
semantic_created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
semantic_updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
```

#### Citation Metrics (3 fields)
```sql
semantic_citation_count     INTEGER,      -- Total citation count
semantic_reference_count    INTEGER,      -- Total reference count
influentialCitationCount    INTEGER,      -- Influential citation count
```

#### Author Information (5 fields)
```sql
semantic_authors           JSONB,         -- Array of S2 author objects with IDs
first_author_semantic_id   VARCHAR(50),   -- S2 ID of first author
all_authors_count          INTEGER,       -- Total number of authors
all_author_names           TEXT,          -- Concatenated author names
all_author_ids             TEXT,          -- Concatenated S2 author IDs
```

#### Research Fields (4 fields)
```sql
semantic_fields_of_study   JSONB,         -- S2 field classifications
s2_fields_primary          TEXT,          -- Primary research field
s2_fields_secondary        TEXT,          -- Secondary research fields
s2_fields_all              TEXT,          -- All research fields concatenated
```

#### External Identifiers (7 fields)
```sql
semantic_external_ids      JSONB,         -- All external IDs from S2
doi                        VARCHAR(100),   -- Digital Object Identifier
arxiv_id                   VARCHAR(50),    -- arXiv identifier
mag_id                     VARCHAR(50),    -- Microsoft Academic Graph ID
acl_id                     VARCHAR(50),    -- ACL Anthology ID
corpus_id                  VARCHAR(50),    -- S2 Corpus ID
pmid                       VARCHAR(50),    -- PubMed ID
```

#### Open Access Information (4 fields)
```sql
open_access_url           TEXT,           -- Direct PDF download URL
open_access_status        VARCHAR(50),    -- Open access status
open_access_license       VARCHAR(100),   -- License information
pdf_available             VARCHAR(10),    -- "TRUE"/"FALSE" PDF availability
```

#### PDF Management (2 fields)
```sql
pdf_filename              VARCHAR(255),   -- Downloaded PDF filename
pdf_file_path             TEXT,           -- Full path to PDF file
```

#### Validation & Quality (5 fields)
```sql
match_method              VARCHAR(100),   -- How paper was matched with S2
validation_tier           VARCHAR(50),    -- Tier2_SimilarityMatch, Tier3_FuzzyMatch, etc.
match_confidence          DECIMAL(5,3),   -- Confidence score (0.000-99.999)
data_source_primary       VARCHAR(50),    -- Primary data source
data_completeness_score   DECIMAL(5,3),   -- Completeness score
```

**Matching Tiers**:
- **Tier2_SimilarityMatch**: Title and author similarity matching
- **Tier3_FuzzyMatch**: Fuzzy matching with confidence scoring

**Key Indexes**:
- `idx_enriched_papers_semantic_id` - S2 paper ID lookups
- `idx_enriched_papers_validation_tier` - Quality filtering
- `idx_enriched_papers_authors (GIN)` - Author searches
- `idx_enriched_papers_venue_year` - Composite venue+year queries

---

### 3. s2_processing_meta
**Purpose**: Track Semantic Scholar enrichment process statistics

```sql
CREATE TABLE s2_processing_meta (
    id                  SERIAL PRIMARY KEY,
    process_type        VARCHAR(50) NOT NULL,           -- "s2_enrichment"
    last_run_time       TIMESTAMP NOT NULL,             -- When process last ran
    status              VARCHAR(20) NOT NULL,           -- success, failed, partial_success, running
    records_processed   INTEGER DEFAULT 0,              -- Papers processed this run
    records_inserted    INTEGER DEFAULT 0,              -- New papers added
    records_updated     INTEGER DEFAULT 0,              -- Papers updated
    records_tier2       INTEGER DEFAULT 0,              -- Tier2 matches
    records_tier3       INTEGER DEFAULT 0,              -- Tier3 matches
    api_calls_made      INTEGER DEFAULT 0,              -- S2 API calls made
    error_message       TEXT,                           -- Error details if failed
    execution_duration  INTEGER,                        -- Duration in seconds
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 👥 Author Analysis Tables

### 4. authorships
**Purpose**: Store paper-author relationships with matching metadata

```sql
CREATE TABLE authorships (
    id                    SERIAL PRIMARY KEY,
    paper_id              INTEGER NOT NULL REFERENCES enriched_papers(id),
    semantic_paper_id     VARCHAR(255),                 -- S2 paper ID
    paper_title           TEXT,                         -- Paper title for reference
    dblp_author_name      TEXT NOT NULL,                -- Author name from DBLP
    s2_author_name        TEXT,                         -- Author name from S2
    s2_author_id          VARCHAR(255),                 -- S2 author unique ID
    authorship_order      INTEGER NOT NULL,             -- Position in author list
    match_confidence      VARCHAR(50) NOT NULL,         -- matched/unmatched
    match_method          VARCHAR(100),                 -- multi_tier_matching, no_match_found
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Key Features**:
- Links papers to individual authors
- Preserves authorship order (first, second, last author positions)
- Tracks author name matching between DBLP and S2
- Enables author disambiguation analysis

---

### 5. author_profiles
**Purpose**: Consolidated author profiles with basic statistics

```sql
CREATE TABLE author_profiles (
    id                    SERIAL PRIMARY KEY,
    s2_author_id          VARCHAR(255),                 -- S2 author ID (may be NULL for unmatched)
    dblp_author_name      TEXT NOT NULL,                -- Primary DBLP name
    s2_author_name        TEXT,                         -- Primary S2 name
    paper_count           INTEGER DEFAULT 0,            -- Total papers authored
    total_citations       INTEGER DEFAULT 0,            -- Sum of all paper citations
    career_length         INTEGER DEFAULT 0,            -- Years from first to last publication
    first_author_count    INTEGER DEFAULT 0,            -- Papers as first author
    last_author_count     INTEGER DEFAULT 0,            -- Papers as last author  
    first_author_ratio    DECIMAL(5,3) DEFAULT 0,       -- first_author_count / paper_count
    last_author_ratio     DECIMAL(5,3) DEFAULT 0,       -- last_author_count / paper_count
    avg_citations_per_paper DECIMAL(8,2) DEFAULT 0,     -- total_citations / paper_count
    first_publication_year INTEGER,                     -- Career start year
    latest_publication_year INTEGER,                    -- Most recent publication
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

### 6. final_author_table
**Purpose**: Final unified author table with all computed metrics

```sql
CREATE TABLE final_author_table (
    id                                   SERIAL PRIMARY KEY,
    
    -- Core Identification
    dblp_author                         VARCHAR(500) NOT NULL,    -- Primary DBLP name
    note                                VARCHAR(500),             -- Currently empty as specified
    
    -- External IDs (TODO sections as specified)
    google_scholarid                    VARCHAR(255),             -- TODO: Google Scholar integration
    external_ids_dblp                   TEXT,                     -- DBLP aliases (4-digit disambiguation)
    
    -- Institution Information  
    semantic_scholar_affiliations       TEXT,                     -- TODO: S2 Author API needed
    csrankings_affiliation              TEXT,                     -- TODO: CSRankings integration
    
    -- Publication Statistics
    dblp_top_paper_total_paper_captured INTEGER DEFAULT 0,       -- TODO: Top venue definition
    dblp_top_paper_last_author_count    INTEGER DEFAULT 0,       -- TODO: Top venue definition  
    first_author_count                  INTEGER DEFAULT 0,       -- From authorships + author_profiles
    semantic_scholar_paper_count        INTEGER,                 -- Computed from enriched_papers
    
    -- Career Metrics
    career_length                       INTEGER DEFAULT 0,       -- From author_profiles
    last_author_percentage              NUMERIC DEFAULT 0,       -- Calculated from author_profiles
    
    -- Citation and Impact
    total_influential_citations         INTEGER,                 -- Sum from enriched_papers
    semantic_scholar_citation_count     INTEGER,                 -- Sum from enriched_papers  
    semantic_scholar_h_index            INTEGER,                 -- Calculated H-index
    
    -- Identity and Contact
    name                               VARCHAR(500) NOT NULL,    -- Display name
    name_snapshot                      VARCHAR(500),             -- Name variation tracking
    affiliations_snapshot              TEXT,                     -- Currently empty
    homepage                           TEXT,                     -- TODO: S2 Author API
    
    -- Internal Tracking
    s2_author_id                       VARCHAR(255),             -- Reference to author_profiles
    data_source_notes                  TEXT,                     -- Processing metadata
    created_at                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Key Computational Features**:
- **H-index Calculation**: Computed directly from paper citation counts
- **DBLP Alias Extraction**: Uses 4-digit disambiguation pattern matching
- **Comprehensive Citation Metrics**: Sums influential citations and regular citations
- **Career Length Tracking**: From first to most recent publication
- **Authorship Analysis**: First author counts and percentages

---

## 🛠️ System Tables

### 7. dblp_processing_meta
**Purpose**: Track DBLP data processing operations

```sql
CREATE TABLE dblp_processing_meta (
    id                  SERIAL PRIMARY KEY,
    process_type        VARCHAR(50) NOT NULL,           -- "dblp_import", "xml_parsing"  
    last_run_time       TIMESTAMP NOT NULL,
    status              VARCHAR(20) NOT NULL CHECK (status IN 
                         ('success', 'failed', 'partial_success', 'running')),
    records_processed   INTEGER DEFAULT 0,
    records_inserted    INTEGER DEFAULT 0,
    records_updated     INTEGER DEFAULT 0,
    error_message       TEXT,
    execution_duration  INTEGER,                        -- Duration in seconds
    create_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

### 8. scheduler_jobs
**Purpose**: APScheduler job persistence for automated processing

```sql
CREATE TABLE scheduler_jobs (
    id              VARCHAR(191) PRIMARY KEY,           -- APScheduler job ID
    next_run_time   DOUBLE PRECISION,                   -- Unix timestamp
    job_state       BYTEA NOT NULL                      -- Serialized job state
);
```

**Usage**: 
- Enables persistent scheduling across application restarts
- Stores comprehensive pipeline jobs (weekly 7-day cycles)
- Tracks job execution state and next run times

---

## 🔄 Data Flow and Relationships

### Simplified Processing Pipeline Flow:

1. **DBLP Import**: `dblp_papers` ← XML parsing
2. **S2 Enrichment**: `enriched_papers` ← DBLP + Semantic Scholar API
3. **Author Extraction**: `authorships` ← enriched_papers analysis  
4. **Profile Building**: `author_profiles` ← authorships aggregation
5. **Final Integration**: `final_author_table` ← direct calculation from authorships + enriched_papers

### Key Relationships:

```sql
-- Core paper relationship
enriched_papers.dblp_paper_id → dblp_papers.id

-- Author relationships  
authorships.paper_id → enriched_papers.id
author_profiles.s2_author_id ← authorships.s2_author_id
final_author_table.s2_author_id → author_profiles.s2_author_id

-- All final metrics computed directly from:
-- authorships + enriched_papers + author_profiles
```

---

## 📈 Direct Calculation Approach

The system now uses a **simplified approach** where intermediate metric tables are skipped:

### Metrics Computed Directly in final_author_table:

1. **H-Index Calculation**:
   ```sql
   -- Computed from paper citation counts, sorted descending
   -- H-index = largest h where author has h papers with ≥h citations each
   ```

2. **Citation Aggregations**:
   ```sql
   -- Sum of semantic_citation_count from enriched_papers
   -- Sum of influentialCitationCount from enriched_papers
   ```

3. **Publication Statistics**:
   ```sql
   -- Count of papers from authorships + enriched_papers
   -- First author count from authorships where authorship_order = 1
   ```

4. **DBLP Alias Extraction**:
   ```sql
   -- Pattern matching for 4-digit disambiguation numbers
   -- Query similar names from author_profiles
   ```

### Benefits of Simplified Approach:
- **Reduced Complexity**: No intermediate tables to maintain
- **Better Performance**: Fewer JOIN operations
- **Easier Maintenance**: Single calculation point
- **Storage Efficiency**: Less data duplication

---

## 📊 Usage Examples

### Find highly cited recent papers:
```sql
SELECT semantic_title, semantic_citation_count, semantic_year
FROM enriched_papers 
WHERE semantic_year >= 2020 
  AND semantic_citation_count > 100
ORDER BY semantic_citation_count DESC;
```

### Get top authors by H-index:
```sql
SELECT dblp_author, semantic_scholar_h_index, 
       semantic_scholar_citation_count, career_length
FROM final_author_table
WHERE semantic_scholar_h_index IS NOT NULL
ORDER BY semantic_scholar_h_index DESC
LIMIT 20;
```

### Analyze author career progression:
```sql
SELECT dblp_author, career_length, first_author_count,
       last_author_percentage, semantic_scholar_paper_count
FROM final_author_table
WHERE career_length >= 10
ORDER BY career_length DESC, first_author_count DESC;
```

### Find authors with DBLP aliases:
```sql
SELECT dblp_author, external_ids_dblp
FROM final_author_table
WHERE external_ids_dblp != dblp_author
AND external_ids_dblp LIKE '%;%';
```

---

This simplified schema maintains all essential functionality while significantly reducing system complexity and improving maintainability.