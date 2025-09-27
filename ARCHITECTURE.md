# Project Architecture

## Overview
DBLP Semantic Scholar Data Processing Pipeline - An automated service for processing academic paper data.

## Directory Structure

```
Semantic/
├── src/                    # Main source code
│   └── semantic/           # Core application package
│       ├── database/       # Database layer
│       ├── services/       # Business logic services
│       └── utils/          # Utility functions
├── scripts/                # Executable scripts
├── docker/                 # Docker configuration
├── data/                   # Data storage
├── logs/                   # Application logs
└── external/               # External dependencies
```

## Core Components

### `/src/semantic/` - Main Application Package

#### `database/`
- **models/** - SQLAlchemy ORM models
  - `paper.py` - Paper data model
  - `enriched_paper.py` - Enriched paper model
- **repositories/** - Data access layer
  - `paper.py` - Paper repository
  - `enriched_paper.py` - Enriched paper repository
- **schemas/** - Database schemas
  - `base.py` - Base schema definitions
  - `paper.py`, `enriched_paper.py` - Specific schemas
  - `processing.py`, `scheduler.py` - Processing schemas
- **connection.py** - Database connection management

**Key Tables**: `dblp_papers`, `enriched_papers`, `s2_author_profiles`, `authorships`, `author_profiles`, `final_author_table`

#### `services/`
- **author_service/** - Author processing pipeline
  - `authorship_pandas_service.py` - Authorship creation
  - `author_disambiguation_service.py` - Author disambiguation
  - `author_profile_pandas_service.py` - Profile generation
  - `final_author_table_pandas_service.py` - Final table creation
- **dblp_service/** - DBLP data import service
  - `dblp_service.py` - DBLP data processing
- **s2_service/** - Semantic Scholar enrichment services
  - `s2_service.py` - Core S2 API service
  - `s2_paper_enrichment_service.py` - Paper enrichment logic
  - `s2_author_enrichment_service.py` - Author enrichment logic
  - `s2_author_profile_sync_service.py` - Author profile synchronization
  - `_s2_author_profile_batch_service.py` - Batch author API processing

#### `utils/` - Utility functions
- `config.py` - Configuration management

### `/scripts/` - Executable Scripts

| Script | Purpose |
|--------|---------|
| `setup_database.py` | Database initialization |
| `run_dblp_service_once.py` | DBLP data import |
| `run_s2_enrichment.py` | Semantic Scholar paper enrichment |
| `run_all_steps.py` | Complete author processing pipeline |
| `step1_create_authorships.py` | Create author-paper relationships |
| `step2_create_s2_author_profiles.py` | Create S2 author profiles via API |
| `step3_create_author_profiles.py` | Generate consolidated author profiles |
| `step4_enrich_author_profiles_with_s2.py` | Enrich author profiles with S2 data |
| `step5_create_final_table.py` | Create final author table |
| `step6_generate_reports.py` | Generate processing reports |

### Supporting Directories

- **`docker/`** - Docker configuration files
- **`data/reports/`** - Generated reports storage
- **`logs/`** - Application logs
- **`external/`** - External dependencies

## Data Flow

1. **Setup** → Database initialization
2. **Import** → DBLP data ingestion
3. **Enrich** → Semantic Scholar data enhancement
4. **Process** → Author disambiguation and profiling
5. **Report** → Generate final reports

## Key Configuration Files

- **`pyproject.toml`** - Python project configuration
- **`.env`** - Environment variables
- **`Dockerfile`** - Container configuration
- **`CLAUDE.md`** - Development instructions