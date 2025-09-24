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

#### `services/`
- **author_service/** - Author processing pipeline
  - `authorship_pandas_service.py` - Authorship creation
  - `author_disambiguation_service.py` - Author disambiguation
  - `author_profile_pandas_service.py` - Profile generation
  - `final_author_table_pandas_service.py` - Final table creation
- **dblp_service/** - DBLP data import service
  - `dblp_service.py` - DBLP data processing
- **s2_service/** - Semantic Scholar enrichment service
  - `s2_service.py` - Core S2 API service
  - `s2_enrichment_service.py` - Paper enrichment logic

#### `utils/` - Utility functions
- `config.py` - Configuration management

### `/scripts/` - Executable Scripts

| Script | Purpose |
|--------|---------|
| `setup_database.py` | Database initialization |
| `run_dblp_service_once.py` | DBLP data import |
| `run_s2_enrichment.py` | Semantic Scholar enrichment |
| `run_all_steps.py` | Complete author processing pipeline |
| `step1_create_authorships.py` | Create author-paper relationships |
| `step2_create_author_profiles.py` | Generate author profiles |
| `step3_create_final_table.py` | Create final author table |
| `step4_generate_reports.py` | Generate processing reports |

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