# DBLP Semantic Data Processing Pipeline v2.1

A modern DBLP data processing pipeline with scheduling, incremental updates, and modular architecture.

## Key Features

- **🚀 Complete Pipeline**: One-stop processing from download to database storage
- **🔄 Incremental Processing**: Supports incremental updates, avoiding reprocessing existing data  
- **⏰ Scheduled Tasks**: Configurable timed tasks based on APScheduler
- **🏗️ Modular Architecture**: Independent service components for easy scaling and maintenance
- **📊 PostgreSQL Optimized**: Specialized data storage optimized for PostgreSQL
- **⚙️ Environment Configuration**: Manage all settings via .env files
- **📦 UV Project Management**: Modern Python package management with uv
- **📈 Progress Tracking**: Detailed progress bars and logging
- **🛡️ Error Handling**: Comprehensive error handling and retry mechanisms  
- **💾 Batch Processing**: Memory-friendly batch processing mechanisms
- **✨ NEW: Enhanced Time Tracking**: New `create_time`/`update_time` columns with automatic triggers

## Project Architecture

```
semantic/
├── src/semantic/           # Main source code
│   ├── database/          # Database related modules
│   │   ├── connection.py  # Database connection management
│   │   └── models.py      # Data models and repositories
│   ├── services/          # Business service layer
│   │   ├── dblp_service.py      # DBLP data processing service
│   │   └── pipeline_service.py  # Data pipeline service
│   ├── scheduler/         # Task scheduling
│   │   └── scheduler.py   # APScheduler scheduler
│   └── utils/            # Utility modules
│       └── config.py     # Configuration management
├── scripts/              # Execution scripts
│   ├── run_scheduler.py   # Scheduler startup script
│   └── run_pipeline_once.py # Single run script
├── config/              # Configuration files
├── logs/                # Log files
├── data/                # Data files
└── external/            # External download files
```

## Quick Start

### 1. Environment Setup

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone/download project to local
cd semantic

# Install dependencies using uv
uv sync
```

### 2. Database Configuration

Copy environment variable template:
```bash
cp .env.example .env
```

Edit `.env` file and fill in your PostgreSQL connection information:
```bash
# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dblp_semantic
DB_USER=postgres
DB_PASSWORD=your_password

# Processing Configuration
TARGET_VENUES=acl,naacl,emnlp,findings
ENABLE_VENUE_FILTER=true
BATCH_SIZE=10000
LOG_LEVEL=INFO

# Scheduling Configuration  
SCHEDULE_CRON=0 2 * * 1
ENABLE_INCREMENTAL=true
```

### 3. Run Pipeline

#### Single Run
```bash
# Run complete pipeline once using script
./scripts/run_pipeline_once.py

# Or run using uv
uv run python scripts/run_pipeline_once.py
```

#### Start Scheduled Scheduler
```bash
# Start scheduled scheduler (runs according to configured cron expression)
./scripts/run_scheduler.py

# Manual execution once (execute immediately)
./scripts/run_scheduler.py --manual

# List all tasks
./scripts/run_scheduler.py --list-jobs
```

#### Direct Python Module Usage
```bash
# Enter virtual environment
uv shell

# Use Python module
python -m semantic.scheduler.scheduler --manual
```

## Configuration Options

### Database Configuration
- `DB_HOST`: PostgreSQL host address
- `DB_PORT`: PostgreSQL port
- `DB_NAME`: Database name
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password

### Processing Configuration
- `TARGET_VENUES`: Target venue list (comma-separated)
- `ENABLE_VENUE_FILTER`: Whether to enable venue filtering (true/false)
- `BATCH_SIZE`: Batch processing size (default 10000)
- `LOG_LEVEL`: Log level (INFO/DEBUG/WARNING/ERROR)

### Scheduling Configuration
- `SCHEDULE_CRON`: Cron expression (default: 0 2 * * 1, every Monday at 2 AM)
- `MAX_RETRIES`: Maximum retry count (default 3)
- `RETRY_DELAY`: Retry delay in seconds (default 300)

### Incremental Processing Configuration
- `ENABLE_INCREMENTAL`: Whether to enable incremental processing (true/false)
- `INCREMENTAL_CHECK_DAYS`: Incremental check days (default 7)

## Database Table Structure

### Main Data Tables
```sql
CREATE TABLE dblp_papers (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    authors JSONB NOT NULL,
    author_count INTEGER,
    venue VARCHAR(50),
    year VARCHAR(4),
    pages VARCHAR(50),
    ee TEXT,
    booktitle TEXT,
    doi VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Processing Metadata Tables
```sql
CREATE TABLE dblp_processing_meta (
    id SERIAL PRIMARY KEY,
    process_type VARCHAR(50) NOT NULL,
    last_run_time TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Scheduler Job Tables
```sql
CREATE TABLE scheduler_jobs (
    -- Table structure automatically created by APScheduler
);
```

## Workflow

### Incremental Processing Workflow
1. **Check Last Run Time**: Get last successful run time from `dblp_processing_meta` table
2. **Determine Processing Mode**: Decide between full or incremental processing based on configuration and time interval
3. **Data Preparation**: Download and extract DBLP data files
4. **Incremental Parsing**: Only process new papers that don't exist in the database
5. **Batch Update**: Use UPSERT operations to batch insert or update data
6. **Record Metadata**: Record statistics for this processing run

### Scheduler Workflow
1. **Initialization**: Connect to database, set up job storage
2. **Job Registration**: Register scheduled tasks based on Cron expressions
3. **Task Execution**: Execute data pipeline at specified times
4. **Error Handling**: Automatic retry on failure, record error logs
5. **Status Monitoring**: Monitor job execution status, generate execution reports

## Usage Examples

### Programming Interface Usage
```python
from semantic.services.pipeline_service import DataPipelineService
from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager

# Create configuration
config = AppConfig.from_env()

# Create pipeline service
pipeline = DataPipelineService(config)

# Run complete pipeline
success = pipeline.run_pipeline()

# Export data
if success:
    pipeline.export_to_csv("output/papers.csv")
```

### Independent Module Usage
```python
from semantic.services.dblp_service import DBLPService
from semantic.database.models import PaperRepository
from semantic.database.connection import get_db_manager

# Use DBLP service
dblp_service = DBLPService(config)
papers = dblp_service.parse_papers()

# Use database repository
db_manager = get_db_manager()
paper_repo = PaperRepository(db_manager)
paper_repo.batch_insert_papers(papers)
```

## Development Guide

### Install Development Dependencies
```bash
uv sync --dev
```

### Code Formatting
```bash
uv run black src/
uv run isort src/
```

### Code Checking
```bash
uv run flake8 src/
```

### Run Tests
```bash
uv run pytest tests/
```

## Extension and Integration

### Adding New Data Sources
1. Create new service classes in `src/semantic/services/`
2. Implement unified data interfaces (inherit from base service classes)
3. Integrate new data sources in pipeline service

### Adding New Data Processing Steps
1. Add new step methods in pipeline service
2. Update execution flow of `run_pipeline()` method
3. Add corresponding configuration options and error handling

### Integrating Other Scheduling Systems
1. Implement custom scheduler classes
2. Maintain interface compatibility with existing pipeline services
3. Provide the same monitoring and logging functionality

## Monitoring and Logging

### Log Files
- Pipeline execution logs: `logs/dblp_service_YYYYMMDD_HHMMSS.log`
- Scheduler logs: `logs/scheduler_YYYYMMDD.log`
- Database operation logs: Integrated in pipeline logs

### Monitoring Metrics
- Number of papers processed
- New/updated paper statistics
- Execution time
- Error rate
- Retry count

### Database Monitoring
```sql
-- View processing history
SELECT * FROM dblp_processing_meta ORDER BY created_at DESC LIMIT 10;

-- View data statistics
SELECT venue, COUNT(*) as count 
FROM dblp_papers 
GROUP BY venue 
ORDER BY count DESC;
```

## Troubleshooting

### Common Issues

**Connection Failed**
- Check if PostgreSQL service is running
- Verify connection information in .env file
- Confirm database exists and user has permissions

**Download Failed**
- Check network connection
- Verify DBLP URL is accessible
- Check disk space

**Scheduler Startup Failed**
- Check Cron expression format
- Verify database connection
- Check scheduler log files

**Incremental Processing Exception**
- Check if metadata tables exist
- Verify incremental configuration parameters
- Clean temporary files and re-run

### Performance Optimization Recommendations

1. **Database Optimization**
   - Regularly execute VACUUM ANALYZE
   - Monitor index usage
   - Adjust batch processing size

2. **Memory Optimization**
   - Adjust batch processing size based on server memory
   - Monitor memory usage
   - Clean up temporary files promptly

3. **Network Optimization**
   - Use stable network connections
   - Consider using proxy servers
   - Set appropriate timeout values

## License

This project follows the MIT License.

## Contributing Guide

Welcome to submit Pull Requests and Issues! Please ensure:

1. Code follows the project's formatting standards
2. Add appropriate test cases
3. Update relevant documentation
4. Follow existing architectural patterns

## Changelog

### v2.0.0
- Refactored to modular architecture
- Added incremental processing functionality
- Integrated APScheduler scheduled tasks
- Enhanced error handling and retry mechanisms
- Improved logging and monitoring systems

### v1.0.0
- Basic DBLP data processing functionality
- PostgreSQL data storage
- Basic batch processing mechanisms