# DBLP Semantic Scholar Data Processing Pipeline

An automated data processing service that runs four scripts sequentially every 7 days.

## 🚀 Getting Started

### Prerequisites
- Docker (for containerized deployment)
- OR Python 3.10+ with UV package manager (for direct execution)
- PostgreSQL database
- `.env` file with proper configuration

## 📋 Service Startup Options

### Option 1: Using Docker (Recommended)

1. **Setup environment variables**
   Create a `.env` file based on `.env.example`:
   ```bash
   # Database Configuration
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=dblp_semantic
   DB_USER=postgres
   DB_PASSWORD=your_password

   # Scheduling Configuration
   SCHEDULE_INTERVAL_DAYS=7

   # Other required settings...
   ```

2. **Build and run the Docker container**
   ```bash
   # Build the image
   docker build -t semantic-scheduler .

   # Run with default settings (7-day interval)
   docker run -d --name semantic-service semantic-scheduler

   # Or run with custom interval and environment file
   docker run -d --name semantic-service --env-file .env semantic-scheduler

   # Or run with specific interval override
   docker run -d --name semantic-service -e SCHEDULE_INTERVAL_DAYS=3 semantic-scheduler
   ```

3. **Monitor the service**
   ```bash
   # View logs
   docker logs -f semantic-service

   # Check status
   docker ps
   ```

### Option 2: Direct Python Execution

1. **Install dependencies**
   ```bash
   uv sync
   ```

2. **Setup database**
   ```bash
   uv run python scripts/setup_database.py
   ```

3. **Run scripts individually**
   ```bash
   # Run all scripts in sequence (manual execution)
   uv run python scripts/setup_database.py
   uv run python scripts/run_dblp_service_once.py
   uv run python scripts/run_s2_enrichment.py
   uv run python scripts/run_all_steps.py
   ```

4. **Or use the scheduler directly**
   ```bash
   # Set environment variables and run scheduler
   export SCHEDULE_INTERVAL_DAYS=7
   uv run python docker/scheduler.py
   ```

## ⚙️ Configuration

### Environment Variables
Key configuration options in `.env`:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dblp_semantic
DB_USER=postgres
DB_PASSWORD=your_password

# Scheduling (Docker service)
SCHEDULE_INTERVAL_DAYS=7

# API Keys
SEMANTIC_SCHOLAR_API_KEY=your_key_here

# Processing Configuration
BATCH_SIZE=10000
LOG_LEVEL=INFO
TARGET_VENUES=acl,naacl,emnlp,findings
```

## 📊 Service Behavior

### Script Execution Order
The service runs these scripts sequentially:
1. `scripts/setup_database.py` - Database initialization
2. `scripts/run_dblp_service_once.py` - DBLP data import
3. `scripts/run_s2_enrichment.py` - Semantic Scholar paper enrichment (incremental by timestamp)
4. `scripts/run_all_steps.py` - Complete author processing pipeline:
   - Step 1: Create authorships from enriched papers
   - Step 2: Create S2 author profiles via API (incremental by timestamp)
   - Step 3: Create consolidated author profiles
   - Step 4: Enrich author profiles with S2 data
   - Step 5: Generate final author table
   - Step 6: Generate processing reports

### Scheduling
- **Default**: Runs every 7 days
- **Configurable**: Set `SCHEDULE_INTERVAL_DAYS` environment variable
- **Logging**: All execution logs stored in `/var/log/semantic/` (Docker) or console output (direct execution)

### Error Handling
- If any script fails, the execution stops
- The service waits for the next scheduled interval before retrying
- All errors are logged with timestamps

### Incremental Processing
- **S2 API Scripts**: Both `run_s2_enrichment.py` and `step2_create_s2_author_profiles.py` run incrementally based on timestamps
- Only processes new or updated records since the last successful run
- Reduces API calls and improves performance on subsequent runs

## 🧪 Testing

### Data Verification Tests
Run comprehensive verification tests for the final author table:

```bash
# Run table verification tests
uv run python test/final_author_table/verify_table.py

# View test results (generated in test/final_author_table/results/)
```

## 🔧 Troubleshooting

### Docker Issues
```bash
# Check container logs
docker logs semantic-service

# Restart the service
docker restart semantic-service

# Access container shell for debugging
docker exec -it semantic-service bash
```

### Direct Execution Issues
- Ensure all dependencies are installed: `uv sync`
- Check database connectivity
- Verify `.env` file configuration
- Review console output for specific error messages