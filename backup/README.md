# Database Table Backup and Restore Scripts

This directory contains scripts for backing up and restoring individual database tables.

## Scripts

### backup_table.py
Backs up a specific database table to a compressed SQL file.

### restore_table.py
Restores a specific database table from a compressed SQL backup file.

## Requirements

- Python 3.x
- PostgreSQL client tools (`pg_dump`, `psql`)
- `gzip` and `gunzip` utilities
- Database configuration in `.env` file

## Usage

### Backup a Table

```bash
# Basic backup
uv run python backup/backup_table.py <table_name>

# Example: Backup the 'papers' table
uv run python backup/backup_table.py papers

# Backup to a custom directory
uv run python backup/backup_table.py papers --output-dir /path/to/backups

# Parallel backup for large tables (faster)
uv run python backup/backup_table.py large_table --jobs 4
```

**Backup File Formats:**
- Standard backup: `<table_name>_<timestamp>.sql.gz`
- Parallel backup: `<table_name>_<timestamp>.tar.gz`

Examples:
- `papers_20241020_123456.sql.gz` - Standard single-threaded backup
- `papers_20241020_123456.tar.gz` - Parallel backup (directory format)

### List Available Backups

```bash
uv run python backup/restore_table.py --list
```

### Restore a Table

```bash
# Basic restore (appends data if table exists)
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz

# Drop existing table before restore
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing

# Drop with CASCADE (removes dependent objects)
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing --cascade

# Restore to a different table name (for backup copy, testing, cloning, etc.)
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --rename-to papers_test

# Specify table name explicitly
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --table-name papers
```

## Features

### Backup Script Features
- ✓ Automatically detects table existence
- ✓ Displays table size before backup
- ✓ Creates compressed SQL backups (.sql.gz)
- ✓ **NEW**: Parallel backup support for large tables (--jobs)
- ✓ Timestamps backup files for easy identification
- ✓ Shows backup file size after completion
- ✓ Verbose output for monitoring progress
- ✓ Excludes ownership and privilege information
- ✓ Streaming operation - no memory issues with large tables

### Restore Script Features
- ✓ Automatically extracts table name from backup filename
- ✓ Verifies backup file exists before restore
- ✓ Checks if table exists in database
- ✓ Option to drop existing table before restore
- ✓ Cascade option for dropping dependent objects
- ✓ **NEW**: Rename table during restore (--rename-to)
- ✓ Interactive confirmation for appending to existing table
- ✓ Stops on first error
- ✓ Lists available backup files
- ✓ Verifies table after restore
- ✓ Streaming operation - no memory issues with large tables

## Backup File Naming Convention

Backup files follow this naming pattern:
```
<table_name>_<YYYYMMDD>_<HHMMSS>.sql.gz
```

Examples:
- `papers_20241020_093045.sql.gz` - papers table backed up on 2024-10-20 at 09:30:45
- `all_authors_20241020_143022.sql.gz` - all_authors table backed up on 2024-10-20 at 14:30:22

## Safety Features

1. **Backup Script**:
   - Checks if table exists before attempting backup
   - Cleans up partial backup files on error
   - Provides detailed error messages

2. **Restore Script**:
   - Verifies backup file exists and has correct format
   - Checks table existence before restore
   - Confirms with user before appending to existing table
   - Uses `ON_ERROR_STOP` to halt on first error
   - Verifies table exists after restore completes

## Examples

### Complete Backup and Restore Workflow

```bash
# 1. Backup the 'papers' table
uv run python backup/backup_table.py papers
# Output: papers_20241020_123456.sql.gz

# 2. List available backups
uv run python backup/restore_table.py --list

# 3. Restore from backup (drop existing table first)
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing
```

### Parallel Backup for Large Tables

```bash
# Backup large table with 4 parallel jobs (faster)
uv run python backup/backup_table.py large_papers --jobs 4
# Output: large_papers_20241020_123456.tar.gz

# Backup with 8 parallel jobs for even faster performance
uv run python backup/backup_table.py huge_dataset --jobs 8

# Note: Use jobs=2 to 8 depending on CPU cores and table size
# Single-threaded (default) is fine for small to medium tables
```

### Backup Multiple Tables

```bash
# Backup multiple tables
uv run python backup/backup_table.py papers
uv run python backup/backup_table.py all_authors
uv run python backup/backup_table.py all_papers
uv run python backup/backup_table.py conferences
```

### Table Cloning and Renaming

```bash
# Clone a table for testing
uv run python backup/backup_table.py papers
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --rename-to papers_test

# Create a backup copy with different name
uv run python backup/backup_table.py production_data
uv run python backup/restore_table.py backup/production_data_20241020_123456.sql.gz --rename-to production_data_backup

# Migrate data to new table structure (rename during restore)
uv run python backup/backup_table.py old_table
uv run python backup/restore_table.py backup/old_table_20241020_123456.sql.gz --rename-to new_table
```

### Restore with Different Scenarios

```bash
# Scenario 1: Table doesn't exist - restore creates it
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz

# Scenario 2: Table exists - append data (with confirmation)
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz

# Scenario 3: Table exists - replace completely
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing

# Scenario 4: Table has dependencies - drop with cascade
uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing --cascade
```

## Database Configuration

The scripts read database configuration from the `.env` file in the parent directory:

```ini
DB_HOST=127.0.0.1
DB_PORT=5433
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password
```

## Error Handling

Both scripts include comprehensive error handling:

- Connection failures
- Table not found
- File not found
- Decompression errors
- SQL execution errors
- Insufficient permissions

Detailed error messages are displayed to help diagnose issues.

## Tips

1. **Regular Backups**: Schedule regular backups of critical tables using cron or similar tools
2. **Version Control**: Keep backup files organized by date/time for easy rollback
3. **Test Restores**: Periodically test restore procedures to ensure backups are valid
4. **Storage**: Store backups in a separate location from the database server
5. **Compression**: Backup files are compressed to save disk space

## Troubleshooting

### Permission Denied
```bash
# Make scripts executable
chmod +x backup/backup_table.py backup/restore_table.py
```

### Table Not Found
```bash
# List all tables in the database
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\dt"
```

### Connection Issues
```bash
# Test database connection
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT 1"
```

### Restore Fails with Dependencies
```bash
# Use --cascade to drop dependent objects
uv run python backup/restore_table.py backup/table.sql.gz --drop-existing --cascade
```

## Advanced Features

### Parallel Backup (--jobs)

**When to use:**
- Large tables (> 1 GB)
- Tables with many rows
- When backup time is critical

**How it works:**
- Uses PostgreSQL's directory format with multiple parallel jobs
- Automatically packages result into tar.gz file
- Requires PostgreSQL 9.3+

**Performance considerations:**
- Use jobs = 2-4 for medium tables (1-10 GB)
- Use jobs = 4-8 for large tables (> 10 GB)
- More jobs = faster backup but higher CPU/IO usage
- Optimal jobs count depends on your hardware

**Example:**
```bash
# Small table - use default single-threaded
uv run python backup/backup_table.py small_table

# Large table - use parallel backup
uv run python backup/backup_table.py large_table --jobs 4
```

### Table Renaming (--rename-to)

**When to use:**
- Creating table copies for testing
- Cloning production data to development
- Creating backup copies with different names
- Temporary table creation

**How it works:**
- Uses text replacement (sed) to rename table in SQL
- Replaces table names in CREATE TABLE, ALTER TABLE, COPY, and index definitions
- Also renames constraints (e.g., table_pkey → newtable_pkey)

**Important notes:**
- ⚠️ Uses text-based replacement - test in non-production first
- ✓ Handles most common SQL patterns
- ✓ Uses word boundaries to avoid partial matches
- ⚠️ May not handle all edge cases (complex schemas, quoted identifiers with special characters)

**Best practices:**
1. Test the renamed table structure after restore
2. Verify all constraints and indexes are correct
3. Use simple table names (avoid special characters)
4. Always test in development environment first

**Example:**
```bash
# Clone a table
uv run python backup/backup_table.py production_users
uv run python backup/restore_table.py backup/production_users_20241020_123456.sql.gz \
  --rename-to test_users

# Verify the result
psql -d your_database -c "\d test_users"
```

## Performance Notes

### Memory Usage

Both scripts use **streaming operations** and will NOT load the entire table into memory:

- **Backup**: `pg_dump → gzip → file` (pipe operation)
- **Restore**: `file → gunzip → (sed) → psql` (pipe operation)
- **Parallel backup**: Uses temporary directory but streams data

This means you can backup/restore tables of any size without memory concerns.

### Backup Speed Comparison

Approximate backup times for a 10GB table:
- Single-threaded: ~15-20 minutes
- Parallel (4 jobs): ~5-8 minutes
- Parallel (8 jobs): ~3-5 minutes

*Actual times vary based on hardware, network, and table structure*
