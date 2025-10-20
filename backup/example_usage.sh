#!/bin/bash
# Example usage of backup and restore scripts

echo "=== Database Table Backup and Restore Examples ==="
echo ""

# Example 1: Backup a table
echo "Example 1: Backup a table"
echo "Command: uv run python backup/backup_table.py papers"
echo ""

# Example 2: Parallel backup for large tables (NEW)
echo "Example 2: Parallel backup for large tables"
echo "Command: uv run python backup/backup_table.py large_table --jobs 4"
echo "Note: Use --jobs for faster backup of large tables"
echo ""

# Example 3: Backup multiple tables
echo "Example 3: Backup multiple tables"
echo "Command: uv run python backup/backup_table.py papers"
echo "Command: uv run python backup/backup_table.py all_authors"
echo "Command: uv run python backup/backup_table.py conferences"
echo ""

# Example 4: List available backups
echo "Example 4: List available backups"
echo "Command: uv run python backup/restore_table.py --list"
echo ""

# Example 5: Restore a table
echo "Example 5: Restore a table (append mode)"
echo "Command: uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz"
echo ""

# Example 6: Restore with drop existing
echo "Example 6: Restore with drop existing table"
echo "Command: uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing"
echo ""

# Example 7: Restore with cascade
echo "Example 7: Restore with cascade (drop dependencies)"
echo "Command: uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing --cascade"
echo ""

# Example 8: Restore to different table name (NEW)
echo "Example 8: Restore to different table name"
echo "Command: uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --rename-to papers_test"
echo "Note: Use --rename-to for cloning, testing, or creating backup copies"
echo ""

echo "=== Common Workflows ==="
echo ""

# Workflow 1: Backup before major changes
echo "Workflow 1: Backup before major changes"
echo "uv run python backup/backup_table.py papers"
echo "# Make your changes..."
echo "# If something goes wrong, restore:"
echo "uv run python backup/restore_table.py backup/papers_YYYYMMDD_HHMMSS.sql.gz --drop-existing"
echo ""

# Workflow 2: Migrate data to new table structure
echo "Workflow 2: Migrate data to new table structure"
echo "# 1. Backup original table"
echo "uv run python backup/backup_table.py old_papers"
echo "# 2. Create new table structure"
echo "# 3. Restore data and transform as needed"
echo ""

# Workflow 3: Clone a table
echo "Workflow 3: Clone a table for testing"
echo "# 1. Backup original table"
echo "uv run python backup/backup_table.py papers"
echo "# 2. Restore to a different table name (NEW: using --rename-to)"
echo "uv run python backup/restore_table.py backup/papers_YYYYMMDD_HHMMSS.sql.gz --rename-to papers_test"
echo ""

# Workflow 4: Fast backup of large tables
echo "Workflow 4: Fast backup of large tables"
echo "# Use parallel jobs for faster backup"
echo "uv run python backup/backup_table.py huge_dataset --jobs 8"
echo "# Output: huge_dataset_YYYYMMDD_HHMMSS.tar.gz"
echo ""
