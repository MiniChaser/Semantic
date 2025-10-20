#!/usr/bin/env python3
"""
Database table restore script
Restores a specific database table from a compressed SQL backup file
Usage: uv run python backup/restore_table.py <backup_file>
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from dotenv import load_dotenv


def setup_environment():
    """Load environment variables"""
    # Load from parent directory's .env file
    parent_dir = Path(__file__).parent.parent
    env_file = parent_dir / '.env'
    if env_file.exists():
        load_dotenv(env_file)
    else:
        print(f"Warning: .env file not found at {env_file}")


def get_db_config():
    """Get database configuration from environment variables"""
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    return config


def extract_table_name_from_filename(backup_file):
    """
    Extract table name from backup filename
    Expected format: tablename_YYYYMMDD_HHMMSS.sql.gz
    """
    filename = Path(backup_file).stem  # Remove .gz
    filename = Path(filename).stem     # Remove .sql

    # Split by underscore and take everything except last two parts (timestamp)
    parts = filename.split('_')
    if len(parts) >= 3:
        # Last two parts are date and time
        table_name = '_'.join(parts[:-2])
        return table_name
    else:
        # If format doesn't match, return the whole filename
        return filename


def check_table_exists(table_name, config):
    """Check if the table exists in the database"""
    try:
        check_cmd = [
            'psql',
            '-h', config['host'],
            '-p', config['port'],
            '-U', config['user'],
            '-d', config['database'],
            '-t',  # tuple only mode
            '-c', f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"
        ]

        env = os.environ.copy()
        env['PGPASSWORD'] = config['password']

        result = subprocess.run(
            check_cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )

        exists = result.stdout.strip().lower() == 't'
        return exists
    except subprocess.CalledProcessError as e:
        print(f"Error checking table existence: {e}")
        print(f"stderr: {e.stderr}")
        return False


def drop_table(table_name, config, cascade=False):
    """Drop a table from the database"""
    try:
        cascade_clause = "CASCADE" if cascade else ""
        drop_cmd = [
            'psql',
            '-h', config['host'],
            '-p', config['port'],
            '-U', config['user'],
            '-d', config['database'],
            '-c', f"DROP TABLE IF EXISTS {table_name} {cascade_clause};"
        ]

        env = os.environ.copy()
        env['PGPASSWORD'] = config['password']

        result = subprocess.run(
            drop_cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )

        print(f"✓ Table '{table_name}' dropped successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error dropping table: {e}")
        print(f"stderr: {e.stderr}")
        return False


def build_sed_replace_patterns(source_table, target_table):
    """
    Build sed pattern for table name replacement

    Args:
        source_table: Original table name
        target_table: New table name

    Returns:
        sed pattern string
    """
    # Build sed pattern to replace table names in various SQL contexts
    # Using word boundaries to avoid partial matches
    patterns = [
        f's/\\bCREATE TABLE {source_table}\\b/CREATE TABLE {target_table}/gI',
        f's/\\bCREATE TABLE IF NOT EXISTS {source_table}\\b/CREATE TABLE IF NOT EXISTS {target_table}/gI',
        f's/\\bALTER TABLE {source_table}\\b/ALTER TABLE {target_table}/gI',
        f's/\\bALTER TABLE ONLY {source_table}\\b/ALTER TABLE ONLY {target_table}/gI',
        f's/\\bCOPY {source_table}\\b/COPY {target_table}/gI',
        f's/\\bCREATE INDEX.*ON {source_table}\\b/&/gI; s/ON {source_table}\\b/ON {target_table}/gI',
        f's/\\bCREATE.*INDEX.*ON {source_table} /&/gI; s/ ON {source_table} / ON {target_table} /gI',
        # Handle table name in constraint definitions
        f's/{source_table}_pkey/{target_table}_pkey/g',
        f's/{source_table}_/{target_table}_/g',
    ]

    # Join all patterns with semicolon
    sed_pattern = '; '.join(patterns)
    return sed_pattern


def restore_table(backup_file, config, table_name=None, drop_existing=False, cascade=False, rename_to=None):
    """
    Restore a specific database table from a compressed SQL backup file

    Args:
        backup_file: Path to the backup file (.sql.gz)
        config: Database configuration dictionary
        table_name: Name of the table (if None, extract from filename)
        drop_existing: Whether to drop existing table before restore
        cascade: Whether to use CASCADE when dropping table
        rename_to: Rename table during restore (restore to different table name)

    Returns:
        True if successful, False otherwise
    """
    backup_path = Path(backup_file)

    # Check if backup file exists
    if not backup_path.exists():
        print(f"Error: Backup file not found: {backup_file}")
        return False

    # Check if file is compressed
    if not backup_path.name.endswith('.sql.gz'):
        print(f"Error: Backup file must be a .sql.gz file")
        return False

    # Extract source table name from backup file if not provided
    source_table = table_name
    if source_table is None:
        source_table = extract_table_name_from_filename(backup_path)
        print(f"Detected source table name: {source_table}")

    # Determine target table name (use rename_to if specified)
    target_table = rename_to if rename_to else source_table

    # If renaming, show information
    if rename_to:
        print(f"Table will be renamed during restore:")
        print(f"  Source: {source_table}")
        print(f"  Target: {target_table}")
        print(f"  Warning: This uses text replacement - verify results in test environment first")

    # Check if target table exists and handle accordingly
    table_exists = check_table_exists(target_table, config)

    if table_exists:
        if drop_existing:
            print(f"Table '{target_table}' exists, dropping...")
            if not drop_table(target_table, config, cascade):
                print(f"Warning: Failed to drop existing table")
                if not cascade:
                    print(f"Tip: Try with --cascade option if table has dependencies")
                return False
        else:
            print(f"Warning: Table '{target_table}' already exists!")
            print(f"Data will be appended. Use --drop-existing to replace the table.")
            response = input("Continue? (y/N): ")
            if response.lower() != 'y':
                print("Restore cancelled")
                return False

    # Set PGPASSWORD environment variable
    env = os.environ.copy()
    env['PGPASSWORD'] = config['password']

    try:
        print(f"Starting restore of table '{target_table}' to database '{config['database']}'...")
        print(f"Backup file: {backup_path}")

        # Build psql command
        restore_cmd = [
            'psql',
            '-h', config['host'],
            '-p', config['port'],
            '-U', config['user'],
            '-d', config['database'],
            '--set', 'ON_ERROR_STOP=on',  # Stop on first error
            '-v', 'ON_ERROR_STOP=1'       # Alternative syntax
        ]

        # Decompress and pipe to psql (with optional sed for table renaming)
        with open(backup_path, 'rb') as f:
            # Run gunzip
            gunzip_process = subprocess.Popen(
                ['gunzip', '-c'],
                stdin=f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # If renaming, insert sed in the pipeline
            if rename_to:
                sed_pattern = build_sed_replace_patterns(source_table, target_table)
                sed_process = subprocess.Popen(
                    ['sed', sed_pattern],
                    stdin=gunzip_process.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                gunzip_process.stdout.close()
                input_for_psql = sed_process.stdout
            else:
                input_for_psql = gunzip_process.stdout

            # Run psql
            psql_process = subprocess.Popen(
                restore_cmd,
                env=env,
                stdin=input_for_psql,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Close input stream to allow proper SIGPIPE handling
            if rename_to:
                sed_process.stdout.close()
            else:
                gunzip_process.stdout.close()

            # Wait for all processes to complete
            psql_stdout, psql_stderr = psql_process.communicate()

            if rename_to:
                sed_return_code = sed_process.wait()
                if sed_return_code != 0:
                    sed_stderr = sed_process.stderr.read().decode()
                    print(f"Error during table name replacement: {sed_stderr}")
                    return False

            gunzip_return_code = gunzip_process.wait()
            if gunzip_return_code != 0:
                gunzip_stderr = gunzip_process.stderr.read().decode()
                print(f"Error during decompression: {gunzip_stderr}")
                return False

            if psql_process.returncode != 0:
                print(f"Error during restore:")
                print(psql_stderr.decode())
                return False

        print(f"✓ Restore completed successfully!")
        print(f"  Table: {target_table}")
        print(f"  Database: {config['database']}")

        # Verify table exists after restore
        if check_table_exists(target_table, config):
            print(f"✓ Table '{target_table}' verified in database")
            return True
        else:
            print(f"Warning: Table '{target_table}' not found after restore")
            return False

    except Exception as e:
        print(f"Error during restore: {e}")
        return False


def list_backups(backup_dir=None):
    """List available backup files"""
    if backup_dir is None:
        backup_dir = Path(__file__).parent
    else:
        backup_dir = Path(backup_dir)

    if not backup_dir.exists():
        print(f"Backup directory not found: {backup_dir}")
        return []

    backups = sorted(backup_dir.glob("*.sql.gz"), key=lambda x: x.stat().st_mtime, reverse=True)

    if not backups:
        print(f"No backup files found in {backup_dir}")
        return []

    print(f"\nAvailable backups in {backup_dir}:")
    print("-" * 80)
    for backup in backups:
        size_mb = backup.stat().st_size / (1024 * 1024)
        table_name = extract_table_name_from_filename(backup)
        print(f"  {backup.name}")
        print(f"    Table: {table_name}, Size: {size_mb:.2f} MB")
    print("-" * 80)

    return backups


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Restore a specific database table from a compressed SQL backup file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available backups
  uv run python backup/restore_table.py --list

  # Restore from a backup file
  uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz

  # Restore and drop existing table first
  uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing

  # Restore with cascade (drop dependent objects)
  uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --drop-existing --cascade

  # Restore to a different table name (for backup copy, testing, etc.)
  uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --rename-to papers_test

  # Specify table name explicitly
  uv run python backup/restore_table.py backup/papers_20241020_123456.sql.gz --table-name papers
        """
    )

    parser.add_argument(
        'backup_file',
        nargs='?',
        help='Path to the backup file (.sql.gz)'
    )

    parser.add_argument(
        '-l', '--list',
        action='store_true',
        help='List available backup files'
    )

    parser.add_argument(
        '-t', '--table-name',
        help='Name of the table (default: extract from filename)',
        default=None
    )

    parser.add_argument(
        '-r', '--rename-to',
        help='Rename table during restore (restore to different table name)',
        default=None
    )

    parser.add_argument(
        '-d', '--drop-existing',
        action='store_true',
        help='Drop existing table before restore'
    )

    parser.add_argument(
        '-c', '--cascade',
        action='store_true',
        help='Use CASCADE when dropping existing table'
    )

    parser.add_argument(
        '--backup-dir',
        help='Directory containing backup files (for --list)',
        default=None
    )

    args = parser.parse_args()

    # Setup environment
    setup_environment()

    # Handle list mode
    if args.list:
        list_backups(args.backup_dir)
        sys.exit(0)

    # Require backup_file if not in list mode
    if not args.backup_file:
        parser.error("backup_file is required unless --list is specified")

    # Get database configuration
    config = get_db_config()

    print(f"Database Configuration:")
    print(f"  Host: {config['host']}")
    print(f"  Port: {config['port']}")
    print(f"  Database: {config['database']}")
    print(f"  User: {config['user']}")
    print()

    # Perform restore
    result = restore_table(
        args.backup_file,
        config,
        table_name=args.table_name,
        drop_existing=args.drop_existing,
        cascade=args.cascade,
        rename_to=args.rename_to
    )

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
