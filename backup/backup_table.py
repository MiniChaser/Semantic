#!/usr/bin/env python3
"""
Database table backup script
Backs up a specific database table to a compressed SQL file
Usage: uv run python backup/backup_table.py <table_name>
"""

import os
import sys
import argparse
import subprocess
import shutil
import tempfile
from datetime import datetime
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


def check_table_exists(table_name, config):
    """Check if the table exists in the database"""
    try:
        # Use psql to check if table exists
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


def get_table_size(table_name, config):
    """Get the size of a table in the database"""
    try:
        size_cmd = [
            'psql',
            '-h', config['host'],
            '-p', config['port'],
            '-U', config['user'],
            '-d', config['database'],
            '-t',
            '-c', f"SELECT pg_size_pretty(pg_total_relation_size('{table_name}'));"
        ]

        env = os.environ.copy()
        env['PGPASSWORD'] = config['password']

        result = subprocess.run(
            size_cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )

        size = result.stdout.strip()
        return size if size else "Unknown"
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not get table size: {e}")
        return "Unknown"


def backup_table_parallel(table_name, config, output_dir=None, jobs=4):
    """
    Backup a specific database table using parallel jobs

    Args:
        table_name: Name of the table to backup
        config: Database configuration dictionary
        output_dir: Directory to store backup files (defaults to backup directory)
        jobs: Number of parallel jobs

    Returns:
        Path to the backup file if successful, None otherwise
    """
    # Set default output directory
    if output_dir is None:
        output_dir = Path(__file__).parent
    else:
        output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check if table exists
    if not check_table_exists(table_name, config):
        print(f"Error: Table '{table_name}' does not exist in database '{config['database']}'")
        return None

    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_dir = None
    backup_path = output_dir / f"{table_name}_{timestamp}.tar.gz"

    # Set PGPASSWORD environment variable
    env = os.environ.copy()
    env['PGPASSWORD'] = config['password']

    try:
        # Create temporary directory for pg_dump directory format
        temp_dir = Path(tempfile.mkdtemp(prefix=f"backup_{table_name}_"))
        dump_dir = temp_dir / "dump"

        print(f"Starting parallel backup of table '{table_name}' with {jobs} jobs...")
        print(f"Backup location: {backup_path}")

        # Build pg_dump command with directory format and parallel jobs
        dump_cmd = [
            'pg_dump',
            '-h', config['host'],
            '-p', config['port'],
            '-U', config['user'],
            '-d', config['database'],
            '-t', table_name,
            '--format=directory',  # Directory format for parallel dumps
            '-j', str(jobs),       # Number of parallel jobs
            '-f', str(dump_dir),   # Output directory
            '--no-owner',
            '--no-privileges',
            '--verbose'
        ]

        # Execute pg_dump
        result = subprocess.run(
            dump_cmd,
            env=env,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"Error during backup: {result.stderr}")
            return None

        # Compress the directory into tar.gz
        print("Compressing backup...")
        tar_cmd = [
            'tar',
            '-czf',
            str(backup_path),
            '-C', str(temp_dir),
            'dump'
        ]

        tar_result = subprocess.run(
            tar_cmd,
            capture_output=True,
            text=True
        )

        if tar_result.returncode != 0:
            print(f"Error during compression: {tar_result.stderr}")
            return None

        # Get file size
        file_size = backup_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        print(f"✓ Parallel backup completed successfully!")
        print(f"  Table: {table_name}")
        print(f"  Jobs: {jobs}")
        print(f"  File: {backup_path}")
        print(f"  Size: {file_size_mb:.2f} MB")

        return backup_path

    except Exception as e:
        print(f"Error during backup: {e}")
        # Clean up partial backup file
        if backup_path.exists():
            backup_path.unlink()
        return None
    finally:
        # Clean up temporary directory
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir)


def backup_table(table_name, config, output_dir=None):
    """
    Backup a specific database table to a compressed SQL file

    Args:
        table_name: Name of the table to backup
        config: Database configuration dictionary
        output_dir: Directory to store backup files (defaults to backup directory)

    Returns:
        Path to the backup file if successful, None otherwise
    """
    # Set default output directory
    if output_dir is None:
        output_dir = Path(__file__).parent
    else:
        output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check if table exists
    if not check_table_exists(table_name, config):
        print(f"Error: Table '{table_name}' does not exist in database '{config['database']}'")
        return None

    # Get and display table size
    table_size = get_table_size(table_name, config)
    print(f"Table size: {table_size}")

    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f"{table_name}_{timestamp}.sql.gz"
    backup_path = output_dir / backup_filename

    # Build pg_dump command
    # Using --format=plain for SQL format, pipe to gzip for compression
    dump_cmd = [
        'pg_dump',
        '-h', config['host'],
        '-p', config['port'],
        '-U', config['user'],
        '-d', config['database'],
        '-t', table_name,  # Only dump specified table
        '--format=plain',  # Plain SQL format
        '--no-owner',      # Don't output ownership commands
        '--no-privileges', # Don't output privilege commands
        '--verbose'        # Verbose output
    ]

    # Set PGPASSWORD environment variable
    env = os.environ.copy()
    env['PGPASSWORD'] = config['password']

    try:
        print(f"Starting backup of table '{table_name}' from database '{config['database']}'...")
        print(f"Backup location: {backup_path}")

        # Execute pg_dump and pipe to gzip
        with open(backup_path, 'wb') as f:
            # Run pg_dump
            dump_process = subprocess.Popen(
                dump_cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Run gzip
            gzip_process = subprocess.Popen(
                ['gzip'],
                stdin=dump_process.stdout,
                stdout=f,
                stderr=subprocess.PIPE
            )

            # Close dump_process stdout to allow it to receive SIGPIPE
            dump_process.stdout.close()

            # Wait for both processes to complete
            gzip_stderr = gzip_process.communicate()[1]
            dump_return_code = dump_process.wait()

            if dump_return_code != 0:
                dump_stderr = dump_process.stderr.read().decode()
                print(f"Error during backup: {dump_stderr}")
                return None

            if gzip_process.returncode != 0:
                print(f"Error during compression: {gzip_stderr.decode()}")
                return None

        # Get file size
        file_size = backup_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        print(f"✓ Backup completed successfully!")
        print(f"  Table: {table_name}")
        print(f"  File: {backup_path}")
        print(f"  Size: {file_size_mb:.2f} MB")

        return backup_path

    except Exception as e:
        print(f"Error during backup: {e}")
        # Clean up partial backup file
        if backup_path.exists():
            backup_path.unlink()
        return None


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Backup a specific database table to a compressed SQL file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backup a table
  uv run python backup/backup_table.py papers

  # Backup to a custom directory
  uv run python backup/backup_table.py papers --output-dir /path/to/backups

  # Parallel backup for large tables
  uv run python backup/backup_table.py large_table --jobs 4
        """
    )

    parser.add_argument(
        'table_name',
        help='Name of the table to backup'
    )

    parser.add_argument(
        '-o', '--output-dir',
        help='Directory to store backup files (default: backup directory)',
        default=None
    )

    parser.add_argument(
        '-j', '--jobs',
        type=int,
        default=1,
        help='Number of parallel jobs (default: 1, use 4+ for large tables)'
    )

    args = parser.parse_args()

    # Setup environment
    setup_environment()

    # Get database configuration
    config = get_db_config()

    print(f"Database Configuration:")
    print(f"  Host: {config['host']}")
    print(f"  Port: {config['port']}")
    print(f"  Database: {config['database']}")
    print(f"  User: {config['user']}")
    print()

    # Perform backup
    if args.jobs > 1:
        # Use parallel backup
        result = backup_table_parallel(args.table_name, config, args.output_dir, args.jobs)
    else:
        # Use standard backup
        result = backup_table(args.table_name, config, args.output_dir)

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
