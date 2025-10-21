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
import psycopg2


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


def detect_postgres_mode(config=None):
    """
    Detect how to access PostgreSQL (native, docker, or none)

    Args:
        config: Database configuration dict (optional, for Docker container detection)

    Returns:
        tuple: (mode, container_name) where mode is 'native', 'docker', or 'none'
    """
    # First check if user manually specified a Docker container
    docker_container = os.getenv('POSTGRES_DOCKER_CONTAINER')
    if docker_container:
        return ('docker', docker_container)

    # Check if native psql/pg_dump is available
    try:
        result = subprocess.run(
            ['which', 'psql'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return ('native', None)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Check for Docker container running PostgreSQL
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            containers = result.stdout.strip().split('\n')
            # Look for common PostgreSQL container names
            for container in containers:
                if container and 'postgres' in container.lower():
                    return ('docker', container)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Try with container IDs if permission denied
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.ID}}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            container_ids = result.stdout.strip().split('\n')
            for cid in container_ids:
                if cid:
                    return ('docker', cid)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # If Docker command failed (permission issue), try to find container via port mapping
    if config:
        try:
            # Try sudo docker ps (might work in some environments)
            result = subprocess.run(
                ['sudo', '-n', 'docker', 'ps', '--format', '{{.Names}}\t{{.Ports}}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        container_name = parts[0]
                        ports = parts[1]
                        # Check if this container has the same port as our config
                        if config['port'] in ports and 'postgres' in container_name.lower():
                            return ('docker', container_name)
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError):
            pass

    return ('none', None)


def check_table_exists(table_name, config):
    """Check if the table exists in the database using psycopg2"""
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);",
            (table_name,)
        )
        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


def get_table_size(table_name, config):
    """Get the size of a table in the database using psycopg2"""
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT pg_size_pretty(pg_total_relation_size(%s));",
            (table_name,)
        )
        size = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return size if size else "Unknown"
    except Exception as e:
        print(f"Warning: Could not get table size: {e}")
        return "Unknown"


def backup_table_parallel(table_name, config, output_dir=None, jobs=4, mode='native', container=None):
    """
    Backup a specific database table using parallel jobs

    Args:
        table_name: Name of the table to backup
        config: Database configuration dictionary
        output_dir: Directory to store backup files (defaults to backup directory)
        jobs: Number of parallel jobs
        mode: Execution mode ('native' or 'docker')
        container: Docker container name (required if mode='docker')

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
        if mode == 'docker':
            if not container:
                print("Error: Docker container name required for docker mode")
                return None
            dump_cmd = [
                'sudo', 'docker', 'exec', container,
                'pg_dump',
                '-h', 'localhost',  # Inside container, use localhost
                '-U', config['user'],
                '-d', config['database'],
                '-t', table_name,
                '--format=directory',
                '-j', str(jobs),
                '-f', '/tmp/dump',  # Temporary location inside container
                '--no-owner',
                '--no-privileges',
                '--verbose'
            ]
        else:
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

        # For Docker mode, copy the dump directory from container
        if mode == 'docker':
            print("Copying backup from container...")
            copy_cmd = ['sudo', 'docker', 'cp', f'{container}:/tmp/dump', str(dump_dir)]
            copy_result = subprocess.run(copy_cmd, capture_output=True, text=True)
            if copy_result.returncode != 0:
                print(f"Error copying from container: {copy_result.stderr}")
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


def backup_table(table_name, config, output_dir=None, mode='native', container=None):
    """
    Backup a specific database table to a compressed SQL file

    Args:
        table_name: Name of the table to backup
        config: Database configuration dictionary
        output_dir: Directory to store backup files (defaults to backup directory)
        mode: Execution mode ('native' or 'docker')
        container: Docker container name (required if mode='docker')

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

    # Set PGPASSWORD environment variable
    env = os.environ.copy()
    env['PGPASSWORD'] = config['password']

    try:
        print(f"Starting backup of table '{table_name}' from database '{config['database']}'...")
        print(f"Backup location: {backup_path}")

        # Build pg_dump command based on mode
        if mode == 'docker':
            if not container:
                print("Error: Docker container name required for docker mode")
                return None
            # For Docker mode, run pg_dump inside container and pipe output
            # Try with sudo if docker command fails due to permissions
            dump_cmd = [
                'sudo', 'docker', 'exec', '-i', container,
                'pg_dump',
                '-h', 'localhost',
                '-U', config['user'],
                '-d', config['database'],
                '-t', table_name,
                '--format=plain',
                '--no-owner',
                '--no-privileges'
            ]
        else:
            # Native mode - use system pg_dump
            dump_cmd = [
                'pg_dump',
                '-h', config['host'],
                '-p', config['port'],
                '-U', config['user'],
                '-d', config['database'],
                '-t', table_name,
                '--format=plain',
                '--no-owner',
                '--no-privileges',
                '--verbose'
            ]

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

    # Detect PostgreSQL access mode
    mode, container = detect_postgres_mode(config)

    print(f"Database Configuration:")
    print(f"  Host: {config['host']}")
    print(f"  Port: {config['port']}")
    print(f"  Database: {config['database']}")
    print(f"  User: {config['user']}")
    print(f"  Mode: {mode.upper()}" + (f" (container: {container})" if container else ""))
    print()

    if mode == 'none':
        print("Error: PostgreSQL tools not found!")
        print("Please either:")
        print("  1. Install PostgreSQL client tools (psql, pg_dump)")
        print("  2. Ensure PostgreSQL Docker container is running")
        sys.exit(1)

    # Perform backup
    if args.jobs > 1:
        # Use parallel backup
        result = backup_table_parallel(args.table_name, config, args.output_dir, args.jobs, mode, container)
    else:
        # Use standard backup
        result = backup_table(args.table_name, config, args.output_dir, mode, container)

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
