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


def drop_table(table_name, config, cascade=False):
    """Drop a table from the database using psycopg2"""
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cascade_clause = "CASCADE" if cascade else ""
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} {cascade_clause};")
        cursor.close()
        conn.close()

        print(f"✓ Table '{table_name}' dropped successfully")
        return True
    except Exception as e:
        print(f"Error dropping table: {e}")
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


def check_sudo_password_required():
    """
    Check if sudo requires password for docker commands

    Returns:
        True if password is required, False otherwise
    """
    try:
        result = subprocess.run(
            ['sudo', '-n', 'docker', 'ps'],
            capture_output=True,
            timeout=2
        )
        # If return code is 0, passwordless sudo works
        return result.returncode != 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return True


def restore_table_docker_copy_method(backup_path, source_table, target_table, rename_to, config, container, env):
    """
    Alternative restore method for Docker when sudo requires password.
    Copies the file to container first, then restores from inside.

    Args:
        backup_path: Path to backup file
        source_table: Original table name from backup
        target_table: Target table name
        rename_to: Whether renaming is needed
        config: Database configuration
        container: Docker container name
        env: Environment variables including PGPASSWORD

    Returns:
        True if successful, False otherwise
    """
    import tempfile
    import shutil

    temp_dir = None
    try:
        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp(prefix="restore_"))
        temp_sql = temp_dir / "restore.sql"

        print("Step 1: Decompressing backup file...")
        # Decompress and optionally rename table
        if rename_to:
            # Decompress with table renaming
            with open(backup_path, 'rb') as f_in:
                gunzip_proc = subprocess.Popen(
                    ['gunzip', '-c'],
                    stdin=f_in,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

                sed_pattern = build_sed_replace_patterns(source_table, target_table)
                sed_proc = subprocess.Popen(
                    ['sed', sed_pattern],
                    stdin=gunzip_proc.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                gunzip_proc.stdout.close()

                # Write to temp file
                with open(temp_sql, 'wb') as f_out:
                    shutil.copyfileobj(sed_proc.stdout, f_out)

                sed_proc.wait()
                gunzip_proc.wait()

                if gunzip_proc.returncode != 0:
                    print(f"Error: Failed to decompress backup file")
                    return False
                if sed_proc.returncode != 0:
                    print(f"Error: Failed to rename table in SQL")
                    return False
        else:
            # Simple decompression
            with open(backup_path, 'rb') as f_in, open(temp_sql, 'wb') as f_out:
                gunzip_proc = subprocess.Popen(
                    ['gunzip', '-c'],
                    stdin=f_in,
                    stdout=f_out,
                    stderr=subprocess.PIPE
                )
                gunzip_proc.wait()

                if gunzip_proc.returncode != 0:
                    stderr = gunzip_proc.stderr.read().decode()
                    print(f"Error: Failed to decompress backup file: {stderr}")
                    return False

        print(f"✓ Decompressed to {temp_sql}")
        print(f"  Size: {temp_sql.stat().st_size / (1024*1024):.2f} MB")

        print("\nStep 2: Copying SQL file to Docker container...")
        # Copy SQL file to container
        copy_cmd = ['sudo', 'docker', 'cp', str(temp_sql), f'{container}:/tmp/restore.sql']
        copy_result = subprocess.run(copy_cmd, capture_output=True, text=True)

        if copy_result.returncode != 0:
            print(f"Error: Failed to copy file to container: {copy_result.stderr}")
            return False

        print("✓ File copied to container:/tmp/restore.sql")

        print(f"\nStep 3: Restoring table '{target_table}'...")
        # Run psql inside container
        psql_cmd = [
            'sudo', 'docker', 'exec', container,
            'sh', '-c',
            f"PGPASSWORD='{config['password']}' psql -h localhost -U {config['user']} -d {config['database']} --set ON_ERROR_STOP=on < /tmp/restore.sql"
        ]

        psql_result = subprocess.run(psql_cmd, capture_output=True, text=True)

        if psql_result.returncode != 0:
            print(f"Error during restore:")
            print(psql_result.stderr)
            return False

        print("✓ Restore completed successfully!")

        # Clean up file in container
        print("\nStep 4: Cleaning up...")
        cleanup_cmd = ['sudo', 'docker', 'exec', container, 'rm', '/tmp/restore.sql']
        subprocess.run(cleanup_cmd, capture_output=True)

        print(f"✓ Table '{target_table}' restored to database '{config['database']}'")
        return True

    except Exception as e:
        print(f"Error during restore: {e}")
        return False
    finally:
        # Clean up temporary directory
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir)


def restore_table(backup_file, config, table_name=None, drop_existing=False, cascade=False, rename_to=None, mode='native', container=None):
    """
    Restore a specific database table from a compressed SQL backup file

    Args:
        backup_file: Path to the backup file (.sql.gz)
        config: Database configuration dictionary
        table_name: Name of the table (if None, extract from filename)
        drop_existing: Whether to drop existing table before restore
        cascade: Whether to use CASCADE when dropping table
        rename_to: Rename table during restore (restore to different table name)
        mode: Execution mode ('native' or 'docker')
        container: Docker container name (required if mode='docker')

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

        # Build psql command based on mode
        use_docker_copy_method = False
        if mode == 'docker':
            if not container:
                print("Error: Docker container name required for docker mode")
                return False

            # Check if sudo requires password
            if check_sudo_password_required():
                print("Warning: sudo requires password for docker commands")
                print("Using alternative method: copy file to container first")
                use_docker_copy_method = True
            else:
                restore_cmd = [
                    'sudo', 'docker', 'exec', '-i', container,
                    'psql',
                    '-h', 'localhost',
                    '-U', config['user'],
                    '-d', config['database'],
                    '--set', 'ON_ERROR_STOP=on',
                    '-v', 'ON_ERROR_STOP=1'
                ]
        else:
            restore_cmd = [
                'psql',
                '-h', config['host'],
                '-p', config['port'],
                '-U', config['user'],
                '-d', config['database'],
                '--set', 'ON_ERROR_STOP=on',  # Stop on first error
                '-v', 'ON_ERROR_STOP=1'       # Alternative syntax
            ]

        # Alternative method for Docker when sudo requires password
        if use_docker_copy_method:
            return restore_table_docker_copy_method(
                backup_path, source_table, target_table, rename_to,
                config, container, env
            )

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

            # Check all processes in the pipeline for errors
            errors = []

            if rename_to:
                sed_return_code = sed_process.wait()
                if sed_return_code != 0:
                    sed_stderr = sed_process.stderr.read().decode()
                    errors.append(f"Table name replacement (sed) failed: {sed_stderr}")

            gunzip_return_code = gunzip_process.wait()
            if gunzip_return_code != 0:
                gunzip_stderr = gunzip_process.stderr.read().decode()
                if gunzip_stderr.strip():
                    errors.append(f"Decompression (gunzip) failed: {gunzip_stderr}")
                else:
                    # Gunzip failed but no error message - likely SIGPIPE from psql failure
                    errors.append(f"Decompression (gunzip) failed with exit code {gunzip_return_code} (possibly due to downstream process failure)")

            if psql_process.returncode != 0:
                psql_stderr_text = psql_stderr.decode()
                if psql_stderr_text.strip():
                    errors.append(f"Database restore (psql) failed: {psql_stderr_text}")
                else:
                    errors.append(f"Database restore (psql) failed with exit code {psql_process.returncode}")

            # Report all errors
            if errors:
                print(f"Error during restore:")
                for i, error in enumerate(errors, 1):
                    print(f"  {i}. {error}")
                print("\nTroubleshooting tips:")
                if mode == 'docker' and 'sudo' in str(restore_cmd):
                    print("  - If you see 'sudo password' prompts, configure passwordless sudo for docker:")
                    print("    sudo visudo -f /etc/sudoers.d/docker")
                    print("    Add: <your-username> ALL=(ALL) NOPASSWD: /usr/bin/docker")
                print("  - Check if the database is running and accessible")
                print("  - Verify database credentials in .env file")
                print("  - Check if table schema is compatible")
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

    # Perform restore
    result = restore_table(
        args.backup_file,
        config,
        table_name=args.table_name,
        drop_existing=args.drop_existing,
        cascade=args.cascade,
        rename_to=args.rename_to,
        mode=mode,
        container=container
    )

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
