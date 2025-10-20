#!/usr/bin/env python3
"""
Progress monitoring utilities for long-running database operations

Provides real-time progress tracking for:
- SQL UPDATE statements (bulk updates)
- Index creation operations
- Custom database queries
"""

import time
import threading
from typing import Optional, Callable
from datetime import datetime
from tqdm import tqdm


class SQLUpdateMonitor:
    """
    Monitor progress of SQL UPDATE statements

    Works by periodically querying the database to count updated records.
    Displays real-time progress bar with ETA.
    """

    def __init__(self, db_manager, table_name: str, condition: str,
                 total_records: Optional[int] = None, update_interval: float = 2.0):
        """
        Initialize monitor

        Args:
            db_manager: DatabaseManager instance
            table_name: Target table name
            condition: WHERE condition to identify records being updated
            total_records: Total number of records to update (if known)
            update_interval: How often to check progress (seconds)
        """
        self.db_manager = db_manager
        self.table_name = table_name
        self.condition = condition
        self.total_records = total_records
        self.update_interval = update_interval

        self._stop_event = threading.Event()
        self._monitor_thread = None
        self._pbar = None
        self._last_count = 0
        self._start_time = None

    def start(self):
        """Start monitoring in background thread"""
        if self._monitor_thread is not None:
            return

        # Get total if not provided
        if self.total_records is None:
            print("Counting total records to update...")
            result = self.db_manager.fetch_one(f"""
                SELECT COUNT(*) as total
                FROM {self.table_name}
                WHERE {self.condition}
            """)
            self.total_records = result['total'] if result else 0
            print(f"Total records: {self.total_records:,}")

        if self.total_records == 0:
            print("No records to update")
            return

        # Initialize progress bar
        self._pbar = tqdm(
            total=self.total_records,
            desc="SQL UPDATE",
            unit="records",
            unit_scale=True,
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )

        self._start_time = time.time()
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def _monitor_loop(self):
        """Background monitoring loop"""
        while not self._stop_event.is_set():
            try:
                # Count records that have been updated (opposite of the condition)
                # We need to invert the condition logic
                # The condition identifies records TO BE updated (venue_normalized IS NULL)
                # So we count records that are NOT NULL (already updated)

                # Extract the core condition and invert it
                # For "venue_normalized IS NULL", we want to count "venue_normalized IS NOT NULL"
                inverted_condition = self._invert_condition(self.condition)

                result = self.db_manager.fetch_one(f"""
                    SELECT COUNT(*) as updated
                    FROM {self.table_name}
                    WHERE {inverted_condition}
                """)

                current_count = result['updated'] if result else 0

                # Update progress bar
                delta = current_count - self._last_count
                if delta > 0:
                    self._pbar.update(delta)
                    self._last_count = current_count

                # Sleep before next check
                time.sleep(self.update_interval)

            except Exception as e:
                # Silently continue if there's an error (might be temporary)
                time.sleep(self.update_interval)

    def _invert_condition(self, condition: str) -> str:
        """
        Invert the WHERE condition to count updated records

        This is a simplified version - handles common cases
        """
        # For complex conditions, we can use a different approach:
        # Count total minus remaining
        # But for now, we'll build a complementary condition

        lines = condition.strip().split('\n')
        inverted_lines = []

        for line in lines:
            line = line.strip()
            if 'IS NULL' in line:
                inverted_lines.append(line.replace('IS NULL', 'IS NOT NULL'))
            elif 'IS NOT NULL' in line:
                inverted_lines.append(line.replace('IS NOT NULL', 'IS NULL'))
            elif '!=' in line:
                inverted_lines.append(line.replace('!=', '='))
            elif '=' in line and '!=' not in line:
                # Be careful with =, might be part of another operator
                inverted_lines.append(line.replace('=', '!='))
            else:
                inverted_lines.append(line)

        return '\n'.join(inverted_lines)

    def stop(self):
        """Stop monitoring and close progress bar"""
        if self._monitor_thread is None:
            return

        self._stop_event.set()
        self._monitor_thread.join(timeout=5)

        if self._pbar:
            # Update to final count
            try:
                inverted_condition = self._invert_condition(self.condition)
                result = self.db_manager.fetch_one(f"""
                    SELECT COUNT(*) as updated
                    FROM {self.table_name}
                    WHERE {inverted_condition}
                """)
                final_count = result['updated'] if result else 0

                # Update to final value
                if final_count > self._last_count:
                    self._pbar.update(final_count - self._last_count)
            except:
                pass

            self._pbar.close()

        self._monitor_thread = None


class IndexCreationMonitor:
    """
    Monitor PostgreSQL index creation progress

    Uses pg_stat_progress_create_index system view (PostgreSQL 12+)
    """

    def __init__(self, db_manager, index_name: str, update_interval: float = 2.0):
        """
        Initialize monitor

        Args:
            db_manager: DatabaseManager instance
            index_name: Name of index being created
            update_interval: How often to check progress (seconds)
        """
        self.db_manager = db_manager
        self.index_name = index_name
        self.update_interval = update_interval

        self._stop_event = threading.Event()
        self._monitor_thread = None
        self._pbar = None
        self._pg_version = None

    def _check_pg_version(self) -> bool:
        """Check if PostgreSQL version supports pg_stat_progress_create_index"""
        try:
            result = self.db_manager.fetch_one("SHOW server_version")
            if result:
                version_str = result['server_version']
                # Extract major version number
                major_version = int(version_str.split('.')[0])
                self._pg_version = major_version
                return major_version >= 12
        except:
            pass
        return False

    def start(self):
        """Start monitoring in background thread"""
        if self._monitor_thread is not None:
            return

        # Check PostgreSQL version
        if not self._check_pg_version():
            print(f"⚠️  PostgreSQL version {self._pg_version or 'unknown'} doesn't support progress monitoring")
            print("   (Requires PostgreSQL 12+)")
            print("   Index creation will proceed without progress bar...")
            return

        print(f"✓ PostgreSQL {self._pg_version} supports progress monitoring\n")

        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

        # Give thread time to initialize
        time.sleep(0.5)

    def _monitor_loop(self):
        """Background monitoring loop"""
        pbar_initialized = False

        while not self._stop_event.is_set():
            try:
                # Query progress from pg_stat_progress_create_index
                result = self.db_manager.fetch_one("""
                    SELECT
                        phase,
                        blocks_total,
                        blocks_done,
                        tuples_total,
                        tuples_done
                    FROM pg_stat_progress_create_index
                    WHERE relid = (
                        SELECT oid FROM pg_class
                        WHERE relname = (
                            SELECT tablename FROM pg_indexes
                            WHERE indexname = %s
                            LIMIT 1
                        )
                    )
                    LIMIT 1
                """, (self.index_name,))

                if result and result['blocks_total'] > 0:
                    # Initialize progress bar if not done yet
                    if not pbar_initialized:
                        self._pbar = tqdm(
                            total=result['blocks_total'],
                            desc=f"Creating index ({result['phase']})",
                            unit="blocks",
                            unit_scale=True,
                            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
                        )
                        pbar_initialized = True

                    # Update progress
                    if self._pbar:
                        self._pbar.n = result['blocks_done']
                        self._pbar.set_description(f"Creating index ({result['phase']})")
                        self._pbar.refresh()

                time.sleep(self.update_interval)

            except Exception as e:
                # Silently continue
                time.sleep(self.update_interval)

    def stop(self):
        """Stop monitoring and close progress bar"""
        if self._monitor_thread is None:
            return

        self._stop_event.set()
        self._monitor_thread.join(timeout=5)

        if self._pbar:
            self._pbar.close()

        self._monitor_thread = None


class SimpleCountMonitor:
    """
    Simple progress monitor that counts matching records periodically

    Useful when you can't easily track progress through the operation itself,
    but can count how many records match a certain condition.
    """

    def __init__(self, db_manager, count_query: str, total: int,
                 description: str = "Progress", update_interval: float = 2.0):
        """
        Initialize monitor

        Args:
            db_manager: DatabaseManager instance
            count_query: SQL query that returns a 'count' column
            total: Total expected count
            description: Description for progress bar
            update_interval: How often to check progress (seconds)
        """
        self.db_manager = db_manager
        self.count_query = count_query
        self.total = total
        self.description = description
        self.update_interval = update_interval

        self._stop_event = threading.Event()
        self._monitor_thread = None
        self._pbar = None
        self._last_count = 0

    def start(self):
        """Start monitoring in background thread"""
        if self._monitor_thread is not None:
            return

        self._pbar = tqdm(
            total=self.total,
            desc=self.description,
            unit="records",
            unit_scale=True,
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )

        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def _monitor_loop(self):
        """Background monitoring loop"""
        while not self._stop_event.is_set():
            try:
                result = self.db_manager.fetch_one(self.count_query)
                current_count = result['count'] if result and 'count' in result else 0

                # Update progress bar
                delta = current_count - self._last_count
                if delta > 0:
                    self._pbar.update(delta)
                    self._last_count = current_count

                time.sleep(self.update_interval)

            except Exception as e:
                time.sleep(self.update_interval)

    def stop(self):
        """Stop monitoring and close progress bar"""
        if self._monitor_thread is None:
            return

        self._stop_event.set()
        self._monitor_thread.join(timeout=5)

        if self._pbar:
            # Final update
            try:
                result = self.db_manager.fetch_one(self.count_query)
                final_count = result['count'] if result and 'count' in result else 0
                if final_count > self._last_count:
                    self._pbar.update(final_count - self._last_count)
            except:
                pass

            self._pbar.close()

        self._monitor_thread = None
