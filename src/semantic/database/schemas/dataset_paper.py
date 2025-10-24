"""
Dataset Paper table schema definition
"""

import logging
from pathlib import Path
from typing import List
from ..connection import DatabaseManager


class DatasetPaperSchema:
    """Dataset Paper table schema definition - Always uses partitioned table"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.DatasetPaperSchema')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def get_indexes_sql(self) -> List[str]:
        """
        Get SQL statements for creating indexes on dataset_papers table (for drop/recreate operations)

        Optimized index configuration (7 core indexes):
        1. corpus_id + year - Composite UNIQUE constraint (required for UPSERT)
        2. paper_id - Semantic Scholar paper ID lookups
        3. title - Normalized title lookups (exact match)
        4. conference_normalized - Conference filtering
        5. year - Partition key
        6. dblp_id - DBLP identifier lookups (partial index)
        7. authors (GIN) - Author JSONB queries
        """
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_papers_corpus_id_year ON dataset_papers(corpus_id, year);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_paper_id ON dataset_papers(paper_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_title ON dataset_papers(title);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_conference ON dataset_papers(conference_normalized);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_year ON dataset_papers(year);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_dblp_id ON dataset_papers(dblp_id) WHERE dblp_id IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_authors ON dataset_papers USING GIN (authors);",
        ]

    def create_table(self) -> bool:
        """Create partitioned dataset_papers table with indexes and triggers"""
        try:
            self.logger.info("Creating PARTITIONED dataset_papers table...")
            return self._create_partitioned_table()
        except Exception as e:
            self.logger.error(f"Failed to create dataset_papers table: {e}")
            return False

    def _create_partitioned_table(self) -> bool:
        """Create partitioned version of dataset_papers table"""
        try:
            # Get SQL file path
            sql_file = Path(__file__).parent.parent / 'sql' / 'schemas' / 'dataset_papers_partitioned.sql'

            if not sql_file.exists():
                raise Exception(f"Partitioned schema SQL file not found: {sql_file}")

            # Read and execute SQL file
            self.logger.info(f"Loading partitioned schema from: {sql_file}")
            with open(sql_file, 'r') as f:
                sql_content = f.read()

            # Execute the entire SQL file
            if not self.db_manager.execute_query(sql_content):
                raise Exception("Failed to execute partitioned schema SQL")

            self.logger.info("Partitioned dataset_papers table created successfully")
            self.logger.info("Created 34 partitions (NULL, 3 historical, 30 annual, 1 future)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create partitioned table: {e}")
            return False

    def check_is_partitioned(self) -> bool:
        """Check if dataset_papers table is partitioned"""
        try:
            query = """
                SELECT EXISTS (
                    SELECT 1 FROM pg_partitioned_table
                    WHERE partrelid = 'dataset_papers'::regclass
                ) as is_partitioned;
            """
            result = self.db_manager.fetch_one(query)
            return result['is_partitioned'] if result else False
        except Exception as e:
            self.logger.warning(f"Could not check partition status: {e}")
            return False

    def get_partition_info(self) -> List[dict]:
        """Get information about all partitions"""
        try:
            query = """
                SELECT
                    child.relname AS partition_name,
                    pg_get_expr(child.relpartbound, child.oid) AS partition_bounds,
                    pg_size_pretty(pg_total_relation_size(child.oid)) AS size,
                    (SELECT count(*) FROM pg_class c WHERE c.oid = child.oid) as exists
                FROM pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                WHERE parent.relname = 'dataset_papers'
                ORDER BY child.relname;
            """
            return self.db_manager.fetch_all(query)
        except Exception as e:
            self.logger.warning(f"Could not get partition info: {e}")
            return []

    def drop_indexes(self) -> bool:
        """
        Drop non-essential indexes on dataset_papers table for bulk import performance

        IMPORTANT: Keeps composite UNIQUE constraint (corpus_id, year) for ON CONFLICT to work!
        Only drops the 6 secondary indexes that slow down inserts.

        WARNING: This will make queries very slow until indexes are recreated!
        """
        try:
            self.logger.info("Dropping non-essential indexes on dataset_papers table for bulk import...")
            self.logger.info("⚠️  Keeping UNIQUE constraint on (corpus_id, year) - required for ON CONFLICT")

            # Drop only 6 secondary indexes (NOT the unique constraint!)
            # Keep idx_dataset_papers_corpus_id_year because ON CONFLICT needs it
            drop_statements = [
                "DROP INDEX IF EXISTS idx_dataset_papers_paper_id CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_title CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_conference CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_year CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_dblp_id CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_authors CASCADE;",
            ]

            total = len(drop_statements)
            for idx, drop_sql in enumerate(drop_statements, 1):
                # Extract index name for logging
                index_name = drop_sql.split("DROP INDEX IF EXISTS ")[1].split(" ")[0]
                self.logger.info(f"Dropping index {idx}/{total}: {index_name}...")
                self.db_manager.execute_query(drop_sql)
                self.logger.info(f"✓ Index {idx}/{total} dropped")

            self.logger.info("✓ All non-essential indexes dropped successfully")
            self.logger.info("✓ Kept UNIQUE constraint on (corpus_id, year) - needed for ON CONFLICT")
            self.logger.info("⚠️  Queries will be slow until indexes are recreated!")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop indexes: {e}")
            return False

    def recreate_indexes(self) -> bool:
        """
        Recreate the 6 secondary indexes on dataset_papers table after bulk import

        Note: UNIQUE constraint on (corpus_id, year) is kept during import, so only 6 indexes need rebuilding

        Optimized index set (7 total, rebuild 6):
        - paper_id, title, conference_normalized, year, dblp_id (B-tree, fast)
        - authors (GIN, slower)

        This will take time depending on the number of records:
        - 1M records: ~2-5 minutes
        - 10M records: ~10-35 minutes
        - 17M records: ~30-70 minutes (GIN index takes ~30 minutes, title index ~5-10 minutes)
        """
        try:
            self.logger.info("Recreating secondary indexes on dataset_papers table...")
            self.logger.info("Creating 6 indexes (paper_id, title, conference, year, dblp_id, authors)")
            self.logger.info("Note: UNIQUE constraint on (corpus_id, year) was kept during import")
            self.logger.info("This may take 30-70 minutes for 17M records...")

            indexes = self.get_indexes_sql()

            # Filter out corpus_id_year unique index (already exists)
            indexes_to_create = [idx for idx in indexes if 'corpus_id_year' not in idx.lower()]

            self.logger.info(f"Will recreate {len(indexes_to_create)} secondary indexes")

            for idx, index_sql in enumerate(indexes_to_create, 1):
                index_name = index_sql.split("idx_")[1].split(" ")[0] if "idx_" in index_sql else f"index_{idx}"

                # Estimate time for each index
                if "GIN" in index_sql:
                    self.logger.info(f"Creating index {idx}/{len(indexes_to_create)}: idx_dataset_papers_{index_name}... (GIN index - may take 30+ minutes)")
                elif "dblp_id" in index_sql:
                    self.logger.info(f"Creating index {idx}/{len(indexes_to_create)}: idx_dataset_papers_{index_name}... (partial index - faster)")
                else:
                    self.logger.info(f"Creating index {idx}/{len(indexes_to_create)}: idx_dataset_papers_{index_name}...")

                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:80]}...")
                else:
                    self.logger.info(f"✓ Index {idx}/{len(indexes_to_create)} created successfully")

            self.logger.info("✓ All secondary indexes recreated successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to recreate indexes: {e}")
            return False

    def check_indexes_exist(self) -> bool:
        """Check if indexes exist on dataset_papers table"""
        try:
            query = """
                SELECT COUNT(*) as index_count
                FROM pg_indexes
                WHERE tablename = 'dataset_papers'
                AND indexname LIKE 'idx_dataset_papers_%'
            """
            result = self.db_manager.fetch_one(query)
            count = result['index_count'] if result else 0
            self.logger.info(f"Found {count} indexes on dataset_papers table")
            return count > 0
        except Exception as e:
            self.logger.warning(f"Could not check indexes: {e}")
            return False
