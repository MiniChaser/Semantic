"""
Dataset Paper table schema definition
"""

import logging
from pathlib import Path
from typing import List
from ..connection import DatabaseManager


class DatasetPaperSchema:
    """Dataset Paper table schema definition"""

    def __init__(self, db_manager: DatabaseManager, use_partitioning: bool = False):
        self.db_manager = db_manager
        self.use_partitioning = use_partitioning
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

    def get_table_sql(self) -> str:
        """Get SQL for creating dataset_papers table"""
        return """
        CREATE TABLE IF NOT EXISTS dataset_papers (
            id SERIAL PRIMARY KEY,
            corpus_id BIGINT UNIQUE NOT NULL,
            paper_id VARCHAR(100),
            external_ids JSONB,
            title TEXT NOT NULL,
            abstract TEXT,
            venue TEXT,
            year INTEGER,
            citation_count INTEGER DEFAULT 0,
            reference_count INTEGER DEFAULT 0,
            influential_citation_count INTEGER DEFAULT 0,
            authors JSONB,
            fields_of_study JSONB,
            publication_types JSONB,
            is_open_access BOOLEAN DEFAULT FALSE,
            open_access_pdf TEXT,
            conference_normalized VARCHAR(100),
            source_file VARCHAR(255),
            release_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on dataset_papers table"""
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_papers_corpus_id ON dataset_papers(corpus_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_venue ON dataset_papers(venue);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_conference ON dataset_papers(conference_normalized);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_year ON dataset_papers(year);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_release_id ON dataset_papers(release_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_citation_count ON dataset_papers(citation_count);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_papers_authors ON dataset_papers USING GIN (authors);",
        ]

    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers on dataset_papers table"""
        return [
            # Trigger function to update updated_at on row updates
            """
            CREATE OR REPLACE FUNCTION update_dataset_papers_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Trigger to automatically update updated_at column
            """
            DROP TRIGGER IF EXISTS trigger_dataset_papers_updated_at ON dataset_papers;
            CREATE TRIGGER trigger_dataset_papers_updated_at
                BEFORE UPDATE ON dataset_papers
                FOR EACH ROW
                EXECUTE FUNCTION update_dataset_papers_updated_at();
            """
        ]

    def create_table(self) -> bool:
        """Create dataset_papers table with indexes and triggers"""
        try:
            if self.use_partitioning:
                self.logger.info("Creating PARTITIONED dataset_papers table...")
                return self._create_partitioned_table()
            else:
                self.logger.info("Creating dataset_papers table...")
                if not self.db_manager.execute_query(self.get_table_sql()):
                    raise Exception("Failed to create dataset_papers table")

                # Create indexes
                self.logger.info("Creating indexes for dataset_papers table...")
                for index_sql in self.get_indexes_sql():
                    if not self.db_manager.execute_query(index_sql):
                        self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

                # Create triggers
                self.logger.info("Creating triggers for dataset_papers table...")
                for trigger_sql in self.get_triggers_sql():
                    if not self.db_manager.execute_query(trigger_sql):
                        self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")

                self.logger.info("dataset_papers table created successfully")
                return True

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
        Drop all indexes on dataset_papers table (except primary key)
        Used before bulk import for maximum performance

        WARNING: This will make queries very slow until indexes are recreated!
        """
        try:
            self.logger.info("Dropping indexes on dataset_papers table for bulk import...")

            # Drop all 7 indexes
            drop_statements = [
                "DROP INDEX IF EXISTS idx_dataset_papers_corpus_id CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_venue CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_conference CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_year CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_release_id CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_citation_count CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_papers_authors CASCADE;",
            ]

            total = len(drop_statements)
            for idx, drop_sql in enumerate(drop_statements, 1):
                # Extract index name for logging
                index_name = drop_sql.split("DROP INDEX IF EXISTS ")[1].split(" ")[0]
                self.logger.info(f"Dropping index {idx}/{total}: {index_name}...")
                self.db_manager.execute_query(drop_sql)
                self.logger.info(f"✓ Index {idx}/{total} dropped")

            # Also drop UNIQUE constraint on corpus_id (will be recreated with index)
            self.logger.info("Dropping UNIQUE constraint on corpus_id...")
            self.db_manager.execute_query(
                "ALTER TABLE dataset_papers DROP CONSTRAINT IF EXISTS dataset_papers_corpus_id_key CASCADE;"
            )
            self.logger.info("✓ UNIQUE constraint dropped")

            self.logger.info("✓ All indexes and constraints dropped successfully")
            self.logger.info("⚠️  Queries will be slow until indexes are recreated!")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop indexes: {e}")
            return False

    def recreate_indexes(self) -> bool:
        """
        Recreate all 7 indexes on dataset_papers table after bulk import

        This will take time depending on the number of records:
        - 1M records: ~2-5 minutes
        - 10M records: ~20-50 minutes
        - 17M records: ~30-90 minutes (GIN index is slowest)
        """
        try:
            self.logger.info("Recreating indexes on dataset_papers table...")
            self.logger.info("Creating 7 indexes (corpus_id, venue, conference, year, release_id, citation_count, authors)")
            self.logger.info("This may take 30-90 minutes for 17M records...")

            indexes = self.get_indexes_sql()

            for idx, index_sql in enumerate(indexes, 1):
                index_name = index_sql.split("idx_")[1].split(" ")[0] if "idx_" in index_sql else f"index_{idx}"

                # Estimate time for each index
                if "GIN" in index_sql:
                    self.logger.info(f"Creating index {idx}/{len(indexes)}: idx_dataset_papers_{index_name}... (GIN index - may take 30+ minutes)")
                else:
                    self.logger.info(f"Creating index {idx}/{len(indexes)}: idx_dataset_papers_{index_name}...")

                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:80]}...")
                else:
                    self.logger.info(f"✓ Index {idx}/{len(indexes)} created successfully")

            self.logger.info("✓ All indexes recreated successfully")
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
