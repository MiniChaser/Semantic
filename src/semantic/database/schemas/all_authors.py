"""
Dataset Authors table schema definition
Base table for all 75M authors from S2 dataset (no filtering)
"""

import logging
from typing import List
from ..connection import DatabaseManager


class DatasetAuthorsSchema:
    """Dataset Authors table schema definition"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.DatasetAuthorsSchema')
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
        """Get SQL for creating dataset_authors table"""
        return """
        CREATE TABLE IF NOT EXISTS dataset_authors (
            id SERIAL PRIMARY KEY,
            author_id VARCHAR(100) UNIQUE NOT NULL,
            name VARCHAR(500) NOT NULL,
            aliases JSONB,
            affiliations JSONB,
            homepage VARCHAR(500),
            paper_count INTEGER DEFAULT 0,
            citation_count INTEGER DEFAULT 0,
            h_index INTEGER DEFAULT 0,
            external_ids JSONB,
            url VARCHAR(500),
            source_file VARCHAR(255),
            release_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on dataset_authors table"""
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_authors_author_id ON dataset_authors(author_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_authors_name ON dataset_authors(name);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_authors_h_index ON dataset_authors(h_index);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_authors_citation_count ON dataset_authors(citation_count);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_authors_paper_count ON dataset_authors(paper_count);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_authors_release_id ON dataset_authors(release_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_authors_aliases ON dataset_authors USING GIN (aliases);",
        ]

    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers on dataset_authors table"""
        return [
            # Trigger function to update updated_at on row updates
            """
            CREATE OR REPLACE FUNCTION update_dataset_authors_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Trigger to automatically update updated_at column
            """
            DROP TRIGGER IF EXISTS trigger_dataset_authors_updated_at ON dataset_authors;
            CREATE TRIGGER trigger_dataset_authors_updated_at
                BEFORE UPDATE ON dataset_authors
                FOR EACH ROW
                EXECUTE FUNCTION update_dataset_authors_updated_at();
            """
        ]

    def create_table(self) -> bool:
        """Create dataset_authors table with indexes and triggers"""
        try:
            self.logger.info("Creating dataset_authors table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create dataset_authors table")

            # Create indexes
            self.logger.info("Creating indexes for dataset_authors table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            # Create triggers
            self.logger.info("Creating triggers for dataset_authors table...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")

            self.logger.info("dataset_authors table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create dataset_authors table: {e}")
            return False

    def drop_indexes(self) -> bool:
        """
        Drop all indexes on dataset_authors table (except primary key)
        Used before bulk import for maximum performance
        """
        try:
            self.logger.info("Dropping indexes on dataset_authors table for bulk import...")

            drop_statements = [
                "DROP INDEX IF EXISTS idx_dataset_authors_author_id CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_authors_name CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_authors_h_index CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_authors_citation_count CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_authors_paper_count CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_authors_release_id CASCADE;",
                "DROP INDEX IF EXISTS idx_dataset_authors_aliases CASCADE;",
            ]

            for drop_sql in drop_statements:
                self.db_manager.execute_query(drop_sql)

            # Also drop UNIQUE constraint on author_id (will be recreated with index)
            self.logger.info("Dropping UNIQUE constraint on author_id...")
            self.db_manager.execute_query(
                "ALTER TABLE dataset_authors DROP CONSTRAINT IF EXISTS dataset_authors_author_id_key CASCADE;"
            )

            self.logger.info("✓ All indexes and constraints dropped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop indexes: {e}")
            return False

    def recreate_indexes(self) -> bool:
        """
        Recreate all indexes on dataset_authors table after bulk import
        Uses CONCURRENTLY where possible to avoid blocking
        """
        try:
            self.logger.info("Recreating indexes on dataset_authors table...")
            self.logger.info("This may take 10-30 minutes for 75M records...")

            # Recreate indexes (same as get_indexes_sql but with progress tracking)
            indexes = self.get_indexes_sql()

            for idx, index_sql in enumerate(indexes, 1):
                index_name = index_sql.split("idx_")[1].split(" ")[0] if "idx_" in index_sql else f"index_{idx}"
                self.logger.info(f"Creating index {idx}/{len(indexes)}: idx_dataset_authors_{index_name}...")

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
        """Check if indexes exist on dataset_authors table"""
        try:
            query = """
                SELECT COUNT(*) as index_count
                FROM pg_indexes
                WHERE tablename = 'dataset_authors'
                AND indexname LIKE 'idx_dataset_authors_%'
            """
            result = self.db_manager.fetch_one(query)
            count = result['index_count'] if result else 0
            return count > 0
        except Exception as e:
            self.logger.warning(f"Could not check indexes: {e}")
            return False
