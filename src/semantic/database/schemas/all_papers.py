"""
All Papers table schema definition
Base table for all 200M papers from S2 dataset (no filtering)
"""

import logging
from typing import List
from ..connection import DatabaseManager


class AllPapersSchema:
    """All Papers table schema definition"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.AllPapersSchema')
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
        """Get SQL for creating all_papers table"""
        return """
        CREATE TABLE IF NOT EXISTS all_papers (
            id SERIAL PRIMARY KEY,
            corpus_id BIGINT UNIQUE NOT NULL,
            paper_id VARCHAR(100),
            external_ids JSONB,
            title TEXT NOT NULL,
            abstract TEXT,
            venue TEXT,
            venue_normalized TEXT,
            year INTEGER,
            citation_count INTEGER DEFAULT 0,
            reference_count INTEGER DEFAULT 0,
            influential_citation_count INTEGER DEFAULT 0,
            authors JSONB,
            fields_of_study JSONB,
            publication_types JSONB,
            is_open_access BOOLEAN DEFAULT FALSE,
            open_access_pdf TEXT,
            source_file VARCHAR(255),
            release_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """
        Get SQL statements for creating indexes on all_papers table

        Only creates 2 essential indexes required by Stage 2:
        - corpus_id (UNIQUE): Required by Stage 2/3 for ORDER BY cursor pagination
        - venue_normalized (B-tree): Required by Stage 2 for conference filtering with IN queries

        Note: authors GIN index removed due to extremely long build time (6+ hours for 200M records).
              If Stage 3 requires author queries, consider adding optimized index later.
        """
        return [
            # Required by Stage 2/3: ORDER BY corpus_id (cursor pagination)
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_all_papers_corpus_id ON all_papers(corpus_id);",
            # Required by Stage 2: Fast IN query on venue_normalized (B-tree, partial index for non-NULL)
            "CREATE INDEX IF NOT EXISTS idx_all_papers_venue_normalized ON all_papers(venue_normalized) WHERE venue_normalized IS NOT NULL;",
        ]

    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers on all_papers table"""
        return [
            # Trigger function to update updated_at on row updates
            """
            CREATE OR REPLACE FUNCTION update_all_papers_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Trigger to automatically update updated_at column
            """
            DROP TRIGGER IF EXISTS trigger_all_papers_updated_at ON all_papers;
            CREATE TRIGGER trigger_all_papers_updated_at
                BEFORE UPDATE ON all_papers
                FOR EACH ROW
                EXECUTE FUNCTION update_all_papers_updated_at();
            """
        ]

    def create_table(self) -> bool:
        """Create all_papers table with indexes and triggers"""
        try:
            self.logger.info("Creating all_papers table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create all_papers table")

            # Check if table has existing data
            count_query = "SELECT COUNT(*) as count FROM all_papers LIMIT 1"
            result = self.db_manager.fetch_one(count_query)
            has_data = result and result.get('count', 0) > 0

            if has_data:
                self.logger.info("Table has existing data - skipping index creation")
                self.logger.info("(Indexes will be managed by drop_indexes/recreate_indexes)")
            else:
                # Create indexes only if table is empty
                self.logger.info("Creating indexes for all_papers table...")
                for index_sql in self.get_indexes_sql():
                    if not self.db_manager.execute_query(index_sql):
                        self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            # Create triggers (always safe, CREATE OR REPLACE)
            self.logger.info("Creating triggers for all_papers table...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")

            self.logger.info("all_papers table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create all_papers table: {e}")
            return False

    def drop_indexes(self) -> bool:
        """
        Drop all indexes on all_papers table (except primary key)
        Used before bulk import for maximum performance
        """
        try:
            self.logger.info("Dropping indexes on all_papers table for bulk import...")

            # Drop only the 2 indexes we will recreate
            drop_statements = [
                "DROP INDEX IF EXISTS idx_all_papers_corpus_id CASCADE;",
                "DROP INDEX IF EXISTS idx_all_papers_venue_normalized CASCADE;",
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
                "ALTER TABLE all_papers DROP CONSTRAINT IF EXISTS all_papers_corpus_id_key CASCADE;"
            )
            self.logger.info("✓ UNIQUE constraint dropped")

            self.logger.info("✓ All indexes and constraints dropped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop indexes: {e}")
            return False

    def recreate_indexes(self) -> bool:
        """
        Recreate 2 essential indexes on all_papers table after bulk import

        Creates only the indexes required by Stage 2:
        - corpus_id (UNIQUE)
        - venue_normalized (B-tree partial index)
        """
        try:
            self.logger.info("Recreating indexes on all_papers table...")
            self.logger.info("Creating 2 essential indexes (corpus_id, venue_normalized)")
            self.logger.info("This may take 20-30 minutes for 200M records...")

            indexes = self.get_indexes_sql()

            for idx, index_sql in enumerate(indexes, 1):
                index_name = index_sql.split("idx_")[1].split(" ")[0] if "idx_" in index_sql else f"index_{idx}"
                self.logger.info(f"Creating index {idx}/{len(indexes)}: idx_all_papers_{index_name}...")

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
        """Check if indexes exist on all_papers table"""
        try:
            query = """
                SELECT COUNT(*) as index_count
                FROM pg_indexes
                WHERE tablename = 'all_papers'
                AND indexname LIKE 'idx_all_papers_%'
            """
            result = self.db_manager.fetch_one(query)
            count = result['index_count'] if result else 0
            return count > 0
        except Exception as e:
            self.logger.warning(f"Could not check indexes: {e}")
            return False
