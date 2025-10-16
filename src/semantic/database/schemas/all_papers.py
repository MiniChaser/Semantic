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
        """Get SQL statements for creating indexes on all_papers table"""
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_all_papers_corpus_id ON all_papers(corpus_id);",
            "CREATE INDEX IF NOT EXISTS idx_all_papers_venue ON all_papers(venue);",
            "CREATE INDEX IF NOT EXISTS idx_all_papers_year ON all_papers(year);",
            "CREATE INDEX IF NOT EXISTS idx_all_papers_release_id ON all_papers(release_id);",
            "CREATE INDEX IF NOT EXISTS idx_all_papers_citation_count ON all_papers(citation_count);",
            "CREATE INDEX IF NOT EXISTS idx_all_papers_authors ON all_papers USING GIN (authors);",
            "CREATE INDEX IF NOT EXISTS idx_all_papers_paper_id ON all_papers(paper_id);",
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

            # Create indexes
            self.logger.info("Creating indexes for all_papers table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            # Create triggers
            self.logger.info("Creating triggers for all_papers table...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")

            self.logger.info("all_papers table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create all_papers table: {e}")
            return False
