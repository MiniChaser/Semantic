"""
Dataset Author Papers table schema definition
Stores all papers by authors from dataset_papers (conference authors)
"""

import logging
from typing import List
from ..connection import DatabaseManager


class DatasetAuthorPapersSchema:
    """Dataset Author Papers table schema definition"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.DatasetAuthorPapersSchema')
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
        """Get SQL for creating dataset_author_papers table"""
        return """
        CREATE TABLE IF NOT EXISTS dataset_author_papers (
            id SERIAL PRIMARY KEY,
            corpus_id BIGINT NOT NULL,
            author_id VARCHAR(100) NOT NULL,
            author_name VARCHAR(500),
            author_sequence INTEGER,
            paper_id VARCHAR(100),
            external_ids JSONB,
            title TEXT NOT NULL,
            abstract TEXT,
            venue TEXT,
            year INTEGER,
            citation_count INTEGER DEFAULT 0,
            reference_count INTEGER DEFAULT 0,
            influential_citation_count INTEGER DEFAULT 0,
            fields_of_study JSONB,
            publication_types JSONB,
            is_open_access BOOLEAN DEFAULT FALSE,
            open_access_pdf TEXT,
            is_conference_paper BOOLEAN DEFAULT FALSE,
            source_file VARCHAR(255),
            release_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(corpus_id, author_id)
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on dataset_author_papers table"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_dataset_author_papers_corpus_id ON dataset_author_papers(corpus_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_author_papers_author_id ON dataset_author_papers(author_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_author_papers_venue ON dataset_author_papers(venue);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_author_papers_year ON dataset_author_papers(year);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_author_papers_release_id ON dataset_author_papers(release_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_author_papers_citation_count ON dataset_author_papers(citation_count);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_author_papers_conference_flag ON dataset_author_papers(is_conference_paper);",
        ]

    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers on dataset_author_papers table"""
        return [
            # Trigger function to update updated_at on row updates
            """
            CREATE OR REPLACE FUNCTION update_dataset_author_papers_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Trigger to automatically update updated_at column
            """
            DROP TRIGGER IF EXISTS trigger_dataset_author_papers_updated_at ON dataset_author_papers;
            CREATE TRIGGER trigger_dataset_author_papers_updated_at
                BEFORE UPDATE ON dataset_author_papers
                FOR EACH ROW
                EXECUTE FUNCTION update_dataset_author_papers_updated_at();
            """
        ]

    def create_table(self) -> bool:
        """Create dataset_author_papers table with indexes and triggers"""
        try:
            self.logger.info("Creating dataset_author_papers table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create dataset_author_papers table")

            # Create indexes
            self.logger.info("Creating indexes for dataset_author_papers table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            # Create triggers
            self.logger.info("Creating triggers for dataset_author_papers table...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")

            self.logger.info("dataset_author_papers table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create dataset_author_papers table: {e}")
            return False
