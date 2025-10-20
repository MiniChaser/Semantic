"""
Conference Pattern table schema definition
Used for database-side conference matching
"""

import logging
from typing import List
from ..connection import DatabaseManager


class ConferencePatternSchema:
    """Conference Pattern table schema definition"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.ConferencePatternSchema')
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
        """Get SQL for creating conference_patterns table"""
        return """
        CREATE TABLE IF NOT EXISTS conference_patterns (
            id SERIAL PRIMARY KEY,
            conference VARCHAR(100) NOT NULL,
            pattern TEXT NOT NULL,
            match_type VARCHAR(20) CHECK (match_type IN ('exact', 'contains', 'alias')),
            UNIQUE(conference, pattern, match_type)
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_conference_patterns_conference ON conference_patterns(conference);",
            "CREATE INDEX IF NOT EXISTS idx_conference_patterns_pattern ON conference_patterns(pattern);",
            "CREATE INDEX IF NOT EXISTS idx_conference_patterns_lower_pattern ON conference_patterns(LOWER(pattern));",
        ]

    def create_table(self) -> bool:
        """Create conference_patterns table with indexes"""
        try:
            self.logger.info("Creating conference_patterns table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create conference_patterns table")

            # Create indexes
            self.logger.info("Creating indexes for conference_patterns table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            self.logger.info("conference_patterns table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create conference_patterns table: {e}")
            return False
