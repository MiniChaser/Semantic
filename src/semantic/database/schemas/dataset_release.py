"""
Dataset Release table schema definition
"""

import logging
from typing import List
from ..connection import DatabaseManager


class DatasetReleaseSchema:
    """Dataset Release table schema definition"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.DatasetReleaseSchema')
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
        """Get SQL for creating dataset_release table"""
        return """
        CREATE TABLE IF NOT EXISTS dataset_release (
            id SERIAL PRIMARY KEY,
            release_id VARCHAR(100) UNIQUE NOT NULL,
            dataset_name VARCHAR(50) NOT NULL,
            release_date TIMESTAMP,
            description TEXT,
            file_count INTEGER DEFAULT 0,
            total_papers_processed INTEGER DEFAULT 0,
            papers_inserted INTEGER DEFAULT 0,
            papers_updated INTEGER DEFAULT 0,
            processing_status VARCHAR(20) DEFAULT 'pending',
            download_start_time TIMESTAMP,
            download_end_time TIMESTAMP,
            processing_start_time TIMESTAMP,
            processing_end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on dataset_release table"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_dataset_release_release_id ON dataset_release(release_id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_release_dataset_name ON dataset_release(dataset_name);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_release_status ON dataset_release(processing_status);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_release_date ON dataset_release(release_date);",
        ]

    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers on dataset_release table"""
        return [
            # Trigger function to update updated_at on row updates
            """
            CREATE OR REPLACE FUNCTION update_dataset_release_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Trigger to automatically update updated_at column
            """
            DROP TRIGGER IF EXISTS trigger_dataset_release_updated_at ON dataset_release;
            CREATE TRIGGER trigger_dataset_release_updated_at
                BEFORE UPDATE ON dataset_release
                FOR EACH ROW
                EXECUTE FUNCTION update_dataset_release_updated_at();
            """
        ]

    def create_table(self) -> bool:
        """Create dataset_release table with indexes and triggers"""
        try:
            self.logger.info("Creating dataset_release table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create dataset_release table")

            # Create indexes
            self.logger.info("Creating indexes for dataset_release table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            # Create triggers
            self.logger.info("Creating triggers for dataset_release table...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")

            self.logger.info("dataset_release table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create dataset_release table: {e}")
            return False
