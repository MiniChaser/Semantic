"""
Processing metadata table schema definition
Unified table to manage all processing metadata, replacing separate metadata tables
"""

import logging
from typing import List
from ..connection import DatabaseManager


class ProcessingMetadataSchema:
    """Processing metadata table schema definition"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.ProcessingMetadataSchema')
        logger.setLevel(logging.INFO)

        # Don't add handlers if root logger is already configured
        # This prevents duplicate logging when root logger has handlers
        root_logger = logging.getLogger()
        if not logger.handlers and not root_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def get_table_sql(self) -> str:
        """Get SQL for creating processing metadata table"""
        return """
        CREATE TABLE IF NOT EXISTS processing_metadata (
            id                  SERIAL PRIMARY KEY,
            entity_type         VARCHAR(50) NOT NULL,          -- 'dblp_paper', 'enriched_paper', 'author', 'pipeline'
            entity_id           INTEGER,                       -- Related entity ID (can be NULL for global status)
            process_type        VARCHAR(50) NOT NULL,          -- 'dblp_sync', 's2_enrichment', 'pdf_download', 'author_processing', 'full_pipeline'
            status              VARCHAR(20) DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed', 'skipped'
            started_at          TIMESTAMP,                     -- Start time
            completed_at        TIMESTAMP,                     -- Completion time
            error_message       TEXT,                          -- Error information
            metadata_json       JSONB,                         -- Additional processing info (flexible extension)
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on processing metadata table"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_processing_metadata_entity ON processing_metadata(entity_type, entity_id);",
            "CREATE INDEX IF NOT EXISTS idx_processing_metadata_process ON processing_metadata(process_type, status);",
            "CREATE INDEX IF NOT EXISTS idx_processing_metadata_status ON processing_metadata(status, created_at);",
            "CREATE INDEX IF NOT EXISTS idx_processing_metadata_updated ON processing_metadata(updated_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_processing_metadata_json ON processing_metadata USING GIN(metadata_json);"
        ]

    def get_constraints_sql(self) -> List[str]:
        """Get SQL statements for creating constraints"""
        return [
            "ALTER TABLE processing_metadata ADD CONSTRAINT IF NOT EXISTS chk_status CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'skipped'));",
            "ALTER TABLE processing_metadata ADD CONSTRAINT IF NOT EXISTS chk_entity_type CHECK (entity_type IN ('dblp_paper', 'enriched_paper', 'author', 'pipeline'));"
        ]

    def create_table(self) -> bool:
        """Create processing metadata table with indexes and constraints"""
        try:
            self.logger.info("Creating processing metadata table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create processing metadata table")

            # Create indexes
            self.logger.info("Creating indexes for processing metadata table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            # Create constraints
            self.logger.info("Creating constraints for processing metadata table...")
            for constraint_sql in self.get_constraints_sql():
                if not self.db_manager.execute_query(constraint_sql):
                    self.logger.warning(f"Failed to create constraint: {constraint_sql[:50]}...")

            self.logger.info("Processing metadata table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create processing metadata table: {e}")
            return False

    def drop_table(self) -> bool:
        """Drop processing metadata table (for development/testing)"""
        try:
            self.logger.info("Dropping processing metadata table...")
            if not self.db_manager.execute_query("DROP TABLE IF EXISTS processing_metadata CASCADE;"):
                raise Exception("Failed to drop processing metadata table")

            self.logger.info("Processing metadata table dropped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop processing metadata table: {e}")
            return False