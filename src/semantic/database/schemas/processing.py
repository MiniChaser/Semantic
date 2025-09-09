"""
Processing metadata table schema definition
"""

import logging
from typing import List
from ..connection import DatabaseManager


class ProcessingMetaSchema:
    """Processing metadata table schema definition"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.ProcessingMetaSchema')
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
        """Get SQL for creating processing metadata table"""
        return """
        CREATE TABLE IF NOT EXISTS dblp_processing_meta (
            id SERIAL PRIMARY KEY,
            process_type VARCHAR(50) NOT NULL,
            last_run_time TIMESTAMP NOT NULL,
            status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed', 'partial_success', 'running')),
            records_processed INTEGER DEFAULT 0,
            records_inserted INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            error_message TEXT,
            execution_duration INTEGER,  -- Duration in seconds
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    
    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on processing metadata table"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_process_type ON dblp_processing_meta(process_type);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_last_run ON dblp_processing_meta(last_run_time);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_status ON dblp_processing_meta(status);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_create_time ON dblp_processing_meta(create_time);",
        ]
    
    def create_table(self) -> bool:
        """Create processing metadata table with indexes"""
        try:
            self.logger.info("Creating processing metadata table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create processing metadata table")
            
            # Create indexes for processing metadata
            self.logger.info("Creating indexes for processing metadata table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")
            
            self.logger.info("Processing metadata table created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create processing metadata table: {e}")
            return False