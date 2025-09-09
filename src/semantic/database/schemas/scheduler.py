"""
Scheduler jobs table schema definition
"""

import logging
from typing import List
from ..connection import DatabaseManager


class SchedulerSchema:
    """Scheduler jobs table schema definition"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.SchedulerSchema')
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
        """Get SQL for creating scheduler jobs table (APScheduler)"""
        return """
        CREATE TABLE IF NOT EXISTS scheduler_jobs (
            id VARCHAR(191) PRIMARY KEY,
            next_run_time DOUBLE PRECISION,
            job_state BYTEA NOT NULL
        );
        """
    
    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on scheduler jobs table"""
        return [
            "CREATE INDEX IF NOT EXISTS ix_scheduler_jobs_next_run_time ON scheduler_jobs(next_run_time);",
        ]
    
    def create_table(self) -> bool:
        """Create scheduler jobs table with indexes"""
        try:
            self.logger.info("Creating scheduler jobs table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create scheduler jobs table")
            
            # Create indexes for scheduler jobs
            self.logger.info("Creating indexes for scheduler jobs table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")
            
            self.logger.info("Scheduler jobs table created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create scheduler jobs table: {e}")
            return False