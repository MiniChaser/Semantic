"""
Paper table schema definition
"""

import logging
from typing import List
from ..connection import DatabaseManager


class PaperSchema:
    """Paper table schema definition"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.PaperSchema')
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
        """Get SQL for creating dblp_papers table"""
        return """
        CREATE TABLE IF NOT EXISTS dblp_papers (
            id SERIAL PRIMARY KEY,
            key VARCHAR(255) UNIQUE NOT NULL,
            title TEXT NOT NULL,
            authors JSONB NOT NULL,
            author_count INTEGER,
            venue VARCHAR(50),
            year VARCHAR(4),
            pages VARCHAR(50),
            ee TEXT,
            booktitle TEXT,
            doi VARCHAR(100),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    
    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on dblp_papers table"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_venue ON dblp_papers(venue);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_year ON dblp_papers(year);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_doi ON dblp_papers(doi);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_authors ON dblp_papers USING GIN (authors);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_key ON dblp_papers(key);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_create_time ON dblp_papers(create_time);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_update_time ON dblp_papers(update_time);",
        ]
    
    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers on dblp_papers table"""
        return [
            # Trigger function to update update_time on row updates
            """
            CREATE OR REPLACE FUNCTION update_dblp_papers_update_time()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.update_time = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Trigger to automatically update update_time column
            """
            CREATE TRIGGER trigger_dblp_papers_update_time
                BEFORE UPDATE ON dblp_papers
                FOR EACH ROW
                EXECUTE FUNCTION update_dblp_papers_update_time();
            """
        ]
    
    def create_table(self) -> bool:
        """Create dblp_papers table with indexes and triggers"""
        try:
            self.logger.info("Creating dblp_papers table...")
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create dblp_papers table")
            
            # Create indexes for dblp_papers
            self.logger.info("Creating indexes for dblp_papers table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")
            
            # Create triggers for dblp_papers
            self.logger.info("Creating triggers for dblp_papers table...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")
            
            self.logger.info("dblp_papers table created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create dblp_papers table: {e}")
            return False