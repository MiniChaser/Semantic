"""
Base database schema definition and management
"""

import logging
from datetime import datetime
from typing import Dict, List
from ..connection import DatabaseManager
from .paper import PaperSchema
from .processing import ProcessingMetaSchema
from .scheduler import SchedulerSchema
from .enriched_paper import EnrichedPaperSchema


class DatabaseSchema:
    """Database schema definition and management"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()
        
        # Initialize schema modules
        self.paper_schema = PaperSchema(db_manager)
        self.processing_schema = ProcessingMetaSchema(db_manager)
        self.scheduler_schema = SchedulerSchema(db_manager)
        self.enriched_paper_schema = EnrichedPaperSchema(db_manager)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging for schema management"""
        logger = logging.getLogger(f'{__name__}.DatabaseSchema')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def create_all_tables(self) -> bool:
        """Create all tables with their indexes and triggers"""
        try:
            self.logger.info("Starting database schema creation...")
            
            # Create dblp_papers table
            if not self.paper_schema.create_table():
                raise Exception("Failed to create dblp_papers table")

            # Create enriched papers table (without foreign key constraint)
            if not self.enriched_paper_schema.create_table():
                raise Exception("Failed to create enriched_papers table")

            # Create processing metadata table
            if not self.processing_schema.create_table():
                raise Exception("Failed to create processing metadata table")

            # Create scheduler jobs table
            if not self.scheduler_schema.create_table():
                raise Exception("Failed to create scheduler jobs table")
            
            self.logger.info("Database schema creation completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create database schema: {e}")
            return False
    
    def drop_all_tables(self) -> bool:
        """Drop all tables (use with caution!)"""
        try:
            self.logger.warning("Dropping all tables...")
            
            tables = [
                'scheduler_jobs',
                's2_processing_meta',
                'enriched_papers',
                'dblp_processing_meta',
                'dblp_papers'
            ]
            
            for table in tables:
                drop_sql = f"DROP TABLE IF EXISTS {table} CASCADE;"
                if not self.db_manager.execute_query(drop_sql):
                    self.logger.warning(f"Failed to drop table: {table}")
            
            self.logger.info("All tables dropped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to drop tables: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict:
        """Get information about a specific table"""
        try:
            # Get table structure
            structure_query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position;
            """
            
            columns = self.db_manager.fetch_all(structure_query, (table_name,))
            
            # Get table size
            size_query = """
            SELECT 
                pg_size_pretty(pg_total_relation_size(%s)) as total_size,
                pg_size_pretty(pg_relation_size(%s)) as table_size
            """
            
            size_info = self.db_manager.fetch_one(size_query, (table_name, table_name))
            
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            count_info = self.db_manager.fetch_one(count_query)
            
            return {
                'columns': [dict(col) for col in columns] if columns else [],
                'total_size': size_info.get('total_size', 'Unknown') if size_info else 'Unknown',
                'table_size': size_info.get('table_size', 'Unknown') if size_info else 'Unknown',
                'row_count': count_info.get('row_count', 0) if count_info else 0
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {e}")
            return {}