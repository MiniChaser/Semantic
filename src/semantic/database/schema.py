"""
Database schema definition and management
Defines all database tables and their structures
"""

import logging
from datetime import datetime
from typing import Dict, List
from .connection import DatabaseManager


class DatabaseSchema:
    """Database schema definition and management"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()
    
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
    
    def get_dblp_papers_table_sql(self) -> str:
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
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP,  -- Legacy field for backward compatibility
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Legacy field for backward compatibility
        );
        """
    
    def get_dblp_papers_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on dblp_papers table"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_venue ON dblp_papers(venue);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_year ON dblp_papers(year);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_doi ON dblp_papers(doi);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_authors ON dblp_papers USING GIN (authors);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_key ON dblp_papers(key);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_create_time ON dblp_papers(create_time);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_update_time ON dblp_papers(update_time);",
            # Legacy indexes for backward compatibility
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_created_at ON dblp_papers(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_papers_updated_at ON dblp_papers(updated_at);",
        ]
    
    def get_dblp_papers_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers on dblp_papers table"""
        return [
            # Trigger function to update update_time on row updates
            """
            CREATE OR REPLACE FUNCTION update_dblp_papers_update_time()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.update_time = CURRENT_TIMESTAMP;
                NEW.updated_at = CURRENT_TIMESTAMP;  -- Legacy field
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
    
    def get_processing_meta_table_sql(self) -> str:
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
    
    def get_processing_meta_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on processing metadata table"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_process_type ON dblp_processing_meta(process_type);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_last_run ON dblp_processing_meta(last_run_time);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_status ON dblp_processing_meta(status);",
            "CREATE INDEX IF NOT EXISTS idx_dblp_meta_create_time ON dblp_processing_meta(create_time);",
        ]
    
    def get_scheduler_jobs_table_sql(self) -> str:
        """Get SQL for creating scheduler jobs table (APScheduler)"""
        return """
        CREATE TABLE IF NOT EXISTS scheduler_jobs (
            id VARCHAR(191) PRIMARY KEY,
            next_run_time DOUBLE PRECISION,
            job_state BYTEA NOT NULL
        );
        """
    
    def get_scheduler_jobs_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes on scheduler jobs table"""
        return [
            "CREATE INDEX IF NOT EXISTS ix_scheduler_jobs_next_run_time ON scheduler_jobs(next_run_time);",
        ]
    
    def create_all_tables(self) -> bool:
        """Create all tables with their indexes and triggers"""
        try:
            self.logger.info("Starting database schema creation...")
            
            # Create dblp_papers table
            self.logger.info("Creating dblp_papers table...")
            if not self.db_manager.execute_query(self.get_dblp_papers_table_sql()):
                raise Exception("Failed to create dblp_papers table")
            
            # Create indexes for dblp_papers
            self.logger.info("Creating indexes for dblp_papers table...")
            for index_sql in self.get_dblp_papers_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")
            
            # Create triggers for dblp_papers
            self.logger.info("Creating triggers for dblp_papers table...")
            for trigger_sql in self.get_dblp_papers_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")
            
            # Create processing metadata table
            self.logger.info("Creating processing metadata table...")
            if not self.db_manager.execute_query(self.get_processing_meta_table_sql()):
                raise Exception("Failed to create processing metadata table")
            
            # Create indexes for processing metadata
            self.logger.info("Creating indexes for processing metadata table...")
            for index_sql in self.get_processing_meta_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")
            
            # Create scheduler jobs table
            self.logger.info("Creating scheduler jobs table...")
            if not self.db_manager.execute_query(self.get_scheduler_jobs_table_sql()):
                raise Exception("Failed to create scheduler jobs table")
            
            # Create indexes for scheduler jobs
            self.logger.info("Creating indexes for scheduler jobs table...")
            for index_sql in self.get_scheduler_jobs_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")
            
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
    
    def migrate_legacy_timestamps(self) -> bool:
        """Migrate data from legacy timestamp fields to new ones"""
        try:
            self.logger.info("Starting timestamp migration...")
            
            # Update create_time from created_at where create_time is null
            update_create_time = """
            UPDATE dblp_papers 
            SET create_time = created_at::timestamp
            WHERE create_time IS NULL AND created_at IS NOT NULL;
            """
            
            # Update update_time from updated_at where update_time is null
            update_update_time = """
            UPDATE dblp_papers 
            SET update_time = updated_at::timestamp
            WHERE update_time IS NULL AND updated_at IS NOT NULL;
            """
            
            self.db_manager.execute_query(update_create_time)
            self.db_manager.execute_query(update_update_time)
            
            self.logger.info("Timestamp migration completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to migrate timestamps: {e}")
            return False


def main():
    """Main function for standalone schema management"""
    import argparse
    from .connection import get_db_manager
    
    parser = argparse.ArgumentParser(description='Database schema management')
    parser.add_argument('--create', action='store_true', help='Create all tables')
    parser.add_argument('--drop', action='store_true', help='Drop all tables')
    parser.add_argument('--info', type=str, help='Get table information')
    parser.add_argument('--migrate', action='store_true', help='Migrate legacy timestamps')
    
    args = parser.parse_args()
    
    # Get database manager
    db_manager = get_db_manager()
    
    try:
        if not db_manager.connect():
            print("Failed to connect to database")
            return
        
        schema = DatabaseSchema(db_manager)
        
        if args.create:
            if schema.create_all_tables():
                print("✅ Database schema created successfully")
            else:
                print("❌ Failed to create database schema")
        
        elif args.drop:
            confirm = input("Are you sure you want to drop all tables? (yes/no): ")
            if confirm.lower() == 'yes':
                if schema.drop_all_tables():
                    print("✅ All tables dropped successfully")
                else:
                    print("❌ Failed to drop tables")
            else:
                print("Operation cancelled")
        
        elif args.info:
            info = schema.get_table_info(args.info)
            if info:
                print(f"\n📊 Table Information: {args.info}")
                print(f"Rows: {info['row_count']}")
                print(f"Total Size: {info['total_size']}")
                print(f"Table Size: {info['table_size']}")
                print("\nColumns:")
                for col in info['columns']:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                    print(f"  {col['column_name']}: {col['data_type']} {nullable}{default}")
            else:
                print(f"❌ Failed to get information for table: {args.info}")
        
        elif args.migrate:
            if schema.migrate_legacy_timestamps():
                print("✅ Timestamp migration completed")
            else:
                print("❌ Timestamp migration failed")
        
        else:
            print("Please specify an action: --create, --drop, --info <table>, or --migrate")
    
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    main()