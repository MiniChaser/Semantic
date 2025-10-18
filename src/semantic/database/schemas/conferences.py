"""
Conferences table schema definition
Stores conference definitions and aliases for venue normalization
"""

import logging
from typing import List
from ..connection import DatabaseManager


class ConferencesSchema:
    """Conferences table schema definition"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.ConferencesSchema')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def get_conferences_table_sql(self) -> str:
        """Get SQL for creating conferences table"""
        return """
        CREATE TABLE IF NOT EXISTS conferences (
            id SERIAL PRIMARY KEY,
            conference_name VARCHAR(100) UNIQUE NOT NULL,
            full_name TEXT,
            category VARCHAR(50),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

    def get_aliases_table_sql(self) -> str:
        """Get SQL for creating conference_aliases table"""
        return """
        CREATE TABLE IF NOT EXISTS conference_aliases (
            id SERIAL PRIMARY KEY,
            conference_name VARCHAR(100) NOT NULL,
            alias VARCHAR(200) NOT NULL,
            priority INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (conference_name) REFERENCES conferences(conference_name) ON UPDATE CASCADE ON DELETE CASCADE
        );
        """

    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_conference_aliases_conference ON conference_aliases(conference_name);",
            "CREATE INDEX IF NOT EXISTS idx_conference_aliases_alias_lower ON conference_aliases(LOWER(alias));",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_conference_aliases_unique ON conference_aliases(conference_name, alias);",
        ]

    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers"""
        return [
            # Trigger function to update updated_at on row updates
            """
            CREATE OR REPLACE FUNCTION update_conferences_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Trigger for conferences table
            """
            DROP TRIGGER IF EXISTS trigger_conferences_updated_at ON conferences;
            CREATE TRIGGER trigger_conferences_updated_at
                BEFORE UPDATE ON conferences
                FOR EACH ROW
                EXECUTE FUNCTION update_conferences_updated_at();
            """
        ]

    def create_tables(self) -> bool:
        """Create conferences and conference_aliases tables with indexes and triggers"""
        try:
            self.logger.info("Creating conferences table...")
            if not self.db_manager.execute_query(self.get_conferences_table_sql()):
                raise Exception("Failed to create conferences table")

            self.logger.info("Creating conference_aliases table...")
            if not self.db_manager.execute_query(self.get_aliases_table_sql()):
                raise Exception("Failed to create conference_aliases table")

            # Create indexes
            self.logger.info("Creating indexes...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")

            # Create triggers
            self.logger.info("Creating triggers...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")

            self.logger.info("✓ Conferences tables created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create conferences tables: {e}")
            return False

    def drop_tables(self) -> bool:
        """Drop conferences tables (cascade)"""
        try:
            self.logger.info("Dropping conferences tables...")
            self.db_manager.execute_query("DROP TABLE IF EXISTS conference_aliases CASCADE;")
            self.db_manager.execute_query("DROP TABLE IF EXISTS conferences CASCADE;")
            self.logger.info("✓ Conferences tables dropped successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop conferences tables: {e}")
            return False

    def check_tables_exist(self) -> bool:
        """Check if conferences tables exist"""
        try:
            query = """
                SELECT COUNT(*) as count
                FROM information_schema.tables
                WHERE table_name IN ('conferences', 'conference_aliases')
            """
            result = self.db_manager.fetch_one(query)
            return result and result['count'] == 2
        except Exception as e:
            self.logger.warning(f"Could not check tables: {e}")
            return False
