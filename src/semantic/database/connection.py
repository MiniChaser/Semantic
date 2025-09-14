"""
Database connection management module
Provides unified database connection and configuration management
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv


class DatabaseConfig:
    """Database configuration class"""
    
    def __init__(self):
        load_dotenv()
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = int(os.getenv('DB_PORT', '5432'))
        self.database = os.getenv('DB_NAME', 'dblp_semantic')
        self.username = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '10'))
        self.max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '20'))
    
    def get_connection_string(self) -> str:
        """Get database connection string"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get database connection parameters"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.username,
            'password': self.password,
            'cursor_factory': RealDictCursor
        }


class DatabaseManager:
    """Database manager class"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.logger = self._setup_logger()
        self._connection = None
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for database operations"""
        logger = logging.getLogger(f'{__name__}.DatabaseManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            if self._connection and not self._connection.closed:
                return True
            
            self._connection = psycopg2.connect(**self.config.get_connection_params())
            self.logger.info(f"Database connection established: {self.config.database}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self.logger.info("Database connection closed")
    
    def get_connection(self):
        """Get database connection"""
        if not self._connection or self._connection.closed:
            if not self.connect():
                raise Exception("Unable to establish database connection")
        return self._connection
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor"""
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            yield cursor
            connection.commit()
        except Exception as e:
            connection.rollback()
            self.logger.error(f"Database operation failed: {e}")
            raise
        finally:
            cursor.close()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params = None) -> bool:
        """Execute SQL query"""
        try:
            with self.get_cursor() as cursor:
                # Convert dict/list parameters to Json objects for JSONB compatibility
                processed_params = self._process_json_params(params) if params else None
                cursor.execute(query, processed_params)
                return True
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return False
    
    def fetch_one(self, query: str, params = None) -> Optional[Dict]:
        """Execute query and return single record"""
        try:
            with self.get_cursor() as cursor:
                processed_params = self._process_json_params(params) if params else None
                cursor.execute(query, processed_params)
                return cursor.fetchone()
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return None
    
    def fetch_all(self, query: str, params = None) -> list:
        """Execute query and return all records"""
        try:
            with self.get_cursor() as cursor:
                processed_params = self._process_json_params(params) if params else None
                cursor.execute(query, processed_params)
                return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return []
    
    def execute_batch_query(self, query: str, params_list: List = None) -> bool:
        """Execute batch SQL query with multiple parameter sets"""
        try:
            with self.get_cursor() as cursor:
                if params_list:
                    # Process each parameter set for JSON compatibility
                    processed_params = [
                        self._process_json_params(params) for params in params_list
                    ]
                    cursor.executemany(query, processed_params)
                else:
                    cursor.execute(query)
                return True
        except Exception as e:
            self.logger.error(f"Batch query execution failed: {e}")
            return False
    
    def _process_json_params(self, params):
        """Process parameters to handle JSON objects for JSONB compatibility"""
        if not params:
            return params
            
        processed = []
        # Handle both lists and tuples
        param_list = params if isinstance(params, (list, tuple)) else [params]
        
        for param in param_list:
            if isinstance(param, (dict, list)):
                processed.append(Json(param))
            else:
                processed.append(param)
        
        # Return same type as input
        return tuple(processed) if isinstance(params, tuple) else processed
    
    def __enter__(self):
        """Support for with statement"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for with statement"""
        self.disconnect()


# Global database manager instance
_db_manager = None


def get_db_manager() -> DatabaseManager:
    """Get global database manager instance"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def reset_db_manager():
    """Reset global database manager instance"""
    global _db_manager
    if _db_manager:
        _db_manager.disconnect()
    _db_manager = None