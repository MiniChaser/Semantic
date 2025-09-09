"""
数据库连接管理模块
提供统一的数据库连接和配置管理
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, Dict, Any
from dotenv import load_dotenv


class DatabaseConfig:
    """数据库配置类"""
    
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
        """获取数据库连接字符串"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """获取数据库连接参数"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.username,
            'password': self.password,
            'cursor_factory': RealDictCursor
        }


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.logger = self._setup_logger()
        self._connection = None
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
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
        """建立数据库连接"""
        try:
            if self._connection and not self._connection.closed:
                return True
            
            self._connection = psycopg2.connect(**self.config.get_connection_params())
            self.logger.info(f"数据库连接成功: {self.config.database}")
            return True
            
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            return False
    
    def disconnect(self):
        """关闭数据库连接"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self.logger.info("数据库连接已关闭")
    
    def get_connection(self):
        """获取数据库连接"""
        if not self._connection or self._connection.closed:
            if not self.connect():
                raise Exception("无法建立数据库连接")
        return self._connection
    
    @contextmanager
    def get_cursor(self):
        """获取数据库游标的上下文管理器"""
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            yield cursor
            connection.commit()
        except Exception as e:
            connection.rollback()
            self.logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            cursor.close()
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            self.logger.error(f"连接测试失败: {e}")
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> bool:
        """执行SQL查询"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return True
        except Exception as e:
            self.logger.error(f"查询执行失败: {e}")
            return False
    
    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """执行查询并返回单条记录"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchone()
        except Exception as e:
            self.logger.error(f"查询执行失败: {e}")
            return None
    
    def fetch_all(self, query: str, params: tuple = None) -> list:
        """执行查询并返回所有记录"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"查询执行失败: {e}")
            return []
    
    def __enter__(self):
        """支持with语句"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持with语句"""
        self.disconnect()


# 全局数据库管理器实例
_db_manager = None


def get_db_manager() -> DatabaseManager:
    """获取全局数据库管理器实例"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def reset_db_manager():
    """重置全局数据库管理器实例"""
    global _db_manager
    if _db_manager:
        _db_manager.disconnect()
    _db_manager = None