"""
数据库模型和表结构定义
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from .connection import DatabaseManager


@dataclass
class Paper:
    """论文数据模型"""
    key: str
    title: str
    authors: List[str]
    author_count: int
    venue: str
    year: Optional[str] = None
    pages: Optional[str] = None
    ee: Optional[str] = None
    booktitle: Optional[str] = None
    doi: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    id: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Paper':
        """从字典创建Paper对象"""
        # 处理authors字段（可能是JSON字符串或列表）
        authors = data.get('authors', [])
        if isinstance(authors, str):
            try:
                authors = json.loads(authors)
            except json.JSONDecodeError:
                authors = authors.split('|') if authors else []
        
        return cls(
            id=data.get('id'),
            key=data.get('key', ''),
            title=data.get('title', ''),
            authors=authors,
            author_count=data.get('author_count', len(authors)),
            venue=data.get('venue', ''),
            year=data.get('year'),
            pages=data.get('pages'),
            ee=data.get('ee'),
            booktitle=data.get('booktitle'),
            doi=data.get('doi'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )


class PaperRepository:
    """论文数据仓库类"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger(f'{__name__}.PaperRepository')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def create_tables(self) -> bool:
        """创建数据库表"""
        try:
            create_table_sql = """
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
                created_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 创建索引
            CREATE INDEX IF NOT EXISTS idx_dblp_papers_venue ON dblp_papers(venue);
            CREATE INDEX IF NOT EXISTS idx_dblp_papers_year ON dblp_papers(year);
            CREATE INDEX IF NOT EXISTS idx_dblp_papers_doi ON dblp_papers(doi);
            CREATE INDEX IF NOT EXISTS idx_dblp_papers_authors ON dblp_papers USING GIN (authors);
            CREATE INDEX IF NOT EXISTS idx_dblp_papers_key ON dblp_papers(key);
            CREATE INDEX IF NOT EXISTS idx_dblp_papers_updated_at ON dblp_papers(updated_at);
            
            -- 创建增量处理元数据表
            CREATE TABLE IF NOT EXISTS dblp_processing_meta (
                id SERIAL PRIMARY KEY,
                process_type VARCHAR(50) NOT NULL,
                last_run_time TIMESTAMP NOT NULL,
                status VARCHAR(20) NOT NULL,
                records_processed INTEGER DEFAULT 0,
                records_inserted INTEGER DEFAULT 0,
                records_updated INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_dblp_meta_process_type ON dblp_processing_meta(process_type);
            CREATE INDEX IF NOT EXISTS idx_dblp_meta_last_run ON dblp_processing_meta(last_run_time);
            """
            
            return self.db.execute_query(create_table_sql)
            
        except Exception as e:
            self.logger.error(f"创建表失败: {e}")
            return False
    
    def insert_paper(self, paper: Paper) -> bool:
        """插入单篇论文"""
        try:
            sql = """
            INSERT INTO dblp_papers 
            (key, title, authors, author_count, venue, year, pages, ee, booktitle, doi, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET
                title = EXCLUDED.title,
                authors = EXCLUDED.authors,
                author_count = EXCLUDED.author_count,
                venue = EXCLUDED.venue,
                year = EXCLUDED.year,
                pages = EXCLUDED.pages,
                ee = EXCLUDED.ee,
                booktitle = EXCLUDED.booktitle,
                doi = EXCLUDED.doi,
                updated_at = CURRENT_TIMESTAMP
            """
            
            params = (
                paper.key,
                paper.title,
                json.dumps(paper.authors),
                paper.author_count,
                paper.venue,
                paper.year,
                paper.pages,
                paper.ee,
                paper.booktitle,
                paper.doi,
                paper.created_at or datetime.now().isoformat()
            )
            
            return self.db.execute_query(sql, params)
            
        except Exception as e:
            self.logger.error(f"插入论文失败: {e}")
            return False
    
    def batch_insert_papers(self, papers: List[Paper]) -> Tuple[int, int, int]:
        """批量插入论文"""
        if not papers:
            return 0, 0, 0
        
        inserted = 0
        updated = 0
        errors = 0
        
        try:
            with self.db.get_cursor() as cursor:
                for paper in papers:
                    try:
                        # 检查论文是否已存在
                        check_sql = "SELECT id, updated_at FROM dblp_papers WHERE key = %s"
                        cursor.execute(check_sql, (paper.key,))
                        existing = cursor.fetchone()
                        
                        if existing:
                            # 更新已存在的论文
                            update_sql = """
                            UPDATE dblp_papers SET
                                title = %s, authors = %s, author_count = %s,
                                venue = %s, year = %s, pages = %s, ee = %s,
                                booktitle = %s, doi = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE key = %s
                            """
                            cursor.execute(update_sql, (
                                paper.title, json.dumps(paper.authors), paper.author_count,
                                paper.venue, paper.year, paper.pages, paper.ee,
                                paper.booktitle, paper.doi, paper.key
                            ))
                            updated += 1
                        else:
                            # 插入新论文
                            insert_sql = """
                            INSERT INTO dblp_papers 
                            (key, title, authors, author_count, venue, year, pages, ee, booktitle, doi, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_sql, (
                                paper.key, paper.title, json.dumps(paper.authors),
                                paper.author_count, paper.venue, paper.year,
                                paper.pages, paper.ee, paper.booktitle, paper.doi,
                                paper.created_at or datetime.now().isoformat()
                            ))
                            inserted += 1
                            
                    except Exception as e:
                        self.logger.debug(f"处理论文失败 {paper.key}: {e}")
                        errors += 1
            
            self.logger.info(f"批量操作完成: 新增 {inserted}, 更新 {updated}, 错误 {errors}")
            return inserted, updated, errors
            
        except Exception as e:
            self.logger.error(f"批量操作失败: {e}")
            return 0, 0, len(papers)
    
    def get_paper_by_key(self, key: str) -> Optional[Paper]:
        """根据key获取论文"""
        try:
            sql = "SELECT * FROM dblp_papers WHERE key = %s"
            result = self.db.fetch_one(sql, (key,))
            return Paper.from_dict(dict(result)) if result else None
        except Exception as e:
            self.logger.error(f"获取论文失败: {e}")
            return None
    
    def get_papers_by_venue(self, venue: str, limit: int = None) -> List[Paper]:
        """根据会议获取论文列表"""
        try:
            sql = "SELECT * FROM dblp_papers WHERE venue = %s ORDER BY year DESC, key"
            if limit:
                sql += f" LIMIT {limit}"
            
            results = self.db.fetch_all(sql, (venue,))
            return [Paper.from_dict(dict(row)) for row in results]
        except Exception as e:
            self.logger.error(f"获取论文列表失败: {e}")
            return []
    
    def get_last_update_time(self) -> Optional[datetime]:
        """获取最后更新时间"""
        try:
            sql = """
            SELECT MAX(updated_at) as last_update 
            FROM dblp_papers 
            WHERE updated_at IS NOT NULL
            """
            result = self.db.fetch_one(sql)
            if result and result['last_update']:
                return result['last_update']
            return None
        except Exception as e:
            self.logger.error(f"获取最后更新时间失败: {e}")
            return None
    
    def get_statistics(self) -> Dict:
        """获取数据库统计信息"""
        try:
            stats = {}
            
            # 总论文数
            total_result = self.db.fetch_one("SELECT COUNT(*) as total FROM dblp_papers")
            stats['total_papers'] = total_result['total'] if total_result else 0
            
            # 按会议统计
            venue_results = self.db.fetch_all("""
                SELECT venue, COUNT(*) as count 
                FROM dblp_papers 
                GROUP BY venue 
                ORDER BY count DESC
            """)
            stats['by_venue'] = {row['venue']: row['count'] for row in venue_results}
            
            # 按年份统计
            year_results = self.db.fetch_all("""
                SELECT year, COUNT(*) as count 
                FROM dblp_papers 
                WHERE year IS NOT NULL 
                GROUP BY year 
                ORDER BY year DESC 
                LIMIT 10
            """)
            stats['by_year'] = {row['year']: row['count'] for row in year_results}
            
            # 最后更新时间
            stats['last_update'] = self.get_last_update_time()
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取统计信息失败: {e}")
            return {}
    
    def record_processing_meta(self, process_type: str, status: str, 
                             records_processed: int = 0, records_inserted: int = 0,
                             records_updated: int = 0, error_message: str = None) -> bool:
        """记录处理元数据"""
        try:
            sql = """
            INSERT INTO dblp_processing_meta 
            (process_type, last_run_time, status, records_processed, 
             records_inserted, records_updated, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                process_type,
                datetime.now(),
                status,
                records_processed,
                records_inserted,
                records_updated,
                error_message
            )
            
            return self.db.execute_query(sql, params)
            
        except Exception as e:
            self.logger.error(f"记录处理元数据失败: {e}")
            return False
    
    def get_last_successful_run(self, process_type: str) -> Optional[datetime]:
        """获取上次成功运行时间"""
        try:
            sql = """
            SELECT last_run_time 
            FROM dblp_processing_meta 
            WHERE process_type = %s AND status = 'success'
            ORDER BY last_run_time DESC 
            LIMIT 1
            """
            
            result = self.db.fetch_one(sql, (process_type,))
            return result['last_run_time'] if result else None
            
        except Exception as e:
            self.logger.error(f"获取上次成功运行时间失败: {e}")
            return None