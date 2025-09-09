"""
Database models and table structure definitions
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from .connection import DatabaseManager


@dataclass
class Paper:
    """Paper data model"""
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
    create_time: Optional[str] = None
    update_time: Optional[str] = None
    created_at: Optional[str] = None  # Legacy field for backward compatibility
    updated_at: Optional[str] = None  # Legacy field for backward compatibility
    id: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Paper':
        """Create Paper object from dictionary"""
        # Handle authors field (could be JSON string or list)
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
            create_time=data.get('create_time'),
            update_time=data.get('update_time'),
            created_at=data.get('created_at'),  # Legacy field
            updated_at=data.get('updated_at')   # Legacy field
        )


class PaperRepository:
    """Paper data repository class"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for repository operations"""
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
        """Create database tables using schema module"""
        try:
            from .schema import DatabaseSchema
            schema = DatabaseSchema(self.db)
            return schema.create_all_tables()
            
        except Exception as e:
            self.logger.error(f"Failed to create tables: {e}")
            return False
    
    def insert_paper(self, paper: Paper) -> bool:
        """Insert single paper"""
        try:
            sql = """
            INSERT INTO dblp_papers 
            (key, title, authors, author_count, venue, year, pages, ee, booktitle, doi, 
             create_time, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                update_time = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            """
            
            current_time = datetime.now().isoformat()
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
                paper.create_time or current_time,
                paper.created_at or current_time  # Legacy field
            )
            
            return self.db.execute_query(sql, params)
            
        except Exception as e:
            self.logger.error(f"Failed to insert paper: {e}")
            return False
    
    def batch_insert_papers(self, papers: List[Paper]) -> Tuple[int, int, int]:
        """Batch insert papers"""
        if not papers:
            return 0, 0, 0
        
        inserted = 0
        updated = 0
        errors = 0
        
        try:
            with self.db.get_cursor() as cursor:
                for paper in papers:
                    try:
                        # Check if paper already exists
                        check_sql = "SELECT id, update_time FROM dblp_papers WHERE key = %s"
                        cursor.execute(check_sql, (paper.key,))
                        existing = cursor.fetchone()
                        
                        current_time = datetime.now().isoformat()
                        
                        if existing:
                            # Update existing paper
                            update_sql = """
                            UPDATE dblp_papers SET
                                title = %s, authors = %s, author_count = %s,
                                venue = %s, year = %s, pages = %s, ee = %s,
                                booktitle = %s, doi = %s, 
                                update_time = CURRENT_TIMESTAMP,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE key = %s
                            """
                            cursor.execute(update_sql, (
                                paper.title, json.dumps(paper.authors), paper.author_count,
                                paper.venue, paper.year, paper.pages, paper.ee,
                                paper.booktitle, paper.doi, paper.key
                            ))
                            updated += 1
                        else:
                            # Insert new paper
                            insert_sql = """
                            INSERT INTO dblp_papers 
                            (key, title, authors, author_count, venue, year, pages, ee, 
                             booktitle, doi, create_time, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_sql, (
                                paper.key, paper.title, json.dumps(paper.authors),
                                paper.author_count, paper.venue, paper.year,
                                paper.pages, paper.ee, paper.booktitle, paper.doi,
                                paper.create_time or current_time,
                                paper.created_at or current_time  # Legacy field
                            ))
                            inserted += 1
                            
                    except Exception as e:
                        self.logger.debug(f"Failed to process paper {paper.key}: {e}")
                        errors += 1
            
            self.logger.info(f"Batch operation completed: inserted {inserted}, updated {updated}, errors {errors}")
            return inserted, updated, errors
            
        except Exception as e:
            self.logger.error(f"Batch operation failed: {e}")
            return 0, 0, len(papers)
    
    def get_paper_by_key(self, key: str) -> Optional[Paper]:
        """Get paper by key"""
        try:
            sql = "SELECT * FROM dblp_papers WHERE key = %s"
            result = self.db.fetch_one(sql, (key,))
            return Paper.from_dict(dict(result)) if result else None
        except Exception as e:
            self.logger.error(f"Failed to get paper: {e}")
            return None
    
    def get_papers_by_venue(self, venue: str, limit: int = None) -> List[Paper]:
        """Get papers by venue"""
        try:
            sql = "SELECT * FROM dblp_papers WHERE venue = %s ORDER BY year DESC, key"
            if limit:
                sql += f" LIMIT {limit}"
            
            results = self.db.fetch_all(sql, (venue,))
            return [Paper.from_dict(dict(row)) for row in results]
        except Exception as e:
            self.logger.error(f"Failed to get papers by venue: {e}")
            return []
    
    def get_last_update_time(self) -> Optional[datetime]:
        """Get last update time"""
        try:
            sql = """
            SELECT MAX(update_time) as last_update 
            FROM dblp_papers 
            WHERE update_time IS NOT NULL
            """
            result = self.db.fetch_one(sql)
            if result and result['last_update']:
                return result['last_update']
            return None
        except Exception as e:
            self.logger.error(f"Failed to get last update time: {e}")
            return None
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        try:
            stats = {}
            
            # Total papers count
            total_result = self.db.fetch_one("SELECT COUNT(*) as total FROM dblp_papers")
            stats['total_papers'] = total_result['total'] if total_result else 0
            
            # Statistics by venue
            venue_results = self.db.fetch_all("""
                SELECT venue, COUNT(*) as count 
                FROM dblp_papers 
                GROUP BY venue 
                ORDER BY count DESC
            """)
            stats['by_venue'] = {row['venue']: row['count'] for row in venue_results}
            
            # Statistics by year
            year_results = self.db.fetch_all("""
                SELECT year, COUNT(*) as count 
                FROM dblp_papers 
                WHERE year IS NOT NULL 
                GROUP BY year 
                ORDER BY year DESC 
                LIMIT 10
            """)
            stats['by_year'] = {row['year']: row['count'] for row in year_results}
            
            # Last update time
            stats['last_update'] = self.get_last_update_time()
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get statistics: {e}")
            return {}
    
    def record_processing_meta(self, process_type: str, status: str, 
                             records_processed: int = 0, records_inserted: int = 0,
                             records_updated: int = 0, error_message: str = None,
                             execution_duration: int = None) -> bool:
        """Record processing metadata"""
        try:
            sql = """
            INSERT INTO dblp_processing_meta 
            (process_type, last_run_time, status, records_processed, 
             records_inserted, records_updated, error_message, execution_duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                process_type,
                datetime.now(),
                status,
                records_processed,
                records_inserted,
                records_updated,
                error_message,
                execution_duration
            )
            
            return self.db.execute_query(sql, params)
            
        except Exception as e:
            self.logger.error(f"Failed to record processing metadata: {e}")
            return False
    
    def get_last_successful_run(self, process_type: str) -> Optional[datetime]:
        """Get last successful run time"""
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
            self.logger.error(f"Failed to get last successful run time: {e}")
            return None