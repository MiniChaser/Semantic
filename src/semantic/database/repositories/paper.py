"""
Paper data repository class
"""

import json
import logging
import time
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from functools import wraps
from tqdm import tqdm
from ..connection import DatabaseManager
from ..models.paper import DBLP_Paper

# Import signal only on Unix-like systems
if os.name != 'nt':  # Not Windows
    import signal
else:
    signal = None


class DBLPPaperRepository:
    """DBLP Paper data repository class"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.logger = self._setup_logger()
        self._operation_timeout = 3600  # 1 hour timeout for batch operations
        self._interrupted = False
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for repository operations"""
        logger = logging.getLogger(f'{__name__}.DBLPPaperRepository')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def _timeout_handler(self, signum, frame):
        """Handle timeout signal (Unix only)"""
        self.logger.warning("Operation timeout detected, attempting graceful shutdown...")
        self._interrupted = True

    def _setup_timeout(self, timeout_seconds: int) -> tuple:
        """Setup timeout handling based on platform"""
        if signal is None:  # Windows
            # For Windows, we'll use time-based checking instead of signals
            return None, time.time() + timeout_seconds
        else:  # Unix-like systems
            old_handler = signal.signal(signal.SIGALRM, self._timeout_handler)
            signal.alarm(timeout_seconds)
            return old_handler, None

    def _cleanup_timeout(self, old_handler, start_time_limit):
        """Clean up timeout handling"""
        if signal is not None and old_handler is not None:  # Unix
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    def _check_timeout(self, start_time_limit):
        """Check if timeout has occurred (for Windows)"""
        if start_time_limit is not None and time.time() > start_time_limit:
            self.logger.warning("Operation timeout detected (time-based), attempting graceful shutdown...")
            self._interrupted = True
            return True
        return False

    def _handle_keyboard_interrupt(self, processed: int, total: int, start_time: float) -> Tuple[int, int, int]:
        """Handle keyboard interrupt gracefully"""
        elapsed_time = time.time() - start_time
        self.logger.warning(
            f"Operation interrupted by user after {elapsed_time:.2f}s. "
            f"Processed {processed}/{total} papers ({processed/total*100:.1f}%)"
        )
        return processed, 0, total - processed
    
    def create_tables(self) -> bool:
        """Create database tables using schema module"""
        try:
            from ..schemas import DatabaseSchema
            schema = DatabaseSchema(self.db)
            return schema.create_all_tables()
            
        except Exception as e:
            self.logger.error(f"Failed to create tables: {e}")
            return False
    
    def insert_paper(self, paper: DBLP_Paper) -> bool:
        """Insert single paper"""
        try:
            sql = """
            INSERT INTO dblp_papers 
            (key, title, authors, author_count, venue, year, pages, ee, booktitle, doi, 
             create_time)
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
                update_time = CURRENT_TIMESTAMP
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
                paper.create_time or current_time
            )
            
            return self.db.execute_query(sql, params)
            
        except Exception as e:
            self.logger.error(f"Failed to insert paper: {e}")
            return False
    
    def batch_insert_papers(self, papers: List[DBLP_Paper], chunk_size: int = 1000, timeout: int = None) -> Tuple[int, int, int]:
        """Batch insert papers with optimized performance, progress monitoring, and robust error handling"""
        if not papers:
            return 0, 0, 0

        total_papers = len(papers)
        inserted = 0
        updated = 0
        errors = 0
        self._interrupted = False

        timeout = timeout or self._operation_timeout
        self.logger.info(
            f"Starting batch insert of {total_papers} papers in chunks of {chunk_size} "
            f"(timeout: {timeout}s)"
        )

        start_time = time.time()

        # Set up timeout handler (cross-platform)
        old_handler, timeout_limit = self._setup_timeout(timeout)

        try:
            # Process papers in chunks with progress monitoring
            with tqdm(total=total_papers, desc="Inserting papers", unit="papers") as pbar:
                for i in range(0, total_papers, chunk_size):
                    # Check for interruption (timeout or keyboard interrupt)
                    if self._interrupted or self._check_timeout(timeout_limit):
                        self.logger.warning("Operation interrupted, stopping processing")
                        processed = min(i, total_papers)
                        return self._handle_keyboard_interrupt(processed, total_papers, start_time)

                    chunk = papers[i:i + chunk_size]
                    chunk_start_time = time.time()

                    try:
                        chunk_inserted, chunk_updated, chunk_errors = self._process_chunk(chunk)

                        inserted += chunk_inserted
                        updated += chunk_updated
                        errors += chunk_errors

                        # Update progress bar
                        pbar.update(len(chunk))

                        # Calculate performance metrics
                        chunk_time = time.time() - chunk_start_time
                        papers_per_second = len(chunk) / chunk_time if chunk_time > 0 else 0

                        # Log progress every 5 chunks or at the end
                        if (i // chunk_size + 1) % 5 == 0 or i + chunk_size >= total_papers:
                            processed = min(i + chunk_size, total_papers)
                            progress_percent = (processed / total_papers) * 100
                            elapsed_time = time.time() - start_time
                            estimated_total_time = elapsed_time / progress_percent * 100 if progress_percent > 0 else 0
                            remaining_time = max(0, estimated_total_time - elapsed_time)

                            self.logger.info(
                                f"Progress: {processed}/{total_papers} ({progress_percent:.1f}%) - "
                                f"Speed: {papers_per_second:.1f} papers/sec - "
                                f"ETA: {remaining_time:.0f}s"
                            )

                    except KeyboardInterrupt:
                        processed = min(i + len(chunk), total_papers)
                        return self._handle_keyboard_interrupt(processed, total_papers, start_time)

                    except Exception as chunk_error:
                        self.logger.error(f"Chunk processing error at position {i}: {chunk_error}")
                        errors += len(chunk)
                        pbar.update(len(chunk))  # Still update progress

            total_time = time.time() - start_time
            average_speed = total_papers / total_time if total_time > 0 else 0

            self.logger.info(
                f"Batch operation completed in {total_time:.2f}s: "
                f"inserted {inserted}, updated {updated}, errors {errors} "
                f"(avg speed: {average_speed:.1f} papers/sec)"
            )

            return inserted, updated, errors

        except KeyboardInterrupt:
            return self._handle_keyboard_interrupt(inserted + updated, total_papers, start_time)

        except Exception as e:
            elapsed_time = time.time() - start_time
            processed = inserted + updated
            self.logger.error(
                f"Batch operation failed after {elapsed_time:.2f}s: {e}. "
                f"Processed {processed}/{total_papers} papers"
            )
            return inserted, updated, errors + (total_papers - processed)

        finally:
            # Clean up timeout handler (cross-platform)
            self._cleanup_timeout(old_handler, timeout_limit)
            self._interrupted = False

    def _process_chunk(self, papers: List[DBLP_Paper]) -> Tuple[int, int, int]:
        """Process a single chunk of papers using optimized bulk operations"""
        if not papers:
            return 0, 0, 0

        inserted = 0
        updated = 0
        errors = 0

        try:
            with self.db.get_cursor() as cursor:
                # Use PostgreSQL's UPSERT with ON CONFLICT for efficient bulk operations
                upsert_sql = """
                INSERT INTO dblp_papers
                (key, title, authors, author_count, venue, year, pages, ee, booktitle, doi, create_time)
                VALUES %s
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
                    update_time = CURRENT_TIMESTAMP
                """

                # Prepare data for bulk insert
                current_time = datetime.now().isoformat()
                values_list = []

                for paper in papers:
                    try:
                        values_list.append((
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
                            paper.create_time or current_time
                        ))
                    except Exception as e:
                        self.logger.debug(f"Failed to prepare paper {paper.key}: {e}")
                        errors += 1

                if values_list:
                    # Get existing keys to determine insert vs update counts
                    existing_keys = self._get_existing_keys([paper.key for paper in papers])

                    # Use psycopg2's execute_values for efficient bulk insert
                    from psycopg2.extras import execute_values

                    execute_values(
                        cursor,
                        upsert_sql,
                        values_list,
                        template=None,
                        page_size=1000
                    )

                    # Calculate insert vs update counts
                    for paper in papers:
                        if paper.key in existing_keys:
                            updated += 1
                        else:
                            inserted += 1

                return inserted, updated, errors

        except Exception as e:
            self.logger.error(f"Chunk processing failed: {e}")
            return 0, 0, len(papers)

    def _get_existing_keys(self, keys: List[str]) -> set:
        """Get set of existing paper keys from database"""
        if not keys:
            return set()

        try:
            with self.db.get_cursor() as cursor:
                # Use ANY for efficient bulk lookup
                cursor.execute("SELECT key FROM dblp_papers WHERE key = ANY(%s)", (keys,))
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.debug(f"Failed to get existing keys: {e}")
            return set()
    
    def get_paper_by_key(self, key: str) -> Optional[DBLP_Paper]:
        """Get paper by key"""
        try:
            sql = "SELECT * FROM dblp_papers WHERE key = %s"
            result = self.db.fetch_one(sql, (key,))
            return DBLP_Paper.from_dict(dict(result)) if result else None
        except Exception as e:
            self.logger.error(f"Failed to get paper: {e}")
            return None
    
    def get_papers_by_venue(self, venue: str, limit: int = None) -> List[DBLP_Paper]:
        """Get papers by venue"""
        try:
            sql = "SELECT * FROM dblp_papers WHERE venue = %s ORDER BY year DESC, key"
            if limit:
                sql += f" LIMIT {limit}"
            
            results = self.db.fetch_all(sql, (venue,))
            return [DBLP_Paper.from_dict(dict(row)) for row in results]
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