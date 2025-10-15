"""
Dataset Paper data repository class
"""

import logging
from typing import List, Optional, Dict
from ..connection import DatabaseManager
from ..models.dataset_paper import DatasetPaper


class DatasetPaperRepository:
    """Dataset Paper data repository class"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger for repository operations"""
        logger = logging.getLogger(f'{__name__}.DatasetPaperRepository')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def get_paper_by_corpus_id(self, corpus_id: int) -> Optional[DatasetPaper]:
        """Get paper by corpus_id"""
        try:
            sql = "SELECT * FROM dataset_papers WHERE corpus_id = %s"
            result = self.db.fetch_one(sql, (corpus_id,))
            return DatasetPaper.from_dict(dict(result)) if result else None
        except Exception as e:
            self.logger.error(f"Failed to get paper: {e}")
            return None

    def get_papers_by_conference(self, conference: str, limit: Optional[int] = None) -> List[DatasetPaper]:
        """Get papers by conference"""
        try:
            sql = """
            SELECT * FROM dataset_papers
            WHERE conference_normalized = %s
            ORDER BY year DESC, citation_count DESC
            """
            if limit:
                sql += f" LIMIT {limit}"

            results = self.db.fetch_all(sql, (conference,))
            return [DatasetPaper.from_dict(dict(row)) for row in results]
        except Exception as e:
            self.logger.error(f"Failed to get papers by conference: {e}")
            return []

    def get_papers_by_release(self, release_id: str, limit: Optional[int] = None) -> List[DatasetPaper]:
        """Get papers by release_id"""
        try:
            sql = """
            SELECT * FROM dataset_papers
            WHERE release_id = %s
            ORDER BY citation_count DESC
            """
            if limit:
                sql += f" LIMIT {limit}"

            results = self.db.fetch_all(sql, (release_id,))
            return [DatasetPaper.from_dict(dict(row)) for row in results]
        except Exception as e:
            self.logger.error(f"Failed to get papers by release: {e}")
            return []

    def get_existing_corpus_ids(self, corpus_ids: List[int]) -> set:
        """Get set of existing corpus_ids from database"""
        if not corpus_ids:
            return set()

        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    "SELECT corpus_id FROM dataset_papers WHERE corpus_id = ANY(%s)",
                    (corpus_ids,)
                )
                return {row['corpus_id'] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.error(f"Failed to get existing corpus_ids: {e}")
            return set()

    def get_statistics(self) -> Dict:
        """Get database statistics"""
        try:
            stats = {}

            # Total papers count
            total_result = self.db.fetch_one("SELECT COUNT(*) as total FROM dataset_papers")
            stats['total_papers'] = total_result['total'] if total_result else 0

            # Statistics by conference
            conference_results = self.db.fetch_all("""
                SELECT conference_normalized, COUNT(*) as count
                FROM dataset_papers
                GROUP BY conference_normalized
                ORDER BY count DESC
            """)
            stats['by_conference'] = {row['conference_normalized']: row['count'] for row in conference_results}

            # Statistics by year
            year_results = self.db.fetch_all("""
                SELECT year, COUNT(*) as count
                FROM dataset_papers
                WHERE year IS NOT NULL
                GROUP BY year
                ORDER BY year DESC
                LIMIT 20
            """)
            stats['by_year'] = {row['year']: row['count'] for row in year_results}

            # Statistics by release
            release_results = self.db.fetch_all("""
                SELECT release_id, COUNT(*) as count
                FROM dataset_papers
                GROUP BY release_id
                ORDER BY count DESC
            """)
            stats['by_release'] = {row['release_id']: row['count'] for row in release_results}

            # Open access statistics
            oa_result = self.db.fetch_one("""
                SELECT
                    COUNT(*) FILTER (WHERE is_open_access = TRUE) as open_access_count,
                    COUNT(*) FILTER (WHERE is_open_access = FALSE) as closed_access_count
                FROM dataset_papers
            """)
            if oa_result:
                stats['open_access_count'] = oa_result['open_access_count'] or 0
                stats['closed_access_count'] = oa_result['closed_access_count'] or 0

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get statistics: {e}")
            return {}

    def delete_by_release(self, release_id: str) -> int:
        """Delete all papers from a specific release. Returns number of deleted rows."""
        try:
            sql = "DELETE FROM dataset_papers WHERE release_id = %s"
            with self.db.get_cursor() as cursor:
                cursor.execute(sql, (release_id,))
                deleted_count = cursor.rowcount
                self.logger.info(f"Deleted {deleted_count} papers from release {release_id}")
                return deleted_count
        except Exception as e:
            self.logger.error(f"Failed to delete papers by release: {e}")
            return 0
