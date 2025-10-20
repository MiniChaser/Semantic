"""
Dataset Release data repository class
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict
from ..connection import DatabaseManager
from ..models.dataset_release import DatasetRelease


class DatasetReleaseRepository:
    """Dataset Release data repository class"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger for repository operations"""
        logger = logging.getLogger(f'{__name__}.DatasetReleaseRepository')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def create_release_record(self, release: DatasetRelease) -> Optional[int]:
        """
        Create release record and return ID
        Uses ON CONFLICT DO NOTHING to handle duplicate release_ids
        """
        try:
            sql = """
            INSERT INTO dataset_release
            (release_id, dataset_name, release_date, description, file_count,
             processing_status, download_start_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (release_id) DO NOTHING
            RETURNING id
            """
            params = (
                release.release_id,
                release.dataset_name,
                release.release_date,
                release.description,
                release.file_count,
                release.processing_status,
                release.download_start_time
            )

            result = self.db.fetch_one(sql, params)

            if result:
                self.logger.info(f"Created release record: {release.release_id}")
                return result['id']
            else:
                self.logger.info(f"Release already exists: {release.release_id}")
                # Get existing ID
                existing = self.get_release_by_id(release.release_id)
                return existing.id if existing else None

        except Exception as e:
            self.logger.error(f"Failed to create release record: {e}")
            return None

    def update_release_status(self, release_id: str, status: str, **kwargs):
        """
        Update release status and other optional fields
        kwargs can include: download_end_time, processing_start_time, processing_end_time,
                          total_papers_processed, papers_inserted, papers_updated, etc.
        """
        try:
            update_fields = ['processing_status = %s', 'updated_at = CURRENT_TIMESTAMP']
            params = [status]

            # Add optional fields
            for key, value in kwargs.items():
                update_fields.append(f'{key} = %s')
                params.append(value)

            params.append(release_id)

            sql = f"""
            UPDATE dataset_release
            SET {', '.join(update_fields)}
            WHERE release_id = %s
            """

            if self.db.execute_query(sql, tuple(params)):
                self.logger.info(f"Updated release status: {release_id} -> {status}")
                return True
            return False

        except Exception as e:
            self.logger.error(f"Failed to update release status: {e}")
            return False

    def get_release_by_id(self, release_id: str) -> Optional[DatasetRelease]:
        """Get release information by release_id"""
        try:
            sql = "SELECT * FROM dataset_release WHERE release_id = %s"
            result = self.db.fetch_one(sql, (release_id,))

            if result:
                return DatasetRelease.from_dict(dict(result))
            return None

        except Exception as e:
            self.logger.error(f"Failed to get release by ID: {e}")
            return None

    def get_latest_release(self, dataset_name: str) -> Optional[DatasetRelease]:
        """Get the latest release for a specific dataset"""
        try:
            sql = """
            SELECT * FROM dataset_release
            WHERE dataset_name = %s
            ORDER BY release_date DESC, created_at DESC
            LIMIT 1
            """
            result = self.db.fetch_one(sql, (dataset_name,))

            if result:
                return DatasetRelease.from_dict(dict(result))
            return None

        except Exception as e:
            self.logger.error(f"Failed to get latest release: {e}")
            return None

    def get_all_releases(self, dataset_name: Optional[str] = None) -> List[DatasetRelease]:
        """Get all releases, optionally filtered by dataset_name"""
        try:
            if dataset_name:
                sql = """
                SELECT * FROM dataset_release
                WHERE dataset_name = %s
                ORDER BY release_date DESC, created_at DESC
                """
                results = self.db.fetch_all(sql, (dataset_name,))
            else:
                sql = """
                SELECT * FROM dataset_release
                ORDER BY release_date DESC, created_at DESC
                """
                results = self.db.fetch_all(sql)

            return [DatasetRelease.from_dict(dict(row)) for row in results]

        except Exception as e:
            self.logger.error(f"Failed to get releases: {e}")
            return []

    def get_statistics(self) -> Dict:
        """Get release statistics"""
        try:
            stats = {}

            # Total releases
            total_result = self.db.fetch_one("SELECT COUNT(*) as total FROM dataset_release")
            stats['total_releases'] = total_result['total'] if total_result else 0

            # Statistics by status
            status_results = self.db.fetch_all("""
                SELECT processing_status, COUNT(*) as count
                FROM dataset_release
                GROUP BY processing_status
            """)
            stats['by_status'] = {row['processing_status']: row['count'] for row in status_results}

            # Statistics by dataset
            dataset_results = self.db.fetch_all("""
                SELECT dataset_name, COUNT(*) as count
                FROM dataset_release
                GROUP BY dataset_name
            """)
            stats['by_dataset'] = {row['dataset_name']: row['count'] for row in dataset_results}

            # Total papers processed
            papers_result = self.db.fetch_one("""
                SELECT
                    SUM(total_papers_processed) as total_processed,
                    SUM(papers_inserted) as total_inserted,
                    SUM(papers_updated) as total_updated
                FROM dataset_release
                WHERE processing_status = 'completed'
            """)

            if papers_result:
                stats['total_papers_processed'] = papers_result['total_processed'] or 0
                stats['total_papers_inserted'] = papers_result['total_inserted'] or 0
                stats['total_papers_updated'] = papers_result['total_updated'] or 0

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get statistics: {e}")
            return {}
