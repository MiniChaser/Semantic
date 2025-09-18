"""
Processing Metadata Repository
Unified metadata management for all processing operations
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from ..connection import DatabaseManager


class ProcessingMetadataRepository:
    """Repository for managing processing metadata"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.ProcessingMetadataRepository')
        logger.setLevel(logging.INFO)

        # Don't add handlers if root logger is already configured
        # This prevents duplicate logging when root logger has handlers
        root_logger = logging.getLogger()
        if not logger.handlers and not root_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def record_processing_start(self, entity_type: str, process_type: str,
                              entity_id: Optional[int] = None,
                              metadata: Optional[Dict[str, Any]] = None) -> int:
        """
        Record the start of a processing operation

        Args:
            entity_type: Type of entity being processed ('dblp_paper', 'enriched_paper', 'author', 'pipeline')
            process_type: Type of processing ('dblp_sync', 's2_enrichment', 'pdf_download', 'author_processing', 'full_pipeline')
            entity_id: ID of the specific entity (None for global operations)
            metadata: Additional metadata as JSON

        Returns:
            ID of the created metadata record
        """
        try:
            query = """
            INSERT INTO processing_metadata
            (entity_type, entity_id, process_type, status, started_at, metadata_json, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """
            now = datetime.now()

            result = self.db_manager.fetch_one(
                query,
                (entity_type, entity_id, process_type, 'processing', now,
                 metadata, now, now)
            )

            if result:
                metadata_id = result['id']
                self.logger.info(f"Started processing: {process_type} for {entity_type} (ID: {entity_id}) - Metadata ID: {metadata_id}")
                return metadata_id
            else:
                raise Exception("Failed to insert processing metadata record")

        except Exception as e:
            self.logger.error(f"Failed to record processing start: {e}")
            raise

    def record_processing_success(self, metadata_id: int,
                                metadata: Optional[Dict[str, Any]] = None):
        """
        Record successful completion of a processing operation

        Args:
            metadata_id: ID of the metadata record to update
            metadata: Additional completion metadata
        """
        try:
            query = """
            UPDATE processing_metadata
            SET status = %s, completed_at = %s, updated_at = %s
            """
            params = ['completed', datetime.now(), datetime.now()]

            if metadata:
                query += ", metadata_json = COALESCE(metadata_json, '{}') || %s"
                params.append(metadata)

            query += " WHERE id = %s"
            params.append(metadata_id)

            if self.db_manager.execute_query(query, tuple(params)):
                self.logger.info(f"Processing completed successfully - Metadata ID: {metadata_id}")
            else:
                raise Exception("Failed to update processing metadata")

        except Exception as e:
            self.logger.error(f"Failed to record processing success: {e}")
            raise

    def record_processing_failure(self, metadata_id: int, error_message: str,
                                metadata: Optional[Dict[str, Any]] = None):
        """
        Record failure of a processing operation

        Args:
            metadata_id: ID of the metadata record to update
            error_message: Error message describing the failure
            metadata: Additional failure metadata
        """
        try:
            query = """
            UPDATE processing_metadata
            SET status = %s, completed_at = %s, error_message = %s, updated_at = %s
            """
            params = ['failed', datetime.now(), error_message, datetime.now()]

            if metadata:
                query += ", metadata_json = COALESCE(metadata_json, '{}') || %s"
                params.append(metadata)

            query += " WHERE id = %s"
            params.append(metadata_id)

            if self.db_manager.execute_query(query, tuple(params)):
                self.logger.warning(f"Processing failed - Metadata ID: {metadata_id}, Error: {error_message}")
            else:
                raise Exception("Failed to update processing metadata")

        except Exception as e:
            self.logger.error(f"Failed to record processing failure: {e}")
            raise

    def get_latest_processing_status(self, entity_type: str, process_type: str,
                                   entity_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Get the latest processing status for a specific entity and process type

        Args:
            entity_type: Type of entity
            process_type: Type of processing
            entity_id: ID of the specific entity (None for global operations)

        Returns:
            Dictionary with processing status information or None if not found
        """
        try:
            query = """
            SELECT * FROM processing_metadata
            WHERE entity_type = %s AND process_type = %s
            """
            params = [entity_type, process_type]

            if entity_id is not None:
                query += " AND entity_id = %s"
                params.append(entity_id)
            else:
                query += " AND entity_id IS NULL"

            query += " ORDER BY created_at DESC LIMIT 1"

            result = self.db_manager.fetch_one(query, tuple(params))
            return dict(result) if result else None

        except Exception as e:
            self.logger.error(f"Failed to get processing status: {e}")
            return None

    def get_processing_statistics(self, process_type: Optional[str] = None,
                                entity_type: Optional[str] = None,
                                hours: Optional[int] = 24) -> Dict[str, Any]:
        """
        Get processing statistics

        Args:
            process_type: Filter by specific process type
            entity_type: Filter by specific entity type
            hours: Look back this many hours (default: 24)

        Returns:
            Dictionary with processing statistics
        """
        try:
            base_query = """
            SELECT
                status,
                COUNT(*) as count,
                COUNT(CASE WHEN completed_at IS NOT NULL THEN 1 END) as completed_count,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration_seconds
            FROM processing_metadata
            WHERE created_at >= NOW() - INTERVAL '%s hours'
            """
            params = [hours]

            if process_type:
                base_query += " AND process_type = %s"
                params.append(process_type)

            if entity_type:
                base_query += " AND entity_type = %s"
                params.append(entity_type)

            base_query += " GROUP BY status ORDER BY status"

            results = self.db_manager.fetch_all(base_query, tuple(params))

            stats = {
                'total_operations': 0,
                'by_status': {},
                'avg_duration_seconds': 0
            }

            total_duration = 0
            completed_count = 0

            for row in results:
                status = row['status']
                count = row['count']
                stats['total_operations'] += count
                stats['by_status'][status] = count

                if row['avg_duration_seconds']:
                    total_duration += row['avg_duration_seconds'] * row['completed_count']
                    completed_count += row['completed_count']

            if completed_count > 0:
                stats['avg_duration_seconds'] = total_duration / completed_count

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get processing statistics: {e}")
            return {'total_operations': 0, 'by_status': {}, 'avg_duration_seconds': 0}

    def get_last_successful_run(self, process_type: str, entity_type: str = 'pipeline') -> Optional[datetime]:
        """
        Get the timestamp of the last successful run for a specific process type

        Args:
            process_type: Type of processing to check
            entity_type: Type of entity (default: 'pipeline' for global operations)

        Returns:
            Datetime of last successful run or None if not found
        """
        try:
            query = """
            SELECT completed_at FROM processing_metadata
            WHERE entity_type = %s AND process_type = %s AND status = 'completed'
            AND entity_id IS NULL
            ORDER BY completed_at DESC LIMIT 1
            """

            result = self.db_manager.fetch_one(query, (entity_type, process_type))
            return result['completed_at'] if result else None

        except Exception as e:
            self.logger.error(f"Failed to get last successful run: {e}")
            return None

    def cleanup_old_metadata(self, days: int = 30) -> int:
        """
        Clean up old metadata records

        Args:
            days: Remove records older than this many days

        Returns:
            Number of records deleted
        """
        try:
            query = """
            DELETE FROM processing_metadata
            WHERE created_at < NOW() - INTERVAL '%s days'
            AND status IN ('completed', 'failed')
            """

            result = self.db_manager.execute_query(query, (days,))

            # Get affected row count (this is database-specific)
            cleanup_count = 0  # Would need to implement row count retrieval

            self.logger.info(f"Cleaned up old metadata records (older than {days} days)")
            return cleanup_count

        except Exception as e:
            self.logger.error(f"Failed to cleanup old metadata: {e}")
            return 0

    def get_processing_history(self, entity_type: str, entity_id: Optional[int] = None,
                             limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get processing history for an entity

        Args:
            entity_type: Type of entity
            entity_id: ID of the specific entity (None for global operations)
            limit: Maximum number of records to return

        Returns:
            List of processing history records
        """
        try:
            query = """
            SELECT * FROM processing_metadata
            WHERE entity_type = %s
            """
            params = [entity_type]

            if entity_id is not None:
                query += " AND entity_id = %s"
                params.append(entity_id)
            else:
                query += " AND entity_id IS NULL"

            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)

            results = self.db_manager.fetch_all(query, tuple(params))
            return [dict(row) for row in results] if results else []

        except Exception as e:
            self.logger.error(f"Failed to get processing history: {e}")
            return []