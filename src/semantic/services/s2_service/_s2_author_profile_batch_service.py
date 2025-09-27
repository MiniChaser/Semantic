#!/usr/bin/env python3
"""
S2 Author Profile Batch Service
Optimized batch processing service for S2 Author API data with efficient caching
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from ...database.connection import DatabaseManager
from .s2_service import SemanticScholarAPI


class S2AuthorProfileBatchService:
    """
    Service for efficient batch processing of S2 Author API data

    Key Features:
    - Batch API calls (up to 1000 IDs per call)
    - Timestamp-based cache invalidation
    - Separate storage for S2 author data
    - Efficient sync to author_profiles table
    """

    def __init__(self, db_manager: DatabaseManager, api_key: Optional[str] = None):
        self.db_manager = db_manager
        self.s2_api = SemanticScholarAPI(api_key)
        self.logger = logging.getLogger(__name__)

    def create_s2_author_profiles_table(self) -> bool:
        """
        Create the s2_author_profiles table for storing S2 author data

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Creating s2_author_profiles table...")

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS s2_author_profiles (
                id SERIAL PRIMARY KEY,
                s2_author_id VARCHAR(255) UNIQUE NOT NULL,
                name TEXT,
                url TEXT,
                affiliations JSONB,
                paper_count INTEGER,
                citation_count INTEGER,
                h_index INTEGER,
                raw_data JSONB,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """

            self.db_manager.execute_query(create_table_sql)

            # Create indexes for efficient lookups
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_s2_author_profiles_author_id ON s2_author_profiles(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_s2_author_profiles_updated_at ON s2_author_profiles(updated_at);",
                "CREATE INDEX IF NOT EXISTS idx_s2_author_profiles_created_at ON s2_author_profiles(created_at);",
            ]

            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)

            self.logger.info("s2_author_profiles table created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create s2_author_profiles table: {e}")
            return False

    def get_author_ids_needing_update(self, limit: Optional[int] = None) -> List[str]:
        """
        Get S2 author IDs that need updating based on timestamp comparison

        Finds IDs where:
        1. No record exists in s2_author_profiles, OR
        2. s2_author_profiles.updated_at < authorships.created_at (data is stale)

        Args:
            limit: Maximum number of author IDs to return

        Returns:
            List of S2 author IDs needing update
        """
        try:
            self.logger.info("Finding S2 author IDs that need updating...")

            # Query to find S2 author IDs that need refreshing
            query = """
            SELECT DISTINCT a.s2_author_id
            FROM authorships a
            LEFT JOIN s2_author_profiles sap ON a.s2_author_id = sap.s2_author_id
            WHERE a.s2_author_id IS NOT NULL
              AND a.s2_author_id != ''
              AND (
                  sap.s2_author_id IS NULL  -- No record in s2_author_profiles
                  OR sap.updated_at < a.created_at  -- Data is stale
              )
            ORDER BY a.s2_author_id
            """

            if limit:
                query += f" LIMIT {limit}"

            result = self.db_manager.fetch_all(query)

            author_ids = [row['s2_author_id'] for row in result if row['s2_author_id']]

            self.logger.info(f"Found {len(author_ids)} author IDs needing update")
            return author_ids

        except Exception as e:
            self.logger.error(f"Failed to get author IDs needing update: {e}")
            return []

    def batch_fetch_and_store_s2_authors(self, author_ids: List[str]) -> Dict[str, int]:
        """
        Batch fetch S2 author data and store in s2_author_profiles table

        Args:
            author_ids: List of S2 author IDs to fetch

        Returns:
            Statistics about the batch operation
        """
        if not author_ids:
            return {'processed': 0, 'successful': 0, 'api_calls': 0, 'errors': 0}

        stats = {
            'processed': 0,
            'successful': 0,
            'api_calls': 0,
            'errors': 0
        }

        # Process in batches of 1000 (S2 API limit)
        batch_size = 1000

        for i in range(0, len(author_ids), batch_size):
            batch_ids = author_ids[i:i + batch_size]

            try:
                self.logger.info(f"Processing batch {i//batch_size + 1}: {len(batch_ids)} author IDs")

                # Call S2 API for batch
                authors_data = self.s2_api.batch_get_authors(batch_ids)
                stats['api_calls'] += 1
                stats['processed'] += len(batch_ids)

                if authors_data:
                    # Store successful results
                    stored_count = self._store_s2_author_batch(authors_data, batch_ids)
                    stats['successful'] += stored_count

                    self.logger.info(f"Stored {stored_count}/{len(batch_ids)} authors from batch")
                else:
                    self.logger.warning(f"No data received for batch starting at index {i}")

            except Exception as e:
                self.logger.error(f"Error processing batch starting at index {i}: {e}")
                stats['errors'] += len(batch_ids)

        return stats

    def _store_s2_author_batch(self, authors_data: List[Optional[Dict]], batch_ids: List[str]) -> int:
        """
        Store a batch of S2 author data in the database

        Args:
            authors_data: List of S2 author API responses (may contain None values)
            batch_ids: Corresponding list of S2 author IDs

        Returns:
            Number of authors successfully stored
        """
        stored_count = 0

        try:
            # Prepare batch insert/update data
            for i, (author_data, author_id) in enumerate(zip(authors_data, batch_ids)):
                if author_data is None:
                    continue

                try:
                    # Extract fields from S2 API response
                    name = author_data.get('name')
                    url = author_data.get('url')
                    affiliations = author_data.get('affiliations', [])
                    paper_count = author_data.get('paperCount')
                    citation_count = author_data.get('citationCount')
                    h_index = author_data.get('hIndex')

                    # Use UPSERT to insert or update existing records
                    upsert_sql = """
                    INSERT INTO s2_author_profiles
                        (s2_author_id, name, url, affiliations, paper_count, citation_count, h_index, raw_data, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (s2_author_id)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        url = EXCLUDED.url,
                        affiliations = EXCLUDED.affiliations,
                        paper_count = EXCLUDED.paper_count,
                        citation_count = EXCLUDED.citation_count,
                        h_index = EXCLUDED.h_index,
                        raw_data = EXCLUDED.raw_data,
                        updated_at = CURRENT_TIMESTAMP
                    """

                    params = (
                        author_id,
                        name,
                        url,
                        json.dumps(affiliations) if affiliations else None,
                        paper_count,
                        citation_count,
                        h_index,
                        json.dumps(author_data)  # Store complete response
                    )

                    if self.db_manager.execute_query(upsert_sql, params):
                        stored_count += 1
                    else:
                        self.logger.error(f"Failed to store author {author_id}")

                except Exception as e:
                    self.logger.error(f"Error storing author {author_id}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error in batch storage: {e}")

        return stored_count

    def sync_to_author_profiles(self, author_ids: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Sync data from s2_author_profiles to author_profiles table

        Args:
            author_ids: Optional list of specific author IDs to sync. If None, sync all.

        Returns:
            Statistics about the sync operation
        """
        try:
            self.logger.info("Syncing s2_author_profiles data to author_profiles...")

            # Build WHERE clause for specific author IDs
            where_clause = ""
            params = []
            if author_ids:
                placeholders = ','.join(['%s'] * len(author_ids))
                where_clause = f"WHERE sap.s2_author_id IN ({placeholders})"
                params = author_ids

            # Update author_profiles with data from s2_author_profiles
            sync_sql = f"""
            UPDATE author_profiles ap
            SET
                homepage = sap.url,
                s2_affiliations = CASE
                    WHEN sap.affiliations IS NOT NULL
                    THEN (
                        SELECT string_agg(value::text, ',')
                        FROM jsonb_array_elements_text(sap.affiliations)
                    )
                    ELSE NULL
                END,
                s2_paper_count = sap.paper_count,
                s2_citation_count = sap.citation_count,
                s2_h_index = sap.h_index,
                updated_at = CURRENT_TIMESTAMP
            FROM s2_author_profiles sap
            WHERE ap.s2_author_id = sap.s2_author_id
              AND (ap.homepage IS NULL
                   OR ap.s2_affiliations IS NULL
                   OR ap.s2_paper_count IS NULL
                   OR ap.s2_citation_count IS NULL
                   OR ap.s2_h_index IS NULL)
            {where_clause}
            """

            result = self.db_manager.execute_query(sync_sql, params)

            # Get count of updated records
            if result:
                count_sql = f"""
                SELECT COUNT(DISTINCT ap.id) as updated_count
                FROM author_profiles ap
                JOIN s2_author_profiles sap ON ap.s2_author_id = sap.s2_author_id
                WHERE ap.updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 minute'
                {where_clause}
                """

                count_result = self.db_manager.fetch_one(count_sql, params)
                updated_count = count_result['updated_count'] if count_result else 0

                self.logger.info(f"Successfully synced {updated_count} author profiles")
                return {'updated': updated_count, 'errors': 0}
            else:
                return {'updated': 0, 'errors': 1}

        except Exception as e:
            self.logger.error(f"Failed to sync to author_profiles: {e}")
            return {'updated': 0, 'errors': 1}

    def get_processing_statistics(self) -> Dict:
        """Get comprehensive statistics about the batch processing system"""
        try:
            stats = {}

            # S2 author profiles statistics
            s2_stats = self.db_manager.fetch_one("""
                SELECT
                    COUNT(*) as total_s2_profiles,
                    COUNT(CASE WHEN url IS NOT NULL THEN 1 END) as profiles_with_homepage,
                    COUNT(CASE WHEN affiliations IS NOT NULL THEN 1 END) as profiles_with_affiliations,
                    COUNT(CASE WHEN paper_count IS NOT NULL THEN 1 END) as profiles_with_paper_count,
                    COUNT(CASE WHEN citation_count IS NOT NULL THEN 1 END) as profiles_with_citation_count,
                    COUNT(CASE WHEN h_index IS NOT NULL THEN 1 END) as profiles_with_h_index,
                    AVG(paper_count) as avg_paper_count,
                    AVG(citation_count) as avg_citation_count,
                    AVG(h_index) as avg_h_index,
                    MAX(updated_at) as last_update
                FROM s2_author_profiles
            """)

            # Author profiles sync statistics
            sync_stats = self.db_manager.fetch_one("""
                SELECT
                    COUNT(CASE WHEN ap.homepage IS NOT NULL THEN 1 END) as profiles_with_s2_homepage,
                    COUNT(CASE WHEN ap.s2_affiliations IS NOT NULL THEN 1 END) as profiles_with_s2_affiliations,
                    COUNT(CASE WHEN ap.s2_paper_count IS NOT NULL THEN 1 END) as profiles_with_s2_paper_count,
                    COUNT(CASE WHEN ap.s2_citation_count IS NOT NULL THEN 1 END) as profiles_with_s2_citation_count,
                    COUNT(CASE WHEN ap.s2_h_index IS NOT NULL THEN 1 END) as profiles_with_s2_h_index
                FROM author_profiles ap
                WHERE ap.s2_author_id IS NOT NULL
            """)

            stats = {
                's2_author_profiles': s2_stats,
                'author_profiles_sync': sync_stats,
                'timestamp': datetime.now().isoformat()
            }

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get processing statistics: {e}")
            return {'error': str(e)}

    def run_batch_enrichment(self, limit: Optional[int] = None) -> Dict:
        """
        Main method to run the complete batch enrichment process

        Args:
            limit: Maximum number of authors to process

        Returns:
            Comprehensive statistics about the enrichment process
        """
        start_time = time.time()
        self.logger.info("Starting S2 author batch enrichment process...")

        try:
            # Step 1: Get author IDs that need updating
            author_ids_to_update = self.get_author_ids_needing_update(limit)

            if not author_ids_to_update:
                self.logger.info("No author IDs need updating")
                return {
                    'total_ids_to_process': 0,
                    'batch_fetch_stats': {'processed': 0, 'successful': 0, 'api_calls': 0, 'errors': 0},
                    'sync_stats': {'updated': 0, 'errors': 0},
                    'processing_time': 0
                }

            # Step 2: Batch fetch and store S2 author data
            batch_stats = self.batch_fetch_and_store_s2_authors(author_ids_to_update)

            # Step 3: Sync successful data to author_profiles
            sync_stats = self.sync_to_author_profiles(author_ids_to_update)

            # Calculate processing time
            end_time = time.time()
            processing_time = end_time - start_time

            # Compile final statistics
            final_stats = {
                'total_ids_to_process': len(author_ids_to_update),
                'batch_fetch_stats': batch_stats,
                'sync_stats': sync_stats,
                'processing_time': processing_time,
                'performance_improvement': f"Processed {len(author_ids_to_update)} authors in {batch_stats['api_calls']} API calls vs {len(author_ids_to_update)} individual calls"
            }

            self.logger.info(f"Batch enrichment completed in {processing_time:.2f} seconds")
            return final_stats

        except Exception as e:
            self.logger.error(f"Batch enrichment process failed: {e}")
            return {'error': str(e)}