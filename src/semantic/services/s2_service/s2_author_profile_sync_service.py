#!/usr/bin/env python3
"""
S2 Author Profile Sync Service
Simple service for syncing data from s2_author_profiles table to author_profiles table
No API calls - uses cached data only
"""

import logging
import time
from datetime import datetime
from typing import Dict

from ...database.connection import DatabaseManager


class S2AuthorProfileSyncService:
    """
    Service for syncing S2 author data from s2_author_profiles to author_profiles

    This service reads cached S2 data and updates author_profiles without API calls
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)

    def sync_author_profiles(self, limit: int = None) -> Dict:
        """
        Sync S2 author data from s2_author_profiles to author_profiles table

        Args:
            limit: Maximum number of authors to process

        Returns:
            Sync statistics
        """
        start_time = time.time()
        self.logger.info("Starting S2 author profile sync from cached data...")

        try:
            # Get authors that need S2 enrichment from cached data
            query = """
                SELECT
                    ap.id,
                    ap.s2_author_id,
                    ap.dblp_author_name,
                    sap.name as s2_name,
                    sap.url as homepage,
                    sap.affiliations,
                    sap.paper_count as s2_paper_count,
                    sap.citation_count as s2_citation_count,
                    sap.h_index as s2_h_index,
                    sap.updated_at as s2_data_updated
                FROM author_profiles ap
                JOIN s2_author_profiles sap ON ap.s2_author_id = sap.s2_author_id
                WHERE ap.s2_author_id IS NOT NULL
                  AND ap.s2_author_id != ''
                  AND (ap.homepage IS NULL
                       OR ap.s2_affiliations IS NULL
                       OR ap.s2_paper_count IS NULL
                       OR ap.s2_citation_count IS NULL
                       OR ap.s2_h_index IS NULL)
            """

            if limit:
                query += f" LIMIT {limit}"

            authors_to_sync = self.db_manager.fetch_all(query)

            if not authors_to_sync:
                self.logger.info("No authors need S2 data sync from cached data")
                return {
                    'total_authors_processed': 0,
                    'authors_synced': 0,
                    'errors': 0,
                    'processing_time': time.time() - start_time
                }

            self.logger.info(f"Found {len(authors_to_sync)} authors to sync from cached S2 data")

            # Statistics tracking
            stats = {
                'total_authors_processed': 0,
                'authors_synced': 0,
                'errors': 0
            }

            # Process each author
            for author_record in authors_to_sync:
                try:
                    sync_result = self._sync_single_author(author_record)

                    stats['total_authors_processed'] += 1
                    if sync_result:
                        stats['authors_synced'] += 1

                    # Progress logging
                    if stats['total_authors_processed'] % 10 == 0:
                        self.logger.info(f"Processed {stats['total_authors_processed']}/{len(authors_to_sync)} authors")

                except Exception as e:
                    self.logger.error(f"Error processing author {author_record['dblp_author_name']}: {e}")
                    stats['errors'] += 1
                    continue

            # Calculate processing time
            processing_time = time.time() - start_time
            stats['processing_time'] = processing_time

            self.logger.info(f"S2 author profile sync completed. Synced {stats['authors_synced']} authors in {processing_time:.2f} seconds.")
            return stats

        except Exception as e:
            self.logger.error(f"S2 author profile sync failed: {e}")
            return {
                'error': str(e),
                'processing_time': time.time() - start_time
            }

    def _sync_single_author(self, author_record: Dict) -> bool:
        """
        Sync a single author record with cached S2 data

        Args:
            author_record: Author data from query

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build update data
            update_data = {}

            # Homepage URL
            if author_record['homepage'] and not author_record.get('homepage'):
                update_data['homepage'] = author_record['homepage']

            # Affiliations (convert JSONB array to comma-separated string)
            if author_record['affiliations']:
                try:
                    if isinstance(author_record['affiliations'], list):
                        affiliations_str = ','.join([str(aff) for aff in author_record['affiliations'] if aff])
                    else:
                        # Already a string or JSON string
                        affiliations_str = str(author_record['affiliations'])

                    if affiliations_str and not author_record.get('s2_affiliations'):
                        update_data['s2_affiliations'] = affiliations_str
                except Exception as e:
                    self.logger.warning(f"Error processing affiliations for {author_record['dblp_author_name']}: {e}")

            # Paper count
            if author_record['s2_paper_count'] is not None and not author_record.get('s2_paper_count'):
                update_data['s2_paper_count'] = author_record['s2_paper_count']

            # Citation count
            if author_record['s2_citation_count'] is not None and not author_record.get('s2_citation_count'):
                update_data['s2_citation_count'] = author_record['s2_citation_count']

            # H-index
            if author_record['s2_h_index'] is not None and not author_record.get('s2_h_index'):
                update_data['s2_h_index'] = author_record['s2_h_index']

            if not update_data:
                return False

            # Build UPDATE query
            set_clauses = []
            params = []

            for field, value in update_data.items():
                set_clauses.append(f"{field} = %s")
                params.append(value)

            set_clauses.append("updated_at = CURRENT_TIMESTAMP")

            update_query = f"""
                UPDATE author_profiles
                SET {', '.join(set_clauses)}
                WHERE id = %s
            """
            params.append(author_record['id'])

            # Execute update
            result = self.db_manager.execute_query(update_query, params)

            if result:
                self.logger.debug(f"Successfully synced author {author_record['dblp_author_name']} with cached S2 data")
                return True
            else:
                self.logger.error(f"Failed to sync author {author_record['id']}")
                return False

        except Exception as e:
            self.logger.error(f"Error syncing author {author_record['id']}: {e}")
            return False

    def get_sync_statistics(self) -> Dict:
        """Get statistics about available cached data and sync status"""
        try:
            # S2 author profiles statistics
            s2_stats = self.db_manager.fetch_one("""
                SELECT
                    COUNT(*) as total_s2_profiles,
                    COUNT(CASE WHEN url IS NOT NULL THEN 1 END) as profiles_with_homepage,
                    COUNT(CASE WHEN affiliations IS NOT NULL THEN 1 END) as profiles_with_affiliations,
                    COUNT(CASE WHEN paper_count IS NOT NULL THEN 1 END) as profiles_with_paper_count,
                    COUNT(CASE WHEN citation_count IS NOT NULL THEN 1 END) as profiles_with_citation_count,
                    COUNT(CASE WHEN h_index IS NOT NULL THEN 1 END) as profiles_with_h_index,
                    MAX(updated_at) as last_update
                FROM s2_author_profiles
            """)

            # Author profiles with S2 data statistics
            sync_stats = self.db_manager.fetch_one("""
                SELECT
                    COUNT(*) as total_author_profiles,
                    COUNT(CASE WHEN s2_author_id IS NOT NULL AND s2_author_id != '' THEN 1 END) as profiles_with_s2_id,
                    COUNT(CASE WHEN homepage IS NOT NULL THEN 1 END) as profiles_with_homepage,
                    COUNT(CASE WHEN s2_affiliations IS NOT NULL THEN 1 END) as profiles_with_affiliations,
                    COUNT(CASE WHEN s2_paper_count IS NOT NULL THEN 1 END) as profiles_with_paper_count,
                    COUNT(CASE WHEN s2_citation_count IS NOT NULL THEN 1 END) as profiles_with_citation_count,
                    COUNT(CASE WHEN s2_h_index IS NOT NULL THEN 1 END) as profiles_with_h_index
                FROM author_profiles
            """)

            # Authors that can be synced
            syncable_stats = self.db_manager.fetch_one("""
                SELECT COUNT(*) as authors_ready_to_sync
                FROM author_profiles ap
                JOIN s2_author_profiles sap ON ap.s2_author_id = sap.s2_author_id
                WHERE ap.s2_author_id IS NOT NULL
                  AND ap.s2_author_id != ''
                  AND (ap.homepage IS NULL
                       OR ap.s2_affiliations IS NULL
                       OR ap.s2_paper_count IS NULL
                       OR ap.s2_citation_count IS NULL
                       OR ap.s2_h_index IS NULL)
            """)

            return {
                'cached_s2_data': s2_stats,
                'author_profiles_status': sync_stats,
                'sync_ready': syncable_stats,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get sync statistics: {e}")
            return {'error': str(e)}