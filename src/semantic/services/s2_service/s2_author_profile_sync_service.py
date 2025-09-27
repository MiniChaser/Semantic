#!/usr/bin/env python3
"""
S2 Author Profile Sync Service
Simple service for syncing data from s2_author_profiles table to author_profiles table
No API calls - uses cached data only
"""

import logging
import time
import threading
import os
from datetime import datetime
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from ...database.connection import DatabaseManager


class S2AuthorProfileSyncService:
    """
    Service for syncing S2 author data from s2_author_profiles to author_profiles

    This service reads cached S2 data and updates author_profiles without API calls
    """

    def __init__(self, db_manager: DatabaseManager, max_workers: int = None):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        self._stats_lock = threading.Lock()
        self._max_workers = max_workers

    def _calculate_optimal_workers(self) -> int:
        """Calculate optimal number of worker threads based on system and database configuration"""
        if self._max_workers is not None:
            return self._max_workers

        # Get database pool configuration
        db_pool_size = getattr(self.db_manager.config, 'pool_size', 10)
        db_max_overflow = getattr(self.db_manager.config, 'max_overflow', 20)
        total_db_connections = db_pool_size + db_max_overflow

        # Get CPU count
        cpu_count = os.cpu_count() or 4

        # Calculate optimal workers based on testing results
        # Testing shows 6-8 workers perform best for database I/O operations
        optimal_workers = min(
            12,  # Maximum cap - testing shows diminishing returns after 8
            max(6, db_pool_size // 2),  # More aggressive: use 1/2 of base pool size
            cpu_count * 2,  # For I/O bound tasks, can use more than CPU count
            total_db_connections // 3  # Less conservative connection usage
        )

        self.logger.info(f"Calculated optimal workers: {optimal_workers} "
                        f"(DB pool: {db_pool_size}, CPU: {cpu_count})")

        return optimal_workers

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
            # Get authors that need S2 enrichment (without JOIN to handle comma-separated IDs)
            query = """
                SELECT
                    ap.id,
                    ap.s2_author_id,
                    ap.dblp_author_name,
                    -- Current values in author_profiles (targets)
                    ap.homepage as current_homepage,
                    ap.s2_affiliations as current_s2_affiliations,
                    ap.s2_paper_count as current_s2_paper_count,
                    ap.s2_citation_count as current_s2_citation_count,
                    ap.s2_h_index as current_s2_h_index
                FROM author_profiles ap
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
                'errors': 0,
                'total_authors': len(authors_to_sync)
            }

            # Process authors using ThreadPoolExecutor with optimal worker count
            optimal_workers = self._calculate_optimal_workers()
            with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
                # Submit all tasks
                future_to_author = {
                    executor.submit(self._sync_single_author, author_record): author_record
                    for author_record in authors_to_sync
                }

                # Process completed tasks
                for future in as_completed(future_to_author):
                    author_record = future_to_author[future]
                    try:
                        sync_result = future.result()
                        self._update_stats_thread_safe(stats, sync_result, author_record['dblp_author_name'])
                    except Exception as e:
                        self.logger.error(f"Error processing author {author_record['dblp_author_name']}: {e}")
                        with self._stats_lock:
                            stats['errors'] += 1

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

    def _fetch_and_aggregate_s2_data(self, s2_author_ids: list) -> Dict:
        """
        Fetch and aggregate S2 data from multiple s2_author_profiles records

        Args:
            s2_author_ids: List of s2 author IDs

        Returns:
            Aggregated S2 data dictionary
        """
        if not s2_author_ids:
            return {}

        # Remove duplicates and empty values
        unique_ids = list(set([str(id_).strip() for id_ in s2_author_ids if id_ and str(id_).strip()]))

        if not unique_ids:
            return {}

        try:
            # Build IN clause for multiple IDs
            placeholders = ','.join(['%s'] * len(unique_ids))
            query = f"""
                SELECT
                    s2_author_id,
                    name,
                    url,
                    affiliations,
                    paper_count,
                    citation_count,
                    h_index,
                    updated_at
                FROM s2_author_profiles
                WHERE s2_author_id IN ({placeholders})
                ORDER BY updated_at DESC
            """

            s2_profiles = self.db_manager.fetch_all(query, unique_ids)

            if not s2_profiles:
                return {}

            # Aggregate the data
            aggregated = {
                'names': [],
                'urls': [],
                'affiliations_list': [],
                'paper_counts': [],
                'citation_counts': [],
                'h_indices': [],
                'latest_update': None
            }

            for profile in s2_profiles:
                # Collect names
                if profile['name']:
                    aggregated['names'].append(str(profile['name']))

                # Collect URLs (first non-empty one will be used)
                if profile['url'] and not aggregated['urls']:
                    aggregated['urls'].append(str(profile['url']))

                # Collect affiliations
                if profile['affiliations']:
                    if isinstance(profile['affiliations'], list):
                        for aff in profile['affiliations']:
                            if aff:
                                aggregated['affiliations_list'].append(str(aff))
                    else:
                        aggregated['affiliations_list'].append(str(profile['affiliations']))

                # Collect numeric values
                if profile['paper_count'] is not None:
                    aggregated['paper_counts'].append(int(profile['paper_count']))

                if profile['citation_count'] is not None:
                    aggregated['citation_counts'].append(int(profile['citation_count']))

                if profile['h_index'] is not None:
                    aggregated['h_indices'].append(int(profile['h_index']))

                # Track latest update
                if profile['updated_at']:
                    if not aggregated['latest_update'] or profile['updated_at'] > aggregated['latest_update']:
                        aggregated['latest_update'] = profile['updated_at']

            # Return final aggregated values
            return {
                's2_name': ','.join(aggregated['names']) if aggregated['names'] else None,
                's2_homepage': aggregated['urls'][0] if aggregated['urls'] else None,
                's2_affiliations': list(set(aggregated['affiliations_list'])) if aggregated['affiliations_list'] else None,
                's2_paper_count': sum(aggregated['paper_counts']) if aggregated['paper_counts'] else None,
                's2_citation_count': sum(aggregated['citation_counts']) if aggregated['citation_counts'] else None,
                's2_h_index': max(aggregated['h_indices']) if aggregated['h_indices'] else None,
                's2_data_updated': aggregated['latest_update']
            }

        except Exception as e:
            self.logger.error(f"Error fetching/aggregating S2 data for IDs {unique_ids}: {e}")
            return {}

    def _sync_single_author(self, author_record: Dict) -> bool:
        """
        Sync a single author record with cached S2 data (supports comma-separated s2_author_ids)

        Args:
            author_record: Author data from query

        Returns:
            True if successful, False otherwise
        """
        try:
            # Split comma-separated s2_author_ids and fetch aggregated S2 data
            s2_author_id_str = str(author_record['s2_author_id']).strip()
            s2_author_ids = [id_.strip() for id_ in s2_author_id_str.split(',') if id_.strip()]

            if not s2_author_ids:
                self.logger.debug(f"No valid S2 author IDs for {author_record['dblp_author_name']}")
                return False

            # Fetch and aggregate S2 data from multiple profiles
            s2_data = self._fetch_and_aggregate_s2_data(s2_author_ids)

            if not s2_data:
                self.logger.debug(f"No S2 data found for IDs {s2_author_ids} for author {author_record['dblp_author_name']}")
                return False

            # Build update data using aggregated S2 data
            update_data = {}
            updates_made = []  # Track what we're updating for logging

            # Homepage URL - use S2 data if author_profiles field is empty
            if s2_data.get('s2_homepage') and not author_record['current_homepage']:
                update_data['homepage'] = s2_data['s2_homepage']
                updates_made.append(f"homepage: '{s2_data['s2_homepage']}'")

            # Affiliations (convert list to comma-separated string)
            if s2_data.get('s2_affiliations') and not author_record['current_s2_affiliations']:
                try:
                    if isinstance(s2_data['s2_affiliations'], list):
                        affiliations_str = ','.join([str(aff) for aff in s2_data['s2_affiliations'] if aff])
                    else:
                        affiliations_str = str(s2_data['s2_affiliations'])

                    if affiliations_str:
                        update_data['s2_affiliations'] = affiliations_str
                        updates_made.append(f"s2_affiliations: '{affiliations_str[:50]}{'...' if len(affiliations_str) > 50 else ''}'")
                except Exception as e:
                    self.logger.warning(f"Error processing affiliations for {author_record['dblp_author_name']}: {e}")

            # Paper count - use aggregated S2 data if author_profiles field is empty
            if s2_data.get('s2_paper_count') is not None and author_record['current_s2_paper_count'] is None:
                update_data['s2_paper_count'] = s2_data['s2_paper_count']
                updates_made.append(f"s2_paper_count: {s2_data['s2_paper_count']}")

            # Citation count - use aggregated S2 data if author_profiles field is empty
            if s2_data.get('s2_citation_count') is not None and author_record['current_s2_citation_count'] is None:
                update_data['s2_citation_count'] = s2_data['s2_citation_count']
                updates_made.append(f"s2_citation_count: {s2_data['s2_citation_count']}")

            # H-index - use aggregated S2 data if author_profiles field is empty
            if s2_data.get('s2_h_index') is not None and author_record['current_s2_h_index'] is None:
                update_data['s2_h_index'] = s2_data['s2_h_index']
                updates_made.append(f"s2_h_index: {s2_data['s2_h_index']}")

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
                if updates_made:
                    self.logger.debug(f"Successfully synced author {author_record['dblp_author_name']} - Updated: {', '.join(updates_made)}")
                else:
                    self.logger.debug(f"No updates needed for author {author_record['dblp_author_name']}")
                return True
            else:
                self.logger.error(f"Failed to sync author {author_record['id']} - Query execution failed")
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

            # Authors that can be synced (consistent with main sync query)
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

    def _update_stats_thread_safe(self, stats: Dict, sync_result: bool, author_name: str):
        """Thread-safe statistics update"""
        with self._stats_lock:
            stats['total_authors_processed'] += 1
            if sync_result:
                stats['authors_synced'] += 1

            # Progress logging every 10 records
            if stats['total_authors_processed'] % 10 == 0:
                total = stats.get('total_authors', 0)
                self.logger.info(f"Processed {stats['total_authors_processed']}/{total} authors")