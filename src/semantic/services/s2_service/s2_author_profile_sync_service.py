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
import pandas as pd

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
        Sync S2 author data from s2_author_profiles to author_profiles table using bulk DataFrame operations

        Args:
            limit: Maximum number of authors to process

        Returns:
            Sync statistics
        """
        start_time = time.time()
        self.logger.info("Starting S2 author profile sync using bulk DataFrame operations...")

        try:
            # Load all author profiles into DataFrame
            self.logger.info("Loading author_profiles table into memory...")
            author_profiles_query = "SELECT * FROM author_profiles"

            author_profiles_data = self.db_manager.fetch_all(author_profiles_query)
            if not author_profiles_data:
                self.logger.info("No author profiles found")
                return {
                    'total_authors_processed': 0,
                    'authors_synced': 0,
                    'errors': 0,
                    'processing_time': time.time() - start_time
                }

            author_profiles_df = pd.DataFrame(author_profiles_data)
            self.logger.info(f"Loaded {len(author_profiles_df)} author profiles")

            # Load all S2 author profiles into DataFrame
            self.logger.info("Loading s2_author_profiles table into memory...")
            s2_profiles_data = self.db_manager.fetch_all("SELECT * FROM s2_author_profiles")
            if not s2_profiles_data:
                self.logger.info("No S2 profiles found for enrichment")
                return {
                    'total_authors_processed': 0,
                    'authors_synced': 0,
                    'errors': 0,
                    'processing_time': time.time() - start_time
                }

            s2_profiles_df = pd.DataFrame(s2_profiles_data)
            self.logger.info(f"Loaded {len(s2_profiles_df)} S2 profiles")

            # Process data enrichment in memory
            authors_synced = self._enrich_author_profiles_bulk(author_profiles_df, s2_profiles_df)

            # Write the enriched data back to database (overwrite entire table)
            self.logger.info("Writing enriched author profiles back to database...")
            # Ensure correct column order and data types for database compatibility
            author_profiles_df['updated_at'] = pd.Timestamp.now()

            # Use pandas to_sql to replace the entire table
            from sqlalchemy import create_engine
            engine = create_engine(self.db_manager.config.get_connection_string())
            author_profiles_df.to_sql('author_profiles', engine, if_exists='replace', index=False, method='multi')

            processing_time = time.time() - start_time

            self.logger.info(f"S2 author profile bulk sync completed. Enriched {authors_synced} authors in {processing_time:.2f} seconds.")

            return {
                'total_authors_processed': len(author_profiles_df),
                'authors_synced': authors_synced,
                'errors': 0,
                'processing_time': processing_time
            }

        except Exception as e:
            self.logger.error(f"S2 author profile bulk sync failed: {e}")
            return {
                'error': str(e),
                'processing_time': time.time() - start_time
            }

    def _enrich_author_profiles_bulk(self, author_profiles_df: pd.DataFrame, s2_profiles_df: pd.DataFrame) -> int:
        """
        Enrich author profiles with S2 data using DataFrame operations

        Args:
            author_profiles_df: DataFrame containing all author profiles
            s2_profiles_df: DataFrame containing all S2 profiles

        Returns:
            Number of authors that were enriched
        """
        self.logger.info("Processing author profile enrichment in memory...")

        authors_synced = 0

        # Filter authors that need S2 enrichment and have S2 IDs
        mask = (
            author_profiles_df['s2_author_id'].notna() &
            (author_profiles_df['s2_author_id'] != '') &
            (
                author_profiles_df['homepage'].isna() |
                author_profiles_df['s2_affiliations'].isna() |
                author_profiles_df['s2_paper_count'].isna() |
                author_profiles_df['s2_citation_count'].isna() |
                author_profiles_df['s2_h_index'].isna()
            )
        )

        authors_to_enrich = author_profiles_df[mask].copy()

        if authors_to_enrich.empty:
            self.logger.info("No authors need S2 enrichment")
            return 0

        self.logger.info(f"Found {len(authors_to_enrich)} authors that need S2 enrichment")

        # Process each author that needs enrichment
        for idx, author_row in authors_to_enrich.iterrows():
            try:
                # Handle comma-separated S2 author IDs
                s2_author_id_str = str(author_row['s2_author_id']).strip()
                s2_author_ids = [id_.strip() for id_ in s2_author_id_str.split(',') if id_.strip()]

                if not s2_author_ids:
                    continue

                # Get matching S2 profiles for this author
                matching_s2_profiles = s2_profiles_df[s2_profiles_df['s2_author_id'].isin(s2_author_ids)]

                if matching_s2_profiles.empty:
                    continue

                # Aggregate S2 data from multiple profiles
                aggregated_data = self._aggregate_s2_data(matching_s2_profiles)

                # Apply enrichment only to empty fields
                updated = False

                if (pd.isna(author_row['homepage']) or author_row['homepage'] is None) and aggregated_data.get('homepage'):
                    author_profiles_df.at[idx, 'homepage'] = aggregated_data['homepage']
                    updated = True

                if (pd.isna(author_row['s2_affiliations']) or author_row['s2_affiliations'] is None) and aggregated_data.get('affiliations'):
                    author_profiles_df.at[idx, 's2_affiliations'] = aggregated_data['affiliations']
                    updated = True

                if (pd.isna(author_row['s2_paper_count']) or author_row['s2_paper_count'] is None) and aggregated_data.get('paper_count') is not None:
                    author_profiles_df.at[idx, 's2_paper_count'] = aggregated_data['paper_count']
                    updated = True

                if (pd.isna(author_row['s2_citation_count']) or author_row['s2_citation_count'] is None) and aggregated_data.get('citation_count') is not None:
                    author_profiles_df.at[idx, 's2_citation_count'] = aggregated_data['citation_count']
                    updated = True

                if (pd.isna(author_row['s2_h_index']) or author_row['s2_h_index'] is None) and aggregated_data.get('h_index') is not None:
                    author_profiles_df.at[idx, 's2_h_index'] = aggregated_data['h_index']
                    updated = True

                if updated:
                    authors_synced += 1

            except Exception as e:
                self.logger.error(f"Error enriching author {author_row.get('dblp_author_name', 'unknown')}: {e}")

        self.logger.info(f"Successfully enriched {authors_synced} authors")
        return authors_synced

    def _aggregate_s2_data(self, s2_profiles: pd.DataFrame) -> Dict:
        """
        Aggregate S2 data from multiple profiles for the same author

        Args:
            s2_profiles: DataFrame containing S2 profiles for one author

        Returns:
            Dictionary with aggregated S2 data
        """
        if s2_profiles.empty:
            return {}

        # Sort by updated_at to get most recent data first
        s2_profiles = s2_profiles.sort_values('updated_at', ascending=False)

        # Get the first non-null homepage URL
        homepage = None
        for url in s2_profiles['url'].dropna():
            if url:
                homepage = str(url)
                break

        # Aggregate affiliations (combine all unique affiliations)
        affiliations_list = []
        for affiliations in s2_profiles['affiliations'].dropna():
            if affiliations:
                if isinstance(affiliations, list):
                    affiliations_list.extend([str(aff) for aff in affiliations if aff])
                else:
                    affiliations_list.append(str(affiliations))

        affiliations_str = ','.join(list(set(affiliations_list))) if affiliations_list else None

        # Sum paper counts and citation counts
        paper_count = s2_profiles['paper_count'].fillna(0).sum()
        paper_count = int(paper_count) if paper_count > 0 else None

        citation_count = s2_profiles['citation_count'].fillna(0).sum()
        citation_count = int(citation_count) if citation_count > 0 else None

        # Take maximum H-index
        h_index = s2_profiles['h_index'].fillna(0).max()
        h_index = int(h_index) if h_index > 0 else None

        return {
            'homepage': homepage,
            'affiliations': affiliations_str,
            'paper_count': paper_count,
            'citation_count': citation_count,
            'h_index': h_index
        }

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

