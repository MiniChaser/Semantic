#!/usr/bin/env python3
"""
Author Profile Pandas Service
Optimized version of AuthorProfileService using pandas for batch processing
Reduces database queries from thousands to single-digit numbers for better performance
"""

import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional

from ...database.connection import DatabaseManager
from .author_disambiguation_service import AuthorMatcher


logger = logging.getLogger(__name__)


class AuthorProfilePandasService:
    """
    Optimized service for creating and managing author profiles using pandas

    This service reduces database queries by:
    1. Loading all required data in a single query
    2. Using pandas for data processing and aggregation
    3. Batch inserting results back to database
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.matcher = AuthorMatcher()

        # Data containers for efficient processing
        self.authorships_df: Optional[pd.DataFrame] = None
        self.papers_df: Optional[pd.DataFrame] = None
        self.author_profiles_df: Optional[pd.DataFrame] = None

    def create_author_profiles_table(self) -> bool:
        """
        Create the author_profiles table for unique author records
        Uses the same schema as the original service for compatibility

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating author_profiles table...")

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS author_profiles (
                id SERIAL PRIMARY KEY,
                s2_author_id TEXT,  -- Changed to TEXT to support comma-separated multiple IDs
                dblp_author_name TEXT NOT NULL UNIQUE,  -- Make DBLP name unique instead
                s2_author_name TEXT,  -- Can contain comma-separated names

                -- Basic statistics
                paper_count INTEGER DEFAULT 0,
                total_citations INTEGER DEFAULT 0,
                avg_citations_per_paper NUMERIC DEFAULT 0,

                -- Career information
                first_publication_year INTEGER,
                latest_publication_year INTEGER,
                career_length INTEGER DEFAULT 0,

                -- Authorship position statistics
                first_author_count INTEGER DEFAULT 0,
                last_author_count INTEGER DEFAULT 0,
                middle_author_count INTEGER DEFAULT 0,
                first_author_ratio REAL DEFAULT 0,
                last_author_ratio REAL DEFAULT 0,

                -- Affiliation and external data (for future enhancement)
                affiliation TEXT,  -- TODO: Add CSRankings integration
                homepage TEXT,     -- TODO: Add from S2 Author API
                google_scholar_id VARCHAR(255),  -- TODO: Add Google Scholar integration

                -- S2 enhanced metrics (for Phase 2)
                s2_paper_count INTEGER,     -- TODO: Fetch from S2 Author API
                s2_citation_count INTEGER,  -- TODO: Fetch from S2 Author API
                s2_h_index INTEGER,         -- TODO: Fetch from S2 Author API
                s2_affiliations TEXT,       -- TODO: Fetch from S2 Author API

                -- Calculated metrics
                contribution_score NUMERIC DEFAULT 0,
                rising_star_score NUMERIC DEFAULT 0,
                collaboration_network_strength NUMERIC DEFAULT 0,

                -- Data quality indicators
                match_confidence VARCHAR(50) DEFAULT 'medium',
                data_completeness_score NUMERIC DEFAULT 0,

                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """

            self.db_manager.execute_query(create_table_sql)

            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_author_profiles_dblp_name ON author_profiles(dblp_author_name);",
                "CREATE INDEX IF NOT EXISTS idx_author_profiles_paper_count ON author_profiles(paper_count DESC);",
                "CREATE INDEX IF NOT EXISTS idx_author_profiles_citations ON author_profiles(total_citations DESC);",
            ]

            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)

            logger.info("Author profiles table created successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to create author profiles table: {e}")
            return False

    def load_all_data(self) -> bool:
        """
        Load all required data in a single optimized query

        Returns:
            True if data loaded successfully, False otherwise
        """
        try:
            logger.info("Loading all authorships and papers data...")

            # Load all authorships with associated paper data in one query
            authorships_query = """
            SELECT
                a.id,
                a.paper_id,
                a.semantic_paper_id,
                a.paper_title,
                a.dblp_author_name,
                a.s2_author_name,
                a.s2_author_id,
                a.authorship_order,
                a.match_confidence,
                a.match_method,
                e.semantic_citation_count,
                e.semantic_year,
                e.dblp_authors,
                e.semantic_authors
            FROM authorships a
            LEFT JOIN enriched_papers e ON a.paper_id = e.id
            ORDER BY a.dblp_author_name, a.semantic_paper_id, a.authorship_order
            """

            authorships_data = self.db_manager.fetch_all(authorships_query)

            if not authorships_data:
                logger.warning("No authorships data found")
                return False

            # Convert to pandas DataFrame
            self.authorships_df = pd.DataFrame(authorships_data)
            logger.info(f"Loaded {len(self.authorships_df)} authorship records")

            # Load paper-level max authorship order information
            papers_max_order_query = """
            SELECT
                semantic_paper_id,
                MAX(authorship_order) as max_authorship_order
            FROM authorships
            WHERE semantic_paper_id IS NOT NULL
            GROUP BY semantic_paper_id
            """

            papers_max_order_data = self.db_manager.fetch_all(papers_max_order_query)
            self.papers_df = pd.DataFrame(papers_max_order_data)
            logger.info(f"Loaded max authorship order for {len(self.papers_df)} papers")

            # Merge max order information into authorships
            if not self.papers_df.empty:
                self.authorships_df = self.authorships_df.merge(
                    self.papers_df,
                    on='semantic_paper_id',
                    how='left'
                )

            return True

        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False

    def calculate_author_profiles_pandas(self) -> pd.DataFrame:
        """
        Calculate author profiles using pandas for efficient batch processing

        Returns:
            DataFrame with calculated author profiles
        """
        if self.authorships_df is None or self.authorships_df.empty:
            logger.error("No authorships data available for processing")
            return pd.DataFrame()

        logger.info("Calculating author profiles using pandas...")

        # Group by DBLP author name to ensure unique authors
        author_groups = self.authorships_df.groupby('dblp_author_name')

        profiles_list = []
        current_year = 2024

        for author_name, author_data in author_groups:
            try:
                # Basic statistics
                paper_count = len(author_data)
                total_citations = author_data['semantic_citation_count'].fillna(0).sum()
                avg_citations = total_citations / paper_count if paper_count > 0 else 0

                # Career information
                years = author_data['semantic_year'].dropna()
                first_year = int(years.min()) if not years.empty else None
                latest_year = int(years.max()) if not years.empty else None
                career_length = (latest_year - first_year + 1) if first_year and latest_year else 0

                # Authorship position analysis
                first_author_count = len(author_data[author_data['authorship_order'] == 1])

                # Calculate last author count more efficiently
                # Find papers where this author is in the last position
                last_author_count = 0
                author_papers = author_data[author_data['semantic_paper_id'].notna()]

                for _, row in author_papers.iterrows():
                    if pd.notna(row['max_authorship_order']) and row['authorship_order'] == row['max_authorship_order']:
                        last_author_count += 1

                middle_author_count = paper_count - first_author_count - last_author_count

                # Calculate ratios
                first_author_ratio = first_author_count / paper_count if paper_count > 0 else 0
                last_author_ratio = last_author_count / paper_count if paper_count > 0 else 0

                # S2 author information aggregation
                s2_author_ids = author_data['s2_author_id'].dropna().unique()
                s2_author_names = author_data['s2_author_name'].dropna().unique()

                s2_ids_str = ','.join(s2_author_ids) if len(s2_author_ids) > 0 else None
                s2_names_str = ','.join(s2_author_names) if len(s2_author_names) > 0 else None
                has_s2_id = s2_ids_str is not None

                # Calculate derived metrics
                contribution_score = (first_author_ratio * 0.4 + last_author_ratio * 0.6)

                # Rising star score (papers in recent years)
                recent_papers = len(author_data[author_data['semantic_year'] >= current_year - 3])
                rising_star_score = recent_papers / paper_count if paper_count > 0 else 0

                # Data completeness score
                completeness_factors = [
                    1 if has_s2_id else 0,
                    1 if paper_count > 0 else 0,
                    1 if total_citations > 0 else 0,
                    1 if career_length > 0 else 0,
                ]
                data_completeness = sum(completeness_factors) / len(completeness_factors)

                # Handle potential integer overflow for PostgreSQL
                safe_total_citations = min(int(total_citations), 2147483647)  # PostgreSQL INTEGER max value
                safe_paper_count = min(paper_count, 2147483647)
                safe_career_length = min(career_length, 2147483647) if career_length else None
                safe_first_year = min(first_year, 2147483647) if first_year else None
                safe_latest_year = min(latest_year, 2147483647) if latest_year else None

                profile = {
                    's2_author_id': s2_ids_str,
                    'dblp_author_name': author_name,
                    's2_author_name': s2_names_str,
                    'paper_count': safe_paper_count,
                    'total_citations': safe_total_citations,
                    'avg_citations_per_paper': float(avg_citations),
                    'first_publication_year': safe_first_year,
                    'latest_publication_year': safe_latest_year,
                    'career_length': safe_career_length,
                    'first_author_count': first_author_count,
                    'last_author_count': last_author_count,
                    'middle_author_count': middle_author_count,
                    'first_author_ratio': float(first_author_ratio),
                    'last_author_ratio': float(last_author_ratio),
                    'contribution_score': float(contribution_score),
                    'rising_star_score': float(rising_star_score),
                    'match_confidence': 'high' if has_s2_id else 'low',
                    'data_completeness_score': float(data_completeness)
                }

                profiles_list.append(profile)

            except Exception as e:
                logger.error(f"Error processing author {author_name}: {e}")
                continue

        # Convert to DataFrame
        self.author_profiles_df = pd.DataFrame(profiles_list)
        logger.info(f"Calculated profiles for {len(self.author_profiles_df)} authors")

        return self.author_profiles_df

    def batch_insert_profiles(self, profiles_df: pd.DataFrame) -> bool:
        """
        High-performance batch insert using pandas.to_sql
        Replaces the previous slow loop-based approach with direct pandas insertion

        Args:
            profiles_df: DataFrame with author profiles

        Returns:
            True if successful, False otherwise
        """
        if profiles_df.empty:
            logger.warning("No profiles to insert")
            return True

        try:
            logger.info(f"High-performance inserting {len(profiles_df)} author profiles using pandas.to_sql...")

            # Import SQLAlchemy for pandas.to_sql
            try:
                from sqlalchemy import create_engine
            except ImportError:
                logger.error("SQLAlchemy not installed. Please install with: pip install sqlalchemy>=1.4.0")
                return self._fallback_to_batch_insert(profiles_df)

            # Clear existing data first
            self.db_manager.execute_query("DELETE FROM author_profiles;")
            logger.info("Cleared existing author profiles data")

            # Prepare DataFrame for insertion
            insert_df = self._prepare_profiles_dataframe(profiles_df)

            # Create database engine for pandas.to_sql
            connection_string = self.db_manager.config.get_connection_string()
            engine = create_engine(connection_string)

            start_time = datetime.now()

            # High-performance single-operation insert
            insert_df.to_sql(
                name='author_profiles',
                con=engine,
                if_exists='append',      # Append to existing table
                index=False,             # Don't insert DataFrame index
                method='multi',          # Use multi-row INSERT for better performance
                chunksize=2000          # Process in reasonable chunks
            )

            end_time = datetime.now()
            insertion_time = (end_time - start_time).total_seconds()

            logger.info(f"Successfully inserted all {len(insert_df)} author profiles using pandas.to_sql")
            logger.info(f"Insertion completed in {insertion_time:.2f} seconds")

            # Close the engine
            engine.dispose()
            return True

        except Exception as e:
            logger.error(f"pandas.to_sql insertion failed: {e}")
            logger.info("Falling back to traditional batch insert method...")
            return self._fallback_to_batch_insert(profiles_df)

    def _prepare_profiles_dataframe(self, profiles_df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for insertion with proper data types and null handling

        Args:
            profiles_df: Original profiles DataFrame

        Returns:
            DataFrame ready for insertion
        """
        insert_df = profiles_df.copy()

        # Handle NaN/null values properly for PostgreSQL
        insert_df['s2_author_id'] = insert_df['s2_author_id'].fillna('')
        insert_df['s2_author_name'] = insert_df['s2_author_name'].fillna('')

        # Ensure proper data types for PostgreSQL compatibility
        insert_df['paper_count'] = insert_df['paper_count'].astype('int32')
        insert_df['total_citations'] = insert_df['total_citations'].astype('int64')
        insert_df['first_author_count'] = insert_df['first_author_count'].astype('int32')
        insert_df['last_author_count'] = insert_df['last_author_count'].astype('int32')
        insert_df['middle_author_count'] = insert_df['middle_author_count'].astype('int32')

        # Handle year fields - convert NaN to None for proper NULL insertion
        for year_col in ['first_publication_year', 'latest_publication_year', 'career_length']:
            insert_df[year_col] = insert_df[year_col].where(insert_df[year_col].notna(), None)
            # Use fillna with a default value and convert to int for PostgreSQL
            insert_df[year_col] = insert_df[year_col].fillna(0).astype('int32')
            # Replace 0 with None for proper NULL handling
            insert_df.loc[insert_df[year_col] == 0, year_col] = None

        # Ensure float columns are proper type
        float_cols = ['avg_citations_per_paper', 'first_author_ratio', 'last_author_ratio',
                     'contribution_score', 'rising_star_score', 'data_completeness_score']
        for col in float_cols:
            insert_df[col] = insert_df[col].astype('float64')

        # Select only the columns that exist in the database table
        columns_to_insert = [
            's2_author_id', 'dblp_author_name', 's2_author_name',
            'paper_count', 'total_citations', 'avg_citations_per_paper',
            'first_publication_year', 'latest_publication_year', 'career_length',
            'first_author_count', 'last_author_count', 'middle_author_count',
            'first_author_ratio', 'last_author_ratio',
            'contribution_score', 'rising_star_score',
            'match_confidence', 'data_completeness_score'
        ]

        return insert_df[columns_to_insert]

    def _fallback_to_batch_insert(self, profiles_df: pd.DataFrame) -> bool:
        """
        Fallback to the original batch insert method if to_sql fails

        Args:
            profiles_df: DataFrame with author profiles

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Using fallback batch insert method...")

            insert_sql = """
            INSERT INTO author_profiles (
                s2_author_id, dblp_author_name, s2_author_name,
                paper_count, total_citations, avg_citations_per_paper,
                first_publication_year, latest_publication_year, career_length,
                first_author_count, last_author_count, middle_author_count,
                first_author_ratio, last_author_ratio,
                contribution_score, rising_star_score,
                match_confidence, data_completeness_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            batch_size = 1000
            total_inserted = 0

            # Process in batches
            for i in range(0, len(profiles_df), batch_size):
                batch_df = profiles_df.iloc[i:i+batch_size]

                # Convert batch to list of tuples for insertion
                values = []
                for _, profile in batch_df.iterrows():
                    try:
                        # Handle NaN values and ensure proper data types
                        first_year = None if pd.isna(profile['first_publication_year']) else int(profile['first_publication_year'])
                        latest_year = None if pd.isna(profile['latest_publication_year']) else int(profile['latest_publication_year'])
                        career_length = None if pd.isna(profile['career_length']) else int(profile['career_length'])

                        values.append((
                            profile['s2_author_id'] if not pd.isna(profile['s2_author_id']) else None,
                            profile['dblp_author_name'],
                            profile['s2_author_name'] if not pd.isna(profile['s2_author_name']) else None,
                            int(profile['paper_count']),
                            int(profile['total_citations']),
                            float(profile['avg_citations_per_paper']),
                            first_year,
                            latest_year,
                            career_length,
                            int(profile['first_author_count']),
                            int(profile['last_author_count']),
                            int(profile['middle_author_count']),
                            float(profile['first_author_ratio']),
                            float(profile['last_author_ratio']),
                            float(profile['contribution_score']),
                            float(profile['rising_star_score']),
                            profile['match_confidence'],
                            float(profile['data_completeness_score'])
                        ))
                    except Exception as e:
                        logger.error(f"Error preparing profile data for {profile.get('dblp_author_name', 'unknown')}: {e}")
                        continue

                # Batch insert
                if values and self.db_manager.execute_batch_query(insert_sql, values):
                    total_inserted += len(values)
                    if total_inserted % 5000 == 0:  # Less frequent logging
                        logger.info(f"Inserted: {total_inserted}/{len(profiles_df)} profiles")
                else:
                    logger.error(f"Failed to insert batch starting at index {i}")
                    return False

            logger.info(f"Successfully inserted all {total_inserted} profiles using fallback method")
            return True

        except Exception as e:
            logger.error(f"Fallback batch insert failed: {e}")
            return False

    def populate_author_profiles_table(self) -> Dict:
        """
        Main method to populate author profiles using pandas optimization

        Returns:
            Statistics about the population process
        """
        try:
            logger.info("Starting optimized author profiles population with pandas...")

            # Load all required data
            if not self.load_all_data():
                return {'error': 'Failed to load data'}

            # Calculate profiles using pandas
            profiles_df = self.calculate_author_profiles_pandas()

            if profiles_df.empty:
                return {'error': 'No profiles calculated'}

            # Batch insert profiles
            if not self.batch_insert_profiles(profiles_df):
                return {'error': 'Failed to insert profiles'}

            # Calculate statistics
            stats = {
                'total_unique_authors': len(profiles_df),
                'authors_with_s2_id': len(profiles_df[profiles_df['s2_author_id'].notna()]),
                'authors_without_s2_id': len(profiles_df[profiles_df['s2_author_id'].isna()]),
                'processing_errors': 0,  # Errors were handled individually
                'total_papers_processed': len(self.authorships_df) if self.authorships_df is not None else 0,
                'optimization_method': 'pandas_batch_processing'
            }

            logger.info("Optimized author profiles population completed successfully")
            return stats

        except Exception as e:
            logger.error(f"Failed to populate author profiles: {e}")
            return {'error': str(e)}

    def get_processing_statistics(self) -> Dict:
        """Get comprehensive statistics about the pandas-optimized processing"""
        stats = {}

        # Get basic database statistics
        profiles_stats = self.db_manager.fetch_one("""
            SELECT
                COUNT(*) as total_authors,
                COUNT(CASE WHEN s2_author_id IS NOT NULL THEN 1 END) as authors_with_s2_id,
                AVG(paper_count) as avg_papers_per_author,
                AVG(total_citations) as avg_citations_per_author,
                AVG(career_length) as avg_career_length,
                AVG(first_author_ratio) as avg_first_author_ratio,
                AVG(last_author_ratio) as avg_last_author_ratio,
                AVG(contribution_score) as avg_contribution_score,
                AVG(data_completeness_score) as avg_data_completeness
            FROM author_profiles
        """)

        # Add pandas processing statistics
        pandas_stats = {
            'processing_method': 'pandas_optimized',
            'data_loaded': self.authorships_df is not None,
            'authorships_records': len(self.authorships_df) if self.authorships_df is not None else 0,
            'papers_with_max_order': len(self.papers_df) if self.papers_df is not None else 0,
            'profiles_calculated': len(self.author_profiles_df) if self.author_profiles_df is not None else 0
        }

        stats = {
            'profiles': profiles_stats,
            'pandas_processing': pandas_stats,
            'timestamp': datetime.now().isoformat()
        }

        return stats