#!/usr/bin/env python3
"""
Final Author Table Pandas Service
Optimized version of final author table creation using pandas for batch processing
Reduces database queries from tens of thousands to just 3-5 queries
"""

import logging
import pandas as pd
import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ...database.connection import DatabaseManager


logger = logging.getLogger(__name__)


class FinalAuthorTablePandasService:
    """
    Optimized service for creating final author table using pandas batch processing

    This service improves upon the original by:
    1. Loading all required data in 3-5 queries instead of thousands
    2. Using pandas vectorized operations for efficient calculations
    3. Batch inserting all results using pandas.to_sql for maximum performance
    4. Eliminating the N+1 query problem completely
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

        # Data containers for efficient processing
        self.authors_df: Optional[pd.DataFrame] = None
        self.authorships_df: Optional[pd.DataFrame] = None
        self.papers_df: Optional[pd.DataFrame] = None
        self.final_authors_df: Optional[pd.DataFrame] = None

    def create_final_author_table(self) -> bool:
        """
        Create the final author table matching document requirements
        Uses the same schema as the original service for compatibility

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating final author table...")

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS final_author_table (
                id SERIAL PRIMARY KEY,

                -- Core identification
                dblp_author VARCHAR(500) NOT NULL,
                note VARCHAR(500),  -- Currently empty as specified

                -- External IDs (TODO sections as specified)
                google_scholarid VARCHAR(255),  -- TODO: Google Scholar integration needed
                external_ids_dblp TEXT,         -- DBLP aliases based on 4-digit disambiguation

                -- Institution information
                semantic_scholar_affiliations TEXT,  -- TODO: S2 Author API needed
                csrankings_affiliation TEXT,         -- TODO: CSRankings data integration needed

                -- Publication statistics
                dblp_top_paper_total_paper_captured INTEGER DEFAULT 0,  -- TODO: Top venue definition needed
                dblp_top_paper_last_author_count INTEGER DEFAULT 0,     -- TODO: Top venue definition needed
                first_author_count INTEGER DEFAULT 0,
                semantic_scholar_paper_count INTEGER,    -- COUNT of papers from authorships+enriched_papers

                -- Career metrics
                career_length INTEGER DEFAULT 0,
                last_author_percentage INTEGER DEFAULT 0, -- TODO: = (dblp_top_paper_last_author_count / dblp_top_paper_total_paper_captured) * 100

                -- Citation and impact metrics
                total_influential_citations INTEGER,     -- Sum of influentialCitationCount from enriched_papers
                semantic_scholar_citation_count INTEGER, -- Sum of semantic_citation_count from enriched_papers
                semantic_scholar_h_index INTEGER,        -- Calculated H-index

                -- Naming and identity
                name VARCHAR(500) NOT NULL,
                name_snapshot VARCHAR(500),
                affiliations_snapshot TEXT,  -- Currently empty as specified
                homepage TEXT,               -- TODO: S2 Author API integration needed

                -- Internal tracking
                s2_author_id TEXT,   -- Internal reference to author_profiles (can be comma-separated)
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """

            self.db_manager.execute_query(create_table_sql)

            # Add table comment
            comment_sql = "COMMENT ON TABLE final_author_table IS 'Final output (step3)';"
            self.db_manager.execute_query(comment_sql)

            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_final_author_dblp_name ON final_author_table(dblp_author);",
                "CREATE INDEX IF NOT EXISTS idx_final_author_s2_id ON final_author_table(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_final_author_career_length ON final_author_table(career_length DESC);",
                "CREATE INDEX IF NOT EXISTS idx_final_author_citations ON final_author_table(semantic_scholar_citation_count DESC);",
            ]

            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)

            logger.info("Final author table created successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to create final author table: {e}")
            return False

    def load_all_data(self) -> bool:
        """
        Load all required data in just 3 queries for pandas processing
        Eliminates the N+1 query problem by loading everything upfront

        Returns:
            True if data loaded successfully, False otherwise
        """
        try:
            logger.info("Loading all data for pandas processing...")

            # Query 1: Load author profiles data 
            authors_query = """
            SELECT
                p.s2_author_id,
                p.dblp_author_name,
                p.s2_author_name,
                p.paper_count,
                p.total_citations,
                p.career_length,
                p.first_author_count,
                p.last_author_count,
                p.first_author_ratio,
                p.last_author_ratio
            FROM author_profiles p
            WHERE p.dblp_author_name IS NOT NULL
            ORDER BY p.paper_count DESC
            """

            authors_data = self.db_manager.fetch_all(authors_query)
            if not authors_data:
                logger.warning("No author profiles data found")
                return False

            self.authors_df = pd.DataFrame(authors_data)
            logger.info(f"Loaded {len(self.authors_df)} author profiles")

            # Query 2: Load authorships data for all authors
            author_ids = list(self.authors_df[
                (self.authors_df['s2_author_id'].notna()) &
                (self.authors_df['s2_author_id'] != '')
            ]['s2_author_id'])

            # Split author IDs and create a comprehensive list
            all_author_ids = set()
            for author_ids_str in author_ids:
                if author_ids_str and author_ids_str.strip():
                    ids = [aid.strip() for aid in str(author_ids_str).split(',') if aid.strip()]
                    all_author_ids.update(ids)

            all_author_ids = list(all_author_ids)
            logger.info(f"Found {len(all_author_ids)} unique author IDs")

            if all_author_ids:
                # Create placeholders for IN query
                placeholders = ','.join(['%s'] * len(all_author_ids))
                authorships_query = f"""
                SELECT
                    a.s2_author_id,
                    a.semantic_paper_id,
                    a.dblp_author_name,
                    a.authorship_order
                FROM authorships a
                WHERE a.s2_author_id IN ({placeholders})
                AND a.s2_author_id IS NOT NULL
                """

                authorships_data = self.db_manager.fetch_all(authorships_query, tuple(all_author_ids))
                self.authorships_df = pd.DataFrame(authorships_data)
                logger.info(f"Loaded {len(self.authorships_df)} authorship records")

                # Query 3: Load enriched papers data
                paper_ids = list(self.authorships_df['semantic_paper_id'].dropna().unique())
                if paper_ids:
                    paper_placeholders = ','.join(['%s'] * len(paper_ids))
                    papers_query = f"""
                    SELECT
                        e.semantic_paper_id,
                        e.semantic_citation_count,
                        e.influentialcitationcount
                    FROM enriched_papers e
                    WHERE e.semantic_paper_id IN ({paper_placeholders})
                    AND e.semantic_paper_id IS NOT NULL
                    """

                    papers_data = self.db_manager.fetch_all(papers_query, tuple(paper_ids))
                    self.papers_df = pd.DataFrame(papers_data)
                    logger.info(f"Loaded {len(self.papers_df)} enriched papers")
            else:
                self.authorships_df = pd.DataFrame()
                self.papers_df = pd.DataFrame()

            return True

        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False

    def process_final_author_calculations_pandas(self) -> pd.DataFrame:
        """
        Process all final author calculations using pandas vectorized operations
        Replaces thousands of individual database queries with efficient pandas operations

        Returns:
            DataFrame with processed final author records
        """
        if self.authors_df is None or self.authors_df.empty:
            logger.error("No authors data available for processing")
            return pd.DataFrame()

        logger.info("Processing final author calculations with pandas optimization...")

        try:
            # Start with author profiles data
            final_df = self.authors_df.copy()

            matched_authors = final_df[
                (final_df['s2_author_id'].notna()) &
                (final_df['s2_author_id'] != '')
            ].copy()

            unmatched_authors = final_df[
                (final_df['s2_author_id'].isna()) |
                (final_df['s2_author_id'] == '')
            ].copy()

            logger.info(f"Processing {len(matched_authors)} matched authors and {len(unmatched_authors)} unmatched authors")

            if not matched_authors.empty and not self.authorships_df.empty and not self.papers_df.empty:
                # Create comprehensive dataset for calculations
                author_paper_data = self._create_author_paper_dataset()
                matched_authors = self._calculate_pandas_metrics(matched_authors, author_paper_data)
            else:
                logger.info("Using basic metrics for matched authors due to missing authorship/papers data")
                matched_authors = self._add_default_metrics(matched_authors)

            unmatched_authors = self._add_default_metrics(unmatched_authors)

            final_df = pd.concat([matched_authors, unmatched_authors], ignore_index=True)

            # Add additional fields
            final_df = self._prepare_final_author_records(final_df)

            self.final_authors_df = final_df
            logger.info(f"Final author calculations completed: {len(final_df)} records processed")

            return final_df

        except Exception as e:
            logger.error(f"Error in pandas calculations: {e}")
            return pd.DataFrame()

    def _create_author_paper_dataset(self) -> pd.DataFrame:
        """
        Create a comprehensive dataset by merging authorships with papers
        This enables vectorized calculations across all author-paper relationships
        """
        # Merge authorships with papers
        author_papers = self.authorships_df.merge(
            self.papers_df,
            on='semantic_paper_id',
            how='left'
        )

        # Fill missing values
        author_papers['semantic_citation_count'] = author_papers['semantic_citation_count'].fillna(0)
        author_papers['influentialcitationcount'] = author_papers['influentialcitationcount'].fillna(0)

        return author_papers

    def _calculate_pandas_metrics(self, final_df: pd.DataFrame, author_paper_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all metrics using pandas groupby and vectorized operations
        This replaces individual database queries with efficient batch processing
        """
        logger.info("Calculating metrics using pandas vectorized operations...")

        # Group by author ID for efficient calculations
        author_metrics = author_paper_data.groupby('s2_author_id').agg({
            'semantic_paper_id': 'nunique',  # Unique paper count
            'semantic_citation_count': 'sum',  # Total citations
            'influentialcitationcount': 'sum',  # Total influential citations
        }).reset_index()

        author_metrics.columns = [
            's2_author_id',
            'semantic_scholar_paper_count',
            'semantic_scholar_citation_count',
            'total_influential_citations'
        ]

        # Calculate H-index for each author using pandas apply
        h_index_data = author_paper_data.groupby('s2_author_id')['semantic_citation_count'].apply(
            lambda x: self._calculate_h_index_vectorized(x.tolist())
        ).reset_index()
        h_index_data.columns = ['s2_author_id', 'semantic_scholar_h_index']

        # Merge all metrics back to final dataframe
        final_df = final_df.merge(author_metrics, on='s2_author_id', how='left')
        final_df = final_df.merge(h_index_data, on='s2_author_id', how='left')

        # Fill missing values
        final_df['semantic_scholar_paper_count'] = final_df['semantic_scholar_paper_count'].fillna(0)
        final_df['semantic_scholar_citation_count'] = final_df['semantic_scholar_citation_count'].fillna(0)
        final_df['total_influential_citations'] = final_df['total_influential_citations'].fillna(0)
        final_df['semantic_scholar_h_index'] = final_df['semantic_scholar_h_index'].fillna(0)

        return final_df

    def _calculate_h_index_vectorized(self, citations: List[int]) -> int:
        """
        Calculate H-index from a list of citation counts
        Vectorized version of the H-index calculation
        """
        if not citations:
            return 0

        # Sort citations in descending order
        sorted_citations = sorted([c for c in citations if c is not None and c > 0], reverse=True)

        h_index = 0
        for i, citation_count in enumerate(sorted_citations, 1):
            if citation_count >= i:
                h_index = i
            else:
                break

        return h_index

    def _add_default_metrics(self, final_df: pd.DataFrame) -> pd.DataFrame:
        """Add default metric values when no authorship/papers data is available"""
        final_df['semantic_scholar_paper_count'] = 0
        final_df['semantic_scholar_citation_count'] = 0
        final_df['total_influential_citations'] = 0
        final_df['semantic_scholar_h_index'] = 0
        return final_df

    def _prepare_final_author_records(self, final_df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the final author records with all required fields
        Uses vectorized operations instead of row-by-row processing
        """
        logger.info("Preparing final author records...")

        # Extract DBLP aliases using vectorized string operations
        final_df['external_ids_dblp'] = final_df['dblp_author_name'].apply(
            self._extract_dblp_aliases_vectorized
        )

        # Add all required fields
        final_df['dblp_author'] = final_df['dblp_author_name']
        final_df['note'] = ''  # Empty as specified
        final_df['google_scholarid'] = None  # TODO: Google Scholar integration
        final_df['semantic_scholar_affiliations'] = None  # TODO: S2 Author API needed
        final_df['csrankings_affiliation'] = None  # TODO: CSRankings integration
        final_df['dblp_top_paper_total_paper_captured'] = 0  # TODO: Top venue definition
        final_df['dblp_top_paper_last_author_count'] = 0  # TODO: Top venue definition
        final_df['last_author_percentage'] = None  # TODO: Calculate from top papers
        final_df['name'] = final_df['dblp_author_name']
        final_df['name_snapshot'] = final_df['dblp_author_name']
        final_df['affiliations_snapshot'] = ''  # Empty as specified
        final_df['homepage'] = None  # TODO: S2 Author API


        # Ensure proper data types
        final_df['first_author_count'] = final_df['first_author_count'].fillna(0).astype('int32')
        final_df['career_length'] = final_df['career_length'].fillna(0).astype('int32')
        final_df['semantic_scholar_paper_count'] = final_df['semantic_scholar_paper_count'].astype('int32')
        final_df['semantic_scholar_citation_count'] = final_df['semantic_scholar_citation_count'].astype('int32')
        final_df['total_influential_citations'] = final_df['total_influential_citations'].astype('int32')
        final_df['semantic_scholar_h_index'] = final_df['semantic_scholar_h_index'].astype('int32')

        return final_df

    def _extract_dblp_aliases_vectorized(self, author_name: str) -> str:
        """
        Vectorized version of DBLP alias extraction
        For now, returns the original name (TODO: implement full alias lookup)
        """
        if pd.isna(author_name):
            return ''

        # Look for 4-digit disambiguation numbers
        match = re.search(r'\b(\d{4})\b', str(author_name))
        if match:
            # For now, just return original name
            # TODO: Implement comprehensive alias lookup using pandas operations
            return str(author_name)
        else:
            return str(author_name)


    def batch_insert_final_authors_pandas(self) -> bool:
        """
        High-performance batch insert using pandas.to_sql
        Replaces the slow loop-based individual INSERT approach

        Returns:
            True if successful, False otherwise
        """
        if self.final_authors_df is None or self.final_authors_df.empty:
            logger.warning("No final authors data to insert")
            return True

        try:
            logger.info(f"High-performance inserting {len(self.final_authors_df)} final authors using pandas.to_sql...")

            # Import SQLAlchemy for pandas.to_sql
            try:
                from sqlalchemy import create_engine
            except ImportError:
                logger.error("SQLAlchemy not installed. Please install with: pip install sqlalchemy>=1.4.0")
                return self._fallback_to_batch_insert()

            # Clear existing data first
            self.db_manager.execute_query("DELETE FROM final_author_table;")
            logger.info("Cleared existing final author table data")

            # Prepare DataFrame for insertion
            insert_df = self._prepare_dataframe_for_insertion()

            # Create database engine for pandas.to_sql
            connection_string = self.db_manager.config.get_connection_string()
            engine = create_engine(connection_string)

            start_time = datetime.now()

            # High-performance single-operation insert
            insert_df.to_sql(
                name='final_author_table',
                con=engine,
                if_exists='append',      # Append to existing table
                index=False,             # Don't insert DataFrame index
                method='multi',          # Use multi-row INSERT for better performance
                chunksize=2000          # Process in reasonable chunks
            )

            end_time = datetime.now()
            insertion_time = (end_time - start_time).total_seconds()

            logger.info(f"Successfully inserted all {len(insert_df)} final authors using pandas.to_sql")
            logger.info(f"Insertion completed in {insertion_time:.2f} seconds")

            # Close the engine
            engine.dispose()
            return True

        except Exception as e:
            logger.error(f"pandas.to_sql insertion failed: {e}")
            logger.info("Falling back to traditional batch insert method...")
            return self._fallback_to_batch_insert()

    def _prepare_dataframe_for_insertion(self) -> pd.DataFrame:
        """
        Prepare DataFrame for insertion with proper data types and column mapping

        Returns:
            DataFrame ready for insertion
        """
        insert_df = self.final_authors_df.copy()

        # Select only the columns that exist in the database table
        columns_to_insert = [
            'dblp_author', 'note', 'google_scholarid', 'external_ids_dblp',
            'semantic_scholar_affiliations', 'csrankings_affiliation',
            'dblp_top_paper_total_paper_captured', 'dblp_top_paper_last_author_count',
            'first_author_count', 'semantic_scholar_paper_count', 'career_length',
            'last_author_percentage', 'total_influential_citations',
            'semantic_scholar_citation_count', 'semantic_scholar_h_index',
            'name', 'name_snapshot', 'affiliations_snapshot', 'homepage',
            's2_author_id'
        ]

        # Ensure all required columns exist
        for col in columns_to_insert:
            if col not in insert_df.columns:
                if col in ['last_author_percentage']:
                    insert_df[col] = None
                else:
                    insert_df[col] = 0 if 'count' in col else ''

        # Handle None/null values properly for text fields
        text_columns = ['note', 'google_scholarid', 'external_ids_dblp',
                       'semantic_scholar_affiliations', 'csrankings_affiliation',
                       'name', 'name_snapshot', 'affiliations_snapshot',
                       'homepage', 's2_author_id']

        for col in text_columns:
            if col in insert_df.columns:
                insert_df[col] = insert_df[col].fillna('')

        # 保持数据完整性 - 不截断任何数据
        # s2_author_id现在使用TEXT类型，支持任意长度

        return insert_df[columns_to_insert]

    def _fallback_to_batch_insert(self) -> bool:
        """
        Fallback to the original batch insert method if to_sql fails

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Using fallback batch insert method...")

            insert_sql = """
            INSERT INTO final_author_table (
                dblp_author, note, google_scholarid, external_ids_dblp,
                semantic_scholar_affiliations, csrankings_affiliation,
                dblp_top_paper_total_paper_captured, dblp_top_paper_last_author_count,
                first_author_count, semantic_scholar_paper_count, career_length,
                last_author_percentage, total_influential_citations,
                semantic_scholar_citation_count, semantic_scholar_h_index,
                name, name_snapshot, affiliations_snapshot, homepage,
                s2_author_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            batch_size = 1000
            total_inserted = 0

            # Process in batches
            for i in range(0, len(self.final_authors_df), batch_size):
                batch_df = self.final_authors_df.iloc[i:i+batch_size]

                # Convert batch to list of tuples for insertion
                batch_values = []
                for _, row in batch_df.iterrows():
                    values = (
                        row.get('dblp_author', ''),
                        row.get('note', ''),
                        row.get('google_scholarid'),
                        row.get('external_ids_dblp', ''),
                        row.get('semantic_scholar_affiliations'),
                        row.get('csrankings_affiliation'),
                        int(row.get('dblp_top_paper_total_paper_captured', 0)),
                        int(row.get('dblp_top_paper_last_author_count', 0)),
                        int(row.get('first_author_count', 0)),
                        int(row.get('semantic_scholar_paper_count', 0)),
                        int(row.get('career_length', 0)),
                        row.get('last_author_percentage'),
                        int(row.get('total_influential_citations', 0)),
                        int(row.get('semantic_scholar_citation_count', 0)),
                        int(row.get('semantic_scholar_h_index', 0)),
                        row.get('name', ''),
                        row.get('name_snapshot', ''),
                        row.get('affiliations_snapshot', ''),
                        row.get('homepage'),
                        row.get('s2_author_id', '')
                    )
                    batch_values.append(values)

                # Batch insert
                if self.db_manager.execute_batch_query(insert_sql, batch_values):
                    total_inserted += len(batch_values)
                    if total_inserted % 5000 == 0:
                        logger.info(f"Inserted: {total_inserted}/{len(self.final_authors_df)} final authors")
                else:
                    logger.error(f"Failed to insert batch starting at index {i}")
                    return False

            logger.info(f"Successfully inserted all {total_inserted} final authors using fallback method")
            return True

        except Exception as e:
            logger.error(f"Fallback batch insert failed: {e}")
            return False

    def populate_final_author_table_pandas(self) -> Dict:
        """
        Main method to populate final author table using pandas optimization

        Returns:
            Statistics about the population process
        """
        try:
            logger.info("Starting pandas-optimized final author table population...")
            start_time = datetime.now()

            # Step 1: Load all required data in 3-5 queries
            if not self.load_all_data():
                return {'error': 'Failed to load required data'}

            # Step 2: Process all calculations using pandas
            final_df = self.process_final_author_calculations_pandas()
            if final_df.empty:
                return {'error': 'No final author records generated'}

            # Step 3: Batch insert results using to_sql
            if not self.batch_insert_final_authors_pandas():
                return {'error': 'Failed to insert final author data'}

            # Calculate statistics
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Count statistics from the processed data
            complete_data_count = len(final_df[
                (final_df['first_author_count'] > 0) |
                (final_df['career_length'] > 0) |
                (final_df['semantic_scholar_citation_count'] > 0)
            ])
            partial_data_count = len(final_df) - complete_data_count

            stats = {
                'total_authors_processed': len(final_df),
                'authors_with_complete_data': complete_data_count,
                'authors_with_partial_data': partial_data_count,
                'processing_time_seconds': processing_time,
                'optimization_method': 'pandas_batch_processing',
                'database_queries_eliminated': 'thousands_to_3-5'
            }

            logger.info("Pandas-optimized final author table population completed successfully")
            return stats

        except Exception as e:
            logger.error(f"Failed to populate final author table: {e}")
            return {'error': str(e)}

    def get_sample_records(self, limit: int = 10) -> List[Dict]:
        """Get sample records from the final author table for verification"""
        try:
            sample_records = self.db_manager.fetch_all(f"""
                SELECT
                    dblp_author, first_author_count, career_length,
                    last_author_percentage, external_ids_dblp,
                    semantic_scholar_citation_count, semantic_scholar_h_index
                FROM final_author_table
                ORDER BY semantic_scholar_citation_count DESC, first_author_count DESC
                LIMIT {limit}
            """)

            return [dict(record) for record in sample_records]

        except Exception as e:
            logger.error(f"Failed to get sample records: {e}")
            return []