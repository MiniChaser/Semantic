#!/usr/bin/env python3
"""
Authorship Pandas Service
Optimized version of authorship creation using pandas for batch processing
Reduces database queries and improves performance while ensuring data completeness
"""

import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ...database.connection import DatabaseManager
from .author_disambiguation_service import AuthorMatcher


logger = logging.getLogger(__name__)


class AuthorshipPandasService:
    """
    Optimized service for creating authorships table using pandas

    This service improves upon the original by:
    1. Loading all papers data in a single query instead of pagination
    2. Using pandas for efficient data processing and author matching
    3. Batch inserting all results to minimize database interactions
    4. Processing ALL papers with semantic_authors (fixing data completeness issue)
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.matcher = AuthorMatcher()

        # Data containers for efficient processing
        self.papers_df: Optional[pd.DataFrame] = None
        self.authorships_df: Optional[pd.DataFrame] = None

    def create_authorships_table(self) -> bool:
        """
        Create the authorships table for paper-author relationships
        Uses the same schema as the original service for compatibility

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating authorships table...")

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS authorships (
                id SERIAL PRIMARY KEY,
                paper_id INTEGER REFERENCES enriched_papers(id),
                semantic_paper_id TEXT,
                paper_title TEXT,
                dblp_author_name TEXT NOT NULL,
                s2_author_name TEXT,
                s2_author_id TEXT,
                authorship_order INTEGER,
                match_confidence VARCHAR(50) DEFAULT 'medium',
                match_method VARCHAR(100),
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """

            self.db_manager.execute_query(create_table_sql)

            # Create indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_authorships_paper_id ON authorships(paper_id);",
                "CREATE INDEX IF NOT EXISTS idx_authorships_semantic_paper_id ON authorships(semantic_paper_id);",
                "CREATE INDEX IF NOT EXISTS idx_authorships_dblp_author ON authorships(dblp_author_name);",
                "CREATE INDEX IF NOT EXISTS idx_authorships_s2_author_id ON authorships(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_authorships_order ON authorships(authorship_order);"
            ]

            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)

            logger.info("Authorships table created successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to create authorships table: {e}")
            return False

    def load_all_papers_data(self) -> bool:
        """
        Load ALL papers with semantic_authors data in a single query
        This fixes the data completeness issue where only ~4000/36976 papers were processed

        Returns:
            True if data loaded successfully, False otherwise
        """
        try:
            logger.info("Loading all papers with semantic_authors data...")

            # Load ALL papers with both dblp_authors and semantic_authors in one query
            papers_query = """
            SELECT
                id, semantic_paper_id, dblp_title,
                dblp_authors, semantic_authors
            FROM enriched_papers
            WHERE semantic_authors IS NOT NULL
              AND dblp_authors IS NOT NULL
            ORDER BY id
            """

            papers_data = self.db_manager.fetch_all(papers_query)

            if not papers_data:
                logger.warning("No papers data found with both author sources")
                return False

            # Convert to pandas DataFrame for efficient processing
            self.papers_df = pd.DataFrame(papers_data)
            logger.info(f"Loaded {len(self.papers_df)} papers with complete author data")

            return True

        except Exception as e:
            logger.error(f"Failed to load papers data: {e}")
            return False

    def process_author_matching_pandas(self) -> pd.DataFrame:
        """
        Process author matching using pandas for efficient batch processing

        Returns:
            DataFrame with processed authorship records
        """
        if self.papers_df is None or self.papers_df.empty:
            logger.error("No papers data available for processing")
            return pd.DataFrame()

        logger.info("Processing author matching with pandas optimization...")

        authorships_list = []
        processed_count = 0
        error_count = 0

        # Process each paper - this could be further optimized with vectorization
        for _, paper in self.papers_df.iterrows():
            try:
                processed_count += 1

                # Parse author data
                dblp_authors = paper['dblp_authors'] if paper['dblp_authors'] else []
                s2_authors = paper['semantic_authors'] if paper['semantic_authors'] else []

                # Ensure s2_authors is a list of dicts
                if isinstance(s2_authors, str):
                    s2_authors = json.loads(s2_authors)

                if not dblp_authors or not s2_authors:
                    continue

                # Perform author matching using existing AuthorMatcher
                matched_pairs, unmatched_dblp = self.matcher.match_authors_enhanced(
                    dblp_authors, s2_authors
                )

                authorship_order = 1

                # Process matched authors
                for dblp_name, s2_author in matched_pairs.items():
                    authorship_record = {
                        'paper_id': paper['id'],
                        'semantic_paper_id': paper['semantic_paper_id'],
                        'paper_title': paper['dblp_title'],
                        'dblp_author_name': dblp_name,
                        's2_author_name': s2_author['name'],
                        's2_author_id': s2_author['authorId'],
                        'authorship_order': authorship_order,
                        'match_confidence': 'matched',
                        'match_method': 'multi_tier_matching'
                    }
                    authorships_list.append(authorship_record)
                    authorship_order += 1

                # Process unmatched authors
                for dblp_name in unmatched_dblp:
                    authorship_record = {
                        'paper_id': paper['id'],
                        'semantic_paper_id': paper['semantic_paper_id'],
                        'paper_title': paper['dblp_title'],
                        'dblp_author_name': dblp_name,
                        's2_author_name': None,
                        's2_author_id': None,
                        'authorship_order': authorship_order,
                        'match_confidence': 'unmatched',
                        'match_method': 'no_match_found'
                    }
                    authorships_list.append(authorship_record)
                    authorship_order += 1

                # Progress logging
                if processed_count % 5000 == 0:
                    logger.info(f"Processed {processed_count}/{len(self.papers_df)} papers...")

            except Exception as e:
                error_count += 1
                logger.error(f"Error processing paper {paper.get('id')}: {e}")
                continue

        # Convert to DataFrame
        self.authorships_df = pd.DataFrame(authorships_list)

        logger.info(f"Author matching completed: {len(self.authorships_df)} authorships created")
        logger.info(f"Papers processed: {processed_count}, Errors: {error_count}")

        return self.authorships_df

    def batch_insert_authorships_pandas(self) -> bool:
        """
        High-performance batch insert using pandas.to_sql
        Replaces the previous slow loop-based approach with direct pandas insertion

        Returns:
            True if successful, False otherwise
        """
        if self.authorships_df is None or self.authorships_df.empty:
            logger.warning("No authorships data to insert")
            return True

        try:
            logger.info(f"High-performance inserting {len(self.authorships_df)} authorships using pandas.to_sql...")

            # Import SQLAlchemy for pandas.to_sql
            try:
                from sqlalchemy import create_engine
            except ImportError:
                logger.error("SQLAlchemy not installed. Please install with: pip install sqlalchemy>=1.4.0")
                return self._fallback_to_batch_insert()

            # Clear existing data first
            self.db_manager.execute_query("DELETE FROM authorships;")
            logger.info("Cleared existing authorships data")

            # Prepare DataFrame for insertion
            insert_df = self._prepare_dataframe_for_insertion()

            # Create database engine for pandas.to_sql
            connection_string = self.db_manager.config.get_connection_string()
            engine = create_engine(connection_string)

            start_time = datetime.now()

            # High-performance single-operation insert
            insert_df.to_sql(
                name='authorships',
                con=engine,
                if_exists='append',      # Append to existing table
                index=False,             # Don't insert DataFrame index
                method='multi',          # Use multi-row INSERT for better performance
                chunksize=5000          # Process in reasonable chunks
            )

            end_time = datetime.now()
            insertion_time = (end_time - start_time).total_seconds()

            logger.info(f"Successfully inserted all {len(insert_df)} authorships using pandas.to_sql")
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
        insert_df = self.authorships_df.copy()

        # Ensure proper data types for PostgreSQL compatibility
        insert_df['paper_id'] = insert_df['paper_id'].astype('int64')
        insert_df['authorship_order'] = insert_df['authorship_order'].astype('int32')

        # Handle None/null values properly
        insert_df['s2_author_name'] = insert_df['s2_author_name'].fillna('')
        insert_df['s2_author_id'] = insert_df['s2_author_id'].fillna('')

        # Select only the columns that exist in the database table
        columns_to_insert = [
            'paper_id', 'semantic_paper_id', 'paper_title',
            'dblp_author_name', 's2_author_name', 's2_author_id',
            'authorship_order', 'match_confidence', 'match_method'
        ]

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
            INSERT INTO authorships (
                paper_id, semantic_paper_id, paper_title,
                dblp_author_name, s2_author_name, s2_author_id,
                authorship_order, match_confidence, match_method
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            batch_size = 2000
            total_inserted = 0

            # Process in batches
            for i in range(0, len(self.authorships_df), batch_size):
                batch_df = self.authorships_df.iloc[i:i+batch_size]

                # Convert batch to list of tuples for insertion
                batch_values = []
                for _, row in batch_df.iterrows():
                    values = (
                        row['paper_id'],
                        row['semantic_paper_id'],
                        row['paper_title'],
                        row['dblp_author_name'],
                        row['s2_author_name'] if pd.notna(row['s2_author_name']) else None,
                        row['s2_author_id'] if pd.notna(row['s2_author_id']) else None,
                        row['authorship_order'],
                        row['match_confidence'],
                        row['match_method']
                    )
                    batch_values.append(values)

                # Batch insert
                if self.db_manager.execute_batch_query(insert_sql, batch_values):
                    total_inserted += len(batch_values)
                    if total_inserted % 10000 == 0:  # Less frequent logging
                        logger.info(f"Inserted: {total_inserted}/{len(self.authorships_df)} authorships")
                else:
                    logger.error(f"Failed to insert batch starting at index {i}")
                    return False

            logger.info(f"Successfully inserted all {total_inserted} authorships using fallback method")
            return True

        except Exception as e:
            logger.error(f"Fallback batch insert failed: {e}")
            return False

    def populate_authorships_table_pandas(self) -> Dict:
        """
        Main method to populate authorships table using pandas optimization

        Returns:
            Statistics about the population process
        """
        try:
            logger.info("Starting pandas-optimized authorships table population...")
            start_time = datetime.now()

            # Step 1: Load all papers data
            if not self.load_all_papers_data():
                return {'error': 'Failed to load papers data'}

            # Step 2: Process author matching
            authorships_df = self.process_author_matching_pandas()
            if authorships_df.empty:
                return {'error': 'No authorships generated from author matching'}

            # Step 3: Batch insert results
            if not self.batch_insert_authorships_pandas():
                return {'error': 'Failed to insert authorships data'}

            # Calculate statistics
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Count statistics from the processed data
            matched_count = len(authorships_df[authorships_df['match_confidence'] == 'matched'])
            unmatched_count = len(authorships_df[authorships_df['match_confidence'] == 'unmatched'])

            stats = {
                'processed_papers': len(self.papers_df),
                'total_authorships': len(authorships_df),
                'matched_authors': matched_count,
                'unmatched_authors': unmatched_count,
                'processing_time_seconds': processing_time,
                'optimization_method': 'pandas_batch_processing',
                'data_completeness': 'full_coverage'
            }

            logger.info("Pandas-optimized authorships population completed successfully")
            return stats

        except Exception as e:
            logger.error(f"Failed to populate authorships table: {e}")
            return {'error': str(e)}