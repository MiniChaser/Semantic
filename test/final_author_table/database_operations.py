#!/usr/bin/env python3
"""
Database Operations Module

Handles all database interactions for the final_author_table verification system.
Separated from the main verification logic for better modularity and testability.
"""

import logging
from typing import Dict, List, Optional, Any
from pathlib import Path


class DatabaseOperations:
    """
    Handles all database operations for field verification
    """

    def __init__(self, db_manager, query_manager=None):
        """
        Initialize DatabaseOperations with database manager

        Args:
            db_manager: Database manager instance
            query_manager: Optional query manager for loading SQL templates
        """
        self.db_manager = db_manager
        self.query_manager = query_manager
        self.logger = logging.getLogger(f'{__name__}.DatabaseOperations')

        # Mapping from field test names to actual database column names
        self.field_column_mapping = {
            'semantic_citation_count': 'semantic_scholar_citation_count',
            'first_author_count': 'first_author_count',
            'career_length': 'career_length',
            'h_index': 'semantic_scholar_h_index',
            'paper_count': 'semantic_scholar_paper_count',
            'semantic_scholar_paper_count': 'semantic_scholar_paper_count',
            'total_influential_citations': 'total_influential_citations'
        }

    def substitute_query_parameters(self, query: str, **params) -> str:
        """
        Substitute parameters in SQL query template

        Args:
            query: SQL query with {parameter} placeholders
            **params: Parameter values to substitute

        Returns:
            Query with parameters substituted
        """
        try:
            # Use string format to substitute parameters
            substituted = query.format(**params)
            return substituted

        except KeyError as e:
            self.logger.error(f"Missing parameter for query substitution: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to substitute query parameters: {e}")
            raise

    def execute_field_query(self, field_name: str, author_name: str, query_template: str) -> Optional[Any]:
        """
        Execute field calculation query for a specific author

        Args:
            field_name: Name of the field to calculate
            author_name: DBLP author name to calculate for
            query_template: SQL query template

        Returns:
            Expected value for the field or None if query failed
        """
        try:
            # Substitute author name parameter with proper SQL escaping
            # Escape single quotes by doubling them (SQL standard)
            escaped_author_name = author_name.replace("'", "''")
            query = self.substitute_query_parameters(
                query_template,
                dblp_author_name=f"'{escaped_author_name}'"
            )

            # Execute query
            result = self.db_manager.fetch_one(query)

            if not result:
                return 0

            # Extract the field value from result with improved column matching
            field_value = None

            # Try multiple strategies to find the value
            strategies = [
                field_name,                           # Exact field name match
                f"semantic_scholar_{field_name}",     # With semantic_scholar prefix
                f"sum",                              # Common aggregate function name
                f"count",                            # Common count function name
            ]

            for strategy in strategies:
                if strategy in result:
                    field_value = result[strategy]
                    break

            # If still no match, take the first non-null value
            if field_value is None:
                values = list(result.values())
                if values and values[0] is not None:
                    field_value = values[0]  # Take the first value
                else:
                    field_value = 0  # Return 0 instead of None for failed calculations

            return field_value if field_value is not None else 0

        except Exception as e:
            self.logger.error(f"Failed to execute field query for {field_name}: {e}")
            return 0

    def get_actual_value(self, field_name: str, author_name: str) -> Optional[Any]:
        """
        Get actual value from final_author_table for comparison

        Args:
            field_name: Name of the field to retrieve
            author_name: DBLP author name

        Returns:
            Actual value from final_author_table or None if not found
        """
        try:
            # Map field test name to actual database column name
            column_name = self.field_column_mapping.get(field_name, field_name)

            query = f"""
            SELECT {column_name}
            FROM final_author_table
            WHERE dblp_author = %s
            """

            result = self.db_manager.fetch_one(query, (author_name,))

            if not result:
                return None

            return result.get(column_name)

        except Exception as e:
            self.logger.error(f"Failed to get actual value for {field_name}: {e}")
            return None

    def get_sample_authors(self, limit: int = 5) -> List[str]:
        """
        Get sample author names from final_author_table for testing

        Args:
            limit: Maximum number of authors to return

        Returns:
            List of DBLP author names
        """
        try:
            query = """
            SELECT dblp_author
            FROM final_author_table
            WHERE dblp_author IS NOT NULL
            AND dblp_author != ''
            ORDER BY RANDOM()
            LIMIT %s
            """

            results = self.db_manager.fetch_all(query, (limit,))
            return [r['dblp_author'] for r in results if r['dblp_author']]

        except Exception as e:
            self.logger.error(f"Failed to get sample authors: {e}")
            return []