#!/usr/bin/env python3
"""
S2 Author Enrichment Service
Service for enriching author profiles with S2 Author API data
"""

import os
import logging
import time
from typing import Dict


class S2AuthorEnrichmentService:
    """Service for enriching author profiles with S2 Author API data"""

    def __init__(self, db_manager, api_key: str = None):
        from ..author_service.author_profile_pandas_service import AuthorProfilePandasService

        self.db_manager = db_manager
        self.api_key = api_key or os.getenv('SEMANTIC_SCHOLAR_API_KEY')
        self.profile_service = AuthorProfilePandasService(db_manager, self.api_key)
        self.logger = logging.getLogger(__name__)

    def run_enrichment(self, limit: int = None) -> Dict:
        """
        Run S2 Author API enrichment process

        Args:
            limit: Maximum number of authors to process

        Returns:
            Enrichment statistics
        """
        start_time = time.time()
        self.logger.info("Starting S2 Author API enrichment process...")

        try:
            # Run the enrichment
            stats = self.profile_service.enrich_with_s2_author_api(limit=limit)

            # Calculate processing time
            end_time = time.time()
            processing_time = end_time - start_time

            # Check for errors
            if 'error' in stats:
                self.logger.error(f"Enrichment failed: {stats['error']}")
                return stats

            # Display results
            self._display_results(stats, processing_time)
            return stats

        except Exception as e:
            self.logger.error(f"S2 Author API enrichment failed: {e}")
            return {'error': str(e)}

    def _display_results(self, stats: Dict, processing_time: float):
        """Display enrichment results"""
        print("\n" + "=" * 60)
        print("S2 AUTHOR API ENRICHMENT COMPLETED")
        print("=" * 60)
        print(f"Processing time: {processing_time:.2f} seconds")
        print(f"Total authors processed: {stats.get('total_authors_processed', 0)}")
        print(f"Authors successfully enriched: {stats.get('authors_enriched', 0)}")
        print(f"API calls made: {stats.get('api_calls_made', 0)}")
        print(f"Total S2 IDs queried: {stats.get('total_s2_ids_queried', 0)}")
        print(f"Successful S2 IDs: {stats.get('successful_s2_ids', 0)}")
        print(f"Errors encountered: {stats.get('errors', 0)}")

        # Calculate success rates
        total_processed = stats.get('total_authors_processed', 0)
        if total_processed > 0:
            enrichment_rate = (stats.get('authors_enriched', 0) / total_processed) * 100
            print(f"Enrichment success rate: {enrichment_rate:.1f}%")

        total_ids = stats.get('total_s2_ids_queried', 0)
        if total_ids > 0:
            id_success_rate = (stats.get('successful_s2_ids', 0) / total_ids) * 100
            print(f"S2 ID query success rate: {id_success_rate:.1f}%")

        print("=" * 60)