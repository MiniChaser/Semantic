#!/usr/bin/env python3
"""
S2 Author Enrichment Service
Service for enriching author profiles with S2 Author API data using batch processing
"""

import os
import logging
import time
from typing import Dict

from .s2_author_profile_batch_service import S2AuthorProfileBatchService


class S2AuthorEnrichmentService:
    """Service for enriching author profiles with S2 Author API data using batch processing"""

    def __init__(self, db_manager, api_key: str = None, use_batch: bool = True):
        self.db_manager = db_manager
        self.api_key = api_key or os.getenv('SEMANTIC_SCHOLAR_API_KEY')
        self.use_batch = use_batch
        self.logger = logging.getLogger(__name__)

        # Initialize services
        if use_batch:
            self.batch_service = S2AuthorProfileBatchService(db_manager, self.api_key)
        else:
            # Fallback to old service for compatibility
            from ..author_service.author_profile_pandas_service import AuthorProfilePandasService
            self.profile_service = AuthorProfilePandasService(db_manager, self.api_key)

    def run_enrichment(self, limit: int = None) -> Dict:
        """
        Run S2 Author API enrichment process

        Args:
            limit: Maximum number of authors to process

        Returns:
            Enrichment statistics
        """
        start_time = time.time()

        if self.use_batch:
            self.logger.info("Starting S2 Author API BATCH enrichment process...")
            return self._run_batch_enrichment(limit, start_time)
        else:
            self.logger.info("Starting S2 Author API individual enrichment process...")
            return self._run_individual_enrichment(limit, start_time)

    def _run_batch_enrichment(self, limit: int = None, start_time: float = None) -> Dict:
        """Run the batch enrichment process"""
        try:
            # Create s2_author_profiles table if it doesn't exist
            if not self.batch_service.create_s2_author_profiles_table():
                return {'error': 'Failed to create s2_author_profiles table'}

            # Run batch enrichment
            batch_stats = self.batch_service.run_batch_enrichment(limit=limit)

            # Check for errors
            if 'error' in batch_stats:
                self.logger.error(f"Batch enrichment failed: {batch_stats['error']}")
                return batch_stats

            # Calculate processing time if not already included
            if 'processing_time' not in batch_stats and start_time:
                end_time = time.time()
                batch_stats['processing_time'] = end_time - start_time

            # Display results
            self._display_batch_results(batch_stats)
            return batch_stats

        except Exception as e:
            self.logger.error(f"S2 Author API batch enrichment failed: {e}")
            return {'error': str(e)}

    def _run_individual_enrichment(self, limit: int = None, start_time: float = None) -> Dict:
        """Run the individual enrichment process (fallback)"""
        try:
            # Run the enrichment
            stats = self.profile_service.enrich_with_s2_author_api(limit=limit)

            # Calculate processing time
            if start_time:
                end_time = time.time()
                processing_time = end_time - start_time
                stats['processing_time'] = processing_time

            # Check for errors
            if 'error' in stats:
                self.logger.error(f"Individual enrichment failed: {stats['error']}")
                return stats

            # Display results
            self._display_individual_results(stats, stats.get('processing_time', 0))
            return stats

        except Exception as e:
            self.logger.error(f"S2 Author API individual enrichment failed: {e}")
            return {'error': str(e)}

    def _display_batch_results(self, stats: Dict):
        """Display batch enrichment results"""
        print("\n" + "=" * 70)
        print("S2 AUTHOR API BATCH ENRICHMENT COMPLETED")
        print("=" * 70)
        print(f"Processing time: {stats.get('processing_time', 0):.2f} seconds")
        print(f"Total author IDs to process: {stats.get('total_ids_to_process', 0)}")

        # Batch fetch statistics
        batch_stats = stats.get('batch_fetch_stats', {})
        print(f"Author IDs processed: {batch_stats.get('processed', 0)}")
        print(f"Author IDs successfully fetched: {batch_stats.get('successful', 0)}")
        print(f"API calls made: {batch_stats.get('api_calls', 0)}")
        print(f"API errors: {batch_stats.get('errors', 0)}")

        # Sync statistics
        sync_stats = stats.get('sync_stats', {})
        print(f"Author profiles updated: {sync_stats.get('updated', 0)}")
        print(f"Sync errors: {sync_stats.get('errors', 0)}")

        # Performance improvement
        performance = stats.get('performance_improvement', '')
        if performance:
            print(f"Performance: {performance}")

        # Calculate success rates
        total_processed = batch_stats.get('processed', 0)
        if total_processed > 0:
            fetch_rate = (batch_stats.get('successful', 0) / total_processed) * 100
            print(f"S2 API fetch success rate: {fetch_rate:.1f}%")

        print("=" * 70)

    def _display_individual_results(self, stats: Dict, processing_time: float):
        """Display individual enrichment results (fallback)"""
        print("\n" + "=" * 60)
        print("S2 AUTHOR API INDIVIDUAL ENRICHMENT COMPLETED")
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