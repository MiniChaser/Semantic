"""
Conference Filter Service
Filters papers from dataset_all_papers table by conference and populates dataset_papers table
Uses SQL-based filtering for efficiency with multi-process support
"""

import logging
import os
import multiprocessing as mp
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm

from ...database.connection import DatabaseManager, DatabaseConfig
from ...database.repositories.dataset_release import DatasetReleaseRepository
from .database_conference_matcher import DatabaseConferenceMatcher


class ConferenceFilterService:
    """
    Filters papers by conference using SQL queries
    Populates dataset_papers table from dataset_all_papers table
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.conference_matcher = DatabaseConferenceMatcher(db_manager)
        self.release_repo = DatasetReleaseRepository(db_manager)
        self.logger = self._setup_logger()

        # Statistics
        self.total_matched = 0
        self.total_inserted = 0
        self.total_updated = 0

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.ConferenceFilterService')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger


    def filter_and_populate_dataset_papers(self, batch_size: int = 10000) -> Dict:
        """
        Filter papers by conference from dataset_all_papers and populate dataset_papers
        Uses optimized venue_normalized B-tree index with IN queries and cursor-based pagination
        """
        start_time = datetime.now()

        self.logger.info("="*80)
        self.logger.info("Starting conference filtering from dataset_all_papers to dataset_papers")
        self.logger.info("="*80)

        try:
            # Get conference list from database
            self.logger.info("Loading conferences from database...")
            conferences = self.conference_matcher.get_conferences()

            if not conferences:
                self.logger.error("No conferences found in database")
                return {
                    'status': 'failed',
                    'error': 'No conferences found. Please run init_conferences_table.py first'
                }

            self.logger.info(f"Loaded {len(conferences)} conferences")

            # Count total matching papers using venue_normalized B-tree index
            self.logger.info("Counting matching papers in dataset_all_papers (using B-tree index)...")
            placeholders = ','.join(['%s'] * len(conferences))
            count_query = f"""
            SELECT COUNT(*) as total
            FROM dataset_all_papers
            WHERE venue_normalized IN ({placeholders})
            """
            result = self.db_manager.fetch_one(count_query, tuple(conferences))
            total_papers = result['total'] if result else 0

            self.logger.info(f"Found {total_papers:,} papers matching conference criteria")

            if total_papers == 0:
                self.logger.warning("No papers matched conference criteria")
                return {
                    'status': 'completed',
                    'total_matched': 0,
                    'total_inserted': 0,
                    'total_updated': 0
                }

            # Process in batches using cursor-based pagination
            total_batches = (total_papers + batch_size - 1) // batch_size
            self.logger.info(f"Processing in {total_batches} batches of {batch_size}")
            self.logger.info("Using venue_normalized B-tree index + cursor pagination for optimal performance")

            last_corpus_id = 0
            batch_num = 0

            with tqdm(total=total_batches, desc="Filtering conferences") as pbar:
                while True:
                    # Fetch batch using cursor (WHERE corpus_id > last_id) and venue_normalized IN query
                    batch_query = f"""
                    SELECT
                        corpus_id,
                        paper_id,
                        url,
                        external_ids,
                        title,
                        abstract,
                        venue,
                        venue_normalized,
                        year,
                        citation_count,
                        reference_count,
                        influential_citation_count,
                        authors,
                        fields_of_study,
                        publication_types,
                        is_open_access,
                        open_access_pdf,
                        source_file,
                        release_id
                    FROM dataset_all_papers
                    WHERE venue_normalized IN ({placeholders})
                        AND corpus_id > %s
                    ORDER BY corpus_id
                    LIMIT %s
                    """

                    # Parameters: conferences tuple + last_corpus_id + batch_size
                    params = tuple(conferences) + (last_corpus_id, batch_size)
                    papers = self.db_manager.fetch_all(batch_query, params)

                    if not papers:
                        break

                    # Update cursor for next batch
                    last_corpus_id = papers[-1]['corpus_id']

                    # venue_normalized is already set, use it directly as conference_normalized
                    papers_with_conf = []
                    for paper in papers:
                        paper_with_conf = dict(paper)
                        # Use venue_normalized as conference_normalized (already standardized)
                        paper_with_conf['conference_normalized'] = paper.get('venue_normalized')
                        papers_with_conf.append(paper_with_conf)

                    # Batch upsert papers using INSERT ON CONFLICT
                    upserted = self._batch_upsert_papers(papers_with_conf)
                    self.total_matched += len(papers_with_conf)
                    self.total_inserted += upserted

                    batch_num += 1
                    pbar.update(1)

                    if batch_num % 10 == 0 or batch_num == total_batches:
                        self.logger.info(
                            f"Progress: Batch {batch_num}/{total_batches}, "
                            f"Matched={self.total_matched:,}, "
                            f"Upserted={self.total_inserted:,}"
                        )

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            self.logger.info("="*80)
            self.logger.info("Conference filtering completed!")
            self.logger.info(f"Total papers matched: {self.total_matched:,}")
            self.logger.info(f"Papers upserted: {self.total_inserted:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.2f} minutes)")
            self.logger.info("="*80)

            return {
                'status': 'completed',
                'total_matched': self.total_matched,
                'total_inserted': self.total_inserted,
                'total_updated': 0,  # UPSERT doesn't distinguish
                'processing_time_seconds': processing_time
            }

        except Exception as e:
            self.logger.error(f"Conference filtering failed: {e}", exc_info=True)
            raise

    def _batch_upsert_papers(self, papers: List[Dict]) -> int:
        """
        Batch upsert papers into dataset_papers using INSERT ON CONFLICT
        This replaces the need for separate insert/update logic
        """
        if not papers:
            return 0

        try:
            upsert_query = """
            INSERT INTO dataset_papers (
                corpus_id, paper_id, url, external_ids, title, abstract, venue, year,
                citation_count, reference_count, influential_citation_count,
                authors, fields_of_study, publication_types,
                is_open_access, open_access_pdf, conference_normalized,
                source_file, release_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (corpus_id) DO UPDATE SET
                paper_id = EXCLUDED.paper_id,
                url = EXCLUDED.url,
                external_ids = EXCLUDED.external_ids,
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                venue = EXCLUDED.venue,
                year = EXCLUDED.year,
                citation_count = EXCLUDED.citation_count,
                reference_count = EXCLUDED.reference_count,
                influential_citation_count = EXCLUDED.influential_citation_count,
                authors = EXCLUDED.authors,
                fields_of_study = EXCLUDED.fields_of_study,
                publication_types = EXCLUDED.publication_types,
                is_open_access = EXCLUDED.is_open_access,
                open_access_pdf = EXCLUDED.open_access_pdf,
                conference_normalized = EXCLUDED.conference_normalized,
                source_file = EXCLUDED.source_file,
                release_id = EXCLUDED.release_id,
                updated_at = CURRENT_TIMESTAMP
            """

            params_list = [
                (
                    p['corpus_id'], p.get('paper_id'), p.get('url'), p.get('external_ids'), p['title'],
                    p.get('abstract'), p.get('venue_normalized'), p.get('year') or 0, p.get('citation_count', 0),
                    p.get('reference_count', 0), p.get('influential_citation_count', 0),
                    p.get('authors'), p.get('fields_of_study'), p.get('publication_types'),
                    p.get('is_open_access', False), p.get('open_access_pdf'),
                    p.get('conference_normalized'), p.get('source_file'), p['release_id']
                )
                for p in papers
            ]

            self.db_manager.execute_batch_query(upsert_query, params_list)
            return len(papers)

        except Exception as e:
            self.logger.error(f"Batch upsert failed: {e}")
            raise

    def _batch_upsert_papers_fast(self, papers: List[Dict]) -> int:
        """
        Fast batch upsert papers using execute_values (2-3x faster than executemany)
        Uses psycopg2.extras.execute_values for optimal bulk insert performance
        """
        if not papers:
            return 0

        try:
            # Build upsert query with %s placeholder for VALUES
            upsert_query = """
            INSERT INTO dataset_papers (
                corpus_id, paper_id, url, external_ids, title, abstract, venue, year,
                citation_count, reference_count, influential_citation_count,
                authors, fields_of_study, publication_types,
                is_open_access, open_access_pdf, conference_normalized,
                source_file, release_id
            ) VALUES %s
            ON CONFLICT (corpus_id) DO UPDATE SET
                paper_id = EXCLUDED.paper_id,
                url = EXCLUDED.url,
                external_ids = EXCLUDED.external_ids,
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                venue = EXCLUDED.venue,
                year = EXCLUDED.year,
                citation_count = EXCLUDED.citation_count,
                reference_count = EXCLUDED.reference_count,
                influential_citation_count = EXCLUDED.influential_citation_count,
                authors = EXCLUDED.authors,
                fields_of_study = EXCLUDED.fields_of_study,
                publication_types = EXCLUDED.publication_types,
                is_open_access = EXCLUDED.is_open_access,
                open_access_pdf = EXCLUDED.open_access_pdf,
                conference_normalized = EXCLUDED.conference_normalized,
                source_file = EXCLUDED.source_file,
                release_id = EXCLUDED.release_id,
                updated_at = CURRENT_TIMESTAMP
            """

            params_list = [
                (
                    p['corpus_id'], p.get('paper_id'), p.get('url'), p.get('external_ids'), p['title'],
                    p.get('abstract'), p.get('venue_normalized'), p.get('year') or 0, p.get('citation_count', 0),
                    p.get('reference_count', 0), p.get('influential_citation_count', 0),
                    p.get('authors'), p.get('fields_of_study'), p.get('publication_types'),
                    p.get('is_open_access', False), p.get('open_access_pdf'),
                    p.get('conference_normalized'), p.get('source_file'), p['release_id']
                )
                for p in papers
            ]

            # Use execute_values for 2-3x better performance
            self.db_manager.execute_values_query(upsert_query, params_list, page_size=1000)
            return len(papers)

        except Exception as e:
            self.logger.error(f"Fast batch upsert failed: {e}")
            raise

    def _get_corpus_id_range(self, conferences: List[str]) -> Tuple[int, int]:
        """Get the min and max corpus_id for matching papers"""
        placeholders = ','.join(['%s'] * len(conferences))
        query = f"""
        SELECT MIN(corpus_id) as min_id, MAX(corpus_id) as max_id
        FROM dataset_all_papers
        WHERE venue_normalized IN ({placeholders})
        """
        result = self.db_manager.fetch_one(query, tuple(conferences))
        if result and result['min_id'] and result['max_id']:
            return result['min_id'], result['max_id']
        return 0, 0

    def _calculate_process_ranges(self, min_id: int, max_id: int, num_processes: int) -> List[Tuple[int, int]]:
        """
        Split corpus_id range into equal chunks for parallel processing
        Returns list of (start_id, end_id) tuples for each process
        """
        if num_processes <= 1:
            return [(min_id, max_id)]

        range_size = (max_id - min_id) // num_processes
        ranges = []

        for i in range(num_processes):
            start = min_id + (i * range_size)
            end = min_id + ((i + 1) * range_size) if i < num_processes - 1 else max_id
            ranges.append((start, end))

        return ranges

    @staticmethod
    def _worker_process(worker_id: int, start_corpus_id: int, end_corpus_id: int,
                       conferences: List[str], release_id: str, batch_size: int,
                       shared_dict: Dict) -> Dict:
        """
        Worker process for parallel filtering
        Each worker handles a non-overlapping corpus_id range

        Args:
            worker_id: Process identifier (0-based)
            start_corpus_id: Start of corpus_id range (inclusive)
            end_corpus_id: End of corpus_id range (inclusive)
            conferences: List of conference names to filter
            release_id: Release ID for new records
            batch_size: Batch size for processing
            shared_dict: Shared dictionary for progress tracking

        Returns:
            Dict with statistics for this worker
        """
        # Create new database connection for this worker
        db_manager = DatabaseManager()

        # Setup logger for this worker
        logger = logging.getLogger(f'ConferenceFilterService.Worker{worker_id}')
        logger.setLevel(logging.INFO)

        try:
            logger.info(f"Worker {worker_id} starting: corpus_id {start_corpus_id:,} to {end_corpus_id:,}")

            matched = 0
            inserted = 0

            placeholders = ','.join(['%s'] * len(conferences))
            last_corpus_id = start_corpus_id - 1

            while True:
                # Fetch batch within the assigned range
                batch_query = f"""
                SELECT
                    corpus_id, paper_id, url, external_ids, title, abstract, venue,
                    venue_normalized, year, citation_count, reference_count,
                    influential_citation_count, authors, fields_of_study,
                    publication_types, is_open_access, open_access_pdf,
                    source_file, release_id
                FROM dataset_all_papers
                WHERE venue_normalized IN ({placeholders})
                    AND corpus_id > %s
                    AND corpus_id <= %s
                ORDER BY corpus_id
                LIMIT %s
                """

                params = tuple(conferences) + (last_corpus_id, end_corpus_id, batch_size)
                papers = db_manager.fetch_all(batch_query, params)

                if not papers:
                    break

                # Update cursor
                last_corpus_id = papers[-1]['corpus_id']

                # Add conference_normalized field
                papers_with_conf = []
                for paper in papers:
                    paper_with_conf = dict(paper)
                    paper_with_conf['conference_normalized'] = paper.get('venue_normalized')
                    papers_with_conf.append(paper_with_conf)

                # Fast batch upsert using execute_values
                upsert_query = """
                INSERT INTO dataset_papers (
                    corpus_id, paper_id, url, external_ids, title, abstract, venue, year,
                    citation_count, reference_count, influential_citation_count,
                    authors, fields_of_study, publication_types,
                    is_open_access, open_access_pdf, conference_normalized,
                    source_file, release_id
                ) VALUES %s
                ON CONFLICT (corpus_id) DO UPDATE SET
                    paper_id = EXCLUDED.paper_id,
                    url = EXCLUDED.url,
                    external_ids = EXCLUDED.external_ids,
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    venue = EXCLUDED.venue,
                    year = EXCLUDED.year,
                    citation_count = EXCLUDED.citation_count,
                    reference_count = EXCLUDED.reference_count,
                    influential_citation_count = EXCLUDED.influential_citation_count,
                    authors = EXCLUDED.authors,
                    fields_of_study = EXCLUDED.fields_of_study,
                    publication_types = EXCLUDED.publication_types,
                    is_open_access = EXCLUDED.is_open_access,
                    open_access_pdf = EXCLUDED.open_access_pdf,
                    conference_normalized = EXCLUDED.conference_normalized,
                    source_file = EXCLUDED.source_file,
                    release_id = EXCLUDED.release_id,
                    updated_at = CURRENT_TIMESTAMP
                """

                params_list = [
                    (
                        p['corpus_id'], p.get('paper_id'), p.get('url'), p.get('external_ids'), p['title'],
                        p.get('abstract'), p.get('venue_normalized'), p.get('year') or 0,
                        p.get('citation_count', 0), p.get('reference_count', 0),
                        p.get('influential_citation_count', 0), p.get('authors'),
                        p.get('fields_of_study'), p.get('publication_types'),
                        p.get('is_open_access', False), p.get('open_access_pdf'),
                        p.get('conference_normalized'), p.get('source_file'), release_id
                    )
                    for p in papers_with_conf
                ]

                db_manager.execute_values_query(upsert_query, params_list, page_size=1000)

                matched += len(papers_with_conf)
                inserted += len(papers_with_conf)

                # Update shared progress
                if shared_dict is not None:
                    shared_dict[f'worker_{worker_id}_matched'] = matched
                    shared_dict[f'worker_{worker_id}_inserted'] = inserted

            logger.info(f"Worker {worker_id} completed: matched={matched:,}, inserted={inserted:,}")

            return {
                'worker_id': worker_id,
                'matched': matched,
                'inserted': inserted,
                'status': 'completed'
            }

        except Exception as e:
            logger.error(f"Worker {worker_id} failed: {e}", exc_info=True)
            return {
                'worker_id': worker_id,
                'matched': 0,
                'inserted': 0,
                'status': 'failed',
                'error': str(e)
            }
        finally:
            db_manager.disconnect()

    def filter_and_populate_parallel(self, batch_size: int = 10000, num_processes: Optional[int] = None) -> Dict:
        """
        Filter papers by conference using multiple processes for better performance

        Args:
            batch_size: Batch size for each process
            num_processes: Number of processes (None = auto-detect)

        Returns:
            Dict with statistics
        """
        start_time = datetime.now()

        self.logger.info("="*80)
        self.logger.info("Starting PARALLEL conference filtering (Multi-Process)")
        self.logger.info("="*80)

        try:
            # Auto-detect optimal process count
            if num_processes is None:
                cpu_count = os.cpu_count() or 4
                db_max_conn = 30  # From DB_POOL_SIZE + DB_MAX_OVERFLOW
                num_processes = min(cpu_count, db_max_conn - 2, 8)  # Reserve 2 connections, max 8

            self.logger.info(f"CPU cores detected: {os.cpu_count()}")
            self.logger.info(f"Using {num_processes} parallel processes")

            # Get conferences
            self.logger.info("Loading conferences from database...")
            conferences = self.conference_matcher.get_conferences()

            if not conferences:
                self.logger.error("No conferences found in database")
                return {
                    'status': 'failed',
                    'error': 'No conferences found. Please run init_conferences_table.py first'
                }

            self.logger.info(f"Loaded {len(conferences)} conferences")

            # Count total papers
            self.logger.info("Counting matching papers...")
            placeholders = ','.join(['%s'] * len(conferences))
            count_query = f"""
            SELECT COUNT(*) as total
            FROM dataset_all_papers
            WHERE venue_normalized IN ({placeholders})
            """
            result = self.db_manager.fetch_one(count_query, tuple(conferences))
            total_papers = result['total'] if result else 0

            self.logger.info(f"Found {total_papers:,} papers matching conference criteria")

            if total_papers == 0:
                return {
                    'status': 'completed',
                    'total_matched': 0,
                    'total_inserted': 0,
                    'total_updated': 0
                }

            # Get corpus_id range
            min_id, max_id = self._get_corpus_id_range(conferences)
            self.logger.info(f"Corpus ID range: {min_id:,} to {max_id:,}")

            # Calculate ranges for each process
            ranges = self._calculate_process_ranges(min_id, max_id, num_processes)
            self.logger.info(f"Split into {len(ranges)} ranges:")
            for i, (start, end) in enumerate(ranges):
                self.logger.info(f"  Process {i}: corpus_id {start:,} to {end:,}")

            # Create shared memory for progress tracking
            manager = mp.Manager()
            shared_dict = manager.dict()

            # Start worker processes
            self.logger.info(f"\nStarting {num_processes} worker processes...")
            with mp.Pool(processes=num_processes) as pool:
                worker_args = [
                    (i, start, end, conferences, self.release_id, batch_size, shared_dict)
                    for i, (start, end) in enumerate(ranges)
                ]

                # Use tqdm to track completion
                results = []
                with tqdm(total=num_processes, desc="Worker processes", unit="worker") as pbar:
                    for result in pool.starmap(self._worker_process, worker_args):
                        results.append(result)
                        pbar.update(1)

            # Aggregate results
            total_matched = sum(r['matched'] for r in results)
            total_inserted = sum(r['inserted'] for r in results)
            failed_workers = [r for r in results if r['status'] == 'failed']

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            self.logger.info("="*80)
            self.logger.info("Parallel conference filtering completed!")
            self.logger.info(f"Total papers matched: {total_matched:,}")
            self.logger.info(f"Papers upserted: {total_inserted:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.2f} minutes)")
            self.logger.info(f"Throughput: {total_matched/processing_time:.2f} records/second")

            if failed_workers:
                self.logger.warning(f"Warning: {len(failed_workers)} workers failed")
                for w in failed_workers:
                    self.logger.warning(f"  Worker {w['worker_id']}: {w.get('error', 'Unknown error')}")

            self.logger.info("="*80)

            return {
                'status': 'completed',
                'total_matched': total_matched,
                'total_inserted': total_inserted,
                'total_updated': 0,
                'processing_time_seconds': processing_time,
                'num_processes': num_processes,
                'failed_workers': len(failed_workers)
            }

        except Exception as e:
            self.logger.error(f"Parallel filtering failed: {e}", exc_info=True)
            raise
