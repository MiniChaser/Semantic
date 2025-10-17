"""
Conference Filter Service
Filters papers from all_papers table by conference and populates dataset_papers table
Uses SQL-based filtering for efficiency
"""

import logging
from datetime import datetime
from typing import Dict, List
from tqdm import tqdm

from ...database.connection import DatabaseManager
from ...database.repositories.dataset_release import DatasetReleaseRepository
from .conference_matcher import ConferenceMatcher


class ConferenceFilterService:
    """
    Filters papers by conference using SQL queries
    Populates dataset_papers table from all_papers table
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.conference_matcher = ConferenceMatcher()
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

    def _build_tsquery_string(self) -> str:
        """
        Build tsquery string for full-text search using tsvector
        Returns a tsquery-compatible string with all conference patterns joined by OR
        """
        conferences = self.conference_matcher.get_conferences()
        aliases = self.conference_matcher.aliases

        patterns = []

        # Add main conference patterns
        for conf in conferences:
            patterns.append(conf.lower())

        # Add alias patterns
        for conf, alias_list in aliases.items():
            for alias in alias_list:
                patterns.append(alias.lower())

        # Join with OR operator for tsquery
        tsquery_string = ' | '.join(patterns)

        self.logger.info(f"Built tsquery with {len(patterns)} conference patterns")

        return tsquery_string

    def filter_and_populate_dataset_papers(self, batch_size: int = 10000) -> Dict:
        """
        Filter papers by conference from all_papers and populate dataset_papers
        Uses optimized tsvector full-text search and cursor-based pagination
        """
        start_time = datetime.now()

        self.logger.info("="*80)
        self.logger.info("Starting conference filtering from all_papers to dataset_papers")
        self.logger.info("="*80)

        try:
            # Build tsquery string for full-text search
            self.logger.info("Building tsquery for full-text search...")
            tsquery_string = self._build_tsquery_string()

            # Count total matching papers using tsvector full-text search
            self.logger.info("Counting matching papers in all_papers (using GIN index)...")
            count_query = """
            SELECT COUNT(*) as total
            FROM all_papers
            WHERE venue_tsv @@ to_tsquery('simple', %s)
            """
            result = self.db_manager.fetch_one(count_query, (tsquery_string,))
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
            self.logger.info("Using tsvector full-text search + cursor pagination for optimal performance")

            last_corpus_id = 0
            batch_num = 0

            with tqdm(total=total_batches, desc="Filtering conferences") as pbar:
                while True:
                    # Fetch batch using cursor (WHERE corpus_id > last_id) and tsvector match
                    batch_query = """
                    SELECT
                        corpus_id,
                        paper_id,
                        external_ids,
                        title,
                        abstract,
                        venue,
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
                    FROM all_papers
                    WHERE venue_tsv @@ to_tsquery('simple', %s)
                        AND corpus_id > %s
                    ORDER BY corpus_id
                    LIMIT %s
                    """

                    papers = self.db_manager.fetch_all(batch_query, (tsquery_string, last_corpus_id, batch_size))

                    if not papers:
                        break

                    # Update cursor for next batch
                    last_corpus_id = papers[-1]['corpus_id']

                    # Determine conference_normalized for each paper
                    papers_with_conf = []
                    for paper in papers:
                        venue = paper.get('venue', '')
                        matched_conf = self.conference_matcher.match_conference(venue)

                        # Add conference_normalized field
                        paper_with_conf = dict(paper)
                        paper_with_conf['conference_normalized'] = matched_conf
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
                corpus_id, paper_id, external_ids, title, abstract, venue, year,
                citation_count, reference_count, influential_citation_count,
                authors, fields_of_study, publication_types,
                is_open_access, open_access_pdf, conference_normalized,
                source_file, release_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (corpus_id) DO UPDATE SET
                paper_id = EXCLUDED.paper_id,
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
                    p['corpus_id'], p.get('paper_id'), p.get('external_ids'), p['title'],
                    p.get('abstract'), p.get('venue'), p.get('year'), p.get('citation_count', 0),
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
