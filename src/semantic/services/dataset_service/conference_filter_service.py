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

    def _build_conference_sql_conditions(self) -> List[str]:
        """
        Build SQL WHERE conditions for conference matching
        Returns list of SQL conditions that match conference patterns
        """
        conferences = self.conference_matcher.get_conferences()
        aliases = self.conference_matcher.aliases

        conditions = []

        # Add exact match conditions for each conference
        for conf in conferences:
            conf_lower = conf.lower()
            # Exact match or contained in venue
            conditions.append(f"LOWER(venue) LIKE '%{conf_lower}%'")

        # Add alias conditions
        for conf, alias_list in aliases.items():
            for alias in alias_list:
                alias_lower = alias.lower()
                conditions.append(f"LOWER(venue) LIKE '%{alias_lower}%'")

        return conditions

    def filter_and_populate_dataset_papers(self, batch_size: int = 10000) -> Dict:
        """
        Filter papers by conference from all_papers and populate dataset_papers
        Uses SQL-based filtering with batching for memory efficiency
        """
        start_time = datetime.now()

        self.logger.info("="*80)
        self.logger.info("Starting conference filtering from all_papers to dataset_papers")
        self.logger.info("="*80)

        try:
            # Build SQL conditions
            self.logger.info("Building conference matching conditions...")
            sql_conditions = self._build_conference_sql_conditions()
            where_clause = " OR ".join(sql_conditions)

            self.logger.info(f"Built {len(sql_conditions)} matching conditions")

            # Count total matching papers
            self.logger.info("Counting matching papers in all_papers...")
            count_query = f"""
            SELECT COUNT(*) as total
            FROM all_papers
            WHERE {where_clause}
            """
            result = self.db_manager.fetch_one(count_query)
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

            # Process in batches using OFFSET/LIMIT
            total_batches = (total_papers + batch_size - 1) // batch_size
            self.logger.info(f"Processing in {total_batches} batches of {batch_size}")

            for batch_num in tqdm(range(total_batches), desc="Filtering conferences"):
                offset = batch_num * batch_size

                # Fetch batch of matching papers
                batch_query = f"""
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
                WHERE {where_clause}
                ORDER BY corpus_id
                LIMIT {batch_size} OFFSET {offset}
                """

                papers = self.db_manager.fetch_all(batch_query)

                if not papers:
                    continue

                # Process each paper to determine exact conference match
                papers_to_insert = []
                papers_to_update = []

                # Get existing corpus_ids in dataset_papers
                corpus_ids = [p['corpus_id'] for p in papers]
                placeholders = ','.join(['%s'] * len(corpus_ids))
                existing_query = f"SELECT corpus_id FROM dataset_papers WHERE corpus_id IN ({placeholders})"
                existing = self.db_manager.fetch_all(existing_query, tuple(corpus_ids))
                existing_ids = {row['corpus_id'] for row in existing}

                for paper in papers:
                    venue = paper.get('venue', '')
                    matched_conf = self.conference_matcher.match_conference(venue)

                    if matched_conf:
                        paper_data = {
                            'corpus_id': paper['corpus_id'],
                            'paper_id': paper.get('paper_id'),
                            'external_ids': paper.get('external_ids'),
                            'title': paper['title'],
                            'abstract': paper.get('abstract'),
                            'venue': paper.get('venue'),
                            'year': paper.get('year'),
                            'citation_count': paper.get('citation_count', 0),
                            'reference_count': paper.get('reference_count', 0),
                            'influential_citation_count': paper.get('influential_citation_count', 0),
                            'authors': paper.get('authors'),
                            'fields_of_study': paper.get('fields_of_study'),
                            'publication_types': paper.get('publication_types'),
                            'is_open_access': paper.get('is_open_access', False),
                            'open_access_pdf': paper.get('open_access_pdf'),
                            'conference_normalized': matched_conf,
                            'source_file': paper.get('source_file'),
                            'release_id': paper['release_id']
                        }

                        if paper['corpus_id'] in existing_ids:
                            papers_to_update.append(paper_data)
                        else:
                            papers_to_insert.append(paper_data)

                # Batch insert new papers
                if papers_to_insert:
                    inserted = self._batch_insert_papers(papers_to_insert)
                    self.total_inserted += inserted
                    self.total_matched += inserted

                # Batch update existing papers
                if papers_to_update:
                    updated = self._batch_update_papers(papers_to_update)
                    self.total_updated += updated
                    self.total_matched += updated

                if (batch_num + 1) % 10 == 0 or batch_num == total_batches - 1:
                    self.logger.info(
                        f"Progress: Batch {batch_num + 1}/{total_batches}, "
                        f"Matched={self.total_matched:,}, "
                        f"Inserted={self.total_inserted:,}, "
                        f"Updated={self.total_updated:,}"
                    )

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            self.logger.info("="*80)
            self.logger.info("Conference filtering completed!")
            self.logger.info(f"Total papers matched: {self.total_matched:,}")
            self.logger.info(f"Papers inserted (new): {self.total_inserted:,}")
            self.logger.info(f"Papers updated (existing): {self.total_updated:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.2f} minutes)")
            self.logger.info("="*80)

            return {
                'status': 'completed',
                'total_matched': self.total_matched,
                'total_inserted': self.total_inserted,
                'total_updated': self.total_updated,
                'processing_time_seconds': processing_time
            }

        except Exception as e:
            self.logger.error(f"Conference filtering failed: {e}", exc_info=True)
            raise

    def _batch_insert_papers(self, papers: List[Dict]) -> int:
        """Batch insert papers into dataset_papers"""
        if not papers:
            return 0

        try:
            insert_query = """
            INSERT INTO dataset_papers (
                corpus_id, paper_id, external_ids, title, abstract, venue, year,
                citation_count, reference_count, influential_citation_count,
                authors, fields_of_study, publication_types,
                is_open_access, open_access_pdf, conference_normalized,
                source_file, release_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            params_list = [
                (
                    p['corpus_id'], p['paper_id'], p['external_ids'], p['title'],
                    p['abstract'], p['venue'], p['year'], p['citation_count'],
                    p['reference_count'], p['influential_citation_count'],
                    p['authors'], p['fields_of_study'], p['publication_types'],
                    p['is_open_access'], p['open_access_pdf'], p['conference_normalized'],
                    p['source_file'], p['release_id']
                )
                for p in papers
            ]

            self.db_manager.execute_batch_query(insert_query, params_list)
            return len(papers)

        except Exception as e:
            self.logger.error(f"Batch insert failed: {e}")
            raise

    def _batch_update_papers(self, papers: List[Dict]) -> int:
        """Batch update papers in dataset_papers"""
        if not papers:
            return 0

        try:
            update_query = """
            UPDATE dataset_papers SET
                paper_id = %s,
                external_ids = %s,
                title = %s,
                abstract = %s,
                venue = %s,
                year = %s,
                citation_count = %s,
                reference_count = %s,
                influential_citation_count = %s,
                authors = %s,
                fields_of_study = %s,
                publication_types = %s,
                is_open_access = %s,
                open_access_pdf = %s,
                conference_normalized = %s,
                source_file = %s,
                release_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE corpus_id = %s
            """

            params_list = [
                (
                    p['paper_id'], p['external_ids'], p['title'], p['abstract'],
                    p['venue'], p['year'], p['citation_count'], p['reference_count'],
                    p['influential_citation_count'], p['authors'], p['fields_of_study'],
                    p['publication_types'], p['is_open_access'], p['open_access_pdf'],
                    p['conference_normalized'], p['source_file'], p['release_id'],
                    p['corpus_id']
                )
                for p in papers
            ]

            self.db_manager.execute_batch_query(update_query, params_list)
            return len(papers)

        except Exception as e:
            self.logger.error(f"Batch update failed: {e}")
            raise
