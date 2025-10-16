"""
Author Papers Extractor Service
Extracts all authors from dataset_papers and finds all their papers in all_papers
Populates dataset_author_papers table
"""

import logging
from datetime import datetime
from typing import Dict, List, Set
from tqdm import tqdm

from ...database.connection import DatabaseManager
from ...database.repositories.dataset_release import DatasetReleaseRepository


class AuthorPapersExtractor:
    """
    Extracts author papers using SQL queries
    1. Gets unique author_ids from dataset_papers
    2. Finds all papers by these authors in all_papers
    3. Populates dataset_author_papers table
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.release_repo = DatasetReleaseRepository(db_manager)
        self.logger = self._setup_logger()

        # Statistics
        self.total_authors = 0
        self.total_papers_found = 0
        self.total_inserted = 0
        self.total_updated = 0

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.AuthorPapersExtractor')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def extract_and_populate_author_papers(self, batch_size: int = 100) -> Dict:
        """
        Extract authors from dataset_papers and populate dataset_author_papers
        Processes authors in batches for memory efficiency
        """
        start_time = datetime.now()

        self.logger.info("="*80)
        self.logger.info("Starting author papers extraction")
        self.logger.info("="*80)

        try:
            # Step 1: Extract unique author_ids from dataset_papers
            self.logger.info("Extracting unique authors from dataset_papers...")
            author_ids = self._extract_unique_author_ids()

            if not author_ids:
                self.logger.warning("No authors found in dataset_papers")
                return {
                    'status': 'completed',
                    'total_authors': 0,
                    'total_papers_found': 0,
                    'total_inserted': 0,
                    'total_updated': 0
                }

            self.total_authors = len(author_ids)
            self.logger.info(f"Found {self.total_authors:,} unique authors")

            # Step 2: Get corpus_ids of conference papers for marking
            self.logger.info("Getting conference paper corpus_ids...")
            conference_corpus_ids = self._get_conference_corpus_ids()
            self.logger.info(f"Found {len(conference_corpus_ids):,} conference papers")

            # Step 3: Process authors in batches
            author_list = list(author_ids)
            total_batches = (len(author_list) + batch_size - 1) // batch_size

            self.logger.info(f"Processing {self.total_authors:,} authors in {total_batches} batches")

            for batch_num in tqdm(range(total_batches), desc="Extracting author papers"):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(author_list))
                batch_author_ids = author_list[start_idx:end_idx]

                # Find all papers by these authors
                papers = self._find_papers_by_authors(batch_author_ids, conference_corpus_ids)

                if papers:
                    # Separate into insert and update
                    papers_to_insert, papers_to_update = self._separate_insert_update(papers)

                    # Batch insert
                    if papers_to_insert:
                        inserted = self._batch_insert_papers(papers_to_insert)
                        self.total_inserted += inserted

                    # Batch update
                    if papers_to_update:
                        updated = self._batch_update_papers(papers_to_update)
                        self.total_updated += updated

                    self.total_papers_found += len(papers)

                if (batch_num + 1) % 10 == 0 or batch_num == total_batches - 1:
                    self.logger.info(
                        f"Progress: Batch {batch_num + 1}/{total_batches}, "
                        f"Authors processed={end_idx:,}/{self.total_authors:,}, "
                        f"Papers found={self.total_papers_found:,}, "
                        f"Inserted={self.total_inserted:,}, "
                        f"Updated={self.total_updated:,}"
                    )

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            self.logger.info("="*80)
            self.logger.info("Author papers extraction completed!")
            self.logger.info(f"Total authors processed: {self.total_authors:,}")
            self.logger.info(f"Total papers found: {self.total_papers_found:,}")
            self.logger.info(f"Papers inserted (new): {self.total_inserted:,}")
            self.logger.info(f"Papers updated (existing): {self.total_updated:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.2f} minutes)")
            self.logger.info("="*80)

            return {
                'status': 'completed',
                'total_authors': self.total_authors,
                'total_papers_found': self.total_papers_found,
                'total_inserted': self.total_inserted,
                'total_updated': self.total_updated,
                'processing_time_seconds': processing_time
            }

        except Exception as e:
            self.logger.error(f"Author papers extraction failed: {e}", exc_info=True)
            raise

    def _extract_unique_author_ids(self) -> Set[str]:
        """Extract unique author_ids from dataset_papers JSONB authors field"""
        try:
            # Use JSONB functions to extract author_ids
            query = """
            SELECT DISTINCT author_elem->>'authorId' as author_id
            FROM dataset_papers,
                 jsonb_array_elements(authors) as author_elem
            WHERE authors IS NOT NULL
              AND jsonb_array_length(authors) > 0
              AND author_elem->>'authorId' IS NOT NULL
              AND author_elem->>'authorId' != ''
            """

            results = self.db_manager.fetch_all(query)
            author_ids = {row['author_id'] for row in results if row['author_id']}

            return author_ids

        except Exception as e:
            self.logger.error(f"Failed to extract author_ids: {e}")
            raise

    def _get_conference_corpus_ids(self) -> Set[int]:
        """Get set of corpus_ids that are conference papers"""
        try:
            query = "SELECT corpus_id FROM dataset_papers"
            results = self.db_manager.fetch_all(query)
            return {row['corpus_id'] for row in results}

        except Exception as e:
            self.logger.error(f"Failed to get conference corpus_ids: {e}")
            raise

    def _find_papers_by_authors(self, author_ids: List[str], conference_corpus_ids: Set[int]) -> List[Dict]:
        """
        Find all papers in all_papers where authors array contains any of the given author_ids
        """
        try:
            # Build JSONB query to find papers by author_ids
            # For each author_id, we need to find papers where authors array contains that author

            papers_map = {}  # (corpus_id, author_id) -> paper data

            # Process each author_id (we batch them above, so this is a small list)
            for author_id in author_ids:
                query = """
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
                WHERE authors @> %s::jsonb
                """

                # Query papers where authors contains this author_id
                # Use JSONB contains operator @>
                author_filter = f'[{{"authorId": "{author_id}"}}]'
                results = self.db_manager.fetch_all(query, (author_filter,))

                for paper in results:
                    key = (paper['corpus_id'], author_id)
                    if key not in papers_map:
                        is_conference = paper['corpus_id'] in conference_corpus_ids
                        papers_map[key] = {
                            'corpus_id': paper['corpus_id'],
                            'author_id': author_id,
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
                            'is_conference_paper': is_conference,
                            'source_file': paper.get('source_file'),
                            'release_id': paper['release_id']
                        }

            return list(papers_map.values())

        except Exception as e:
            self.logger.error(f"Failed to find papers by authors: {e}")
            raise

    def _separate_insert_update(self, papers: List[Dict]) -> tuple:
        """Separate papers into insert and update lists based on existing records"""
        if not papers:
            return [], []

        try:
            # Build query to check existing (corpus_id, author_id) pairs
            corpus_author_pairs = [(p['corpus_id'], p['author_id']) for p in papers]

            # Query existing records
            placeholders = ','.join([f"({p[0]}, '{p[1]}')" for p in corpus_author_pairs])
            query = f"""
            SELECT corpus_id, author_id
            FROM dataset_author_papers
            WHERE (corpus_id, author_id) IN ({placeholders})
            """

            existing = self.db_manager.fetch_all(query)
            existing_pairs = {(row['corpus_id'], row['author_id']) for row in existing}

            papers_to_insert = []
            papers_to_update = []

            for paper in papers:
                key = (paper['corpus_id'], paper['author_id'])
                if key in existing_pairs:
                    papers_to_update.append(paper)
                else:
                    papers_to_insert.append(paper)

            return papers_to_insert, papers_to_update

        except Exception as e:
            self.logger.error(f"Failed to separate insert/update: {e}")
            raise

    def _batch_insert_papers(self, papers: List[Dict]) -> int:
        """Batch insert papers into dataset_author_papers"""
        if not papers:
            return 0

        try:
            insert_query = """
            INSERT INTO dataset_author_papers (
                corpus_id, author_id, paper_id, external_ids, title, abstract, venue, year,
                citation_count, reference_count, influential_citation_count,
                authors, fields_of_study, publication_types,
                is_open_access, open_access_pdf, is_conference_paper,
                source_file, release_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            params_list = [
                (
                    p['corpus_id'], p['author_id'], p['paper_id'], p['external_ids'],
                    p['title'], p['abstract'], p['venue'], p['year'],
                    p['citation_count'], p['reference_count'], p['influential_citation_count'],
                    p['authors'], p['fields_of_study'], p['publication_types'],
                    p['is_open_access'], p['open_access_pdf'], p['is_conference_paper'],
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
        """Batch update papers in dataset_author_papers"""
        if not papers:
            return 0

        try:
            update_query = """
            UPDATE dataset_author_papers SET
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
                is_conference_paper = %s,
                source_file = %s,
                release_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE corpus_id = %s AND author_id = %s
            """

            params_list = [
                (
                    p['paper_id'], p['external_ids'], p['title'], p['abstract'],
                    p['venue'], p['year'], p['citation_count'], p['reference_count'],
                    p['influential_citation_count'], p['authors'], p['fields_of_study'],
                    p['publication_types'], p['is_open_access'], p['open_access_pdf'],
                    p['is_conference_paper'], p['source_file'], p['release_id'],
                    p['corpus_id'], p['author_id']
                )
                for p in papers
            ]

            self.db_manager.execute_batch_query(update_query, params_list)
            return len(papers)

        except Exception as e:
            self.logger.error(f"Batch update failed: {e}")
            raise
