"""
Author Papers Extractor Service
Extracts author-paper pairs from dataset_papers and populates dataset_author_papers table
Optimized version: Direct extraction from dataset_papers (conference papers only)
"""

import json
import logging
from datetime import datetime
from typing import Dict, List

from ...database.connection import DatabaseManager
from ...database.repositories.dataset_release import DatasetReleaseRepository


class AuthorPapersExtractor:
    """
    Extracts author-paper pairs from dataset_papers (conference papers)
    Expands JSONB authors array into individual rows
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.release_repo = DatasetReleaseRepository(db_manager)
        self.logger = self._setup_logger()

        # Statistics
        self.total_papers_processed = 0
        self.total_papers_found = 0
        self.total_inserted = 0

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

    def extract_and_populate_author_papers(self, batch_size: int = 5000) -> Dict:
        """
        Extract author-paper pairs from dataset_papers only

        Args:
            batch_size: Number of papers to process per batch

        Returns:
            Dictionary with extraction statistics
        """
        start_time = datetime.now()

        self.logger.info("="*80)
        self.logger.info("Starting author papers extraction (from dataset_papers)")
        self.logger.info("="*80)

        try:
            # Get total count
            count_query = "SELECT COUNT(*) as cnt FROM dataset_papers"
            result = self.db_manager.fetch_one(count_query)
            total_papers = result['cnt'] if result else 0

            self.logger.info(f"Total conference papers to process: {total_papers:,}")

            # Process in batches using cursor pagination
            last_corpus_id = 0

            while True:
                # Fetch batch of papers
                query = """
                SELECT
                    corpus_id, paper_id, external_ids, title, abstract, venue, year,
                    citation_count, reference_count, influential_citation_count,
                    authors, fields_of_study, publication_types,
                    is_open_access, open_access_pdf, source_file, release_id
                FROM dataset_papers
                WHERE corpus_id > %s
                ORDER BY corpus_id
                LIMIT %s
                """

                papers = self.db_manager.fetch_all(query, (last_corpus_id, batch_size))

                if not papers:
                    break  # All papers processed

                # Extract author-paper pairs from this batch
                records = []
                for paper in papers:
                    authors_jsonb = paper.get('authors', [])

                    # Parse JSONB (may be string or list)
                    if isinstance(authors_jsonb, str):
                        try:
                            authors_list = json.loads(authors_jsonb)
                        except json.JSONDecodeError:
                            self.logger.warning(
                                f"Failed to parse authors JSON for corpus_id={paper['corpus_id']}"
                            )
                            authors_list = []
                    else:
                        authors_list = authors_jsonb if authors_jsonb else []

                    # Create record for each author
                    for idx, author_dict in enumerate(authors_list):
                        author_id = author_dict.get('authorId')
                        if not author_id:
                            continue

                        record = {
                            'corpus_id': paper['corpus_id'],
                            'author_id': author_id,
                            'author_name': author_dict.get('name', ''),
                            'author_sequence': idx,
                            'paper_id': paper.get('paper_id'),
                            'external_ids': paper.get('external_ids'),
                            'title': paper['title'],
                            'abstract': paper.get('abstract'),
                            'venue': paper.get('venue'),
                            'year': paper.get('year'),
                            'citation_count': paper.get('citation_count', 0),
                            'reference_count': paper.get('reference_count', 0),
                            'influential_citation_count': paper.get('influential_citation_count', 0),
                            'fields_of_study': paper.get('fields_of_study'),
                            'publication_types': paper.get('publication_types'),
                            'is_open_access': paper.get('is_open_access', False),
                            'open_access_pdf': paper.get('open_access_pdf'),
                            'is_conference_paper': True,  # All from dataset_papers
                            'source_file': paper.get('source_file'),
                            'release_id': paper['release_id']
                        }
                        records.append(record)

                    last_corpus_id = paper['corpus_id']

                # Batch insert
                if records:
                    inserted = self._batch_insert_optimized(records)
                    self.total_inserted += inserted
                    self.total_papers_found += len(records)

                self.total_papers_processed += len(papers)

                # Log progress
                self.logger.info(
                    f"Progress: {self.total_papers_processed:,}/{total_papers:,} papers processed, "
                    f"{self.total_papers_found:,} author-paper pairs created"
                )

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            self.logger.info("="*80)
            self.logger.info("Author papers extraction completed!")
            self.logger.info(f"Papers processed: {self.total_papers_processed:,}")
            self.logger.info(f"Author-paper pairs created: {self.total_papers_found:,}")
            self.logger.info(f"Records inserted: {self.total_inserted:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.2f} minutes)")
            self.logger.info("="*80)

            return {
                'status': 'completed',
                'total_papers': self.total_papers_processed,
                'total_papers_found': self.total_papers_found,
                'total_inserted': self.total_inserted,
                'processing_time_seconds': processing_time
            }

        except Exception as e:
            self.logger.error(f"Author papers extraction failed: {e}", exc_info=True)
            raise

    def _batch_insert_optimized(self, records: List[Dict]) -> int:
        """
        Batch insert using ON CONFLICT DO NOTHING

        Args:
            records: List of author-paper pair records

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        try:
            insert_query = """
            INSERT INTO dataset_author_papers (
                corpus_id, author_id, author_name, author_sequence,
                paper_id, external_ids, title, abstract, venue, year,
                citation_count, reference_count, influential_citation_count,
                fields_of_study, publication_types,
                is_open_access, open_access_pdf, is_conference_paper,
                source_file, release_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (corpus_id, author_id) DO NOTHING
            """

            params_list = [
                (
                    r['corpus_id'], r['author_id'], r['author_name'], r['author_sequence'],
                    r['paper_id'], r['external_ids'], r['title'], r['abstract'],
                    r['venue'], r['year'],
                    r['citation_count'], r['reference_count'], r['influential_citation_count'],
                    r['fields_of_study'], r['publication_types'],
                    r['is_open_access'], r['open_access_pdf'], r['is_conference_paper'],
                    r['source_file'], r['release_id']
                )
                for r in records
            ]

            self.db_manager.execute_batch_query(insert_query, params_list)
            return len(records)

        except Exception as e:
            self.logger.error(f"Batch insert failed: {e}")
            raise
