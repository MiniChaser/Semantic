"""
S2 Dataset Processor with Pandas and UPSERT Logic
Processes S2 dataset files, filters by conference, and batch inserts with UPSERT
"""

import gzip
import json
import logging
import os
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Generator, Dict, Tuple, Optional
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

from ...database.connection import DatabaseManager
from ...database.repositories.dataset_release import DatasetReleaseRepository
from .conference_matcher import ConferenceMatcher


class S2DatasetProcessorPandas:
    """
    S2 Dataset Processor using pandas for efficient batch processing
    Implements UPSERT logic to ensure only latest release_id is kept for each corpus_id
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.conference_matcher = ConferenceMatcher()
        self.release_repo = DatasetReleaseRepository(db_manager)
        self.logger = self._setup_logger()

        # Statistics counters
        self.total_processed = 0
        self.total_matched = 0
        self.total_inserted = 0
        self.total_updated = 0

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.S2DatasetProcessorPandas')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def parse_jsonl_gz_to_dataframe(self, file_path: str) -> Generator[pd.DataFrame, None, None]:
        """
        Stream parse .jsonl.gz file, return DataFrames of matched conference papers
        Yields DataFrame chunks every 100k papers to avoid memory overflow
        """
        papers_list = []
        line_count = 0

        self.logger.info(f"Parsing file: {file_path}")

        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line_count += 1
                    self.total_processed += 1

                    try:
                        paper_json = json.loads(line)
                        venue = paper_json.get('venue', '')

                        # Conference matching
                        matched_conf = self.conference_matcher.match_conference(venue)
                        if matched_conf:
                            paper_dict = self._parse_s2_paper(
                                paper_json,
                                matched_conf,
                                os.path.basename(file_path),
                                self.release_id
                            )
                            papers_list.append(paper_dict)
                            self.total_matched += 1

                        # Yield chunk every 100k papers
                        if len(papers_list) >= 100000:
                            self.logger.info(f"Yielding chunk: {len(papers_list)} papers (processed {line_count} lines)")
                            yield pd.DataFrame(papers_list)
                            papers_list = []

                    except json.JSONDecodeError as e:
                        self.logger.debug(f"JSON decode error on line {line_count}: {e}")
                        continue
                    except Exception as e:
                        self.logger.error(f"Error parsing line {line_count}: {e}")
                        continue

                    # Progress log every 1M lines
                    if line_count % 1000000 == 0:
                        self.logger.info(
                            f"Progress: {line_count:,} lines processed, "
                            f"{self.total_matched:,} matched papers"
                        )

            # Yield remaining papers
            if papers_list:
                self.logger.info(f"Yielding final chunk: {len(papers_list)} papers")
                yield pd.DataFrame(papers_list)

            self.logger.info(f"Finished parsing file: {file_path} ({line_count:,} lines total)")

        except Exception as e:
            self.logger.error(f"Fatal error parsing file {file_path}: {e}")
            raise

    def _parse_s2_paper(self, json_obj: Dict, conference: str,
                       source_file: str, release_id: str) -> Dict:
        """
        Parse S2 JSON to flat dictionary (suitable for DataFrame)
        Handles both camelCase and lowercase field names from S2 dataset
        """
        # Get corpusId (dataset uses lowercase: corpusid)
        corpus_id = json_obj.get('corpusId') or json_obj.get('corpusid')

        # Get paperId (dataset uses lowercase: paperid)
        paper_id = json_obj.get('paperId') or json_obj.get('paperid')

        # Get citation counts (dataset uses lowercase)
        citation_count = json_obj.get('citationCount') or json_obj.get('citationcount') or 0
        reference_count = json_obj.get('referenceCount') or json_obj.get('referencecount') or 0
        influential_count = json_obj.get('influentialCitationCount') or json_obj.get('influentialcitationcount') or 0

        # Get isOpenAccess (dataset uses lowercase: isopenaccess)
        is_open_access = json_obj.get('isOpenAccess') or json_obj.get('isopenaccess') or False
        if isinstance(is_open_access, str):
            is_open_access = is_open_access.lower() == 'true'

        # Get openAccessPdf (dataset might use lowercase: openaccesspdf)
        open_access_pdf = None
        open_access_field = json_obj.get('openAccessPdf') or json_obj.get('openaccesspdf')
        if open_access_field:
            if isinstance(open_access_field, dict):
                open_access_pdf = open_access_field.get('url')
            elif isinstance(open_access_field, str):
                open_access_pdf = open_access_field

        # Get externalIds (dataset uses lowercase: externalids)
        external_ids = json_obj.get('externalIds') or json_obj.get('externalids') or {}

        # Get fieldsOfStudy or s2fieldsofstudy
        fields_of_study = (json_obj.get('fieldsOfStudy') or
                          json_obj.get('fieldsofstudy') or
                          json_obj.get('s2fieldsofstudy') or
                          json_obj.get('s2FieldsOfStudy') or [])

        # Get publicationTypes (dataset uses lowercase: publicationtypes)
        pub_types = json_obj.get('publicationTypes') or json_obj.get('publicationtypes') or []

        return {
            'corpus_id': corpus_id,
            'paper_id': paper_id,
            'title': json_obj.get('title'),
            'abstract': json_obj.get('abstract'),
            'venue': json_obj.get('venue'),
            'year': json_obj.get('year'),
            'citation_count': citation_count,
            'reference_count': reference_count,
            'influential_citation_count': influential_count,
            'authors': json_obj.get('authors', []),  # Pass as Python list, SQLAlchemy JSONB will handle conversion
            'external_ids': external_ids,  # Pass as Python dict, SQLAlchemy JSONB will handle conversion
            'fields_of_study': fields_of_study,  # Pass as Python list, SQLAlchemy JSONB will handle conversion
            'publication_types': pub_types if pub_types else [],  # Pass as Python list, SQLAlchemy JSONB will handle conversion
            'is_open_access': is_open_access,
            'open_access_pdf': open_access_pdf,
            'conference_normalized': conference,
            'source_file': source_file,
            'release_id': release_id
        }

    def batch_upsert_papers_pandas(self, df: pd.DataFrame) -> Tuple[int, int]:
        """
        Batch UPSERT: Keep only latest release_id for each corpus_id
        Returns (inserted_count, updated_count)
        """
        if df is None or df.empty:
            return 0, 0

        try:
            from sqlalchemy import create_engine, text

            # Prepare DataFrame
            insert_df = self._prepare_dataframe_for_insertion(df)

            if insert_df.empty:
                self.logger.warning("No valid records after preparation")
                return 0, 0

            # Query which corpus_ids already exist
            corpus_ids = insert_df['corpus_id'].tolist()
            placeholders = ','.join(['%s'] * len(corpus_ids))
            query = f"SELECT corpus_id FROM dataset_papers WHERE corpus_id IN ({placeholders})"
            existing = self.db_manager.fetch_all(query, tuple(corpus_ids))
            existing_ids = {row['corpus_id'] for row in existing}

            # Separate new records and update records
            new_records = insert_df[~insert_df['corpus_id'].isin(existing_ids)]
            update_records = insert_df[insert_df['corpus_id'].isin(existing_ids)]

            connection_string = self.db_manager.config.get_connection_string()
            engine = create_engine(connection_string)

            inserted = 0
            updated = 0

            # 1. Insert new records (using pandas.to_sql)
            if not new_records.empty:
                # Define dtype mapping for JSONB columns
                dtype_mapping = {
                    'authors': JSONB,
                    'external_ids': JSONB,
                    'fields_of_study': JSONB,
                    'publication_types': JSONB
                }

                new_records.to_sql(
                    name='dataset_papers',
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=5000,
                    dtype=dtype_mapping
                )
                inserted = len(new_records)
                self.logger.info(f"Inserted {inserted} new papers")

            # 2. Update existing records (using batch UPDATE)
            if not update_records.empty:
                updated = self._batch_update_papers(engine, update_records)
                self.logger.info(f"Updated {updated} existing papers")

            engine.dispose()
            return inserted, updated

        except Exception as e:
            self.logger.error(f"Batch upsert failed: {e}")
            raise

    def _batch_update_papers(self, engine, df: pd.DataFrame) -> int:
        """
        Batch update existing paper records
        Uses temporary table + UPDATE FROM for efficient batch update
        """
        try:
            # Create temp table name
            temp_table_name = f'temp_update_{int(time.time() * 1000)}'

            # Write update data to temp table
            # Define dtype mapping for JSONB columns
            dtype_mapping = {
                'authors': JSONB,
                'external_ids': JSONB,
                'fields_of_study': JSONB,
                'publication_types': JSONB
            }

            df.to_sql(
                name=temp_table_name,
                con=engine,
                if_exists='replace',
                index=False,
                method='multi',
                dtype=dtype_mapping
            )

            # Batch UPDATE using UPDATE FROM
            update_sql = f"""
            UPDATE dataset_papers
            SET
                paper_id = temp.paper_id,
                external_ids = temp.external_ids::jsonb,
                title = temp.title,
                abstract = temp.abstract,
                venue = temp.venue,
                year = temp.year,
                citation_count = temp.citation_count,
                reference_count = temp.reference_count,
                influential_citation_count = temp.influential_citation_count,
                authors = temp.authors::jsonb,
                fields_of_study = temp.fields_of_study::jsonb,
                publication_types = temp.publication_types::jsonb,
                is_open_access = temp.is_open_access,
                open_access_pdf = temp.open_access_pdf,
                conference_normalized = temp.conference_normalized,
                source_file = temp.source_file,
                release_id = temp.release_id,
                updated_at = CURRENT_TIMESTAMP
            FROM {temp_table_name} temp
            WHERE dataset_papers.corpus_id = temp.corpus_id;
            """

            with engine.connect() as conn:
                result = conn.execute(text(update_sql))
                conn.commit()
                updated_count = result.rowcount

            # Drop temp table
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                conn.commit()

            return updated_count

        except Exception as e:
            self.logger.error(f"Batch update failed: {e}")
            # Try to clean up temp table
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                    conn.commit()
            except:
                pass
            raise

    def _prepare_dataframe_for_insertion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame: type conversion, NULL handling, deduplication
        """
        insert_df = df.copy()

        # Data validation: remove invalid records
        insert_df = insert_df[insert_df['corpus_id'].notna()]
        insert_df = insert_df[insert_df['title'].notna()]
        insert_df = insert_df[insert_df['title'].str.strip() != '']

        if insert_df.empty:
            return insert_df

        # Type conversion
        insert_df['corpus_id'] = insert_df['corpus_id'].astype('int64')
        insert_df['year'] = pd.to_numeric(insert_df['year'], errors='coerce').astype('Int64')
        insert_df['citation_count'] = pd.to_numeric(insert_df['citation_count'], errors='coerce').fillna(0).astype('int32')
        insert_df['reference_count'] = pd.to_numeric(insert_df['reference_count'], errors='coerce').fillna(0).astype('int32')
        insert_df['influential_citation_count'] = pd.to_numeric(insert_df['influential_citation_count'], errors='coerce').fillna(0).astype('int32')
        insert_df['is_open_access'] = insert_df['is_open_access'].fillna(False).astype('bool')

        # Handle NULL values
        insert_df['abstract'] = insert_df['abstract'].fillna('')
        insert_df['open_access_pdf'] = insert_df['open_access_pdf'].fillna('')
        insert_df['paper_id'] = insert_df['paper_id'].fillna('')
        insert_df['venue'] = insert_df['venue'].fillna('')

        # Deduplication: if duplicate corpus_id in same batch, keep first
        insert_df = insert_df.drop_duplicates(subset=['corpus_id'], keep='first')

        return insert_df

    def process_dataset_files(self, data_dir: str) -> Dict:
        """
        Process all downloaded dataset files (main workflow)
        """
        start_time = datetime.now()

        # Update release status
        self.release_repo.update_release_status(
            self.release_id,
            'processing',
            processing_start_time=start_time
        )

        # Get all .jsonl.gz or .gz files
        files = sorted(Path(data_dir).glob('*.jsonl.gz'))
        if not files:
            # Try to find .gz files (S2 dataset may use either format)
            files = sorted(Path(data_dir).glob('*.gz'))

        self.logger.info(f"Found {len(files)} dataset files to process")

        if not files:
            self.logger.warning(f"No .jsonl.gz or .gz files found in {data_dir}")
            return {
                'release_id': self.release_id,
                'status': 'failed',
                'error': 'No dataset files found',
                'total_files': 0
            }

        try:
            # Process each file
            for file_path in tqdm(files, desc="Processing files"):
                self.logger.info(f"Processing: {file_path.name}")

                # Stream process file (returns DataFrame generator)
                for df_chunk in self.parse_jsonl_gz_to_dataframe(str(file_path)):
                    # UPSERT this chunk
                    inserted, updated = self.batch_upsert_papers_pandas(df_chunk)
                    self.total_inserted += inserted
                    self.total_updated += updated

                    self.logger.info(
                        f"Progress: Processed={self.total_processed:,}, "
                        f"Matched={self.total_matched:,}, "
                        f"Inserted={self.total_inserted:,}, "
                        f"Updated={self.total_updated:,}"
                    )

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Update release statistics
            self.release_repo.update_release_status(
                self.release_id,
                'completed',
                processing_end_time=end_time,
                total_papers_processed=self.total_processed,
                papers_inserted=self.total_inserted,
                papers_updated=self.total_updated
            )

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Update release statistics
            self.release_repo.update_release_status(
                self.release_id,
                'completed',
                processing_end_time=end_time,
                total_papers_processed=self.total_processed,
                papers_inserted=self.total_inserted,
                papers_updated=self.total_updated
            )

            self.logger.info("="*80)
            self.logger.info("Processing completed successfully!")
            self.logger.info(f"Total papers processed: {self.total_processed:,}")
            self.logger.info(f"Papers matched to conferences: {self.total_matched:,}")
            self.logger.info(f"Papers inserted (new): {self.total_inserted:,}")
            self.logger.info(f"Papers updated (existing): {self.total_updated:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s")
            self.logger.info("="*80)

            return {
                'release_id': self.release_id,
                'status': 'completed',
                'total_files': len(files),
                'total_papers_processed': self.total_processed,
                'papers_matched': self.total_matched,
                'papers_inserted': self.total_inserted,
                'papers_updated': self.total_updated,
                'processing_time_seconds': processing_time
            }

        except Exception as e:
            self.logger.error(f"Processing failed: {e}", exc_info=True)
            self.release_repo.update_release_status(
                self.release_id,
                'failed',
                processing_end_time=datetime.now()
            )
            raise
