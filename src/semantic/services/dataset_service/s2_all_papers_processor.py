"""
S2 All Papers Processor with Pandas and UPSERT Logic
Processes S2 dataset files, imports papers with venue filtering
Note: Papers with empty venue field will be skipped during import
"""

import gzip
import json
import logging
import os
import time
import csv
import pandas as pd
from io import StringIO
from datetime import datetime
from pathlib import Path
from typing import Generator, Dict, Tuple, Optional
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

from ...database.connection import DatabaseManager
from ...database.repositories.dataset_release import DatasetReleaseRepository


class S2AllPapersProcessor:
    """
    S2 All Papers Processor using pandas for efficient batch processing
    Imports papers from S2 dataset with venue filtering
    Papers with empty venue field will be skipped
    Implements UPSERT logic to ensure only latest release_id is kept for each corpus_id
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.release_repo = DatasetReleaseRepository(db_manager)
        self.logger = self._setup_logger()

        # Statistics counters
        self.total_processed = 0
        self.total_inserted = 0

        # Load venue mapping table into memory for fast lookup
        self.venue_mapping = self._load_venue_mapping()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.S2AllPapersProcessor')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _load_venue_mapping(self) -> dict:
        """
        Load venue_mapping table into memory for O(1) lookup performance
        Returns dict mapping venue_raw -> conference_name (e.g., 'CVPR 2020' -> 'CVPR')

        Expected memory usage: ~10-20MB for 90K mappings
        """
        try:
            query = "SELECT venue_raw, conference_name FROM venue_mapping"
            results = self.db_manager.fetch_all(query)

            if not results:
                self.logger.warning(
                    "venue_mapping table is empty or does not exist. "
                    "venue_normalized will be NULL for all papers. "
                    "Please run: uv run python scripts/build_venue_mapping.py"
                )
                return {}

            mapping = {row['venue_raw']: row['conference_name'] for row in results}
            self.logger.info(f"Loaded {len(mapping):,} venue mappings into memory (~{len(mapping)*100//1024}KB)")

            return mapping

        except Exception as e:
            self.logger.warning(
                f"Failed to load venue_mapping table: {e}. "
                f"venue_normalized will be NULL for all papers."
            )
            return {}

    def parse_jsonl_gz_to_dataframe(self, file_path: str, chunk_size: int = 500000) -> Generator[pd.DataFrame, None, None]:
        """
        Stream parse .jsonl.gz file, return DataFrames of papers with venue
        Yields DataFrame chunks every 500k papers (optimized for fast import)
        Papers with empty venue will be skipped
        """
        papers_list = []
        line_count = 0

        self.logger.info(f"Parsing file: {file_path} (chunk_size={chunk_size:,})")

        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line_count += 1
                    self.total_processed += 1

                    try:
                        paper_json = json.loads(line)

                        # Parse paper with venue filtering
                        paper_dict = self._parse_s2_paper(
                            paper_json,
                            os.path.basename(file_path),
                            self.release_id
                        )

                        if paper_dict:  # Only add if paper has venue
                            papers_list.append(paper_dict)

                        # Yield chunk with configurable size
                        if len(papers_list) >= chunk_size:
                            self.logger.info(f"Yielding chunk: {len(papers_list):,} papers (processed {line_count:,} lines)")
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
                            f"{len(papers_list):,} papers in current batch"
                        )

            # Yield remaining papers
            if papers_list:
                self.logger.info(f"Yielding final chunk: {len(papers_list):,} papers")
                yield pd.DataFrame(papers_list)

            self.logger.info(f"Finished parsing file: {file_path} ({line_count:,} lines total)")

        except Exception as e:
            self.logger.error(f"Fatal error parsing file {file_path}: {e}")
            raise

    def _parse_s2_paper(self, json_obj: Dict, source_file: str, release_id: str) -> Optional[Dict]:
        """
        Parse S2 JSON to flat dictionary (suitable for DataFrame)
        Handles both camelCase and lowercase field names from S2 dataset
        Returns None if required fields are missing
        """
        # Get corpusId (dataset uses lowercase: corpusid)
        corpus_id = json_obj.get('corpusId') or json_obj.get('corpusid')

        # Get title
        title = json_obj.get('title')

        # Get venue
        venue = json_obj.get('venue')

        # Skip if missing required fields (corpus_id, title, or venue)
        if not corpus_id or not title:
            return None

        # Filter: Skip papers with empty venue
        if not venue or (isinstance(venue, str) and venue.strip() == ''):
            return None

        # Get URL
        url = json_obj.get('url', '')

        # Get paperId - first try direct field, then extract from URL
        paper_id = json_obj.get('paperId') or json_obj.get('paperid')
        if not paper_id and url:
            # Extract paperId from URL: https://www.semanticscholar.org/paper/<40-char-hash>
            import re
            match = re.search(r'/paper/([a-f0-9]{40})', url)
            if match:
                paper_id = match.group(1)

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

        # Compute venue_normalized using in-memory mapping (O(1) lookup)
        venue_normalized = self.venue_mapping.get(venue) if venue else None

        return {
            'corpus_id': corpus_id,
            'paper_id': paper_id,
            'url': url,
            'title': title,
            'abstract': json_obj.get('abstract'),
            'venue': json_obj.get('venue'),
            'venue_normalized': venue_normalized,  # Computed inline during import
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
            'source_file': source_file,
            'release_id': release_id
        }

    def _psql_insert_copy(self, table, conn, keys, data_iter):
        """
        Use PostgreSQL COPY FROM for ultra-fast bulk insert
        10-50x faster than executemany INSERT

        This method is passed to pandas.to_sql(method=...)
        """
        # Get raw psycopg2 connection
        dbapi_conn = conn.connection

        with dbapi_conn.cursor() as cur:
            # Create CSV buffer
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            # Build COPY command
            columns = ', '.join([f'"{k}"' for k in keys])
            table_name = table.name

            # Fixed SQL: Use CSV format without specifying NULL
            # Let PostgreSQL use default NULL handling for CSV
            copy_sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'

            # Execute COPY
            cur.copy_expert(sql=copy_sql, file=s_buf)

    def batch_insert_papers_fast(self, df: pd.DataFrame, use_copy: bool = True) -> int:
        """
        Fast batch insert: Direct INSERT without UPSERT checks
        Optimized for first-time import
        Returns inserted_count
        """
        if df is None or df.empty:
            return 0

        try:
            # Prepare DataFrame
            insert_df = self._prepare_dataframe_for_insertion(df)

            if insert_df.empty:
                self.logger.warning("No valid records after preparation")
                return 0

            connection_string = self.db_manager.config.get_connection_string()
            engine = create_engine(connection_string)

            try:
                # Direct insert without existence checks
                # Note: JSON fields are already serialized as strings in _prepare_dataframe_for_insertion
                if use_copy:
                    # Use PostgreSQL COPY for maximum speed (10-50x faster)
                    insert_df.to_sql(
                        name='dataset_all_papers',
                        con=engine,
                        if_exists='append',
                        index=False,
                        method=self._psql_insert_copy
                    )
                else:
                    # Use standard executemany INSERT
                    # For non-COPY mode, we still need dtype mapping
                    dtype_mapping = {
                        'authors': JSONB,
                        'external_ids': JSONB,
                        'fields_of_study': JSONB,
                        'publication_types': JSONB
                    }
                    insert_df.to_sql(
                        name='dataset_all_papers',
                        con=engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=50000,
                        dtype=dtype_mapping
                    )

                inserted = len(insert_df)
                return inserted

            finally:
                engine.dispose()

        except Exception as e:
            self.logger.error(f"Batch insert failed: {e}")
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
        insert_df['url'] = insert_df['url'].fillna('')
        insert_df['venue'] = insert_df['venue'].fillna('')
        # venue_normalized can be NULL (not all venues have mappings)
        # No fillna needed - PostgreSQL will handle NULL correctly

        # OPTIMIZATION: Convert JSON fields to proper JSON strings for PostgreSQL COPY
        # This is critical: Python list/dict -> JSON string (with double quotes)
        # Using string constant '[]' instead of json.dumps([]) for better performance
        for json_field in ['authors', 'external_ids', 'fields_of_study', 'publication_types']:
            insert_df[json_field] = insert_df[json_field].apply(
                lambda x: json.dumps(x) if x is not None else '[]'
            )

        # Deduplication: if duplicate corpus_id in same batch, keep first
        insert_df = insert_df.drop_duplicates(subset=['corpus_id'], keep='first')

        return insert_df

    def get_processed_files(self) -> set:
        """
        Get set of already processed source files from database
        Returns set of filenames (e.g., {'papers_0.jsonl.gz', 'papers_1.jsonl.gz'})
        """
        try:
            query = "SELECT DISTINCT source_file FROM dataset_all_papers WHERE release_id = %s"
            results = self.db_manager.fetch_all(query, (self.release_id,))
            processed = {row['source_file'] for row in results if row['source_file']}
            self.logger.info(f"Found {len(processed)} already processed files in database")
            return processed
        except Exception as e:
            self.logger.warning(f"Could not query processed files: {e}")
            return set()

    async def process_dataset_files(self, data_dir: str, pipeline_depth: int = 3, chunk_size: int = 500000, resume: bool = False) -> Dict:
        """
        Process all downloaded dataset files with async pipeline - FAST IMPORT MODE

        Uses producer-consumer pattern to overlap parsing and database insertion.
        Optimized for first-time import with direct INSERT (no UPSERT checks).

        Args:
            data_dir: Directory containing .gz dataset files
            pipeline_depth: Max chunks buffered in queue (default: 3)
            chunk_size: Papers per chunk (default: 500k, optimized for performance)
            resume: If True, skip files that are already in database (based on source_file)
        """
        import asyncio

        start_time = datetime.now()

        # Update release status
        self.release_repo.update_release_status(
            self.release_id,
            'processing_all_papers_async',
            processing_start_time=start_time
        )

        # Get all .jsonl.gz or .gz files
        files = sorted(Path(data_dir).glob('*.jsonl.gz'))
        if not files:
            files = sorted(Path(data_dir).glob('*.gz'))

        if not files:
            self.logger.warning(f"No .jsonl.gz or .gz files found in {data_dir}")
            return {
                'release_id': self.release_id,
                'status': 'failed',
                'error': 'No dataset files found',
                'total_files': 0
            }

        # Filter out already processed files if resume mode
        if resume:
            processed_files = self.get_processed_files()
            files_before = len(files)
            files = [f for f in files if f.name not in processed_files]
            skipped_count = files_before - len(files)

            if skipped_count > 0:
                self.logger.info(f"Resume mode: Skipping {skipped_count} already processed files")
                self.logger.info(f"Remaining files to process: {len(files)}")

            if not files:
                self.logger.info("All files already processed!")
                return {
                    'release_id': self.release_id,
                    'status': 'completed',
                    'total_files': files_before,
                    'total_papers_processed': 0,
                    'papers_inserted': 0,
                    'processing_time_seconds': 0,
                    'skipped_files': skipped_count
                }

        self.logger.info(f"Found {len(files)} dataset files to process")
        self.logger.info(f"Async pipeline: depth={pipeline_depth}, chunk_size={chunk_size:,}")
        if resume:
            self.logger.info(f"Resume mode: ENABLED")

        try:
            # Create queue for pipeline (producer-consumer)
            queue = asyncio.Queue(maxsize=pipeline_depth)

            # Consumer worker: process chunks from queue and insert to DB
            async def insert_worker():
                while True:
                    item = await queue.get()

                    if item is None:  # Poison pill - end signal
                        queue.task_done()
                        break

                    df_chunk, file_idx, total_files = item

                    try:
                        # Run sync DB operation in thread pool (don't block event loop)
                        inserted = await asyncio.to_thread(
                            self.batch_insert_papers_fast, df_chunk
                        )

                        self.total_inserted += inserted

                        self.logger.info(
                            f"Progress: File {file_idx}/{total_files}, "
                            f"Processed={self.total_processed:,}, "
                            f"Inserted={self.total_inserted:,}"
                        )

                    except Exception as e:
                        self.logger.error(f"Insert worker error: {e}", exc_info=True)
                        queue.task_done()
                        raise

                    queue.task_done()

            # Start consumer worker task
            insert_task = asyncio.create_task(insert_worker())

            # Producer: parse files and feed chunks to queue
            for file_idx, file_path in enumerate(tqdm(files, desc="Processing files"), 1):
                self.logger.info(f"Processing file {file_idx}/{len(files)}: {file_path.name}")

                # Parse file with custom chunk size
                for df_chunk in self.parse_jsonl_gz_to_dataframe(str(file_path), chunk_size):
                    # Put chunk in queue (blocks if queue is full - automatic backpressure)
                    await queue.put((df_chunk, file_idx, len(files)))

            # Send poison pill to stop worker
            await queue.put(None)

            # Wait for all queued work to complete
            await queue.join()
            await insert_task

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Update release statistics
            self.release_repo.update_release_status(
                self.release_id,
                'all_papers_imported',
                processing_end_time=end_time,
                total_papers_processed=self.total_processed,
                papers_inserted=self.total_inserted
            )

            self.logger.info("="*80)
            self.logger.info("All papers import completed successfully (FAST IMPORT MODE)!")
            self.logger.info(f"Total papers processed: {self.total_processed:,}")
            self.logger.info(f"Papers inserted: {self.total_inserted:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.2f} minutes, {processing_time/3600:.2f} hours)")
            self.logger.info(f"Average speed: {self.total_processed/processing_time:.0f} papers/sec")
            self.logger.info("="*80)

            return {
                'release_id': self.release_id,
                'status': 'completed',
                'total_files': len(files),
                'total_papers_processed': self.total_processed,
                'papers_inserted': self.total_inserted,
                'processing_time_seconds': processing_time
            }

        except Exception as e:
            self.logger.error(f"Async processing failed: {e}", exc_info=True)
            self.release_repo.update_release_status(
                self.release_id,
                'failed',
                processing_end_time=datetime.now()
            )
            raise
