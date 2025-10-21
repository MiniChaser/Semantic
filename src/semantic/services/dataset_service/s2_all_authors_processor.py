"""
S2 All Authors Processor with Pandas and Fast Import
Processes S2 dataset author files, imports ALL authors without filtering
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
from typing import Generator, Dict, Optional
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB

from ...database.connection import DatabaseManager
from ...database.repositories.dataset_release import DatasetReleaseRepository


class S2AllAuthorsProcessor:
    """
    S2 All Authors Processor using pandas for efficient batch processing
    Imports ALL authors from S2 dataset without any filtering
    Fast import mode: Direct INSERT using PostgreSQL COPY (no UPSERT)
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.release_repo = DatasetReleaseRepository(db_manager)
        self.logger = self._setup_logger()

        # Statistics counters
        self.total_processed = 0
        self.total_inserted = 0

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.S2AllAuthorsProcessor')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def parse_jsonl_gz_to_dataframe(self, file_path: str, chunk_size: int = 500000) -> Generator[pd.DataFrame, None, None]:
        """
        Stream parse .jsonl.gz file, return DataFrames of ALL authors
        Yields DataFrame chunks every 500k authors (optimized for fast import)
        """
        authors_list = []
        line_count = 0

        self.logger.info(f"Parsing file: {file_path} (chunk_size={chunk_size:,})")

        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line_count += 1
                    self.total_processed += 1

                    try:
                        author_json = json.loads(line)

                        # Import ALL authors (no filtering)
                        author_dict = self._parse_s2_author(
                            author_json,
                            os.path.basename(file_path),
                            self.release_id
                        )

                        if author_dict:  # Only skip if parsing failed
                            authors_list.append(author_dict)

                        # Yield chunk with configurable size
                        if len(authors_list) >= chunk_size:
                            self.logger.info(f"Yielding chunk: {len(authors_list):,} authors (processed {line_count:,} lines)")
                            yield pd.DataFrame(authors_list)
                            authors_list = []

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
                            f"{len(authors_list):,} authors in current batch"
                        )

            # Yield remaining authors
            if authors_list:
                self.logger.info(f"Yielding final chunk: {len(authors_list):,} authors")
                yield pd.DataFrame(authors_list)

            self.logger.info(f"Finished parsing file: {file_path} ({line_count:,} lines total)")

        except Exception as e:
            self.logger.error(f"Fatal error parsing file {file_path}: {e}")
            raise

    def _parse_s2_author(self, json_obj: Dict, source_file: str, release_id: str) -> Optional[Dict]:
        """
        Parse S2 JSON to flat dictionary (suitable for DataFrame)
        Note: All field names in dataset are lowercase (authorid, papercount, etc.)
        Returns None if required fields are missing
        """
        # Get authorid (lowercase in dataset)
        author_id = json_obj.get('authorid')

        # Get name
        name = json_obj.get('name')

        # Skip if missing required fields
        if not author_id or not name:
            return None

        return {
            'author_id': author_id,
            'name': name,
            'aliases': json_obj.get('aliases'),  # list or None
            'affiliations': json_obj.get('affiliations'),  # list or None
            'homepage': json_obj.get('homepage'),
            'paper_count': json_obj.get('papercount', 0),
            'citation_count': json_obj.get('citationcount', 0),
            'h_index': json_obj.get('hindex', 0),
            'external_ids': json_obj.get('externalids'),  # dict or None
            'url': json_obj.get('url'),
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

            # Use CSV format with default NULL handling
            copy_sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'

            # Execute COPY
            cur.copy_expert(sql=copy_sql, file=s_buf)

    def batch_insert_authors_fast(self, df: pd.DataFrame, use_copy: bool = True) -> int:
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
                        name='dataset_authors',
                        con=engine,
                        if_exists='append',
                        index=False,
                        method=self._psql_insert_copy
                    )
                else:
                    # Use standard executemany INSERT
                    # For non-COPY mode, we still need dtype mapping
                    dtype_mapping = {
                        'aliases': JSONB,
                        'affiliations': JSONB,
                        'external_ids': JSONB
                    }
                    insert_df.to_sql(
                        name='dataset_authors',
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
        insert_df = insert_df[insert_df['author_id'].notna()]
        insert_df = insert_df[insert_df['name'].notna()]
        insert_df = insert_df[insert_df['name'].str.strip() != '']

        if insert_df.empty:
            return insert_df

        # Type conversion
        insert_df['paper_count'] = pd.to_numeric(insert_df['paper_count'], errors='coerce').fillna(0).astype('int32')
        insert_df['citation_count'] = pd.to_numeric(insert_df['citation_count'], errors='coerce').fillna(0).astype('int32')
        insert_df['h_index'] = pd.to_numeric(insert_df['h_index'], errors='coerce').fillna(0).astype('int32')

        # Handle NULL values for text fields
        insert_df['homepage'] = insert_df['homepage'].fillna('')
        insert_df['url'] = insert_df['url'].fillna('')

        # OPTIMIZATION: Convert JSON fields to proper JSON strings using vectorized operation
        # Using json.dumps on entire Series is much faster than applying to each row
        for json_field in ['aliases', 'affiliations', 'external_ids']:
            # Convert None to 'null' string, keep others as JSON strings
            insert_df[json_field] = insert_df[json_field].apply(
                lambda x: json.dumps(x) if x is not None else 'null'
            )

        # Deduplication: if duplicate author_id in same batch, keep first
        insert_df = insert_df.drop_duplicates(subset=['author_id'], keep='first')

        return insert_df

    def get_processed_files(self) -> set:
        """
        Get set of already processed source files from database
        Returns set of filenames (e.g., {'authors_0.gz', 'authors_1.gz'})
        """
        try:
            query = "SELECT DISTINCT source_file FROM dataset_authors WHERE release_id = %s"
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
            chunk_size: Authors per chunk (default: 500k, optimized for performance)
            resume: If True, skip files that are already in database (based on source_file)
        """
        import asyncio

        start_time = datetime.now()

        # Update release status
        self.release_repo.update_release_status(
            self.release_id,
            'processing_all_authors_async',
            processing_start_time=start_time
        )

        # Get all .gz files
        files = sorted(Path(data_dir).glob('*.gz'))

        if not files:
            self.logger.warning(f"No .gz files found in {data_dir}")
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
                    'total_authors_processed': 0,
                    'authors_inserted': 0,
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
                            self.batch_insert_authors_fast, df_chunk
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
                'all_authors_imported',
                processing_end_time=end_time,
                total_papers_processed=self.total_processed,
                papers_inserted=self.total_inserted
            )

            self.logger.info("="*80)
            self.logger.info("All authors import completed successfully (FAST IMPORT MODE)!")
            self.logger.info(f"Total authors processed: {self.total_processed:,}")
            self.logger.info(f"Authors inserted: {self.total_inserted:,}")
            self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.2f} minutes, {processing_time/3600:.2f} hours)")
            self.logger.info(f"Average speed: {self.total_processed/processing_time:.0f} authors/sec")
            self.logger.info("="*80)

            return {
                'release_id': self.release_id,
                'status': 'completed',
                'total_files': len(files),
                'total_authors_processed': self.total_processed,
                'authors_inserted': self.total_inserted,
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
