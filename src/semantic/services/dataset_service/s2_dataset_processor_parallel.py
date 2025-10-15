"""
S2 Dataset Processor with SQL-based filtering and parallel processing

Key improvements:
- No Python-side conference matching (uses SQL JOIN)
- No Bloom Filter or existing ID queries (uses ON CONFLICT)
- Parallel file processing with multiprocessing
- Dynamic worker count calculation
"""

import gzip
import json
import logging
import multiprocessing
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from tqdm import tqdm

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

from ...database.connection import DatabaseManager
from ...database.repositories.dataset_release import DatasetReleaseRepository
from .conference_pattern_setup import setup_conference_patterns, check_conference_patterns_exist
from .processing_config import ProcessingConfig


def process_single_file(args: Tuple[str, str, Dict]) -> Dict:
    """
    Worker function to process a single .gz file

    Args:
        args: (file_path, release_id, db_config)

    Returns:
        Result dictionary with status and statistics
    """
    file_path, release_id, db_config = args
    worker_id = os.getpid()
    temp_table = f'temp_import_{worker_id}'

    logger = logging.getLogger(f'{__name__}.Worker-{worker_id}')

    try:
        # Create database engine for this worker
        engine = create_engine(db_config['connection_string'], pool_pre_ping=True)

        # 1. Parse JSON to list (NO conference filtering in Python)
        papers = []
        line_count = 0

        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                line_count += 1
                try:
                    paper = json.loads(line)

                    # Extract all fields (keep venue for SQL matching)
                    papers.append({
                        'corpus_id': paper.get('corpusid') or paper.get('corpusId'),
                        'paper_id': paper.get('paperid') or paper.get('paperId'),
                        'title': paper.get('title'),
                        'abstract': paper.get('abstract'),
                        'venue': paper.get('venue', ''),  # Keep original venue for SQL matching
                        'year': paper.get('year'),
                        'citation_count': paper.get('citationcount') or paper.get('citationCount') or 0,
                        'reference_count': paper.get('referencecount') or paper.get('referenceCount') or 0,
                        'influential_citation_count': paper.get('influentialcitationcount') or paper.get('influentialCitationCount') or 0,
                        'authors': paper.get('authors', []),
                        'external_ids': paper.get('externalids') or paper.get('externalIds') or {},
                        'fields_of_study': (paper.get('fieldsofstudy') or
                                          paper.get('fieldsOfStudy') or
                                          paper.get('s2fieldsofstudy') or
                                          paper.get('s2FieldsOfStudy') or []),
                        'publication_types': paper.get('publicationtypes') or paper.get('publicationTypes') or [],
                        'is_open_access': paper.get('isopenaccess') or paper.get('isOpenAccess') or False,
                        'open_access_pdf': _extract_open_access_pdf(paper),
                        'source_file': os.path.basename(file_path),
                        'release_id': release_id
                    })

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing line {line_count}: {e}")
                    continue

        if not papers:
            return {
                'file': os.path.basename(file_path),
                'total_lines': line_count,
                'matched_papers': 0,
                'status': 'success',
                'message': 'No valid papers'
            }

        # 2. Create DataFrame and write to temporary table
        df = pd.DataFrame(papers)

        # Data type mapping for JSONB columns
        dtype_mapping = {
            'authors': JSONB,
            'external_ids': JSONB,
            'fields_of_study': JSONB,
            'publication_types': JSONB
        }

        # Write to temp table (fast, no indexes/constraints)
        df.to_sql(
            temp_table,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000,
            dtype=dtype_mapping
        )

        # 3. SQL-based conference filtering + UPSERT (ONE SQL STATEMENT)
        upsert_sql = f"""
        INSERT INTO dataset_papers (
            corpus_id, paper_id, title, abstract, venue, year,
            citation_count, reference_count, influential_citation_count,
            authors, external_ids, fields_of_study, publication_types,
            is_open_access, open_access_pdf, conference_normalized,
            source_file, release_id, created_at, updated_at
        )
        SELECT
            t.corpus_id,
            t.paper_id,
            t.title,
            t.abstract,
            t.venue,
            t.year,
            t.citation_count,
            t.reference_count,
            t.influential_citation_count,
            t.authors,
            t.external_ids,
            t.fields_of_study,
            t.publication_types,
            t.is_open_access,
            t.open_access_pdf,
            cp.conference as conference_normalized,
            t.source_file,
            t.release_id,
            CURRENT_TIMESTAMP as created_at,
            CURRENT_TIMESTAMP as updated_at
        FROM {temp_table} t
        INNER JOIN conference_patterns cp ON (
            LOWER(t.venue) = cp.pattern OR
            LOWER(t.venue) LIKE '%' || cp.pattern || '%'
        )
        ON CONFLICT (corpus_id)
        DO UPDATE SET
            paper_id = EXCLUDED.paper_id,
            title = EXCLUDED.title,
            abstract = EXCLUDED.abstract,
            venue = EXCLUDED.venue,
            year = EXCLUDED.year,
            citation_count = EXCLUDED.citation_count,
            reference_count = EXCLUDED.reference_count,
            influential_citation_count = EXCLUDED.influential_citation_count,
            authors = EXCLUDED.authors,
            external_ids = EXCLUDED.external_ids,
            fields_of_study = EXCLUDED.fields_of_study,
            publication_types = EXCLUDED.publication_types,
            is_open_access = EXCLUDED.is_open_access,
            open_access_pdf = EXCLUDED.open_access_pdf,
            conference_normalized = EXCLUDED.conference_normalized,
            source_file = EXCLUDED.source_file,
            release_id = EXCLUDED.release_id,
            updated_at = CURRENT_TIMESTAMP;
        """

        with engine.connect() as conn:
            result = conn.execute(text(upsert_sql))
            conn.commit()
            matched_count = result.rowcount

        # 4. Clean up temporary table
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {temp_table}'))
            conn.commit()

        engine.dispose()

        return {
            'file': os.path.basename(file_path),
            'total_lines': line_count,
            'total_papers': len(papers),
            'matched_papers': matched_count,
            'status': 'success'
        }

    except Exception as e:
        # Clean up temp table on error
        try:
            engine = create_engine(db_config['connection_string'])
            with engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS {temp_table}'))
                conn.commit()
            engine.dispose()
        except:
            pass

        return {
            'file': os.path.basename(file_path),
            'status': 'failed',
            'error': str(e)
        }


def _extract_open_access_pdf(paper: Dict) -> str:
    """Extract open access PDF URL from paper data"""
    open_access_field = paper.get('openaccesspdf') or paper.get('openAccessPdf')
    if open_access_field:
        if isinstance(open_access_field, dict):
            return open_access_field.get('url') or ''
        elif isinstance(open_access_field, str):
            return open_access_field
    return ''


class S2DatasetProcessorParallel:
    """
    S2 Dataset Processor with parallel processing and SQL-based filtering

    Key features:
    - Parallel file processing with multiprocessing
    - SQL-based conference filtering (no Python matching)
    - PostgreSQL ON CONFLICT for UPSERT (no Bloom Filter needed)
    - Dynamic worker count based on system resources
    """

    def __init__(self, db_manager: DatabaseManager, release_id: str):
        self.db_manager = db_manager
        self.release_id = release_id
        self.release_repo = DatasetReleaseRepository(db_manager)
        self.config = ProcessingConfig()
        self.logger = self._setup_logger()

        # Statistics
        self.total_files = 0
        self.success_count = 0
        self.failed_count = 0
        self.total_matched = 0

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.S2DatasetProcessorParallel')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def process_dataset_files(self, data_dir: str) -> Dict:
        """
        Process all dataset files in parallel

        Args:
            data_dir: Directory containing .gz files

        Returns:
            Processing results and statistics
        """
        start_time = datetime.now()

        # 1. Setup conference patterns table (if not already done)
        if not check_conference_patterns_exist(self.db_manager):
            self.logger.info("Setting up conference patterns in database...")
            setup_conference_patterns(self.db_manager)
        else:
            self.logger.info("✓ Conference patterns already exist in database")

        # 2. Update release status
        self.release_repo.update_release_status(
            self.release_id,
            'processing',
            processing_start_time=start_time
        )

        # 3. Get all files to process
        files = sorted(Path(data_dir).glob('*.gz'))
        self.total_files = len(files)

        if not files:
            self.logger.warning(f"No .gz files found in {data_dir}")
            return {
                'release_id': self.release_id,
                'status': 'failed',
                'error': 'No dataset files found',
                'total_files': 0
            }

        self.logger.info(f"Found {self.total_files} files to process")

        # 4. Calculate optimal workers
        max_workers = self.config.get_max_workers(self.db_manager)

        # 5. Prepare arguments for workers
        db_config = {
            'connection_string': self.db_manager.config.get_connection_string()
        }
        args_list = [(str(f), self.release_id, db_config) for f in files]

        # 6. Process files (parallel or sequential based on config)
        results = []

        if self.config.should_use_parallel() and max_workers > 1:
            # Parallel processing
            self.logger.info(f"Processing {self.total_files} files with {max_workers} workers in parallel")

            with multiprocessing.Pool(processes=max_workers) as pool:
                for result in tqdm(
                    pool.imap_unordered(process_single_file, args_list),
                    total=self.total_files,
                    desc=f"Processing files ({max_workers} workers)"
                ):
                    results.append(result)
                    self._log_result(result)

        else:
            # Sequential processing (for debugging)
            self.logger.info(f"Processing {self.total_files} files sequentially")

            for args in tqdm(args_list, desc="Processing files"):
                result = process_single_file(args)
                results.append(result)
                self._log_result(result)

        # 7. Calculate statistics
        self.success_count = sum(1 for r in results if r['status'] == 'success')
        self.failed_count = sum(1 for r in results if r['status'] == 'failed')
        self.total_matched = sum(r.get('matched_papers', 0) for r in results if r['status'] == 'success')

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        # 8. Update release statistics
        self.release_repo.update_release_status(
            self.release_id,
            'completed' if self.failed_count == 0 else 'completed_with_errors',
            processing_end_time=end_time,
            total_papers_processed=sum(r.get('total_lines', 0) for r in results if r['status'] == 'success'),
            papers_inserted=self.total_matched,  # All matched papers (new + updated)
            papers_updated=0  # Can't distinguish in ON CONFLICT
        )

        # 9. Log summary
        self.logger.info("="*80)
        self.logger.info("Processing completed!")
        self.logger.info(f"Total files: {self.total_files}")
        self.logger.info(f"Success: {self.success_count}")
        self.logger.info(f"Failed: {self.failed_count}")
        self.logger.info(f"Total matched papers: {self.total_matched:,}")
        self.logger.info(f"Processing time: {processing_time:.2f}s ({processing_time/60:.1f} minutes)")
        self.logger.info("="*80)

        return {
            'release_id': self.release_id,
            'status': 'completed' if self.failed_count == 0 else 'completed_with_errors',
            'total_files': self.total_files,
            'success_count': self.success_count,
            'failed_count': self.failed_count,
            'total_matched_papers': self.total_matched,
            'processing_time_seconds': processing_time,
            'results': results
        }

    def _log_result(self, result: Dict):
        """Log processing result for a single file"""
        if result['status'] == 'success':
            self.logger.info(
                f"✓ {result['file']}: "
                f"{result.get('total_papers', 0):,} parsed, "
                f"{result.get('matched_papers', 0):,} matched"
            )
        else:
            self.logger.error(f"✗ {result['file']}: {result.get('error', 'Unknown error')}")
