"""
Data Pipeline Service
Implements incremental processing logic and complete data processing workflow
"""

import logging
import time
from datetime import datetime, timedelta
from typing import List, Set, Tuple, Dict, Any
from ..database.connection import DatabaseManager, get_db_manager
from ..database.models import PaperRepository, Paper
from ..services.dblp_service import DBLPService
from ..utils.config import AppConfig


class DataPipelineService:
    """Data Pipeline Service"""
    
    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.paper_repo = PaperRepository(self.db_manager)
        self.dblp_service = DBLPService(config)
        self.logger = self._setup_logger()
        
        # Pipeline status
        self.current_process_type = "dblp_full_sync"
        self.start_time = None
        self.stats = {
            'papers_processed': 0,
            'papers_inserted': 0,
            'papers_updated': 0,
            'errors': 0
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.DataPipelineService')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def step1_prepare_data(self) -> bool:
        """Step 1: Prepare data (download and extract)"""
        try:
            self.logger.info(f"[{datetime.now()}] Executing Step 1: Prepare DBLP data")
            
            # Decide whether to force download based on incremental mode
            force_download = not self.config.enable_incremental
            
            if not self.dblp_service.prepare_data(force_download=force_download):
                raise Exception("DBLP data preparation failed")
            
            self.logger.info("Step 1 completed: DBLP data preparation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Step 1 failed: {e}")
            return False
    
    def step2_extract_papers(self) -> List[Paper]:
        """Step 2: Extract paper data"""
        try:
            self.logger.info(f"[{datetime.now()}] Executing Step 2: Extract paper data")
            
            papers = []
            existing_keys = set()
            
            # If incremental processing is enabled, get existing paper keys
            if self.config.enable_incremental:
                self.logger.info("Incremental mode: Getting existing paper list...")
                existing_keys = self._get_existing_paper_keys()
                self.logger.info(f"Number of existing papers: {len(existing_keys)}")
                self.current_process_type = "dblp_incremental_sync"
            
            # Parse papers
            papers = self.dblp_service.parse_papers(
                incremental=self.config.enable_incremental,
                existing_keys=existing_keys
            )
            
            if not papers:
                self.logger.warning("No new paper data extracted")
                return []
            
            self.stats['papers_processed'] = len(papers)
            self.logger.info(f"Step 2 completed: Extracted {len(papers)} papers")
            return papers
            
        except Exception as e:
            self.logger.error(f"Step 2 failed: {e}")
            return []
    
    def step3_load_papers(self, papers: List[Paper]) -> bool:
        """Step 3: Load papers to database"""
        try:
            if not papers:
                self.logger.info("No papers to load")
                return True
            
            self.logger.info(f"[{datetime.now()}] Executing Step 3: Loading {len(papers)} papers to database")
            
            # Ensure database tables exist
            if not self.paper_repo.create_tables():
                raise Exception("Database table creation failed")
            
            # Batch insert papers
            inserted, updated, errors = self.paper_repo.batch_insert_papers(papers)
            
            # Update statistics
            self.stats['papers_inserted'] = inserted
            self.stats['papers_updated'] = updated
            self.stats['errors'] = errors
            
            self.logger.info(
                f"Step 3 completed: Inserted {inserted}, updated {updated}, errors {errors}"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Step 3 failed: {e}")
            return False
    
    def step4_post_process(self) -> bool:
        """Step 4: Post processing (cleanup files, record metadata, etc.)"""
        try:
            self.logger.info(f"[{datetime.now()}] Executing Step 4: Post processing")
            
            # Record processing metadata
            success = self.stats['errors'] == 0
            status = 'success' if success else 'partial_success'
            
            self.paper_repo.record_processing_meta(
                process_type=self.current_process_type,
                status=status,
                records_processed=self.stats['papers_processed'],
                records_inserted=self.stats['papers_inserted'],
                records_updated=self.stats['papers_updated'],
                error_message=None if success else f"Processing encountered {self.stats['errors']} errors"
            )
            
            # Clean up temporary files (keep XML files for next incremental use)
            if not self.config.enable_incremental:
                self.dblp_service.cleanup(keep_xml=False)
            else:
                self.dblp_service.cleanup(keep_xml=True)
            
            self.logger.info("Step 4 completed: Post processing completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Step 4 failed: {e}")
            return False
    
    def run_pipeline(self) -> bool:
        """Run the entire data pipeline"""
        self.start_time = datetime.now()
        self.logger.info(f"\n[{self.start_time}] Starting data pipeline execution")
        self.logger.info(f"Processing mode: {'Incremental' if self.config.enable_incremental else 'Full'}")
        
        try:
            # Reset statistics
            self._reset_stats()
            
            # Step 1: Prepare data
            if not self.step1_prepare_data():
                raise Exception("Data preparation failed")
            
            # Step 2: Extract papers
            papers = self.step2_extract_papers()
            if papers is None:  # Distinguish between empty list and failure
                raise Exception("Paper extraction failed")
            
            # Step 3: Load papers
            if not self.step3_load_papers(papers):
                raise Exception("Paper loading failed")
            
            # Step 4: Post processing
            if not self.step4_post_process():
                raise Exception("Post processing failed")
            
            # Generate final report
            self._generate_final_report()
            
            self.logger.info(f"[{datetime.now()}] Data pipeline execution completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"[{datetime.now()}] Data pipeline execution failed: {str(e)}")
            
            # Record failure metadata
            self.paper_repo.record_processing_meta(
                process_type=self.current_process_type,
                status='failed',
                records_processed=self.stats['papers_processed'],
                records_inserted=self.stats['papers_inserted'],
                records_updated=self.stats['papers_updated'],
                error_message=str(e)
            )
            
            return False
    
    def _get_existing_paper_keys(self) -> Set[str]:
        """Get existing paper keys from database"""
        try:
            # If check days are configured, only check recent papers
            if self.config.incremental_check_days > 0:
                cutoff_date = datetime.now() - timedelta(days=self.config.incremental_check_days)
                query = """
                SELECT key FROM dblp_papers 
                WHERE update_time >= %s OR created_at >= %s
                """
                results = self.db_manager.fetch_all(query, (cutoff_date, cutoff_date))
            else:
                # Get all paper keys
                query = "SELECT key FROM dblp_papers"
                results = self.db_manager.fetch_all(query)
            
            return {row['key'] for row in results}
            
        except Exception as e:
            self.logger.error(f"Failed to get existing paper keys: {e}")
            return set()
    
    def _reset_stats(self):
        """Reset statistics"""
        self.stats = {
            'papers_processed': 0,
            'papers_inserted': 0,
            'papers_updated': 0,
            'errors': 0
        }
        self.dblp_service.reset_stats()
    
    def _generate_final_report(self):
        """Generate final report"""
        end_time = datetime.now()
        duration = end_time - self.start_time if self.start_time else timedelta(0)
        
        # Get database statistics
        db_stats = self.paper_repo.get_statistics()
        
        self.logger.info("=" * 80)
        self.logger.info("DBLP Data Processing Pipeline Completed")
        self.logger.info("=" * 80)
        self.logger.info(f"Processing time: {duration}")
        self.logger.info(f"Papers processed: {self.stats['papers_processed']}")
        self.logger.info(f"New papers: {self.stats['papers_inserted']}")
        self.logger.info(f"Updated papers: {self.stats['papers_updated']}")
        self.logger.info(f"Error count: {self.stats['errors']}")
        self.logger.info(f"Total papers in database: {db_stats.get('total_papers', 0)}")
        self.logger.info(f"Last update time: {db_stats.get('last_update', 'N/A')}")
        
        # Statistics by venue
        venue_stats = db_stats.get('by_venue', {})
        if venue_stats:
            self.logger.info("Statistics by venue:")
            for venue, count in list(venue_stats.items())[:5]:  # Show top 5
                self.logger.info(f"  {venue}: {count}")
    
    def get_last_successful_run(self) -> datetime:
        """Get last successful run time"""
        return self.paper_repo.get_last_successful_run(self.current_process_type)
    
    def should_run_incremental(self) -> bool:
        """Determine whether incremental update should be run"""
        if not self.config.enable_incremental:
            return False
        
        last_run = self.get_last_successful_run()
        if not last_run:
            self.logger.info("No previous successful run record found, will execute full sync")
            return False
        
        # Check if incremental check interval has been exceeded
        time_since_last_run = datetime.now() - last_run
        if time_since_last_run.days >= self.config.incremental_check_days:
            self.logger.info(f"More than {self.config.incremental_check_days} days since last run, executing incremental update")
            return True
        
        self.logger.info(f"Only {time_since_last_run.days} days since last run, skipping this run")
        return False
    
    def export_to_csv(self, output_path: str = "data/dblp_papers_export.csv") -> bool:
        """Export data to CSV file"""
        try:
            import pandas as pd
            import os
            
            self.logger.info(f"Exporting data to CSV: {output_path}")
            
            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Query data
            query = """
            SELECT key, title, 
                   array_to_string(ARRAY(SELECT jsonb_array_elements_text(authors)), '|') as authors,
                   year, pages, ee, venue, booktitle, doi
            FROM dblp_papers 
            ORDER BY venue, year DESC, key
            """
            
            df = pd.read_sql_query(query, self.db_manager.get_connection())
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            
            self.logger.info(f"CSV export completed: {len(df)} rows of data")
            return True
            
        except Exception as e:
            self.logger.error(f"CSV export failed: {e}")
            return False