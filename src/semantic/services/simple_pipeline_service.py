"""
Simple Pipeline Service
Unified service that manages all data processing phases with simplified error handling
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional

from ..utils.config import AppConfig
from ..database.connection import DatabaseManager, get_db_manager
from ..database.repositories.processing_metadata import ProcessingMetadataRepository
from .pipeline_service import DataPipelineService
from .s2_service.s2_enrichment_service import S2EnrichmentService
from .author_service.author_profile_service import AuthorProfileService
from .author_service.final_author_table_service import FinalAuthorTableService
from .s2_service.pdf_download_service import PDFDownloadService


class SimplePipelineService:
    """Simplified pipeline service that manages all processing phases"""

    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.metadata_repo = ProcessingMetadataRepository(self.db_manager)
        self.logger = self._setup_logger()

        # Pipeline phases configuration
        self.pipeline_phases = [
            {
                'name': 'DBLP Data Pipeline',
                'process_type': 'dblp_sync',
                'method': self._run_dblp_phase,
                'critical': True
            },
            {
                'name': 'Semantic Scholar Enrichment',
                'process_type': 's2_enrichment',
                'method': self._run_s2_phase,
                'critical': True
            },
            {
                'name': 'Author Processing',
                'process_type': 'author_processing',
                'method': self._run_author_phase,
                'critical': False
            },
            {
                'name': 'PDF Download',
                'process_type': 'pdf_download',
                'method': self._run_pdf_phase,
                'critical': False
            }
        ]

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.SimplePipelineService')
        logger.setLevel(getattr(logging, self.config.log_level))

        if not logger.handlers:
            # Create logs directory
            os.makedirs('logs', exist_ok=True)

            # File handler
            file_handler = logging.FileHandler(
                f'logs/simple_pipeline_{datetime.now().strftime("%Y%m%d")}.log'
            )
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)

            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger

    def _run_dblp_phase(self, metadata_id: int) -> Tuple[bool, Dict[str, Any]]:
        """Run DBLP data pipeline phase"""
        try:
            self.logger.info("Starting DBLP data pipeline phase...")

            # Create DBLP pipeline service
            pipeline_service = DataPipelineService(self.config, self.db_manager)

            # Run pipeline
            success = pipeline_service.run_pipeline()

            if success:
                # Export to CSV
                output_path = "data/dblp_papers_export.csv"
                csv_success = pipeline_service.export_to_csv(output_path)

                result = {
                    'success': success and csv_success,
                    'csv_exported': csv_success,
                    'csv_path': output_path if csv_success else None
                }

                return True, result
            else:
                return False, {'error': 'DBLP pipeline execution failed'}

        except Exception as e:
            error_msg = f"DBLP phase failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, {'error': error_msg}

    async def _run_s2_phase(self, metadata_id: int) -> Tuple[bool, Dict[str, Any]]:
        """Run Semantic Scholar enrichment phase"""
        try:
            self.logger.info("Starting Semantic Scholar enrichment phase...")

            # Get API key
            api_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY')
            if api_key:
                self.logger.info("✅ Semantic Scholar API key loaded")
            else:
                self.logger.warning("⚠️ No API key found - using public rate limits")

            # Initialize S2 enrichment service
            s2_service = S2EnrichmentService(
                config=self.config,
                db_manager=self.db_manager,
                api_key=api_key
            )

            # Process papers
            success = s2_service.enrich_papers(limit=38000)

            if success:
                # Get statistics
                stats = s2_service.get_enrichment_statistics()

                # Export results to CSV
                export_path = "data/s2_enriched_results.csv"
                os.makedirs("data", exist_ok=True)
                csv_success = s2_service.export_enriched_papers(export_path, include_all_fields=False)

                result = {
                    'success': True,
                    'enrichment_stats': stats,
                    'csv_exported': csv_success,
                    'csv_path': export_path if csv_success else None
                }

                return True, result
            else:
                return False, {'error': 'S2 enrichment failed'}

        except Exception as e:
            error_msg = f"S2 enrichment phase failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, {'error': error_msg}

    def _run_author_phase(self, metadata_id: int) -> Tuple[bool, Dict[str, Any]]:
        """Run author processing phase"""
        try:
            self.logger.info("Starting author processing phase...")

            # Initialize services
            profile_service = AuthorProfileService(self.db_manager)
            final_table_service = FinalAuthorTableService(self.db_manager)

            # Create and populate authorships table
            self.logger.info("Creating authorships table...")
            if not profile_service.create_authorships_table():
                raise Exception("Failed to create authorships table")

            authorship_stats = profile_service.populate_authorships_table()
            if 'error' in authorship_stats:
                raise Exception(f"Failed to populate authorships table: {authorship_stats['error']}")

            # Create and populate author profiles table
            self.logger.info("Creating author profiles table...")
            if not profile_service.create_author_profiles_table():
                raise Exception("Failed to create author profiles table")

            profile_stats = profile_service.populate_author_profiles_table()
            if 'error' in profile_stats:
                raise Exception(f"Failed to populate author profiles: {profile_stats['error']}")

            # Create final target table
            self.logger.info("Creating final author table...")
            if not final_table_service.create_final_author_table():
                raise Exception("Failed to create final author table")

            final_stats = final_table_service.populate_final_author_table()
            if 'error' in final_stats:
                raise Exception(f"Failed to populate final table: {final_stats['error']}")

            # Generate reports
            self.logger.info("Generating author processing reports...")
            reports_dir = "data/reports"
            os.makedirs(reports_dir, exist_ok=True)

            final_report_path = f"{reports_dir}/final_author_table_report.json"
            final_table_service.generate_final_table_report(final_report_path)

            result = {
                'success': True,
                'authorship_stats': authorship_stats,
                'profile_stats': profile_stats,
                'final_stats': final_stats,
                'final_report_path': final_report_path
            }

            return True, result

        except Exception as e:
            error_msg = f"Author processing phase failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, {'error': error_msg}

    async def _run_pdf_phase(self, metadata_id: int) -> Tuple[bool, Dict[str, Any]]:
        """Run PDF download phase"""
        try:
            self.logger.info("Starting PDF download phase...")

            # Initialize PDF download service
            pdf_service = PDFDownloadService(self.db_manager)

            # Check papers that need PDF download
            pending_papers = self.db_manager.fetch_all("""
                SELECT COUNT(*) as count
                FROM enriched_papers
                WHERE semantic_paper_id IS NOT NULL
                AND dblp_url IS NOT NULL
                AND dblp_url != ''
                AND (pdf_filename IS NULL OR pdf_file_path IS NULL)
            """)

            if pending_papers and pending_papers[0]['count'] > 0:
                pending_count = pending_papers[0]['count']
                self.logger.info(f"Found {pending_count} papers ready for PDF download")

                # Download PDFs in batch
                download_stats = await pdf_service.download_papers_batch(
                    limit=None,  # Download all available
                    concurrent_downloads=3  # Conservative concurrency
                )

                result = {
                    'success': True,
                    'download_stats': download_stats,
                    'message': f'Downloaded PDFs for {pending_count} papers'
                }

                return True, result
            else:
                result = {
                    'success': True,
                    'message': 'No papers need PDF download',
                    'download_stats': {'total_processed': 0, 'successful_downloads': 0, 'failed_downloads': 0}
                }

                return True, result

        except Exception as e:
            error_msg = f"PDF download phase failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, {'error': error_msg}

    async def run_full_pipeline(self) -> bool:
        """
        Run the complete data processing pipeline

        Returns:
            True if all critical phases succeed, False otherwise
        """
        self.logger.info("🚀 Starting Simple Data Processing Pipeline")
        self.logger.info("=" * 80)

        # Record pipeline start
        pipeline_metadata_id = self.metadata_repo.record_processing_start(
            entity_type='pipeline',
            process_type='full_pipeline',
            metadata={'phases': len(self.pipeline_phases)}
        )

        overall_success = True
        phase_results = []

        try:
            # Execute each phase in sequence
            for i, phase_info in enumerate(self.pipeline_phases, 1):
                phase_name = phase_info['name']
                process_type = phase_info['process_type']
                phase_method = phase_info['method']
                is_critical = phase_info['critical']

                self.logger.info(f"\n📋 Phase {i}/{len(self.pipeline_phases)}: {phase_name}")
                self.logger.info("-" * 60)

                # Record phase start
                phase_metadata_id = self.metadata_repo.record_processing_start(
                    entity_type='pipeline',
                    process_type=process_type,
                    metadata={'phase_number': i, 'phase_name': phase_name}
                )

                try:
                    # Execute the phase
                    if asyncio.iscoroutinefunction(phase_method):
                        success, result = await phase_method(phase_metadata_id)
                    else:
                        success, result = phase_method(phase_metadata_id)

                    if success:
                        self.logger.info(f"✅ Phase {i} completed successfully")
                        self.metadata_repo.record_processing_success(
                            phase_metadata_id, result
                        )
                    else:
                        self.logger.error(f"❌ Phase {i} failed: {result.get('error', 'Unknown error')}")
                        self.metadata_repo.record_processing_failure(
                            phase_metadata_id, result.get('error', 'Unknown error'), result
                        )

                        if is_critical:
                            overall_success = False
                            self.logger.error(f"🛑 Critical phase failed, stopping pipeline")
                            break
                        else:
                            self.logger.warning(f"⚠️ Non-critical phase failed, continuing")

                    phase_results.append({
                        'phase_name': phase_name,
                        'success': success,
                        'result': result
                    })

                except Exception as e:
                    error_msg = f"Phase {i} execution error: {str(e)}"
                    self.logger.error(error_msg, exc_info=True)
                    self.metadata_repo.record_processing_failure(
                        phase_metadata_id, error_msg
                    )

                    if is_critical:
                        overall_success = False
                        self.logger.error(f"🛑 Critical phase failed with exception, stopping pipeline")
                        break
                    else:
                        self.logger.warning(f"⚠️ Non-critical phase failed with exception, continuing")

            # Record pipeline completion
            if overall_success:
                self.logger.info(f"\n🎉 Simple pipeline completed successfully!")
                self.metadata_repo.record_processing_success(
                    pipeline_metadata_id,
                    {
                        'total_phases': len(self.pipeline_phases),
                        'successful_phases': sum(1 for r in phase_results if r['success']),
                        'phase_results': phase_results
                    }
                )
            else:
                self.logger.error(f"\n⚠️ Simple pipeline completed with failures")
                self.metadata_repo.record_processing_failure(
                    pipeline_metadata_id,
                    "One or more critical phases failed",
                    {
                        'total_phases': len(self.pipeline_phases),
                        'successful_phases': sum(1 for r in phase_results if r['success']),
                        'phase_results': phase_results
                    }
                )

            return overall_success

        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.metadata_repo.record_processing_failure(
                pipeline_metadata_id, error_msg
            )
            return False

    def should_run_pipeline(self, force: bool = False) -> bool:
        """
        Check if pipeline should run based on last successful execution

        Args:
            force: If True, always return True

        Returns:
            True if pipeline should run, False otherwise
        """
        if force:
            self.logger.info("Force run requested, pipeline will execute")
            return True

        # Check last successful run
        last_run = self.metadata_repo.get_last_successful_run('full_pipeline')

        if not last_run:
            self.logger.info("No previous successful run found, pipeline will execute")
            return True

        # Check if enough time has passed (configurable interval)
        time_since_last_run = datetime.now() - last_run
        interval_days = 7  # Default 7 days, could be made configurable

        if time_since_last_run.days >= interval_days:
            self.logger.info(f"More than {interval_days} days since last run ({time_since_last_run.days} days), pipeline will execute")
            return True
        else:
            self.logger.info(f"Only {time_since_last_run.days} days since last run, skipping execution")
            return False

    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status"""
        try:
            # Get latest pipeline run status
            latest_run = self.metadata_repo.get_latest_processing_status('pipeline', 'full_pipeline')

            # Get statistics for last 24 hours
            stats = self.metadata_repo.get_processing_statistics(hours=24)

            # Get last successful run
            last_successful = self.metadata_repo.get_last_successful_run('full_pipeline')

            return {
                'latest_run': latest_run,
                'statistics': stats,
                'last_successful_run': last_successful.isoformat() if last_successful else None,
                'next_scheduled_run': self._calculate_next_run(last_successful) if last_successful else None
            }

        except Exception as e:
            self.logger.error(f"Failed to get pipeline status: {e}")
            return {'error': str(e)}

    def _calculate_next_run(self, last_run: datetime) -> str:
        """Calculate next scheduled run time"""
        next_run = last_run + timedelta(days=7)  # 7 days interval
        return next_run.isoformat()