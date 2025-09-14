"""
Comprehensive Pipeline Scheduler
Extends the existing scheduler to run the complete 4-phase data processing pipeline every 7 days
"""

import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

from ..services.comprehensive_pipeline_service import ComprehensivePipelineService
from ..database.connection import DatabaseManager, get_db_manager
from ..utils.config import AppConfig


class ComprehensivePipelineScheduler:
    """Comprehensive Pipeline Scheduler - runs all 4 phases every 7 days"""
    
    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.pipeline_service = ComprehensivePipelineService(config, self.db_manager)
        self.logger = self._setup_logger()
        
        # Configure scheduler
        self.scheduler = self._setup_scheduler()
        
        # Retry counter (use config values)
        self.retry_count = 0
        self.max_retries = self.config.max_retries
        self.retry_delay = self.config.retry_delay
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.ComprehensivePipelineScheduler')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            import os
            
            # Create log directory
            os.makedirs('logs', exist_ok=True)
            
            # File handler
            file_handler = logging.FileHandler(
                f'logs/comprehensive_scheduler_{datetime.now().strftime("%Y%m%d")}.log'
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
    
    def _setup_scheduler(self) -> BlockingScheduler:
        """Setup scheduler"""
        try:
            # Job store (using database)
            connection_string = self.db_manager.config.get_connection_string()
            jobstore = SQLAlchemyJobStore(url=connection_string, tablename='comprehensive_scheduler_jobs')
            
            # Executor configuration
            executors = {
                'default': ThreadPoolExecutor(max_workers=1),  # Only one pipeline at a time
            }
            
            # Job default configuration
            job_defaults = {
                'coalesce': True,  # Merge accumulated jobs
                'max_instances': 1,  # Only run one instance at a time
                'misfire_grace_time': 7200,  # 2 hours grace period
            }
            
            # Create scheduler
            scheduler = BlockingScheduler(
                jobstores={'default': jobstore},
                executors=executors,
                job_defaults=job_defaults,
                timezone='Asia/Shanghai'  # Set timezone
            )
            
            # Add event listeners
            scheduler.add_listener(self._job_executed_listener, EVENT_JOB_EXECUTED)
            scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
            scheduler.add_listener(self._job_missed_listener, EVENT_JOB_MISSED)
            
            return scheduler
            
        except Exception as e:
            self.logger.error(f"Scheduler setup failed: {e}")
            # If failed to connect to database, use memory store as fallback
            scheduler = BlockingScheduler(timezone='Asia/Shanghai')
            scheduler.add_listener(self._job_executed_listener, EVENT_JOB_EXECUTED)
            scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
            scheduler.add_listener(self._job_missed_listener, EVENT_JOB_MISSED)
            return scheduler
    
    def _job_executed_listener(self, event):
        """Job executed successfully listener"""
        self.logger.info(f"Comprehensive pipeline job executed successfully: {event.job_id}")
        self.retry_count = 0  # Reset retry count
    
    def _job_error_listener(self, event):
        """Job execution failed listener"""
        self.logger.error(f"Comprehensive pipeline job execution failed: {event.job_id}, exception: {event.exception}")
        self._handle_job_retry(event.job_id)
    
    def _job_missed_listener(self, event):
        """Job execution missed listener"""
        self.logger.warning(f"Comprehensive pipeline job execution missed: {event.job_id}")
    
    def _handle_job_retry(self, job_id: str):
        """Handle job retry"""
        if self.retry_count < self.max_retries:
            self.retry_count += 1
            self.logger.info(f"Preparing to retry comprehensive pipeline job {job_id}, attempt {self.retry_count}/{self.max_retries}")
            
            # Delayed retry
            self.scheduler.add_job(
                func=self.run_comprehensive_pipeline,
                trigger='date',
                run_date=datetime.now().timestamp() + self.retry_delay,
                id=f"{job_id}_retry_{self.retry_count}",
                replace_existing=True
            )
        else:
            self.logger.error(f"Comprehensive pipeline job {job_id} has reached maximum retry limit, stopping retries")
            self.retry_count = 0
    
    def run_comprehensive_pipeline(self):
        """Wrapper method for running comprehensive pipeline"""
        try:
            self.logger.info("=" * 100)
            self.logger.info("🚀 SCHEDULED COMPREHENSIVE PIPELINE EXECUTION STARTED")
            self.logger.info("=" * 100)
            self.logger.info(f"⏰ Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Execute comprehensive pipeline
            success = self.pipeline_service.run_comprehensive_pipeline()
            
            # Export execution report
            report_path = f"data/reports/comprehensive_pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            self.pipeline_service.export_execution_report(report_path)
            
            if success:
                self.logger.info("✅ SCHEDULED COMPREHENSIVE PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            else:
                raise Exception("Comprehensive pipeline execution failed")
                
        except Exception as e:
            self.logger.error(f"❌ SCHEDULED COMPREHENSIVE PIPELINE EXECUTION FAILED: {str(e)}")
            raise  # Re-raise exception to trigger retry mechanism
    
    def add_weekly_pipeline_job(self):
        """Add comprehensive pipeline scheduled task using config"""
        try:
            # Use comprehensive_schedule_cron from config
            cron_expr = self.config.comprehensive_schedule_cron
            
            # Parse cron expression
            cron_parts = cron_expr.split()
            if len(cron_parts) != 5:
                raise ValueError(f"Invalid comprehensive cron expression in config: {cron_expr}")
            
            minute, hour, day, month, day_of_week = cron_parts
            
            self.scheduler.add_job(
                func=self.run_comprehensive_pipeline,
                trigger='cron',
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                id='comprehensive_pipeline_scheduled',
                name='Comprehensive Data Pipeline (Scheduled)',
                replace_existing=True
            )
            
            self.logger.info(f"Added comprehensive pipeline scheduled task: {cron_expr}")
            
        except Exception as e:
            self.logger.error(f"Failed to add comprehensive pipeline scheduled task: {e}")
            raise
    
    def add_manual_job(self, delay_seconds: int = 30):
        """Add manual execution task (delayed execution)"""
        try:
            run_time = datetime.now().timestamp() + delay_seconds
            
            self.scheduler.add_job(
                func=self.run_comprehensive_pipeline,
                trigger='date',
                run_date=run_time,
                id='comprehensive_pipeline_manual',
                name='Comprehensive Data Pipeline (Manual Trigger)',
                replace_existing=True
            )
            
            self.logger.info(f"Added manual comprehensive pipeline task, will execute in {delay_seconds} seconds")
            
        except Exception as e:
            self.logger.error(f"Failed to add manual comprehensive pipeline task: {e}")
            raise
    
    def add_custom_schedule_job(self, cron_expression: str):
        """
        Add custom scheduled task using cron expression
        
        Args:
            cron_expression: Cron expression in format "minute hour day month day_of_week"
        """
        try:
            # Parse cron expression
            cron_parts = cron_expression.split()
            if len(cron_parts) != 5:
                raise ValueError(f"Invalid cron expression: {cron_expression}")
            
            minute, hour, day, month, day_of_week = cron_parts
            
            # Add scheduled task
            self.scheduler.add_job(
                func=self.run_comprehensive_pipeline,
                trigger='cron',
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                id='comprehensive_pipeline_custom',
                name='Comprehensive Data Pipeline (Custom Schedule)',
                replace_existing=True
            )
            
            self.logger.info(f"Added custom comprehensive pipeline task: {cron_expression}")
            
        except Exception as e:
            self.logger.error(f"Failed to add custom comprehensive pipeline task: {e}")
            raise
    
    def start(self, mode: str = "weekly"):
        """
        Start scheduler
        
        Args:
            mode: Scheduling mode - "weekly", "manual", or "custom:cron_expression"
        """
        try:
            self.logger.info("Starting comprehensive pipeline scheduler...")
            
            # Test database connection
            if not self.db_manager.test_connection():
                raise Exception("Database connection test failed")
            
            # Add tasks based on mode
            if mode == "manual":
                self.add_manual_job()
                self.logger.info("Scheduler started (manual mode), comprehensive pipeline will execute in 30 seconds...")
            elif mode.startswith("custom:"):
                cron_expr = mode.split(":", 1)[1]
                self.add_custom_schedule_job(cron_expr)
                next_run = self.scheduler.get_job('comprehensive_pipeline_custom').next_run_time
                self.logger.info(f"Scheduler started with custom schedule: {cron_expr}, next run time: {next_run}")
            else:  # weekly mode (default)
                self.add_weekly_pipeline_job()
                next_run = self.scheduler.get_job('comprehensive_pipeline_scheduled').next_run_time
                self.logger.info(f"Scheduler started (scheduled mode), next run time: {next_run}")
            
            self.logger.info("Press Ctrl+C to stop scheduler")
            
            # Start scheduler
            self.scheduler.start()
            
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Received stop signal, shutting down scheduler...")
            self.shutdown()
        except Exception as e:
            self.logger.error(f"Scheduler startup failed: {e}")
            raise
    
    def shutdown(self):
        """Shutdown scheduler"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            
            if self.db_manager:
                self.db_manager.disconnect()
            
            self.logger.info("Comprehensive pipeline scheduler stopped")
            
        except Exception as e:
            self.logger.error(f"Scheduler shutdown failed: {e}")
    
    def list_jobs(self):
        """List all tasks"""
        jobs = self.scheduler.get_jobs()
        if not jobs:
            self.logger.info("No registered tasks")
            return
        
        self.logger.info("Registered comprehensive pipeline tasks:")
        for job in jobs:
            self.logger.info(f"- {job.id}: {job.name}, next run: {job.next_run_time}")
    
    def remove_job(self, job_id: str):
        """Remove specified task"""
        try:
            self.scheduler.remove_job(job_id)
            self.logger.info(f"Removed comprehensive pipeline task: {job_id}")
        except Exception as e:
            self.logger.error(f"Failed to remove comprehensive pipeline task: {e}")


def main():
    """Main function"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Comprehensive Data Pipeline Scheduler')
    parser.add_argument('--mode', type=str, default='weekly', 
                       help='Scheduling mode: weekly, manual, or custom:cron_expression (e.g., custom:"0 2 */3 * *")')
    parser.add_argument('--list-jobs', action='store_true', help='List all tasks')
    
    args = parser.parse_args()
    
    # Load configuration
    config = AppConfig.from_env()
    
    # Validate configuration
    if not config.validate():
        print("Configuration validation failed, please check environment variables")
        return
    
    # Create scheduler
    scheduler = ComprehensivePipelineScheduler(config)
    
    try:
        if args.list_jobs:
            scheduler.list_jobs()
        else:
            scheduler.start(mode=args.mode)
            
    except KeyboardInterrupt:
        print("\nReceived interrupt signal, stopping...")
    except Exception as e:
        print(f"Comprehensive scheduler run failed: {e}")
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()