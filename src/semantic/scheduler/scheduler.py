"""
Task Scheduler
Implements scheduled task scheduling based on APScheduler
"""

import time
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

from ..services.pipeline_service import DataPipelineService
from ..database.connection import DatabaseManager, get_db_manager
from ..utils.config import AppConfig


class DataPipelineScheduler:
    """Data Pipeline Scheduler"""
    
    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.pipeline_service = DataPipelineService(config, self.db_manager)
        self.logger = self._setup_logger()
        
        # Configure scheduler
        self.scheduler = self._setup_scheduler()
        
        # Retry counter
        self.retry_count = 0
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.DataPipelineScheduler')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            import os
            
            # Create log directory
            os.makedirs('logs', exist_ok=True)
            
            # File handler
            file_handler = logging.FileHandler(
                f'logs/scheduler_{datetime.now().strftime("%Y%m%d")}.log'
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
            jobstore = SQLAlchemyJobStore(url=connection_string, tablename='scheduler_jobs')
            
            # Executor configuration
            executors = {
                'default': ThreadPoolExecutor(max_workers=2),
            }
            
            # Job default configuration
            job_defaults = {
                'coalesce': True,  # Merge accumulated jobs
                'max_instances': 1,  # Only run one instance at a time
                'misfire_grace_time': 3600,  # Grace period after missing execution time (seconds)
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
        self.logger.info(f"Job executed successfully: {event.job_id}")
        self.retry_count = 0  # Reset retry count
    
    def _job_error_listener(self, event):
        """Job execution failed listener"""
        self.logger.error(f"Job execution failed: {event.job_id}, exception: {event.exception}")
        self._handle_job_retry(event.job_id)
    
    def _job_missed_listener(self, event):
        """Job execution missed listener"""
        self.logger.warning(f"Job execution missed: {event.job_id}")
    
    def _handle_job_retry(self, job_id: str):
        """Handle job retry"""
        if self.retry_count < self.config.max_retries:
            self.retry_count += 1
            self.logger.info(f"Preparing to retry job {job_id}, attempt {self.retry_count}/{self.config.max_retries}")
            
            # Delayed retry
            self.scheduler.add_job(
                func=self.run_data_pipeline,
                trigger='date',
                run_date=datetime.now().timestamp() + self.config.retry_delay,
                id=f"{job_id}_retry_{self.retry_count}",
                replace_existing=True
            )
        else:
            self.logger.error(f"Job {job_id} has reached maximum retry limit, stopping retries")
            self.retry_count = 0
    
    def run_data_pipeline(self):
        """Wrapper method for running data pipeline"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("Scheduled task triggered: Starting data pipeline execution")
            self.logger.info("=" * 80)
            
            # Check if execution should proceed (additional check in incremental mode)
            if self.config.enable_incremental and not self.pipeline_service.should_run_incremental():
                self.logger.info("Skipping this execution based on incremental strategy")
                return
            
            # Execute data pipeline
            success = self.pipeline_service.run_pipeline()
            
            if success:
                self.logger.info("Scheduled task completed: Data pipeline execution completed successfully")
            else:
                raise Exception("Data pipeline execution failed")
                
        except Exception as e:
            self.logger.error(f"Scheduled task failed: {str(e)}")
            raise  # Re-raise exception to trigger retry mechanism
    
    def add_pipeline_job(self):
        """Add data pipeline scheduled task"""
        try:
            # Parse cron expression
            cron_parts = self.config.schedule_cron.split()
            if len(cron_parts) != 5:
                raise ValueError(f"Invalid cron expression: {self.config.schedule_cron}")
            
            minute, hour, day, month, day_of_week = cron_parts
            
            # Add scheduled task
            self.scheduler.add_job(
                func=self.run_data_pipeline,
                trigger='cron',
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                id='dblp_pipeline_job',
                name='DBLP Data Pipeline',
                replace_existing=True
            )
            
            self.logger.info(f"Added scheduled task: {self.config.schedule_cron}")
            
        except Exception as e:
            self.logger.error(f"Failed to add scheduled task: {e}")
            raise
    
    def add_manual_job(self, delay_seconds: int = 10):
        """Add manual execution task (delayed execution)"""
        try:
            run_time = datetime.now().timestamp() + delay_seconds
            
            self.scheduler.add_job(
                func=self.run_data_pipeline,
                trigger='date',
                run_date=run_time,
                id='dblp_pipeline_manual',
                name='DBLP Data Pipeline (Manual Trigger)',
                replace_existing=True
            )
            
            self.logger.info(f"Added manual task, will execute in {delay_seconds} seconds")
            
        except Exception as e:
            self.logger.error(f"Failed to add manual task: {e}")
            raise
    
    def start(self, manual_run: bool = False):
        """Start scheduler"""
        try:
            self.logger.info("Starting data pipeline scheduler...")
            
            # Test database connection
            if not self.db_manager.test_connection():
                raise Exception("Database connection test failed")
            
            # Add tasks
            if manual_run:
                self.add_manual_job()
                self.logger.info("Scheduler started (manual mode), data pipeline will execute immediately...")
            else:
                self.add_pipeline_job()
                next_run = self.scheduler.get_job('dblp_pipeline_job').next_run_time
                self.logger.info(f"Scheduler started, next run time: {next_run}")
            
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
            
            self.logger.info("Scheduler stopped")
            
        except Exception as e:
            self.logger.error(f"Scheduler shutdown failed: {e}")
    
    def list_jobs(self):
        """List all tasks"""
        jobs = self.scheduler.get_jobs()
        if not jobs:
            self.logger.info("No registered tasks")
            return
        
        self.logger.info("Registered tasks:")
        for job in jobs:
            self.logger.info(f"- {job.id}: {job.name}, next run: {job.next_run_time}")
    
    def remove_job(self, job_id: str):
        """Remove specified task"""
        try:
            self.scheduler.remove_job(job_id)
            self.logger.info(f"Removed task: {job_id}")
        except Exception as e:
            self.logger.error(f"Failed to remove task: {e}")


def main():
    """Main function"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='DBLP Data Pipeline Scheduler')
    parser.add_argument('--manual', action='store_true', help='Execute once manually')
    parser.add_argument('--list-jobs', action='store_true', help='List all tasks')
    parser.add_argument('--config', type=str, help='Configuration file path')
    
    args = parser.parse_args()
    
    # Load configuration
    config = AppConfig.from_env()
    
    # Validate configuration
    if not config.validate():
        print("Configuration validation failed, please check environment variables")
        return
    
    # Create scheduler
    scheduler = DataPipelineScheduler(config)
    
    try:
        if args.list_jobs:
            scheduler.list_jobs()
        elif args.manual:
            scheduler.start(manual_run=True)
        else:
            scheduler.start(manual_run=False)
            
    except KeyboardInterrupt:
        print("\nReceived interrupt signal, stopping...")
    except Exception as e:
        print(f"Scheduler run failed: {e}")
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()