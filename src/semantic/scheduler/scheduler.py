"""
任务调度器
基于APScheduler实现定时任务调度
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
    """数据管道调度器"""
    
    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.pipeline_service = DataPipelineService(config, self.db_manager)
        self.logger = self._setup_logger()
        
        # 配置调度器
        self.scheduler = self._setup_scheduler()
        
        # 重试计数器
        self.retry_count = 0
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger(f'{__name__}.DataPipelineScheduler')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            import os
            
            # 创建日志目录
            os.makedirs('logs', exist_ok=True)
            
            # 文件处理器
            file_handler = logging.FileHandler(
                f'logs/scheduler_{datetime.now().strftime("%Y%m%d")}.log'
            )
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def _setup_scheduler(self) -> BlockingScheduler:
        """设置调度器"""
        try:
            # 作业存储（使用数据库）
            connection_string = self.db_manager.config.get_connection_string()
            jobstore = SQLAlchemyJobStore(url=connection_string, tablename='scheduler_jobs')
            
            # 执行器配置
            executors = {
                'default': ThreadPoolExecutor(max_workers=2),
            }
            
            # 作业默认配置
            job_defaults = {
                'coalesce': True,  # 合并堆积的作业
                'max_instances': 1,  # 同时只运行一个实例
                'misfire_grace_time': 3600,  # 错过执行时间后的宽限期（秒）
            }
            
            # 创建调度器
            scheduler = BlockingScheduler(
                jobstores={'default': jobstore},
                executors=executors,
                job_defaults=job_defaults,
                timezone='Asia/Shanghai'  # 设置时区
            )
            
            # 添加事件监听器
            scheduler.add_listener(self._job_executed_listener, EVENT_JOB_EXECUTED)
            scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
            scheduler.add_listener(self._job_missed_listener, EVENT_JOB_MISSED)
            
            return scheduler
            
        except Exception as e:
            self.logger.error(f"调度器设置失败: {e}")
            # 如果数据库连接失败，使用内存存储作为后备
            scheduler = BlockingScheduler(timezone='Asia/Shanghai')
            scheduler.add_listener(self._job_executed_listener, EVENT_JOB_EXECUTED)
            scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
            scheduler.add_listener(self._job_missed_listener, EVENT_JOB_MISSED)
            return scheduler
    
    def _job_executed_listener(self, event):
        """作业执行成功监听器"""
        self.logger.info(f"作业执行成功: {event.job_id}")
        self.retry_count = 0  # 重置重试计数
    
    def _job_error_listener(self, event):
        """作业执行失败监听器"""
        self.logger.error(f"作业执行失败: {event.job_id}, 异常: {event.exception}")
        self._handle_job_retry(event.job_id)
    
    def _job_missed_listener(self, event):
        """作业错过执行监听器"""
        self.logger.warning(f"作业错过执行: {event.job_id}")
    
    def _handle_job_retry(self, job_id: str):
        """处理作业重试"""
        if self.retry_count < self.config.max_retries:
            self.retry_count += 1
            self.logger.info(f"准备重试作业 {job_id}，第 {self.retry_count}/{self.config.max_retries} 次")
            
            # 延迟重试
            self.scheduler.add_job(
                func=self.run_data_pipeline,
                trigger='date',
                run_date=datetime.now().timestamp() + self.config.retry_delay,
                id=f"{job_id}_retry_{self.retry_count}",
                replace_existing=True
            )
        else:
            self.logger.error(f"作业 {job_id} 重试次数已达上限，停止重试")
            self.retry_count = 0
    
    def run_data_pipeline(self):
        """运行数据管道的包装方法"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("定时任务触发：开始执行数据管道")
            self.logger.info("=" * 80)
            
            # 检查是否应该执行（增量模式下的额外检查）
            if self.config.enable_incremental and not self.pipeline_service.should_run_incremental():
                self.logger.info("根据增量策略，跳过此次执行")
                return
            
            # 执行数据管道
            success = self.pipeline_service.run_pipeline()
            
            if success:
                self.logger.info("定时任务完成：数据管道执行成功")
            else:
                raise Exception("数据管道执行失败")
                
        except Exception as e:
            self.logger.error(f"定时任务失败: {str(e)}")
            raise  # 重新抛出异常以触发重试机制
    
    def add_pipeline_job(self):
        """添加数据管道定时任务"""
        try:
            # 解析cron表达式
            cron_parts = self.config.schedule_cron.split()
            if len(cron_parts) != 5:
                raise ValueError(f"无效的cron表达式: {self.config.schedule_cron}")
            
            minute, hour, day, month, day_of_week = cron_parts
            
            # 添加定时任务
            self.scheduler.add_job(
                func=self.run_data_pipeline,
                trigger='cron',
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                id='dblp_pipeline_job',
                name='DBLP数据管道',
                replace_existing=True
            )
            
            self.logger.info(f"已添加定时任务: {self.config.schedule_cron}")
            
        except Exception as e:
            self.logger.error(f"添加定时任务失败: {e}")
            raise
    
    def add_manual_job(self, delay_seconds: int = 10):
        """添加手动执行任务（延迟执行）"""
        try:
            run_time = datetime.now().timestamp() + delay_seconds
            
            self.scheduler.add_job(
                func=self.run_data_pipeline,
                trigger='date',
                run_date=run_time,
                id='dblp_pipeline_manual',
                name='DBLP数据管道（手动触发）',
                replace_existing=True
            )
            
            self.logger.info(f"已添加手动任务，将在 {delay_seconds} 秒后执行")
            
        except Exception as e:
            self.logger.error(f"添加手动任务失败: {e}")
            raise
    
    def start(self, manual_run: bool = False):
        """启动调度器"""
        try:
            self.logger.info("正在启动数据管道调度器...")
            
            # 测试数据库连接
            if not self.db_manager.test_connection():
                raise Exception("数据库连接测试失败")
            
            # 添加任务
            if manual_run:
                self.add_manual_job()
                self.logger.info("调度器已启动（手动模式），数据管道将立即执行...")
            else:
                self.add_pipeline_job()
                next_run = self.scheduler.get_job('dblp_pipeline_job').next_run_time
                self.logger.info(f"调度器已启动，下次运行时间: {next_run}")
            
            self.logger.info("按Ctrl+C停止调度器")
            
            # 启动调度器
            self.scheduler.start()
            
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("收到停止信号，正在关闭调度器...")
            self.shutdown()
        except Exception as e:
            self.logger.error(f"调度器启动失败: {e}")
            raise
    
    def shutdown(self):
        """关闭调度器"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            
            if self.db_manager:
                self.db_manager.disconnect()
            
            self.logger.info("调度器已关闭")
            
        except Exception as e:
            self.logger.error(f"调度器关闭失败: {e}")
    
    def list_jobs(self):
        """列出所有任务"""
        jobs = self.scheduler.get_jobs()
        if not jobs:
            self.logger.info("没有已注册的任务")
            return
        
        self.logger.info("已注册的任务:")
        for job in jobs:
            self.logger.info(f"- {job.id}: {job.name}, 下次运行: {job.next_run_time}")
    
    def remove_job(self, job_id: str):
        """移除指定任务"""
        try:
            self.scheduler.remove_job(job_id)
            self.logger.info(f"已移除任务: {job_id}")
        except Exception as e:
            self.logger.error(f"移除任务失败: {e}")


def main():
    """主函数"""
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='DBLP数据管道调度器')
    parser.add_argument('--manual', action='store_true', help='手动执行一次')
    parser.add_argument('--list-jobs', action='store_true', help='列出所有任务')
    parser.add_argument('--config', type=str, help='配置文件路径')
    
    args = parser.parse_args()
    
    # 加载配置
    config = AppConfig.from_env()
    
    # 验证配置
    if not config.validate():
        print("配置验证失败，请检查环境变量")
        return
    
    # 创建调度器
    scheduler = DataPipelineScheduler(config)
    
    try:
        if args.list_jobs:
            scheduler.list_jobs()
        elif args.manual:
            scheduler.start(manual_run=True)
        else:
            scheduler.start(manual_run=False)
            
    except KeyboardInterrupt:
        print("\n收到中断信号，正在停止...")
    except Exception as e:
        print(f"调度器运行失败: {e}")
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()