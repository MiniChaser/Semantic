"""
数据管道服务
实现增量处理逻辑和完整的数据处理流程
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
    """数据管道服务"""
    
    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.paper_repo = PaperRepository(self.db_manager)
        self.dblp_service = DBLPService(config)
        self.logger = self._setup_logger()
        
        # 管道状态
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
        """第一步：准备数据（下载和解压）"""
        try:
            self.logger.info(f"[{datetime.now()}] 执行Step 1：Prepare DBLP data")
            
            # 根据增量模式决定是否强制下载
            force_download = not self.config.enable_incremental
            
            if not self.dblp_service.prepare_data(force_download=force_download):
                raise Exception("DBLP数据准备失败")
            
            self.logger.info("Step 1完成：DBLP数据准备成功")
            return True
            
        except Exception as e:
            self.logger.error(f"Step 1失败：{e}")
            return False
    
    def step2_extract_papers(self) -> List[Paper]:
        """第二步：Extract paper data"""
        try:
            self.logger.info(f"[{datetime.now()}] 执行Step 2：Extract paper data")
            
            papers = []
            existing_keys = set()
            
            # 如果启用增量处理，获取已存在的论文键值
            if self.config.enable_incremental:
                self.logger.info("增量模式：获取已存在论文列表...")
                existing_keys = self._get_existing_paper_keys()
                self.logger.info(f"已存在论文数量: {len(existing_keys)}")
                self.current_process_type = "dblp_incremental_sync"
            
            # 解析论文
            papers = self.dblp_service.parse_papers(
                incremental=self.config.enable_incremental,
                existing_keys=existing_keys
            )
            
            if not papers:
                self.logger.warning("没有提取到新论文数据")
                return []
            
            self.stats['papers_processed'] = len(papers)
            self.logger.info(f"Step 2完成：提取到 {len(papers)} 篇论文")
            return papers
            
        except Exception as e:
            self.logger.error(f"Step 2失败：{e}")
            return []
    
    def step3_load_papers(self, papers: List[Paper]) -> bool:
        """第三步：Load papers to database"""
        try:
            if not papers:
                self.logger.info("没有论文需要加载")
                return True
            
            self.logger.info(f"[{datetime.now()}] 执行Step 3：加载 {len(papers)} 篇论文到数据库")
            
            # 确保数据库表存在
            if not self.paper_repo.create_tables():
                raise Exception("数据库表创建失败")
            
            # 批量插入论文
            inserted, updated, errors = self.paper_repo.batch_insert_papers(papers)
            
            # 更新统计信息
            self.stats['papers_inserted'] = inserted
            self.stats['papers_updated'] = updated
            self.stats['errors'] = errors
            
            self.logger.info(
                f"Step 3完成：插入 {inserted} 篇，更新 {updated} 篇，错误 {errors} 篇"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Step 3失败：{e}")
            return False
    
    def step4_post_process(self) -> bool:
        """第四步：Post processing（清理文件、记录元数据等）"""
        try:
            self.logger.info(f"[{datetime.now()}] 执行Step 4：Post processing")
            
            # 记录处理元数据
            success = self.stats['errors'] == 0
            status = 'success' if success else 'partial_success'
            
            self.paper_repo.record_processing_meta(
                process_type=self.current_process_type,
                status=status,
                records_processed=self.stats['papers_processed'],
                records_inserted=self.stats['papers_inserted'],
                records_updated=self.stats['papers_updated'],
                error_message=None if success else f"处理过程中出现 {self.stats['errors']} 个错误"
            )
            
            # 清理临时文件（保留XML文件以备下次增量使用）
            if not self.config.enable_incremental:
                self.dblp_service.cleanup(keep_xml=False)
            else:
                self.dblp_service.cleanup(keep_xml=True)
            
            self.logger.info("Step 4完成：Post processing完成")
            return True
            
        except Exception as e:
            self.logger.error(f"Step 4失败：{e}")
            return False
    
    def run_pipeline(self) -> bool:
        """运行整个数据管道"""
        self.start_time = datetime.now()
        self.logger.info(f"\n[{self.start_time}] Starting data pipeline execution")
        self.logger.info(f"处理模式: {'增量' if self.config.enable_incremental else '全量'}")
        
        try:
            # Reset statistics
            self._reset_stats()
            
            # Step 1: 准备数据
            if not self.step1_prepare_data():
                raise Exception("数据准备失败")
            
            # Step 2: 提取论文
            papers = self.step2_extract_papers()
            if papers is None:  # 区分空列表和失败
                raise Exception("论文提取失败")
            
            # Step 3: 加载论文
            if not self.step3_load_papers(papers):
                raise Exception("论文加载失败")
            
            # Step 4: Post processing
            if not self.step4_post_process():
                raise Exception("Post processing失败")
            
            # 生成最终报告
            self._generate_final_report()
            
            self.logger.info(f"[{datetime.now()}] Data pipeline execution completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"[{datetime.now()}] Data pipeline execution failed: {str(e)}")
            
            # 记录失败的元数据
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
        """获取数据库中已存在的论文键值"""
        try:
            # 如果配置了检查天数，只检查最近的论文
            if self.config.incremental_check_days > 0:
                cutoff_date = datetime.now() - timedelta(days=self.config.incremental_check_days)
                query = """
                SELECT key FROM dblp_papers 
                WHERE update_time >= %s OR created_at >= %s
                """
                results = self.db_manager.fetch_all(query, (cutoff_date, cutoff_date))
            else:
                # 获取所有论文键值
                query = "SELECT key FROM dblp_papers"
                results = self.db_manager.fetch_all(query)
            
            return {row['key'] for row in results}
            
        except Exception as e:
            self.logger.error(f"获取已存在论文键值失败: {e}")
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
        """生成最终报告"""
        end_time = datetime.now()
        duration = end_time - self.start_time if self.start_time else timedelta(0)
        
        # 获取数据库统计信息
        db_stats = self.paper_repo.get_statistics()
        
        self.logger.info("=" * 80)
        self.logger.info("DBLP数据处理管道完成")
        self.logger.info("=" * 80)
        self.logger.info(f"处理时间: {duration}")
        self.logger.info(f"处理论文数: {self.stats['papers_processed']}")
        self.logger.info(f"新增论文: {self.stats['papers_inserted']}")
        self.logger.info(f"更新论文: {self.stats['papers_updated']}")
        self.logger.info(f"错误数量: {self.stats['errors']}")
        self.logger.info(f"数据库总论文数: {db_stats.get('total_papers', 0)}")
        self.logger.info(f"最后更新时间: {db_stats.get('last_update', 'N/A')}")
        
        # 按会议统计
        venue_stats = db_stats.get('by_venue', {})
        if venue_stats:
            self.logger.info("按会议统计:")
            for venue, count in list(venue_stats.items())[:5]:  # 显示前5个
                self.logger.info(f"  {venue}: {count}")
    
    def get_last_successful_run(self) -> datetime:
        """获取上次成功运行时间"""
        return self.paper_repo.get_last_successful_run(self.current_process_type)
    
    def should_run_incremental(self) -> bool:
        """判断是否应该运行增量更新"""
        if not self.config.enable_incremental:
            return False
        
        last_run = self.get_last_successful_run()
        if not last_run:
            self.logger.info("未找到上次成功运行记录，将执行全量同步")
            return False
        
        # 检查是否超过了增量检查间隔
        time_since_last_run = datetime.now() - last_run
        if time_since_last_run.days >= self.config.incremental_check_days:
            self.logger.info(f"距离上次运行已超过 {self.config.incremental_check_days} 天，执行增量更新")
            return True
        
        self.logger.info(f"距离上次运行仅 {time_since_last_run.days} 天，跳过此次运行")
        return False
    
    def export_to_csv(self, output_path: str = "data/dblp_papers_export.csv") -> bool:
        """导出数据到CSV文件"""
        try:
            import pandas as pd
            import os
            
            self.logger.info(f"导出数据到CSV: {output_path}")
            
            # 创建输出目录
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # 查询数据
            query = """
            SELECT key, title, 
                   array_to_string(ARRAY(SELECT jsonb_array_elements_text(authors)), '|') as authors,
                   year, pages, ee, venue, booktitle, doi
            FROM dblp_papers 
            ORDER BY venue, year DESC, key
            """
            
            df = pd.read_sql_query(query, self.db_manager.get_connection())
            
            # 保存到CSV
            df.to_csv(output_path, index=False)
            
            self.logger.info(f"CSV导出完成: {len(df)} 行数据")
            return True
            
        except Exception as e:
            self.logger.error(f"CSV导出失败: {e}")
            return False