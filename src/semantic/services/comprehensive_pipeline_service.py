"""
Comprehensive Pipeline Service
Orchestrates the complete data processing pipeline including 3 phases:
1. DBLP data pipeline
2. Semantic Scholar enrichment
3. Author processing
"""

import os
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Tuple
from ..utils.config import AppConfig
from ..database.connection import DatabaseManager, get_db_manager


class ComprehensivePipelineService:
    """Comprehensive Pipeline Service that runs all data processing phases"""
    
    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.logger = self._setup_logger()
        
        # Define the script execution sequence
        self.script_sequence = [
            {
                'name': 'DBLP Data Pipeline',
                'script_path': 'scripts/run_pipeline_once.py',
                'description': 'Download and process DBLP data',
                'timeout': 3600  # 1 hour timeout
            },
            {
                'name': 'Semantic Scholar Enrichment',
                'script_path': 'scripts/run_s2_enrichment.py', 
                'description': 'Enrich papers with Semantic Scholar data',
                'timeout': 7200  # 2 hour timeout
            },
            {
                'name': 'Author Processing Phase 1',
                'script_path': 'scripts/run_author_processing_phase1.py',
                'description': 'Process author profiles and metrics',
                'timeout': 1800  # 30 minutes timeout
            }
        ]
        
        # Pipeline execution statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_duration': 0,
            'scripts_executed': 0,
            'scripts_successful': 0,
            'scripts_failed': 0,
            'execution_results': []
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.ComprehensivePipelineService')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            # Create logs directory
            log_dir = Path('logs')
            log_dir.mkdir(exist_ok=True)
            
            # File handler
            file_handler = logging.FileHandler(
                log_dir / f'comprehensive_pipeline_{datetime.now().strftime("%Y%m%d")}.log'
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
    
    def _execute_script(self, script_info: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute a single script
        
        Args:
            script_info: Script information dictionary
            
        Returns:
            Tuple of (success, execution_result)
        """
        script_path = script_info['script_path']
        script_name = script_info['name']
        timeout = script_info.get('timeout', 3600)
        
        # Get project root directory
        project_root = Path(__file__).parent.parent.parent.parent
        full_script_path = project_root / script_path
        
        if not full_script_path.exists():
            error_msg = f"Script not found: {full_script_path}"
            self.logger.error(error_msg)
            return False, {
                'script_name': script_name,
                'script_path': str(script_path),
                'success': False,
                'error': error_msg,
                'start_time': datetime.now(),
                'end_time': datetime.now(),
                'duration': 0,
                'return_code': -1
            }
        
        self.logger.info(f"Starting execution of {script_name}")
        self.logger.info(f"Script path: {full_script_path}")
        
        start_time = datetime.now()
        
        try:
            # Execute script using uv run python as specified in CLAUDE.md
            cmd = ['uv', 'run', 'python', str(full_script_path)]
            
            result = subprocess.run(
                cmd,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            execution_result = {
                'script_name': script_name,
                'script_path': str(script_path),
                'success': result.returncode == 0,
                'return_code': result.returncode,
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
            if result.returncode == 0:
                self.logger.info(f"✅ {script_name} completed successfully in {duration:.1f}s")
                return True, execution_result
            else:
                self.logger.error(f"❌ {script_name} failed with return code {result.returncode}")
                self.logger.error(f"Error output: {result.stderr}")
                return False, execution_result
                
        except subprocess.TimeoutExpired:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            error_msg = f"Script execution timed out after {timeout}s"
            self.logger.error(f"❌ {script_name} timed out after {timeout}s")
            
            return False, {
                'script_name': script_name,
                'script_path': str(script_path),
                'success': False,
                'error': error_msg,
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'return_code': -1
            }
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            error_msg = f"Script execution failed: {str(e)}"
            self.logger.error(f"❌ {script_name} execution failed: {e}")
            
            return False, {
                'script_name': script_name,
                'script_path': str(script_path),
                'success': False,
                'error': error_msg,
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'return_code': -1
            }
    
    def run_comprehensive_pipeline(self) -> bool:
        """
        Run the complete comprehensive pipeline
        
        Returns:
            True if all scripts execute successfully, False otherwise
        """
        self.logger.info("🚀 Starting Comprehensive Data Processing Pipeline")
        self.logger.info("=" * 80)
        
        # Initialize statistics
        self.stats['start_time'] = datetime.now()
        self.stats['execution_results'] = []
        
        # Track overall success
        overall_success = True
        
        # Execute each script in sequence
        for i, script_info in enumerate(self.script_sequence, 1):
            script_name = script_info['name']
            
            self.logger.info(f"\n📋 Phase {i}/{len(self.script_sequence)}: {script_name}")
            self.logger.info(f"Description: {script_info['description']}")
            self.logger.info("-" * 60)
            
            # Execute the script
            success, result = self._execute_script(script_info)
            
            # Update statistics
            self.stats['scripts_executed'] += 1
            self.stats['execution_results'].append(result)
            
            if success:
                self.stats['scripts_successful'] += 1
                self.logger.info(f"✅ Phase {i} completed successfully")
            else:
                self.stats['scripts_failed'] += 1
                overall_success = False
                
                # Log failure details
                self.logger.error(f"❌ Phase {i} failed")
                if 'error' in result:
                    self.logger.error(f"Error: {result['error']}")
                if 'stderr' in result and result['stderr']:
                    self.logger.error(f"Error output: {result['stderr']}")
                
                # Decide whether to continue or stop
                if self._should_continue_on_failure(script_info):
                    self.logger.warning(f"⚠️ Continuing to next phase despite failure in {script_name}")
                    continue
                else:
                    self.logger.error(f"🛑 Stopping pipeline execution due to critical failure in {script_name}")
                    break
        
        # Finalize statistics
        self.stats['end_time'] = datetime.now()
        self.stats['total_duration'] = (
            self.stats['end_time'] - self.stats['start_time']
        ).total_seconds()
        
        # Generate final report
        self._generate_comprehensive_report()
        
        return overall_success
    
    def _should_continue_on_failure(self, script_info: Dict[str, Any]) -> bool:
        """
        Determine if pipeline should continue after a script failure
        
        Args:
            script_info: Information about the failed script
            
        Returns:
            True if should continue, False if should stop
        """
        # For now, we'll be conservative and stop on any failure
        # This can be made configurable in the future
        return False
    
    def _generate_comprehensive_report(self):
        """Generate comprehensive execution report"""
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("🎯 COMPREHENSIVE PIPELINE EXECUTION REPORT")
        self.logger.info("=" * 80)
        
        # Overall statistics
        success_rate = (self.stats['scripts_successful'] / self.stats['scripts_executed']) * 100 if self.stats['scripts_executed'] > 0 else 0
        
        self.logger.info(f"⏰ Start time: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"🏁 End time: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"⌛ Total duration: {timedelta(seconds=int(self.stats['total_duration']))}")
        self.logger.info(f"📊 Scripts executed: {self.stats['scripts_executed']}")
        self.logger.info(f"✅ Scripts successful: {self.stats['scripts_successful']}")
        self.logger.info(f"❌ Scripts failed: {self.stats['scripts_failed']}")
        self.logger.info(f"📈 Success rate: {success_rate:.1f}%")
        
        # Detailed execution results
        self.logger.info(f"\n📋 DETAILED EXECUTION RESULTS:")
        self.logger.info("-" * 60)
        
        for i, result in enumerate(self.stats['execution_results'], 1):
            status = "✅" if result['success'] else "❌"
            duration = result['duration']
            
            self.logger.info(f"{status} Phase {i}: {result['script_name']}")
            self.logger.info(f"   Duration: {duration:.1f}s")
            self.logger.info(f"   Return code: {result['return_code']}")
            
            if not result['success'] and 'error' in result:
                self.logger.info(f"   Error: {result['error']}")
        
        # Final status
        if self.stats['scripts_failed'] == 0:
            self.logger.info(f"\n🎉 All phases completed successfully!")
        else:
            self.logger.info(f"\n⚠️ Pipeline completed with {self.stats['scripts_failed']} failures")
    
    def get_execution_statistics(self) -> Dict[str, Any]:
        """Get execution statistics"""
        return self.stats.copy()
    
    def export_execution_report(self, output_path: str = "data/comprehensive_pipeline_report.json") -> bool:
        """
        Export execution report to JSON file
        
        Args:
            output_path: Path to save the report
            
        Returns:
            True if export successful, False otherwise
        """
        try:
            import json
            
            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Prepare report data
            report_data = {
                'execution_metadata': {
                    'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
                    'end_time': self.stats['end_time'].isoformat() if self.stats['end_time'] else None,
                    'total_duration_seconds': self.stats['total_duration'],
                    'total_duration_formatted': str(timedelta(seconds=int(self.stats['total_duration'])))
                },
                'execution_summary': {
                    'scripts_executed': self.stats['scripts_executed'],
                    'scripts_successful': self.stats['scripts_successful'],
                    'scripts_failed': self.stats['scripts_failed'],
                    'success_rate': (self.stats['scripts_successful'] / self.stats['scripts_executed']) * 100 if self.stats['scripts_executed'] > 0 else 0
                },
                'detailed_results': []
            }
            
            # Add detailed results (convert datetime objects to strings)
            for result in self.stats['execution_results']:
                detailed_result = result.copy()
                if 'start_time' in detailed_result:
                    detailed_result['start_time'] = detailed_result['start_time'].isoformat()
                if 'end_time' in detailed_result:
                    detailed_result['end_time'] = detailed_result['end_time'].isoformat()
                report_data['detailed_results'].append(detailed_result)
            
            # Save to JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"✅ Execution report exported to: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to export execution report: {e}")
            return False