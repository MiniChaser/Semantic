#!/usr/bin/env python3
"""
Author Processing Step 5: Generate Reports
Generates comprehensive reports and statistics for the Phase 1 implementation
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from decimal import Decimal

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.author_service.author_profile_service import AuthorProfileService
from semantic.services.author_service.author_metrics_service import AuthorMetricsService
from semantic.services.author_service.final_author_table_service import FinalAuthorTableService


def convert_decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_to_float(item) for item in obj]
    return obj


def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step5_generate_reports.log')
        ]
    )


def main():
    """Execute Step 5: Generate Reports"""
    
    print("📊 Step 5: Generating Reports")
    print("=" * 40)
    
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Load configuration and initialize database
        config = AppConfig.from_env()
        db_manager = get_db_manager()
        logger.info("✅ Database connection established")
        print("✅ Database connection established")
        
        # Initialize services
        profile_service = AuthorProfileService(db_manager)
        metrics_service = AuthorMetricsService(db_manager)
        final_table_service = FinalAuthorTableService(db_manager)
        
        # Create reports directory
        reports_dir = Path("data/reports")
        reports_dir.mkdir(exist_ok=True)
        
        # Generate final table report
        final_report_path = reports_dir / "final_author_table_report.json"
        if final_table_service.generate_final_table_report(str(final_report_path)):
            print(f"✅ Final table report: {final_report_path}")
        
        # Get comprehensive statistics
        processing_stats = profile_service.get_processing_statistics()
        metrics_stats = metrics_service.get_metrics_statistics()
        
        # Generate comprehensive Phase 1 report
        phase1_report = {
            'generation_timestamp': datetime.now().isoformat(),
            'report_description': 'Complete Author Processing Phase 1 Implementation Report',
            'comprehensive_processing_stats': processing_stats,
            'comprehensive_metrics_stats': metrics_stats,
            'phase_completion_status': {
                'authorships_table': 'completed',
                'author_profiles_table': 'completed',
                'collaboration_metrics': 'completed',
                'rising_star_metrics': 'completed',
                'comprehensive_rankings': 'completed',
                'final_author_table': 'completed'
            },
            'database_tables_created': [
                'authorships - Paper-author relationships',
                'author_profiles - Unique author profiles', 
                'author_collaboration_metrics - Collaboration networks',
                'author_rising_star_metrics - Rising star analysis',
                'author_comprehensive_rankings - Multi-dimensional rankings',
                'final_author_table - Target output table'
            ],
            'implementation_notes': {
                'phase_1_completed_features': [
                    'Multi-tier author disambiguation (6 tiers)',
                    'Paper-author relationship mapping',
                    'Unique author profile creation',
                    'Collaboration network analysis',
                    'Rising star detection algorithm',
                    'Comprehensive ranking system',
                    'DBLP alias extraction using 4-digit pattern',
                    'Final table matching document specifications'
                ],
                'phase_2_todo_items': [
                    'Google Scholar API integration',
                    'S2 Author API enhancement', 
                    'CSRankings data integration',
                    'Top venue classification system',
                    'Homepage and affiliation extraction',
                    'Influential citation aggregation'
                ]
            }
        }
        
        # Convert Decimal objects and save report
        phase1_report_path = reports_dir / "phase1_implementation_final_report.json"
        phase1_report_converted = convert_decimal_to_float(phase1_report)
        with open(phase1_report_path, 'w', encoding='utf-8') as f:
            json.dump(phase1_report_converted, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Phase 1 comprehensive report: {phase1_report_path}")
        
        # Display summary
        print(f"\n📁 Generated Reports:")
        print(f"  📊 Final table report: {final_report_path}")
        print(f"  📈 Phase 1 comprehensive report: {phase1_report_path}")
        
        print("\n✅ All reports generated successfully!")
        
        return 0
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 5 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())