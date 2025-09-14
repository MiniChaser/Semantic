#!/usr/bin/env python3
"""
Author Processing Phase 1 - Complete Implementation
Executes the complete author data processing pipeline without S2 API calls
"""

import sys
import os
import logging
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.author_service.author_profile_service import AuthorProfileService
from semantic.services.author_service.author_metrics_service import AuthorMetricsService
from semantic.services.author_service.final_author_table_service import FinalAuthorTableService


def convert_decimal_to_float(obj):
    """
    Convert Decimal objects to float for JSON serialization
    
    Args:
        obj: Object to convert
        
    Returns:
        Converted object with Decimal -> float
    """
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
            logging.FileHandler(log_dir / 'author_processing_phase1.log')
        ]
    )


def main():
    """Execute the complete Author Processing Phase 1"""
    
    # Record start time
    start_time = datetime.now()
    start_timestamp = time.time()
    
    print("🚀 Starting Author Processing Phase 1")
    print("=" * 60)
    print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        logger.info("Starting Author Processing Phase 1")
        
        # Load configuration and initialize database
        config = AppConfig.from_env()
        db_manager = get_db_manager()
        logger.info("✅ Database connection established")
        print("✅ Database connection established")
        
        # Initialize services
        profile_service = AuthorProfileService(db_manager)
        metrics_service = AuthorMetricsService(db_manager)
        final_table_service = FinalAuthorTableService(db_manager)
        
        print("✅ All services initialized")
        
        # Phase 1.1: Create and populate authorships table
        print("\n📋 Phase 1.1: Creating Authorships Table")
        print("-" * 40)
        
        if not profile_service.create_authorships_table():
            print("❌ Failed to create authorships table")
            return 1
        print("✅ Authorships table created")
        
        authorship_stats = profile_service.populate_authorships_table()
        if 'error' in authorship_stats:
            print(f"❌ Failed to populate authorships table: {authorship_stats['error']}")
            return 1
        
        print("✅ Authorships table populated successfully!")
        print(f"📊 Processed {authorship_stats['processed_papers']} papers")
        print(f"📋 Created {authorship_stats['total_authorships']} authorship records")
        print(f"🔗 Matched {authorship_stats['matched_authors']} authors")
        print(f"❓ Unmatched {authorship_stats['unmatched_authors']} authors")
        
        # Phase 1.2: Create and populate author profiles table
        print("\n👤 Phase 1.2: Creating Author Profiles Table")
        print("-" * 40)
        
        if not profile_service.create_author_profiles_table():
            print("❌ Failed to create author profiles table")
            return 1
        print("✅ Author profiles table created")
        
        profile_stats = profile_service.populate_author_profiles_table()
        if 'error' in profile_stats:
            print(f"❌ Failed to populate author profiles: {profile_stats['error']}")
            return 1
        
        print("✅ Author profiles table populated successfully!")
        print(f"👥 Total unique authors: {profile_stats['total_unique_authors']}")
        print(f"🆔 Authors with S2 ID: {profile_stats['authors_with_s2_id']}")
        print(f"❓ Authors without S2 ID: {profile_stats['authors_without_s2_id']}")
        
        # Phase 1.3: Calculate advanced metrics
        print("\n📈 Phase 1.3: Calculating Advanced Metrics")
        print("-" * 40)
        
        if not metrics_service.create_author_metrics_tables():
            print("❌ Failed to create metrics tables")
            return 1
        print("✅ Metrics tables created")
        
        # Calculate collaboration metrics
        print("🤝 Calculating collaboration network metrics...")
        collab_stats = metrics_service.calculate_collaboration_metrics()
        if 'error' in collab_stats:
            print(f"⚠️ Collaboration metrics warning: {collab_stats['error']}")
        else:
            print(f"✅ Processed {collab_stats['processed_authors']} authors for collaboration")
        
        # Calculate rising star metrics
        print("⭐ Calculating rising star metrics...")
        rising_stats = metrics_service.calculate_rising_star_metrics()
        if 'error' in rising_stats:
            print(f"⚠️ Rising star metrics warning: {rising_stats['error']}")
        else:
            print(f"✅ Processed {rising_stats['processed_authors']} authors for rising star analysis")
        
        # Calculate comprehensive rankings
        print("🏆 Calculating comprehensive rankings...")
        ranking_stats = metrics_service.calculate_comprehensive_rankings()
        if 'error' in ranking_stats:
            print(f"⚠️ Rankings calculation warning: {ranking_stats['error']}")
        else:
            print(f"✅ Processed {ranking_stats['processed_authors']} authors for comprehensive ranking")
        
        # Phase 1.4: Create final target table
        print("\n🎯 Phase 1.4: Creating Final Target Table")
        print("-" * 40)
        
        if not final_table_service.create_final_author_table():
            print("❌ Failed to create final author table")
            return 1
        print("✅ Final author table structure created")
        
        final_stats = final_table_service.populate_final_author_table()
        if 'error' in final_stats:
            print(f"❌ Failed to populate final table: {final_stats['error']}")
            return 1
        
        print("✅ Final author table populated successfully!")
        print(f"👥 Total authors processed: {final_stats['total_authors_processed']}")
        print(f"📋 Complete data records: {final_stats['authors_with_complete_data']}")
        print(f"⚠️ Partial data records: {final_stats['authors_with_partial_data']}")
        
        # Phase 1.5: Generate reports and statistics
        print("\n📊 Phase 1.5: Generating Reports")
        print("-" * 40)
        
        # Create reports directory
        reports_dir = Path("data/reports")
        reports_dir.mkdir(exist_ok=True)
        
        # Generate final table report
        final_report_path = reports_dir / "final_author_table_report.json"
        if final_table_service.generate_final_table_report(str(final_report_path)):
            print(f"✅ Final table report: {final_report_path}")
        
        # Get comprehensive processing statistics
        processing_stats = profile_service.get_processing_statistics()
        metrics_stats = metrics_service.get_metrics_statistics()
        
        # Calculate timing statistics
        end_time = datetime.now()
        end_timestamp = time.time()
        total_duration = end_timestamp - start_timestamp
        duration_formatted = str(timedelta(seconds=int(total_duration)))
        
        # Generate comprehensive phase 1 report
        phase1_report = {
            'execution_metadata': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'total_duration': duration_formatted,
                'total_duration_seconds': total_duration
            },
            'authorships_statistics': authorship_stats,
            'profiles_statistics': profile_stats,
            'collaboration_statistics': collab_stats,
            'rising_star_statistics': rising_stats,
            'ranking_statistics': ranking_stats,
            'final_table_statistics': final_stats,
            'comprehensive_processing_stats': processing_stats,
            'comprehensive_metrics_stats': metrics_stats,
            'phase_completion': {
                'authorships_table': 'completed',
                'author_profiles_table': 'completed', 
                'collaboration_metrics': 'completed',
                'rising_star_metrics': 'completed',
                'comprehensive_rankings': 'completed',
                'final_author_table': 'completed'
            },
            'data_quality_summary': {
                'total_papers_processed': authorship_stats.get('processed_papers', 0),
                'total_authorships_created': authorship_stats.get('total_authorships', 0),
                'total_unique_authors': profile_stats.get('total_unique_authors', 0),
                'author_matching_success_rate': (
                    authorship_stats.get('matched_authors', 0) / 
                    (authorship_stats.get('matched_authors', 0) + authorship_stats.get('unmatched_authors', 1))
                ) * 100 if authorship_stats else 0,
                'authors_with_s2_integration': profile_stats.get('authors_with_s2_id', 0),
                'final_table_records': final_stats.get('total_authors_processed', 0)
            }
        }
        
        # Convert Decimal objects and save phase 1 report
        phase1_report_path = reports_dir / "author_processing_phase1_report.json"
        phase1_report_converted = convert_decimal_to_float(phase1_report)
        with open(phase1_report_path, 'w', encoding='utf-8') as f:
            json.dump(phase1_report_converted, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Phase 1 comprehensive report: {phase1_report_path}")
        
        # Display final summary
        print("\n🎉 PHASE 1 COMPLETION SUMMARY")
        print("=" * 60)
        print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏁 Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⌛ Total duration: {duration_formatted}")
        print()
        
        print("📋 DATABASE TABLES CREATED:")
        tables = [
            "✅ authorships - Paper-author relationships", 
            "✅ author_profiles - Unique author profiles",
            "✅ author_collaboration_metrics - Collaboration networks",
            "✅ author_rising_star_metrics - Rising star analysis", 
            "✅ author_comprehensive_rankings - Multi-dimensional rankings",
            "✅ final_author_table - Target output table"
        ]
        for table in tables:
            print(f"  {table}")
        
        print(f"\n📊 KEY STATISTICS:")
        print(f"  📄 Papers processed: {authorship_stats.get('processed_papers', 0):,}")
        print(f"  📋 Authorship records: {authorship_stats.get('total_authorships', 0):,}")
        print(f"  👥 Unique authors: {profile_stats.get('total_unique_authors', 0):,}")
        print(f"  🔗 Match success rate: {phase1_report['data_quality_summary']['author_matching_success_rate']:.1f}%")
        print(f"  🎯 Final table records: {final_stats.get('total_authors_processed', 0):,}")
        
        print(f"\n📁 GENERATED REPORTS:")
        print(f"  📊 Final table report: {final_report_path}")
        print(f"  📈 Phase 1 comprehensive report: {phase1_report_path}")
        
        # Show sample final table records
        print(f"\n🔍 SAMPLE FINAL TABLE RECORDS:")
        sample_records = final_table_service.get_sample_records(5)
        for i, record in enumerate(sample_records, 1):
            print(f"  {i}. {record['dblp_author']}")
            print(f"     Papers: {record['first_author_count']}, Career: {record['career_length']} years")
            print(f"     Last author: {record['last_author_percentage']:.1f}%")
        
        print("\n✅ Author Processing Phase 1 completed successfully!")
        print("🚀 Ready for Phase 2 (S2 API enhancement) when needed")
        
        return 0
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        logging.getLogger(__name__).error(f"Phase 1 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())