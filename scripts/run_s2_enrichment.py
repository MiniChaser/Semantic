#!/usr/bin/env python3
"""
S2 Enrichment Script
Processes papers individually with Semantic Scholar integration for incremental processing
Each paper is saved to database immediately after processing to support interruption and resume
"""

import sys
import os
import logging
import time
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.services.s2_service.s2_paper_enrichment_service import S2EnrichmentService
from semantic.database.connection import get_db_manager


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/s2_enrichment.log')
        ]
    )




async def main():
    """Run S2 enrichment"""
    # Record start time
    start_time = datetime.now()
    start_timestamp = time.time()
    
    print("Starting S2 Enrichment Process")
    print("=" * 50)
    print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Load configuration
        config = AppConfig.from_env()
        logger.info(f"Loaded configuration: {config}")
        
        # Check for API key
        api_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY')
        if api_key:
            logger.info("✅ Semantic Scholar API key loaded")
            print("✅ API key found - will use authenticated requests (100 req/sec)")
        else:
            logger.warning("⚠️ No API key found - using public rate limits (100 req/5min)")
            print("⚠️ No API key - using public rate limits (100 requests per 5 minutes)")
        
        # Initialize database manager
        db_manager = get_db_manager()
        logger.info("✅ Database connection established")
        print("✅ Database connection established")
        
        # Initialize S2 enrichment service
        s2_service = S2EnrichmentService(
            config=config,
            db_manager=db_manager,
            api_key=api_key
        )
        
        logger.info("✅ S2 Enrichment Service initialized")
        print("✅ S2 Enrichment Service initialized")
        
        # Process papers individually (each paper is saved immediately)
        print("\n🔄 Starting individual paper enrichment process...")
        success = s2_service.enrich_papers(limit=100000)
        
        if success:
            # Calculate timing statistics
            end_time = datetime.now()
            end_timestamp = time.time()
            total_duration = end_timestamp - start_timestamp
            duration_formatted = str(timedelta(seconds=int(total_duration)))
            
            print("✅ S2 enrichment completed successfully!")
            
            # Get and display statistics
            stats = s2_service.get_enrichment_statistics()
            print("\n📊 ENRICHMENT STATISTICS:")
            print(f"Total DBLP papers in database: {stats.get('total_dblp_papers', 0)}")
            print(f"Total enriched papers: {stats.get('total_enriched_papers', 0)}")
            print(f"Enrichment coverage: {stats.get('enrichment_coverage', 0):.1f}%")
            print(f"S2 match rate: {stats.get('s2_match_rate', 0):.1f}%")
            
            # Show tier distribution
            tier_dist = stats.get('validation_tiers', {})
            if tier_dist:
                print("\n🎯 VALIDATION TIERS:")
                for tier, count in tier_dist.items():
                    print(f"  {tier}: {count}")
            
            # Show timing and performance statistics
            print("\n⏱️  PERFORMANCE STATISTICS:")
            print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🏁 Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⌛ Total duration: {duration_formatted}")
            
            # Get processing statistics from the service
            processed_papers = s2_service.stats.get('papers_processed', 0)
            if processed_papers > 0:
                avg_time_per_paper = total_duration / processed_papers
                papers_per_minute = processed_papers / (total_duration / 60) if total_duration > 0 else 0
                papers_per_hour = papers_per_minute * 60
                
                print(f"📄 Papers processed this run: {processed_papers}")
                print(f"⚡ Average time per paper: {avg_time_per_paper:.2f} seconds")
                print(f"📈 Processing rate: {papers_per_minute:.1f} papers/minute ({papers_per_hour:.0f} papers/hour)")
                
                # Estimate time for remaining papers
                total_dblp_papers = stats.get('total_dblp_papers', 0)
                total_enriched = stats.get('total_enriched_papers', 0)
                remaining_papers = max(0, total_dblp_papers - total_enriched)
                
                if remaining_papers > 0:
                    estimated_time_seconds = remaining_papers * avg_time_per_paper
                    estimated_time_formatted = str(timedelta(seconds=int(estimated_time_seconds)))
                    estimated_days = estimated_time_seconds / (24 * 3600)
                    
                    print(f"\n📊 COMPLETION ESTIMATES:")
                    print(f"📋 Remaining papers to process: {remaining_papers:,}")
                    print(f"🕐 Estimated time to complete all: {estimated_time_formatted}")
                    if estimated_days >= 1:
                        print(f"📅 Estimated days: {estimated_days:.1f} days")
                    
                    # Show different scenarios
                    if papers_per_hour > 0:
                        print(f"\n⚡ PROCESSING SCENARIOS:")
                        print(f"🔄 Continuous processing: {estimated_time_formatted}")
                        print(f"🕒 8 hours/day: {(estimated_time_seconds / (8 * 3600)):.1f} working days")
                        print(f"📅 24/7 processing: {estimated_days:.1f} calendar days")
            else:
                print("📄 No papers were processed this run")
            
            # Export results to CSV
            print("\n💾 Exporting results to CSV...")
            export_path = "data/s2_enriched_test_results.csv"
            os.makedirs("data", exist_ok=True)
            
            if s2_service.export_enriched_papers(export_path, include_all_fields=False):
                print(f"✅ Results exported to: {export_path}")
            else:
                print("❌ Failed to export CSV results")
            
            # Generate JSON validation report
            print("\n📋 Generating validation report...")
            json_report_path = "data/s2_validation_report.json"
            
            if s2_service.generate_validation_report(json_report_path):
                print(f"✅ Validation report generated: {json_report_path}")
            else:
                print("❌ Failed to generate validation report")
            
            print("\n📊 FINAL SUMMARY:")
            print("=" * 50)
            print("✅ S2 enrichment process completed successfully!")
            
        else:
            print("❌ S2 enrichment failed!")
            return 1
        
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.getLogger(__name__).error(f"Script failed: {e}", exc_info=True)
        return 1
    
    print("\n🎉 S2 enrichment test completed!")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))