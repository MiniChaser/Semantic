#!/usr/bin/env python3
"""
Author Processing Step 3: Create Final Author Table
Creates and populates the final target table with all computed metrics
"""

import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.author_service.final_author_table_service import FinalAuthorTableService


def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step3_create_final_table.log')
        ]
    )


def main():
    """Execute Step 3: Create Final Author Table"""
    
    print("🎯 Step 3: Creating Final Target Table")
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
        
        # Initialize service
        final_table_service = FinalAuthorTableService(db_manager)
        
        # Create final author table
        if not final_table_service.create_final_author_table():
            print("❌ Failed to create final author table")
            return 1
        print("✅ Final author table structure created")
        
        # Populate final author table
        final_stats = final_table_service.populate_final_author_table()
        if 'error' in final_stats:
            print(f"❌ Failed to populate final table: {final_stats['error']}")
            return 1
        
        print("✅ Final author table populated successfully!")
        print(f"👥 Total authors processed: {final_stats['total_authors_processed']}")
        print(f"📋 Complete data records: {final_stats['authors_with_complete_data']}")
        print(f"⚠️ Partial data records: {final_stats['authors_with_partial_data']}")
        
        # Show sample records
        print(f"\n🔍 Sample Final Table Records:")
        sample_records = final_table_service.get_sample_records(5)
        for i, record in enumerate(sample_records, 1):
            print(f"  {i}. {record['dblp_author']}")
            print(f"     Papers: {record['first_author_count']}, Career: {record['career_length']} years")
            print(f"     Last author: {record['last_author_percentage']}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 3 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())