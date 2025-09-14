#!/usr/bin/env python3
"""
Author Processing Step 1: Create Authorships Table
Creates and populates the authorships table with paper-author relationships
"""

import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.author_service.author_profile_service import AuthorProfileService


def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step1_create_authorships.log')
        ]
    )


def main():
    """Execute Step 1: Create Authorships Table"""
    
    print("📋 Step 1: Creating Authorships Table")
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
        profile_service = AuthorProfileService(db_manager)
        
        # Create authorships table
        if not profile_service.create_authorships_table():
            print("❌ Failed to create authorships table")
            return 1
        print("✅ Authorships table created")
        
        # Populate authorships table
        authorship_stats = profile_service.populate_authorships_table()
        if 'error' in authorship_stats:
            print(f"❌ Failed to populate authorships table: {authorship_stats['error']}")
            return 1
        
        print("✅ Authorships table populated successfully!")
        print(f"📊 Processed {authorship_stats['processed_papers']} papers")
        print(f"📋 Created {authorship_stats['total_authorships']} authorship records")
        print(f"🔗 Matched {authorship_stats['matched_authors']} authors")
        print(f"❓ Unmatched {authorship_stats['unmatched_authors']} authors")
        
        return 0
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 1 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())