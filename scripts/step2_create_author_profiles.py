#!/usr/bin/env python3
"""
Author Processing Step 2: Create Author Profiles Table
Creates and populates the author profiles table with unique author information
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
            logging.FileHandler(log_dir / 'step2_create_author_profiles.log')
        ]
    )


def main():
    """Execute Step 2: Create Author Profiles Table"""
    
    print("👤 Step 2: Creating Author Profiles Table")
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
        
        # Create author profiles table
        if not profile_service.create_author_profiles_table():
            print("❌ Failed to create author profiles table")
            return 1
        print("✅ Author profiles table created")
        
        # Populate author profiles table
        profile_stats = profile_service.populate_author_profiles_table()
        if 'error' in profile_stats:
            print(f"❌ Failed to populate author profiles: {profile_stats['error']}")
            return 1
        
        print("✅ Author profiles table populated successfully!")
        print(f"👥 Total unique authors: {profile_stats['total_unique_authors']}")
        print(f"🆔 Authors with S2 ID: {profile_stats['authors_with_s2_id']}")
        print(f"❓ Authors without S2 ID: {profile_stats['authors_without_s2_id']}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 2 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())