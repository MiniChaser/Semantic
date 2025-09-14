#!/usr/bin/env python3
"""
Author Processing Step 3: Calculate Advanced Metrics
Creates metrics tables and calculates collaboration, rising star, and comprehensive metrics
"""

import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager
from semantic.services.author_service.author_metrics_service import AuthorMetricsService


def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / 'step3_calculate_metrics.log')
        ]
    )


def main():
    """Execute Step 3: Calculate Advanced Metrics"""
    
    print("📈 Step 3: Calculating Advanced Metrics")
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
        metrics_service = AuthorMetricsService(db_manager)
        
        # Create metrics tables
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
        
        print("✅ All metrics calculation completed!")
        
        return 0
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
        logging.getLogger(__name__).error(f"Step 3 failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())