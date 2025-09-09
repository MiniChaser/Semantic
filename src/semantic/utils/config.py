"""
Application configuration management module
"""

import os
from dataclasses import dataclass, field
from typing import Set
from dotenv import load_dotenv


@dataclass
class AppConfig:
    """Application configuration class"""
    
    # Database configuration (retrieved from connection.py)
    
    # DBLP configuration
    dblp_url: str = "https://dblp.org/xml/dblp.xml.gz"
    download_dir: str = "external"
    compressed_file: str = "external/dblp.xml.gz"
    xml_file: str = "external/dblp.xml"
    
    # Processing configuration
    target_venues: Set[str] = field(default_factory=lambda: {'acl', 'naacl', 'emnlp', 'findings'})
    enable_venue_filter: bool = True
    batch_size: int = 10000
    log_level: str = "INFO"
    
    # Scheduling configuration
    schedule_cron: str = "0 2 * * 1"  # Every Monday at 2 AM
    max_retries: int = 3
    retry_delay: int = 300  # 5 minutes
    
    # Incremental processing configuration
    enable_incremental: bool = True
    incremental_check_days: int = 7  # Check data from last 7 days
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Create configuration from environment variables"""
        load_dotenv()
        
        # Parse target venues
        target_venues_str = os.getenv('TARGET_VENUES', 'acl,naacl,emnlp,findings')
        target_venues = set(venue.strip().lower() for venue in target_venues_str.split(','))
        
        return cls(
            # DBLP configuration
            dblp_url=os.getenv('DBLP_URL', 'https://dblp.org/xml/dblp.xml.gz'),
            download_dir=os.getenv('DOWNLOAD_DIR', 'external'),
            compressed_file=os.getenv('COMPRESSED_FILE', 'external/dblp.xml.gz'),
            xml_file=os.getenv('XML_FILE', 'external/dblp.xml'),
            
            # Processing configuration
            target_venues=target_venues,
            enable_venue_filter=os.getenv('ENABLE_VENUE_FILTER', 'true').lower() == 'true',
            batch_size=int(os.getenv('BATCH_SIZE', '10000')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            
            # Scheduling configuration
            schedule_cron=os.getenv('SCHEDULE_CRON', '0 2 * * 1'),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay=int(os.getenv('RETRY_DELAY', '300')),
            
            # Incremental processing configuration
            enable_incremental=os.getenv('ENABLE_INCREMENTAL', 'true').lower() == 'true',
            incremental_check_days=int(os.getenv('INCREMENTAL_CHECK_DAYS', '7'))
        )
    
    def validate(self) -> bool:
        """Validate configuration"""
        if not self.dblp_url:
            return False
        
        if not self.download_dir or not self.compressed_file or not self.xml_file:
            return False
        
        if self.batch_size <= 0:
            return False
        
        if self.max_retries < 0 or self.retry_delay < 0:
            return False
        
        return True
    
    def __str__(self) -> str:
        """String representation"""
        return f"AppConfig(venues={len(self.target_venues)}, batch_size={self.batch_size}, incremental={self.enable_incremental})"