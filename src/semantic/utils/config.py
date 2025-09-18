"""
Application configuration management module
"""

import os
from dataclasses import dataclass, field
from typing import Set, Optional
from dotenv import load_dotenv


@dataclass
class AppConfig:
    """Application configuration class"""
    
    # Database configuration (retrieved from connection.py)
    
    # DBLP configuration
    dblp_url: str = "https://dblp.org/xml/dblp.xml.gz"
    dblp_dtd_url: str = "https://dblp.org/xml/dblp.dtd"
    download_dir: str = "external"
    compressed_file: str = "external/dblp.xml.gz"
    xml_file: str = "external/dblp.xml"
    dtd_file: str = "external/dblp.dtd"
    
    # Processing configuration
    target_venues: Set[str] = field(default_factory=lambda: {'acl', 'naacl', 'emnlp', 'findings'})
    enable_venue_filter: bool = True
    batch_size: int = 10000
    log_level: str = "INFO"
    
    # Scheduling configuration
    schedule_cron: str = "0 2 * * 1"  # Every Monday at 2 AM
    comprehensive_schedule_cron: str = "0 2 */7 * *"  # Every 7 days at 2 AM
    max_retries: int = 3
    retry_delay: int = 300  # 5 minutes
    
    # Incremental processing configuration
    enable_incremental: bool = True
    incremental_check_days: int = 7  # Check data from last 7 days
    
    # Semantic Scholar API configuration
    semantic_scholar_api_key: Optional[str] = None

    # DBLP API fallback configuration
    enable_dblp_api_fallback: bool = True
    dblp_api_base_url: str = "https://dblp.org/search/publ/api"
    dblp_api_rate_limit: float = 0.1  # Seconds between API calls
    dblp_api_timeout: int = 10  # API request timeout in seconds
    dblp_api_max_retries: int = 2  # Max retries for API calls
    
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
            dblp_dtd_url=os.getenv('DBLP_DTD_URL', 'https://dblp.org/xml/dblp.dtd'),
            download_dir=os.getenv('DOWNLOAD_DIR', 'external'),
            compressed_file=os.getenv('COMPRESSED_FILE', 'external/dblp.xml.gz'),
            xml_file=os.getenv('XML_FILE', 'external/dblp.xml'),
            dtd_file=os.getenv('DTD_FILE', 'external/dblp.dtd'),
            
            # Processing configuration
            target_venues=target_venues,
            enable_venue_filter=os.getenv('ENABLE_VENUE_FILTER', 'true').lower() == 'true',
            batch_size=int(os.getenv('BATCH_SIZE', '10000')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            
            # Scheduling configuration
            schedule_cron=os.getenv('SCHEDULE_CRON', '0 2 * * 1'),
            comprehensive_schedule_cron=os.getenv('COMPREHENSIVE_SCHEDULE_CRON', '0 2 */7 * *'),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay=int(os.getenv('RETRY_DELAY', '300')),
            
            # Incremental processing configuration
            enable_incremental=os.getenv('ENABLE_INCREMENTAL', 'true').lower() == 'true',
            incremental_check_days=int(os.getenv('INCREMENTAL_CHECK_DAYS', '7')),
            
            # Semantic Scholar API configuration
            semantic_scholar_api_key=os.getenv('SEMANTIC_SCHOLAR_API_KEY'),

            # DBLP API fallback configuration
            enable_dblp_api_fallback=os.getenv('ENABLE_DBLP_API_FALLBACK', 'true').lower() == 'true',
            dblp_api_base_url=os.getenv('DBLP_API_BASE_URL', 'https://dblp.org/search/publ/api'),
            dblp_api_rate_limit=float(os.getenv('DBLP_API_RATE_LIMIT', '0.1')),
            dblp_api_timeout=int(os.getenv('DBLP_API_TIMEOUT', '10')),
            dblp_api_max_retries=int(os.getenv('DBLP_API_MAX_RETRIES', '2'))
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

        # Validate DBLP API fallback configuration
        if self.enable_dblp_api_fallback:
            if not self.dblp_api_base_url:
                return False
            if self.dblp_api_rate_limit < 0:
                return False
            if self.dblp_api_timeout <= 0:
                return False
            if self.dblp_api_max_retries < 0:
                return False

        return True
    
    def __str__(self) -> str:
        """String representation"""
        return f"AppConfig(venues={len(self.target_venues)}, batch_size={self.batch_size}, incremental={self.enable_incremental})"