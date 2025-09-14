"""
DBLP data processing service
Provides DBLP data download, parsing and processing functionality
"""

import os
import gzip
import logging
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass
from tqdm import tqdm
from lxml import etree
from ...database.models import DBLP_Paper
from ...utils.config import AppConfig


@dataclass
class DBLPProcessingStats:
    """DBLP processing statistics"""
    total_papers: int = 0
    filtered_papers: int = 0
    errors: int = 0
    venues_found: Set[str] = None
    
    def __post_init__(self):
        if self.venues_found is None:
            self.venues_found = set()


class DBLPDownloader:
    """DBLP data downloader"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.DBLPDownloader')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def download_dblp_data(self, force_download: bool = False) -> bool:
        """Download DBLP XML.gz file"""
        try:
            # Create download directory
            os.makedirs(self.config.download_dir, exist_ok=True)
            
            # Check if file already exists and not forcing download
            if os.path.exists(self.config.compressed_file) and not force_download:
                self.logger.info(f"File already exists: {self.config.compressed_file}")
                
                # Check file size, if too small may be incomplete download
                file_size = os.path.getsize(self.config.compressed_file)
                if file_size < 100 * 1024 * 1024:  # Less than 100MB
                    self.logger.warning("File size abnormal, re-downloading...")
                    os.remove(self.config.compressed_file)
                else:
                    return True
            
            self.logger.info(f"Starting DBLP data download: {self.config.dblp_url}")
            
            # Initiate download request
            response = requests.get(self.config.dblp_url, stream=True)
            response.raise_for_status()
            
            # Get file size
            total_size = int(response.headers.get('content-length', 0))
            
            # Download file and show progress
            with open(self.config.compressed_file, 'wb') as f:
                with tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    desc="Downloading DBLP data"
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            self.logger.info(f"Download completed: {self.config.compressed_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Download failed: {e}")
            return False
    
    def extract_xml(self, force_extract: bool = False) -> bool:
        """Extract XML.gz file"""
        try:
            if not os.path.exists(self.config.compressed_file):
                self.logger.error("Compressed file does not exist, please download first")
                return False
            
            # Check if XML file already exists and not forcing extraction
            if os.path.exists(self.config.xml_file) and not force_extract:
                self.logger.info(f"XML file already exists: {self.config.xml_file}")
                return True
            
            self.logger.info("Starting XML file extraction...")
            
            # Get compressed file size for progress display
            compressed_size = os.path.getsize(self.config.compressed_file)
            
            with gzip.open(self.config.compressed_file, 'rb') as f_in:
                with open(self.config.xml_file, 'wb') as f_out:
                    with tqdm(
                        total=compressed_size,
                        unit='B',
                        unit_scale=True,
                        desc="Extracting XML file"
                    ) as pbar:
                        while True:
                            chunk = f_in.read(8192)
                            if not chunk:
                                break
                            f_out.write(chunk)
                            pbar.update(len(chunk))
            
            xml_size = os.path.getsize(self.config.xml_file)
            self.logger.info(f"Extraction completed, XML file size: {xml_size / 1024 / 1024 / 1024:.2f} GB")
            return True
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            return False
    
    def cleanup_files(self, keep_xml: bool = True):
        """Cleanup downloaded files"""
        try:
            if os.path.exists(self.config.compressed_file):
                os.remove(self.config.compressed_file)
                self.logger.info("Deleted compressed file")
            
            if not keep_xml and os.path.exists(self.config.xml_file):
                os.remove(self.config.xml_file)
                self.logger.info("Deleted XML file")
                
        except Exception as e:
            self.logger.error(f"File cleanup failed: {e}")


class DBLPParser:
    """DBLP XML parser"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = self._setup_logger()
        self.stats = DBLPProcessingStats()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.DBLPParser')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def parse_xml(self, incremental: bool = False, 
                  existing_keys: Set[str] = None) -> List[DBLP_Paper]:
        """Parse DBLP XML file"""
        if not os.path.exists(self.config.xml_file):
            self.logger.error("XML file does not exist, please download and extract first")
            return []
        
        self.logger.info("Starting DBLP XML parsing...")
        papers = []
        batch_papers = []
        existing_keys = existing_keys or set()
        
        # Get file size for progress display
        xml_size = os.path.getsize(self.config.xml_file)
        
        try:
            with open(self.config.xml_file, 'rb') as f:
                with tqdm(
                    total=xml_size,
                    unit='B',
                    unit_scale=True,
                    desc="Parsing XML"
                ) as pbar:
                    
                    # Create incremental parser
                    context = etree.iterparse(
                        f,
                        events=('end',),
                        tag='inproceedings',
                        dtd_validation=False,
                        load_dtd=True,
                        resolve_entities=False,
                        encoding='ISO-8859-1'
                    )
                    
                    last_position = 0
                    for event, paper_elem in context:
                        try:
                            paper_data = self._extract_paper_data(paper_elem)
                            if paper_data:
                                # Incremental processing: skip existing papers
                                if incremental and paper_data.key in existing_keys:
                                    continue
                                
                                batch_papers.append(paper_data)
                                self.stats.filtered_papers += 1
                                
                                # Batch processing
                                if len(batch_papers) >= self.config.batch_size:
                                    papers.extend(batch_papers)
                                    batch_papers = []
                            
                            self.stats.total_papers += 1
                            
                        except Exception as e:
                            self.logger.debug(f"Error parsing paper: {e}")
                            self.stats.errors += 1
                        
                        finally:
                            # Clean up memory
                            paper_elem.clear()
                            while paper_elem.getprevious() is not None:
                                del paper_elem.getparent()[0]
                            
                            # Update progress (estimated)
                            current_position = f.tell()
                            if current_position > last_position:
                                pbar.update(current_position - last_position)
                                last_position = current_position
                    
                    # Process remaining papers
                    if batch_papers:
                        papers.extend(batch_papers)
            
            self.logger.info(
                f"Parsing completed: Total papers {self.stats.total_papers}, "
                f"Filtered {self.stats.filtered_papers}, "
                f"Errors {self.stats.errors}"
            )
            
            return papers
            
        except Exception as e:
            self.logger.error(f"XML parsing failed: {e}")
            return []
    
    def _extract_paper_data(self, paper_elem) -> Optional[DBLP_Paper]:
        """Extract single paper data"""
        try:
            # Get venue information
            key = paper_elem.attrib.get('key', '')
            if not key:
                return None
            
            # Parse venue name
            key_parts = key.split('/')
            if len(key_parts) < 2:
                return None
            
            venue_type = key_parts[0]  # Usually 'conf'
            venue_name = key_parts[1].lower()
            
            # Decide whether to filter venues based on configuration
            if self.config.enable_venue_filter and venue_name not in self.config.target_venues:
                return None
            
            # Extract basic information
            title_elem = paper_elem.find("title")
            if title_elem is None or not title_elem.text:
                return None
            
            authors = [author.text for author in paper_elem.findall("author") if author.text]
            if not authors:
                return None
            
            # Build paper record
            paper = DBLP_Paper(
                key=key,
                title=self._clean_text(title_elem.text),
                authors=authors,
                author_count=len(authors),
                venue=venue_name,
                year=self._extract_text(paper_elem.find("year")),
                pages=self._extract_text(paper_elem.find("pages")),
                ee=self._extract_text(paper_elem.find("ee")),
                booktitle=self._extract_text(paper_elem.find("booktitle")),
                doi=self._extract_doi(paper_elem.find("ee")),
                create_time=datetime.now().isoformat()
            )
            
            # Record found venue
            self.stats.venues_found.add(venue_name)
            
            return paper
            
        except Exception as e:
            self.logger.debug(f"Error extracting paper data: {e}")
            return None
    
    def _extract_text(self, element) -> Optional[str]:
        """Safely extract element text"""
        if element is not None and element.text:
            return self._clean_text(element.text)
        return None
    
    def _clean_text(self, text: str) -> str:
        """Clean text data"""
        if not text:
            return ""
        return text.strip().replace('\n', ' ').replace('\r', '')
    
    def _extract_doi(self, ee_element) -> Optional[str]:
        """Extract DOI from ee element"""
        if ee_element is None or not ee_element.text:
            return None
        
        ee_text = ee_element.text.lower()
        if 'doi.org' in ee_text:
            # Extract DOI
            parts = ee_text.split('/')
            if len(parts) >= 2:
                return '/'.join(parts[-2:])  # Get last two parts as DOI
        return None
    
    def get_stats(self) -> DBLPProcessingStats:
        """Get processing statistics"""
        return self.stats
    
    def reset_stats(self):
        """Reset statistics"""
        self.stats = DBLPProcessingStats()


class DBLPService:
    """DBLP data processing service"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = self._setup_logger()
        self.downloader = DBLPDownloader(config)
        self.parser = DBLPParser(config)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.DBLPService')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        if not logger.handlers:
            # Create log directory
            os.makedirs('logs', exist_ok=True)
            
            # File handler
            file_handler = logging.FileHandler(
                f'logs/dblp_service_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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
    
    def prepare_data(self, force_download: bool = False, force_extract: bool = False) -> bool:
        """Prepare DBLP data (download and extract)"""
        self.logger.info("Starting DBLP data preparation...")
        
        # Step 1: Download data
        if not self.downloader.download_dblp_data(force_download):
            self.logger.error("Data download failed")
            return False
        
        # Step 2: Extract data
        if not self.downloader.extract_xml(force_extract):
            self.logger.error("Data extraction failed")
            return False
        
        self.logger.info("DBLP data preparation completed")
        return True
    
    def parse_papers(self, incremental: bool = False, 
                    existing_keys: Set[str] = None) -> List[DBLP_Paper]:
        """Parse paper data"""
        self.logger.info(f"Starting paper data parsing (incremental mode: {incremental})...")
        
        papers = self.parser.parse_xml(incremental, existing_keys)
        stats = self.parser.get_stats()
        
        self.logger.info(f"Paper parsing completed: {len(papers)} papers")
        self.logger.info(f"Statistics: {stats}")
        
        return papers
    
    def cleanup(self, keep_xml: bool = True):
        """Cleanup downloaded files"""
        self.downloader.cleanup_files(keep_xml)
    
    def get_processing_stats(self) -> DBLPProcessingStats:
        """Get processing statistics"""
        return self.parser.get_stats()
    
    def reset_stats(self):
        """Reset statistics"""
        self.parser.reset_stats()