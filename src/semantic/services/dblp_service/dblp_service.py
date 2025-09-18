"""
DBLP data processing service
Provides DBLP data download, parsing and processing functionality
"""

import os
import gzip
import logging
import requests
import time
import json
from datetime import datetime
from typing import List, Optional, Set, Dict, Any
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
    api_fallback_calls: int = 0
    api_fallback_success: int = 0
    api_fallback_failures: int = 0
    non_english_detected: int = 0

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
        self._last_api_call = 0  # Rate limiting for API calls
    
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

            # Log API fallback statistics if any calls were made
            if self.stats.api_fallback_calls > 0:
                self.logger.info(
                    f"API Fallback Statistics: "
                    f"Non-English detected: {self.stats.non_english_detected}, "
                    f"API calls: {self.stats.api_fallback_calls}, "
                    f"Success: {self.stats.api_fallback_success}, "
                    f"Failures: {self.stats.api_fallback_failures}"
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

            # Initialize title and authors from XML
            title = self._clean_text(title_elem.text)
            cleaned_authors = [self._clean_text(author) for author in authors]

            # Check for non-English characters in title or authors
            has_non_english_title = self._has_non_english_chars(title)
            has_non_english_authors = any(self._has_non_english_chars(author) for author in cleaned_authors)

            if has_non_english_title or has_non_english_authors:
                self.stats.non_english_detected += 1
                self.logger.debug(f"Non-English characters detected in paper: {key}")

                # Try API fallback
                api_data = self._query_dblp_api(key)
                if api_data:
                    api_title, api_authors = self._extract_api_data(api_data)

                    # Use API data to override XML data if available
                    if api_title and has_non_english_title:
                        title = api_title
                        self.logger.debug(f"Title corrected via API for key: {key}")

                    if api_authors and has_non_english_authors:
                        cleaned_authors = api_authors
                        self.logger.debug(f"Authors corrected via API for key: {key}")

            # Build paper record
            paper = DBLP_Paper(
                key=key,
                title=title,
                authors=cleaned_authors,
                author_count=len(cleaned_authors),
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

    def _has_non_english_chars(self, text: str) -> bool:
        """Detect if text contains non-English characters (non-ASCII)"""
        if not text:
            return False

        for char in text:
            # ASCII characters have ord values 0-127
            if ord(char) > 127:
                return True
        return False

    def _query_dblp_api(self, paper_key: str) -> Optional[Dict[str, Any]]:
        """Query DBLP API for paper information by key"""
        if not self.config.enable_dblp_api_fallback:
            return None

        try:
            # Rate limiting
            current_time = time.time()
            time_since_last = current_time - self._last_api_call
            if time_since_last < self.config.dblp_api_rate_limit:
                time.sleep(self.config.dblp_api_rate_limit - time_since_last)

            self._last_api_call = time.time()
            self.stats.api_fallback_calls += 1

            # Build query URL - search by exact key
            params = {
                'q': f'key:{paper_key}',
                'format': 'json',
                'h': '1'  # Only need first result
            }

            retries = 0
            while retries <= self.config.dblp_api_max_retries:
                try:
                    response = requests.get(
                        self.config.dblp_api_base_url,
                        params=params,
                        timeout=self.config.dblp_api_timeout
                    )
                    response.raise_for_status()

                    data = response.json()

                    # Check if we have results
                    if 'result' in data and 'hits' in data['result'] and 'hit' in data['result']['hits']:
                        hits = data['result']['hits']['hit']
                        if hits and len(hits) > 0:
                            paper_info = hits[0].get('info', {})
                            self.stats.api_fallback_success += 1

                            self.logger.debug(f"API fallback successful for key: {paper_key}")
                            return paper_info

                    self.logger.debug(f"API fallback no results for key: {paper_key}")
                    return None

                except requests.RequestException as e:
                    retries += 1
                    self.logger.debug(f"API request failed (attempt {retries}): {e}")
                    if retries <= self.config.dblp_api_max_retries:
                        time.sleep(1 * retries)  # Exponential backoff

            self.stats.api_fallback_failures += 1
            return None

        except Exception as e:
            self.logger.debug(f"API fallback error for key {paper_key}: {e}")
            self.stats.api_fallback_failures += 1
            return None

    def _extract_api_data(self, api_data: Dict[str, Any]) -> tuple[Optional[str], Optional[List[str]]]:
        """Extract title and authors from API response data"""
        try:
            title = None
            authors = []

            # Extract title
            if 'title' in api_data:
                title_data = api_data['title']
                if isinstance(title_data, str):
                    title = title_data.strip()
                elif isinstance(title_data, dict) and 'text' in title_data:
                    title = title_data['text'].strip()

            # Extract authors
            if 'authors' in api_data:
                authors_data = api_data['authors']
                if isinstance(authors_data, dict) and 'author' in authors_data:
                    author_list = authors_data['author']
                    if isinstance(author_list, list):
                        for author in author_list:
                            if isinstance(author, dict) and 'text' in author:
                                authors.append(author['text'].strip())
                            elif isinstance(author, str):
                                authors.append(author.strip())
                    elif isinstance(author_list, dict) and 'text' in author_list:
                        authors.append(author_list['text'].strip())
                    elif isinstance(author_list, str):
                        authors.append(author_list.strip())

            return title, authors if authors else None

        except Exception as e:
            self.logger.debug(f"Error extracting API data: {e}")
            return None, None


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

    def test_non_english_detection(self) -> Dict[str, bool]:
        """Test non-English character detection with various examples"""
        test_cases = {
            "Simple English text": "This is a simple English title",
            "German with umlauts": "Über die Mööglichkeit der Entwicklung",
            "French accents": "Les données à traiter",
            "Chinese characters": "机器学习与深度学习",
            "Japanese text": "自然言語処理",
            "Korean text": "한국어 자연어 처리",
            "Arabic text": "معالجة اللغة الطبيعية",
            "Mixed English-German": "Machine Learning für Künstliche Intelligenz",
            "Empty string": "",
            "Numbers and symbols": "123 ABC !@# $%^"
        }

        results = {}
        for description, text in test_cases.items():
            has_non_english = self.parser._has_non_english_chars(text)
            results[description] = has_non_english
            self.logger.info(f"Test '{description}': {text} -> {has_non_english}")

        return results