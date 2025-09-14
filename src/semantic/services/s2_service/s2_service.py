"""
Semantic Scholar API Service
Handles API integration, data enrichment, and validation
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
import requests
from difflib import SequenceMatcher

from ...utils.config import AppConfig


class SemanticScholarAPI:
    """Semantic Scholar API client with rate limiting and retry logic"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.base_url = "https://api.semanticscholar.org/graph/v1"
        
        # Rate limiting based on API key availability
        if api_key:
            self.requests_per_second = 100
            self.requests_per_period = 100
            self.period_seconds = 1
        else:
            self.requests_per_second = 100
            self.requests_per_period = 100
            self.period_seconds = 300  # 5 minutes
        
        self.last_request_time = 0
        self.request_count = 0
        self.period_start = time.time()
        
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.SemanticScholarAPI')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers"""
        headers = {'Content-Type': 'application/json'}
        if self.api_key:
            headers['x-api-key'] = self.api_key
        return headers
    
    def _rate_limit(self):
        """Apply rate limiting"""
        current_time = time.time()
        
        # Reset period if needed
        if current_time - self.period_start >= self.period_seconds:
            self.period_start = current_time
            self.request_count = 0
        
        # Check if we've exceeded period limit
        if self.request_count >= self.requests_per_period:
            sleep_time = self.period_seconds - (current_time - self.period_start)
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                self.period_start = time.time()
                self.request_count = 0
        
        # Ensure minimum time between requests
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.requests_per_second
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _make_request(self, url: str, params: Dict = None, json_data: Dict = None, 
                     max_retries: int = 3) -> Optional[Dict]:
        """Make API request with retry logic"""
        for attempt in range(max_retries + 1):
            try:
                self._rate_limit()
                
                if json_data:
                    response = requests.post(
                        url, 
                        headers=self._get_headers(),
                        params=params,
                        json=json_data,
                        timeout=30
                    )
                else:
                    response = requests.get(
                        url,
                        headers=self._get_headers(),
                        params=params,
                        timeout=30
                    )
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    self.logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    self.logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Request failed after {max_retries + 1} attempts: {e}")
                    return None
    
    def get_paper_fields(self) -> List[str]:
        """Get complete list of fields to request from S2 API"""
        return [
            'paperId', 'corpusId', 'externalIds', 'title', 'abstract',
            'year', 'venue', 'authors', 'citationCount', 'referenceCount',
            'fieldsOfStudy', 'influentialCitationCount', 'openAccessPdf',
            'citationStyles', 'url', 'publicationVenue', 'publicationTypes',
            's2FieldsOfStudy', 'publicationDate'
        ]
    
    def batch_get_papers(self, paper_ids: List[str]) -> List[Optional[Dict]]:
        """Batch retrieve papers by IDs"""
        if not paper_ids:
            return []
        
        # S2 API supports up to 500 IDs per batch
        batch_size = 500
        all_results = []
        
        for i in range(0, len(paper_ids), batch_size):
            batch_ids = paper_ids[i:i + batch_size]
            
            url = f"{self.base_url}/paper/batch"
            params = {'fields': ','.join(self.get_paper_fields())}
            json_data = {"ids": batch_ids}
            
            self.logger.info(f"Requesting batch {i//batch_size + 1}: {len(batch_ids)} papers")
            result = self._make_request(url, params=params, json_data=json_data)
            
            if result:
                all_results.extend(result)
            else:
                # Add None for each failed paper in batch
                all_results.extend([None] * len(batch_ids))
        
        return all_results
    
    def search_paper_by_title(self, title: str, limit: int = 1) -> Optional[Dict]:
        """Search for paper by title"""
        url = f"{self.base_url}/paper/search"
        params = {
            'query': title,
            'limit': limit,
            'fields': ','.join(self.get_paper_fields())
        }
        
        result = self._make_request(url, params=params)
        
        if result and result.get('data') and len(result['data']) > 0:
            return result['data'][0]
        return None


class S2DataParser:
    """Parser for Semantic Scholar API responses"""
    
    @staticmethod
    def parse_s2_response(paper_data: Dict) -> Dict[str, Any]:
        """Parse S2 API response into structured data"""
        if not paper_data or not isinstance(paper_data, dict):
            return {}
        
        # Direct field mappings
        parsed_data = {
            'semantic_paper_id': paper_data.get('paperId'),
            'semantic_title': paper_data.get('title'),
            'semantic_year': paper_data.get('year'),
            'semantic_venue': paper_data.get('venue'),
            'semantic_abstract': paper_data.get('abstract'),
            'semantic_url': paper_data.get('url'),
            'semantic_citation_count': paper_data.get('citationCount'),
            'semantic_reference_count': paper_data.get('referenceCount'),
            'influentialCitationCount': paper_data.get('influentialCitationCount'),
            'semantic_full_data': json.dumps(paper_data, ensure_ascii=False)
        }
        
        # Parse external IDs
        external_ids = paper_data.get('externalIds', {}) or {}
        parsed_data.update({
            'semantic_external_ids': json.dumps(external_ids, ensure_ascii=False),
            'doi': external_ids.get('DOI'),
            'arxiv_id': external_ids.get('ArXiv'),
            'mag_id': external_ids.get('MAG'),
            'acl_id': external_ids.get('ACL'),
            'corpus_id': external_ids.get('CorpusId'),
            'pmid': external_ids.get('PubMed')
        })
        
        # Parse authors
        authors = paper_data.get('authors', []) or []
        if authors:
            parsed_data.update({
                'semantic_authors': json.dumps(authors, ensure_ascii=False),
                'all_authors_count': len(authors),
                'all_author_names': ';'.join([author.get('name', '') for author in authors if author.get('name')]),
                'all_author_ids': ';'.join([str(author.get('authorId', '')) for author in authors if author.get('authorId')]),
                'first_author_semantic_id': str(authors[0].get('authorId', '')) if authors and authors[0].get('authorId') else None
            })
        
        # Parse fields of study
        fields_of_study = paper_data.get('fieldsOfStudy', []) or []
        if fields_of_study:
            parsed_data.update({
                'semantic_fields_of_study': json.dumps(fields_of_study, ensure_ascii=False),
                's2_fields_primary': fields_of_study[0] if len(fields_of_study) > 0 else None,
                's2_fields_secondary': fields_of_study[1] if len(fields_of_study) > 1 else None,
                's2_fields_all': ';'.join(fields_of_study)
            })
        
        # Parse open access info
        open_access = paper_data.get('openAccessPdf', {}) or {}
        if open_access:
            parsed_data.update({
                'open_access_url': open_access.get('url'),
                'open_access_status': open_access.get('status'),
                'open_access_license': open_access.get('license'),
                'pdf_available': 'TRUE' if open_access.get('url') else 'FALSE'
            })
        else:
            parsed_data['pdf_available'] = 'FALSE'
        
        # Parse citation styles
        citation_styles = paper_data.get('citationStyles', {}) or {}
        if citation_styles:
            parsed_data['bibtex_citation'] = citation_styles.get('bibtex')
        
        # Parse venue info
        pub_venue = paper_data.get('publicationVenue', {}) or {}
        if pub_venue:
            alternate_names = pub_venue.get('alternate_names', []) or []
            parsed_data['venue_alternate_names'] = ';'.join(alternate_names) if alternate_names else None
        
        return parsed_data
    
    @staticmethod
    def parse_dblp_author_fields(dblp_authors_str: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse DBLP authors field to extract first/last author info"""
        try:
            if not dblp_authors_str or dblp_authors_str.strip() == '':
                return None, None, None
            
            # Try to parse as JSON first
            if dblp_authors_str.startswith('['):
                authors_list = json.loads(dblp_authors_str)
            else:
                # Split by common delimiters
                authors_list = [author.strip() for author in dblp_authors_str.split(';') if author.strip()]
                if len(authors_list) == 1:
                    authors_list = [author.strip() for author in dblp_authors_str.split('|') if author.strip()]
            
            if not authors_list:
                return None, None, None
            
            first_author = authors_list[0] if len(authors_list) > 0 else None
            last_author = authors_list[-1] if len(authors_list) > 0 else None
            first_author_dblp_id = first_author  # In DBLP, name often serves as ID
            
            return first_author, last_author, first_author_dblp_id
            
        except Exception as e:
            logging.getLogger(__name__).debug(f"Failed to parse DBLP authors: {e}")
            return None, None, None


class S2ValidationService:
    """Service for validating and scoring S2 matches"""
    
    def __init__(self):
        self.logger = logging.getLogger(f'{__name__}.S2ValidationService')
    
    def calculate_title_similarity(self, title1: str, title2: str) -> float:
        """Calculate similarity between two titles"""
        if not title1 or not title2:
            return 0.0
        
        # Clean titles by removing non-alphanumeric characters and converting to lowercase
        clean1 = ''.join(char.lower() for char in title1 if char.isalnum() or char.isspace())
        clean2 = ''.join(char.lower() for char in title2 if char.isalnum() or char.isspace())
        
        return SequenceMatcher(None, clean1, clean2).ratio()
    
    
    def determine_validation_tier(self, match_method: str, confidence: float) -> str:
        """Determine validation tier based on match method and confidence"""
        if match_method.startswith('Title Match'):
            if confidence >= 0.85:
                return 'Tier2_TitleMatch_High'
            elif confidence >= 0.70:
                return 'Tier2_TitleMatch_Medium'
            else:
                return 'Tier3_NoMatch'
        else:
            return 'Tier3_NoMatch'
    
    def calculate_completeness_score(self, paper_data: Dict) -> float:
        """Calculate data completeness score"""
        required_fields = [
            'title', 'year', 'semantic_abstract', 'semantic_citation_count',
            'semantic_authors', 'semantic_paper_id', 'doi'
        ]
        
        filled_fields = 0
        for field in required_fields:
            value = paper_data.get(field)
            if value is not None and str(value).strip():
                filled_fields += 1
        
        return filled_fields / len(required_fields)