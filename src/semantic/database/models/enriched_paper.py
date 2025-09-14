"""
Enriched Paper data model with S2 integration
"""

import json
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict


@dataclass
class EnrichedPaper:
    """Enriched Paper data model with DBLP and S2 data"""
    
    # Reference fields
    id: Optional[int] = None
    dblp_paper_id: Optional[int] = None
    
    # DBLP fields (12 fields)
    dblp_id: Optional[int] = None
    dblp_key: Optional[str] = None
    dblp_title: Optional[str] = None
    dblp_authors: Optional[List[str]] = None
    dblp_year: Optional[str] = None
    dblp_pages: Optional[str] = None
    dblp_url: Optional[str] = None
    dblp_venue: Optional[str] = None
    dblp_created_at: Optional[datetime] = None
    dblp_first_author: Optional[str] = None
    dblp_last_author: Optional[str] = None
    first_author_dblp_id: Optional[str] = None
    
    # Semantic Scholar basic fields (9 fields)
    semantic_id: Optional[int] = None
    semantic_paper_id: Optional[str] = None
    semantic_title: Optional[str] = None
    semantic_year: Optional[int] = None
    semantic_venue: Optional[str] = None
    semantic_abstract: Optional[str] = None
    semantic_url: Optional[str] = None
    semantic_created_at: Optional[datetime] = None
    semantic_updated_at: Optional[datetime] = None
    
    # Semantic Scholar statistics (3 fields)
    semantic_citation_count: Optional[int] = None
    semantic_reference_count: Optional[int] = None
    influentialCitationCount: Optional[int] = None
    
    # Author information (5 fields)
    semantic_authors: Optional[List[Dict]] = None
    first_author_semantic_id: Optional[str] = None
    all_authors_count: Optional[int] = None
    all_author_names: Optional[str] = None
    all_author_ids: Optional[str] = None
    
    # Research fields (4 fields)
    semantic_fields_of_study: Optional[List[str]] = None
    s2_fields_primary: Optional[str] = None
    s2_fields_secondary: Optional[str] = None
    s2_fields_all: Optional[str] = None
    
    # External identifiers (7 fields)
    semantic_external_ids: Optional[Dict] = None
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    mag_id: Optional[str] = None
    acl_id: Optional[str] = None
    corpus_id: Optional[str] = None
    pmid: Optional[str] = None
    
    # Open access information (4 fields)
    open_access_url: Optional[str] = None
    open_access_status: Optional[str] = None
    open_access_license: Optional[str] = None
    pdf_available: Optional[str] = None
    
    # Additional metadata (3 fields)
    bibtex_citation: Optional[str] = None
    semantic_full_data: Optional[Dict] = None
    venue_alternate_names: Optional[str] = None
    
    # Future fields for completeness (5 fields)
    author_affiliations: Optional[Dict] = None
    author_contacts: Optional[Dict] = None
    corresponding_authors: Optional[str] = None
    pdf_filename: Optional[str] = None
    pdf_file_path: Optional[str] = None
    
    # Validation and processing metadata (5 fields)
    match_method: Optional[str] = None
    validation_tier: Optional[str] = None
    match_confidence: Optional[float] = None
    data_source_primary: Optional[str] = None
    data_completeness_score: Optional[float] = None
    
    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, handling JSON fields properly"""
        data = asdict(self)
        
        # Convert datetime objects to ISO strings
        for field in ['dblp_created_at', 'semantic_created_at', 'semantic_updated_at', 'created_at', 'updated_at']:
            if data.get(field) and isinstance(data[field], datetime):
                data[field] = data[field].isoformat()
        
        # Keep JSON fields as Python objects for JSONB columns
        # PostgreSQL JSONB columns handle the JSON conversion automatically
        # No need to manually convert to JSON strings
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EnrichedPaper':
        """Create EnrichedPaper object from dictionary"""
        # Handle JSON fields
        json_fields = {
            'dblp_authors': list,
            'semantic_authors': list,
            'semantic_fields_of_study': list,
            'semantic_external_ids': dict,
            'semantic_full_data': dict,
            'author_affiliations': dict,
            'author_contacts': dict
        }
        
        processed_data = data.copy()
        
        for field, expected_type in json_fields.items():
            value = processed_data.get(field)
            if value is not None:
                if isinstance(value, str):
                    try:
                        processed_data[field] = json.loads(value)
                    except (json.JSONDecodeError, ValueError):
                        processed_data[field] = expected_type()
                elif not isinstance(value, expected_type):
                    processed_data[field] = expected_type()
        
        # Handle datetime fields
        datetime_fields = ['dblp_created_at', 'semantic_created_at', 'semantic_updated_at', 'created_at', 'updated_at']
        for field in datetime_fields:
            value = processed_data.get(field)
            if value is not None and isinstance(value, str):
                try:
                    processed_data[field] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    processed_data[field] = None
        
        return cls(**{k: v for k, v in processed_data.items() if k in cls.__annotations__})
    
    def merge_dblp_data(self, dblp_paper: 'DBLP_Paper'):
        """Merge data from DBLP paper object"""
        # 确保我们有正确的DBLP paper ID
        if dblp_paper.id is not None:
            self.dblp_paper_id = dblp_paper.id
            self.dblp_id = dblp_paper.id
        else:
            # 如果没有ID，我们需要从数据库获取
            self.dblp_paper_id = None
            self.dblp_id = None
        self.dblp_key = dblp_paper.key
        self.dblp_title = dblp_paper.title
        self.dblp_authors = dblp_paper.authors
        self.dblp_year = dblp_paper.year
        self.dblp_pages = dblp_paper.pages
        self.dblp_url = dblp_paper.ee
        self.dblp_venue = dblp_paper.venue
        # Handle DBLP created_at time
        if dblp_paper.create_time:
            if isinstance(dblp_paper.create_time, str):
                try:
                    self.dblp_created_at = datetime.fromisoformat(dblp_paper.create_time.replace('Z', '+00:00'))
                except ValueError:
                    self.dblp_created_at = None
            elif isinstance(dblp_paper.create_time, datetime):
                self.dblp_created_at = dblp_paper.create_time
            else:
                self.dblp_created_at = None
        else:
            self.dblp_created_at = None
        
        # Parse additional DBLP author fields
        from ...services.s2_service.s2_service import S2DataParser
        first_author, last_author, first_author_id = S2DataParser.parse_dblp_author_fields(
            json.dumps(dblp_paper.authors) if isinstance(dblp_paper.authors, list) else str(dblp_paper.authors or '')
        )
        self.dblp_first_author = first_author
        self.dblp_last_author = last_author
        self.first_author_dblp_id = first_author_id
    
    def merge_s2_data(self, s2_parsed_data: Dict[str, Any]):
        """Merge parsed S2 data"""
        for field, value in s2_parsed_data.items():
            if hasattr(self, field) and value is not None:
                setattr(self, field, value)
    
    def get_primary_identifier(self) -> Optional[str]:
        """Get primary identifier for S2 API lookup"""
        if self.acl_id:
            return f"ACL:{self.acl_id}"
        elif self.doi:
            return f"DOI:{self.doi}"
        return None
    
    def get_search_title(self) -> Optional[str]:
        """Get title for S2 search"""
        return self.semantic_title or self.dblp_title
    
    def is_s2_enriched(self) -> bool:
        """Check if paper has been enriched with S2 data"""
        return bool(self.semantic_paper_id)
    
    def calculate_enrichment_coverage(self) -> float:
        """Calculate percentage of S2 fields that are populated"""
        s2_fields = [
            'semantic_paper_id', 'semantic_title', 'semantic_year', 'semantic_venue',
            'semantic_abstract', 'semantic_url', 'semantic_citation_count',
            'semantic_reference_count', 'influentialCitationCount', 'semantic_authors',
            'first_author_semantic_id', 'all_authors_count', 'all_author_names',
            'all_author_ids', 'semantic_fields_of_study', 's2_fields_primary',
            's2_fields_secondary', 's2_fields_all', 'semantic_external_ids',
            'doi', 'arxiv_id', 'mag_id', 'acl_id', 'corpus_id', 'pmid',
            'open_access_url', 'open_access_status', 'open_access_license',
            'pdf_available', 'bibtex_citation', 'semantic_full_data',
            'venue_alternate_names'
        ]
        
        populated_fields = 0
        for field in s2_fields:
            value = getattr(self, field, None)
            if value is not None and str(value).strip():
                populated_fields += 1
        
        return populated_fields / len(s2_fields) if s2_fields else 0.0
    
    def get_quality_tier_summary(self) -> str:
        """Get human-readable summary of validation tier"""
        tier_descriptions = {
            'Tier2_TitleMatch_High': 'High Quality (High Title Similarity)',
            'Tier2_TitleMatch_Medium': 'Medium Quality (Medium Title Similarity)', 
            'Tier3_NoMatch': 'DBLP Only (No S2 Match)'
        }
        return tier_descriptions.get(self.validation_tier, 'Unknown')