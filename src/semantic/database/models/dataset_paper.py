"""
Dataset Paper data model
Stores papers from S2 dataset filtered by conference
"""

import json
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict


@dataclass
class DatasetPaper:
    """Dataset Paper data model"""
    corpus_id: int
    title: str
    venue: str
    release_id: str
    paper_id: Optional[str] = None
    external_ids: Optional[Dict] = None
    abstract: Optional[str] = None
    year: Optional[int] = None
    citation_count: int = 0
    reference_count: int = 0
    influential_citation_count: int = 0
    authors: Optional[List[Dict]] = None
    fields_of_study: Optional[List] = None
    publication_types: Optional[List] = None
    is_open_access: bool = False
    open_access_pdf: Optional[str] = None
    conference_normalized: Optional[str] = None
    source_file: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'DatasetPaper':
        """Create DatasetPaper object from dictionary"""
        # Handle JSON fields that might be strings or already parsed
        def parse_json_field(field_name):
            value = data.get(field_name)
            if value is None:
                return None
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return None
            return value

        return cls(
            id=data.get('id'),
            corpus_id=data.get('corpus_id'),
            paper_id=data.get('paper_id'),
            external_ids=parse_json_field('external_ids'),
            title=data.get('title', ''),
            abstract=data.get('abstract'),
            venue=data.get('venue', ''),
            year=data.get('year'),
            citation_count=data.get('citation_count', 0),
            reference_count=data.get('reference_count', 0),
            influential_citation_count=data.get('influential_citation_count', 0),
            authors=parse_json_field('authors'),
            fields_of_study=parse_json_field('fields_of_study'),
            publication_types=parse_json_field('publication_types'),
            is_open_access=data.get('is_open_access', False),
            open_access_pdf=data.get('open_access_pdf'),
            conference_normalized=data.get('conference_normalized'),
            source_file=data.get('source_file'),
            release_id=data.get('release_id', ''),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )

    @classmethod
    def from_s2_json(cls, json_obj: Dict, conference: str, source_file: str, release_id: str) -> 'DatasetPaper':
        """Create DatasetPaper object from S2 JSON format"""
        return cls(
            corpus_id=json_obj.get('corpusId'),
            paper_id=json_obj.get('paperId'),
            external_ids=json_obj.get('externalIds', {}),
            title=json_obj.get('title', ''),
            abstract=json_obj.get('abstract'),
            venue=json_obj.get('venue', ''),
            year=json_obj.get('year'),
            citation_count=json_obj.get('citationCount', 0),
            reference_count=json_obj.get('referenceCount', 0),
            influential_citation_count=json_obj.get('influentialCitationCount', 0),
            authors=json_obj.get('authors', []),
            fields_of_study=json_obj.get('fieldsOfStudy', []),
            publication_types=json_obj.get('publicationTypes', []),
            is_open_access=json_obj.get('isOpenAccess', False),
            open_access_pdf=json_obj.get('openAccessPdf', {}).get('url') if json_obj.get('openAccessPdf') else None,
            conference_normalized=conference,
            source_file=source_file,
            release_id=release_id
        )
