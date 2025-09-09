"""
Paper data model
"""

import json
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict


@dataclass
class Paper:
    """Paper data model"""
    key: str
    title: str
    authors: List[str]
    author_count: int
    venue: str
    year: Optional[str] = None
    pages: Optional[str] = None
    ee: Optional[str] = None
    booktitle: Optional[str] = None
    doi: Optional[str] = None
    create_time: Optional[str] = None
    update_time: Optional[str] = None
    id: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Paper':
        """Create Paper object from dictionary"""
        # Handle authors field (could be JSON string or list)
        authors = data.get('authors', [])
        if isinstance(authors, str):
            try:
                authors = json.loads(authors)
            except json.JSONDecodeError:
                authors = authors.split('|') if authors else []
        
        return cls(
            id=data.get('id'),
            key=data.get('key', ''),
            title=data.get('title', ''),
            authors=authors,
            author_count=data.get('author_count', len(authors)),
            venue=data.get('venue', ''),
            year=data.get('year'),
            pages=data.get('pages'),
            ee=data.get('ee'),
            booktitle=data.get('booktitle'),
            doi=data.get('doi'),
            create_time=data.get('create_time'),
            update_time=data.get('update_time')
        )