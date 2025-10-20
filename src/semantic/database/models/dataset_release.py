"""
Dataset Release data model
Records information about each S2 dataset release
"""

from datetime import datetime
from typing import Optional, Dict
from dataclasses import dataclass, asdict


@dataclass
class DatasetRelease:
    """Dataset Release data model"""
    release_id: str
    dataset_name: str
    release_date: Optional[datetime] = None
    description: Optional[str] = None
    file_count: int = 0
    total_papers_processed: int = 0
    papers_inserted: int = 0
    papers_updated: int = 0
    processing_status: str = 'pending'
    download_start_time: Optional[datetime] = None
    download_end_time: Optional[datetime] = None
    processing_start_time: Optional[datetime] = None
    processing_end_time: Optional[datetime] = None
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'DatasetRelease':
        """Create DatasetRelease object from dictionary"""
        return cls(
            id=data.get('id'),
            release_id=data.get('release_id', ''),
            dataset_name=data.get('dataset_name', ''),
            release_date=data.get('release_date'),
            description=data.get('description'),
            file_count=data.get('file_count', 0),
            total_papers_processed=data.get('total_papers_processed', 0),
            papers_inserted=data.get('papers_inserted', 0),
            papers_updated=data.get('papers_updated', 0),
            processing_status=data.get('processing_status', 'pending'),
            download_start_time=data.get('download_start_time'),
            download_end_time=data.get('download_end_time'),
            processing_start_time=data.get('processing_start_time'),
            processing_end_time=data.get('processing_end_time'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )
