"""
Semantic Scholar service subpackage
Contains S2 API integration, data enrichment, and PDF download services
"""

from .s2_service import SemanticScholarAPI, S2DataParser, S2ValidationService
from .s2_enrichment_service import S2EnrichmentService
from .pdf_download_service import PDFDownloadService

__all__ = [
    'SemanticScholarAPI',
    'S2DataParser', 
    'S2ValidationService',
    'S2EnrichmentService',
    'PDFDownloadService'
]