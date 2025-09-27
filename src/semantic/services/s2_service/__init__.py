"""
Semantic Scholar service subpackage
Contains S2 API integration and data enrichment services
"""

from .s2_service import SemanticScholarAPI, S2DataParser, S2ValidationService
from .s2_paper_enrichment_service import S2EnrichmentService
from .s2_author_enrichment_service import S2AuthorEnrichmentService

__all__ = [
    'SemanticScholarAPI',
    'S2DataParser',
    'S2ValidationService',
    'S2EnrichmentService',
    'S2AuthorEnrichmentService'
]