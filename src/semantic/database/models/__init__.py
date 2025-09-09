"""
Database models module
"""

from .paper import DBLP_Paper
# Import DBLPPaperRepository for backward compatibility (it was originally in models.py)
from ..repositories.paper import DBLPPaperRepository

__all__ = ['DBLP_Paper', 'DBLPPaperRepository']