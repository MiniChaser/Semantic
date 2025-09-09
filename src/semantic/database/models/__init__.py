"""
Database models module
"""

from .paper import Paper
# Import PaperRepository for backward compatibility (it was originally in models.py)
from ..repositories.paper import PaperRepository

__all__ = ['Paper', 'PaperRepository']