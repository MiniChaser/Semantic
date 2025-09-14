"""
Author service subpackage
Contains all author-related services including profile management and final table generation
"""

from .author_profile_service import AuthorProfileService
from .final_author_table_service import FinalAuthorTableService
from .author_disambiguation_service import AuthorMatcher

__all__ = [
    'AuthorProfileService',
    'FinalAuthorTableService',
    'AuthorMatcher'
]