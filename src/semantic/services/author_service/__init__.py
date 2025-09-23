"""
Author service subpackage
Contains all author-related services including profile management and final table generation
"""

from .author_profile_pandas_service import AuthorProfilePandasService
from .authorship_pandas_service import AuthorshipPandasService
from .final_author_table_pandas_service import FinalAuthorTablePandasService
from .author_disambiguation_service import AuthorMatcher

__all__ = [
    'AuthorProfilePandasService',
    'AuthorshipPandasService',
    'FinalAuthorTablePandasService',
    'AuthorMatcher'
]