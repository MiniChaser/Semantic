"""
Database package
"""

# Import from new modular structure
from .models import Paper
from .repositories import PaperRepository
from .schemas import DatabaseSchema
from .connection import DatabaseManager, get_db_manager

# Backward compatibility - import from old files for existing code
try:
    # This allows existing imports like "from semantic.database.models import Paper, PaperRepository" to still work
    from .models import *
    from .repositories import *
    from .schemas import *
except ImportError:
    pass

__all__ = ['Paper', 'PaperRepository', 'DatabaseSchema', 'DatabaseManager', 'get_db_manager']