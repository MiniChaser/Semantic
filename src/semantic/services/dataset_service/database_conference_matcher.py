"""
Database Conference Matcher
Reads conference list from database instead of GitHub API
"""

import logging
from typing import List, Dict, Optional

from ...database.connection import DatabaseManager


class DatabaseConferenceMatcher:
    """
    Database-backed conference matcher
    Reads from conferences table instead of GitHub API for better performance and reliability
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()

        # Cache data in memory after first load
        self._conferences = None
        self._aliases = None
        self._exact_match_dict = None
        self._conf_lowercase_set = None
        self._alias_dict = None

        # Load from database
        self._load_from_database()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.DatabaseConferenceMatcher')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _load_from_database(self) -> None:
        """Load conference list and aliases from database"""
        try:
            # Load active conferences
            results = self.db_manager.fetch_all("""
                SELECT conference_name
                FROM conferences
                WHERE is_active = TRUE
                ORDER BY conference_name
            """)
            self._conferences = [r['conference_name'] for r in results]

            # Load aliases
            results = self.db_manager.fetch_all("""
                SELECT conference_name, alias, priority
                FROM conference_aliases
                ORDER BY priority DESC, conference_name
            """)

            # Build aliases dict
            self._aliases = {}
            for r in results:
                conf = r['conference_name']
                alias = r['alias']

                # Skip if alias is same as conference name (to avoid duplicates)
                if alias.lower() != conf.lower():
                    if conf not in self._aliases:
                        self._aliases[conf] = []
                    self._aliases[conf].append(alias)

            self.logger.info(f"Loaded {len(self._conferences)} conferences with {len(results)} aliases from database")

            # Build lookup dictionaries for O(1) matching
            self._build_lookup_dicts()

        except Exception as e:
            self.logger.error(f"Failed to load conferences from database: {e}")
            # Fallback to empty lists
            self._conferences = []
            self._aliases = {}
            self._build_lookup_dicts()

    def _build_lookup_dicts(self) -> None:
        """
        Build optimized lookup dictionaries for fast O(1) matching
        Pre-computes all possible variations to avoid looping during matching
        """
        # Exact match: lowercase conf name -> original conf name
        self._exact_match_dict = {conf.lower(): conf for conf in self._conferences}

        # Containment match: For checking if venue contains conf name
        # Store as set for faster membership testing
        self._conf_lowercase_set = {conf.lower() for conf in self._conferences}

        # Alias match: lowercase alias -> original conf name
        self._alias_dict = {}
        for conf, aliases in self._aliases.items():
            for alias in aliases:
                self._alias_dict[alias.lower()] = conf

        self.logger.debug(f"Built lookup dicts: {len(self._exact_match_dict)} exact, {len(self._alias_dict)} aliases")

    def _normalize_venue(self, venue: str) -> str:
        """Normalize venue string for matching"""
        if not venue:
            return ''
        # Convert to lowercase, strip whitespace
        normalized = venue.lower().strip()
        # Remove common prefixes
        for prefix in ['proceedings of', 'proc.', 'conference on', 'symposium on', 'international']:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):].strip()
        return normalized

    def match_conference(self, venue: str) -> Optional[str]:
        """
        Match venue string to a standard conference name (OPTIMIZED)
        Returns the standard conference name if matched, None otherwise

        Matching strategy:
        1. Exact match O(1) - dict lookup
        2. Containment match O(n) - check if any conf name in venue
        3. Alias match O(1) per alias - dict lookup
        4. Normalized match O(n) - fallback

        Performance: Most matches resolve in O(1) or O(n) where n = number of conferences
        """
        if not venue or not isinstance(venue, str):
            return None

        venue_lower = venue.lower().strip()

        # Strategy 1: Exact match - O(1)
        if venue_lower in self._exact_match_dict:
            return self._exact_match_dict[venue_lower]

        # Strategy 2: Containment match - O(n) but n is small (66 conferences)
        # Check if venue contains any conference name
        for conf_lower in self._conf_lowercase_set:
            if conf_lower in venue_lower:
                return self._exact_match_dict[conf_lower]

        # Strategy 3: Alias match - O(1) per alias check
        # Check if venue contains any alias
        for alias_lower, conf in self._alias_dict.items():
            if alias_lower in venue_lower:
                return conf

        # Strategy 4: Normalized match - O(n) fallback
        venue_normalized = self._normalize_venue(venue)
        for conf_lower in self._conf_lowercase_set:
            if conf_lower in venue_normalized:
                return self._exact_match_dict[conf_lower]

        return None

    def get_conferences(self) -> List[str]:
        """Return the list of all conferences"""
        return self._conferences.copy()

    def get_conference_count(self) -> int:
        """Return the number of conferences"""
        return len(self._conferences)

    def is_valid_conference(self, conference_name: str) -> bool:
        """Check if a conference name is in the list"""
        return conference_name in self._conferences

    def reload(self) -> None:
        """Reload conferences from database (useful if data changed)"""
        self.logger.info("Reloading conferences from database...")
        self._load_from_database()
