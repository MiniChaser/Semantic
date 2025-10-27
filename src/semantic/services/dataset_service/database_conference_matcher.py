"""
Database Conference Matcher
Reads conference list from database instead of GitHub API
"""

import logging
import re
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

        # Group conferences by length for prioritized matching (long names first)
        self._conferences_by_length = sorted(self._conferences, key=lambda x: len(x), reverse=True)

        self.logger.debug(f"Built lookup dicts: {len(self._exact_match_dict)} exact, {len(self._alias_dict)} aliases")

    def _is_word_boundary_match(self, pattern: str, text: str) -> bool:
        """
        Check if pattern exists as a complete word in text (word boundary match)
        Uses regex to ensure pattern is not part of a larger word

        Example:
            _is_word_boundary_match("ec", "conference on ec") -> True
            _is_word_boundary_match("ec", "technology") -> False
        """
        # Escape special regex characters in pattern
        escaped_pattern = re.escape(pattern.lower())
        # Use word boundary markers \b
        regex = r'\b' + escaped_pattern + r'\b'
        return re.search(regex, text.lower()) is not None

    def _has_conference_context(self, venue: str) -> bool:
        """
        Check if venue string contains conference-related keywords
        Used to validate short code matches
        """
        venue_lower = venue.lower()
        conference_keywords = [
            'proceedings', 'conference', 'symposium', 'workshop',
            'acm', 'ieee', 'international', 'proc.', 'proc '
        ]
        return any(keyword in venue_lower for keyword in conference_keywords)

    def _has_year_pattern(self, venue: str, conf_code: str) -> bool:
        """
        Check if venue contains conference code followed by year pattern
        Examples: "EC 2024", "CHI'23", "EC '23"
        """
        venue_lower = venue.lower()
        conf_lower = conf_code.lower()

        # Pattern: conference code followed by optional separator and 2-4 digit year
        patterns = [
            rf'\b{re.escape(conf_lower)}\s+[12]\d{{3}}\b',  # "EC 2024"
            rf'\b{re.escape(conf_lower)}\s*[\'`]\s*\d{{2}}\b',  # "EC'23" or "EC '23"
            rf'\b{re.escape(conf_lower)}\s*-\s*[12]\d{{3}}\b',  # "EC-2024"
        ]

        return any(re.search(pattern, venue_lower) for pattern in patterns)

    def _is_short_code(self, conf_name: str) -> bool:
        """
        Check if conference code is short (<=3 characters)
        Short codes require stricter matching rules
        """
        return len(conf_name) <= 3

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
        Match venue string to a standard conference name (IMPROVED WITH WORD BOUNDARIES)
        Returns the standard conference name if matched, None otherwise

        Matching strategy (prioritized):
        1. Exact match - O(1) dict lookup
        2. Alias match - O(1) per alias, checks word boundaries for short aliases
        3. Conference name match - sorted by length (longest first), with word boundary checks for short codes
        4. Normalized match - fallback with same rules

        Short codes (<=3 chars) require:
        - Word boundary match (not substring)
        - AND (conference context OR year pattern)

        Long names (>=4 chars) use simple substring matching

        Performance: O(1) for exact, O(n) for containment where n = 66 conferences
        """
        if not venue or not isinstance(venue, str):
            return None

        venue_lower = venue.lower().strip()

        # Strategy 1: Exact match - O(1)
        if venue_lower in self._exact_match_dict:
            return self._exact_match_dict[venue_lower]

        # Strategy 2: Alias match with improved logic
        # Process aliases by length (longest first) to avoid short code false positives
        sorted_aliases = sorted(self._alias_dict.items(), key=lambda x: len(x[0]), reverse=True)

        for alias_lower, conf in sorted_aliases:
            # For short aliases (<=3 chars), require word boundary + context
            if len(alias_lower) <= 3:
                if self._is_word_boundary_match(alias_lower, venue_lower):
                    # Require conference context or year pattern
                    if self._has_conference_context(venue) or self._has_year_pattern(venue, alias_lower):
                        return conf
            else:
                # For long aliases, simple substring match is safe
                if alias_lower in venue_lower:
                    return conf

        # Strategy 3: Conference name match - sorted by length (longest first)
        for conf in self._conferences_by_length:
            conf_lower = conf.lower()

            # Short codes require strict matching
            if self._is_short_code(conf):
                # Must have word boundary
                if not self._is_word_boundary_match(conf_lower, venue_lower):
                    continue

                # Must have conference context OR year pattern
                if self._has_conference_context(venue) or self._has_year_pattern(venue, conf):
                    return conf
            else:
                # Long names use simple substring matching
                if conf_lower in venue_lower:
                    return conf

        # Strategy 4: Normalized match - same rules as above
        venue_normalized = self._normalize_venue(venue)
        if venue_normalized and venue_normalized != venue_lower:
            for conf in self._conferences_by_length:
                conf_lower = conf.lower()

                if self._is_short_code(conf):
                    if not self._is_word_boundary_match(conf_lower, venue_normalized):
                        continue
                    # For normalized venue, we can be slightly less strict
                    # (normalization already removes noise)
                    if self._has_conference_context(venue):
                        return conf
                else:
                    if conf_lower in venue_normalized:
                        return conf

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
