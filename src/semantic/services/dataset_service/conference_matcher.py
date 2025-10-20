"""
Conference Matcher Service
Fetches conference list from csconferences.csv and matches venue strings to standard conference names
"""

import logging
import requests
import pandas as pd
from io import StringIO
from typing import List, Dict, Optional


class ConferenceMatcher:
    """
    Conference matcher for identifying papers from target conferences
    Fetches conference list from GitHub and provides matching logic
    """

    def __init__(self):
        self.logger = self._setup_logger()
        self.conferences = self._fetch_conference_list()
        self.aliases = self._build_alias_map()

        # OPTIMIZATION: Pre-compute lookup dictionaries for O(1) matching
        self._build_lookup_dicts()

        self.logger.info(f"Initialized with {len(self.conferences)} conferences")

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.ConferenceMatcher')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _fetch_conference_list(self) -> List[str]:
        """
        Fetch conference list from GitHub csconferences.csv
        Returns list of unique conference names from Conference column
        """
        try:
            url = 'https://raw.githubusercontent.com/emeryberger/csconferences/main/csconferences.csv'
            self.logger.info(f"Fetching conference list from: {url}")

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Parse CSV and extract Conference column
            df = pd.read_csv(StringIO(response.text))

            if 'Conference' not in df.columns:
                self.logger.error("Conference column not found in CSV")
                return []

            # Get unique conference names
            conferences = df['Conference'].dropna().unique().tolist()
            self.logger.info(f"Fetched {len(conferences)} unique conferences")

            return conferences

        except Exception as e:
            self.logger.error(f"Failed to fetch conference list: {e}")
            # Return fallback list of common conferences
            return self._get_fallback_conferences()

    def _get_fallback_conferences(self) -> List[str]:
        """Fallback list of common conferences if fetch fails"""
        self.logger.warning("Using fallback conference list")
        return [
            'AAAI', 'IJCAI', 'ASPLOS', 'HPCA', 'ISCA', 'MICRO', 'RTAS', 'RTSS',
            'CRYPTO', 'EuroCrypt', 'ICDE', 'PODS', 'SIGMOD', 'VLDB', 'EC', 'WINE',
            'SIGGRAPH', 'CHI', 'UIST', 'CAV', 'LICS', 'ICLR', 'ICML', 'NeurIPS',
            'IMC', 'SIGMETRICS', 'MobiCom', 'MobiSys', 'SenSys', 'ACL', 'EMNLP',
            'NSDI', 'SIGCOMM', 'EuroSys', 'FAST', 'OSDI', 'SOSP', 'USENIX-ATC',
            'CC', 'CGO', 'ECOOP', 'ICFP', 'ISMM', 'OOPSLA', 'PLDI', 'POPL', 'PPoPP',
            'ICRA', 'IROS', 'ASE', 'FSE', 'ICSE', 'ISSTA', 'CCS', 'NDSS', 'Oakland',
            'UsenixSec', 'FOCS', 'SODA', 'STOC', 'CVPR', 'ECCV', 'ICCV', 'SIGIR',
            'WSDM', 'WWW'
        ]

    def _build_alias_map(self) -> Dict[str, List[str]]:
        """
        Build conference alias mapping for common variations and abbreviations
        Maps standard conference name to list of aliases
        """
        return {
            'USENIX-ATC': ['ATC', 'USENIX ATC', 'USENIX Annual Technical Conference'],
            'Oakland': ['S&P', 'IEEE S&P', 'IEEE Symposium on Security and Privacy', 'Security and Privacy'],
            'UsenixSec': ['USENIX Security', 'USENIX Security Symposium'],
            'NeurIPS': ['NIPS', 'Neural Information Processing Systems'],
            'EuroCrypt': ['EUROCRYPT'],
            'EuroSys': ['EUROSYS'],
            'SIGMOD': ['SIGMOD/PODS'],
            'VLDB': ['VLDB Conference'],
            'ICML': ['International Conference on Machine Learning'],
            'CVPR': ['IEEE Conference on Computer Vision and Pattern Recognition'],
            'ICCV': ['International Conference on Computer Vision'],
            'ECCV': ['European Conference on Computer Vision'],
            'ACL': ['Annual Meeting of the Association for Computational Linguistics'],
            'EMNLP': ['Conference on Empirical Methods in Natural Language Processing'],
            'CHI': ['CHI Conference on Human Factors in Computing Systems'],
            'SIGCOMM': ['ACM SIGCOMM'],
            'NSDI': ['Symposium on Networked Systems Design and Implementation'],
            'OSDI': ['Symposium on Operating Systems Design and Implementation'],
            'SOSP': ['Symposium on Operating Systems Principles'],
            'PLDI': ['Programming Language Design and Implementation'],
            'POPL': ['Principles of Programming Languages'],
            'ICSE': ['International Conference on Software Engineering'],
            'FSE': ['Foundations of Software Engineering'],
            'CCS': ['ACM Conference on Computer and Communications Security'],
            'NDSS': ['Network and Distributed System Security Symposium'],
        }

    def _build_lookup_dicts(self) -> None:
        """
        Build optimized lookup dictionaries for fast O(1) matching
        Pre-computes all possible variations to avoid looping during matching
        """
        # Exact match: lowercase conf name -> original conf name
        self.exact_match_dict = {conf.lower(): conf for conf in self.conferences}

        # Containment match: For checking if venue contains conf name
        # Store as set for faster membership testing
        self.conf_lowercase_set = {conf.lower() for conf in self.conferences}

        # Alias match: lowercase alias -> original conf name
        self.alias_dict = {}
        for conf, aliases in self.aliases.items():
            for alias in aliases:
                self.alias_dict[alias.lower()] = conf

        self.logger.debug(f"Built lookup dicts: {len(self.exact_match_dict)} exact, {len(self.alias_dict)} aliases")

    def _normalize_venue(self, venue: str) -> str:
        """Normalize venue string for matching"""
        if not venue:
            return ''
        # Convert to lowercase, strip whitespace, remove special characters
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

        Performance: Most matches resolve in O(1) or O(n) where n = 66 conferences
        """
        if not venue or not isinstance(venue, str):
            return None

        venue_lower = venue.lower().strip()

        # Strategy 1: Exact match - O(1)
        if venue_lower in self.exact_match_dict:
            return self.exact_match_dict[venue_lower]

        # Strategy 2: Containment match - O(n) but n=66 is small
        # Check if venue contains any conference name
        for conf_lower in self.conf_lowercase_set:
            if conf_lower in venue_lower:
                return self.exact_match_dict[conf_lower]

        # Strategy 3: Alias match - O(1) per alias check
        # Check if venue contains any alias
        for alias_lower, conf in self.alias_dict.items():
            if alias_lower in venue_lower:
                return conf

        # Strategy 4: Normalized match - O(n) fallback
        venue_normalized = self._normalize_venue(venue)
        for conf_lower in self.conf_lowercase_set:
            if conf_lower in venue_normalized:
                return self.exact_match_dict[conf_lower]

        return None

    def get_conferences(self) -> List[str]:
        """Return the list of all conferences"""
        return self.conferences.copy()

    def get_conference_count(self) -> int:
        """Return the number of conferences"""
        return len(self.conferences)

    def is_valid_conference(self, conference_name: str) -> bool:
        """Check if a conference name is in the list"""
        return conference_name in self.conferences
