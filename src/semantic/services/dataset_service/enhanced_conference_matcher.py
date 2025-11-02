"""
Enhanced Conference Matcher with Sentence Transformers
Uses semantic similarity for better venue matching
"""

import logging
import numpy as np
from typing import List, Dict, Optional, Tuple
from sentence_transformers import SentenceTransformer
from pathlib import Path
import pickle

from ...database.connection import DatabaseManager
from .database_conference_matcher import DatabaseConferenceMatcher


class EnhancedConferenceMatcher:
    """
    Enhanced conference matcher with two-layer strategy:
    1. Exact matching (fast, rule-based)
    2. Semantic similarity (embedding-based)
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        model_name: str = 'all-MiniLM-L6-v2',
        similarity_threshold: float = 0.75,
        cache_dir: Optional[str] = None
    ):
        """
        Initialize enhanced matcher

        Args:
            db_manager: Database manager instance
            model_name: Sentence transformer model name (default: all-MiniLM-L6-v2, 80MB)
            similarity_threshold: Minimum cosine similarity for semantic match (0.0-1.0)
            cache_dir: Directory to cache embeddings (default: .cache/embeddings)
        """
        self.db_manager = db_manager
        self.similarity_threshold = similarity_threshold
        self.logger = self._setup_logger()

        # Layer 1: Exact matcher
        self.exact_matcher = DatabaseConferenceMatcher(db_manager)

        # Layer 2: Semantic matcher
        self.logger.info(f"Loading sentence transformer model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.logger.info("Model loaded successfully")

        # Cache settings
        self.cache_dir = Path(cache_dir or '.cache/embeddings')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.embeddings_cache_file = self.cache_dir / 'conference_embeddings.pkl'

        # Conference embeddings
        self._conference_embeddings = None
        self._conference_names = None
        self._conference_texts = None

        # Build embeddings
        self._build_conference_embeddings()

        # Statistics
        self.stats = {
            'exact_matches': 0,
            'semantic_matches': 0,
            'no_matches': 0
        }

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.EnhancedConferenceMatcher')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _build_conference_embeddings(self) -> None:
        """Build or load cached embeddings for all conferences"""

        # Try to load from cache first
        if self._load_embeddings_from_cache():
            return

        self.logger.info("Building conference embeddings...")

        # Get all conferences with full names and aliases
        conferences = self.exact_matcher.get_conferences()

        # Build rich text representations for each conference
        conference_texts = []
        conference_names = []

        for conf in conferences:
            # Get full name and aliases
            full_name = self.exact_matcher._full_names.get(conf, '')
            aliases = self.exact_matcher._aliases.get(conf, [])

            # Build text representation: combine all information
            text_parts = [conf]
            if full_name:
                text_parts.append(full_name)
            if aliases:
                text_parts.extend(aliases)

            text = ' | '.join(text_parts)

            conference_texts.append(text)
            conference_names.append(conf)

        # Encode all conferences (batch processing is faster)
        self.logger.info(f"Encoding {len(conference_texts)} conferences...")
        self._conference_embeddings = self.model.encode(
            conference_texts,
            batch_size=32,
            show_progress_bar=True,
            convert_to_numpy=True
        )
        self._conference_names = conference_names
        self._conference_texts = conference_texts

        # Normalize embeddings for faster cosine similarity
        norms = np.linalg.norm(self._conference_embeddings, axis=1, keepdims=True)
        self._conference_embeddings = self._conference_embeddings / norms

        self.logger.info(f"Built embeddings: shape={self._conference_embeddings.shape}")

        # Save to cache
        self._save_embeddings_to_cache()

    def _load_embeddings_from_cache(self) -> bool:
        """Load embeddings from cache file"""
        if not self.embeddings_cache_file.exists():
            return False

        try:
            self.logger.info(f"Loading embeddings from cache: {self.embeddings_cache_file}")
            with open(self.embeddings_cache_file, 'rb') as f:
                cache = pickle.load(f)

            self._conference_embeddings = cache['embeddings']
            self._conference_names = cache['names']
            self._conference_texts = cache['texts']

            self.logger.info(f"Loaded {len(self._conference_names)} conference embeddings from cache")
            return True

        except Exception as e:
            self.logger.warning(f"Failed to load embeddings from cache: {e}")
            return False

    def _save_embeddings_to_cache(self) -> None:
        """Save embeddings to cache file"""
        try:
            cache = {
                'embeddings': self._conference_embeddings,
                'names': self._conference_names,
                'texts': self._conference_texts
            }

            with open(self.embeddings_cache_file, 'wb') as f:
                pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)

            self.logger.info(f"Saved embeddings to cache: {self.embeddings_cache_file}")

        except Exception as e:
            self.logger.warning(f"Failed to save embeddings to cache: {e}")

    def _semantic_match(self, venue: str) -> Optional[Tuple[str, float]]:
        """
        Find best matching conference using semantic similarity

        Returns:
            Tuple of (conference_name, similarity_score) or None
        """
        if not venue or not isinstance(venue, str):
            return None

        # Encode venue
        venue_embedding = self.model.encode([venue], convert_to_numpy=True)

        # Normalize
        venue_embedding = venue_embedding / np.linalg.norm(venue_embedding)

        # Calculate cosine similarities (dot product since normalized)
        similarities = np.dot(self._conference_embeddings, venue_embedding.T).flatten()

        # Find best match
        best_idx = np.argmax(similarities)
        best_similarity = similarities[best_idx]
        best_conference = self._conference_names[best_idx]

        if best_similarity >= self.similarity_threshold:
            return (best_conference, float(best_similarity))

        return None

    def match_conference(self, venue: str) -> Optional[str]:
        """
        Match venue to conference using two-layer strategy

        Layer 1: Exact matching (fast)
        Layer 2: Semantic similarity (if layer 1 fails)

        Returns:
            Standard conference name or None
        """
        if not venue or not isinstance(venue, str):
            return None

        # Layer 1: Try exact matching first (fast)
        exact_match = self.exact_matcher.match_conference(venue)
        if exact_match:
            self.stats['exact_matches'] += 1
            return exact_match

        # Layer 2: Try semantic matching
        semantic_result = self._semantic_match(venue)
        if semantic_result:
            conference, similarity = semantic_result
            self.stats['semantic_matches'] += 1
            self.logger.debug(f"Semantic match: '{venue}' -> '{conference}' (similarity={similarity:.3f})")
            return conference

        # No match
        self.stats['no_matches'] += 1
        return None

    def match_conference_with_confidence(self, venue: str) -> Optional[Tuple[str, float, str]]:
        """
        Match venue and return confidence score and method

        Returns:
            Tuple of (conference_name, confidence, method) or None
            method: 'exact' or 'semantic'
        """
        if not venue or not isinstance(venue, str):
            return None

        # Try exact matching first
        exact_match = self.exact_matcher.match_conference(venue)
        if exact_match:
            self.stats['exact_matches'] += 1
            return (exact_match, 1.0, 'exact')

        # Try semantic matching
        semantic_result = self._semantic_match(venue)
        if semantic_result:
            conference, similarity = semantic_result
            self.stats['semantic_matches'] += 1
            return (conference, similarity, 'semantic')

        # No match
        self.stats['no_matches'] += 1
        return None

    def get_conference_count(self) -> int:
        """Return the number of conferences"""
        return len(self._conference_names)

    def get_statistics(self) -> Dict:
        """Return matching statistics"""
        total = sum(self.stats.values())
        if total == 0:
            return self.stats

        return {
            **self.stats,
            'total': total,
            'exact_rate': self.stats['exact_matches'] / total,
            'semantic_rate': self.stats['semantic_matches'] / total,
            'no_match_rate': self.stats['no_matches'] / total
        }

    def reset_statistics(self) -> None:
        """Reset matching statistics"""
        self.stats = {
            'exact_matches': 0,
            'semantic_matches': 0,
            'no_matches': 0
        }

    def reload(self) -> None:
        """Reload conferences and rebuild embeddings"""
        self.logger.info("Reloading conferences and rebuilding embeddings...")
        self.exact_matcher.reload()

        # Delete cache to force rebuild
        if self.embeddings_cache_file.exists():
            self.embeddings_cache_file.unlink()

        self._build_conference_embeddings()
        self.reset_statistics()
