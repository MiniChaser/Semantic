"""
Title Normalization Service

Normalizes paper titles using a strict 4-step sequential process:
1. Remove leading spaces and handle None/empty values
2. Remove prefixes (case-insensitive, longest first)
3. Apply colon rule (delete everything before first : or ：)
4. Remove all non-alphanumeric characters and convert to lowercase

Output: Single concatenated string with no spaces (e.g., "naturallanguageprocessing")

Usage:
    normalizer = TitleNormalizer()
    clean_title = normalizer.normalize(raw_title)
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class NormalizationConfig:
    """Configuration for title normalization"""

    # Common prefixes to remove (will be sorted by length, longest first)
    prefixes: List[str] = field(default_factory=lambda: [
       "Association for Computational Linguistics",
        "Explorer",
        "Erratum to",
        "UvA-DARE ( Digital Academic Repository )",
        "Invited Talk",
        "Combination of",
        "40 80 11 v 1 2 2 A ug 1 99 4",
        "Edinburgh Research Explorer",
        "Toward",
        "Interactive Discourse"
        "Shared task",
        "Thesis Proposal",
        "Supplemental Materials",
        "Supplementary",
        "UvA-DARE (Digital Academic Repository) ",
        "Review of",
        "Position Paper",
        "MeMo",
        "BGE",
        "MaXM",
        "[RE]",
        "CLPsych 2016",
        "CLPsych 2015",
        "Explorer",
        "Panel Chair’s Introduction",
        "KGLM",
        "KGPT",
        "DGST",
        "Statistical Power and",
        "Beyond Geolocation",
        "Position Paper"
    ])


    contractions = {
        "it's": "it is",
        "i'm": "i am",
        "you're": "you are",
        "we're": "we are",
        "they're": "they are",
        "don't": "do not",
        "doesn't": "does not",
        "can't": "cannot",
        "won't": "will not",
        "shan't": "shall not",
        "let's": "let us",
        "there's": "there is",
        "that's": "that is",
        "what's": "what is",
        "where's": "where is",
        "it’s": "it is",
        "i’m": "i am",
        "you’re": "you are",
        "we’re": "we are",
        "they’re": "they are",
        "don’t": "do not",
        "doesn’t": "does not",
        "can’t": "cannot",
        "won’t": "will not",
        "shan’t": "shall not",
        "let’s": "let us",
        "there’s": "there is",
        "that’s": "that is",
        "what’s": "what is",
        "where’s": "where is",
        # 可继续添加...
    }
    

class TitleNormalizer:
    """
    Normalize paper titles using strict 4-step sequential process:

    1. Strip leading spaces and handle None/empty
    2. Remove prefixes (case-insensitive, longest first)
    3. Apply colon rule (delete everything before first : or ：)
    4. Remove all non-alphanumeric characters and convert to lowercase

    Result: Space-free concatenated string (e.g., "naturallanguageprocessing")
    """

    def __init__(self, config: Optional[NormalizationConfig] = None):
        """
        Initialize normalizer with configuration

        Args:
            config: Optional configuration object. Uses defaults if not provided.
        """
        self.config = config or NormalizationConfig()

        # Sort prefixes by length (longest first) for optimal matching
        self.prefixes_sorted = sorted(self.config.prefixes, key=len, reverse=True)

        # Pre-compile regex pattern for step 4 (performance optimization)
        self.non_alphanumeric_pattern = re.compile(r'[^a-z0-9]')

    def normalize_superscript(self,text):
        # 定义上标数字到正常数字的映射
        superscript_map = {
            '⁰': '0', '⁰': '0',  # U+2070
            '¹': '1',           # U+00B9
            '²': '2',           # U+00B2
            '³': '3',           # U+00B3
            '⁴': '4', '⁵': '5', '⁶': '6',
            '⁷': '7', '⁸': '8', '⁹': '9',
        }
        
        # 使用映射替换上标字符
        result = ''.join(superscript_map.get(char, char) for char in text)
        return result
    def expand_contractions(self, text):
        # 将文本转为小写用于匹配，但保留原始大小写信息较复杂，这里简单处理
        words = text.split()
        expanded_words = []
        for word in words:
            # 保留标点符号
            leading_punct = ''
            trailing_punct = ''
            clean_word = word

            # 分离前后的标点符号
            while len(clean_word) > 0 and not clean_word[0].isalnum():
                leading_punct += clean_word[0]
                clean_word = clean_word[1:]
            while len(clean_word) > 0 and not clean_word[-1].isalnum():
                trailing_punct = clean_word[-1] + trailing_punct
                clean_word = clean_word[:-1]

            # 转换为小写进行匹配
            key = clean_word.lower()
            contractions = self.config.contractions
            if key in contractions:
                new_word = contractions[key]
                # 如果原词首字母大写，则新词首字母也大写（如 It's -> It is）
                if clean_word[0].isupper():
                    new_word = new_word.capitalize()
            else:
                new_word = clean_word

            expanded_words.append(leading_punct + new_word + trailing_punct)
        return ' '.join(expanded_words)
    def normalize(self, title: str) -> str:
        """
        Normalize a paper title using strict 4-step process

        Args:
            title: Raw paper title

        Returns:
            Normalized title (space-free, lowercase, alphanumeric only)
        """
        # Handle None/NaN/empty (check for pandas NA using pd.isna equivalent)
        if title is None:
            return ""

        # Check for pandas NA/NaT without importing pandas
        try:
            import pandas as pd
            if pd.isna(title):
                return ""
        except (ImportError, TypeError):
            pass

        # Convert to string and strip leading spaces (Step 1)
        try:
            title = str(title).lstrip()
        except:
            return ""

        if not title:
            return ""
        
        title = self.normalize_superscript(title)

        title = self.expand_contractions(title)

        # Step 2: Remove prefixes (case-insensitive, longest first)
        # Build regex pattern for all prefixes
        pattern = '^(' + '|'.join(re.escape(p) for p in self.prefixes_sorted) + ')'
        match = re.match(pattern, title, re.IGNORECASE)

        if match:
            # Remove matched prefix
            title = title[len(match.group(0)):].lstrip()

        # Step 3: Apply colon rule (delete everything before first colon)
        # Check for English colon (:)
        # if ':' in title:
        #     idx = title.find(':')
        #     title = title[idx+1:].lstrip()

        # Check for Chinese colon (：)
        # if '：' in title:
        #     idx = title.find('：')
        #     title = title[idx+1:].lstrip()

        # Step 4: Remove all non-alphanumeric characters and convert to lowercase
        title = self.non_alphanumeric_pattern.sub('', title.lower())

        return title

    def batch_normalize(self, titles: List[str]) -> List[str]:
        """
        Normalize a batch of titles

        Args:
            titles: List of raw titles

        Returns:
            List of normalized titles
        """
        return [self.normalize(title) for title in titles]


# Singleton instance for easy import
_default_normalizer = None


def get_normalizer(config: Optional[NormalizationConfig] = None) -> TitleNormalizer:
    """
    Get or create default normalizer instance

    Args:
        config: Optional configuration. If None, uses default config.

    Returns:
        TitleNormalizer instance
    """
    global _default_normalizer

    if config is not None:
        # Return new instance with custom config
        return TitleNormalizer(config)

    if _default_normalizer is None:
        _default_normalizer = TitleNormalizer()

    return _default_normalizer


def normalize_title(title: str) -> str:
    """
    Convenience function to normalize a single title using default config

    Args:
        title: Raw paper title

    Returns:
        Normalized title (space-free, lowercase, alphanumeric only)
    """
    return get_normalizer().normalize(title)
