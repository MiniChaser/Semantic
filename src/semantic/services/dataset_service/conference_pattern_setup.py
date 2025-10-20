"""
Setup conference patterns in database for SQL-based matching
"""

import logging
from typing import List, Tuple
from ...database.connection import DatabaseManager
from ...database.schemas.conference_pattern import ConferencePatternSchema
from .conference_matcher import ConferenceMatcher


def setup_conference_patterns(db_manager: DatabaseManager) -> int:
    """
    Extract conference patterns from ConferenceMatcher and populate database table
    This only needs to be done once (or when conference list changes)

    Returns:
        Number of patterns inserted
    """
    logger = logging.getLogger(__name__)

    # 1. Create table if not exists
    schema = ConferencePatternSchema(db_manager)
    if not schema.create_table():
        raise Exception("Failed to create conference_patterns table")

    # 2. Get conference matcher
    matcher = ConferenceMatcher()

    # 3. Build pattern list
    patterns: List[Tuple[str, str, str]] = []

    # Exact match patterns
    for conf in matcher.conferences:
        patterns.append((conf, conf.lower(), 'exact'))

    # Alias patterns
    for conf, aliases in matcher.aliases.items():
        for alias in aliases:
            patterns.append((conf, alias.lower(), 'alias'))

    # Contains patterns (from normalization prefixes)
    # Add common variations that should match
    contains_patterns = {
        'CVPR': ['computer vision', 'pattern recognition'],
        'ACL': ['computational linguistics', 'association for computational linguistics'],
        'EMNLP': ['empirical methods', 'natural language processing'],
        'ICML': ['machine learning'],
        'NeurIPS': ['neural information processing'],
        'ICLR': ['learning representations'],
        'Oakland': ['security', 'privacy', 's&p'],
        'NDSS': ['network', 'distributed system security'],
        'CCS': ['computer and communications security'],
        'UsenixSec': ['usenix security'],
        'SIGMOD': ['management of data'],
        'VLDB': ['very large'],
        'ICDE': ['data engineering'],
        'CHI': ['human factors'],
        'UIST': ['user interface'],
        'SIGGRAPH': ['computer graphics'],
        'OSDI': ['operating systems design'],
        'SOSP': ['operating systems principles'],
        'NSDI': ['networked systems'],
        'EuroSys': ['european'],
        'PLDI': ['programming language design'],
        'POPL': ['principles of programming'],
        'OOPSLA': ['object-oriented'],
        'ICSE': ['software engineering'],
        'FSE': ['foundations of software'],
        'ASE': ['automated software'],
    }

    for conf, keywords in contains_patterns.items():
        for keyword in keywords:
            patterns.append((conf, keyword.lower(), 'contains'))

    logger.info(f"Collected {len(patterns)} conference patterns")

    # 4. Insert patterns (using executemany for efficiency)
    insert_sql = """
    INSERT INTO conference_patterns (conference, pattern, match_type)
    VALUES (%s, %s, %s)
    ON CONFLICT (conference, pattern, match_type) DO NOTHING
    """

    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.executemany(insert_sql, patterns)
        inserted_count = cursor.rowcount
        conn.commit()

        logger.info(f"✓ Inserted {inserted_count} conference patterns")
        return inserted_count

    except Exception as e:
        logger.error(f"Failed to insert conference patterns: {e}")
        raise


def check_conference_patterns_exist(db_manager: DatabaseManager) -> bool:
    """Check if conference_patterns table exists and has data"""
    try:
        result = db_manager.fetch_one(
            "SELECT COUNT(*) as count FROM conference_patterns"
        )
        return result and result['count'] > 0
    except:
        return False
