#!/usr/bin/env python3
"""
Author Profile Service
Creates author profiles, authorships table, and calculates author metrics
"""

import json
import logging
from datetime import datetime
from typing import Dict, List

from ...database.connection import DatabaseManager
from .author_disambiguation_service import AuthorMatcher


logger = logging.getLogger(__name__)


class AuthorProfileService:
    """Service for creating and managing author profiles and relationships"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.matcher = AuthorMatcher()
    
    def create_authorships_table(self) -> bool:
        """
        Create the authorships table to store paper-author relationships
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating authorships table...")
            
            # Create authorships table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS authorships (
                id SERIAL PRIMARY KEY,
                paper_id INTEGER NOT NULL,
                semantic_paper_id VARCHAR(255),
                paper_title TEXT,
                dblp_author_name TEXT NOT NULL,
                s2_author_name TEXT,
                s2_author_id VARCHAR(255),
                authorship_order INTEGER NOT NULL,
                match_confidence VARCHAR(50) NOT NULL,
                match_method VARCHAR(100),
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (paper_id) REFERENCES enriched_papers(id) ON DELETE CASCADE
            );
            """
            
            self.db_manager.execute_query(create_table_sql)
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_authorships_paper_id ON authorships(paper_id);",
                "CREATE INDEX IF NOT EXISTS idx_authorships_s2_author_id ON authorships(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_authorships_semantic_paper_id ON authorships(semantic_paper_id);",
                "CREATE INDEX IF NOT EXISTS idx_authorships_dblp_author_name ON authorships(dblp_author_name);",
            ]
            
            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)
            
            logger.info("Authorships table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create authorships table: {e}")
            return False
    
    def populate_authorships_table(self, batch_size: int = 1000) -> Dict:
        """
        Populate the authorships table from enriched_papers data
        
        Args:
            batch_size: Number of papers to process in each batch
            
        Returns:
            Statistics about the population process
        """
        try:
            logger.info("Starting authorships table population...")
            
            # Clear existing data
            self.db_manager.execute_query("DELETE FROM authorships;")
            
            # Get total count for progress tracking
            total_papers = self.db_manager.fetch_one(
                "SELECT COUNT(*) as count FROM enriched_papers WHERE semantic_authors IS NOT NULL"
            )['count']
            
            logger.info(f"Processing {total_papers} papers with author data...")
            
            stats = {
                'total_papers': 0,
                'processed_papers': 0,
                'total_authorships': 0,
                'matched_authors': 0,
                'unmatched_authors': 0,
                'processing_errors': 0
            }
            
            offset = 0
            while True:
                # Fetch batch of papers
                papers = self.db_manager.fetch_all(f"""
                    SELECT 
                        id, semantic_paper_id, dblp_title,
                        dblp_authors, semantic_authors
                    FROM enriched_papers 
                    WHERE semantic_authors IS NOT NULL
                    ORDER BY id
                    LIMIT {batch_size} OFFSET {offset}
                """)
                
                if not papers:
                    break
                
                batch_authorships = []
                
                for paper in papers:
                    try:
                        stats['total_papers'] += 1
                        
                        # Parse author data
                        dblp_authors = paper['dblp_authors'] if paper['dblp_authors'] else []
                        s2_authors = paper['semantic_authors'] if paper['semantic_authors'] else []
                        
                        # Ensure s2_authors is a list of dicts
                        if isinstance(s2_authors, str):
                            s2_authors = json.loads(s2_authors)
                        
                        if not dblp_authors or not s2_authors:
                            continue
                        
                        # Perform author matching
                        matched_pairs, unmatched_dblp = self.matcher.match_authors_enhanced(
                            dblp_authors, s2_authors
                        )
                        
                        authorship_order = 1
                        
                        # Process matched authors
                        for dblp_name, s2_author in matched_pairs.items():
                            authorship_record = {
                                'paper_id': paper['id'],
                                'semantic_paper_id': paper['semantic_paper_id'],
                                'paper_title': paper['dblp_title'],
                                'dblp_author_name': dblp_name,
                                's2_author_name': s2_author['name'],
                                's2_author_id': s2_author['authorId'],
                                'authorship_order': authorship_order,
                                'match_confidence': 'matched',
                                'match_method': 'multi_tier_matching'
                            }
                            batch_authorships.append(authorship_record)
                            authorship_order += 1
                            stats['matched_authors'] += 1
                        
                        # Process unmatched authors  
                        for dblp_name in unmatched_dblp:
                            authorship_record = {
                                'paper_id': paper['id'],
                                'semantic_paper_id': paper['semantic_paper_id'],
                                'paper_title': paper['dblp_title'],
                                'dblp_author_name': dblp_name,
                                's2_author_name': None,
                                's2_author_id': None,
                                'authorship_order': authorship_order,
                                'match_confidence': 'unmatched',
                                'match_method': 'no_match_found'
                            }
                            batch_authorships.append(authorship_record)
                            authorship_order += 1
                            stats['unmatched_authors'] += 1
                        
                        stats['processed_papers'] += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing paper {paper.get('id')}: {e}")
                        stats['processing_errors'] += 1
                        continue
                
                # Batch insert authorships
                if batch_authorships:
                    self._batch_insert_authorships(batch_authorships)
                    stats['total_authorships'] += len(batch_authorships)
                
                offset += batch_size
                
                # Progress logging
                if stats['processed_papers'] % 5000 == 0:
                    logger.info(f"Processed {stats['processed_papers']}/{total_papers} papers...")
            
            logger.info("Authorships table population completed successfully")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to populate authorships table: {e}")
            return {'error': str(e)}
    
    def _batch_insert_authorships(self, authorships: List[Dict]):
        """Batch insert authorships records"""
        if not authorships:
            return
        
        insert_sql = """
        INSERT INTO authorships (
            paper_id, semantic_paper_id, paper_title,
            dblp_author_name, s2_author_name, s2_author_id,
            authorship_order, match_confidence, match_method
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = [
            (
                auth['paper_id'], auth['semantic_paper_id'], auth['paper_title'],
                auth['dblp_author_name'], auth['s2_author_name'], auth['s2_author_id'],
                auth['authorship_order'], auth['match_confidence'], auth['match_method']
            )
            for auth in authorships
        ]
        
        self.db_manager.execute_batch_query(insert_sql, values)
    
    def create_author_profiles_table(self) -> bool:
        """
        Create the author_profiles table for unique author records
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating author_profiles table...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS author_profiles (
                id SERIAL PRIMARY KEY,
                s2_author_id VARCHAR(255) UNIQUE,
                dblp_author_name TEXT NOT NULL,
                s2_author_name TEXT,
                
                -- Basic statistics
                paper_count INTEGER DEFAULT 0,
                total_citations INTEGER DEFAULT 0,
                avg_citations_per_paper NUMERIC DEFAULT 0,
                
                -- Career information  
                first_publication_year INTEGER,
                latest_publication_year INTEGER,
                career_length INTEGER DEFAULT 0,
                
                -- Authorship position statistics
                first_author_count INTEGER DEFAULT 0,
                last_author_count INTEGER DEFAULT 0,
                middle_author_count INTEGER DEFAULT 0,
                first_author_ratio NUMERIC DEFAULT 0,
                last_author_ratio NUMERIC DEFAULT 0,
                
                -- Affiliation and external data (for future enhancement)
                affiliation TEXT,  -- TODO: Add CSRankings integration
                homepage TEXT,     -- TODO: Add from S2 Author API
                google_scholar_id VARCHAR(255),  -- TODO: Add Google Scholar integration
                
                -- S2 enhanced metrics (for Phase 2)
                s2_paper_count INTEGER,     -- TODO: Fetch from S2 Author API
                s2_citation_count INTEGER,  -- TODO: Fetch from S2 Author API 
                s2_h_index INTEGER,         -- TODO: Fetch from S2 Author API
                s2_affiliations TEXT,       -- TODO: Fetch from S2 Author API
                
                -- Calculated metrics
                contribution_score NUMERIC DEFAULT 0,
                rising_star_score NUMERIC DEFAULT 0,
                collaboration_network_strength NUMERIC DEFAULT 0,
                
                -- Data quality indicators
                match_confidence VARCHAR(50) DEFAULT 'medium',
                data_completeness_score NUMERIC DEFAULT 0,
                
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.db_manager.execute_query(create_table_sql)
            
            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_author_profiles_s2_id ON author_profiles(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_author_profiles_dblp_name ON author_profiles(dblp_author_name);",
                "CREATE INDEX IF NOT EXISTS idx_author_profiles_paper_count ON author_profiles(paper_count DESC);",
                "CREATE INDEX IF NOT EXISTS idx_author_profiles_citations ON author_profiles(total_citations DESC);",
            ]
            
            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)
            
            logger.info("Author profiles table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create author profiles table: {e}")
            return False
    
    def populate_author_profiles_table(self) -> Dict:
        """
        Populate author profiles table from authorships data
        
        Returns:
            Statistics about the population process
        """
        try:
            logger.info("Starting author profiles population...")
            
            # Clear existing data
            self.db_manager.execute_query("DELETE FROM author_profiles;")
            
            stats = {
                'total_unique_authors': 0,
                'authors_with_s2_id': 0,
                'authors_without_s2_id': 0,
                'processing_errors': 0
            }
            
            # Process authors with S2 IDs first (ensure unique S2 author IDs)
            s2_authors = self.db_manager.fetch_all("""
                SELECT 
                    s2_author_id,
                    MIN(s2_author_name) as s2_author_name,
                    MIN(dblp_author_name) as dblp_author_name,
                    COUNT(*) as paper_count,
                    STRING_AGG(DISTINCT semantic_paper_id, ';') as paper_ids
                FROM authorships 
                WHERE s2_author_id IS NOT NULL
                GROUP BY s2_author_id
                ORDER BY paper_count DESC
            """)
            
            for author in s2_authors:
                try:
                    profile = self._calculate_author_profile(author)
                    self._insert_author_profile(profile)
                    stats['total_unique_authors'] += 1
                    stats['authors_with_s2_id'] += 1
                except Exception as e:
                    logger.error(f"Error processing author {author.get('s2_author_id')}: {e}")
                    stats['processing_errors'] += 1
            
            # Process authors without S2 IDs (unmatched)
            unmatched_authors = self.db_manager.fetch_all("""
                SELECT 
                    dblp_author_name,
                    COUNT(*) as paper_count,
                    STRING_AGG(DISTINCT semantic_paper_id, ';') as paper_ids
                FROM authorships 
                WHERE s2_author_id IS NULL
                GROUP BY dblp_author_name
                ORDER BY paper_count DESC
            """)
            
            for author in unmatched_authors:
                try:
                    profile = self._calculate_author_profile(author, has_s2_id=False)
                    self._insert_author_profile(profile)
                    stats['total_unique_authors'] += 1
                    stats['authors_without_s2_id'] += 1
                except Exception as e:
                    logger.error(f"Error processing unmatched author {author.get('dblp_author_name')}: {e}")
                    stats['processing_errors'] += 1
            
            logger.info("Author profiles population completed successfully")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to populate author profiles: {e}")
            return {'error': str(e)}
    
    def _calculate_author_profile(self, author: Dict, has_s2_id: bool = True) -> Dict:
        """Calculate comprehensive author profile metrics"""
        
        # Get paper details for this author
        if has_s2_id:
            papers = self.db_manager.fetch_all("""
                SELECT 
                    a.authorship_order,
                    e.semantic_citation_count,
                    e.semantic_year
                FROM authorships a
                JOIN enriched_papers e ON a.paper_id = e.id
                WHERE a.s2_author_id = %s
                ORDER BY a.authorship_order
            """, (author['s2_author_id'],))
        else:
            papers = self.db_manager.fetch_all("""
                SELECT 
                    a.authorship_order,
                    e.semantic_citation_count,
                    e.semantic_year
                FROM authorships a
                JOIN enriched_papers e ON a.paper_id = e.id
                WHERE a.dblp_author_name = %s AND a.s2_author_id IS NULL
                ORDER BY a.authorship_order
            """, (author['dblp_author_name'],))
        
        # Calculate basic statistics
        paper_count = len(papers)
        total_citations = sum(p['semantic_citation_count'] or 0 for p in papers)
        avg_citations = total_citations / paper_count if paper_count > 0 else 0
        
        # Calculate career span
        years = [p['semantic_year'] for p in papers if p['semantic_year']]
        first_year = min(years) if years else None
        latest_year = max(years) if years else None
        career_length = (latest_year - first_year + 1) if first_year and latest_year else 0
        
        # Calculate authorship positions
        first_author_count = sum(1 for p in papers if p['authorship_order'] == 1)
        
        # For last author, we need to count papers where this author is in the last position
        last_author_count = 0
        for paper_id in author['paper_ids'].split(';') if author.get('paper_ids') else []:
            if paper_id:
                max_order = self.db_manager.fetch_one("""
                    SELECT MAX(authorship_order) as max_order
                    FROM authorships
                    WHERE semantic_paper_id = %s
                """, (paper_id,))
                if max_order and max_order['max_order']:
                    paper_last_authors = self.db_manager.fetch_all("""
                        SELECT s2_author_id, dblp_author_name
                        FROM authorships
                        WHERE semantic_paper_id = %s AND authorship_order = %s
                    """, (paper_id, max_order['max_order']))
                    
                    # Check if this author is the last author for this paper
                    for last_auth in paper_last_authors:
                        if has_s2_id and last_auth['s2_author_id'] == author['s2_author_id']:
                            last_author_count += 1
                            break
                        elif not has_s2_id and last_auth['dblp_author_name'] == author['dblp_author_name']:
                            last_author_count += 1
                            break
        
        middle_author_count = paper_count - first_author_count - last_author_count
        
        # Calculate ratios
        first_author_ratio = first_author_count / paper_count if paper_count > 0 else 0
        last_author_ratio = last_author_count / paper_count if paper_count > 0 else 0
        
        # Calculate contribution score (FAR + LAR weighted)
        contribution_score = (first_author_ratio * 0.4 + last_author_ratio * 0.6)
        
        # Calculate rising star score (simplified version - papers in recent years)
        current_year = 2024
        recent_papers = sum(1 for p in papers if p['semantic_year'] and p['semantic_year'] >= current_year - 3)
        rising_star_score = (recent_papers / paper_count) if paper_count > 0 else 0
        
        # Calculate data completeness
        completeness_factors = [
            1 if has_s2_id else 0,  # Has S2 ID
            1 if paper_count > 0 else 0,  # Has papers
            1 if total_citations > 0 else 0,  # Has citations
            1 if career_length > 0 else 0,  # Has career span
        ]
        data_completeness = sum(completeness_factors) / len(completeness_factors)
        
        profile = {
            's2_author_id': author.get('s2_author_id'),
            'dblp_author_name': author['dblp_author_name'],
            's2_author_name': author.get('s2_author_name'),
            'paper_count': paper_count,
            'total_citations': total_citations,
            'avg_citations_per_paper': avg_citations,
            'first_publication_year': first_year,
            'latest_publication_year': latest_year,
            'career_length': career_length,
            'first_author_count': first_author_count,
            'last_author_count': last_author_count,
            'middle_author_count': middle_author_count,
            'first_author_ratio': first_author_ratio,
            'last_author_ratio': last_author_ratio,
            'contribution_score': contribution_score,
            'rising_star_score': rising_star_score,
            'match_confidence': 'high' if has_s2_id else 'low',
            'data_completeness_score': data_completeness
        }
        
        return profile
    
    def _insert_author_profile(self, profile: Dict):
        """Insert author profile into database"""
        insert_sql = """
        INSERT INTO author_profiles (
            s2_author_id, dblp_author_name, s2_author_name,
            paper_count, total_citations, avg_citations_per_paper,
            first_publication_year, latest_publication_year, career_length,
            first_author_count, last_author_count, middle_author_count,
            first_author_ratio, last_author_ratio,
            contribution_score, rising_star_score,
            match_confidence, data_completeness_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        values = (
            profile['s2_author_id'], profile['dblp_author_name'], profile['s2_author_name'],
            profile['paper_count'], profile['total_citations'], profile['avg_citations_per_paper'],
            profile['first_publication_year'], profile['latest_publication_year'], profile['career_length'],
            profile['first_author_count'], profile['last_author_count'], profile['middle_author_count'],
            profile['first_author_ratio'], profile['last_author_ratio'],
            profile['contribution_score'], profile['rising_star_score'],
            profile['match_confidence'], profile['data_completeness_score']
        )
        
        self.db_manager.execute_query(insert_sql, values)
    
    def get_processing_statistics(self) -> Dict:
        """Get comprehensive statistics about the author processing"""
        stats = {}
        
        # Authorships statistics
        authorships_stats = self.db_manager.fetch_one("""
            SELECT 
                COUNT(*) as total_authorships,
                COUNT(CASE WHEN match_confidence = 'matched' THEN 1 END) as matched_authorships,
                COUNT(CASE WHEN match_confidence = 'unmatched' THEN 1 END) as unmatched_authorships
            FROM authorships
        """)
        
        # Author profiles statistics
        profiles_stats = self.db_manager.fetch_one("""
            SELECT 
                COUNT(*) as total_authors,
                COUNT(CASE WHEN s2_author_id IS NOT NULL THEN 1 END) as authors_with_s2_id,
                AVG(paper_count) as avg_papers_per_author,
                AVG(total_citations) as avg_citations_per_author,
                AVG(career_length) as avg_career_length
            FROM author_profiles
        """)
        
        # Matching statistics from the matcher
        match_stats = self.matcher.get_match_statistics()
        
        stats = {
            'authorships': authorships_stats,
            'profiles': profiles_stats,
            'matching': match_stats,
            'timestamp': datetime.now().isoformat()
        }
        
        return stats