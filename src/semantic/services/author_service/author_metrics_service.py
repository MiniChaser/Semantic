#!/usr/bin/env python3
"""
Author Metrics Service
Calculates advanced author metrics including collaboration networks,
rising star indicators, and comprehensive author rankings
"""

import logging
from typing import Dict, List
from datetime import datetime

from ...database.connection import DatabaseManager


logger = logging.getLogger(__name__)


class AuthorMetricsService:
    """Service for calculating advanced author metrics and rankings"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def create_author_metrics_tables(self) -> bool:
        """
        Create tables for storing advanced author metrics
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating author metrics tables...")
            
            # Author collaboration network table
            collaboration_table_sql = """
            CREATE TABLE IF NOT EXISTS author_collaboration_metrics (
                id SERIAL PRIMARY KEY,
                s2_author_id VARCHAR(255) NOT NULL,
                author_name TEXT NOT NULL,
                total_collaborators INTEGER DEFAULT 0,
                unique_collaborators INTEGER DEFAULT 0,
                avg_collaborators_per_paper NUMERIC DEFAULT 0,
                collaboration_ratio NUMERIC DEFAULT 0,
                network_centrality_score NUMERIC DEFAULT 0,
                cross_institution_collaborations INTEGER DEFAULT 0,
                collaboration_diversity_score NUMERIC DEFAULT 0,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (s2_author_id) REFERENCES author_profiles(s2_author_id) ON DELETE CASCADE
            );
            """
            
            # Rising star metrics table
            rising_star_table_sql = """
            CREATE TABLE IF NOT EXISTS author_rising_star_metrics (
                id SERIAL PRIMARY KEY,
                s2_author_id VARCHAR(255) NOT NULL,
                author_name TEXT NOT NULL,
                recent_papers_count INTEGER DEFAULT 0,
                recent_citations_count INTEGER DEFAULT 0,
                recent_paper_ratio NUMERIC DEFAULT 0,
                recent_citation_ratio NUMERIC DEFAULT 0,
                citation_velocity NUMERIC DEFAULT 0,
                impact_acceleration NUMERIC DEFAULT 0,
                rising_star_score NUMERIC DEFAULT 0,
                career_stage VARCHAR(50),
                potential_rating VARCHAR(50),
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (s2_author_id) REFERENCES author_profiles(s2_author_id) ON DELETE CASCADE
            );
            """
            
            # Comprehensive author rankings table
            rankings_table_sql = """
            CREATE TABLE IF NOT EXISTS author_comprehensive_rankings (
                id SERIAL PRIMARY KEY,
                s2_author_id VARCHAR(255) NOT NULL,
                author_name TEXT NOT NULL,
                
                -- Core productivity metrics
                total_papers INTEGER DEFAULT 0,
                total_citations INTEGER DEFAULT 0,
                avg_citations_per_paper NUMERIC DEFAULT 0,
                h_index_calculated INTEGER DEFAULT 0,
                
                -- Leadership metrics
                first_author_papers INTEGER DEFAULT 0,
                last_author_papers INTEGER DEFAULT 0,
                first_author_percentage NUMERIC DEFAULT 0,
                last_author_percentage NUMERIC DEFAULT 0,
                leadership_score NUMERIC DEFAULT 0,
                
                -- Impact metrics
                highly_cited_papers INTEGER DEFAULT 0,  -- Citations > 50
                top_cited_papers INTEGER DEFAULT 0,     -- Citations > 100
                influential_citations INTEGER DEFAULT 0,  -- TODO: Add influentialCitationCount sum
                impact_score NUMERIC DEFAULT 0,
                
                -- Career metrics
                career_length INTEGER DEFAULT 0,
                publications_per_year NUMERIC DEFAULT 0,
                career_consistency_score NUMERIC DEFAULT 0,
                
                -- Collaboration metrics
                total_collaborators INTEGER DEFAULT 0,
                collaboration_breadth NUMERIC DEFAULT 0,
                
                -- Rising star metrics
                recent_activity_score NUMERIC DEFAULT 0,
                growth_trajectory NUMERIC DEFAULT 0,
                
                -- Overall composite scores
                productivity_rank INTEGER,
                impact_rank INTEGER, 
                leadership_rank INTEGER,
                collaboration_rank INTEGER,
                overall_composite_score NUMERIC DEFAULT 0,
                overall_rank INTEGER,
                
                -- Ranking metadata
                ranking_date DATE DEFAULT CURRENT_DATE,
                data_quality_score NUMERIC DEFAULT 0,
                
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (s2_author_id) REFERENCES author_profiles(s2_author_id) ON DELETE CASCADE
            );
            """
            
            tables = [
                collaboration_table_sql,
                rising_star_table_sql, 
                rankings_table_sql
            ]
            
            for table_sql in tables:
                self.db_manager.execute_query(table_sql)
            
            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_collab_metrics_author_id ON author_collaboration_metrics(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_rising_star_author_id ON author_rising_star_metrics(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_rankings_author_id ON author_comprehensive_rankings(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_rankings_overall_rank ON author_comprehensive_rankings(overall_rank);",
                "CREATE INDEX IF NOT EXISTS idx_rankings_productivity ON author_comprehensive_rankings(productivity_rank);",
                "CREATE INDEX IF NOT EXISTS idx_rankings_impact ON author_comprehensive_rankings(impact_rank);",
            ]
            
            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)
            
            logger.info("Author metrics tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create author metrics tables: {e}")
            return False
    
    def calculate_collaboration_metrics(self) -> Dict:
        """Calculate collaboration network metrics for all authors"""
        try:
            logger.info("Calculating collaboration network metrics...")
            
            # Clear existing data
            self.db_manager.execute_query("DELETE FROM author_collaboration_metrics;")
            
            stats = {'processed_authors': 0, 'errors': 0}
            
            # Get all authors with S2 IDs
            authors = self.db_manager.fetch_all("""
                SELECT s2_author_id, dblp_author_name, s2_author_name, paper_count
                FROM author_profiles 
                WHERE s2_author_id IS NOT NULL
                ORDER BY paper_count DESC
            """)
            
            for author in authors:
                try:
                    metrics = self._calculate_author_collaboration_metrics(author['s2_author_id'])
                    metrics['s2_author_id'] = author['s2_author_id']
                    metrics['author_name'] = author['s2_author_name'] or author['dblp_author_name']
                    
                    self._insert_collaboration_metrics(metrics)
                    stats['processed_authors'] += 1
                    
                except Exception as e:
                    logger.error(f"Error calculating collaboration for author {author['s2_author_id']}: {e}")
                    stats['errors'] += 1
            
            logger.info("Collaboration metrics calculation completed")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to calculate collaboration metrics: {e}")
            return {'error': str(e)}
    
    def _calculate_author_collaboration_metrics(self, s2_author_id: str) -> Dict:
        """Calculate collaboration metrics for a single author"""
        
        # Get all collaborators for this author
        collaborations = self.db_manager.fetch_all("""
            WITH author_papers AS (
                SELECT DISTINCT semantic_paper_id
                FROM authorships
                WHERE s2_author_id = %s AND semantic_paper_id IS NOT NULL
            ),
            all_coauthors AS (
                SELECT DISTINCT a.s2_author_id as collaborator_id, a.dblp_author_name
                FROM authorships a
                JOIN author_papers ap ON a.semantic_paper_id = ap.semantic_paper_id
                WHERE a.s2_author_id != %s AND a.s2_author_id IS NOT NULL
            )
            SELECT 
                collaborator_id,
                ac.dblp_author_name,
                COUNT(*) as collaboration_count
            FROM all_coauthors ac
            JOIN authorships a ON ac.collaborator_id = a.s2_author_id
            JOIN author_papers ap ON a.semantic_paper_id = ap.semantic_paper_id
            GROUP BY collaborator_id, ac.dblp_author_name
            ORDER BY collaboration_count DESC
        """, (s2_author_id, s2_author_id))
        
        # Get author's paper count
        author_papers = self.db_manager.fetch_one("""
            SELECT COUNT(DISTINCT semantic_paper_id) as paper_count
            FROM authorships
            WHERE s2_author_id = %s AND semantic_paper_id IS NOT NULL
        """, (s2_author_id,))['paper_count']
        
        # Calculate metrics
        total_collaborations = sum(c['collaboration_count'] for c in collaborations)
        unique_collaborators = len(collaborations)
        avg_collaborators_per_paper = total_collaborations / author_papers if author_papers > 0 else 0
        collaboration_ratio = unique_collaborators / author_papers if author_papers > 0 else 0
        
        # Calculate network centrality (simplified - based on collaboration frequency)
        if collaborations:
            max_collab_count = max(c['collaboration_count'] for c in collaborations)
            network_centrality = sum(
                c['collaboration_count'] / max_collab_count for c in collaborations
            ) / len(collaborations)
        else:
            network_centrality = 0
        
        # Calculate collaboration diversity (variety of collaborators)
        diversity_score = min(unique_collaborators / 10, 1.0) if unique_collaborators > 0 else 0
        
        return {
            'total_collaborators': total_collaborations,
            'unique_collaborators': unique_collaborators,
            'avg_collaborators_per_paper': avg_collaborators_per_paper,
            'collaboration_ratio': collaboration_ratio,
            'network_centrality_score': network_centrality,
            'cross_institution_collaborations': 0,  # TODO: Calculate when institution data available
            'collaboration_diversity_score': diversity_score
        }
    
    def _insert_collaboration_metrics(self, metrics: Dict):
        """Insert collaboration metrics into database"""
        insert_sql = """
        INSERT INTO author_collaboration_metrics (
            s2_author_id, author_name, total_collaborators, unique_collaborators,
            avg_collaborators_per_paper, collaboration_ratio, network_centrality_score,
            cross_institution_collaborations, collaboration_diversity_score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            metrics['s2_author_id'], metrics['author_name'],
            metrics['total_collaborators'], metrics['unique_collaborators'],
            metrics['avg_collaborators_per_paper'], metrics['collaboration_ratio'],
            metrics['network_centrality_score'], metrics['cross_institution_collaborations'],
            metrics['collaboration_diversity_score']
        )
        
        self.db_manager.execute_query(insert_sql, values)
    
    def calculate_rising_star_metrics(self, recent_years: int = 3) -> Dict:
        """Calculate rising star metrics for all authors"""
        try:
            logger.info(f"Calculating rising star metrics (recent {recent_years} years)...")
            
            # Clear existing data
            self.db_manager.execute_query("DELETE FROM author_rising_star_metrics;")
            
            current_year = datetime.now().year
            recent_threshold = current_year - recent_years
            
            stats = {'processed_authors': 0, 'errors': 0}
            
            # Get authors with career data
            authors = self.db_manager.fetch_all("""
                SELECT s2_author_id, dblp_author_name, s2_author_name,
                       paper_count, total_citations, career_length,
                       first_publication_year, latest_publication_year
                FROM author_profiles 
                WHERE s2_author_id IS NOT NULL AND paper_count > 0
                ORDER BY paper_count DESC
            """)
            
            for author in authors:
                try:
                    metrics = self._calculate_author_rising_star_metrics(
                        author['s2_author_id'], recent_threshold, author
                    )
                    metrics['s2_author_id'] = author['s2_author_id']
                    metrics['author_name'] = author['s2_author_name'] or author['dblp_author_name']
                    
                    self._insert_rising_star_metrics(metrics)
                    stats['processed_authors'] += 1
                    
                except Exception as e:
                    logger.error(f"Error calculating rising star for author {author['s2_author_id']}: {e}")
                    stats['errors'] += 1
            
            logger.info("Rising star metrics calculation completed")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to calculate rising star metrics: {e}")
            return {'error': str(e)}
    
    def _calculate_author_rising_star_metrics(self, s2_author_id: str, recent_threshold: int, author_profile: Dict) -> Dict:
        """Calculate rising star metrics for a single author"""
        
        # Get recent publications data
        recent_papers = self.db_manager.fetch_all("""
            SELECT e.semantic_year, e.semantic_citation_count
            FROM authorships a
            JOIN enriched_papers e ON a.paper_id = e.id
            WHERE a.s2_author_id = %s AND e.semantic_year >= %s
        """, (s2_author_id, recent_threshold))
        
        # Get all publications for comparison
        all_papers = self.db_manager.fetch_all("""
            SELECT e.semantic_year, e.semantic_citation_count
            FROM authorships a
            JOIN enriched_papers e ON a.paper_id = e.id
            WHERE a.s2_author_id = %s AND e.semantic_year IS NOT NULL
        """, (s2_author_id,))
        
        # Calculate basic stats
        recent_papers_count = len(recent_papers)
        total_papers_count = len(all_papers)
        recent_citations = sum(p['semantic_citation_count'] or 0 for p in recent_papers)
        total_citations = sum(p['semantic_citation_count'] or 0 for p in all_papers)
        
        # Calculate ratios
        recent_paper_ratio = recent_papers_count / total_papers_count if total_papers_count > 0 else 0
        recent_citation_ratio = recent_citations / total_citations if total_citations > 0 else 0
        
        # Calculate citation velocity (citations per year in recent period)
        citation_velocity = recent_citations / max(3, 1) if recent_papers_count > 0 else 0
        
        # Calculate impact acceleration (recent vs career average)
        career_avg_citations = total_citations / total_papers_count if total_papers_count > 0 else 0
        recent_avg_citations = recent_citations / recent_papers_count if recent_papers_count > 0 else 0
        impact_acceleration = recent_avg_citations / career_avg_citations if career_avg_citations > 0 else 0
        
        # Calculate overall rising star score
        factors = [
            recent_paper_ratio * 0.3,      # Recent productivity
            recent_citation_ratio * 0.3,   # Recent impact
            min(citation_velocity / 10, 1) * 0.2,  # Citation velocity (normalized)
            min(impact_acceleration, 2) * 0.2      # Impact acceleration (capped at 2x)
        ]
        rising_star_score = sum(factors)
        
        # Determine career stage
        career_length = author_profile.get('career_length', 0)
        if career_length <= 3:
            career_stage = 'early'
        elif career_length <= 8:
            career_stage = 'mid'
        else:
            career_stage = 'senior'
        
        # Determine potential rating
        if rising_star_score > 0.7:
            potential_rating = 'high'
        elif rising_star_score > 0.4:
            potential_rating = 'medium'
        else:
            potential_rating = 'low'
        
        return {
            'recent_papers_count': recent_papers_count,
            'recent_citations_count': recent_citations,
            'recent_paper_ratio': recent_paper_ratio,
            'recent_citation_ratio': recent_citation_ratio,
            'citation_velocity': citation_velocity,
            'impact_acceleration': impact_acceleration,
            'rising_star_score': rising_star_score,
            'career_stage': career_stage,
            'potential_rating': potential_rating
        }
    
    def _insert_rising_star_metrics(self, metrics: Dict):
        """Insert rising star metrics into database"""
        insert_sql = """
        INSERT INTO author_rising_star_metrics (
            s2_author_id, author_name, recent_papers_count, recent_citations_count,
            recent_paper_ratio, recent_citation_ratio, citation_velocity,
            impact_acceleration, rising_star_score, career_stage, potential_rating
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            metrics['s2_author_id'], metrics['author_name'],
            metrics['recent_papers_count'], metrics['recent_citations_count'],
            metrics['recent_paper_ratio'], metrics['recent_citation_ratio'],
            metrics['citation_velocity'], metrics['impact_acceleration'],
            metrics['rising_star_score'], metrics['career_stage'], metrics['potential_rating']
        )
        
        self.db_manager.execute_query(insert_sql, values)
    
    def calculate_comprehensive_rankings(self) -> Dict:
        """Calculate comprehensive author rankings combining all metrics"""
        try:
            logger.info("Calculating comprehensive author rankings...")
            
            # Clear existing data
            self.db_manager.execute_query("DELETE FROM author_comprehensive_rankings;")
            
            stats = {'processed_authors': 0, 'errors': 0}
            
            # Get all authors with their base metrics
            authors = self.db_manager.fetch_all("""
                SELECT 
                    p.s2_author_id, p.dblp_author_name, p.s2_author_name,
                    p.paper_count, p.total_citations, p.avg_citations_per_paper,
                    p.career_length, p.first_author_count, p.last_author_count,
                    p.first_author_ratio, p.last_author_ratio,
                    c.unique_collaborators, c.collaboration_ratio,
                    r.rising_star_score
                FROM author_profiles p
                LEFT JOIN author_collaboration_metrics c ON p.s2_author_id = c.s2_author_id
                LEFT JOIN author_rising_star_metrics r ON p.s2_author_id = r.s2_author_id
                WHERE p.s2_author_id IS NOT NULL AND p.paper_count > 0
                ORDER BY p.paper_count DESC
            """)
            
            # Calculate comprehensive metrics for each author
            ranking_data = []
            
            for author in authors:
                try:
                    comprehensive_metrics = self._calculate_comprehensive_metrics(author)
                    ranking_data.append(comprehensive_metrics)
                    
                except Exception as e:
                    logger.error(f"Error calculating comprehensive metrics for {author['s2_author_id']}: {e}")
                    stats['errors'] += 1
            
            # Calculate rankings
            self._calculate_and_insert_rankings(ranking_data)
            stats['processed_authors'] = len(ranking_data)
            
            logger.info("Comprehensive rankings calculation completed")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to calculate comprehensive rankings: {e}")
            return {'error': str(e)}
    
    def _calculate_comprehensive_metrics(self, author: Dict) -> Dict:
        """Calculate comprehensive metrics for a single author"""
        
        # Calculate H-index (simplified version)
        papers_citations = self.db_manager.fetch_all("""
            SELECT e.semantic_citation_count
            FROM authorships a
            JOIN enriched_papers e ON a.paper_id = e.id
            WHERE a.s2_author_id = %s AND e.semantic_citation_count IS NOT NULL
            ORDER BY e.semantic_citation_count DESC
        """, (author['s2_author_id'],))
        
        h_index = 0
        for i, paper in enumerate(papers_citations, 1):
            if paper['semantic_citation_count'] >= i:
                h_index = i
            else:
                break
        
        # Count highly cited papers
        highly_cited_papers = sum(1 for p in papers_citations if p['semantic_citation_count'] >= 50)
        top_cited_papers = sum(1 for p in papers_citations if p['semantic_citation_count'] >= 100)
        
        # Calculate impact score
        citation_factor = min(author['total_citations'] / 1000, 1.0)  # Normalize to 0-1
        h_index_factor = min(h_index / 50, 1.0)  # Normalize to 0-1
        highly_cited_factor = min(highly_cited_papers / 10, 1.0)  # Normalize to 0-1
        impact_score = (citation_factor * 0.4 + h_index_factor * 0.4 + highly_cited_factor * 0.2)
        
        # Calculate leadership score
        leadership_score = (author['first_author_ratio'] * 0.4 + author['last_author_ratio'] * 0.6)
        
        # Calculate productivity metrics
        publications_per_year = author['paper_count'] / max(author['career_length'], 1)
        
        # Calculate career consistency (simplified)
        career_consistency = min(publications_per_year / 2, 1.0)  # Normalize based on 2 papers/year
        
        # Calculate collaboration metrics
        collaboration_breadth = author.get('unique_collaborators', 0) / max(author['paper_count'], 1)
        
        # Calculate overall composite score
        productivity_component = min(author['paper_count'] / 50, 1.0) * 0.25
        impact_component = impact_score * 0.35
        leadership_component = leadership_score * 0.20
        collaboration_component = min(collaboration_breadth, 1.0) * 0.10
        rising_star_component = float(author.get('rising_star_score', 0) or 0) * 0.10
        
        overall_composite_score = (
            productivity_component + impact_component + leadership_component + 
            collaboration_component + rising_star_component
        )
        
        return {
            's2_author_id': author['s2_author_id'],
            'author_name': author['s2_author_name'] or author['dblp_author_name'],
            'total_papers': author['paper_count'],
            'total_citations': author['total_citations'],
            'avg_citations_per_paper': author['avg_citations_per_paper'] if author['avg_citations_per_paper'] is not None else 0.0,
            'h_index_calculated': h_index,
            'first_author_papers': author['first_author_count'],
            'last_author_papers': author['last_author_count'],
            'first_author_percentage': author['first_author_ratio'] * 100,
            'last_author_percentage': author['last_author_ratio'] * 100,
            'leadership_score': leadership_score,
            'highly_cited_papers': highly_cited_papers,
            'top_cited_papers': top_cited_papers,
            'influential_citations': 0,  # TODO: Sum influentialCitationCount
            'impact_score': impact_score,
            'career_length': author['career_length'],
            'publications_per_year': publications_per_year,
            'career_consistency_score': career_consistency,
            'total_collaborators': author.get('unique_collaborators', 0) or 0,
            'collaboration_breadth': collaboration_breadth,
            'recent_activity_score': float(author.get('rising_star_score', 0) or 0),
            'growth_trajectory': float(author.get('rising_star_score', 0) or 0),
            'overall_composite_score': overall_composite_score,
            'data_quality_score': 1.0 if author['s2_author_id'] else 0.5
        }
    
    def _calculate_and_insert_rankings(self, ranking_data: List[Dict]):
        """Calculate rankings and insert all comprehensive ranking data"""
        
        # Sort by different criteria and assign ranks
        ranking_criteria = [
            ('total_papers', 'productivity_rank'),
            ('impact_score', 'impact_rank'), 
            ('leadership_score', 'leadership_rank'),
            ('collaboration_breadth', 'collaboration_rank'),
            ('overall_composite_score', 'overall_rank')
        ]
        
        for score_field, rank_field in ranking_criteria:
            sorted_authors = sorted(ranking_data, key=lambda x: x[score_field], reverse=True)
            for i, author in enumerate(sorted_authors, 1):
                author[rank_field] = i
        
        # Batch insert all ranking data
        insert_sql = """
        INSERT INTO author_comprehensive_rankings (
            s2_author_id, author_name, total_papers, total_citations,
            avg_citations_per_paper, h_index_calculated, first_author_papers,
            last_author_papers, first_author_percentage, last_author_percentage,
            leadership_score, highly_cited_papers, top_cited_papers,
            influential_citations, impact_score, career_length, publications_per_year,
            career_consistency_score, total_collaborators, collaboration_breadth,
            recent_activity_score, growth_trajectory, productivity_rank, impact_rank,
            leadership_rank, collaboration_rank, overall_composite_score, overall_rank,
            data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        values = [
            (
                author['s2_author_id'], author['author_name'], author['total_papers'],
                author['total_citations'], author['avg_citations_per_paper'],
                author['h_index_calculated'], author['first_author_papers'],
                author['last_author_papers'], author['first_author_percentage'],
                author['last_author_percentage'], author['leadership_score'],
                author['highly_cited_papers'], author['top_cited_papers'],
                author['influential_citations'], author['impact_score'],
                author['career_length'], author['publications_per_year'],
                author['career_consistency_score'], author['total_collaborators'],
                author['collaboration_breadth'], author['recent_activity_score'],
                author['growth_trajectory'], author['productivity_rank'],
                author['impact_rank'], author['leadership_rank'],
                author['collaboration_rank'], author['overall_composite_score'],
                author['overall_rank'], author['data_quality_score']
            )
            for author in ranking_data
        ]
        
        self.db_manager.execute_batch_query(insert_sql, values)
    
    def get_metrics_statistics(self) -> Dict:
        """Get comprehensive statistics about calculated metrics"""
        stats = {}
        
        # Collaboration metrics stats
        collab_stats = self.db_manager.fetch_one("""
            SELECT 
                COUNT(*) as total_authors,
                AVG(unique_collaborators) as avg_collaborators,
                AVG(collaboration_ratio) as avg_collaboration_ratio,
                AVG(network_centrality_score) as avg_centrality
            FROM author_collaboration_metrics
        """)
        
        # Rising star stats
        rising_star_stats = self.db_manager.fetch_one("""
            SELECT 
                COUNT(*) as total_authors,
                AVG(rising_star_score) as avg_rising_star_score,
                COUNT(CASE WHEN potential_rating = 'high' THEN 1 END) as high_potential_authors,
                COUNT(CASE WHEN career_stage = 'early' THEN 1 END) as early_career_authors
            FROM author_rising_star_metrics
        """)
        
        # Rankings stats
        rankings_stats = self.db_manager.fetch_one("""
            SELECT 
                COUNT(*) as total_ranked_authors,
                AVG(overall_composite_score) as avg_composite_score,
                AVG(h_index_calculated) as avg_h_index,
                AVG(impact_score) as avg_impact_score
            FROM author_comprehensive_rankings
        """)
        
        stats = {
            'collaboration_metrics': collab_stats,
            'rising_star_metrics': rising_star_stats,
            'comprehensive_rankings': rankings_stats,
            'timestamp': datetime.now().isoformat()
        }
        
        return stats