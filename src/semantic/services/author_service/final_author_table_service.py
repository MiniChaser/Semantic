#!/usr/bin/env python3
"""
Final Author Table Service
Creates the target database table structure matching the document requirements
"""

import logging
import re
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal

from ...database.connection import DatabaseManager


logger = logging.getLogger(__name__)


class FinalAuthorTableService:
    """Service for creating the final target author table as specified in the document"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def _convert_decimal_to_float(self, obj: Any) -> Any:
        """
        Convert Decimal objects to float for JSON serialization
        
        Args:
            obj: Object to convert
            
        Returns:
            Converted object with Decimal -> float
        """
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_decimal_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimal_to_float(item) for item in obj]
        return obj
    
    def create_final_author_table(self) -> bool:
        """
        Create the final author table matching document requirements (Section 8.1)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating final author table...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS final_author_table (
                id SERIAL PRIMARY KEY,
                
                -- Core identification
                dblp_author VARCHAR(500) NOT NULL,
                note VARCHAR(500),  -- Currently empty as specified
                
                -- External IDs (TODO sections as specified)
                google_scholarid VARCHAR(255),  -- TODO: Google Scholar integration needed
                external_ids_dblp TEXT,         -- DBLP aliases based on 4-digit disambiguation
                
                -- Institution information
                semantic_scholar_affiliations TEXT,  -- TODO: S2 Author API needed (paper-level data lacks affiliations)
                csrankings_affiliation TEXT,         -- TODO: CSRankings data integration needed
                
                -- Publication statistics
                dblp_top_paper_total_paper_captured INTEGER DEFAULT 0,  -- TODO: Top venue definition needed
                dblp_top_paper_last_author_count INTEGER DEFAULT 0,     -- TODO: Top venue definition needed
                first_author_count INTEGER DEFAULT 0,
                semantic_scholar_paper_count INTEGER,    -- COUNT of papers from authorships+enriched_papers
                
                -- Career metrics
                career_length INTEGER DEFAULT 0,
                last_author_percentage INTEGER DEFAULT 0, -- TODO: = (dblp_top_paper_last_author_count / dblp_top_paper_total_paper_captured) * 100
                
                -- Citation and impact metrics
                total_influential_citations INTEGER,     -- Sum of influentialCitationCount from enriched_papers
                semantic_scholar_citation_count INTEGER, -- Sum of semantic_citation_count from enriched_papers
                semantic_scholar_h_index INTEGER,        -- Calculated H-index from author_comprehensive_rankings
                
                -- Naming and identity
                name VARCHAR(500) NOT NULL,
                name_snapshot VARCHAR(500),
                affiliations_snapshot TEXT,  -- Currently empty as specified
                homepage TEXT,               -- TODO: S2 Author API integration needed
                
                -- Internal tracking
                s2_author_id VARCHAR(255),   -- Internal reference to author_profiles
                data_source_notes TEXT,      -- Processing metadata
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.db_manager.execute_query(create_table_sql)
            
            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_final_author_dblp_name ON final_author_table(dblp_author);",
                "CREATE INDEX IF NOT EXISTS idx_final_author_s2_id ON final_author_table(s2_author_id);",
                "CREATE INDEX IF NOT EXISTS idx_final_author_career_length ON final_author_table(career_length DESC);",
                "CREATE INDEX IF NOT EXISTS idx_final_author_citations ON final_author_table(semantic_scholar_citation_count DESC);",
            ]
            
            for index_sql in indexes:
                self.db_manager.execute_query(index_sql)
            
            logger.info("Final author table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create final author table: {e}")
            return False
    
    def populate_final_author_table(self) -> Dict:
        """
        Populate the final author table from processed data
        
        Returns:
            Statistics about the population process
        """
        try:
            logger.info("Populating final author table...")
            
            # Clear existing data
            self.db_manager.execute_query("DELETE FROM final_author_table;")
            
            stats = {
                'total_authors_processed': 0,
                'authors_with_complete_data': 0,
                'authors_with_partial_data': 0,
                'processing_errors': 0
            }
            
            # Get author data directly from author_profiles (simplified approach)
            authors_data = self.db_manager.fetch_all("""
                SELECT 
                    p.s2_author_id,
                    p.dblp_author_name,
                    p.s2_author_name,
                    p.paper_count,
                    p.total_citations,
                    p.career_length,
                    p.first_author_count,
                    p.last_author_count,
                    p.first_author_ratio,
                    p.last_author_ratio
                FROM author_profiles p
                WHERE p.s2_author_id IS NOT NULL
                ORDER BY p.paper_count DESC
            """)
            
            for author in authors_data:
                try:
                    # Extract DBLP aliases using 4-digit disambiguation pattern
                    external_ids_dblp = self._extract_dblp_aliases(author['dblp_author_name'])
                    
                    # Calculate last author percentage as integer
                    last_author_percentage = None  # TODO
                    
                    # Prepare final author record
                    final_author_record = {
                        'dblp_author': author['dblp_author_name'],
                        'note': '',  # Empty as specified
                        'google_scholarid': None,  # TODO: Google Scholar integration
                        'external_ids_dblp': external_ids_dblp,
                        'semantic_scholar_affiliations': None,  # TODO: S2 Author API needed
                        'csrankings_affiliation': None,  # TODO: CSRankings integration
                        'dblp_top_paper_total_paper_captured': 0,  # TODO: Top venue definition
                        'dblp_top_paper_last_author_count': 0,    # TODO: Top venue definition  
                        'first_author_count': author['first_author_count'],
                        'semantic_scholar_paper_count': self._calculate_semantic_scholar_paper_count(author['s2_author_id']),
                        'career_length': author['career_length'],
                        'last_author_percentage': last_author_percentage,
                        'total_influential_citations': self._calculate_total_influential_citations(author['s2_author_id']),
                        'semantic_scholar_citation_count': self._calculate_semantic_scholar_citation_count(author['s2_author_id']),
                        'semantic_scholar_h_index': self._calculate_h_index(author['s2_author_id']),
                        'name': author['dblp_author_name'],
                        'name_snapshot': author['dblp_author_name'],
                        'affiliations_snapshot': '',  # Empty as specified
                        'homepage': None,  # TODO: S2 Author API
                        's2_author_id': author['s2_author_id'],
                        'data_source_notes': self._generate_data_source_notes(author)
                    }
                    
                    self._insert_final_author_record(final_author_record)
                    
                    stats['total_authors_processed'] += 1
                    
                    # Classify data completeness
                    if self._is_complete_data(final_author_record):
                        stats['authors_with_complete_data'] += 1
                    else:
                        stats['authors_with_partial_data'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing author {author.get('s2_author_id')}: {e}")
                    stats['processing_errors'] += 1
                    continue
            
            logger.info("Final author table population completed successfully")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to populate final author table: {e}")
            return {'error': str(e)}
    
    def _calculate_total_influential_citations(self, s2_author_id: str) -> int:
        """
        Calculate total influential citations for an author from enriched_papers
        
        Args:
            s2_author_id: Semantic Scholar author ID
            
        Returns:
            Total influential citation count
        """
        try:
            result = self.db_manager.fetch_one("""
                SELECT SUM(COALESCE(e.influentialcitationcount, 0)) as total_influential
                FROM authorships a
                JOIN enriched_papers e ON a.semantic_paper_id = e.semantic_paper_id
                WHERE a.s2_author_id = %s 
                AND e.semantic_paper_id IS NOT NULL
            """, (s2_author_id,))
            
            return int(result['total_influential']) if result and result['total_influential'] else 0
            
        except Exception as e:
            logger.warning(f"Error calculating influential citations for author {s2_author_id}: {e}")
            return 0
    
    def _calculate_semantic_scholar_paper_count(self, s2_author_id: str) -> int:
        """
        Calculate total paper count for an author from enriched_papers
        
        Args:
            s2_author_id: Semantic Scholar author ID
            
        Returns:
            Total paper count with Semantic Scholar data
        """
        try:
            result = self.db_manager.fetch_one("""
                SELECT COUNT(DISTINCT e.semantic_paper_id) as paper_count
                FROM authorships a
                JOIN enriched_papers e ON a.semantic_paper_id = e.semantic_paper_id
                WHERE a.s2_author_id = %s 
                AND e.semantic_paper_id IS NOT NULL
            """, (s2_author_id,))
            
            return int(result['paper_count']) if result and result['paper_count'] else 0
            
        except Exception as e:
            logger.warning(f"Error calculating paper count for author {s2_author_id}: {e}")
            return 0
    
    def _calculate_semantic_scholar_citation_count(self, s2_author_id: str) -> int:
        """
        Calculate total citation count for an author from enriched_papers
        
        Args:
            s2_author_id: Semantic Scholar author ID
            
        Returns:
            Total citation count across all author's papers
        """
        try:
            result = self.db_manager.fetch_one("""
                SELECT SUM(COALESCE(e.semantic_citation_count, 0)) as total_citations
                FROM authorships a
                JOIN enriched_papers e ON a.semantic_paper_id = e.semantic_paper_id
                WHERE a.s2_author_id = %s 
                AND e.semantic_paper_id IS NOT NULL
            """, (s2_author_id,))
            
            return int(result['total_citations']) if result and result['total_citations'] else 0
            
        except Exception as e:
            logger.warning(f"Error calculating citation count for author {s2_author_id}: {e}")
            return 0
    
    def _calculate_h_index(self, s2_author_id: str) -> int:
        """
        Calculate H-index for an author based on their papers' citation counts
        H-index = the largest number h such that the author has h papers with at least h citations each
        
        Args:
            s2_author_id: Semantic Scholar author ID
            
        Returns:
            H-index value
        """
        try:
            # Get all papers and their citation counts for this author, sorted by citations descending
            papers_citations = self.db_manager.fetch_all("""
                SELECT e.semantic_citation_count
                FROM authorships a
                JOIN enriched_papers e ON a.semantic_paper_id = e.semantic_paper_id
                WHERE a.s2_author_id = %s 
                AND e.semantic_paper_id IS NOT NULL 
                AND e.semantic_citation_count IS NOT NULL
                ORDER BY e.semantic_citation_count DESC
            """, (s2_author_id,))
            
            # Calculate H-index
            h_index = 0
            for i, paper in enumerate(papers_citations, 1):
                citation_count = paper['semantic_citation_count'] or 0
                if citation_count >= i:
                    h_index = i
                else:
                    break
            
            return h_index
            
        except Exception as e:
            logger.warning(f"Error calculating H-index for author {s2_author_id}: {e}")
            return 0
    
    def _extract_dblp_aliases(self, author_name: str) -> str:
        """
        Extract DBLP aliases using 4-digit disambiguation pattern
        Based on analyze_author_ids.py logic as mentioned in document section 9.3
        
        Args:
            author_name: DBLP author name
            
        Returns:
            Semicolon-separated string of aliases
        """
        try:
            # Look for 4-digit disambiguation numbers
            match = re.search(r'\b(\d{4})\b', author_name)
            if match:
                four_digit_number = match.group(1)
                # Remove 4-digit number to get base name
                base_name = re.sub(r'\s*\b\d{4}\b\s*', ' ', author_name).strip()
                
                # Query database for other variants of this base name
                similar_names = self.db_manager.fetch_all("""
                    SELECT DISTINCT dblp_author_name
                    FROM author_profiles
                    WHERE dblp_author_name LIKE %s
                    AND dblp_author_name != %s
                    LIMIT 10
                """, (f'%{base_name}%', author_name))
                
                aliases = [author_name]  # Start with original name
                for name_record in similar_names:
                    aliases.append(name_record['dblp_author_name'])
                
                return ';'.join(aliases[:5])  # Limit to 5 aliases
            else:
                # No 4-digit pattern found, return original name
                return author_name
                
        except Exception as e:
            logger.warning(f"Error extracting aliases for {author_name}: {e}")
            return author_name
    
    def _generate_data_source_notes(self, author: Dict) -> str:
        """Generate metadata about data sources and processing"""
        notes = []
        
        notes.append(f"Papers: {author['paper_count']}")
        notes.append(f"Citations: {author['total_citations']}")
        
        if author['career_length'] and author['career_length'] > 0:
            notes.append(f"Career: {author['career_length']} years")
        
        # Note data completeness issues
        todo_items = []
        todo_items.append("Google Scholar ID needed")
        todo_items.append("S2 Author API metrics needed") 
        todo_items.append("CSRankings affiliation needed")
        todo_items.append("Top venue classification needed")
        
        if todo_items:
            notes.append(f"TODO: {'; '.join(todo_items[:3])}")  # Limit to 3 main TODOs
        
        return ' | '.join(notes)
    
    def _is_complete_data(self, record: Dict) -> bool:
        """Check if author record has complete data (non-TODO fields)"""
        required_complete_fields = [
            'dblp_author', 'first_author_count', 'career_length', 
            'last_author_percentage', 'name'
        ]
        
        return all(
            record.get(field) is not None and record.get(field) != '' 
            for field in required_complete_fields
        )
    
    def _insert_final_author_record(self, record: Dict):
        """Insert final author record into database"""
        insert_sql = """
        INSERT INTO final_author_table (
            dblp_author, note, google_scholarid, external_ids_dblp,
            semantic_scholar_affiliations, csrankings_affiliation,
            dblp_top_paper_total_paper_captured, dblp_top_paper_last_author_count,
            first_author_count, semantic_scholar_paper_count, career_length,
            last_author_percentage, total_influential_citations,
            semantic_scholar_citation_count, semantic_scholar_h_index,
            name, name_snapshot, affiliations_snapshot, homepage,
            s2_author_id, data_source_notes
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        values = (
            record['dblp_author'], record['note'], record['google_scholarid'],
            record['external_ids_dblp'], record['semantic_scholar_affiliations'],
            record['csrankings_affiliation'], record['dblp_top_paper_total_paper_captured'],
            record['dblp_top_paper_last_author_count'], record['first_author_count'],
            record['semantic_scholar_paper_count'], record['career_length'],
            record['last_author_percentage'], record['total_influential_citations'],
            record['semantic_scholar_citation_count'], record['semantic_scholar_h_index'],
            record['name'], record['name_snapshot'], record['affiliations_snapshot'],
            record['homepage'], record['s2_author_id'], record['data_source_notes']
        )
        
        self.db_manager.execute_query(insert_sql, values)
    
    def generate_final_table_report(self, output_path: str) -> bool:
        """
        Generate a comprehensive report about the final author table
        
        Args:
            output_path: Path to save the report JSON
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get comprehensive statistics
            stats = self.db_manager.fetch_one("""
                SELECT 
                    COUNT(*) as total_authors,
                    COUNT(CASE WHEN google_scholarid IS NOT NULL THEN 1 END) as with_google_scholar,
                    COUNT(CASE WHEN external_ids_dblp != dblp_author THEN 1 END) as with_aliases,
                    COUNT(CASE WHEN career_length > 0 THEN 1 END) as with_career_data,
                    COUNT(CASE WHEN first_author_count > 0 THEN 1 END) as with_first_author_papers,
                    COUNT(CASE WHEN semantic_scholar_affiliations IS NOT NULL AND semantic_scholar_affiliations != '' THEN 1 END) as with_affiliations,
                    COUNT(CASE WHEN semantic_scholar_paper_count > 0 THEN 1 END) as with_s2_paper_count,
                    COUNT(CASE WHEN semantic_scholar_citation_count > 0 THEN 1 END) as with_s2_citations,
                    COUNT(CASE WHEN semantic_scholar_h_index IS NOT NULL AND semantic_scholar_h_index > 0 THEN 1 END) as with_h_index,
                    AVG(career_length) as avg_career_length,
                    AVG(first_author_count) as avg_first_author_papers,
                    AVG(last_author_percentage) as avg_last_author_percentage,
                    AVG(semantic_scholar_paper_count) as avg_s2_paper_count,
                    AVG(semantic_scholar_citation_count) as avg_s2_citations,
                    AVG(semantic_scholar_h_index) as avg_h_index
                FROM final_author_table
            """)
            
            # Get top authors by different metrics
            top_by_career = self.db_manager.fetch_all("""
                SELECT dblp_author, career_length, first_author_count
                FROM final_author_table
                WHERE career_length IS NOT NULL
                ORDER BY career_length DESC
                LIMIT 10
            """)
            
            top_by_first_author = self.db_manager.fetch_all("""
                SELECT dblp_author, first_author_count, career_length
                FROM final_author_table
                WHERE first_author_count IS NOT NULL
                ORDER BY first_author_count DESC
                LIMIT 10
            """)
            
            top_by_h_index = self.db_manager.fetch_all("""
                SELECT dblp_author, semantic_scholar_h_index, semantic_scholar_citation_count, career_length
                FROM final_author_table
                WHERE semantic_scholar_h_index IS NOT NULL AND semantic_scholar_h_index > 0
                ORDER BY semantic_scholar_h_index DESC
                LIMIT 10
            """)
            
            # Generate report with Decimal conversion
            report = {
                'generation_timestamp': datetime.now().isoformat(),
                'table_statistics': self._convert_decimal_to_float(dict(stats)) if stats else {},
                'data_completeness': {
                    'implemented_fields': [
                        'dblp_author', 'first_author_count', 'career_length',
                        'last_author_percentage', 'name', 'external_ids_dblp',
                        'total_influential_citations', 'semantic_scholar_paper_count', 
                        'semantic_scholar_citation_count', 'semantic_scholar_h_index'
                    ],
                    'todo_fields': [
                        'google_scholarid', 'semantic_scholar_affiliations', 'csrankings_affiliation',
                        'dblp_top_paper_total_paper_captured', 'dblp_top_paper_last_author_count',
                        'homepage'
                    ]
                },
                'top_authors': {
                    'by_career_length': [self._convert_decimal_to_float(dict(author)) for author in top_by_career],
                    'by_first_author_papers': [self._convert_decimal_to_float(dict(author)) for author in top_by_first_author],
                    'by_h_index': [self._convert_decimal_to_float(dict(author)) for author in top_by_h_index]
                },
                'implementation_notes': {
                    'phase_1_completed': [
                        'Author disambiguation and matching',
                        'Authorships table construction', 
                        'Author profiles creation',
                        'Basic metrics calculation',
                        'H-index calculation and integration',
                        'DBLP aliases extraction',
                        'Final table structure creation'
                    ],
                    'phase_2_requirements': [
                        'Google Scholar API integration',
                        'S2 Author API for detailed metrics',
                        'CSRankings data integration',
                        'Top venue classification system',
                        'InfluentialCitation aggregation',
                        'Homepage and affiliation extraction'
                    ]
                }
            }
            
            # Save report
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Final table report generated: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate final table report: {e}")
            return False
    
    def get_sample_records(self, limit: int = 10) -> List[Dict]:
        """Get sample records from the final author table for verification"""
        try:
            sample_records = self.db_manager.fetch_all(f"""
                SELECT 
                    dblp_author, first_author_count, career_length,
                    last_author_percentage, external_ids_dblp, data_source_notes
                FROM final_author_table
                ORDER BY first_author_count DESC
                LIMIT {limit}
            """)
            
            return [dict(record) for record in sample_records]
            
        except Exception as e:
            logger.error(f"Failed to get sample records: {e}")
            return []