"""
Enriched Paper repository class
Handles database operations for S2-enriched papers
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from ..connection import DatabaseManager
from ..models.enriched_paper import EnrichedPaper
from ..models.paper import DBLP_Paper


class EnrichedPaperRepository:
    """Repository for enriched papers with S2 data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.EnrichedPaperRepository')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def create_tables(self) -> bool:
        """Create enriched papers tables"""
        try:
            from ..schemas.enriched_paper import EnrichedPaperSchema
            schema = EnrichedPaperSchema(self.db)
            return schema.create_table()
        except Exception as e:
            self.logger.error(f"Failed to create tables: {e}")
            return False
    
    def insert_enriched_paper(self, paper: EnrichedPaper) -> bool:
        """Insert or update enriched paper"""
        try:
            paper_dict = paper.to_dict()
            
            # Prepare field lists for SQL
            fields = []
            placeholders = []
            values = []
            
            for field, value in paper_dict.items():
                if field not in ['id'] and value is not None:  # Skip primary key and None values
                    fields.append(field)
                    placeholders.append('%s')
                    values.append(value)
            
            if not fields:
                self.logger.warning("No fields to insert")
                return False
            
            # Create INSERT with ON CONFLICT UPDATE
            fields_str = ', '.join(fields)
            placeholders_str = ', '.join(placeholders)
            
            # Create update clause for conflict resolution
            update_fields = [f"{field} = EXCLUDED.{field}" for field in fields if field not in ['dblp_paper_id', 'created_at']]
            update_clause = ', '.join(update_fields) if update_fields else 'updated_at = CURRENT_TIMESTAMP'
            
            sql = f"""
            INSERT INTO enriched_papers ({fields_str})
            VALUES ({placeholders_str})
            ON CONFLICT (dblp_paper_id) DO UPDATE SET
                {update_clause},
                updated_at = CURRENT_TIMESTAMP
            """
            
            return self.db.execute_query(sql, values)
            
        except Exception as e:
            self.logger.error(f"Failed to insert enriched paper: {e}")
            return False
    
    def batch_insert_enriched_papers(self, papers: List[EnrichedPaper]) -> Tuple[int, int, int]:
        """Batch insert enriched papers"""
        if not papers:
            return 0, 0, 0
        
        inserted = 0
        updated = 0
        errors = 0
        
        try:
            with self.db.get_cursor() as cursor:
                for paper in papers:
                    try:
                        # Check if paper already exists
                        check_sql = "SELECT id FROM enriched_papers WHERE dblp_paper_id = %s"
                        cursor.execute(check_sql, (paper.dblp_paper_id,))
                        existing = cursor.fetchone()
                        
                        paper_dict = paper.to_dict()
                        
                        if existing:
                            # Update existing paper
                            update_fields = []
                            update_values = []
                            
                            for field, value in paper_dict.items():
                                if field not in ['id', 'dblp_paper_id', 'created_at'] and value is not None:
                                    update_fields.append(f"{field} = %s")
                                    update_values.append(value)
                            
                            if update_fields:
                                update_sql = f"""
                                UPDATE enriched_papers SET 
                                    {', '.join(update_fields)},
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE dblp_paper_id = %s
                                """
                                update_values.append(paper.dblp_paper_id)
                                cursor.execute(update_sql, update_values)
                                updated += 1
                        else:
                            # Insert new paper
                            fields = []
                            placeholders = []
                            values = []
                            
                            for field, value in paper_dict.items():
                                if field not in ['id'] and value is not None:
                                    fields.append(field)
                                    placeholders.append('%s')
                                    values.append(value)
                            
                            if fields:
                                insert_sql = f"""
                                INSERT INTO enriched_papers ({', '.join(fields)})
                                VALUES ({', '.join(placeholders)})
                                """
                                cursor.execute(insert_sql, values)
                                inserted += 1
                        
                    except Exception as e:
                        self.logger.error(f"Failed to process paper {paper.dblp_key}: {e}")
                        errors += 1
            
            self.logger.info(f"Batch operation completed: inserted {inserted}, updated {updated}, errors {errors}")
            return inserted, updated, errors
            
        except Exception as e:
            self.logger.error(f"Batch operation failed: {e}")
            return 0, 0, len(papers)
    
    def get_enriched_paper_by_dblp_id(self, dblp_paper_id: int) -> Optional[EnrichedPaper]:
        """Get enriched paper by DBLP paper ID"""
        try:
            sql = "SELECT * FROM enriched_papers WHERE dblp_paper_id = %s"
            result = self.db.fetch_one(sql, (dblp_paper_id,))
            return EnrichedPaper.from_dict(dict(result)) if result else None
        except Exception as e:
            self.logger.error(f"Failed to get enriched paper: {e}")
            return None

    def query_paper_from_dataset(self, title: str, year: int) -> Optional[Dict]:
        """
        Query paper from partitioned dataset_papers table by title and year

        Args:
            title: Paper title to search for
            year: Paper year (used for partition pruning)

        Returns:
            Dictionary with paper data if found, None otherwise

        Note:
            - Queries the specific year partition for performance
            - Returns raw S2 data structure from dataset_papers
            - Uses case-insensitive LIKE matching for better performance
        """
        try:
            if not title or not title.strip():
                return None

            # Normalize title for matching
            title_normalized = title.strip().lower()

            # First try exact case-insensitive match (fastest)
            sql_exact = """
            SELECT
                corpus_id,
                paper_id,
                external_ids,
                title,
                abstract,
                venue,
                year,
                citation_count,
                reference_count,
                influential_citation_count,
                authors,
                fields_of_study,
                publication_types,
                is_open_access,
                open_access_pdf
            FROM dataset_papers
            WHERE year = %s
            AND title = %s
            LIMIT 1
            """

            result = self.db.fetch_one(sql_exact, (year, title_normalized))

            if result:
                # Exact match found - calculate similarity for logging
                from ...services.s2_service.s2_service import S2ValidationService
                validator = S2ValidationService()
                similarity = validator.calculate_title_similarity(title, result['title'])

                result_dict = dict(result)
                result_dict['_title_similarity'] = similarity
                self.logger.info(f"Found dataset match (exact): {title[:50]}... (similarity: {similarity:.3f})")
                return result_dict

            # If no exact match, try fuzzy matching with first few words
            # Extract first 3-5 significant words from title for LIKE query
            words = [w for w in title_normalized.split() if len(w) > 3][:3]
            if not words:
                return None

            # Build LIKE pattern with first few words
            like_pattern = '%' + '%'.join(words[:3]) + '%'

         

            if not results:
                return None

            # Calculate title similarity for fuzzy match candidates
            from ...services.s2_service.s2_service import S2ValidationService
            validator = S2ValidationService()

            best_match = None
            best_similarity = 0.0

            for row in results:
                candidate_title = row['title']
                if not candidate_title:
                    continue

                similarity = validator.calculate_title_similarity(title, candidate_title)

                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = dict(row)

            # Return match only if similarity meets threshold
            if best_match and best_similarity >= 0.70:
                # Add similarity score to result for caller to use
                best_match['_title_similarity'] = best_similarity
                self.logger.info(f"Found dataset match (fuzzy): {title[:50]}... (similarity: {best_similarity:.3f})")
                return best_match

            return None

        except Exception as e:
            self.logger.error(f"Failed to query paper from dataset: {e}")
            return None

    def query_papers_from_dataset_batch(self, papers: List[Tuple[str, int]]) -> Dict[Tuple[str, int], Optional[Dict]]:
        """
        Batch query papers from partitioned dataset_papers table by title and year

        Args:
            papers: List of tuples (title, year) to search for

        Returns:
            Dictionary mapping (title, year) to paper data if found, None otherwise

        Note:
            - Queries the specific year partition for performance
            - Returns raw S2 data structure from dataset_papers
            - Uses case-insensitive exact matching for batch performance
        """
        try:
            if not papers:
                return {}

            # Initialize result dictionary with None values
            results = {(title, year): None for title, year in papers}

            # Filter valid papers
            valid_papers = []
            for title, year in papers:
                if not title or not title.strip():
                    continue
                if not year or year == 0:
                    continue
                valid_papers.append((title.strip(), year))

            if not valid_papers:
                return results

            # Group papers by year for partition pruning
            papers_by_year = {}
            for title, year in valid_papers:
                if year not in papers_by_year:
                    papers_by_year[year] = []
                papers_by_year[year].append(title)

            # Query each year group separately
            for year, titles in papers_by_year.items():
                if not titles:
                    continue

                # Build SQL query with IN clause for exact matching
                placeholders = ', '.join(['%s'] * len(titles))
                sql = f"""
                SELECT
                    corpus_id,
                    paper_id,
                    external_ids,
                    title,
                    abstract,
                    venue,
                    year,
                    citation_count,
                    reference_count,
                    influential_citation_count,
                    authors,
                    fields_of_study,
                    publication_types,
                    is_open_access,
                    open_access_pdf
                FROM dataset_papers
                WHERE year = %s
                AND title IN ({placeholders})
                """

                # Execute query
                batch_results = self.db.fetch_all(sql, (year, placeholders))
                
                self.logger.info(f"Batch query for year {year}: titles： {placeholders} ")



                self.logger.info(f"Batch query for year {year}: found {len(batch_results)} matches for {len(titles)} titles")

                # Create mapping from original title to result
                title_to_result = {}
                for row in batch_results:
                    if row['title']:
                        # Use original title for exact matching
                        title_to_result[row['title']] = dict(row)

                # Match results back to original papers using exact matching
                for original_title, year in valid_papers:
                    # Try exact match (case-sensitive)
                    if original_title in title_to_result:
                        result = title_to_result[original_title]
                        similarity = 1.0  # Exact match
                        result['_title_similarity'] = similarity
                        results[(original_title, year)] = result
                        self.logger.debug(f"Exact match found: '{original_title}' -> '{result['title']}'")
                    else:
                        self.logger.debug(f"No exact match found for: '{original_title}' (year: {year})")

            return results

        except Exception as e:
            self.logger.error(f"Failed to batch query papers from dataset: {e}")
            # Return empty results for all papers on error
            return {(title, year): None for title, year in papers}

    def get_papers_needing_s2_enrichment(self, limit: int = None) -> List[Tuple[int, DBLP_Paper]]:
        """Get DBLP papers that need S2 enrichment (both conditions: new/changed papers without S2 data)"""
        try:
            # Query for DBLP papers that either:
            # 1. Don't exist in enriched_papers table, OR
            # 2. Have been updated more recently than their enrichment
            sql = """
            SELECT dp.*, ep.updated_at as enriched_updated_at
            FROM dblp_papers dp
            LEFT JOIN enriched_papers ep ON dp.id = ep.dblp_paper_id
            WHERE ep.id IS NULL 
               OR dp.update_time > ep.updated_at
               OR ep.semantic_paper_id IS NULL
            ORDER BY dp.year
            """
            
            if limit:
                sql += f" LIMIT {limit}"
            
            results = self.db.fetch_all(sql)
            papers = []
            
            for row in results:
                row_dict = dict(row)
                # Remove enrichment fields for DBLP paper creation
                dblp_data = {k: v for k, v in row_dict.items() 
                           if not k.startswith('enriched_') and k != 'enriched_updated_at'}
                dblp_paper = DBLP_Paper.from_dict(dblp_data)
                
                # 确保 DBLP paper 有 ID
                if dblp_paper.id is None:
                    self.logger.warning(f"DBLP paper {dblp_paper.key} has no ID, skipping")
                    continue
                    
                papers.append((dblp_paper.id, dblp_paper))
            
            self.logger.info(f"Found {len(papers)} papers needing S2 enrichment")
            return papers
            
        except Exception as e:
            self.logger.error(f"Failed to get papers needing enrichment: {e}")
            return []
    
    def get_enrichment_statistics(self) -> Dict[str, Any]:
        """Get enrichment statistics"""
        try:
            stats = {}
            
            # Total counts
            total_dblp = self.db.fetch_one("SELECT COUNT(*) as count FROM dblp_papers")
            total_enriched = self.db.fetch_one("SELECT COUNT(*) as count FROM enriched_papers")
            
            stats['total_dblp_papers'] = total_dblp['count'] if total_dblp else 0
            stats['total_enriched_papers'] = total_enriched['count'] if total_enriched else 0
            
            # Enrichment coverage
            if stats['total_dblp_papers'] > 0:
                stats['enrichment_coverage'] = (stats['total_enriched_papers'] / stats['total_dblp_papers']) * 100
            else:
                stats['enrichment_coverage'] = 0
            
            # Validation tier distribution
            tier_stats = self.db.fetch_all("""
                SELECT validation_tier, COUNT(*) as count
                FROM enriched_papers
                WHERE validation_tier IS NOT NULL
                GROUP BY validation_tier
                ORDER BY count DESC
            """)
            stats['validation_tiers'] = {row['validation_tier']: row['count'] for row in tier_stats}
            
            # S2 match success rate
            s2_matched = self.db.fetch_one("""
                SELECT COUNT(*) as count FROM enriched_papers 
                WHERE semantic_paper_id IS NOT NULL
            """)
            stats['s2_matched'] = s2_matched['count'] if s2_matched else 0
            
            if stats['total_enriched_papers'] > 0:
                stats['s2_match_rate'] = (stats['s2_matched'] / stats['total_enriched_papers']) * 100
            else:
                stats['s2_match_rate'] = 0
            
            # Data completeness statistics
            completeness_stats = self.db.fetch_all("""
                SELECT 
                    CASE 
                        WHEN data_completeness_score >= 0.8 THEN 'high'
                        WHEN data_completeness_score >= 0.5 THEN 'medium'
                        ELSE 'low'
                    END as completeness_category,
                    COUNT(*) as count
                FROM enriched_papers
                WHERE data_completeness_score IS NOT NULL
                GROUP BY completeness_category
            """)
            stats['completeness_distribution'] = {row['completeness_category']: row['count'] for row in completeness_stats}
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get enrichment statistics: {e}")
            return {}
    
    def record_s2_processing_meta(self, process_type: str, status: str,
                                records_processed: int = 0, records_inserted: int = 0,
                                records_updated: int = 0, records_tier1: int = 0,
                                records_tier2: int = 0, records_tier3: int = 0,
                                api_calls_made: int = 0, error_message: str = None,
                                execution_duration: int = None) -> bool:
        """Record S2 processing metadata"""
        try:
            sql = """
            INSERT INTO s2_processing_meta
            (process_type, last_run_time, status, records_processed,
             records_inserted, records_updated, records_tier1, records_tier2, records_tier3,
             api_calls_made, error_message, execution_duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                process_type, datetime.now(), status, records_processed,
                records_inserted, records_updated, records_tier1, records_tier2, records_tier3,
                api_calls_made, error_message, execution_duration
            )

            return self.db.execute_query(sql, params)

        except Exception as e:
            self.logger.error(f"Failed to record S2 processing metadata: {e}")
            return False
    
    def get_last_successful_s2_run(self, process_type: str = 's2_enrichment') -> Optional[datetime]:
        """Get last successful S2 run time"""
        try:
            sql = """
            SELECT last_run_time 
            FROM s2_processing_meta 
            WHERE process_type = %s AND status = 'success'
            ORDER BY last_run_time DESC 
            LIMIT 1
            """
            
            result = self.db.fetch_one(sql, (process_type,))
            return result['last_run_time'] if result else None
            
        except Exception as e:
            self.logger.error(f"Failed to get last successful S2 run time: {e}")
            return None
    
    def export_to_csv(self, output_path: str, include_all_fields: bool = True) -> bool:
        """Export enriched papers to CSV"""
        try:
            import pandas as pd
            import os
            
            self.logger.info(f"Exporting enriched papers to CSV: {output_path}")
            
            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if include_all_fields:
                # Export all 54 fields
                query = "SELECT * FROM enriched_papers ORDER BY validation_tier, match_confidence DESC, dblp_paper_id"
            else:
                # Export essential fields only
                query = """
                SELECT dblp_key, dblp_title, semantic_title, dblp_year, semantic_year,
                       dblp_venue, semantic_venue, semantic_abstract, doi, acl_id,
                       semantic_citation_count, validation_tier, match_confidence,
                       data_completeness_score, match_method
                FROM enriched_papers 
                ORDER BY validation_tier, match_confidence DESC, dblp_paper_id
                """
            
            df = pd.read_sql_query(query, self.db.config.get_connection_string())
            df.to_csv(output_path, index=False)
            
            self.logger.info(f"CSV export completed: {len(df)} enriched papers")
            return True
            
        except Exception as e:
            self.logger.error(f"CSV export failed: {e}")
            return False