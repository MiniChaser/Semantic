"""
Semantic Scholar Enrichment Service
Main service for enriching papers with S2 data using 3-tier validation strategy
"""

import os
import logging
import time
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

from ..database.connection import DatabaseManager, get_db_manager
from ..database.models.paper import DBLP_Paper
from ..database.models.enriched_paper import EnrichedPaper
from ..database.repositories.enriched_paper import EnrichedPaperRepository
from ..services.s2_service import SemanticScholarAPI, S2DataParser, S2ValidationService
from ..utils.config import AppConfig


class S2EnrichmentService:
    """Service for enriching DBLP papers with Semantic Scholar data"""
    
    def __init__(self, config: AppConfig, db_manager: DatabaseManager = None, api_key: str = None):
        self.config = config
        self.db_manager = db_manager or get_db_manager()
        self.enriched_repo = EnrichedPaperRepository(self.db_manager)
        
        # Load S2 API key from environment if not provided
        self.api_key = api_key or os.getenv('SEMANTIC_SCHOLAR_API_KEY')
        self.s2_api = SemanticScholarAPI(self.api_key)
        self.s2_parser = S2DataParser()
        self.validator = S2ValidationService()
        
        self.logger = self._setup_logger()
        
        # Processing statistics
        self.stats = {
            'papers_processed': 0,
            'papers_inserted': 0,
            'papers_updated': 0,
            'tier1_matches': 0,
            'tier2_matches': 0,
            'tier3_no_matches': 0,
            'api_calls_made': 0,
            'errors': 0
        }
        
        self.start_time = None
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.S2EnrichmentService')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def setup_database(self) -> bool:
        """Setup database tables for enriched papers"""
        try:
            self.logger.info("Setting up S2 enrichment database tables...")
            return self.enriched_repo.create_tables()
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}")
            return False
    
    def enrich_papers(self, limit: int = None) -> bool:
        """Main method to enrich papers with S2 data - processes each paper individually"""
        self.start_time = datetime.now()
        self.logger.info(f"Starting S2 enrichment process at {self.start_time}")
        
        try:
            # Reset statistics
            self.stats = {k: 0 for k in self.stats.keys()}
            
            # Step 1: Setup database
            if not self.setup_database():
                raise Exception("Database setup failed")
            
            # Step 2: Get papers needing enrichment
            self.logger.info("Getting papers that need S2 enrichment...")
            papers_to_enrich = self.enriched_repo.get_papers_needing_s2_enrichment(limit=limit)
            
            if not papers_to_enrich:
                self.logger.info("No papers need S2 enrichment")
                return True
            
            self.logger.info(f"Found {len(papers_to_enrich)} papers to enrich")
            self.logger.info("Processing papers individually...")
            
            # Step 3: Process each paper individually
            for i, (dblp_id, dblp_paper) in enumerate(papers_to_enrich, 1):
                try:
                    # Log progress
                    if i % 10 == 0 or i == 1 or i == len(papers_to_enrich):
                        self.logger.info(f"Processing paper {i}/{len(papers_to_enrich)}: {dblp_paper.title[:50]}...")
                    
                    # Process single paper
                    success = self._process_single_paper(dblp_id, dblp_paper)
                    
                    if success:
                        self.stats['papers_processed'] += 1
                    else:
                        self.stats['errors'] += 1
                        
                    # Small delay to avoid overwhelming the API
                    if i % 100 == 0:  # Every 100 papers, take a longer break
                        time.sleep(2)
                    elif i % 10 == 0:  # Every 10 papers, take a short break
                        time.sleep(0.5)
                    else:
                        time.sleep(0.1)  # Small delay between papers
                        
                except Exception as e:
                    self.logger.error(f"Error processing paper {dblp_paper.key}: {e}")
                    self.stats['errors'] += 1
                    continue
            
            # Step 4: Record processing metadata
            self._record_processing_metadata('success')
            
            # Step 5: Generate report
            self._generate_enrichment_report()
            
            self.logger.info(f"S2 enrichment process completed successfully. Processed {self.stats['papers_processed']} papers with {self.stats['errors']} errors.")
            return True
            
        except Exception as e:
            self.logger.error(f"S2 enrichment process failed: {e}")
            self._record_processing_metadata('failed', str(e))
            return False
    
    def _process_single_paper(self, dblp_id: int, dblp_paper: DBLP_Paper) -> bool:
        """Process a single paper through the 3-tier enrichment process"""
        try:
            # Step 1: Try Tier 1 (ID-based matching)
            enriched_paper = self._try_tier1_matching(dblp_id, dblp_paper)
            
            # Step 2: If Tier 1 failed, try Tier 2 (title matching)
            if not enriched_paper:
                enriched_paper = self._try_tier2_matching(dblp_id, dblp_paper)
            
            # Step 3: If both failed, create Tier 3 (no match)
            if not enriched_paper:
                enriched_paper = self._create_tier3_paper(dblp_id, dblp_paper)
            
            # Step 4: Save to database immediately
            if enriched_paper:
                # Check if paper already exists before inserting to determine operation type
                existing_paper = self.enriched_repo.get_enriched_paper_by_dblp_id(dblp_id)
                is_update = existing_paper is not None
                
                success = self.enriched_repo.insert_enriched_paper(enriched_paper)
                if success:
                    if is_update:
                        self.stats['papers_updated'] += 1
                    else:
                        self.stats['papers_inserted'] += 1
                    return True
                else:
                    self.logger.error(f"Failed to save enriched paper {dblp_paper.key}")
                    return False
            else:
                self.logger.error(f"Failed to create enriched paper for {dblp_paper.key}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing single paper {dblp_paper.key}: {e}")
            return False
    
    def _try_tier1_matching(self, dblp_id: int, dblp_paper: DBLP_Paper) -> Optional[EnrichedPaper]:
        """Try Tier 1 ID-based matching for a single paper"""
        try:
            # Extract paper identifier
            paper_id = None
            
            # First try DOI (more reliable)
            if dblp_paper.doi and dblp_paper.doi.strip():
                paper_id = f"DOI:{dblp_paper.doi.strip()}"
            # Then try using paper key as ACL ID for ACL papers
            elif dblp_paper.key and any(venue in dblp_paper.key.lower() for venue in ['acl', 'naacl', 'emnlp', 'findings']):
                # For our test data, the key IS the ACL ID (e.g., P79-1001)
                if dblp_paper.key.startswith('P') or dblp_paper.key.startswith('D') or dblp_paper.key.startswith('N'):
                    paper_id = f"ACL:{dblp_paper.key}"
            
            if not paper_id:
                return None
            
            # Make API call for this specific paper
            s2_results = self.s2_api.batch_get_papers([paper_id])
            self.stats['api_calls_made'] += 1
            
            if s2_results and len(s2_results) > 0 and s2_results[0]:
                s2_data = s2_results[0]
                confidence = self.validator.calculate_id_match_confidence(dblp_paper.to_dict(), s2_data)
                
                # Create enriched paper
                enriched_paper = EnrichedPaper()
                enriched_paper.merge_dblp_data(dblp_paper)
                
                # Parse and merge S2 data
                parsed_s2_data = self.s2_parser.parse_s2_response(s2_data)
                enriched_paper.merge_s2_data(parsed_s2_data)
                
                # Set validation metadata
                enriched_paper.match_method = 'ID Match'
                enriched_paper.match_confidence = confidence
                enriched_paper.validation_tier = 'Tier1_IDMatch'
                enriched_paper.data_source_primary = 'S2+DBLP'
                enriched_paper.data_completeness_score = self.validator.calculate_completeness_score(enriched_paper.to_dict())
                
                self.stats['tier1_matches'] += 1
                return enriched_paper
            
            return None
            
        except Exception as e:
            self.logger.error(f"Tier 1 matching failed for {dblp_paper.key}: {e}")
            return None
    
    def _try_tier2_matching(self, dblp_id: int, dblp_paper: DBLP_Paper) -> Optional[EnrichedPaper]:
        """Try Tier 2 title-based matching for a single paper"""
        try:
            if not dblp_paper.title or not dblp_paper.title.strip():
                return None
            
            # Search by title
            s2_data = self.s2_api.search_paper_by_title(dblp_paper.title)
            self.stats['api_calls_made'] += 1
            
            if s2_data:
                # Calculate title similarity
                s2_title = s2_data.get('title', '')
                similarity = self.validator.calculate_title_similarity(dblp_paper.title, s2_title)
                
                # Apply similarity thresholds
                if similarity >= 0.70:  # Accept matches with similarity >= 0.70
                    match_method = f'Title Match (similarity: {similarity:.3f})'
                    
                    # Create enriched paper
                    enriched_paper = EnrichedPaper()
                    enriched_paper.merge_dblp_data(dblp_paper)
                    
                    # Parse and merge S2 data
                    parsed_s2_data = self.s2_parser.parse_s2_response(s2_data)
                    enriched_paper.merge_s2_data(parsed_s2_data)
                    
                    # Set validation metadata
                    enriched_paper.match_method = match_method
                    enriched_paper.match_confidence = similarity
                    
                    # Determine tier based on similarity
                    if similarity >= 0.85:
                        enriched_paper.validation_tier = 'Tier2_TitleMatch_High'
                    else:
                        enriched_paper.validation_tier = 'Tier2_TitleMatch_Medium'
                    
                    enriched_paper.data_source_primary = 'S2+DBLP'
                    enriched_paper.data_completeness_score = self.validator.calculate_completeness_score(enriched_paper.to_dict())
                    
                    self.stats['tier2_matches'] += 1
                    return enriched_paper
            
            return None
            
        except Exception as e:
            self.logger.error(f"Tier 2 matching failed for {dblp_paper.key}: {e}")
            return None
    
    def _create_tier3_paper(self, dblp_id: int, dblp_paper: DBLP_Paper) -> Optional[EnrichedPaper]:
        """Create Tier 3 paper (no S2 match found)"""
        try:
            enriched_paper = EnrichedPaper()
            enriched_paper.merge_dblp_data(dblp_paper)
            
            # Set Tier 3 metadata
            enriched_paper.match_method = 'No Match'
            enriched_paper.match_confidence = 0.0
            enriched_paper.validation_tier = 'Tier3_NoMatch'
            enriched_paper.data_source_primary = 'DBLP'
            enriched_paper.data_completeness_score = self.validator.calculate_completeness_score(enriched_paper.to_dict())
            
            self.stats['tier3_no_matches'] += 1
            return enriched_paper
            
        except Exception as e:
            self.logger.error(f"Failed to create Tier 3 paper for {dblp_paper.key}: {e}")
            return None
    
    def _record_processing_metadata(self, status: str, error_message: str = None):
        """Record processing metadata"""
        duration = None
        if self.start_time:
            duration = int((datetime.now() - self.start_time).total_seconds())
        
        self.enriched_repo.record_s2_processing_meta(
            process_type='s2_enrichment',
            status=status,
            records_processed=self.stats['papers_processed'],
            records_inserted=self.stats['papers_inserted'],
            records_updated=self.stats['papers_updated'],
            records_tier1=self.stats['tier1_matches'],
            records_tier2=self.stats['tier2_matches'],
            records_tier3=self.stats['tier3_no_matches'],
            api_calls_made=self.stats['api_calls_made'],
            error_message=error_message,
            execution_duration=duration
        )
    
    def _generate_enrichment_report(self):
        """Generate enrichment process report"""
        end_time = datetime.now()
        duration = end_time - self.start_time if self.start_time else None
        
        # Get overall statistics
        overall_stats = self.enriched_repo.get_enrichment_statistics()
        
        self.logger.info("=" * 80)
        self.logger.info("S2 ENRICHMENT PROCESS COMPLETED")
        self.logger.info("=" * 80)
        self.logger.info(f"Processing time: {duration}")
        self.logger.info(f"Papers processed: {self.stats['papers_processed']}")
        self.logger.info(f"Papers inserted: {self.stats['papers_inserted']}")
        self.logger.info(f"Papers updated: {self.stats['papers_updated']}")
        self.logger.info(f"Tier 1 matches (ID): {self.stats['tier1_matches']}")
        self.logger.info(f"Tier 2 matches (Title): {self.stats['tier2_matches']}")
        self.logger.info(f"Tier 3 no matches: {self.stats['tier3_no_matches']}")
        self.logger.info(f"API calls made: {self.stats['api_calls_made']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        
        self.logger.info("\nOVERALL DATABASE STATISTICS:")
        self.logger.info(f"Total DBLP papers: {overall_stats.get('total_dblp_papers', 0)}")
        self.logger.info(f"Total enriched papers: {overall_stats.get('total_enriched_papers', 0)}")
        self.logger.info(f"Enrichment coverage: {overall_stats.get('enrichment_coverage', 0):.1f}%")
        self.logger.info(f"S2 match rate: {overall_stats.get('s2_match_rate', 0):.1f}%")
        
        # Validation tier distribution
        tier_dist = overall_stats.get('validation_tiers', {})
        if tier_dist:
            self.logger.info("\nVALIDATION TIER DISTRIBUTION:")
            for tier, count in tier_dist.items():
                self.logger.info(f"  {tier}: {count}")
    
    def get_enrichment_statistics(self) -> Dict[str, Any]:
        """Get current enrichment statistics"""
        return self.enriched_repo.get_enrichment_statistics()
    
    def export_enriched_papers(self, output_path: str = "data/s2_enriched_papers.csv", 
                              include_all_fields: bool = True) -> bool:
        """Export enriched papers to CSV"""
        return self.enriched_repo.export_to_csv(output_path, include_all_fields)
    
    def generate_validation_report(self, output_path: str = "data/s2_validation_report.json") -> bool:
        """Generate detailed validation report as JSON file"""
        try:
            import os
            import json
            from datetime import datetime
            
            # Get statistics
            stats = self.get_enrichment_statistics()
            
            # Get field completion rates
            field_completion = self._calculate_field_completion_rates()
            
            # Get match distribution details
            match_details = self._get_match_distribution_details()
            
            # Create report
            report = {
                "generation_timestamp": datetime.now().isoformat(),
                "total_papers": stats.get('total_dblp_papers', 0),
                "enriched_papers": stats.get('total_enriched_papers', 0),
                "enrichment_coverage_percentage": round(stats.get('enrichment_coverage', 0), 1),
                "s2_match_success_rate": round(stats.get('s2_match_rate', 0), 1),
                
                "match_distribution": {
                    tier: count for tier, count in stats.get('validation_tiers', {}).items()
                },
                
                "match_distribution_percentages": {
                    tier: round(count / stats.get('total_enriched_papers', 1) * 100, 1) 
                    for tier, count in stats.get('validation_tiers', {}).items()
                },
                
                "field_completion_rates": field_completion,
                
                "data_quality_summary": {
                    "s2_match_success_rate": round(stats.get('s2_match_rate', 0), 1),
                    "field_enrichment_coverage": round(self._calculate_average_completeness(), 1),
                    "high_confidence_matches": len([t for t in stats.get('validation_tiers', {}).keys() if 'Tier1' in t or 'High' in t]),
                    "api_calls_efficiency": f"{stats.get('total_enriched_papers', 0)} papers with {self.stats.get('api_calls_made', 0)} API calls"
                },
                
                "processing_statistics": {
                    "papers_processed": self.stats.get('papers_processed', 0),
                    "papers_inserted": self.stats.get('papers_inserted', 0),
                    "papers_updated": self.stats.get('papers_updated', 0),
                    "tier1_matches": self.stats.get('tier1_matches', 0),
                    "tier2_matches": self.stats.get('tier2_matches', 0),
                    "tier3_no_matches": self.stats.get('tier3_no_matches', 0),
                    "api_calls_made": self.stats.get('api_calls_made', 0),
                    "errors": self.stats.get('errors', 0)
                }
            }
            
            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save JSON report
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"S2 validation report generated: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to generate validation report: {e}")
            return False
    
    def _calculate_field_completion_rates(self) -> Dict[str, float]:
        """Calculate completion rates for key S2 fields"""
        try:
            # Query key field completion rates
            key_fields = [
                'semantic_abstract', 'semantic_citation_count', 'doi', 
                'open_access_url', 'bibtex_citation', 'semantic_authors',
                'semantic_fields_of_study', 'semantic_paper_id'
            ]
            
            total_papers = self.enriched_repo.db.fetch_one(
                "SELECT COUNT(*) as count FROM enriched_papers"
            )['count']
            
            if total_papers == 0:
                return {}
            
            completion_rates = {}
            for field in key_fields:
                # Handle different field types appropriately
                if field in ['semantic_citation_count']:  # Integer fields
                    result = self.enriched_repo.db.fetch_one(f"""
                        SELECT COUNT(*) as count FROM enriched_papers 
                        WHERE {field} IS NOT NULL
                    """)
                else:  # Text fields
                    result = self.enriched_repo.db.fetch_one(f"""
                        SELECT COUNT(*) as count FROM enriched_papers 
                        WHERE {field} IS NOT NULL AND {field} != ''
                    """)
                completion_rates[field] = round(result['count'] / total_papers, 3)
            
            return completion_rates
            
        except Exception as e:
            self.logger.error(f"Failed to calculate field completion rates: {e}")
            return {}
    
    def _get_match_distribution_details(self) -> Dict[str, Any]:
        """Get detailed match distribution information"""
        try:
            results = self.enriched_repo.db.fetch_all("""
                SELECT 
                    validation_tier,
                    COUNT(*) as count,
                    AVG(match_confidence) as avg_confidence,
                    AVG(data_completeness_score) as avg_completeness
                FROM enriched_papers
                WHERE validation_tier IS NOT NULL
                GROUP BY validation_tier
                ORDER BY count DESC
            """)
            
            return {
                row['validation_tier']: {
                    'count': row['count'],
                    'avg_confidence': round(float(row['avg_confidence']) if row['avg_confidence'] else 0, 3),
                    'avg_completeness': round(float(row['avg_completeness']) if row['avg_completeness'] else 0, 3)
                }
                for row in results
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get match distribution details: {e}")
            return {}
    
    def _calculate_average_completeness(self) -> float:
        """Calculate average data completeness score"""
        try:
            result = self.enriched_repo.db.fetch_one("""
                SELECT AVG(data_completeness_score) as avg_score 
                FROM enriched_papers
                WHERE data_completeness_score IS NOT NULL
            """)
            return round(float(result['avg_score']) * 100 if result['avg_score'] else 0, 1)
        except:
            return 0.0