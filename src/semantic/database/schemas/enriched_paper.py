"""
Enriched Paper table schema definition with S2 integration
Supports 54 fields total: DBLP (12) + Semantic Scholar (41) + Validation (1)
"""

import logging
from typing import List, Dict
from ..connection import DatabaseManager


class EnrichedPaperSchema:
    """Enriched Paper table schema with S2 integration"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger(f'{__name__}.EnrichedPaperSchema')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def get_table_sql(self) -> str:
        """Get SQL for creating enriched_papers table"""
        return """
        CREATE TABLE IF NOT EXISTS enriched_papers (
            id SERIAL PRIMARY KEY,
            
            -- Reference to DBLP paper
            dblp_paper_id INTEGER REFERENCES dblp_papers(id),
            
            -- DBLP fields (12 fields)
            dblp_id INTEGER,
            dblp_key VARCHAR(255),
            dblp_title TEXT,
            dblp_authors JSONB,
            dblp_year VARCHAR(4),
            dblp_pages VARCHAR(50),
            dblp_url TEXT,
            dblp_venue VARCHAR(50),
            dblp_created_at TIMESTAMP,
            dblp_first_author TEXT,
            dblp_last_author TEXT,
            first_author_dblp_id TEXT,
            
            -- Semantic Scholar basic fields (9 fields)
            semantic_id SERIAL,
            semantic_paper_id VARCHAR(50),
            semantic_title TEXT,
            semantic_year INTEGER,
            semantic_venue TEXT,
            semantic_abstract TEXT,
            semantic_url TEXT,
            semantic_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            semantic_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Semantic Scholar statistics (3 fields)
            semantic_citation_count INTEGER,
            semantic_reference_count INTEGER,
            influentialCitationCount INTEGER,
            
            -- Author information (5 fields)
            semantic_authors JSONB,
            first_author_semantic_id VARCHAR(50),
            all_authors_count INTEGER,
            all_author_names TEXT,
            all_author_ids TEXT,
            
            -- Research fields (4 fields)
            semantic_fields_of_study JSONB,
            s2_fields_primary TEXT,
            s2_fields_secondary TEXT,
            s2_fields_all TEXT,
            
            -- External identifiers (7 fields)
            semantic_external_ids JSONB,
            doi VARCHAR(100),
            arxiv_id VARCHAR(50),
            mag_id VARCHAR(50),
            acl_id VARCHAR(50),
            corpus_id VARCHAR(50),
            pmid VARCHAR(50),
            
            -- Open access information (4 fields)
            open_access_url TEXT,
            open_access_status VARCHAR(50),
            open_access_license VARCHAR(100),
            pdf_available VARCHAR(10),
            
            -- Additional metadata (3 fields)
            bibtex_citation TEXT,
            semantic_full_data JSONB,
            venue_alternate_names TEXT,
            
            -- Future fields for completeness (5 fields)
            author_affiliations JSONB,
            author_contacts JSONB,
            corresponding_authors TEXT,
            pdf_filename VARCHAR(255),
            pdf_file_path TEXT,
            
            -- Validation and processing metadata (1 field + processing fields)
            match_method VARCHAR(100),
            validation_tier VARCHAR(50),
            match_confidence DECIMAL(5,3),  -- 允许 0.000 到 99.999
            data_source_primary VARCHAR(50),
            data_completeness_score DECIMAL(5,3),  -- 允许 0.000 到 99.999
            
            -- Timestamps
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Unique constraint on DBLP paper reference
            UNIQUE(dblp_paper_id)
        );
        """
    
    def get_indexes_sql(self) -> List[str]:
        """Get SQL statements for creating indexes"""
        return [
            # Primary lookup indexes
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_dblp_id ON enriched_papers(dblp_paper_id);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_semantic_id ON enriched_papers(semantic_paper_id);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_doi ON enriched_papers(doi);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_acl_id ON enriched_papers(acl_id);",
            
            # Search and filtering indexes
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_venue ON enriched_papers(semantic_venue);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_year ON enriched_papers(semantic_year);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_validation_tier ON enriched_papers(validation_tier);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_match_confidence ON enriched_papers(match_confidence);",
            
            # JSON field indexes
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_authors ON enriched_papers USING GIN (semantic_authors);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_fields ON enriched_papers USING GIN (semantic_fields_of_study);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_external_ids ON enriched_papers USING GIN (semantic_external_ids);",
            
            # Timestamp indexes
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_created ON enriched_papers(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_updated ON enriched_papers(updated_at);",
            
            # Composite indexes for common queries
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_venue_year ON enriched_papers(semantic_venue, semantic_year);",
            "CREATE INDEX IF NOT EXISTS idx_enriched_papers_tier_confidence ON enriched_papers(validation_tier, match_confidence);",
        ]
    
    def get_triggers_sql(self) -> List[str]:
        """Get SQL statements for creating triggers"""
        return [
            # Trigger function to update updated_at timestamp
            """
            CREATE OR REPLACE FUNCTION update_enriched_papers_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                NEW.semantic_updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            
            # Trigger to automatically update timestamps
            """
            CREATE TRIGGER trigger_enriched_papers_updated_at
                BEFORE UPDATE ON enriched_papers
                FOR EACH ROW
                EXECUTE FUNCTION update_enriched_papers_updated_at();
            """
        ]
    
    def get_processing_meta_table_sql(self) -> str:
        """Get SQL for S2 processing metadata table"""
        return """
        CREATE TABLE IF NOT EXISTS s2_processing_meta (
            id SERIAL PRIMARY KEY,
            process_type VARCHAR(50) NOT NULL,
            last_run_time TIMESTAMP NOT NULL,
            status VARCHAR(20) NOT NULL,
            records_processed INTEGER DEFAULT 0,
            records_inserted INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            records_tier2 INTEGER DEFAULT 0,
            records_tier3 INTEGER DEFAULT 0,
            api_calls_made INTEGER DEFAULT 0,
            error_message TEXT,
            execution_duration INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    
    def create_table(self) -> bool:
        """Create enriched_papers table with all supporting structures"""
        try:
            self.logger.info("Creating enriched_papers table...")
            
            # Create main table
            if not self.db_manager.execute_query(self.get_table_sql()):
                raise Exception("Failed to create enriched_papers table")
            
            # Create processing metadata table
            self.logger.info("Creating S2 processing metadata table...")
            if not self.db_manager.execute_query(self.get_processing_meta_table_sql()):
                raise Exception("Failed to create s2_processing_meta table")
            
            # Create indexes
            self.logger.info("Creating indexes for enriched_papers table...")
            for index_sql in self.get_indexes_sql():
                if not self.db_manager.execute_query(index_sql):
                    self.logger.warning(f"Failed to create index: {index_sql[:50]}...")
            
            # Create triggers
            self.logger.info("Creating triggers for enriched_papers table...")
            for trigger_sql in self.get_triggers_sql():
                if not self.db_manager.execute_query(trigger_sql):
                    self.logger.warning(f"Failed to create trigger: {trigger_sql[:50]}...")
            
            self.logger.info("Enriched papers schema created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create enriched_papers schema: {e}")
            return False
    
    def get_field_count_summary(self) -> Dict[str, int]:
        """Get summary of field counts by category"""
        return {
            'dblp_fields': 12,
            'semantic_basic': 9, 
            'semantic_statistics': 3,
            'author_info': 5,
            'research_fields': 4,
            'external_ids': 7,
            'open_access': 4,
            'metadata': 3,
            'future_fields': 5,
            'validation': 5,
            'timestamps': 2,
            'total_fields': 54
        }