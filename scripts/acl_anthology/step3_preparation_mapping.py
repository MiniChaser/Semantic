#!/usr/bin/env python3
"""
Step 3 Preparation: DBLP-ACL Mapping for S2 Matching
Create mapping between DBLP and ACL papers for ATIP Step 3
Prepare quality stratification for S2 API matching
"""

import pandas as pd
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dblp_acl_mapping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_title_for_matching(title: str) -> str:
    """Clean title for fuzzy matching between DBLP and ACL"""
    if not title or pd.isna(title):
        return ""
    
    title = str(title).lower().strip()
    
    # Remove common prefixes/suffixes
    title = re.sub(r'^(the|a|an)\\s+', '', title)
    title = re.sub(r'\\s+(the|a|an)$', '', title)
    
    # Remove punctuation and extra spaces
    title = re.sub(r'[^a-zA-Z0-9\\s]', ' ', title)
    title = re.sub(r'\\s+', ' ', title).strip()
    
    return title

def calculate_title_similarity(title1: str, title2: str) -> float:
    """Calculate similarity between two titles"""
    clean1 = clean_title_for_matching(title1)
    clean2 = clean_title_for_matching(title2)
    
    if not clean1 or not clean2:
        return 0.0
    
    if clean1 == clean2:
        return 1.0
    
    return SequenceMatcher(None, clean1, clean2).ratio()

def extract_authors_for_matching(authors_field: any) -> List[str]:
    """Extract author list for matching"""
    if pd.isna(authors_field) or authors_field == '':
        return []
    
    try:
        # Handle different author formats
        if isinstance(authors_field, list):
            return [str(author).strip() for author in authors_field if author]
        elif isinstance(authors_field, str):
            # Try parsing as Python literal
            try:
                import ast
                authors_list = ast.literal_eval(authors_field)
                if isinstance(authors_list, list):
                    return [str(author).strip() for author in authors_list if author]
            except (ValueError, SyntaxError):
                pass
            
            # Fallback: split by common separators
            separators = [';', '|', ',']
            for sep in separators:
                if sep in authors_field:
                    return [author.strip() for author in authors_field.split(sep) if author.strip()]
            
            return [authors_field.strip()]
        
        return []
    except Exception as e:
        logger.warning(f"Error extracting authors from: {authors_field}, error: {e}")
        return []

def calculate_author_overlap(dblp_authors: List[str], acl_authors: List[str]) -> float:
    """Calculate author overlap between DBLP and ACL papers"""
    if not dblp_authors or not acl_authors:
        return 0.0
    
    # Simple lastname matching for now
    dblp_lastnames = set()
    acl_lastnames = set()
    
    for author in dblp_authors:
        # Extract last name (assume format: "First Last" or "Last, First")
        if ',' in author:
            lastname = author.split(',')[0].strip()
        else:
            parts = author.strip().split()
            lastname = parts[-1] if parts else author
        dblp_lastnames.add(lastname.lower())
    
    for author in acl_authors:
        if ',' in author:
            lastname = author.split(',')[0].strip()
        else:
            parts = author.strip().split()
            lastname = parts[-1] if parts else author
        acl_lastnames.add(lastname.lower())
    
    if not dblp_lastnames or not acl_lastnames:
        return 0.0
    
    overlap = len(dblp_lastnames.intersection(acl_lastnames))
    total_unique = len(dblp_lastnames.union(acl_lastnames))
    
    return overlap / total_unique if total_unique > 0 else 0.0

def match_dblp_to_acl(dblp_df: pd.DataFrame, acl_df: pd.DataFrame) -> pd.DataFrame:
    """Match DBLP papers to ACL papers using multiple criteria"""
    logger.info("Starting DBLP-ACL paper matching...")
    
    matches = []
    total_dblp = len(dblp_df)
    
    for idx, dblp_row in dblp_df.iterrows():
        if idx % 1000 == 0:
            logger.info(f"Processing DBLP paper {idx}/{total_dblp}...")
        
        dblp_title = dblp_row.get('title', '')
        dblp_year = dblp_row.get('year', '')
        dblp_authors = extract_authors_for_matching(dblp_row.get('authors', []))
        
        # Extract year as integer
        dblp_year_int = None
        if dblp_year:
            year_match = re.search(r'\\b(19|20)\\d{2}\\b', str(dblp_year))
            if year_match:
                dblp_year_int = int(year_match.group())
        
        best_match = None
        best_score = 0.0
        best_acl_row = None
        
        # Search through ACL papers for matches
        for acl_idx, acl_row in acl_df.iterrows():
            acl_title = acl_row.get('title_clean', acl_row.get('title', ''))
            acl_year = acl_row.get('year_clean', acl_row.get('year', ''))
            acl_authors = extract_authors_for_matching(acl_row.get('authors_clean', acl_row.get('authors', [])))
            
            # Year filtering (must match within 1 year)
            if dblp_year_int and acl_year and abs(dblp_year_int - int(acl_year)) > 1:
                continue
            
            # Calculate similarity scores
            title_sim = calculate_title_similarity(dblp_title, acl_title)
            author_sim = calculate_author_overlap(dblp_authors, acl_authors)
            
            # Combined score (weighted)
            combined_score = 0.7 * title_sim + 0.3 * author_sim
            
            # Minimum thresholds
            if title_sim >= 0.8 and combined_score > best_score:
                best_score = combined_score
                best_match = {
                    'dblp_key': dblp_row.get('key', ''),
                    'dblp_title': dblp_title,
                    'dblp_authors': dblp_authors,
                    'dblp_year': dblp_year_int,
                    'acl_paper_id': acl_row.get('paper_id', ''),
                    'acl_bibkey': acl_row.get('bibkey', ''),
                    'acl_title': acl_title,
                    'acl_authors': acl_authors,
                    'acl_year': acl_year,
                    'acl_doi': acl_row.get('doi_clean', acl_row.get('doi', '')),
                    'acl_url': acl_row.get('url', ''),
                    'title_similarity': title_sim,
                    'author_similarity': author_sim,
                    'combined_score': combined_score,
                    'match_quality': classify_match_quality(title_sim, author_sim, combined_score)
                }
        
        if best_match:
            matches.append(best_match)
    
    logger.info(f"DBLP-ACL matching completed: {len(matches)} matches found from {total_dblp} DBLP papers")
    return pd.DataFrame(matches)

def classify_match_quality(title_sim: float, author_sim: float, combined_score: float) -> str:
    """Classify match quality for Step 3 stratification"""
    
    # TODO: Step 3 - These quality tiers will be used for S2 API matching strategy
    if title_sim >= 0.95 and author_sim >= 0.8 and combined_score >= 0.90:
        return "high_quality"
    elif title_sim >= 0.85 and author_sim >= 0.6 and combined_score >= 0.75:
        return "medium_quality"
    elif title_sim >= 0.80 and combined_score >= 0.60:
        return "low_quality"
    else:
        return "needs_review"

def create_step3_preparation():
    """
    TODO: Step 3 Implementation Plan
    This function will be expanded for S2 API matching preparation
    """
    
    # TODO: Create quality-based matching strategies
    strategies = {
        'high_quality': {
            'method': 'direct_acl_id_lookup',
            'fallback': 'doi_lookup', 
            'description': 'Use ACL paper_id directly with S2 API'
        },
        'medium_quality': {
            'method': 'title_author_search',
            'fallback': 'fuzzy_title_search',
            'description': 'Search S2 by title+authors, verify with metadata'
        },
        'low_quality': {
            'method': 'multiple_candidate_search',
            'fallback': 'manual_review_queue',
            'description': 'Generate multiple S2 candidates, score by similarity'
        }
    }
    
    logger.info("TODO: Step 3 matching strategies defined:")
    for quality, strategy in strategies.items():
        logger.info(f"  {quality}: {strategy['description']}")
    
    # TODO: Create S2 API request templates
    api_templates = {
        'by_acl_id': "https://api.semanticscholar.org/graph/v1/paper/{acl_paper_id}",
        'by_doi': "https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}",
        'by_title': "https://api.semanticscholar.org/graph/v1/paper/search?query={title}&limit=10",
        'by_title_author': "https://api.semanticscholar.org/graph/v1/paper/search?query={title}+{authors}&limit=5"
    }
    
    logger.info("TODO: S2 API request templates prepared")
    
    # TODO: Define quality metrics for S2 matching validation
    validation_metrics = {
        'title_match_threshold': 0.85,
        'author_overlap_threshold': 0.6,
        'year_difference_max': 1,
        'citation_count_reasonableness': True,
        'venue_consistency_check': True
    }
    
    logger.info("TODO: S2 matching validation metrics defined")
    
    return {
        'strategies': strategies,
        'api_templates': api_templates,
        'validation_metrics': validation_metrics
    }

def main():
    """Main function for DBLP-ACL mapping"""
    try:
        # Check input files
        dblp_file = Path("data/dblp_papers_export.csv")
        acl_file = Path("data/revised_data/acl_papers_master.csv")
        
        if not dblp_file.exists():
            raise FileNotFoundError(f"DBLP data not found: {dblp_file}")
        
        if not acl_file.exists():
            raise FileNotFoundError(f"ACL data not found: {acl_file}")
        
        # Load data
        logger.info("Loading DBLP and ACL data...")
        dblp_df = pd.read_csv(dblp_file, encoding='utf-8')
        acl_df = pd.read_csv(acl_file, encoding='utf-8')
        
        logger.info(f"DBLP papers: {len(dblp_df)}, ACL papers: {len(acl_df)}")
        
        # Perform matching
        mapping_df = match_dblp_to_acl(dblp_df, acl_df)
        
        # Save mapping results
        output_dir = Path("data/revised_data")
        mapping_file = output_dir / "acl_dblp_paper_mapping.csv"
        
        mapping_df.to_csv(mapping_file, index=False, encoding='utf-8')
        logger.info(f"DBLP-ACL mapping saved: {mapping_file}")
        
        # Generate quality distribution report
        if not mapping_df.empty:
            quality_dist = mapping_df['match_quality'].value_counts()
            logger.info("\\nMatch quality distribution:")
            for quality, count in quality_dist.items():
                percentage = (count / len(mapping_df)) * 100
                logger.info(f"  {quality}: {count} papers ({percentage:.1f}%)")
        
        # TODO: Step 3 preparation
        step3_config = create_step3_preparation()
        
        # Save Step 3 configuration
        import json
        config_file = output_dir / "step3_matching_config.json"
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(step3_config, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Step 3 configuration saved: {config_file}")
        logger.info("DBLP-ACL mapping and Step 3 preparation completed!")
        
    except Exception as e:
        logger.error(f"DBLP-ACL mapping failed: {e}")
        raise

if __name__ == "__main__":
    main()