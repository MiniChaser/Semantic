#!/usr/bin/env python3
"""
ACL Data Cleaning and Standardization Script
Clean and standardize ACL paper data for ATIP Step 2
Filter papers and standardize fields according to project requirements
"""

import pandas as pd
import ast
import re
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/acl_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_tuple_id(tuple_id_str: str) -> Tuple[str, str, str]:
    """Parse tuple_id string to extract components"""
    try:
        # Handle string format: "('2020.acl', 'main', '1')"
        if isinstance(tuple_id_str, str):
            # Remove parentheses and quotes, then split
            cleaned = tuple_id_str.strip("()").replace("'", "").replace('"', '')
            parts = [part.strip() for part in cleaned.split(',')]
            
            if len(parts) >= 3:
                return parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                return parts[0], parts[1], '1'  # Default third element
            else:
                return parts[0] if parts else '', '', '1'
        else:
            return '', '', '1'
    except Exception as e:
        logger.warning(f"Failed to parse tuple_id: {tuple_id_str}, error: {e}")
        return '', '', '1'

def should_keep_row(row: pd.Series) -> bool:
    """Apply filtering rules to determine if paper should be kept"""
    try:
        venue = row.get('venue', '')
        tuple_id = row.get('tuple_id', '')
        paper_id = row.get('paper_id', 'unknown')
        
        # Parse tuple_id
        first_elem, second_elem, third_elem = parse_tuple_id(tuple_id)
        
        # Rule 1: Remove frontmatter/table of contents papers
        if third_elem == '0':
            logger.debug(f"Removing frontmatter paper: {paper_id}")
            return False
        
        # Rule 2: Conference type filtering - now use exact venue acronyms from ACL Anthology
        valid_venues = {'ACL', 'EMNLP', 'NAACL', 'Findings'}
        if not venue or pd.isna(venue) or venue not in valid_venues:
            logger.debug(f"Removing non-target conference: {venue} - {paper_id}")
            return False
        
        # Rule 3: Findings paper refinement filtering
        if venue == 'Findings':
            valid_findings_types = {'acl', 'emnlp', 'naacl'}
            if second_elem.lower() not in valid_findings_types:
                logger.debug(f"Removing invalid Findings type: {second_elem} - {paper_id}")
                return False
        
        return True
        
    except Exception as e:
        logger.warning(f"Error filtering row {row.get('paper_id', 'unknown')}: {e}")
        return False

def clean_author_list(authors_str: Any) -> List[str]:
    """Standardize author list format"""
    if pd.isna(authors_str) or authors_str == '':
        return []
    
    try:
        # If already a list
        if isinstance(authors_str, list):
            return [standardize_name_format(author) for author in authors_str if author]
        
        # If string representation of list
        if isinstance(authors_str, str):
            # Try to parse as Python literal
            try:
                authors_list = ast.literal_eval(authors_str)
                if isinstance(authors_list, list):
                    return [standardize_name_format(author) for author in authors_list if author]
            except (ValueError, SyntaxError):
                pass
            
            # Fallback: treat as comma-separated string
            return [standardize_name_format(author.strip()) for author in authors_str.split(',') if author.strip()]
        
        return []
        
    except Exception as e:
        logger.warning(f"Error parsing author list: {authors_str}, error: {e}")
        return []

def standardize_name_format(name: str) -> str:
    """Standardize name format from 'Last, First' to 'First Last'"""
    if not name or not isinstance(name, str):
        return ""
    
    name = name.strip()
    
    # Handle format: "Last, First"
    if ',' in name:
        parts = [part.strip() for part in name.split(',')]
        if len(parts) >= 2 and parts[0] and parts[1]:
            return f"{parts[1]} {parts[0]}"
    
    return name

def extract_year_only(year_str: Any) -> Optional[int]:
    """Extract year from potentially messy year field"""
    if pd.isna(year_str):
        return None
    
    try:
        # If already an integer
        if isinstance(year_str, int):
            if 1900 <= year_str <= 2030:
                return year_str
        
        # If string, extract year
        if isinstance(year_str, str):
            # Look for 4-digit year pattern
            year_match = re.search(r'\b(19|20)\d{2}\b', year_str)
            if year_match:
                year = int(year_match.group())
                if 1900 <= year <= 2030:
                    return year
        
        return None
        
    except Exception as e:
        logger.warning(f"Error extracting year from: {year_str}, error: {e}")
        return None

def standardize_doi_format(doi_str: Any) -> Optional[str]:
    """Standardize DOI format"""
    if pd.isna(doi_str) or doi_str == '':
        return None
    
    try:
        doi_str = str(doi_str).strip()
        
        # Remove common prefixes
        if doi_str.startswith('http://dx.doi.org/'):
            doi_str = doi_str.replace('http://dx.doi.org/', '')
        elif doi_str.startswith('https://doi.org/'):
            doi_str = doi_str.replace('https://doi.org/', '')
        elif doi_str.startswith('doi:'):
            doi_str = doi_str.replace('doi:', '')
        
        return doi_str if doi_str else None
        
    except Exception as e:
        logger.warning(f"Error standardizing DOI: {doi_str}, error: {e}")
        return None

def clean_latex_formatting(text: Any) -> Optional[str]:
    """Clean LaTeX commands from text"""
    if pd.isna(text) or text == '':
        return None
    
    try:
        text = str(text)
        
        # Remove common LaTeX commands
        # \textit{word} -> word
        text = re.sub(r'\\textit\{([^}]*)\}', r'\1', text)
        text = re.sub(r'\\textbf\{([^}]*)\}', r'\1', text)
        text = re.sub(r'\\emph\{([^}]*)\}', r'\1', text)
        
        # Remove other LaTeX commands
        text = re.sub(r'\\[a-zA-Z]+\{([^}]*)\}', r'\1', text)
        
        # Clean up extra spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text if text else None
        
    except Exception as e:
        logger.warning(f"Error cleaning LaTeX from text: {text}, error: {e}")
        return str(text) if text else None

def standardize_acl_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize all ACL fields"""
    logger.info("Standardizing ACL fields...")
    
    df_clean = df.copy()
    
    try:
        # Author field standardization
        logger.info("Cleaning author fields...")
        df_clean['authors_clean'] = df_clean['authors'].apply(clean_author_list)
        df_clean['author_count'] = df_clean['authors_clean'].apply(len)
        
        # Year field cleaning
        logger.info("Cleaning year fields...")
        df_clean['year_clean'] = df_clean['year'].apply(extract_year_only)
        
        # DOI standardization
        logger.info("Standardizing DOI fields...")
        df_clean['doi_clean'] = df_clean['doi'].apply(standardize_doi_format)
        
        # Title cleaning
        logger.info("Cleaning title fields...")
        df_clean['title_clean'] = df_clean['title'].apply(clean_latex_formatting)
        
        # Abstract cleaning
        if 'abstract' in df_clean.columns:
            logger.info("Cleaning abstract fields...")
            df_clean['abstract_clean'] = df_clean['abstract'].apply(clean_latex_formatting)
        
        logger.info("Field standardization completed")
        return df_clean
        
    except Exception as e:
        logger.error(f"Error in field standardization: {e}")
        raise

def generate_cleaning_report(df_original: pd.DataFrame, df_filtered: pd.DataFrame, df_cleaned: pd.DataFrame):
    """Generate comprehensive cleaning report"""
    
    print("\n" + "="*60)
    print("ACL DATA CLEANING REPORT")
    print("="*60)
    
    # Overall statistics
    print(f"\nOriginal papers: {len(df_original):,}")
    print(f"After filtering: {len(df_filtered):,}")
    print(f"Final cleaned: {len(df_cleaned):,}")
    print(f"Retention rate: {(len(df_cleaned)/len(df_original)*100):.1f}%")
    
    # Filtering breakdown
    if len(df_original) > len(df_filtered):
        removed = len(df_original) - len(df_filtered)
        print(f"\nPapers removed by filtering: {removed:,}")
        
        # Try to break down by filtering reasons (approximate)
        # This would require more detailed tracking in the filtering function
        
    # Venue distribution
    if 'venue' in df_cleaned.columns:
        print("\nVenue distribution (after cleaning):")
        venue_counts = df_cleaned['venue'].value_counts()
        for venue, count in venue_counts.items():
            percentage = (count / len(df_cleaned)) * 100
            print(f"  {venue}: {count:,} papers ({percentage:.1f}%)")
    
    # Field completeness comparison
    print("\nField completeness analysis:")
    total_papers = len(df_cleaned)
    
    # Check key fields
    key_fields = ['title_clean', 'authors_clean', 'year_clean', 'doi_clean', 'venue', 'abstract_clean']
    
    for field in key_fields:
        if field in df_cleaned.columns:
            if field == 'authors_clean':
                # Special handling for author count
                non_empty = df_cleaned[field].apply(lambda x: len(x) > 0 if isinstance(x, list) else False).sum()
            else:
                non_empty = df_cleaned[field].notna().sum()
            
            percentage = (non_empty / total_papers) * 100 if total_papers > 0 else 0.0
            print(f"  {field}: {percentage:.1f}% complete ({non_empty:,}/{total_papers:,})")
    
    print("\n" + "="*60)

def main():
    """Main cleaning function"""
    try:
        # Load raw ACL data
        input_path = Path("data/revised_data/acl_papers_raw.csv")
        if not input_path.exists():
            raise FileNotFoundError(f"Raw ACL data not found: {input_path}")
        
        logger.info(f"Loading raw ACL data from: {input_path}")
        df_original = pd.read_csv(input_path, encoding='utf-8')
        logger.info(f"Loaded {len(df_original)} papers with {len(df_original.columns)} fields")
        
        # Step 1: Apply filtering rules
        logger.info("Applying filtering rules...")
        filter_mask = df_original.apply(should_keep_row, axis=1)
        df_filtered = df_original[filter_mask].copy()
        
        removed_count = len(df_original) - len(df_filtered)
        logger.info(f"Filtering completed: {removed_count} papers removed, {len(df_filtered)} retained")
        
        # Step 2: Standardize fields
        df_cleaned = standardize_acl_fields(df_filtered)
        
        # Step 3: Save cleaned data
        output_dir = Path("data/revised_data")
        output_path = output_dir / "acl_papers_master.csv"
        
        df_cleaned.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Cleaned ACL data saved: {output_path}")
        
        # Step 4: Generate report
        generate_cleaning_report(df_original, df_filtered, df_cleaned)
        
        # TODO: Step 3 preprocessing - Create DBLP-ACL mapping for S2 matching
        # This will map cleaned ACL papers back to DBLP data for quality stratification
        logger.info("TODO: DBLP-ACL mapping to be implemented for Step 3 S2 matching preparation")
        
        logger.info("ACL data cleaning completed successfully!")
        return df_cleaned
        
    except Exception as e:
        logger.error(f"ACL data cleaning failed: {e}")
        raise

if __name__ == "__main__":
    main()