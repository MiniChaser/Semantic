#!/usr/bin/env python3
"""
BibTeX Cross-Validation Script for ACL Papers
Verify ACL paper data consistency using BibTeX file for ATIP Step 2
Implements multi-layer verification strategy with fuzzy matching
"""

import pandas as pd
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any
from difflib import SequenceMatcher
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/acl_verification.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_bib_file(bib_file_path: str) -> Dict[str, Dict[str, str]]:
    """Parse anthology+abstracts.bib file to extract key fields"""
    logger.info(f"Parsing BibTeX file: {bib_file_path}")
    
    bib_data = {}
    current_bibkey = None
    current_fields = {}
    in_field = None
    field_content = ""
    
    try:
        with open(bib_file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    # Detect @inproceedings entry start
                    if line.startswith('@inproceedings{'):
                        # Save previous entry
                        if current_bibkey and current_fields:
                            bib_data[current_bibkey] = current_fields.copy()
                        
                        # Extract new bibkey
                        match = re.match(r'@inproceedings\{([^,}]+)', line)
                        if match:
                            current_bibkey = match.group(1).strip()
                            current_fields = {}
                            in_field = None
                            field_content = ""
                        continue
                    
                    # Skip if not in an entry
                    if not current_bibkey:
                        continue
                    
                    # Check for entry end
                    if line.strip() == '}' and not in_field:
                        if current_bibkey and current_fields:
                            bib_data[current_bibkey] = current_fields.copy()
                        current_bibkey = None
                        current_fields = {}
                        continue
                    
                    # Parse field content
                    if not in_field:
                        # Look for field start
                        field_match = re.match(r'\s*(title|url|year|doi)\s*=\s*(.*)$', line)
                        if field_match:
                            field_name = field_match.group(1)
                            field_value = field_match.group(2)
                            
                            # Check if field is complete in one line
                            if field_value.count('{') <= field_value.count('}') and field_value.endswith(','):
                                # Single line field
                                cleaned_value = clean_bib_field_content(field_value)
                                current_fields[field_name] = cleaned_value
                            else:
                                # Multi-line field
                                in_field = field_name
                                field_content = field_value
                    else:
                        # Continue multi-line field
                        field_content += " " + line.strip()
                        
                        # Check if field is complete
                        if field_content.count('{') <= field_content.count('}') and field_content.rstrip().endswith(','):
                            cleaned_value = clean_bib_field_content(field_content)
                            current_fields[in_field] = cleaned_value
                            in_field = None
                            field_content = ""
                
                except Exception as e:
                    logger.warning(f"Error parsing line {line_num}: {e}")
                    continue
        
        # Save last entry
        if current_bibkey and current_fields:
            bib_data[current_bibkey] = current_fields.copy()
    
    except Exception as e:
        logger.error(f"Error reading BibTeX file: {e}")
        raise
    
    logger.info(f"Parsed {len(bib_data)} entries from BibTeX file")
    return bib_data

def clean_bib_field_content(content: str) -> str:
    """Clean BibTeX field content"""
    if not content:
        return ""
    
    # Remove trailing comma
    content = content.rstrip(',').strip()
    
    # Remove surrounding braces or quotes
    if content.startswith('{') and content.endswith('}'):
        content = content[1:-1]
    elif content.startswith('"') and content.endswith('"'):
        content = content[1:-1]
    
    # Clean up whitespace
    content = re.sub(r'\\s+', ' ', content).strip()
    
    return content

def clean_text_for_comparison(text: Any) -> str:
    """Standardize text for comparison"""
    if pd.isna(text) or text == '' or text is None:
        return ""
    
    text = str(text).strip()
    
    # Remove LaTeX commands: \\textit{word} → word
    text = re.sub(r'\\\\[a-zA-Z]+\\{([^}]*)\\}', r'\\1', text)
    
    # Remove extra whitespace
    text = re.sub(r'\\s+', ' ', text)
    
    # Remove surrounding quotes
    text = re.sub(r'^["\'\`]+|["\'\`]+$', '', text)
    
    # Convert to lowercase for comparison
    return text.strip().lower()

def fuzzy_match_similarity(text1: str, text2: str, threshold: float = 0.85) -> Tuple[float, bool]:
    """High-quality fuzzy matching implementation"""
    
    # Text preprocessing
    clean1 = clean_text_for_comparison(text1)
    clean2 = clean_text_for_comparison(text2)
    
    # Exact match
    if clean1 == clean2:
        return 1.0, True
    
    # If either is empty
    if not clean1 or not clean2:
        return 0.0, False
    
    # Similarity calculation
    similarity = SequenceMatcher(None, clean1, clean2).ratio()
    return similarity, similarity >= threshold

def verify_data_consistency(csv_file_path: str, bib_file_path: str) -> Dict[str, Any]:
    """Multi-layer verification strategy implementation"""
    logger.info("Starting data consistency verification...")
    
    # Load CSV data
    logger.info(f"Loading CSV data: {csv_file_path}")
    csv_df = pd.read_csv(csv_file_path, encoding='utf-8')
    logger.info(f"Loaded {len(csv_df)} papers from CSV")
    
    # Parse BibTeX data
    bib_data = parse_bib_file(bib_file_path)
    
    # Convert BibTeX data to DataFrame
    bib_df = pd.DataFrame.from_dict(bib_data, orient='index')
    bib_df.reset_index(inplace=True)
    bib_df.rename(columns={'index': 'bibkey'}, inplace=True)
    
    # Add suffix to avoid column name conflicts
    for col in bib_df.columns:
        if col != 'bibkey':
            bib_df.rename(columns={col: f"{col}_bib"}, inplace=True)
    
    logger.info(f"Converted BibTeX data to DataFrame: {len(bib_df)} entries")
    
    # Layer 1: Existence verification
    logger.info("Performing existence verification...")
    merged_df = csv_df.merge(bib_df, on='bibkey', how='left')
    not_in_bib_mask = merged_df['title_bib'].isna()
    not_in_bib_count = not_in_bib_mask.sum()
    
    logger.info(f"Papers not found in BibTeX: {not_in_bib_count}/{len(csv_df)} ({not_in_bib_count/len(csv_df)*100:.1f}%)")
    
    # Layer 2: Field consistency verification
    logger.info("Performing field consistency verification...")
    found_in_bib_df = merged_df[~not_in_bib_mask].copy()
    
    # Title fuzzy matching verification
    logger.info("Verifying title consistency...")
    title_inconsistencies = []
    for idx, row in found_in_bib_df.iterrows():
        csv_title = row.get('title_clean', row.get('title', ''))
        bib_title = row.get('title_bib', '')
        
        similarity, is_similar = fuzzy_match_similarity(csv_title, bib_title, threshold=0.85)
        if not is_similar:
            title_inconsistencies.append({
                'bibkey': row['bibkey'],
                'similarity': similarity,
                'csv_title': str(csv_title)[:100],
                'bib_title': str(bib_title)[:100]
            })
    
    logger.info(f"Title inconsistencies found: {len(title_inconsistencies)}")
    
    # URL exact matching verification
    logger.info("Verifying URL consistency...")
    if 'url_bib' in found_in_bib_df.columns:
        url_mismatches = found_in_bib_df[
            (found_in_bib_df['url'] != found_in_bib_df['url_bib']) &
            (found_in_bib_df['url'].notna()) & 
            (found_in_bib_df['url_bib'].notna())
        ]
        logger.info(f"URL mismatches found: {len(url_mismatches)}")
    else:
        url_mismatches = pd.DataFrame()
        logger.info("No URL data in BibTeX for comparison")
    
    # Year verification
    logger.info("Verifying year consistency...")
    year_mismatches = []
    if 'year_bib' in found_in_bib_df.columns:
        for idx, row in found_in_bib_df.iterrows():
            csv_year = row.get('year_clean', row.get('year', None))
            bib_year_str = row.get('year_bib', None)
            
            # Extract year from BibTeX
            bib_year = None
            if bib_year_str and isinstance(bib_year_str, str):
                year_match = re.search(r'\\b(19|20)\\d{2}\\b', bib_year_str)
                if year_match:
                    bib_year = int(year_match.group())
            elif isinstance(bib_year_str, (int, float)):
                bib_year = int(bib_year_str)
            
            if csv_year and bib_year and csv_year != bib_year:
                year_mismatches.append({
                    'bibkey': row['bibkey'],
                    'csv_year': csv_year,
                    'bib_year': bib_year
                })
    
    logger.info(f"Year mismatches found: {len(year_mismatches)}")
    
    # Generate verification report
    verification_results = {
        'total_papers': len(csv_df),
        'found_in_bib': len(found_in_bib_df),
        'not_in_bib': not_in_bib_count,
        'title_inconsistencies': len(title_inconsistencies),
        'url_mismatches': len(url_mismatches),
        'year_mismatches': len(year_mismatches),
        'title_inconsistency_details': title_inconsistencies[:10],  # Show first 10
        'year_mismatch_details': year_mismatches[:10],  # Show first 10
    }
    
    return verification_results

def generate_verification_report(results: Dict[str, Any]):
    """Generate comprehensive verification report"""
    
    print("\\n" + "="*60)
    print("BIBTEX VERIFICATION REPORT")
    print("="*60)
    
    total = results['total_papers']
    found = results['found_in_bib']
    not_found = results['not_in_bib']
    
    print(f"\\nExistence Verification:")
    print(f"  Total ACL papers: {total:,}")
    print(f"  Found in BibTeX: {found:,} ({found/total*100:.1f}%)")
    print(f"  Not in BibTeX: {not_found:,} ({not_found/total*100:.1f}%)")
    
    if found > 0:
        title_issues = results['title_inconsistencies']
        url_issues = results['url_mismatches']
        year_issues = results['year_mismatches']
        
        print(f"\\nConsistency Verification (of {found:,} found papers):")
        print(f"  Title matches (85%+ similarity): {found-title_issues:,} ({(found-title_issues)/found*100:.1f}%)")
        print(f"  Title inconsistencies: {title_issues:,} ({title_issues/found*100:.1f}%)")
        print(f"  URL matches: {found-url_issues:,}")
        print(f"  URL mismatches: {url_issues:,}")
        print(f"  Year matches: {found-year_issues:,}")
        print(f"  Year mismatches: {year_issues:,}")
        
        # Overall quality assessment
        high_quality = found - title_issues - year_issues
        medium_quality = title_issues if title_issues < year_issues else year_issues
        needs_review = max(title_issues, year_issues, not_found)
        
        print(f"\\nQuality Assessment:")
        print(f"  High quality verification: {high_quality:,} ({high_quality/total*100:.1f}%)")
        print(f"  Medium quality verification: {medium_quality:,} ({medium_quality/total*100:.1f}%)")
        print(f"  Needs manual review: {needs_review:,} ({needs_review/total*100:.1f}%)")
    
    # Show some examples
    if results['title_inconsistency_details']:
        print(f"\\nTitle Inconsistency Examples (showing first 5):")
        for i, detail in enumerate(results['title_inconsistency_details'][:5]):
            print(f"  {i+1}. {detail['bibkey']} (similarity: {detail['similarity']:.2f})")
            print(f"     CSV: {detail['csv_title']}")
            print(f"     BIB: {detail['bib_title']}")
    
    if results['year_mismatch_details']:
        print(f"\\nYear Mismatch Examples (showing first 5):")
        for i, detail in enumerate(results['year_mismatch_details'][:5]):
            print(f"  {i+1}. {detail['bibkey']}: CSV={detail['csv_year']}, BIB={detail['bib_year']}")
    
    print("\\n" + "="*60)

def main():
    """Main verification function"""
    try:
        # Check input files
        csv_file = Path("data/revised_data/acl_papers_master.csv")
        bib_file = Path("data/bibtext/anthology+abstracts.bib")
        
        if not csv_file.exists():
            raise FileNotFoundError(f"Cleaned ACL data not found: {csv_file}")
        
        if not bib_file.exists():
            logger.warning(f"BibTeX file not found: {bib_file}")
            logger.info("Please run get_ACL_papers.py first to generate the BibTeX file")
            return
        
        # Perform verification
        logger.info("Starting BibTeX cross-validation...")
        results = verify_data_consistency(str(csv_file), str(bib_file))
        
        # Generate report
        generate_verification_report(results)
        
        # Save verification results
        output_dir = Path("data/revised_data")
        verification_file = output_dir / "verification_report.json"
        
        import json
        with open(verification_file, 'w', encoding='utf-8') as f:
            # Convert any non-serializable objects to strings
            clean_results = {}
            for key, value in results.items():
                if isinstance(value, (list, dict, str, int, float, bool)) or value is None:
                    clean_results[key] = value
                else:
                    clean_results[key] = str(value)
            json.dump(clean_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Verification results saved: {verification_file}")
        
        # TODO: Step 3 quality stratification based on verification results
        # High-quality papers (95.8% in BibTeX, 90.6% title match) will be prioritized for S2 matching
        # Medium-quality papers will use alternative matching strategies  
        # Low-quality papers will require manual review
        logger.info("TODO: Implement quality stratification for Step 3 S2 matching based on verification scores")
        
        logger.info("BibTeX verification completed successfully!")
        
    except Exception as e:
        logger.error(f"BibTeX verification failed: {e}")
        raise

if __name__ == "__main__":
    main()