#!/usr/bin/env python3
"""
ACL Data Extraction Script
Extract comprehensive paper metadata from ACL Anthology for ATIP Step 2
Expands DBLP 7 fields to ACL 36 fields
"""

import pandas as pd
import ast
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/acl_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_acl_anthology():
    """Initialize ACL Anthology (automatically downloads if first time)"""
    try:
        from acl_anthology import Anthology
        logger.info("Loading ACL Anthology (will download if first time)...")
        anthology = Anthology.from_repo()
        paper_count = len(list(anthology.papers()))
        logger.info(f"ACL Anthology loaded successfully with {paper_count} papers")
        return anthology
    except ImportError:
        logger.error("acl_anthology library not installed. Please install: pip install acl_anthology")
        raise
    except Exception as e:
        logger.error(f"Failed to load ACL Anthology: {e}")
        raise

def generate_bibtex_if_needed():
    """Generate BibTeX file only if it doesn't exist"""
    bibtext_dir = Path("data/bibtext")
    bibtext_dir.mkdir(exist_ok=True)
    
    bib_file_path = bibtext_dir / "anthology+abstracts.bib"
    
    if bib_file_path.exists():
        logger.info(f"BibTeX file already exists at {bib_file_path}")
        return str(bib_file_path)
    
    logger.info("Generating BibTeX file...")
    try:
        from acl_anthology import Anthology
        anthology = Anthology.from_repo()
        
        with open(bib_file_path, 'w', encoding='utf-8') as f:
            count = 0
            for paper in anthology.papers():
                try:
                    bibtex_entry = paper.to_bibtex()
                    f.write(bibtex_entry + '\n\n')
                    count += 1
                    
                    if count % 1000 == 0:
                        logger.info(f"Generated {count} BibTeX entries...")
                        
                except Exception as e:
                    logger.warning(f"Failed to generate BibTeX for paper {getattr(paper, 'full_id', 'unknown')}: {e}")
                    continue
        
        logger.info(f"BibTeX file generated: {bib_file_path} with {count} entries")
        return str(bib_file_path)
        
    except Exception as e:
        logger.error(f"Failed to generate BibTeX file: {e}")
        raise

def load_acl_collections():
    """Load ACL conference collections from config file"""
    collections_file = Path("data/acl_collections.txt")
    if not collections_file.exists():
        raise FileNotFoundError(f"ACL collections config not found: {collections_file}")
    
    collections = []
    with open(collections_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Handle comma-separated values in each line
                collections.extend([col.strip() for col in line.split(',') if col.strip()])
    
    logger.info(f"Loaded {len(collections)} ACL collections")
    return collections

def extract_paper_metadata(paper, volume_info: Dict, anthology) -> Dict[str, Any]:
    """Extract comprehensive metadata for a single paper (36 fields)"""
    try:
        # Extract venue from venue_ids
        venue_acronym = None
        if hasattr(paper, 'venue_ids') and paper.venue_ids and hasattr(anthology, 'venues'):
            venue_obj = anthology.venues.get(paper.venue_ids[0])
            if venue_obj:
                venue_acronym = getattr(venue_obj, 'acronym', None)
        
        return {
            # === Core identifier fields ===
            'paper_id': paper.full_id,
            'tuple_id': str(paper.full_id_tuple), 
            'bibkey': paper.bibkey,
            
            # === Basic metadata fields ===
            'title': str(paper.title) if paper.title else None,
            'authors': [str(author.name) for author in paper.authors] if paper.authors else [],
            'author_ids': [author.id for author in paper.authors] if paper.authors else [],
            'author_affils': [author.affiliation for author in paper.authors if hasattr(author, 'affiliation')] if paper.authors else [],
            'year': paper.year,
            'pages': paper.pages,
            'venue': venue_acronym,
            
            # === Content fields ===
            'abstract': str(paper.abstract) if paper.abstract else None,
            'doi': paper.doi,
            'url': paper.web_url,
            
            # === Attachment fields ===
            'attachments': [str(att) for att in paper.attachments] if hasattr(paper, 'attachments') and paper.attachments else [],
            'awards': [str(award) for award in paper.awards] if hasattr(paper, 'awards') and paper.awards else [],
            'videos': [str(video) for video in paper.videos] if hasattr(paper, 'videos') and paper.videos else [],
            'pdf': str(paper.pdf) if hasattr(paper, 'pdf') and paper.pdf else None,
            
            # === Editorial fields ===
            'editors': [str(editor) for editor in volume_info.get('editors', [])],
            'errata': paper.errata if hasattr(paper, 'errata') else None,
            'revisions': paper.revisions if hasattr(paper, 'revisions') else None,
            
            # === Metadata fields ===
            'parent': str(paper.parent) if hasattr(paper, 'parent') and paper.parent else None,
            'deletion': paper.deletion if hasattr(paper, 'deletion') else None,
            'ingest_date': paper.ingest_date if hasattr(paper, 'ingest_date') else None,
            'issue': paper.issue if hasattr(paper, 'issue') else None,
            'journal': paper.journal if hasattr(paper, 'journal') else None,
            'language': paper.language if hasattr(paper, 'language') else None,
            'note': paper.note if hasattr(paper, 'note') else None,
            
            # === Extended fields ===
            'paperswithcode': paper.paperswithcode if hasattr(paper, 'paperswithcode') else None,
            'address': paper.address if hasattr(paper, 'address') else None,
            'bibtype': paper.bibtype if hasattr(paper, 'bibtype') else None,
            'citeproc_dict': str(paper.citeproc_dict) if hasattr(paper, 'citeproc_dict') and paper.citeproc_dict else None,
            'csltype': paper.csltype if hasattr(paper, 'csltype') else None,
            'is_deleted': paper.is_deleted if hasattr(paper, 'is_deleted') else None,
            'is_frontmatter': paper.is_frontmatter if hasattr(paper, 'is_frontmatter') else None,
            'language_name': paper.language_name if hasattr(paper, 'language_name') else None,
            'month': paper.month if hasattr(paper, 'month') else None,
            'publisher': paper.publisher if hasattr(paper, 'publisher') else None,
            'author_variants': paper.author_variants if hasattr(paper, 'author_variants') else None
        }
    except Exception as e:
        logger.error(f"Error extracting metadata for paper {paper.full_id if hasattr(paper, 'full_id') else 'unknown'}: {e}")
        return None

def extract_comprehensive_paper_data():
    """Extract comprehensive paper data from ACL Anthology"""
    logger.info("Starting ACL paper data extraction...")
    
    # Initialize ACL Anthology
    anthology = setup_acl_anthology()
    
    # Load collections config
    acl_collections = load_acl_collections()
    
    papers_data = []
    processed_collections = 0
    total_papers = 0
    
    for collection_id in acl_collections:
        try:
            logger.info(f"Processing collection: {collection_id}")
            
            # Get collection
            try:
                collection = anthology.get_collection(collection_id)
            except:
                # Fallback to get method
                collection = anthology.get(collection_id)
            
            if not collection:
                logger.warning(f"Collection not found: {collection_id}")
                continue
            
            # Process volumes in collection
            try:
                volumes = list(collection.volumes())
                logger.info(f"Found {len(volumes)} volumes in {collection_id}")
            except Exception as e:
                logger.warning(f"Error getting volumes for {collection_id}: {e}")
                continue
                
            for volume in volumes:
                try:
                    # Get volume info
                    volume_info = {
                        'volume_title': str(getattr(volume, 'title', '')),
                        'editors': [str(editor) for editor in getattr(volume, 'editors', [])],
                        'venue_ids': getattr(volume, 'venue_ids', [])
                    }
                    
                    # Process papers in volume
                    try:
                        papers = list(volume.papers())
                        logger.info(f"Processing {len(papers)} papers in volume")
                    except Exception as e:
                        logger.warning(f"Error getting papers from volume: {e}")
                        continue
                        
                    for paper in papers:
                        try:
                            paper_record = extract_paper_metadata(paper, volume_info, anthology)
                            if paper_record:
                                papers_data.append(paper_record)
                                total_papers += 1
                                
                                if total_papers % 1000 == 0:
                                    logger.info(f"Processed {total_papers} papers...")
                        except Exception as e:
                            logger.warning(f"Error processing paper: {e}")
                            continue
                                
                except Exception as e:
                    logger.warning(f"Error processing volume in {collection_id}: {e}")
                    continue
                    
            processed_collections += 1
            logger.info(f"Completed collection {collection_id} ({processed_collections}/{len(acl_collections)}) - {total_papers} total papers so far")
            
        except Exception as e:
            logger.warning(f"Error processing collection {collection_id}: {e}")
            continue
    
    logger.info(f"Extraction completed: {total_papers} papers from {processed_collections} collections")
    return papers_data

def save_acl_data(papers_data: List[Dict]):
    """Save extracted ACL data to CSV"""
    if not papers_data:
        logger.error("No papers data to save")
        return
    
    # Create DataFrame
    df = pd.DataFrame(papers_data)
    
    # Ensure output directory exists
    output_dir = Path("data/revised_data")
    output_dir.mkdir(exist_ok=True)
    
    # Save raw ACL data
    raw_output_path = output_dir / "acl_papers_raw.csv"
    df.to_csv(raw_output_path, index=False, encoding='utf-8')
    
    logger.info(f"Raw ACL data saved: {raw_output_path}")
    logger.info(f"Data shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Print basic statistics
    print("\n=== ACL Data Extraction Summary ===")
    print(f"Total papers extracted: {len(df)}")
    print(f"Total fields per paper: {len(df.columns)}")
    print(f"Output file: {raw_output_path}")
    
    # Show field completeness
    print("\n=== Field Completeness Analysis ===")
    completeness = df.isnull().sum().sort_values()
    total_papers = len(df)
    
    for field, null_count in completeness.items():
        complete_rate = ((total_papers - null_count) / total_papers) * 100
        print(f"{field}: {complete_rate:.1f}% complete ({total_papers - null_count}/{total_papers})")
    
    return df

def main():
    """Main execution function"""
    try:
        # Step 1: Generate BibTeX file if needed
        generate_bibtex_if_needed()
        
        # Step 2: Extract ACL paper data
        papers_data = extract_comprehensive_paper_data()
        
        # Step 3: Save the data
        df = save_acl_data(papers_data)
        
        # TODO: Step 3 preprocessing - DBLP quality stratification will be implemented here
        # TODO: This will include mapping ACL papers back to DBLP data for S2 matching preparation
        logger.info("TODO: DBLP quality stratification to be implemented in Step 3")
        
        logger.info("ACL data extraction completed successfully!")
        
    except Exception as e:
        logger.error(f"ACL data extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()