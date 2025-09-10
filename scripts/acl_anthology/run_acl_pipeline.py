#!/usr/bin/env python3
"""
ACL Data Processing Pipeline Runner
Execute complete ACL data expansion pipeline for ATIP Step 2
From DBLP 7 fields to ACL 36 fields with verification
"""

import sys
import subprocess
from pathlib import Path
import logging
from typing import Optional
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/acl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required dependencies are available"""
    logger.info("Checking dependencies...")
    
    try:
        # Check acl_anthology library
        import acl_anthology
        logger.info("✓ acl_anthology library available")
        
        # Check pandas
        import pandas as pd
        logger.info("✓ pandas library available")
        
        # Check other required libraries
        import ast
        import re
        from difflib import SequenceMatcher
        logger.info("✓ All required Python libraries available")
        
        return True
        
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        logger.error("Please install required packages:")
        logger.error("  pip install acl_anthology pandas")
        return False

def run_step(script_name: str, description: str) -> bool:
    """Run a single pipeline step"""
    logger.info(f"Starting {description}...")
    
    script_path = Path(__file__).parent / script_name
    
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False
    
    try:
        # Run the script
        start_time = time.time()
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )
        
        elapsed_time = time.time() - start_time
        
        if result.returncode == 0:
            logger.info(f"✓ {description} completed successfully in {elapsed_time:.1f}s")
            if result.stdout:
                logger.info("Output:")
                for line in result.stdout.strip().split('\\n'):
                    logger.info(f"  {line}")
            return True
        else:
            logger.error(f"✗ {description} failed (exit code: {result.returncode})")
            if result.stderr:
                logger.error("Error output:")
                for line in result.stderr.strip().split('\\n'):
                    logger.error(f"  {line}")
            if result.stdout:
                logger.error("Standard output:")
                for line in result.stdout.strip().split('\\n'):
                    logger.error(f"  {line}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to run {script_name}: {e}")
        return False

def check_output_files():
    """Verify that expected output files were created"""
    logger.info("Checking output files...")
    
    expected_files = [
        "data/bibtext/anthology+abstracts.bib",
        "data/revised_data/acl_papers_raw.csv", 
        "data/revised_data/acl_papers_master.csv",
        "data/revised_data/verification_report.json"
    ]
    
    all_exist = True
    for file_path in expected_files:
        path = Path(file_path)
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            logger.info(f"✓ {file_path} ({size_mb:.1f} MB)")
        else:
            logger.error(f"✗ {file_path} - missing")
            all_exist = False
    
    return all_exist

def generate_pipeline_summary():
    """Generate summary of pipeline results"""
    logger.info("Generating pipeline summary...")
    
    print("\\n" + "="*80)
    print("ACL DATA EXPANSION PIPELINE SUMMARY")
    print("="*80)
    print("ATIP Step 2: DBLP 7-field → ACL 36-field expansion")
    
    # Check final data file
    master_file = Path("data/revised_data/acl_papers_master.csv")
    if master_file.exists():
        try:
            import pandas as pd
            df = pd.read_csv(master_file, nrows=1)  # Just check first row for column count
            print(f"\\n✓ Final dataset: {master_file}")
            print(f"✓ Fields expanded from 7 → {len(df.columns)} fields")
            
            # Get full row count
            total_rows = sum(1 for _ in open(master_file)) - 1  # Subtract header
            print(f"✓ Total papers processed: {total_rows:,}")
            
        except Exception as e:
            print(f"\\n✗ Error reading master file: {e}")
    
    # Check verification report
    verification_file = Path("data/revised_data/verification_report.json")
    if verification_file.exists():
        try:
            import json
            with open(verification_file, 'r', encoding='utf-8') as f:
                results = json.load(f)
            
            print(f"\\n✓ Verification completed:")
            print(f"   Found in BibTeX: {results.get('found_in_bib', 0):,}/{results.get('total_papers', 0):,} "
                  f"({results.get('found_in_bib', 0)/results.get('total_papers', 1)*100:.1f}%)")
            print(f"   High-quality verification: ~{results.get('total_papers', 0) - results.get('title_inconsistencies', 0):,} papers")
            
        except Exception as e:
            print(f"\\n✗ Error reading verification report: {e}")
    
    # List all output files
    print("\\n✓ Generated files:")
    output_files = [
        "data/acl_collections.txt",
        "data/bibtext/anthology+abstracts.bib",
        "data/revised_data/acl_papers_raw.csv",
        "data/revised_data/acl_papers_master.csv", 
        "data/revised_data/verification_report.json"
    ]
    
    for file_path in output_files:
        path = Path(file_path)
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"   {file_path} ({size_mb:.1f} MB)")
    
    print("\\n" + "="*80)
    print("Pipeline Status: SUCCESS")
    print("Next Step: Ready for ATIP Step 3 - S2 API matching")
    print("="*80)

def main():
    """Run the complete ACL data processing pipeline"""
    logger.info("Starting ACL Data Expansion Pipeline for ATIP Step 2")
    
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)
    
    pipeline_start = time.time()
    
    try:
        # Step 0: Check dependencies
        if not check_dependencies():
            logger.error("Dependencies check failed. Please install missing packages.")
            return 1
        
        # Step 1: Extract ACL papers (includes BibTeX generation)
        if not run_step("get_ACL_papers.py", "ACL paper extraction"):
            logger.error("Pipeline failed at extraction step")
            return 1
        
        # Step 2: Clean and standardize data
        if not run_step("clean_acl_papers.py", "ACL data cleaning"):
            logger.error("Pipeline failed at cleaning step")
            return 1
        
        # Step 3: Verify with BibTeX cross-validation
        if not run_step("verify_acl_papers_with_bib.py", "BibTeX verification"):
            logger.error("Pipeline failed at verification step")
            return 1
        
        # Step 4: Check output files
        if not check_output_files():
            logger.error("Pipeline completed but some output files are missing")
            return 1
        
        # Step 5: Generate summary
        generate_pipeline_summary()
        
        pipeline_time = time.time() - pipeline_start
        logger.info(f"✓ ACL Data Expansion Pipeline completed successfully in {pipeline_time:.1f}s")
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed with exception: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)