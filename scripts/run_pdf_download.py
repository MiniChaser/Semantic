#!/usr/bin/env python3
"""
PDF Download Script
Downloads PDFs for enriched papers that have DBLP URLs but no PDFs yet
"""

import sys
import os
import logging
import time
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from semantic.services.s2_service.pdf_download_service import PDFDownloadService
from semantic.database.connection import get_db_manager


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/pdf_download.log')
        ]
    )


async def download_pdfs_for_enriched_papers(db_manager, logger):
    """Download PDFs for enriched papers"""
    try:
        print("\n📚 Starting PDF Download Process...")
        logger.info("Starting PDF download for enriched papers")
        
        # Initialize PDF download service
        pdf_service = PDFDownloadService(db_manager)
        
        # Get papers that have DBLP URLs but no PDFs
        recent_papers = db_manager.fetch_all("""
            SELECT COUNT(*) as count
            FROM enriched_papers 
            WHERE semantic_paper_id IS NOT NULL 
            AND dblp_url IS NOT NULL 
            AND dblp_url != ''
            AND (pdf_filename IS NULL OR pdf_file_path IS NULL)
        """)
        
        if recent_papers and recent_papers[0]['count'] > 0:
            pending_count = recent_papers[0]['count']
            print(f"📄 Found {pending_count} papers ready for PDF download")
            
            # Download PDFs in batch with moderate concurrency
            download_stats = await pdf_service.download_papers_batch(
                limit=None,  # Download all available
                concurrent_downloads=3  # Conservative concurrency to avoid overwhelming servers
            )
            
            print("\n📊 PDF DOWNLOAD RESULTS:")
            print(f"📋 Total processed: {download_stats['total_processed']}")
            print(f"✅ Successful downloads: {download_stats['successful_downloads']}")
            print(f"❌ Failed downloads: {download_stats['failed_downloads']}")
            print(f"📁 Already existed: {download_stats['already_exists']}")
            
            if download_stats['total_processed'] > 0:
                success_rate = (download_stats['successful_downloads'] / download_stats['total_processed']) * 100
                print(f"📈 PDF Download success rate: {success_rate:.1f}%")
            
            return download_stats
        else:
            print("📄 No papers need PDF download")
            logger.info("No papers found for PDF download")
            return {'total_processed': 0, 'successful_downloads': 0, 'failed_downloads': 0, 'already_exists': 0}
            
    except Exception as e:
        print(f"❌ PDF download failed: {e}")
        logger.error(f"PDF download failed: {e}", exc_info=True)
        return {'total_processed': 0, 'successful_downloads': 0, 'failed_downloads': 0, 'already_exists': 0}


async def main():
    """Run PDF download process"""
    # Record start time
    start_time = datetime.now()
    start_timestamp = time.time()
    
    print("Starting PDF Download Process")
    print("=" * 50)
    print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Initialize database manager
        db_manager = get_db_manager()
        logger.info("✅ Database connection established")
        print("✅ Database connection established")
        
        # Download PDFs for enriched papers
        download_stats = await download_pdfs_for_enriched_papers(db_manager, logger)
        
        # Calculate timing statistics
        end_time = datetime.now()
        end_timestamp = time.time()
        total_duration = end_timestamp - start_timestamp
        duration_formatted = str(timedelta(seconds=int(total_duration)))
        
        # Show final summary
        print("\n📊 FINAL SUMMARY:")
        print("=" * 50)
        
        # Check PDF directory status
        pdf_dir = Path("data/pdfs")
        if pdf_dir.exists():
            pdf_files = list(pdf_dir.glob("*.pdf"))
            if pdf_files:
                total_size = sum(f.stat().st_size for f in pdf_files)
                print(f"📁 PDF files downloaded: {len(pdf_files)}")
                print(f"💾 Total PDF size: {total_size / (1024*1024):.1f} MB")
            else:
                print("📁 No PDF files downloaded")
        
        # Get final database stats
        final_pdf_stats = db_manager.fetch_one("""
            SELECT 
                COUNT(CASE WHEN semantic_paper_id IS NOT NULL THEN 1 END) as enriched_papers,
                COUNT(CASE WHEN pdf_filename IS NOT NULL AND pdf_file_path IS NOT NULL THEN 1 END) as papers_with_pdf,
                COUNT(CASE WHEN dblp_url IS NOT NULL AND dblp_url != '' THEN 1 END) as papers_with_dblp_url
            FROM enriched_papers
        """)
        
        if final_pdf_stats:
            enriched = final_pdf_stats['enriched_papers']
            with_pdf = final_pdf_stats['papers_with_pdf']
            with_dblp = final_pdf_stats['papers_with_dblp_url']
            
            print(f"📚 Enriched papers: {enriched}")
            print(f"🔗 Papers with DBLP URL: {with_dblp}")
            print(f"📄 Papers with PDF: {with_pdf}")
            
            if with_dblp > 0:
                pdf_coverage = (with_pdf / with_dblp) * 100
                print(f"💾 PDF coverage: {pdf_coverage:.1f}%")
        
        # Show timing and performance statistics
        print("\n⏱️  PERFORMANCE STATISTICS:")
        print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏁 Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⌛ Total duration: {duration_formatted}")
        
        if download_stats['total_processed'] > 0:
            avg_time_per_download = total_duration / download_stats['total_processed']
            downloads_per_minute = download_stats['total_processed'] / (total_duration / 60) if total_duration > 0 else 0
            downloads_per_hour = downloads_per_minute * 60
            
            print(f"📄 Files processed: {download_stats['total_processed']}")
            print(f"⚡ Average time per download: {avg_time_per_download:.2f} seconds")
            print(f"📈 Download rate: {downloads_per_minute:.1f} files/minute ({downloads_per_hour:.0f} files/hour)")
        
        print("\n🎉 PDF download process completed!")
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.getLogger(__name__).error(f"Script failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))