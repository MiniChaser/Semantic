"""
PDF Download Service
Handles downloading PDFs from DBLP URLs and other sources
"""

import asyncio
import aiohttp
import aiofiles
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from urllib.parse import urljoin, urlparse
import hashlib
import re
from datetime import datetime

from ...database.connection import DatabaseManager
from ...database.repositories.enriched_paper import EnrichedPaperRepository


class PDFDownloadService:
    """Service for downloading PDFs from various sources"""
    
    def __init__(self, db_manager: DatabaseManager, data_dir: str = "data"):
        self.db_manager = db_manager
        self.data_dir = Path(data_dir).resolve()
        self.pdf_dir = self.data_dir / "pdfs"
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = self._setup_logger()
        self.repo = EnrichedPaperRepository(db_manager)
        
        # HTTP session configuration
        self.timeout = aiohttp.ClientTimeout(total=300)  # 5 minutes
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.PDFDownloadService')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _generate_filename(self, paper_id: int, title: str) -> str:
        """Generate safe filename for PDF"""
        # Clean title for filename
        clean_title = re.sub(r'[<>:"/\\|?*]', '_', title[:50])
        clean_title = re.sub(r'\s+', '_', clean_title.strip())
        
        # Create hash from paper ID for uniqueness
        hash_suffix = hashlib.md5(str(paper_id).encode()).hexdigest()[:8]
        
        return f"{clean_title}_{hash_suffix}.pdf"
    
    async def _get_redirect_url(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Get the redirect URL from DBLP URL"""
        try:
            async with session.get(url, headers=self.headers, allow_redirects=False) as response:
                if response.status in [301, 302, 303, 307, 308]:
                    redirect_url = response.headers.get('Location')
                    if redirect_url:
                        # Handle relative URLs
                        if redirect_url.startswith('/'):
                            parsed_url = urlparse(url)
                            redirect_url = f"{parsed_url.scheme}://{parsed_url.netloc}{redirect_url}"
                        return redirect_url
                elif response.status == 200:
                    # If no redirect, try to parse HTML for meta redirect or links
                    html_content = await response.text()
                    return self._extract_pdf_url_from_html(html_content, url)
                    
        except Exception as e:
            self.logger.warning(f"Failed to get redirect URL from {url}: {e}")
        
        return None
    
    def _extract_pdf_url_from_html(self, html_content: str, base_url: str) -> Optional[str]:
        """Extract PDF URL from HTML content"""
        try:
            # Look for common PDF link patterns
            pdf_patterns = [
                r'<a[^>]+href=["\']([^"\']+\.pdf)["\'][^>]*>',
                r'<meta[^>]+content=["\']([^"\']+\.pdf)["\'][^>]*>',
                r'window\.location\s*=\s*["\']([^"\']+\.pdf)["\']',
                r'location\.href\s*=\s*["\']([^"\']+\.pdf)["\']'
            ]
            
            for pattern in pdf_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    pdf_url = matches[0]
                    # Convert relative URLs to absolute
                    if pdf_url.startswith('/'):
                        parsed_base = urlparse(base_url)
                        pdf_url = f"{parsed_base.scheme}://{parsed_base.netloc}{pdf_url}"
                    elif not pdf_url.startswith('http'):
                        pdf_url = urljoin(base_url, pdf_url)
                    return pdf_url
                    
        except Exception as e:
            self.logger.debug(f"Failed to extract PDF URL from HTML: {e}")
        
        return None
    
    def _construct_pdf_url(self, redirect_url: str) -> str:
        """Construct PDF URL by removing trailing slash and adding .pdf"""
        # Remove trailing slash if present
        if redirect_url.endswith('/'):
            redirect_url = redirect_url[:-1]
        
        # Add .pdf extension
        return f"{redirect_url}.pdf"
    
    async def _download_pdf(self, session: aiohttp.ClientSession, pdf_url: str, 
                          file_path: Path) -> bool:
        """Download PDF from URL and save to file"""
        try:
            self.logger.info(f"Downloading PDF from: {pdf_url}")
            
            async with session.get(pdf_url, headers=self.headers) as response:
                if response.status == 200:
                    content_type = response.headers.get('content-type', '').lower()
                    
                    # Verify it's actually a PDF
                    if 'application/pdf' not in content_type:
                        # Check first few bytes for PDF magic number
                        first_chunk = await response.content.read(1024)
                        if not first_chunk.startswith(b'%PDF'):
                            self.logger.warning(f"URL does not return a valid PDF: {pdf_url}")
                            return False
                        
                        # Reset the response for full download
                        async with session.get(pdf_url, headers=self.headers) as fresh_response:
                            if fresh_response.status != 200:
                                return False
                            response = fresh_response
                    
                    # Download and save the PDF
                    async with aiofiles.open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    
                    # Verify file was created and has reasonable size
                    if file_path.exists() and file_path.stat().st_size > 1024:  # At least 1KB
                        self.logger.info(f"PDF downloaded successfully: {file_path}")
                        return True
                    else:
                        self.logger.warning(f"Downloaded file is too small or doesn't exist: {file_path}")
                        if file_path.exists():
                            file_path.unlink()
                        return False
                        
                else:
                    self.logger.warning(f"Failed to download PDF, status: {response.status}")
                    
        except Exception as e:
            self.logger.error(f"Error downloading PDF from {pdf_url}: {e}")
        
        return False
    
    async def download_pdf_from_dblp_url(self, paper_id: int, dblp_url: str, 
                                       title: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Download PDF from DBLP URL
        
        Returns:
            Tuple[bool, Optional[str], Optional[str]]: 
            (success, pdf_filename, pdf_file_path)
        """
        try:
            filename = self._generate_filename(paper_id, title)
            file_path = self.pdf_dir / filename
            
            # Skip if file already exists
            if file_path.exists():
                self.logger.info(f"PDF already exists: {filename}")
                return True, filename, str(file_path)
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                # Step 1: Get redirect URL from DBLP
                self.logger.info(f"Getting redirect URL from DBLP: {dblp_url}")
                redirect_url = await self._get_redirect_url(session, dblp_url)
                
                if not redirect_url:
                    self.logger.warning(f"Failed to get redirect URL from: {dblp_url}")
                    return False, None, None
                
                self.logger.info(f"Redirect URL found: {redirect_url}")
                
                # Step 2: Construct PDF URL
                pdf_url = self._construct_pdf_url(redirect_url)
                self.logger.info(f"Constructed PDF URL: {pdf_url}")
                
                # Step 3: Download PDF
                success = await self._download_pdf(session, pdf_url, file_path)
                
                if success:
                    return True, filename, str(file_path)
                else:
                    # Try alternative: download from redirect URL directly
                    self.logger.info("Trying to download from redirect URL directly")
                    success = await self._download_pdf(session, redirect_url, file_path)
                    
                    if success:
                        return True, filename, str(file_path)
        
        except Exception as e:
            self.logger.error(f"Error in download_pdf_from_dblp_url for paper {paper_id}: {e}")
        
        return False, None, None
    
    async def update_paper_pdf_info(self, paper_id: int, filename: str, 
                                  file_path: str) -> bool:
        """Update paper record with PDF information"""
        try:
            result = self.db_manager.execute_query(
                """
                UPDATE enriched_papers 
                SET pdf_filename = %s, 
                    pdf_file_path = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (filename, file_path, datetime.now(), paper_id)
            )
            
            if result:
                self.logger.info(f"Updated PDF info for paper {paper_id}")
                return True
            else:
                self.logger.warning(f"Failed to update PDF info for paper {paper_id}")
                
        except Exception as e:
            self.logger.error(f"Error updating PDF info for paper {paper_id}: {e}")
        
        return False
    
    async def download_papers_batch(self, limit: Optional[int] = None, 
                                  concurrent_downloads: int = 3) -> Dict[str, Any]:
        """
        Download PDFs for papers in batch
        
        Args:
            limit: Maximum number of papers to process
            concurrent_downloads: Number of concurrent downloads
            
        Returns:
            Dict with download statistics
        """
        stats = {
            'total_processed': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'already_exists': 0,
            'no_dblp_url': 0
        }
        
        try:
            # Get papers that need PDF download
            query = """
                SELECT id, dblp_url, dblp_title, semantic_title
                FROM enriched_papers 
                WHERE dblp_url IS NOT NULL 
                AND dblp_url != ''
                AND (pdf_filename IS NULL OR pdf_file_path IS NULL)
                ORDER BY id
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            papers = self.db_manager.fetch_all(query)
            
            if not papers:
                self.logger.info("No papers need PDF download")
                return stats
            
            self.logger.info(f"Found {len(papers)} papers to download")
            
            # Create semaphore for concurrent downloads
            semaphore = asyncio.Semaphore(concurrent_downloads)
            
            async def download_single_paper(paper):
                async with semaphore:
                    paper_id = paper['id']
                    dblp_url = paper['dblp_url']
                    title = paper['semantic_title'] or paper['dblp_title'] or f"paper_{paper_id}"
                    
                    self.logger.info(f"Processing paper {paper_id}: {title[:50]}...")
                    
                    success, filename, file_path = await self.download_pdf_from_dblp_url(
                        paper_id, dblp_url, title
                    )
                    
                    if success and filename and file_path:
                        # Update database
                        update_success = await self.update_paper_pdf_info(
                            paper_id, filename, file_path
                        )
                        
                        if update_success:
                            stats['successful_downloads'] += 1
                            self.logger.info(f"✅ Paper {paper_id} downloaded and updated")
                        else:
                            stats['failed_downloads'] += 1
                            self.logger.warning(f"❌ Paper {paper_id} downloaded but DB update failed")
                    else:
                        stats['failed_downloads'] += 1
                        self.logger.warning(f"❌ Paper {paper_id} download failed")
                    
                    stats['total_processed'] += 1
            
            # Run downloads concurrently
            tasks = [download_single_paper(paper) for paper in papers]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            self.logger.info(f"📊 Download completed. Stats: {stats}")
            
        except Exception as e:
            self.logger.error(f"Error in batch download: {e}")
        
        return stats