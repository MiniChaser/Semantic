"""
Semantic Scholar Dataset Downloader
Downloads dataset files from S2 Datasets API with async support
Migrated from test.random.py
"""

import requests
import json
import os
import asyncio
import aiohttp
import aiofiles
import time
import logging
from pathlib import Path
from urllib.parse import urlparse
from tqdm.asyncio import tqdm
from tqdm import tqdm as sync_tqdm
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

# Constants
CHUNK_SIZE = 8192  # 8KB chunks for streaming
MAX_CONCURRENT_DOWNLOADS = 5  # Maximum concurrent downloads
TIMEOUT_SECONDS = 300  # 5 minutes timeout
PRE_CHECK_TIMEOUT = 10  # Pre-check timeout
PROGRESS_UPDATE_INTERVAL = 0.5  # Progress update interval (seconds)


class AsyncFileDownloader:
    """Async file downloader with progress tracking"""

    def __init__(self, download_dir="downloads", max_concurrent=MAX_CONCURRENT_DOWNLOADS):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.file_info = {}
        self.download_speeds = {}
        self.start_times = {}
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.AsyncFileDownloader')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def _get_filename_from_url(self, url):
        """Extract filename from URL"""
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            filename = f"file_{hash(url) % 10000}"
        return filename

    def _format_size(self, size_bytes):
        """Format file size for display"""
        if size_bytes == 0:
            return "Unknown size"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f}PB"

    def _format_time(self, seconds):
        """Format time for display"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}min"
        else:
            return f"{seconds/3600:.1f}hr"

    async def _pre_check_file(self, session, url):
        """Pre-check file information"""
        try:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=PRE_CHECK_TIMEOUT)) as response:
                if response.status == 200:
                    content_length = int(response.headers.get('content-length', 0))
                    return {
                        "url": url,
                        "filename": self._get_filename_from_url(url),
                        "size": content_length,
                        "status": "available"
                    }
                elif response.status == 403:
                    return await self._estimate_file_size_with_get(session, url)
                else:
                    return {
                        "url": url,
                        "filename": self._get_filename_from_url(url),
                        "size": 0,
                        "status": "unavailable",
                        "error": f"HTTP {response.status}"
                    }
        except Exception as e:
            return await self._estimate_file_size_with_get(session, url)

    async def _estimate_file_size_with_get(self, session, url):
        """Estimate file size using GET request"""
        try:
            headers = {'Range': 'bytes=0-1023'}
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=PRE_CHECK_TIMEOUT)) as response:
                if response.status == 206:
                    content_range = response.headers.get('content-range', '')
                    if '/' in content_range:
                        total_size = int(content_range.split('/')[-1])
                        return {
                            "url": url,
                            "filename": self._get_filename_from_url(url),
                            "size": total_size,
                            "status": "available"
                        }
                elif response.status == 200:
                    content_length = int(response.headers.get('content-length', 0))
                    return {
                        "url": url,
                        "filename": self._get_filename_from_url(url),
                        "size": content_length,
                        "status": "available"
                    }
                else:
                    return {
                        "url": url,
                        "filename": self._get_filename_from_url(url),
                        "size": 0,
                        "status": "unavailable",
                        "error": f"HTTP {response.status}"
                    }
        except Exception as e:
            return {
                "url": url,
                "filename": self._get_filename_from_url(url),
                "size": 0,
                "status": "unknown_size",
                "error": f"Cannot get file size: {str(e)}"
            }

    async def pre_check_files(self, file_urls):
        """Pre-check all files"""
        self.logger.info("Checking file information...")
        progress_bar = sync_tqdm(total=len(file_urls), desc="Checking files", unit="files")

        async with aiohttp.ClientSession() as session:
            tasks = [self._pre_check_file(session, url) for url in file_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        progress_bar.close()

        available_files = []
        unknown_size_files = []
        total_size = 0

        print("\nFile information:")
        print("-" * 80)
        for i, result in enumerate(results):
            if isinstance(result, dict):
                filename = result["filename"]
                size = result["size"]
                status = result["status"]

                self.file_info[result["url"]] = result

                if status == "available":
                    available_files.append(result["url"])
                    total_size += size
                    print(f"✓ {filename:<40} {self._format_size(size):<10} Available")
                elif status == "unknown_size":
                    unknown_size_files.append(result["url"])
                    print(f"? {filename:<40} {'Unknown size':<10} Can try download")
                else:
                    error = result.get("error", "Unknown error")
                    print(f"✗ {filename:<40} {'Unknown size':<10} {error}")
            else:
                print(f"✗ File {i+1}: Check failed - {str(result)}")

        available_files.extend(unknown_size_files)

        print("-" * 80)
        print(f"Total: {len(available_files)} files ({len(unknown_size_files)} unknown size)")
        if total_size > 0:
            print(f"Known size total: {self._format_size(total_size)}")

        return available_files, total_size

    async def _download_single_file(self, session, url, file_info, individual_progress):
        """Download single file"""
        async with self.semaphore:
            try:
                filename = file_info["filename"]
                total_size = file_info["size"]
                filepath = self.download_dir / filename

                if filepath.exists():
                    existing_size = filepath.stat().st_size
                    if existing_size == total_size:
                        individual_progress.update(total_size)
                        return {"url": url, "filename": filename, "status": "skipped", "size": existing_size}

                start_time = time.time()
                self.start_times[url] = start_time
                downloaded_size = 0
                last_update_time = start_time

                async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as response:
                    if response.status == 200:
                        async with aiofiles.open(filepath, 'wb') as f:
                            async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                                await f.write(chunk)
                                downloaded_size += len(chunk)

                                current_time = time.time()
                                if current_time - last_update_time >= PROGRESS_UPDATE_INTERVAL:
                                    elapsed = current_time - start_time
                                    if elapsed > 0:
                                        speed = downloaded_size / elapsed
                                        self.download_speeds[url] = speed

                                        if total_size > 0:
                                            remaining_bytes = total_size - downloaded_size
                                            if speed > 0:
                                                eta = remaining_bytes / speed
                                                individual_progress.set_postfix({
                                                    'speed': f"{speed/1024/1024:.1f}MB/s",
                                                    'eta': self._format_time(eta)
                                                })

                                    last_update_time = current_time

                                individual_progress.update(len(chunk))

                        individual_progress.close()
                        return {
                            "url": url,
                            "filename": filename,
                            "status": "success",
                            "size": downloaded_size
                        }
                    else:
                        individual_progress.close()
                        return {"url": url, "filename": filename, "status": "failed", "error": f"HTTP {response.status}"}

            except asyncio.TimeoutError:
                individual_progress.close()
                return {"url": url, "filename": filename, "status": "timeout", "error": "Timeout"}
            except Exception as e:
                individual_progress.close()
                return {"url": url, "filename": filename, "status": "error", "error": str(e)}

    async def download_files(self, file_urls):
        """Async download multiple files"""
        available_urls, total_size = await self.pre_check_files(file_urls)

        if not available_urls:
            self.logger.info("No files available for download")
            return []

        estimated_speed = 5 * 1024 * 1024  # Assume 5MB/s average speed
        estimated_time = total_size / estimated_speed if estimated_speed > 0 else 0

        print(f"\nStarting download of {len(available_urls)} files to directory: {self.download_dir}")
        print(f"Total size: {self._format_size(total_size)}")
        print(f"Estimated time: {self._format_time(estimated_time)} (based on 5MB/s average speed)")
        print(f"Max concurrent: {MAX_CONCURRENT_DOWNLOADS}")
        print("-" * 80)

        file_progress_bars = {}
        for url in available_urls:
            filename = self._get_filename_from_url(url)
            file_size = 0
            for info in self.file_info.values():
                if info.get("url") == url:
                    file_size = info.get("size", 0)
                    break

            if file_size == 0:
                progress_bar = sync_tqdm(
                    desc=f"Downloading {filename[:30]}{'...' if len(filename) > 30 else ''} (unknown size)",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=len(file_progress_bars),
                    leave=True
                )
            else:
                progress_bar = sync_tqdm(
                    total=file_size,
                    desc=f"Downloading {filename[:30]}{'...' if len(filename) > 30 else ''}",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=len(file_progress_bars),
                    leave=True
                )
            file_progress_bars[url] = progress_bar

        overall_progress = sync_tqdm(
            total=len(available_urls),
            desc="Overall progress",
            unit="files",
            position=len(file_progress_bars),
            leave=True
        )

        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in available_urls:
                file_info = None
                for info in self.file_info.values():
                    if info.get("url") == url:
                        file_info = info
                        break

                if file_info:
                    task = self._download_single_file(
                        session,
                        url,
                        file_info,
                        file_progress_bars[url]
                    )
                    tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

        for progress_bar in file_progress_bars.values():
            progress_bar.close()
        overall_progress.close()

        success_count = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
        skipped_count = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "skipped")
        failed_count = len(results) - success_count - skipped_count

        print(f"\nDownload completed!")
        print(f"Success: {success_count}, Skipped: {skipped_count}, Failed: {failed_count}")

        if self.download_speeds:
            avg_speed = sum(self.download_speeds.values()) / len(self.download_speeds)
            print(f"Average download speed: {avg_speed/1024/1024:.1f}MB/s")

        return results


class S2DatasetDownloader:
    """Semantic Scholar Dataset Downloader"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('SEMANTIC_SCHOLAR_API_KEY')
        self.base_url = "https://api.semanticscholar.org/datasets/v1"
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.S2DatasetDownloader')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers"""
        headers = {'Content-Type': 'application/json'}
        if self.api_key:
            headers['x-api-key'] = self.api_key
        return headers

    def _make_request(self, url: str) -> Optional[Dict]:
        """Make API request"""
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Request failed: {e}")
            return None

    def get_latest_release_info(self) -> Optional[Dict]:
        """Get latest release information"""
        url = f"{self.base_url}/release/latest"
        self.logger.info("Fetching latest release information...")
        return self._make_request(url)

    def get_dataset_info(self, dataset_name='abstracts') -> Optional[Dict]:
        """Get dataset information (includes release_id and file list)"""
        url = f"{self.base_url}/release/latest/dataset/{dataset_name}"
        self.logger.info(f"Fetching dataset information for: {dataset_name}")
        return self._make_request(url)

    async def download_dataset(self, dataset_name: str = 'abstracts', download_dir: str = 'downloads') -> Dict:
        """
        Download dataset and return results with release information
        """
        # Get dataset info
        dataset_info = self.get_dataset_info(dataset_name)

        if not dataset_info:
            self.logger.error("Failed to get dataset information")
            return {
                'success': False,
                'error': 'Failed to get dataset information',
                'file_count': 0
            }

        file_urls = dataset_info.get('files', [])
        if not file_urls:
            self.logger.error("No files found in dataset")
            return {
                'success': False,
                'error': 'No files found',
                'file_count': 0
            }

        # Download files
        downloader = AsyncFileDownloader(download_dir)
        results = await downloader.download_files(file_urls)

        # Count successes
        success_count = sum(1 for r in results if isinstance(r, dict) and r.get("status") in ["success", "skipped"])

        return {
            'success': success_count > 0,
            'release_id': dataset_info.get('release_id'),
            'dataset_name': dataset_name,
            'file_count': len(file_urls),
            'downloaded_count': success_count,
            'download_results': results
        }
