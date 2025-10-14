import requests, json, os
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from urllib.parse import urlparse
from tqdm.asyncio import tqdm
from tqdm import tqdm as sync_tqdm
from dotenv import load_dotenv
import time
from typing import Dict, List, Optional
load_dotenv()

# 方法一：使用项目现有的 SemanticScholarAPI 类（推荐）
from src.semantic.services.s2_service.s2_service import SemanticScholarAPI

# 常量定义
DOWNLOAD_DIR = "downloads"
CHUNK_SIZE = 8192  # 8KB chunks for streaming
MAX_CONCURRENT_DOWNLOADS = 5  # 最大并发下载数
TIMEOUT_SECONDS = 300  # 5分钟超时
PRE_CHECK_TIMEOUT = 10  # 预检查超时时间
PROGRESS_UPDATE_INTERVAL = 0.5  # 进度更新间隔（秒）

class AsyncFileDownloader:
    """异步文件下载器"""
    
    def __init__(self, download_dir=DOWNLOAD_DIR, max_concurrent=MAX_CONCURRENT_DOWNLOADS):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.file_info = {}  # 存储文件信息
        self.download_speeds = {}  # 存储下载速度
        self.start_times = {}  # 存储开始时间
        
    def _get_filename_from_url(self, url):
        """从URL中提取文件名"""
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            filename = f"file_{hash(url) % 10000}"
        return filename
    
    def _format_size(self, size_bytes):
        """格式化文件大小显示"""
        if size_bytes == 0:
            return "未知大小"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f}PB"
    
    def _format_time(self, seconds):
        """格式化时间显示"""
        if seconds < 60:
            return f"{seconds:.0f}秒"
        elif seconds < 3600:
            return f"{seconds/60:.1f}分钟"
        else:
            return f"{seconds/3600:.1f}小时"
    
    async def _pre_check_file(self, session, url):
        """预检查文件信息"""
        try:
            # 首先尝试HEAD请求
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
                    # 对于403错误，尝试使用GET请求获取部分数据来估算大小
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
            # 如果HEAD请求失败，尝试GET请求估算
            return await self._estimate_file_size_with_get(session, url)
    
    async def _estimate_file_size_with_get(self, session, url):
        """使用GET请求估算文件大小"""
        try:
            # 尝试获取文件的前几个字节来估算大小
            headers = {'Range': 'bytes=0-1023'}  # 只获取前1KB
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=PRE_CHECK_TIMEOUT)) as response:
                if response.status == 206:  # Partial Content
                    # 从Content-Range头获取总大小
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
                    # 如果服务器不支持Range请求，返回Content-Length
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
            # 如果所有方法都失败，返回未知大小但标记为可尝试下载
            return {
                "url": url,
                "filename": self._get_filename_from_url(url),
                "size": 0,
                "status": "unknown_size",
                "error": f"无法获取文件大小: {str(e)}"
            }
    
    async def pre_check_files(self, file_urls):
        """预检查所有文件"""
        print("正在检查文件信息...")
        progress_bar = sync_tqdm(total=len(file_urls), desc="检查文件", unit="files")
        
        async with aiohttp.ClientSession() as session:
            tasks = [self._pre_check_file(session, url) for url in file_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        progress_bar.close()
        
        # 处理结果
        available_files = []
        unknown_size_files = []
        total_size = 0
        
        print("\n文件信息:")
        print("-" * 80)
        for i, result in enumerate(results):
            if isinstance(result, dict):
                filename = result["filename"]
                size = result["size"]
                status = result["status"]
                
                # 存储文件信息
                self.file_info[result["url"]] = result
                
                if status == "available":
                    available_files.append(result["url"])
                    total_size += size
                    print(f"✓ {filename:<40} {self._format_size(size):<10} 可用")
                elif status == "unknown_size":
                    unknown_size_files.append(result["url"])
                    print(f"? {filename:<40} {'未知大小':<10} 可尝试下载")
                else:
                    error = result.get("error", "未知错误")
                    print(f"✗ {filename:<40} {'未知大小':<10} {error}")
            else:
                print(f"✗ 文件 {i+1}: 检查失败 - {str(result)}")
        
        # 将未知大小的文件也加入可用文件列表
        available_files.extend(unknown_size_files)
        
        print("-" * 80)
        print(f"总计: {len(available_files)} 个文件 (其中 {len(unknown_size_files)} 个大小未知)")
        if total_size > 0:
            print(f"已知大小文件总大小: {self._format_size(total_size)}")
        
        return available_files, total_size
    
    async def _download_single_file(self, session, url, file_info, individual_progress):
        """下载单个文件"""
        async with self.semaphore:
            try:
                filename = file_info["filename"]
                total_size = file_info["size"]
                filepath = self.download_dir / filename
                
                # 检查文件是否已存在
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
                                
                                # 计算下载速度
                                current_time = time.time()
                                if current_time - last_update_time >= PROGRESS_UPDATE_INTERVAL:
                                    elapsed = current_time - start_time
                                    if elapsed > 0:
                                        speed = downloaded_size / elapsed
                                        self.download_speeds[url] = speed
                                        
                                        # 预估剩余时间
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
        """异步下载多个文件"""
        # 首先进行预检查
        available_urls, total_size = await self.pre_check_files(file_urls)
        
        if not available_urls:
            print("没有可用的文件进行下载")
            return []
        
        # 预估下载时间（基于平均速度假设）
        estimated_speed = 5 * 1024 * 1024  # 假设5MB/s的平均速度
        estimated_time = total_size / estimated_speed if estimated_speed > 0 else 0
        
        print(f"\n开始下载 {len(available_urls)} 个文件到目录: {self.download_dir}")
        print(f"总大小: {self._format_size(total_size)}")
        print(f"预估时间: {self._format_time(estimated_time)} (基于5MB/s平均速度)")
        print(f"最大并发数: {MAX_CONCURRENT_DOWNLOADS}")
        print("-" * 80)
        
        # 为每个文件创建进度条
        file_progress_bars = {}
        for url in available_urls:
            filename = self._get_filename_from_url(url)
            # 获取文件大小
            file_size = 0
            for info in self.file_info.values():
                if info.get("url") == url:
                    file_size = info.get("size", 0)
                    break
            
            # 如果文件大小未知，使用不确定进度条
            if file_size == 0:
                progress_bar = sync_tqdm(
                    desc=f"下载 {filename[:30]}{'...' if len(filename) > 30 else ''} (大小未知)",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=len(file_progress_bars),
                    leave=True
                )
            else:
                progress_bar = sync_tqdm(
                    total=file_size,
                    desc=f"下载 {filename[:30]}{'...' if len(filename) > 30 else ''}",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=len(file_progress_bars),
                    leave=True
                )
            file_progress_bars[url] = progress_bar
        
        # 创建总体进度条
        overall_progress = sync_tqdm(
            total=len(available_urls),
            desc="总体进度",
            unit="files",
            position=len(file_progress_bars),
            leave=True
        )
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in available_urls:
                # 获取文件信息
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
        
        # 关闭所有进度条
        for progress_bar in file_progress_bars.values():
            progress_bar.close()
        overall_progress.close()
        
        # 统计结果
        success_count = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
        skipped_count = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "skipped")
        failed_count = len(results) - success_count - skipped_count
        
        print(f"\n下载完成!")
        print(f"成功: {success_count}, 跳过: {skipped_count}, 失败: {failed_count}")
        
        # 显示平均下载速度
        if self.download_speeds:
            avg_speed = sum(self.download_speeds.values()) / len(self.download_speeds)
            print(f"平均下载速度: {avg_speed/1024/1024:.1f}MB/s")
        
        return results

async def main():
    """主函数"""
    # 获取 API key
    api_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY')
    
    # 创建 API 客户端
    s2_api = SemanticScholarAPI(api_key)
    
    # 尝试不同的API端点获取数据集信息
    print("获取数据集信息...")
    
    # 方法1: 尝试获取最新发布信息
    try:
        print("尝试获取最新发布信息...")
        latest_release = s2_api._make_request('https://api.semanticscholar.org/datasets/v1/release/latest')
        if latest_release:
            print(f"最新发布ID: {latest_release.get('release_id', 'Unknown')}")
            print(f"可用数据集: {[d.get('name') for d in latest_release.get('datasets', [])]}")
    except Exception as e:
        print(f"获取最新发布信息失败: {e}")
    
    # 方法2: 尝试获取特定数据集信息
    try:
        print("\n尝试获取abstracts数据集信息...")
        r3 = s2_api._make_request('https://api.semanticscholar.org/datasets/v1/release/latest/dataset/abstracts')
        if r3:
            print(json.dumps(r3, indent=2))
            
            # 提取文件URL列表
            if 'files' in r3 and r3['files']:
                file_urls = r3['files']
                print(f"\n找到 {len(file_urls)} 个文件需要下载")
                
                # 创建下载器并开始下载
                downloader = AsyncFileDownloader()
                results = await downloader.download_files(file_urls)
                
                # 显示详细结果
                print("\n下载详情:")
                for result in results:
                    if isinstance(result, dict):
                        status = result.get("status", "unknown")
                        filename = result.get("filename", "unknown")
                        size = result.get("size", 0)
                        if status == "success":
                            print(f"✓ {filename} ({size / 1024 / 1024:.1f}MB)")
                        elif status == "skipped":
                            print(f"- {filename} (已存在)")
                        else:
                            error = result.get("error", "未知错误")
                            print(f"✗ {filename} - {error}")
            else:
                print("未找到文件列表")
        else:
            print("无法获取数据集信息")
    except Exception as e:
        print(f"获取数据集信息失败: {e}")
        
        # 方法3: 尝试使用requests直接获取
        print("\n尝试使用requests直接获取...")
        try:
            import requests
            response = requests.get('https://api.semanticscholar.org/datasets/v1/release/latest/dataset/abstracts')
            if response.status_code == 200:
                data = response.json()
                print("使用requests成功获取数据:")
                print(json.dumps(data, indent=2))
            else:
                print(f"requests请求失败: {response.status_code}")
        except Exception as req_e:
            print(f"requests请求异常: {req_e}")

if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())


