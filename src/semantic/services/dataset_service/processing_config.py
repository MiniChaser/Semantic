"""
Processing configuration with dynamic worker calculation
"""

import logging
import multiprocessing
import os
from typing import Optional
from ...database.connection import DatabaseManager


def calculate_optimal_workers(db_manager: DatabaseManager) -> int:
    """
    Calculate optimal number of worker processes based on system resources

    Considers:
    - CPU cores (leave 1 for system)
    - Database max_connections (use 70%)
    - Available memory (2GB per worker)
    - Hard limit of 16 to avoid over-parallelism

    Returns:
        Optimal number of workers (minimum 2)
    """
    logger = logging.getLogger(__name__)

    # 1. CPU cores (保留1个给系统)
    cpu_count = multiprocessing.cpu_count()
    max_cpu_workers = max(1, cpu_count - 1)

    logger.info(f"CPU cores: {cpu_count}, max CPU workers: {max_cpu_workers}")

    # 2. Database connection limit
    try:
        result = db_manager.fetch_one("SHOW max_connections")
        max_connections = int(result['max_connections']) if result else 100
        # 保留30%给其他连接,每个worker需要1个连接
        max_db_workers = max(1, int(max_connections * 0.7))
        logger.info(f"DB max_connections: {max_connections}, max DB workers: {max_db_workers}")
    except Exception as e:
        logger.warning(f"Failed to get max_connections: {e}, using default 20")
        max_db_workers = 20

    # 3. Memory limit (每个worker约2GB)
    try:
        import psutil
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        max_memory_workers = max(1, int(available_memory_gb / 2))
        logger.info(f"Available memory: {available_memory_gb:.1f}GB, max memory workers: {max_memory_workers}")
    except ImportError:
        logger.warning("psutil not installed, skipping memory check")
        max_memory_workers = 999  # Don't limit by memory if psutil not available
    except Exception as e:
        logger.warning(f"Failed to check memory: {e}")
        max_memory_workers = 8

    # 4. 取最小值,但不超过16(硬性上限)
    optimal_workers = min(
        max_cpu_workers,
        max_db_workers,
        max_memory_workers,
        16  # 硬性上限
    )

    # 5. 至少2个worker
    optimal_workers = max(2, optimal_workers)

    logger.info(f"Calculated optimal workers: {optimal_workers}")

    return optimal_workers


class ProcessingConfig:
    """
    Processing configuration with environment variable support

    Environment variables:
    - MAX_WORKERS: Override automatic worker calculation
    - CHUNK_SIZE: Batch size for temp table writes (default: 100000)
    - ENABLE_PARALLEL: Enable/disable parallel processing (default: true)
    - FILE_TIMEOUT: Timeout for single file processing in seconds (default: 600)
    """

    def __init__(self):
        self.logger = self._setup_logger()

        # Worker count (None = auto-calculate)
        if os.getenv('MAX_WORKERS'):
            self.max_workers = int(os.getenv('MAX_WORKERS'))
            self.logger.info(f"MAX_WORKERS set from environment: {self.max_workers}")
        else:
            self.max_workers = None  # Will be calculated lazily

        # Chunk size for batch processing
        self.chunk_size = int(os.getenv('CHUNK_SIZE', '100000'))

        # Enable parallel processing
        self.enable_parallel = os.getenv('ENABLE_PARALLEL', 'true').lower() in ('true', '1', 'yes')

        # Timeout for single file
        self.file_timeout = int(os.getenv('FILE_TIMEOUT', '600'))

        self.logger.info("Processing configuration:")
        self.logger.info(f"  - Chunk size: {self.chunk_size:,}")
        self.logger.info(f"  - Parallel enabled: {self.enable_parallel}")
        self.logger.info(f"  - File timeout: {self.file_timeout}s")

    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(f'{__name__}.ProcessingConfig')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def get_max_workers(self, db_manager: DatabaseManager) -> int:
        """
        Get maximum number of workers (lazy calculation)

        Args:
            db_manager: Database manager for querying connection limits

        Returns:
            Maximum worker count
        """
        if self.max_workers is None:
            self.max_workers = calculate_optimal_workers(db_manager)
            self.logger.info(f"Auto-calculated max workers: {self.max_workers}")
        return self.max_workers

    def should_use_parallel(self) -> bool:
        """Check if parallel processing should be used"""
        return self.enable_parallel and (self.max_workers is None or self.max_workers > 1)
