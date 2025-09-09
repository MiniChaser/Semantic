#!/usr/bin/env python3
"""
调度器启动脚本
"""

import sys
import os

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from semantic.scheduler.scheduler import main

if __name__ == "__main__":
    main()