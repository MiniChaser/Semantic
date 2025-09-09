#!/usr/bin/env python3
"""
Scheduler startup script
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from semantic.scheduler.scheduler import main

if __name__ == "__main__":
    main()