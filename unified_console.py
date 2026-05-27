#!/usr/bin/env python3
"""
自定义数据源控制台（兼容入口）
实际实现位于 console/ 包，请优先使用: python main.py
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from console.ui.main_window import run_app

if __name__ == "__main__":
    sys.exit(run_app())
