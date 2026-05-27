"""融合服务配置。"""

from __future__ import annotations

import os

from services.common.paths import ensure_dirs, get_app_root, get_cache_dir, get_data_dir, get_log_dir


def apply_fused_environment() -> None:
    """在导入 EEW/List 前设置环境变量。"""
    os.environ.setdefault("FUSED_MODE", "1")
    os.environ.setdefault("FUSED_SHARED_FAN", "1")
    root = get_app_root()
    os.environ.setdefault("LOG_DIR", str(get_log_dir()))
    os.environ.setdefault("CACHE_DIR", str(get_cache_dir()))
    os.environ.setdefault("DATA_DIR", str(get_data_dir()))
    os.environ.setdefault("EEW_LOG_DIR", str(get_log_dir() / "eew"))
    os.environ.setdefault("EEW_CACHE_DIR", str(get_cache_dir() / "eew"))
    os.environ.setdefault("EEW_TRANSLATION_DIR", str(get_cache_dir() / "eew" / "translation"))
    os.environ.setdefault("LIST_LOG_DIR", str(get_log_dir() / "list"))
    os.environ.setdefault("LIST_CACHE_DIR", str(get_cache_dir() / "list"))
    os.environ.setdefault("LIST_BASE_DIR", str(get_cache_dir() / "list"))
    ensure_dirs()
