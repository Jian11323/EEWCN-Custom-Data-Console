from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field

def _default_eew_base_dir() -> str:
    env = os.environ.get("EEW_BASE_DIR")
    if env:
        return env
    if os.environ.get("FUSED_MODE", "").strip() in ("1", "true", "yes"):
        try:
            from services.common.paths import get_cache_dir
            return str(get_cache_dir() / "eew")
        except ImportError:
            pass
    if os.name == "nt":
        return os.path.join(
            os.environ.get("APPDATA", os.path.expanduser("~")),
            "custom-datasource-console", "eew",
        )
    return "/opt/eew/fused_eew_api"


def _eew_subdir(name: str) -> str:
    return os.path.join(_default_eew_base_dir(), name)


@dataclass
class Config:
    """全局配置"""
    # WebSocket 上游地址
    ALL_WS_PRIMARY: str = "wss://ws.fanstudio.tech/all"
    ALL_WS_BACKUP: str = "wss://ws.fanstudio.hk/all"
    # Wolfx 聚合 EEW（CEA/CENC + JMA 上游切换时使用，见 WebSocketClientManager）
    WOLFX_ALL_EEW_URL: str = field(default_factory=lambda: os.environ.get(
        "WOLFX_ALL_EEW_URL", "wss://ws-api.wolfx.jp/all_eew"
    ))

    # 百度翻译配置（仅环境变量；未配置时禁用翻译并返回原文）
    BAIDU_APP_ID: str = field(default_factory=lambda: os.environ.get("BAIDU_APP_ID", ""))
    BAIDU_SECRET_KEY: str = field(default_factory=lambda: os.environ.get("BAIDU_SECRET_KEY", ""))
    
    # 目录配置（融合模式默认使用软件根目录 cache/logs）
    LOG_DIR: str = field(default_factory=lambda: os.environ.get("EEW_LOG_DIR", _eew_subdir("logs")))
    CACHE_DIR: str = field(default_factory=lambda: os.environ.get("EEW_CACHE_DIR", _eew_subdir("eew_cache")))
    TRANSLATION_CACHE_DIR: str = field(default_factory=lambda: os.environ.get(
        "EEW_TRANSLATION_DIR", _eew_subdir("translation")
    ))
    
    # 日志配置
    LOG_LEVEL: int = logging.INFO
    LOG_MAX_DAYS: int = 7
    
    # 缓存配置
    CACHE_MAX_AGE: int = 600  # 秒
    DEDUP_TTL: float = 60.0  # 秒
    
    # 性能配置
    MAX_WORKERS: int = 8
    FETCH_TIMEOUT: int = 5
