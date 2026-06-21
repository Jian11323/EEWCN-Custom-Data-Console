"""List 运行时状态单例（替代模块级全局变量的集中管理）。"""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Optional

from flask import Flask

from services.fused.list.config import Config


def _default_error_stats() -> dict:
    template = {
        "fetch_errors": 0,
        "parse_errors": 0,
        "last_error": None,
        "consecutive_failures": 0,
        "backoff_until": None,
    }
    return {
        name: dict(template)
        for name in ("JMA", "GEONET", "BMKG", "INGV", "FAN_STUDIO", "P2PQUAKE", "INTERNAL_WS")
    }


def _default_cache_state() -> dict:
    template = {"id": "", "count": 0, "latest_id": "", "latest_time": "", "last_success": None}
    return {name: dict(template) for name in ("JMA", "GEONET", "BMKG", "INGV")}


class ListRuntimeState:
    """List 融合服务运行时可变状态。"""

    _instance: Optional["ListRuntimeState"] = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        self.logger = logging.getLogger("services.fused.list")
        self.app = Flask(__name__)
        self.fused_events = deque(maxlen=Config.MAX_FUSED_EVENTS)
        self.event_dict_by_key: dict = {}
        self.fused_data_lock = threading.Lock()
        self.translation_cache: dict = {}
        self.location_regions = None
        self.cache_lock = threading.Lock()
        self.fanstudio_raw_cache: dict = {}
        self.fanstudio_cache_lock = threading.Lock()
        self.error_stats = _default_error_stats()
        self.cache_state = _default_cache_state()
        self._http_thread_pool = None
        self._http_thread_pool_lock = threading.Lock()
        self._http_thread_pool_created_time = None
        self._http_thread_pool_task_count = 0
        self._http_thread_pool_health_check_thread = None
        self._http_thread_pool_health_check_stop = threading.Event()
        self._http_thread_pool_current_workers = 4
        self._fanstudio_list_cmd_lock = threading.Lock()

    @classmethod
    def get(cls) -> "ListRuntimeState":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_for_tests(cls) -> None:
        with cls._instance_lock:
            cls._instance = cls()
