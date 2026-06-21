from __future__ import annotations

import logging
import threading
from collections import deque

from flask import Flask

from services.fused.list.config import Config
from services.fused.list.runtime_state import ListRuntimeState

# 默认 logger，setup_logging 后会重新绑定同一命名空间
logger = logging.getLogger("services.fused.list")

# ============================================================================
# 全局变量和存储（ListRuntimeState 单例的模块级别名，保持 import 兼容）
# ============================================================================
runtime = ListRuntimeState.get()

# Flask应用实例（历史 HTTP 8150）
app = runtime.app

# 地震事件数据存储
fused_events = runtime.fused_events
event_dict_by_key = runtime.event_dict_by_key
fused_data_lock = runtime.fused_data_lock
# 兼容旧变量名
fused_events_no_threshold = fused_events
event_dict_by_key_no_threshold = event_dict_by_key
fused_data_lock_no_threshold = fused_data_lock
app_no_threshold = app

# 缓存相关
translation_cache = runtime.translation_cache
location_regions = runtime.location_regions
cache_lock = runtime.cache_lock

# FanStudio原始数据缓存（按数据源分别缓存）
fanstudio_raw_cache = runtime.fanstudio_raw_cache
fanstudio_cache_lock = runtime.fanstudio_cache_lock

# 错误统计
error_stats = runtime.error_stats

# 缓存状态
cache_state = runtime.cache_state

# 全局线程池实例（重用以防止内存泄漏）
_http_thread_pool = runtime._http_thread_pool
_http_thread_pool_lock = runtime._http_thread_pool_lock
_http_thread_pool_created_time = runtime._http_thread_pool_created_time
_http_thread_pool_task_count = runtime._http_thread_pool_task_count
_http_thread_pool_health_check_thread = runtime._http_thread_pool_health_check_thread
_http_thread_pool_health_check_stop = runtime._http_thread_pool_health_check_stop
_http_thread_pool_current_workers = runtime._http_thread_pool_current_workers

_fanstudio_list_cmd_lock = runtime._fanstudio_list_cmd_lock

__all__ = [
    "runtime",
    "logger",
    "app",
    "fused_events",
    "event_dict_by_key",
    "fused_data_lock",
    "translation_cache",
    "location_regions",
    "cache_lock",
    "fanstudio_raw_cache",
    "fanstudio_cache_lock",
    "error_stats",
    "cache_state",
    "_fanstudio_list_cmd_lock",
]
