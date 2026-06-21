"""List 兼容门面：自子模块 re-export。"""

from __future__ import annotations

import warnings

warnings.filterwarnings("ignore", message=".*InsecureRequestWarning.*")

from services.fused.list.api import APIHandler, app
from services.fused.list.cache import CacheManager
from services.fused.list.config import (
    API_URLS,
    BAIDU_TRANSLATE_CONFIG,
    Config,
    EXCLUDED_SOURCES,
    FANSTUDIO_LIST_CMD_GAP_MAX_SEC,
    FANSTUDIO_LIST_CMD_GAP_PER_100,
    FANSTUDIO_LIST_CMD_GAP_SEC,
    FANSTUDIO_LIST_CMD_STATE,
    FANSTUDIO_NO_RAW_CACHE_SOURCES,
    FAN_STUDIO_SWITCH_CONFIG,
    FAN_STUDIO_WS_URL_BACKUP,
    FAN_STUDIO_WS_URL_PRIMARY,
    NO_THRESHOLD_SOURCES,
    P2PQUAKE_WS_URL,
    SOURCE_NAMES,
    _shared_fan_conn,
)
from services.fused.list.fusion import FusionHandler
from services.fused.list.logging_mgr import LogManager
from services.fused.list.main_handler import MainHandler
from services.fused.list.pool import ThreadPoolManager
from services.fused.list.sources.base import DataSourceBase
from services.fused.list.sources.bmkg import BMKGSource
from services.fused.list.sources.geonet import GEONETSource
from services.fused.list.sources.ingv import INGVSource
from services.fused.list.sources.jma import JMASource
from services.fused.list.sources.processor_maps import (
    DataSourceProcessor,
    FAN_STUDIO_PARSERS,
    INTERNAL_WS_PARSERS,
)
from services.fused.list.state import (
    _fanstudio_list_cmd_lock,
    app_no_threshold,
    cache_lock,
    cache_state,
    error_stats,
    event_dict_by_key,
    event_dict_by_key_no_threshold,
    fanstudio_cache_lock,
    fanstudio_raw_cache,
    fused_data_lock,
    fused_data_lock_no_threshold,
    fused_events,
    fused_events_no_threshold,
    location_regions,
    logger,
    translation_cache,
)
from services.fused.list.translation import TranslationService
from services.fused.list.upstream.ws_handler import WebSocketHandler
from services.fused.list.utils import Utils

__all__ = [
    "APIHandler",
    "API_URLS",
    "BAIDU_TRANSLATE_CONFIG",
    "BMKGSource",
    "CacheManager",
    "Config",
    "DataSourceBase",
    "DataSourceProcessor",
    "EXCLUDED_SOURCES",
    "FANSTUDIO_LIST_CMD_GAP_MAX_SEC",
    "FANSTUDIO_LIST_CMD_GAP_PER_100",
    "FANSTUDIO_LIST_CMD_GAP_SEC",
    "FANSTUDIO_LIST_CMD_STATE",
    "FANSTUDIO_NO_RAW_CACHE_SOURCES",
    "FAN_STUDIO_PARSERS",
    "FAN_STUDIO_SWITCH_CONFIG",
    "FAN_STUDIO_WS_URL_BACKUP",
    "FAN_STUDIO_WS_URL_PRIMARY",
    "FusionHandler",
    "GEONETSource",
    "INGVSource",
    "INTERNAL_WS_PARSERS",
    "JMASource",
    "LogManager",
    "MainHandler",
    "NO_THRESHOLD_SOURCES",
    "P2PQUAKE_WS_URL",
    "SOURCE_NAMES",
    "ThreadPoolManager",
    "TranslationService",
    "Utils",
    "WebSocketHandler",
    "app",
    "app_no_threshold",
    "cache_lock",
    "cache_state",
    "error_stats",
    "event_dict_by_key",
    "event_dict_by_key_no_threshold",
    "fanstudio_cache_lock",
    "fanstudio_raw_cache",
    "fused_data_lock",
    "fused_data_lock_no_threshold",
    "fused_events",
    "fused_events_no_threshold",
    "location_regions",
    "logger",
    "translation_cache",
    "_shared_fan_conn",
]
