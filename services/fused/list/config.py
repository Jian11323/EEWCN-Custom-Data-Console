from __future__ import annotations

import os
import threading

from services.common.source_filters import register_list_display_aliases

def _default_list_base_dir() -> str:
    env = os.environ.get("LIST_BASE_DIR")
    if env:
        return env
    if os.environ.get("FUSED_MODE", "").strip() in ("1", "true", "yes"):
        try:
            from services.common.paths import get_cache_dir
            return str(get_cache_dir() / "list")
        except ImportError:
            pass
    if os.name == "nt":
        return os.path.join(
            os.environ.get("APPDATA", os.path.expanduser("~")),
            "custom-datasource-console", "list",
        )
    return "/opt/eew/fused_list"


class Config:
    """配置类：包含所有配置项和常量"""

    # 基础目录配置
    BASE_DIR = _default_list_base_dir()
    LOG_DIR = os.environ.get("LIST_LOG_DIR", os.path.join(BASE_DIR, 'logs'))
    TRANSLATION_DIR = os.environ.get("LIST_TRANSLATION_DIR", os.path.join(BASE_DIR, 'translation_cache'))
    CACHE_DIR = os.environ.get("LIST_CACHE_DIR", os.path.join(BASE_DIR, 'cache'))

    # 日志保留天数
    LOG_RETENTION_DAYS = 7

    # 最大融合事件数量
    MAX_FUSED_EVENTS = 300

    # 熔断器配置
    CIRCUIT_BREAKER_THRESHOLD = 5
    BACKOFF_BASE_DELAY = 30
    BACKOFF_MAX_DELAY = 300

    # 阈值配置
    THRESHOLD_MAG = 4.5

    # 缓存配置
    MAX_CACHE_PER_SOURCE = 25

# 外部数据源API URL配置
API_URLS = {
    'JMA': "https://api.p2pquake.net/v2/history?codes=551&limit=30",  # 日本气象厅 - 获取全部数据
    'GEONET': "https://api.geonet.org.nz/quake?MMI=-1",  # 新西兰GeoNet - 获取全部数据
    'BMKG': "https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json",  # 印度尼西亚BMKG - 获取全部数据
    'INGV': "https://api.terraquakeapi.com/v1/earthquakes/recent?limit=50",  # 意大利国家地球物理与火山学研究 - TerraQuake API / recent
}

# FanStudio WebSocket配置
FAN_STUDIO_WS_URL_PRIMARY = "wss://ws.fanstudio.tech/all"  # 主服务器
FAN_STUDIO_WS_URL_BACKUP = "wss://ws.fanstudio.hk/all"  # 备用服务器

# P2PQuake WebSocket配置（仅地震情报 code 551）
P2PQUAKE_WS_URL = "wss://api.p2pquake.net/v2/ws"

# 内网聚合 WebSocket（BMKG / GeoNet，消息格式同 FanStudio 文档）
# 内网机构数据源改经 internal event bus 接入（不再使用 1450 WS）

_shared_fan_conn = None

FAN_STUDIO_SWITCH_CONFIG = {
    "current_url": FAN_STUDIO_WS_URL_PRIMARY,
    "is_using_backup": False,
    "primary_fail_count": 0,
    "primary_fail_threshold": 20,
    "primary_retry_interval": 30,
    "switch_to_backup_time": None,
    "backup_check_interval": 1800,
    "last_backup_check": None,
    "ws_instance": None,
    "last_event_times": {},
    "manual_lock": False,
    "lock": threading.Lock()
}

# 数据源名称映射表
SOURCE_NAMES = {
    "JMA": "日本气象厅",
    "CWA": "台湾气象署",
    "CENC": "中国地震台网中心",
    "NINGXIA": "宁夏地震局",
    "GUANGXI": "广西地震局",
    "SHANXI": "山西地震局",
    "BEIJING": "北京地震局",
    "YUNNAN": "云南地震局",
    "HKO": "香港天文台",
    "USGS": "美国地质调查局",
    "EMSC": "欧洲地中海地震中心",
    "BCSF": "法国中央地震研究所",
    "GFZ": "德国波茨坦地球科学研究中心",
    "USP": "巴西圣保罗大学地震信息",
    "KMA": "韩国气象厅",
    "FSSN": "FSSN",
    "GEONET": "新西兰GeoNet",
    "BMKG": "印度尼西亚气象气候和地球物理局",
    "INGV": "意大利国家地球物理与火山学研究",
}

# 百度翻译API配置（仅环境变量；未配置时禁用翻译并返回原文）
BAIDU_TRANSLATE_CONFIG = {
    'APP_ID': os.environ.get("BAIDU_APP_ID", ""),
    'SECRET_KEY': os.environ.get("BAIDU_SECRET_KEY", ""),
}

# 排除的数据源：非速报条目，或改由 EEW 融合（cwa-eew/kma-eew）处理
# cwa 速报仍由 cwa / cwalist_response 提供
EXCLUDED_SOURCES = [
    'weatheralarm', 'tsunami', 'cea', 'cea-pr', 'cwa-test', 'cwa-eew', 'kma-eew',
    'fssn-cmt', 'sichuan', 'sa',
]
# 无阈值数据源（显示名，与 LIST_NO_THRESHOLD_IDS 对应）
NO_THRESHOLD_SOURCES = [
    SOURCE_NAMES.get("CENC"),
    SOURCE_NAMES.get("JMA"),
    SOURCE_NAMES.get("CWA"),
    SOURCE_NAMES.get("YUNNAN"),
    "宁夏地震局",
    "广西地震局",
    "山西地震局",
    "北京地震局",
]

from services.common.source_filters import (
    LIST_FOREIGN_IDS,
    LIST_UNFILTERED_IDS,
    get_filter_registry,
    register_list_display_aliases,
    resolve_list_source_id,
)

_register_aliases = {}
for _k, _v in SOURCE_NAMES.items():
    _sid = _k if _k in ("JMA", "INGV") else _k.lower()
    _register_aliases[_v] = _sid
register_list_display_aliases(_register_aliases)

# CENC/CWA/FSSN：不写入 fanstudio 原始磁盘/内存 deque
FANSTUDIO_NO_RAW_CACHE_SOURCES = frozenset({'cenc', 'cwa', 'fssn'})


def _fanstudio_list_cmd_gap_from_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


FANSTUDIO_LIST_CMD_GAP_SEC = _fanstudio_list_cmd_gap_from_env("FANSTUDIO_LIST_CMD_GAP_SEC", 3.0)
FANSTUDIO_LIST_CMD_GAP_PER_100 = _fanstudio_list_cmd_gap_from_env("FANSTUDIO_LIST_CMD_GAP_PER_100", 2.0)
FANSTUDIO_LIST_CMD_GAP_MAX_SEC = _fanstudio_list_cmd_gap_from_env("FANSTUDIO_LIST_CMD_GAP_MAX_SEC", 8.0)
FANSTUDIO_LIST_CMD_STATE = {"timer": None, "phase": 0}
