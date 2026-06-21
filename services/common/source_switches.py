"""公共数据源开关注册表"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Dict, List, Optional

EEW_SOURCES = [
    "CUSTOM", "CEA", "CEA_PR", "CWA_FS", "SA", "KMA", "JMA", "EARLY_EST",
]

EEW_SOURCE_NAMES = {
    "CUSTOM": "自定义数据源",
    "CEA": "中国预警网",
    "CEA_PR": "地震局预警",
    "CWA_FS": "台湾气象署预警（Fan Studio）",
    "SA": "美国地质调查局预警",
    "KMA": "韩国气象厅预警",
    "JMA": "日本气象厅预警",
    "EARLY_EST": "Early-est 预警",
}

# Fan Studio fan_key -> EEW registry ID
FAN_EEW_KEY_MAP = {
    "cea": "CEA",
    "cea-pr": "CEA_PR",
    "cwa-eew": "CWA_FS",
    "jma": "JMA",
    "sa": "SA",
    "kma-eew": "KMA",
    "kma": "KMA",
}

# internal bus source_id -> EEW registry ID
INTERNAL_EEW_ID_MAP = {
    "custom": "CUSTOM",
    "early-est": "EARLY_EST",
    "earlyest": "EARLY_EST",
}


def is_active_eew_source(source_id: str) -> bool:
    """开关为开时才应写入融合列表。"""
    return is_eew_enabled(source_id)


LIST_SOURCES = [
    "cenc", "ningxia", "guangxi", "shanxi", "beijing", "yunnan",
    "hko", "usgs", "emsc", "bcsf", "gfz", "usp", "kma", "fssn",
    "bmkg", "geonet", "JMA", "INGV",
]

LIST_SOURCE_NAMES = {
    "cenc": "中国地震台网中心",
    "ningxia": "宁夏地震局",
    "guangxi": "广西地震局",
    "shanxi": "山西地震局",
    "beijing": "北京地震局",
    "yunnan": "云南地震局",
    "hko": "香港天文台",
    "usgs": "美国地质调查局",
    "emsc": "欧洲地中海地震中心",
    "bcsf": "法国中央地震研究所",
    "gfz": "德国地学研究中心",
    "usp": "巴西圣保罗大学地震信息",
    "kma": "韩国气象厅",
    "fssn": "FSSN",
    "bmkg": "印度尼西亚气象气候和地球物理局",
    "geonet": "新西兰 GeoNet",
    "JMA": "日本气象厅（P2PQuake）",
    "INGV": "意大利国家地球物理与火山学研究所",
}

# internal list bus id -> LIST registry id
INTERNAL_LIST_ID_MAP = {
    "bmkg": "bmkg",
    "geonet": "geonet",
}

from services.common.ports import DEFAULT_EEW_PORT as EEW_PUSH_PORT, DEFAULT_LIST_PORT as LIST_HTTP_PORT

CUSTOM_DATA_SOURCE_URL_KEY = "CUSTOM_DATA_SOURCE_URL"


def get_custom_data_source_url(settings_path: Optional[Path] = None) -> str:
    """读取自定义数据源 URL（空字符串表示关闭）。"""
    if settings_path is None:
        settings_path = _default_settings_path()
    if not settings_path.exists():
        return ""
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return (data.get(CUSTOM_DATA_SOURCE_URL_KEY) or "").strip()
    except Exception:
        return ""


def set_custom_data_source_url(url: str, settings_path: Optional[Path] = None) -> None:
    if settings_path is None:
        settings_path = _default_settings_path()
    data: Dict = {}
    if settings_path.exists():
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    data[CUSTOM_DATA_SOURCE_URL_KEY] = (url or "").strip()
    with open(settings_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _default_settings_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home())) / "custom-datasource-console"
    else:
        base = Path.home() / ".config" / "custom-datasource-console"
    base.mkdir(parents=True, exist_ok=True)
    return base / "settings.json"


class SourceSwitchRegistry:
    def __init__(self, channel: str):
        self.channel = channel
        self._lock = threading.RLock()
        self._enabled: Dict[str, bool] = {}
        self._initialize_defaults()

    def _initialize_defaults(self):
        sources = EEW_SOURCES if self.channel == "eew" else LIST_SOURCES
        for src in sources:
            self._enabled[src] = True

    def is_enabled(self, source_id: str) -> bool:
        with self._lock:
            return self._enabled.get(source_id, True)

    def set_enabled(self, source_id: str, enabled: bool) -> None:
        with self._lock:
            if source_id in self._enabled:
                self._enabled[source_id] = enabled

    def apply_patch(self, patch: Dict[str, bool]) -> List[str]:
        with self._lock:
            for source_id, enabled in patch.items():
                if source_id in self._enabled:
                    self._enabled[source_id] = enabled
        return []

    def snapshot(self) -> Dict[str, bool]:
        with self._lock:
            return dict(self._enabled)

    def all_sources(self) -> Dict[str, str]:
        if self.channel == "eew":
            return dict(EEW_SOURCE_NAMES)
        return dict(LIST_SOURCE_NAMES)


_eew_registry: Optional[SourceSwitchRegistry] = None
_list_registry: Optional[SourceSwitchRegistry] = None
_registry_lock = threading.Lock()


def get_registry(channel: str) -> SourceSwitchRegistry:
    global _eew_registry, _list_registry
    with _registry_lock:
        if channel == "eew":
            if _eew_registry is None:
                _eew_registry = SourceSwitchRegistry("eew")
            return _eew_registry
        if channel == "list":
            if _list_registry is None:
                _list_registry = SourceSwitchRegistry("list")
            return _list_registry
        raise ValueError(f"Unknown channel: {channel}")


def is_eew_enabled(source_id: str) -> bool:
    return get_registry("eew").is_enabled(source_id)


def is_list_enabled(source_id: str) -> bool:
    return get_registry("list").is_enabled(source_id)


def is_fan_eew_enabled(fan_key: str) -> bool:
    eew_id = FAN_EEW_KEY_MAP.get(fan_key)
    if not eew_id:
        return True
    return is_active_eew_source(eew_id)


def is_internal_eew_enabled(bus_source_id: str) -> bool:
    eew_id = INTERNAL_EEW_ID_MAP.get(bus_source_id.lower())
    if not eew_id:
        return True
    if eew_id == "CUSTOM" and not get_custom_data_source_url():
        return False
    return is_active_eew_source(eew_id)


def is_internal_list_enabled(bus_source_id: str) -> bool:
    list_id = INTERNAL_LIST_ID_MAP.get(bus_source_id.lower(), bus_source_id)
    return is_list_enabled(list_id)


def set_eew_enabled(source_id: str, enabled: bool) -> List[str]:
    get_registry("eew").set_enabled(source_id, enabled)
    return []


def apply_eew_patch(patch: Dict[str, bool]) -> List[str]:
    return get_registry("eew").apply_patch(patch)


def load_from_settings_path(settings_path: Optional[Path] = None) -> None:
    if settings_path is None:
        settings_path = _default_settings_path()
    if not settings_path.exists():
        return
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        eew_patch = data.get("eew_source_enabled", {})
        list_patch = data.get("list_source_enabled", {})
        if eew_patch:
            eew_patch = {
                k: v for k, v in eew_patch.items()
                if k not in _DEPRECATED_EEW_IDS
            }
            get_registry("eew").apply_patch(eew_patch)
        if list_patch:
            get_registry("list").apply_patch(list_patch)
        from services.common.source_filters import load_from_settings_path as load_filters
        load_filters(settings_path)
    except Exception:
        pass


def save_to_settings_path(settings_path: Optional[Path] = None) -> None:
    if settings_path is None:
        settings_path = _default_settings_path()
    data: Dict = {}
    if settings_path.exists():
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    data["eew_source_enabled"] = get_registry("eew").snapshot()
    data["list_source_enabled"] = get_registry("list").snapshot()
    from services.common.source_filters import get_filter_registry
    data.update(get_filter_registry().snapshot())
    with open(settings_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_from_env_or_settings() -> None:
    env_json = os.environ.get("SOURCE_SWITCHES_JSON", "")
    if env_json:
        try:
            data = json.loads(env_json)
            eew_patch = data.get("eew_source_enabled", {})
            list_patch = data.get("list_source_enabled", {})
            if eew_patch:
                get_registry("eew").apply_patch(eew_patch)
            if list_patch:
                get_registry("list").apply_patch(list_patch)
            from services.common.source_filters import load_from_env_or_settings as load_filters_env
            load_filters_env()
            return
        except Exception:
            pass
    load_from_settings_path()


def switches_snapshot_for_env() -> str:
    return json.dumps({
        "eew_source_enabled": get_registry("eew").snapshot(),
        "list_source_enabled": get_registry("list").snapshot(),
    }, ensure_ascii=False)
