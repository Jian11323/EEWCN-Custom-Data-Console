"""国外数据源震级阈值与「中台日地区不过滤」配置。"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.common.regions import is_china_taiwan_japan
from services.common.source_switches import LIST_SOURCE_NAMES, _default_settings_path

LIST_NO_THRESHOLD_IDS = [
    "cenc", "ningxia", "guangxi", "shanxi", "beijing", "yunnan", "JMA", "cwa",
]

# 速报写入 8150 时不做震级阈值与地区过滤（收到即入库）
LIST_UNFILTERED_IDS = frozenset({"cwa", "JMA"})

LIST_FOREIGN_IDS = [
    "hko", "usgs", "emsc", "bcsf", "gfz", "usp", "kma", "fssn",
    "bmkg", "geonet", "INGV",
]

EEW_FOREIGN_IDS = ["EARLY_EST"]

DEFAULT_LIST_THRESHOLD = 4.5
DEFAULT_EEW_THRESHOLD = 4.5

# 速报事件 SOURCE 显示名与 registry ID 的额外别名（engine.SOURCE_NAMES 与 LIST_SOURCE_NAMES 不一致时）
_LIST_DISPLAY_ALIASES: Dict[str, str] = {
    "德国波茨坦地球科学研究中心": "gfz",
    "意大利国家地球物理与火山学研究": "INGV",
    "新西兰GeoNet": "geonet",
    "日本气象厅": "JMA",
    "台湾气象署": "cwa",
}

_display_to_id_lock = threading.Lock()
_display_to_id: Optional[Dict[str, str]] = None


def _build_display_to_id() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for sid, name in LIST_SOURCE_NAMES.items():
        m[name] = sid
    for display, sid in _LIST_DISPLAY_ALIASES.items():
        m[display] = sid
    return m


def get_list_display_to_id() -> Dict[str, str]:
    global _display_to_id
    with _display_to_id_lock:
        if _display_to_id is None:
            _display_to_id = _build_display_to_id()
        return _display_to_id


def register_list_display_aliases(extra: Dict[str, str]) -> None:
    """允许 list engine 启动时注册 SOURCE_NAMES 中的显示名。"""
    global _display_to_id
    with _display_to_id_lock:
        base = _build_display_to_id()
        base.update(extra)
        _display_to_id = base


def resolve_list_source_id(display_name: str) -> Optional[str]:
    if not display_name:
        return None
    return get_list_display_to_id().get(display_name)


def _location_text_from_list_event(event: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in ("placeName_zh", "LOCATION_C", "epicenter_tts"):
        val = event.get(key)
        if isinstance(val, str) and val:
            parts.append(val)
    return " ".join(parts)


def list_event_in_target_region(event: Dict[str, Any]) -> bool:
    """速报事件是否位于中国/台湾/日本。"""
    if _check_location_contains_taiwan(event):
        return True
    if _check_fssn_location_in_china_or_japan(event):
        return True
    text = _location_text_from_list_event(event)
    lat = lon = None
    try:
        lat = float(event.get("EPI_LAT", 0))
        lon = float(event.get("EPI_LON", 0))
    except (TypeError, ValueError):
        pass
    return is_china_taiwan_japan(text, lat, lon)


def eew_event_in_target_region(event: Dict[str, Any]) -> bool:
    """预警事件是否位于中国/台湾/日本。"""
    epicenter = event.get("epicenter") or event.get("placeName") or ""
    if not isinstance(epicenter, str):
        epicenter = str(epicenter)
    lat = lon = None
    try:
        lat = float(event.get("latitude", 0))
        lon = float(event.get("longitude", 0))
    except (TypeError, ValueError):
        pass
    return is_china_taiwan_japan(epicenter, lat, lon)


def _check_location_contains_taiwan(event: Dict[str, Any]) -> bool:
    try:
        for key in ("placeName_zh", "LOCATION_C", "epicenter_tts"):
            val = event.get(key)
            if isinstance(val, str) and "台湾" in val:
                return True
        return False
    except (ValueError, TypeError):
        return False


def _check_fssn_location_in_china_or_japan(event: Dict[str, Any]) -> bool:
    try:
        from services.common.source_switches import LIST_SOURCE_NAMES
        fssn_name = LIST_SOURCE_NAMES.get("fssn", "FSSN")
        if event.get("SOURCE") != fssn_name:
            return False
        china_keywords = [
            "中国", "北京", "上海", "广东", "四川", "云南", "新疆", "西藏",
            "内蒙古", "台湾", "香港", "澳门",
        ]
        japan_keywords = ["日本", "东京", "大阪", "北海道", "九州", "本州", "四国"]
        for key in ("placeName_zh", "LOCATION_C", "epicenter_tts"):
            val = event.get(key)
            if isinstance(val, str):
                for kw in china_keywords + japan_keywords:
                    if kw in val:
                        return True
        try:
            lat = float(event.get("EPI_LAT", 0))
            lon = float(event.get("EPI_LON", 0))
            if 18 <= lat <= 54 and 73 <= lon <= 135:
                return True
            if 24 <= lat <= 46 and 123 <= lon <= 146:
                return True
        except (TypeError, ValueError):
            pass
        return False
    except (ValueError, TypeError):
        return False


def list_event_exempt_from_threshold(event: Dict[str, Any]) -> bool:
    """台湾或 FSSN 国内/日本事件不受震级阈值限制。"""
    return _check_location_contains_taiwan(event) or _check_fssn_location_in_china_or_japan(event)


class SourceFilterRegistry:
    def __init__(self):
        self._lock = threading.RLock()
        self._list_threshold: Dict[str, float] = {}
        self._list_region_filter: Dict[str, bool] = {}
        self._eew_threshold: Dict[str, float] = {}
        self._eew_region_filter: Dict[str, bool] = {}
        self._init_defaults()

    def _init_defaults(self) -> None:
        for sid in LIST_FOREIGN_IDS:
            self._list_threshold[sid] = DEFAULT_LIST_THRESHOLD
            self._list_region_filter[sid] = False
        for sid in EEW_FOREIGN_IDS:
            self._eew_threshold[sid] = DEFAULT_EEW_THRESHOLD
            self._eew_region_filter[sid] = False

    def get_list_threshold(self, source_id: str) -> float:
        with self._lock:
            return self._list_threshold.get(source_id, DEFAULT_LIST_THRESHOLD)

    def get_eew_threshold(self, source_id: str) -> float:
        with self._lock:
            return self._eew_threshold.get(source_id, DEFAULT_EEW_THRESHOLD)

    def is_list_region_filter_enabled(self, source_id: str) -> bool:
        with self._lock:
            return self._list_region_filter.get(source_id, False)

    def is_eew_region_filter_enabled(self, source_id: str) -> bool:
        with self._lock:
            return self._eew_region_filter.get(source_id, False)

    def apply_patch(
        self,
        list_threshold: Optional[Dict[str, float]] = None,
        list_region_filter: Optional[Dict[str, bool]] = None,
        eew_threshold: Optional[Dict[str, float]] = None,
        eew_region_filter: Optional[Dict[str, bool]] = None,
    ) -> None:
        with self._lock:
            if list_threshold:
                for sid, val in list_threshold.items():
                    if sid in LIST_FOREIGN_IDS:
                        try:
                            self._list_threshold[sid] = float(val)
                        except (TypeError, ValueError):
                            pass
            if list_region_filter:
                for sid, val in list_region_filter.items():
                    if sid in LIST_FOREIGN_IDS:
                        self._list_region_filter[sid] = bool(val)
            if eew_threshold:
                for sid, val in eew_threshold.items():
                    if sid in EEW_FOREIGN_IDS:
                        try:
                            self._eew_threshold[sid] = float(val)
                        except (TypeError, ValueError):
                            pass
            if eew_region_filter:
                for sid, val in eew_region_filter.items():
                    if sid in EEW_FOREIGN_IDS:
                        self._eew_region_filter[sid] = bool(val)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "list_source_threshold": dict(self._list_threshold),
                "list_source_region_filter": dict(self._list_region_filter),
                "eew_source_threshold": dict(self._eew_threshold),
                "eew_source_region_filter": dict(self._eew_region_filter),
            }

    def should_include_list_event(self, event: Dict[str, Any]) -> Tuple[bool, str]:
        """
        判断速报事件是否应写入融合列表。
        返回 (include, reason)；reason 用于 debug 日志。
        """
        display = event.get("SOURCE", "")
        source_id = resolve_list_source_id(display)
        if source_id is None:
            return True, ""

        if source_id in LIST_UNFILTERED_IDS or source_id in LIST_NO_THRESHOLD_IDS:
            return True, ""

        if source_id not in LIST_FOREIGN_IDS:
            return True, ""

        if self.is_list_region_filter_enabled(source_id):
            if not list_event_in_target_region(event):
                return False, "region_filter"

        if list_event_exempt_from_threshold(event):
            return True, ""

        try:
            mag = float(event.get("M", 0))
        except (TypeError, ValueError):
            mag = 0.0
        threshold = self.get_list_threshold(source_id)
        if mag < threshold:
            return False, f"threshold_M{threshold}"
        return True, ""

    def should_include_eew_event(
        self, source_id: str, event: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """判断国外预警是否写入融合列表（5000）。"""
        if source_id not in EEW_FOREIGN_IDS:
            return True, ""

        if self.is_eew_region_filter_enabled(source_id):
            if not eew_event_in_target_region(event):
                return False, "region_filter"

        if eew_event_in_target_region(event):
            return True, ""

        try:
            mag = float(event.get("magnitude", 0))
        except (TypeError, ValueError):
            mag = 0.0
        threshold = self.get_eew_threshold(source_id)
        if mag < threshold:
            return False, f"threshold_M{threshold}"
        return True, ""


_registry: Optional[SourceFilterRegistry] = None
_registry_lock = threading.Lock()


def get_filter_registry() -> SourceFilterRegistry:
    global _registry
    with _registry_lock:
        if _registry is None:
            _registry = SourceFilterRegistry()
        return _registry


def load_from_settings_path(settings_path: Optional[Path] = None) -> None:
    if settings_path is None:
        settings_path = _default_settings_path()
    if not settings_path.exists():
        return
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        reg = get_filter_registry()
        reg.apply_patch(
            list_threshold=data.get("list_source_threshold"),
            list_region_filter=data.get("list_source_region_filter"),
            eew_threshold=data.get("eew_source_threshold"),
            eew_region_filter=data.get("eew_source_region_filter"),
        )
    except Exception:
        pass


def save_to_settings_path(settings_path: Optional[Path] = None) -> None:
    if settings_path is None:
        settings_path = _default_settings_path()
    data: Dict[str, Any] = {}
    if settings_path.exists():
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    snap = get_filter_registry().snapshot()
    data.update(snap)
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    with open(settings_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_from_env_or_settings() -> None:
    env_json = os.environ.get("SOURCE_FILTERS_JSON", "")
    if env_json:
        try:
            data = json.loads(env_json)
            get_filter_registry().apply_patch(
                list_threshold=data.get("list_source_threshold"),
                list_region_filter=data.get("list_source_region_filter"),
                eew_threshold=data.get("eew_source_threshold"),
                eew_region_filter=data.get("eew_source_region_filter"),
            )
            return
        except Exception:
            pass
    load_from_settings_path()


def filters_snapshot_for_env() -> str:
    return json.dumps(get_filter_registry().snapshot(), ensure_ascii=False)
