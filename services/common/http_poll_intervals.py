"""HTTP 数据源轮询间隔配置（秒）。"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple

HTTP_POLL_INTERVALS_KEY = "http_poll_intervals"

# source_key -> (default_seconds, display_name)
HTTP_POLL_SOURCES: Dict[str, Tuple[float, str]] = {
    "custom": (1.0, "自定义数据源"),
    "early_est": (5.0, "Early-est 预警"),
    "geonet": (5.0, "GeoNet 速报"),
    "bmkg": (5.0, "BMKG 速报"),
    "ingv": (1.0, "INGV 速报"),
}

MIN_INTERVAL = 0.5
MAX_INTERVAL = 300.0

_lock = threading.RLock()


def _default_settings_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home())) / "custom-datasource-console"
    else:
        base = Path.home() / ".config" / "custom-datasource-console"
    base.mkdir(parents=True, exist_ok=True)
    return base / "settings.json"


def clamp_interval(value: float) -> float:
    return max(MIN_INTERVAL, min(MAX_INTERVAL, float(value)))


def parse_interval(value, default: float) -> float:
    try:
        return clamp_interval(float(value))
    except (TypeError, ValueError):
        return default


def get_poll_interval(source_key: str, settings_path: Optional[Path] = None) -> float:
    """读取指定 HTTP 源的轮询间隔（秒）。"""
    default = HTTP_POLL_SOURCES.get(source_key, (5.0, ""))[0]
    if settings_path is None:
        settings_path = _default_settings_path()
    if not settings_path.exists():
        return default
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return default
    intervals = data.get(HTTP_POLL_INTERVALS_KEY, {})
    if not isinstance(intervals, dict):
        return default
    return parse_interval(intervals.get(source_key), default)


def get_all_intervals(settings_path: Optional[Path] = None) -> Dict[str, float]:
    return {key: get_poll_interval(key, settings_path) for key in HTTP_POLL_SOURCES}


def set_poll_intervals(
    patch: Dict[str, float],
    settings_path: Optional[Path] = None,
) -> Dict[str, float]:
    """校验并写入轮询间隔，返回完整快照。"""
    if settings_path is None:
        settings_path = _default_settings_path()
    with _lock:
        data: Dict = {}
        if settings_path.exists():
            try:
                with open(settings_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        current = data.get(HTTP_POLL_INTERVALS_KEY, {})
        if not isinstance(current, dict):
            current = {}
        merged = dict(current)
        for key, value in patch.items():
            if key not in HTTP_POLL_SOURCES:
                continue
            merged[key] = parse_interval(value, HTTP_POLL_SOURCES[key][0])
        data[HTTP_POLL_INTERVALS_KEY] = merged
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return {key: parse_interval(merged.get(key), HTTP_POLL_SOURCES[key][0]) for key in HTTP_POLL_SOURCES}
