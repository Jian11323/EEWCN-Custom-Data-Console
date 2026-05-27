"""统一加载 data/ 目录下的地名修正 JSON。"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.common.paths import get_data_dir

logger = logging.getLogger("common.regions")

_lock = threading.RLock()
_cache: Dict[str, Any] = {}


def _load_json(name: str) -> Any:
    path = get_data_dir() / name
    if not path.is_file():
        logger.warning("地名文件不存在: %s", path)
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("加载地名文件 %s 失败: %s", path, e)
        return None


def _regions_list_from_json(data: Any) -> List[Dict[str, Any]]:
    """支持 {\"regions\": [...]} 或顶层数组。"""
    if isinstance(data, dict):
        regions = data.get("regions")
        return regions if isinstance(regions, list) else []
    if isinstance(data, list):
        return data
    return []


def get_fe_fix_regions() -> List[Dict[str, Any]]:
    """List 速报 fe_fix_region_data.json"""
    with _lock:
        if "fe_fix" not in _cache:
            _cache["fe_fix"] = _regions_list_from_json(_load_json("fe_fix_region_data.json"))
        return _cache["fe_fix"]


def get_korea_regions() -> List[Dict[str, Any]]:
    """KMA-EEW korea_region_data.json"""
    with _lock:
        if "korea" not in _cache:
            _cache["korea"] = _regions_list_from_json(_load_json("korea_region_data.json"))
        return _cache["korea"]


def get_sa_regions() -> List[Dict[str, Any]]:
    """SA/USGS 预警 sa_region_data.json"""
    with _lock:
        if "sa" not in _cache:
            _cache["sa"] = _regions_list_from_json(_load_json("sa_region_data.json"))
        return _cache["sa"]


def get_taiwan_regions() -> List[Dict[str, Any]]:
    """CWA-EEW taiwan_region_data.json（mapping 含 lat_min/lat_max/lon_min/lon_max/name）"""
    with _lock:
        if "taiwan" not in _cache:
            data = _load_json("taiwan_region_data.json")
            if isinstance(data, dict):
                _cache["taiwan"] = data.get("mapping") or data.get("regions") or []
            elif isinstance(data, list):
                _cache["taiwan"] = data
            else:
                _cache["taiwan"] = []
        return _cache["taiwan"]


def match_region_by_coords(
    regions: List[Dict[str, Any]],
    lat: float,
    lon: float,
    *,
    lat_key: str = "lat",
    lon_key: str = "lon",
    name_key: str = "name",
    bbox_keys: Optional[tuple] = None,
) -> str:
    """按坐标匹配区域名。支持 bbox 或 lat/lon 点列表。"""
    if bbox_keys:
        min_lat_k, max_lat_k, min_lon_k, max_lon_k, nk = bbox_keys
        best = ""
        best_dist = float("inf")
        for r in regions:
            try:
                mn_lat = float(r[min_lat_k])
                mx_lat = float(r[max_lat_k])
                mn_lon = float(r[min_lon_k])
                mx_lon = float(r[max_lon_k])
            except (KeyError, TypeError, ValueError):
                continue
            if mn_lat <= lat <= mx_lat and mn_lon <= lon <= mx_lon:
                return str(r.get(nk, r.get(name_key, "")))
            cx = (mn_lat + mx_lat) / 2
            cy = (mn_lon + mx_lon) / 2
            d = (lat - cx) ** 2 + (lon - cy) ** 2
            if d < best_dist:
                best_dist = d
                best = str(r.get(nk, r.get(name_key, "")))
        return best

    best = ""
    best_dist = float("inf")
    for r in regions:
        try:
            rlat = float(r[lat_key])
            rlon = float(r[lon_key])
        except (KeyError, TypeError, ValueError):
            continue
        d = (lat - rlat) ** 2 + (lon - rlon) ** 2
        if d < best_dist:
            best_dist = d
            best = str(r.get(name_key, ""))
    return best


def clear_region_cache() -> None:
    with _lock:
        _cache.clear()


def is_china_taiwan_japan(
    region: str = "",
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> bool:
    """判断是否为中国大陆、台湾或日本（用于国外源地区过滤豁免）。"""
    if region:
        region_lower = region.lower()
        if "taiwan" in region_lower or "台湾" in region:
            return True
        if "japan" in region_lower or "日本" in region:
            return True
        china_keywords = [
            "china", "chinese", "中国", "北京", "上海", "广东", "四川", "云南",
            "新疆", "西藏", "内蒙古", "香港", "澳门", "xinjiang", "xizang", "tibet",
        ]
        if any(kw in region_lower for kw in china_keywords):
            return True

    if latitude is not None and longitude is not None:
        try:
            lat, lon = float(latitude), float(longitude)
            if 18 <= lat <= 54 and 73 <= lon <= 135:
                return True
            if 24 <= lat <= 46 and 123 <= lon <= 146:
                return True
        except (TypeError, ValueError):
            pass
    return False
