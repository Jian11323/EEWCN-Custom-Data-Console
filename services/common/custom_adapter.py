"""自定义数据源 JSON 解析（平铺 / Data 嵌套，与滚动字幕-公开版约定一致）。"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _resolve_organization(*dicts: Optional[Dict[str, Any]]) -> str:
    """机构名：优先 JSON 的 source，其次 sourceName，否则「自定义」。"""
    for d in dicts:
        if not isinstance(d, dict):
            continue
        for key in ("source", "sourceName"):
            raw = d.get(key)
            if raw is None:
                continue
            name = str(raw).strip()
            if name:
                return name
    return "自定义"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_custom_payload(raw_data: Any) -> Optional[Dict[str, Any]]:
    """
    解析原始 JSON。支持格式 A（平铺）或格式 B（Data 嵌套）；数组取首条。
    返回 None 表示无效或缺少 placeName。
    """
    if raw_data is None:
        return None
    data = raw_data
    if isinstance(data, list):
        if not data:
            return None
        data = data[0]
    if not isinstance(data, dict):
        return None
    if "Data" in data and isinstance(data.get("Data"), dict):
        return _parse_nested(data)
    return _parse_flat(data)


def _parse_flat(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    place_name = (data.get("placeName") or "").strip()
    if not place_name:
        return None
    report_num = data.get("reportNum")
    updates = None
    if report_num is not None:
        try:
            updates = int(report_num)
        except (TypeError, ValueError):
            updates = None
    organization = _resolve_organization(data)
    event_id = (data.get("eventID") or data.get("eventId") or data.get("id") or "").strip()
    return {
        "place_name": place_name,
        "magnitude": _safe_float(data.get("magnitude", 0)),
        "latitude": _safe_float(data.get("latitude", 0)),
        "longitude": _safe_float(data.get("longitude", 0)),
        "depth": _safe_float(data.get("depth", 0)),
        "shock_time": str(data.get("shockTime", "") or "").strip(),
        "organization": organization,
        "updates": updates,
        "event_id": event_id,
        "raw_data": data,
    }


def _parse_nested(root: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = root["Data"]
    if not isinstance(data, dict):
        return None
    place_name = (data.get("placeName") or "").strip()
    if not place_name:
        return None
    updates = data.get("updates")
    if updates is not None:
        try:
            updates = int(updates)
        except (TypeError, ValueError):
            updates = None
    event_id = (data.get("id") or data.get("eventID") or data.get("eventId") or "").strip()
    organization = _resolve_organization(data, root)
    return {
        "place_name": place_name,
        "magnitude": _safe_float(data.get("magnitude", 0)),
        "latitude": _safe_float(data.get("latitude", 0)),
        "longitude": _safe_float(data.get("longitude", 0)),
        "depth": _safe_float(data.get("depth", 0)),
        "shock_time": str(data.get("shockTime", "") or "").strip(),
        "organization": organization,
        "updates": updates,
        "event_id": event_id,
        "raw_data": root,
    }
