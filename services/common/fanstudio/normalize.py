"""Fan Studio /all 消息格式规范化（v2.1 initial/update + 旧版 start_all 兼容）。"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple

# 非数据源的控制消息类型
_CONTROL_TYPES = frozenset({"heartbeat", "pong", "ping", "error"})

# start_all / initial_all 顶层可能携带的元数据键
_META_KEYS = frozenset({"type", "ver", "id", "timestamp", "heartbeat", "pong"})


def _extract_inner(entry: Any) -> Tuple[Optional[Dict[str, Any]], str]:
    """从源条目提取 Data 与 md5。"""
    if not isinstance(entry, dict):
        return None, ""
    if "Data" in entry:
        inner = entry.get("Data")
        md5 = entry.get("md5", "")
        if isinstance(inner, dict):
            return inner, str(md5) if md5 else ""
    # 部分旧格式直接就是 Data 字段内容
    if any(k in entry for k in ("eventId", "id", "shockTime", "latitude", "longitude", "magnitude")):
        return entry, entry.get("md5", "")
    return None, ""


def iter_fan_sources(data: dict) -> Iterator[Tuple[str, Dict[str, Any], str]]:
    """
    将 Fan Studio JSON 规范为 (source_key, inner_data, md5) 迭代器。

    支持:
    - v2.1: {"type":"initial"|"update", "data": {"cea": {"Data":..., "md5":...}}}
    - 旧版: {"type":"start_all"|"initial_all", "cea": {"Data":..., "md5":...}}
    - 旧版 update: {"type":"update", "source":"cea", "Data":..., "md5":...}
    """
    if not isinstance(data, dict):
        return

    msg_type = data.get("type")

    if msg_type in _CONTROL_TYPES:
        return

    # v2.1 initial / update with nested data
    if msg_type in ("initial", "update") and isinstance(data.get("data"), dict):
        for source_key, entry in data["data"].items():
            inner, md5 = _extract_inner(entry)
            if inner is not None:
                yield source_key, inner, md5
        return

    # 旧版 update: source + Data 在顶层
    if msg_type == "update":
        source_key = data.get("source") or data.get("institution")
        inner, md5 = _extract_inner({"Data": data.get("Data"), "md5": data.get("md5")})
        if source_key and inner is not None:
            yield str(source_key), inner, md5
        return

    # start_all / initial_all：源键在顶层
    if msg_type in ("start_all", "initial_all", None):
        for key, entry in data.items():
            if key in _META_KEYS:
                continue
            inner, md5 = _extract_inner(entry)
            if inner is not None:
                yield key, inner, md5
        return

    # 单源推送：type 即源名（内网 WS 兼容）
    if isinstance(msg_type, str) and msg_type not in _CONTROL_TYPES:
        inner, md5 = _extract_inner(data.get("data") or data)
        if inner is None and "Data" in data:
            inner, md5 = _extract_inner(data)
        if inner is not None:
            yield msg_type, inner, md5


def normalize_fan_message(data: dict) -> List[Tuple[str, Dict[str, Any], str]]:
    return list(iter_fan_sources(data))


def is_fan_control_message(data: dict) -> bool:
    return data.get("type") in _CONTROL_TYPES
