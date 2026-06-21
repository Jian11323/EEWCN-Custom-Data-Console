from __future__ import annotations

import hashlib
from typing import Any, Dict

def _wolfx_synthetic_md5(parts: Dict[str, Any]) -> str:
    """Wolfx 报文无 md5 时用稳定字段生成去重键。"""
    try:
        s = json.dumps(parts, sort_keys=True, ensure_ascii=False, default=str)
    except TypeError:
        s = str(parts)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _wolfx_cenc_eew_to_cea_raw(wolfx: Dict[str, Any]) -> Dict[str, Any]:
    """cenc_eew -> CEASource.fetch 所读字段（与 Fan Studio cea 对齐）。"""
    depth = wolfx.get("Depth")
    return {
        "shockTime": str(wolfx.get("OriginTime", "")).strip(),
        "placeName": str(wolfx.get("HypoCenter") or wolfx.get("Hypocenter") or ""),
        "updates": Utils.safe_int(wolfx.get("ReportNum"), 1),
        "eventId": str(wolfx.get("EventID") or wolfx.get("ID") or ""),
        "magnitude": wolfx.get("Magnitude"),
        "depth": int(depth) if depth is not None and depth != "" else 0,
        "latitude": Utils.safe_float(wolfx.get("Latitude")),
        "longitude": Utils.safe_float(wolfx.get("Longitude")),
    }


def _wolfx_jma_eew_to_jma_raw(wolfx: Dict[str, Any]) -> Dict[str, Any]:
    """jma_eew -> JMAFanStudioSource 所读字段（与 Fan Studio jma 对齐）；取消报走 cancel 分支。"""
    if wolfx.get("isCancel"):
        return {
            "cancel": True,
            "id": str(wolfx.get("EventID", "unknown")),
            "updates": Utils.safe_int(wolfx.get("Serial"), 1),
        }
    origin = str(wolfx.get("OriginTime", "")).strip()
    hyp = wolfx.get("Hypocenter") or wolfx.get("HypoCenter") or "未知地点"
    mag = wolfx.get("Magunitude")
    if mag is None:
        mag = wolfx.get("Magnitude")
    warn_area = wolfx.get("WarnArea")
    info_type = ""
    if isinstance(warn_area, dict):
        info_type = str(warn_area.get("Type") or "")
    if not info_type:
        info_type = "警報" if wolfx.get("isWarn") else "予報"
    return {
        "shockTime": origin,
        "id": str(wolfx.get("EventID", "unknown")),
        "updates": Utils.safe_int(wolfx.get("Serial"), 1),
        "placeName": hyp,
        "latitude": Utils.safe_float(wolfx.get("Latitude")),
        "longitude": Utils.safe_float(wolfx.get("Longitude")),
        "magnitude": mag,
        "depth": wolfx.get("Depth"),
        "infoTypeName": info_type,
        "final": bool(wolfx.get("isFinal")),
        "cancel": False,
        "epiIntensity": wolfx.get("MaxIntensity", ""),
        "createTime": str(wolfx.get("AnnouncedTime", "")),
    }


# ============================================================================
# WebSocket客户端管理器
# ============================================================================

