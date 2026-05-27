"""管理 WebSocket JSON 响应字段中文化（控制台展示）。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

_CONN_QUALITY = {
    "good": "良好",
    "fair": "一般",
    "poor": "较差",
    "unknown": "未知",
}

_UPSTREAM = {
    "fanstudio": "Fan Studio",
    "wolfx": "Wolfx",
}


def _bool_zh(v: Any) -> Any:
    if v is True:
        return True
    if v is False:
        return False
    return v


def localize_fanstudio_status_eew(raw: Dict[str, Any]) -> Dict[str, Any]:
    mt = raw.get("manual_target")
    cq = raw.get("connection_quality", "unknown")
    return {
        "当前服务器": raw.get("current_server"),
        "手动锁定": mt if mt else None,
        "CEA_JMA上游": _UPSTREAM.get(raw.get("cea_jma_upstream"), raw.get("cea_jma_upstream")),
        "Wolfx地址": raw.get("wolfx_url"),
        "主服务器健康度": raw.get("primary_health"),
        "备用服务器健康度": raw.get("backup_health"),
        "连接质量": _CONN_QUALITY.get(cq, cq),
        "连续失败次数": raw.get("fail_streak"),
        "服务器切换次数": raw.get("server_switches"),
    }


def localize_fanstudio_status_list(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "当前地址": raw.get("current_url"),
        "使用备用服务器": bool(raw.get("is_using_backup")),
        "主站连续失败次数": raw.get("primary_fail_count"),
        "手动锁定": bool(raw.get("manual_lock")),
        "切换备用时间": raw.get("switch_to_backup_time"),
    }


def localize_list_stats(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "8150融合事件数": raw.get("fused_events_8150"),
        "FanStudio地址": raw.get("fanstudio_url"),
    }


def localize_list_auto_check(raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, val in raw.items():
        if not isinstance(val, dict):
            out[key] = val
            continue
        if key.startswith("http_"):
            port = key.replace("http_", "")
            out[f"HTTP端口{port}"] = {
                "正常": val.get("ok"),
                "状态码": val.get("status"),
                **({"错误": val["error"]} if val.get("error") else {}),
            }
        elif key == "fanstudio":
            out["FanStudio"] = {
                "地址": val.get("url"),
                "备用": val.get("backup"),
                "WebSocket已连接": val.get("ws_connected"),
            }
        else:
            out[key] = val
    return out


def localize_source_status_entry(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "源ID": raw.get("source_id"),
        "名称": raw.get("label"),
        "通道": raw.get("channel"),
        "已连接": raw.get("connected"),
        "最近成功时间": raw.get("last_ok_at"),
        "最近错误": raw.get("last_error") or "",
        "最近事件时间": raw.get("last_event_at"),
        "消息数": raw.get("message_count"),
        "扩展": raw.get("extra") or {},
    }


def localize_source_status_snapshot(raw: Dict[str, Any]) -> Dict[str, Any]:
    sources = raw.get("sources") or {}
    return {
        "时间戳": raw.get("timestamp"),
        "数据源": {
            sid: localize_source_status_entry(info)
            for sid, info in sources.items()
        },
    }


def localize_ip_entry(raw: Dict[str, Any]) -> Dict[str, Any]:
    ports = raw.get("ports")
    if isinstance(ports, set):
        ports = sorted(ports)
    return {
        "连接数": raw.get("connections"),
        "首次连接时间": raw.get("first_seen"),
        "最后活动时间": raw.get("last_seen"),
        "连接端口": list(ports) if ports is not None else [],
    }


def localize_ip_details(raw: Any) -> Any:
    if not raw:
        return raw
    if isinstance(raw, dict) and "connections" in raw:
        return localize_ip_entry(raw)
    if isinstance(raw, dict):
        return {ip: localize_ip_entry(info) for ip, info in raw.items() if isinstance(info, dict)}
    return raw


def localize_blacklist_list(raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for ip, info in raw.items():
        if not isinstance(info, dict):
            out[ip] = info
            continue
        t = info.get("type")
        if t == "permanent":
            out[ip] = {"类型": "永久封禁"}
        elif t == "temporary":
            out[ip] = {
                "类型": "临时封禁",
                "剩余分钟": info.get("remaining_minutes"),
            }
        else:
            out[ip] = info
    return out


def localize_source_switches(raw: Dict[str, Any]) -> Dict[str, Any]:
    names = raw.get("names") or {}
    switches = raw.get("switches") or {}
    display = {
        names.get(k, k): ("开" if v else "关")
        for k, v in switches.items()
    }
    return {
        "通道": raw.get("channel"),
        "开关": switches,
        "显示名": display,
    }


def localize_source_switches_set(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "成功": raw.get("ok", True),
        "通道": raw.get("channel"),
        "补丁": raw.get("patch"),
        "互斥关闭": raw.get("disabled_by_mutex", []),
        "已移出推送": raw.get("evicted", []),
    }


def localize_thread_pool_restart(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "已下发": raw.get("started"),
        "说明": raw.get("message"),
    }


def localize_result_ok(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "成功": raw.get("success"),
        "说明": raw.get("message"),
    }


def _localize_auto_check_leaf(d: Dict[str, Any]) -> Dict[str, Any]:
    m = {
        "status": "状态",
        "error": "错误",
        "result": "结果",
    }
    out: Dict[str, Any] = {}
    for k, v in d.items():
        out[m.get(k, k)] = v
    return out


def localize_auto_check(raw: Dict[str, Any]) -> Dict[str, Any]:
    summary = raw.get("summary") or {}
    new_summary = {
        "总计": summary.get("total", 0),
        "通过": summary.get("passed", 0),
        "失败": summary.get("failed", 0),
        "警告": summary.get("warnings", 0),
    }
    modules: Dict[str, Any] = {}
    for mod_name, mod_data in (raw.get("modules") or {}).items():
        if not isinstance(mod_data, dict):
            modules[mod_name] = mod_data
            continue
        sub: Dict[str, Any] = {}
        for item_name, item_data in mod_data.items():
            if isinstance(item_data, dict):
                sub[item_name] = _localize_auto_check_leaf(item_data)
            else:
                sub[item_name] = item_data
        modules[mod_name] = sub
    return {
        "检查时间": raw.get("timestamp"),
        "模块": modules,
        "摘要": new_summary,
    }


def localize_mgmt_data(msg_type: str, data: Any) -> Any:
    if data is None:
        return data
    if msg_type == "fanstudio_status":
        return localize_fanstudio_status_eew(data) if "current_server" in data else localize_fanstudio_status_list(data)
    if msg_type == "stats" and isinstance(data, dict) and "fused_events_8150" in data:
        return localize_list_stats(data)
    if msg_type == "source_status":
        return localize_source_status_snapshot(data)
    if msg_type == "ip_details":
        return localize_ip_details(data)
    if msg_type == "blacklist_list":
        return localize_blacklist_list(data)
    if msg_type == "auto_check":
        if isinstance(data, dict) and "modules" in data and "summary" in data:
            return localize_auto_check(data)
        if isinstance(data, dict):
            return localize_list_auto_check(data)
    if msg_type == "source_switches":
        return localize_source_switches(data)
    if msg_type == "source_switches_set":
        return localize_source_switches_set(data)
    if msg_type == "thread_pool_restart":
        return localize_thread_pool_restart(data)
    if msg_type == "result" and isinstance(data, dict) and "message" in data:
        return localize_result_ok(data)
    if msg_type == "result" and isinstance(data, dict) and "ok" in data:
        return {"成功": data.get("ok"), "说明": data.get("message", "")}
    return data


def localize_mgmt_envelope(payload: Dict[str, Any]) -> Dict[str, Any]:
    """本地化整条管理响应（保留 type 为英文命令标识）。"""
    out = dict(payload)
    msg_type = out.get("type", "")
    if msg_type == "error":
        if "message" in out:
            out["消息"] = out.pop("message")
        return out
    if "data" in out:
        raw_data = out["data"]
        loc_type = msg_type
        if msg_type == "result" and out.get("command"):
            loc_type = out["command"]
        if msg_type == "stats" and out.get("channel") == "both" and isinstance(raw_data, dict):
            lst = raw_data.get("list")
            if isinstance(lst, dict) and "fused_events_8150" in lst:
                list_part = localize_list_stats(lst)
            elif isinstance(lst, dict):
                list_part = localize_list_auto_check(lst)
            else:
                list_part = lst
            out["data"] = {
                "预警": localize_mgmt_data("stats", raw_data.get("eew")),
                "速报": list_part,
            }
        else:
            out["data"] = localize_mgmt_data(loc_type, raw_data)
    if msg_type == "result" and "success" in out and "data" not in out:
        keep = {k: out[k] for k in ("command", "channel") if k in out}
        out = localize_result_ok(out)
        out["type"] = payload.get("type")
        out.update(keep)
    return out
