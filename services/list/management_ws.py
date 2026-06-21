"""
融合 List 管理命令执行（由控制台 IPC 按 channel=list 调用）
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

TEXT_ALIASES: Dict[str, str] = {
    "服务器状态": "fanstudio_status",
    "fanstudio_status": "fanstudio_status",
    "切换备用服务器": "fanstudio_use_backup",
    "fanstudio_use_backup": "fanstudio_use_backup",
    "切换主服务器": "fanstudio_use_primary",
    "fanstudio_use_primary": "fanstudio_use_primary",
    "恢复fanstudio自动切换": "fanstudio_resume_auto",
    "恢复FanStudio自动切换": "fanstudio_resume_auto",
    "fanstudio_resume_auto": "fanstudio_resume_auto",
    "统计": "stats",
    "source_status": "各数据源采集状态",
    "SOURCE_STATUS": "各数据源采集状态",
    "自动检查": "auto_check",
    "auto_check": "auto_check",
    "帮助": "all_commands",
    "help": "all_commands",
    "all_commands": "all_commands",
}


def _resolve_command(text: str) -> Tuple[str, Dict[str, Any]]:
    stripped = (text or "").strip()
    if not stripped:
        return "", {}
    if stripped.casefold() in {k.casefold() for k in TEXT_ALIASES}:
        for alias, cmd in TEXT_ALIASES.items():
            if stripped.casefold() == alias.casefold():
                return cmd, {}
    parts = stripped.split()
    return parts[0], {"args": parts[1:]}


def _get_commands_help() -> Dict[str, str]:
    return {
        "fanstudio_status": "Fan Studio 连接状态",
        "fanstudio_use_backup": "切换至备用服务器",
        "fanstudio_use_primary": "切换至主服务器",
        "fanstudio_resume_auto": "恢复自动主备切换（清除手动锁定）",
        "source_status": "各数据源采集状态（BMKG/GeoNet/Fan/INGV 等）",
        "auto_check": "HTTP 端口与上游状态检查",
        "source_switches_get": "获取数据源开关状态",
        "source_switches_set": "设置数据源开关（热更新）",
        "source_filters_get": "获取国外源阈值与地区过滤配置",
        "source_filters_set": "设置国外源阈值与地区过滤（热更新）",
        "all_commands": "显示本帮助",
    }


def execute_list_command(command: str, fl_module, params: Optional[Dict[str, Any]] = None) -> Any:
    """执行列表管理命令，fl_module 为 fused list engine 模块对象。"""
    params = params or {}
    WebSocketHandler = fl_module.WebSocketHandler
    cfg = fl_module.FAN_STUDIO_SWITCH_CONFIG

    if command in ("fanstudio_status", "服务器状态", "FANSTUDIO_STATUS"):
        with cfg["lock"]:
            return {
                "current_url": cfg["current_url"],
                "is_using_backup": cfg["is_using_backup"],
                "primary_fail_count": cfg["primary_fail_count"],
                "manual_lock": cfg.get("manual_lock", False),
                "switch_to_backup_time": (
                    cfg["switch_to_backup_time"].isoformat()
                    if cfg.get("switch_to_backup_time") else None
                ),
            }

    if command in ("fanstudio_use_backup", "切换备用服务器", "FANSTUDIO_USE_BACKUP"):
        with cfg["lock"]:
            cfg["manual_lock"] = True
        WebSocketHandler.switch_to_backup_server()
        return {"ok": True, "message": "已切换至 Fan Studio 备用服务器"}

    if command in ("fanstudio_use_primary", "切换主服务器", "FANSTUDIO_USE_PRIMARY"):
        with cfg["lock"]:
            cfg["manual_lock"] = True
        WebSocketHandler.switch_to_primary_server()
        return {"ok": True, "message": "已切换至 Fan Studio 主服务器"}

    if command in ("fanstudio_resume_auto", "恢复FanStudio自动切换", "FANSTUDIO_RESUME_AUTO"):
        with cfg["lock"]:
            cfg["manual_lock"] = False
            cfg["primary_fail_count"] = 0
        if cfg["is_using_backup"]:
            if WebSocketHandler.try_connect_primary_server():
                WebSocketHandler.switch_to_primary_server()
        return {"ok": True, "message": "已恢复 Fan Studio 自动主备切换"}

    if command in ("source_status", "SOURCE_STATUS", "数据源状态"):
        from services.common.source_status import get_source_status_registry
        snap = get_source_status_registry().snapshot()
        snap["list_error_stats"] = dict(fl_module.error_stats)
        return snap

    if command in ("error_stats", "ERROR_STATS", "错误统计"):
        return {"error_stats": dict(fl_module.error_stats), "cache_state": dict(fl_module.cache_state)}

    if command in ("stats", "统计", "STATS"):
        with fl_module.fused_data_lock:
            n = len(fl_module.fused_events)
        return {
            "fused_events_8150": n,
            "fanstudio_url": cfg["current_url"],
            "error_stats": dict(fl_module.error_stats),
        }

    if command in ("auto_check", "自动检查", "AUTO_CHECK"):
        import requests

        from services.common.ports import get_list_port

        checks = {}
        list_port = get_list_port()
        url = f"http://127.0.0.1:{list_port}/earthquakes"
        try:
            r = requests.get(url, timeout=5)
            checks[f"http_{list_port}"] = {"ok": r.status_code == 200, "status": r.status_code}
        except Exception as e:
            checks[f"http_{list_port}"] = {"ok": False, "error": str(e)}
        with cfg["lock"]:
            checks["fanstudio"] = {
                "url": cfg["current_url"],
                "backup": cfg["is_using_backup"],
                "ws_connected": cfg.get("ws_instance") is not None,
            }
        return checks

    if command in ("数据源开关", "SOURCE_SWITCHES_GET", "source_switches_get"):
        from services.common.source_switches import get_registry, LIST_SOURCE_NAMES
        ch = "list"
        snap = get_registry(ch).snapshot()
        return {"channel": ch, "switches": snap, "names": LIST_SOURCE_NAMES}

    if command in ("设置数据源开关", "SOURCE_SWITCHES_SET", "source_switches_set"):
        from services.common.source_switches import get_registry, save_to_settings_path
        patch = params.get("patch", {})
        if patch:
            get_registry("list").apply_patch(patch)
        save_to_settings_path()
        return {"ok": True, "channel": "list", "patch": patch}

    if command in ("SOURCE_FILTERS_GET", "source_filters_get"):
        from services.common.source_filters import get_filter_registry
        return {"channel": "list", "filters": get_filter_registry().snapshot()}

    if command in ("SOURCE_FILTERS_SET", "source_filters_set"):
        from services.common.source_filters import get_filter_registry, save_to_settings_path
        reg = get_filter_registry()
        reg.apply_patch(
            list_threshold=params.get("list_source_threshold"),
            list_region_filter=params.get("list_source_region_filter"),
            eew_threshold=params.get("eew_source_threshold"),
            eew_region_filter=params.get("eew_source_region_filter"),
        )
        save_to_settings_path()
        return {"ok": True, "channel": "list", "filters": reg.snapshot()}

    if command in ("all_commands", "帮助", "help", "ALL_COMMANDS"):
        return _get_commands_help()

    raise ValueError(f"未知命令: {command}")
