"""服务注册表：脚本路径、端口、环境变量映射"""

from __future__ import annotations

import sys
from pathlib import Path

from services.common.ports import format_service_ports_label

PROJECT_ROOT = Path(__file__).resolve().parent.parent

FUSED_CORE_ARG = "--run-fused-core"

SERVICE_START_ORDER = ("fused_core",)

SERVICES = {
    "fused_core": {
        "name": "融合数据",
        "script": PROJECT_ROOT / "services" / "fused" / "main.py",
        "cwd": PROJECT_ROOT,
        "description": "EEWCN 客户端配套本地融合服务（预警 WebSocket + 速报 HTTP）",
        "ports": format_service_ports_label(),
        "config": {},
        "color": "#27AE60",
    },
}


def resolve_service_launch_path(service_key: str) -> Path:
    """开发环境返回 .py 脚本；单文件打包后返回主 exe（用于存在性检查）。"""
    info = SERVICES[service_key]
    if getattr(sys, "frozen", False) and service_key == "fused_core":
        return Path(sys.executable).resolve()
    return Path(info["script"])


def resolve_service_launch_cmd(service_key: str) -> list[str]:
    """返回启动融合服务等子进程的命令行。"""
    info = SERVICES[service_key]
    if getattr(sys, "frozen", False) and service_key == "fused_core":
        return [str(Path(sys.executable).resolve()), FUSED_CORE_ARG]
    script = Path(info["script"])
    return [sys.executable, "-u", str(script)]


def refresh_service_ports_label() -> None:
    SERVICES["fused_core"]["ports"] = format_service_ports_label()


def build_env(service_key: str, config: dict) -> dict:
    """将面板配置转为子进程环境变量。"""
    env = {
        "FUSED_MODE": "1",
        "FUSED_SHARED_FAN": "1",
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8",
    }
    if service_key != "fused_core":
        return env
    for cfg_key, val in (config or {}).items():
        if val is not None and str(val).strip() != "":
            env[str(cfg_key)] = str(val).strip()
    try:
        from console.config import ConfigStore

        s = ConfigStore.instance().settings
        env["FUSED_EEW_PORT"] = str(int(s.eew_port))
        env["FUSED_LIST_PORT"] = str(int(s.list_port))
    except Exception:
        pass
    env["FUSED_CONSOLE_IPC"] = "1"
    try:
        from services.common.source_switches import load_from_settings_path, switches_snapshot_for_env

        load_from_settings_path()
        env["SOURCE_SWITCHES_JSON"] = switches_snapshot_for_env()
    except Exception:
        pass
    return env
