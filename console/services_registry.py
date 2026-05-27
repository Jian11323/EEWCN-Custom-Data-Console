"""服务注册表：脚本路径、端口、环境变量映射"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

FUSED_CORE_ARG = "--run-fused-core"

SERVICE_START_ORDER = ("fused_core",)

SERVICES = {
    "fused_core": {
        "name": "融合数据",
        "script": PROJECT_ROOT / "services" / "fused" / "main.py",
        "cwd": PROJECT_ROOT,
        "description": "",
        "ports": "预警 WS 5000 | 历史 HTTP 8150 | 管理 WS 2050",
        "mgmt_port": 2050,
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
        from services.common.source_switches import load_from_settings_path, switches_snapshot_for_env
        load_from_settings_path()
        env["SOURCE_SWITCHES_JSON"] = switches_snapshot_for_env()
    except Exception:
        pass
    return env
