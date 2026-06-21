"""服务端口集中配置（EEW WebSocket / List HTTP）。"""

from __future__ import annotations

import os
from typing import Optional

DEFAULT_EEW_PORT = 5000
DEFAULT_LIST_PORT = 8150
LOCAL_BIND = "127.0.0.1"


def _env_int(name: str) -> Optional[int]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _settings_ports() -> tuple[Optional[int], Optional[int]]:
    try:
        from console.config import ConfigStore

        s = ConfigStore.instance().settings
        return getattr(s, "eew_port", None), getattr(s, "list_port", None)
    except Exception:
        return None, None


def get_eew_port() -> int:
    port = _env_int("FUSED_EEW_PORT")
    if port is not None:
        return port
    settings_eew, _ = _settings_ports()
    if settings_eew is not None:
        return int(settings_eew)
    return DEFAULT_EEW_PORT


def get_list_port() -> int:
    port = _env_int("FUSED_LIST_PORT")
    if port is not None:
        return port
    _, settings_list = _settings_ports()
    if settings_list is not None:
        return int(settings_list)
    return DEFAULT_LIST_PORT


def eew_ws_url(host: str = LOCAL_BIND, port: Optional[int] = None) -> str:
    return f"ws://{host}:{port if port is not None else get_eew_port()}"


def list_http_url(host: str = LOCAL_BIND, port: Optional[int] = None) -> str:
    p = port if port is not None else get_list_port()
    return f"http://{host}:{p}/earthquakes"


def format_service_ports_label() -> str:
    return f"预警 WS {get_eew_port()} | 历史 HTTP {get_list_port()}"
