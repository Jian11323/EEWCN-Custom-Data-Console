"""同步 WebSocket 客户端（websocket-client）安全导入与 SSL 配置。"""

from __future__ import annotations

import os
import ssl
import sys

WebSocketApp = None
try:
    from websocket._app import WebSocketApp
except ImportError:
    try:
        import websocket as _ws_sync_mod

        WebSocketApp = getattr(_ws_sync_mod, "WebSocketApp", None)
    except ImportError:
        WebSocketApp = None

if WebSocketApp is None:
    raise ImportError(
        "无法加载 websocket-client 的 WebSocketApp（Fan Studio / ALL_WS_FULL 需要）。\n"
        "请使用与本脚本相同的解释器安装依赖（Windows 上 `Python` 与 `pip` 可能不是同一环境）：\n"
        f"  {sys.executable} -m pip uninstall websocket -y\n"
        f"  {sys.executable} -m pip install websocket-client\n"
        "若脚本目录下有 websocket.py 或与包同名的文件夹，请改名以免遮挡 site-packages。"
    )

# EEW 历史别名
FanStudioWebSocketApp = WebSocketApp


def ws_ssl_insecure() -> bool:
    """Return True when FUSED_WS_INSECURE explicitly disables certificate verification."""
    return os.environ.get("FUSED_WS_INSECURE", "").strip().lower() in ("1", "true", "yes")


def ws_ssl_options() -> dict:
    """SSL options for websocket-client run_forever. Default verifies certificates."""
    if ws_ssl_insecure():
        return {"cert_reqs": ssl.CERT_NONE}
    return {}


def ws_run_forever_kwargs(**extra) -> dict:
    """Build kwargs for WebSocketApp.run_forever including sslopt when needed."""
    kwargs = dict(extra)
    sslopt = ws_ssl_options()
    if sslopt:
        kwargs["sslopt"] = sslopt
    return kwargs
