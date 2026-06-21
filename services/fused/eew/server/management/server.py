"""Management WebSocket server — 已弃用，改由控制台 stdin IPC。"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from services.fused.eew.server.ws_server import WebSocketServerManager

logger = logging.getLogger(__name__)


def start_management_server(
    manager: "WebSocketServerManager",
    port: Optional[int] = None,
) -> None:
    logger.debug("管理 WebSocket 已移除；请使用 FUSED_CONSOLE_IPC=1 与控制台通信")
