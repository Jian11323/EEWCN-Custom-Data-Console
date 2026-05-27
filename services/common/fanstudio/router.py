"""Fan Studio 消息路由：单连接多订阅者。"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("fanstudio.router")


class FanStudioRouter:
    """将同一条 /all 消息分发给 EEW、List 等注册回调。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._message_handlers: List[Callable[[dict], None]] = []
        self._open_handlers: List[Callable[[Any], None]] = []
        self._close_handlers: List[Callable[[Any, Any, Any], None]] = []
        self._error_handlers: List[Callable[[Any, Any], None]] = []

    def register_message(self, handler: Callable[[dict], None]) -> None:
        with self._lock:
            if handler not in self._message_handlers:
                self._message_handlers.append(handler)

    def register_open(self, handler: Callable[[Any], None]) -> None:
        with self._lock:
            if handler not in self._open_handlers:
                self._open_handlers.append(handler)

    def register_close(self, handler: Callable[[Any, Any, Any], None]) -> None:
        with self._lock:
            if handler not in self._close_handlers:
                self._close_handlers.append(handler)

    def register_error(self, handler: Callable[[Any, Any], None]) -> None:
        with self._lock:
            if handler not in self._error_handlers:
                self._error_handlers.append(handler)

    def dispatch_raw(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("Fan Studio 非 JSON 消息已忽略")
            return
        if not isinstance(data, dict):
            return
        self.dispatch(data)

    def dispatch(self, data: dict) -> None:
        with self._lock:
            handlers = list(self._message_handlers)
        for h in handlers:
            try:
                h(data)
            except Exception as e:
                logger.error(f"Fan 路由回调异常: {e}", exc_info=True)

    def dispatch_open(self, ws) -> None:
        with self._lock:
            handlers = list(self._open_handlers)
        for h in handlers:
            try:
                h(ws)
            except Exception as e:
                logger.error(f"Fan on_open 回调异常: {e}", exc_info=True)

    def dispatch_close(self, ws, code, msg) -> None:
        with self._lock:
            handlers = list(self._close_handlers)
        for h in handlers:
            try:
                h(ws, code, msg)
            except Exception as e:
                logger.error(f"Fan on_close 回调异常: {e}", exc_info=True)

    def dispatch_error(self, ws, error) -> None:
        with self._lock:
            handlers = list(self._error_handlers)
        for h in handlers:
            try:
                h(ws, error)
            except Exception as e:
                logger.error(f"Fan on_error 回调异常: {e}", exc_info=True)


_router: Optional[FanStudioRouter] = None


def get_fanstudio_router() -> FanStudioRouter:
    global _router
    if _router is None:
        _router = FanStudioRouter()
    return _router
