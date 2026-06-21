"""唯一 Fan Studio /all WebSocket 连接。"""

from __future__ import annotations

import json
import logging
import os
import random
from services.fused.common.ws_client import ws_run_forever_kwargs
import sys
import threading
import time
from typing import Any, Literal, Optional

from services.common.fanstudio.health import FanStudioHealth
from services.common.fanstudio.router import get_fanstudio_router

FanStudioWebSocketApp = None
try:
    from websocket._app import WebSocketApp as FanStudioWebSocketApp
except ImportError:
    try:
        import websocket as _ws_mod
        FanStudioWebSocketApp = getattr(_ws_mod, "WebSocketApp", None)
    except ImportError:
        pass

if FanStudioWebSocketApp is None:
    raise ImportError("需要 websocket-client 包以连接 Fan Studio /all")


class FanStudioConnection:
    """全局单例 Fan Studio 客户端。"""

    PRIMARY = os.environ.get("FANSTUDIO_WS_PRIMARY", "wss://ws.fanstudio.tech/all")
    BACKUP = os.environ.get("FANSTUDIO_WS_BACKUP", "wss://ws.fanstudio.hk/all")

    def __init__(self):
        self.logger = logging.getLogger("fanstudio.connection")
        self.health = FanStudioHealth(
            primary_url=self.PRIMARY,
            backup_url=self.BACKUP,
            current_url=self.PRIMARY,
        )
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws = None
        self._lock = threading.RLock()
        self._intentional_close = False
        self._skip_delay = False
        self._disabled = False  # Wolfx 模式时 EEW 可暂停 Fan
        self._ws_ref_for_send = None

    @property
    def ws(self):
        return self._ws_ref_for_send

    def set_disabled(self, disabled: bool) -> None:
        """Wolfx 上游时暂停 Fan 连接。"""
        self._disabled = disabled
        if disabled:
            self._close_ws()

    def switch_to_backup(self) -> None:
        with self.health.lock:
            self.health.manual_target = "backup"
            self.health.is_using_backup = True
            self.health.current_url = self.BACKUP
        self._skip_delay = True
        self._close_ws()
        self.logger.info("[Fan Studio] 手动切换备用并锁定")

    def switch_to_primary(self) -> None:
        with self.health.lock:
            self.health.manual_target = "primary"
            self.health.is_using_backup = False
            self.health.current_url = self.PRIMARY
            self.health.primary_fail_count = 0
        self._skip_delay = True
        self._close_ws()
        self.logger.info("[Fan Studio] 手动切换主站并锁定")

    def resume_auto_switch(self) -> None:
        with self.health.lock:
            self.health.manual_target = None
        self.logger.info("[Fan Studio] 恢复自动主备切换")

    def send_text(self, text: str) -> bool:
        ws = self._ws_ref_for_send
        if ws is None:
            return False
        try:
            ws.send(text)
            return True
        except Exception as e:
            self.logger.error(f"Fan Studio 发送失败: {e}")
            return False

    def _close_ws(self) -> None:
        with self._lock:
            ws = self._ws
        if ws:
            self._intentional_close = True
            try:
                ws.close()
            except Exception:
                self._intentional_close = False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="FanStudio-Shared")
        self._thread.start()
        self.logger.info("共享 Fan Studio 连接线程已启动")

    def stop(self) -> None:
        self._stop.set()
        self._close_ws()

    def _resolve_url(self) -> str:
        with self.health.lock:
            if self.health.manual_target == "backup":
                return self.BACKUP
            if self.health.manual_target == "primary":
                return self.PRIMARY
            if self.health.is_using_backup or (
                self.health.manual_target is None
                and self.health.primary_fail_count >= self.health.primary_fail_threshold
            ):
                return self.BACKUP
            return self.PRIMARY

    def _run_loop(self) -> None:
        router = get_fanstudio_router()
        consecutive = 0

        while not self._stop.is_set():
            if self._disabled:
                time.sleep(0.5)
                continue

            url = self._resolve_url()
            with self.health.lock:
                self.health.current_url = url
                self.health.is_using_backup = url == self.BACKUP

            def on_message(ws, message):
                router.dispatch_raw(message)

            def on_open(ws):
                self._ws_ref_for_send = ws
                with self.health.lock:
                    if not self.health.is_using_backup:
                        self.health.primary_fail_count = 0
                router.dispatch_open(ws)

            def on_close(ws, code, msg):
                self._ws_ref_for_send = None
                intentional = self._intentional_close
                self._intentional_close = False
                if not intentional and not self._stop.is_set():
                    with self.health.lock:
                        if not self.health.is_using_backup:
                            self.health.primary_fail_count += 1
                router.dispatch_close(ws, code, msg)

            def on_error(ws, error):
                router.dispatch_error(ws, error)

            try:
                ws_app = FanStudioWebSocketApp(
                    url,
                    on_message=on_message,
                    on_open=on_open,
                    on_close=on_close,
                    on_error=on_error,
                )
                with self._lock:
                    self._ws = ws_app
                if self._stop.is_set():
                    break
                ws_app.run_forever(ping_interval=None, **ws_run_forever_kwargs())
            except Exception as e:
                self.logger.debug(f"Fan Studio 连接异常: {e}")
            finally:
                with self._lock:
                    self._ws = None
                self._ws_ref_for_send = None

            if self._stop.is_set():
                break

            delay = 3
            if self._skip_delay:
                self._skip_delay = False
                delay = 1
            else:
                delay += min(consecutive * 2, 30)
                delay += random.uniform(0, 2)
            consecutive += 1
            time.sleep(max(1, delay))


_connection: Optional[FanStudioConnection] = None


def get_fanstudio_connection() -> FanStudioConnection:
    global _connection
    if _connection is None:
        _connection = FanStudioConnection()
    return _connection
