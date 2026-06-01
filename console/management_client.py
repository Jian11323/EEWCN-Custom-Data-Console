"""管理 WebSocket 客户端（融合端口 2050，channel 区分 EEW/List）"""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any, Optional

from PyQt5.QtCore import QObject, pyqtSignal, QThread

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore

# 连接握手类消息，不在控制命令日志中重复展示
_MGMT_SKIP_LOG_TYPES = frozenset({"welcome", "available_commands"})

# 收到以下 type 即视为本条命令的正式响应
_MGMT_DONE_TYPES = frozenset({
    "result", "error", "command_result", "source_status",
    "stats", "history", "full_history", "ip_details", "blacklist_list",
    "fanstudio_status", "auto_check", "thread_pool_status",
    "thread_pool_check", "thread_pool_restart",
    "source_switches", "source_switches_set", "custom_data_source_url_set",
})


def format_management_message(raw: str) -> str:
    """将管理端口原始报文格式化为可读 JSON（中文不转义）。"""
    text = (raw or "").strip()
    if not text:
        return ""
    try:
        obj = json.loads(text)
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except json.JSONDecodeError:
        return text


class ManagementWorker(QThread):
    """在后台线程执行单次管理命令。"""

    finished = pyqtSignal(str, object)  # target, result or error str
    message_received = pyqtSignal(str, str)  # target, raw message

    def __init__(
        self,
        target: str,
        host: str,
        port: int,
        command: str,
        params: Optional[dict] = None,
        connect_only: bool = False,
    ):
        super().__init__()
        self._target = target
        self._host = host
        self._port = port
        self._command = command
        self._params = params or {}
        self._connect_only = connect_only

    def run(self):
        if websockets is None:
            self.finished.emit(self._target, "未安装 websockets 库")
            return

        async def _run():
            uri = f"ws://{self._host}:{self._port}"
            async with websockets.connect(uri, open_timeout=8, close_timeout=5) as ws:
                welcome = await asyncio.wait_for(ws.recv(), timeout=10)
                self.message_received.emit(self._target, welcome)
                if self._connect_only:
                    return welcome
                payload = {"command": self._command, **self._params}
                await ws.send(json.dumps(payload, ensure_ascii=False))
                responses = []
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=15)
                        responses.append(msg)
                        self.message_received.emit(self._target, msg)
                        try:
                            data = json.loads(msg)
                            if data.get("type") in _MGMT_DONE_TYPES:
                                break
                        except json.JSONDecodeError:
                            if msg.startswith("ERROR:") or "成功" in msg or "失败" in msg:
                                break
                except asyncio.TimeoutError:
                    pass
                return responses[-1] if responses else welcome

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(_run())
            loop.close()
            self.finished.emit(self._target, result)
        except Exception as e:
            self.finished.emit(self._target, str(e))


class ManagementHub(QObject):
    """管理命令调度中心（单端口 2050）。"""

    result_ready = pyqtSignal(str, object)
    log_line = pyqtSignal(str)

    def __init__(self, host: str, port: int):
        super().__init__()
        self._host = host
        self._port = port
        self._workers: list[ManagementWorker] = []

    def update_endpoint(self, host: str, port: int):
        self._host = host
        self._port = port

    def send_command(self, target: str, command: str, params: Optional[dict] = None):
        """target: eew | list | both | mgmt；会写入 JSON 的 channel 字段。"""
        body = dict(params or {})
        if target in ("eew", "list", "both"):
            body.setdefault("channel", target)
        log_target = target if target != "mgmt" else body.get("channel", "mgmt")
        worker = ManagementWorker(log_target, self._host, self._port, command, body)
        worker.finished.connect(self._on_finished)
        self._workers.append(worker)
        worker.finished.connect(lambda: self._workers.remove(worker) if worker in self._workers else None)
        worker.start()

    def send_both(self, command: str, params: Optional[dict] = None):
        body = dict(params or {})
        body["channel"] = "both"
        worker = ManagementWorker("both", self._host, self._port, command, body)
        worker.finished.connect(self._on_finished)
        self._workers.append(worker)
        worker.finished.connect(lambda: self._workers.remove(worker) if worker in self._workers else None)
        worker.start()

    def stop_all_workers(self, wait_ms: int = 1500) -> None:
        """退出前终止仍在运行的管理 WS 工作线程。"""
        for worker in list(self._workers):
            if worker.isRunning():
                worker.requestInterruption()
                if not worker.wait(min(wait_ms, 1200)):
                    worker.terminate()
                    worker.wait(400)
        self._workers.clear()

    def _on_finished(self, target: str, result: object):
        display: object = result
        if isinstance(result, str):
            try:
                data = json.loads(result)
                if isinstance(data, dict) and data.get("type") in _MGMT_SKIP_LOG_TYPES:
                    return
            except json.JSONDecodeError:
                pass
            display = format_management_message(result) or result
        elif isinstance(result, dict):
            display = json.dumps(result, ensure_ascii=False, indent=2)
        elif result is not None:
            display = str(result)
        else:
            return
        self.result_ready.emit(target, display)
