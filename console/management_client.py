"""管理命令调度（控制台 IPC，无 2050 WebSocket）"""

from __future__ import annotations

import json
from typing import Optional, TYPE_CHECKING

from PyQt5.QtCore import QObject, pyqtSignal, QThread

from console.ipc_client import send_ipc_command

if TYPE_CHECKING:
    from console.process_manager import ServiceProcess

_MGMT_SKIP_LOG_TYPES = frozenset({"welcome", "available_commands"})

_MGMT_DONE_TYPES = frozenset({
    "result", "error", "command_result", "source_status",
    "stats", "history", "full_history", "ip_details", "blacklist_list",
    "fanstudio_status", "auto_check", "thread_pool_status",
    "thread_pool_check", "thread_pool_restart",
    "source_switches", "source_switches_set", "custom_data_source_url_set",
})


def format_management_message(raw: str) -> str:
    """将管理原始报文格式化为可读 JSON（中文不转义）。"""
    text = (raw or "").strip()
    if not text:
        return ""
    try:
        obj = json.loads(text)
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except json.JSONDecodeError:
        return text


class ManagementWorker(QThread):
    """在后台线程执行单次管理命令（stdin IPC）。"""

    finished = pyqtSignal(str, object)
    message_received = pyqtSignal(str, str)

    def __init__(
        self,
        target: str,
        service_process: Optional["ServiceProcess"],
        command: str,
        params: Optional[dict] = None,
    ):
        super().__init__()
        self._target = target
        self._process = service_process
        self._command = command
        self._params = params or {}

    def run(self):
        if self._process is None or not self._process.is_running():
            self.finished.emit(self._target, "融合服务未运行，请先启动服务")
            return
        try:
            result = send_ipc_command(self._process, self._command, self._params)
            if isinstance(result, str):
                self.message_received.emit(self._target, result)
            self.finished.emit(self._target, result)
        except Exception as e:
            self.finished.emit(self._target, str(e))


class ManagementHub(QObject):
    """管理命令调度中心（子进程 stdin IPC）。"""

    result_ready = pyqtSignal(str, object)
    log_line = pyqtSignal(str)

    def __init__(self, service_process: Optional["ServiceProcess"] = None):
        super().__init__()
        self._process: Optional["ServiceProcess"] = service_process
        self._workers: list[ManagementWorker] = []

    def update_process(self, service_process: Optional["ServiceProcess"]) -> None:
        self._process = service_process

    def send_command(self, target: str, command: str, params: Optional[dict] = None):
        """target: eew | list | both | mgmt；会写入 JSON 的 channel 字段。"""
        body = dict(params or {})
        if target in ("eew", "list", "both"):
            body.setdefault("channel", target)
        log_target = target if target != "mgmt" else body.get("channel", "mgmt")
        worker = ManagementWorker(log_target, self._process, command, body)
        worker.finished.connect(self._on_finished)
        self._workers.append(worker)
        worker.finished.connect(lambda: self._workers.remove(worker) if worker in self._workers else None)
        worker.start()

    def send_both(self, command: str, params: Optional[dict] = None):
        body = dict(params or {})
        body["channel"] = "both"
        worker = ManagementWorker("both", self._process, command, body)
        worker.finished.connect(self._on_finished)
        self._workers.append(worker)
        worker.finished.connect(lambda: self._workers.remove(worker) if worker in self._workers else None)
        worker.start()

    def stop_all_workers(self, wait_ms: int = 1500) -> None:
        """退出前终止仍在运行的管理工作线程。"""
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
