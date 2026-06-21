"""GUI 与 fused_core 子进程 stdin IPC 客户端。"""

from __future__ import annotations

import json
import threading
import uuid
from typing import Any, Dict, Optional, TYPE_CHECKING

from services.fused.console_ipc import MGMT_STDOUT_PREFIX

if TYPE_CHECKING:
    from console.process_manager import ServiceProcess


class IpcResponseRegistry:
    """按 request id 匹配子进程 @MGMT@ 响应。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: Dict[str, threading.Event] = {}
        self._results: Dict[str, Any] = {}

    def register(self, req_id: str) -> threading.Event:
        event = threading.Event()
        with self._lock:
            self._pending[req_id] = event
        return event

    def deliver(self, req_id: str, payload: dict) -> bool:
        with self._lock:
            if req_id not in self._pending:
                return False
            self._results[req_id] = payload
            self._pending[req_id].set()
            return True

    def pop(self, req_id: str) -> Any:
        with self._lock:
            self._pending.pop(req_id, None)
            return self._results.pop(req_id, None)


_ipc_registry = IpcResponseRegistry()


def get_ipc_registry() -> IpcResponseRegistry:
    return _ipc_registry


def parse_mgmt_ipc_line(line: str) -> Optional[dict]:
    if not line.startswith(MGMT_STDOUT_PREFIX):
        return None
    raw = line[len(MGMT_STDOUT_PREFIX):]
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def send_ipc_command(
    process: "ServiceProcess",
    command: str,
    params: Optional[dict] = None,
    timeout: float = 20.0,
) -> Any:
    proc = process.subprocess
    if proc is None or proc.poll() is not None:
        raise RuntimeError("融合服务未运行")
    if proc.stdin is None:
        raise RuntimeError("子进程 stdin 不可用")

    req_id = uuid.uuid4().hex
    body = {"id": req_id, "command": command, **(params or {})}
    event = _ipc_registry.register(req_id)
    try:
        proc.stdin.write(json.dumps(body, ensure_ascii=False) + "\n")
        proc.stdin.flush()
    except Exception as exc:
        _ipc_registry.pop(req_id)
        raise RuntimeError(f"写入 IPC 命令失败: {exc}") from exc

    if not event.wait(timeout):
        _ipc_registry.pop(req_id)
        raise TimeoutError("管理命令超时")

    payload = _ipc_registry.pop(req_id)
    if not isinstance(payload, dict):
        raise RuntimeError("IPC 响应格式无效")
    if not payload.get("ok"):
        raise RuntimeError(str(payload.get("error") or "管理命令失败"))
    result = payload.get("result")
    if isinstance(result, dict):
        return json.dumps(result, ensure_ascii=False)
    return result
