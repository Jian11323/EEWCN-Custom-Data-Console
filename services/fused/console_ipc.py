"""GUI 子进程 stdin 管理命令 IPC（替代 2050 管理 WebSocket）。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from services.fused.eew.server.ws_server import WebSocketServerManager

MGMT_STDOUT_PREFIX = "@MGMT@"
logger = logging.getLogger(__name__)


class _ResponseCollector:
    """模拟 WebSocket，收集管理命令响应。"""

    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)

    def last_result(self) -> Any:
        if not self.messages:
            return {"type": "error", "message": "无响应"}
        last = self.messages[-1]
        try:
            return json.loads(last)
        except json.JSONDecodeError:
            return {"type": "result", "data": last}


def _emit_response(req_id: str, ok: bool, result: Any = None, error: str = "") -> None:
    payload = {"id": req_id, "ok": ok}
    if ok:
        payload["result"] = result
    else:
        payload["error"] = error
    line = f"{MGMT_STDOUT_PREFIX}{json.dumps(payload, ensure_ascii=False, default=str)}"
    print(line, flush=True)


async def _execute_ipc_command(
    manager: "WebSocketServerManager",
    command: str,
    params: dict,
) -> Any:
    collector = _ResponseCollector()
    await manager._execute_management_command(collector, command, params, is_json=True)
    return collector.last_result()


def _run_command(manager: "WebSocketServerManager", command: str, params: dict) -> Any:
    loop = manager.broadcast_loop
    if loop is None or not loop.is_running():
        raise RuntimeError("广播事件循环未就绪")
    future = asyncio.run_coroutine_threadsafe(
        _execute_ipc_command(manager, command, params),
        loop,
    )
    return future.result(timeout=45)


def _stdin_loop(manager: "WebSocketServerManager") -> None:
    for raw in sys.stdin:
        if not raw:
            break
        line = raw.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            logger.debug("忽略非 JSON stdin 行")
            continue
        req_id = str(data.get("id") or "")
        command = data.get("command", "")
        if not command:
            _emit_response(req_id, False, error="缺少 command")
            continue
        params = {k: v for k, v in data.items() if k not in ("id", "type")}
        try:
            result = _run_command(manager, command, params)
            _emit_response(req_id, True, result=result)
        except Exception as exc:
            logger.exception("IPC 管理命令失败: %s", command)
            _emit_response(req_id, False, error=str(exc))


def start_console_ipc(manager: "WebSocketServerManager") -> None:
    if os.environ.get("FUSED_CONSOLE_IPC", "").strip() != "1":
        return
    threading.Thread(
        target=_stdin_loop,
        args=(manager,),
        daemon=True,
        name="Console-IPC-Stdin",
    ).start()
    logger.info("控制台 IPC 已启用（stdin/stdout，无管理 TCP 端口）")
