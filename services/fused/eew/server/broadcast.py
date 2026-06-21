"""EEW WebSocket 广播服务（从 ws_server 拆分）。"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import TYPE_CHECKING, Any

import websockets

from services.common.ports import LOCAL_BIND, get_eew_port

if TYPE_CHECKING:
    from services.fused.eew.server.ws_server import WebSocketServerManager


def setup_broadcast_loop(manager: "WebSocketServerManager") -> None:
    def run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        manager.broadcast_loop = loop
        loop.run_forever()

    threading.Thread(target=run_loop, daemon=True, name="Broadcast-Loop").start()
    for _ in range(100):
        if manager.broadcast_loop:
            break
        time.sleep(0.01)


async def handle_client(manager: "WebSocketServerManager", websocket: Any, port: int) -> None:
    client_addr = websocket.remote_address
    client_ip = "unknown"

    if client_addr:
        if isinstance(client_addr, tuple) and len(client_addr) > 0:
            client_ip = client_addr[0]
        elif isinstance(client_addr, str):
            client_ip = client_addr.split(":")[0] if ":" in client_addr else client_addr
        else:
            client_ip = str(client_addr)
    else:
        try:
            if hasattr(websocket, "headers") and websocket.headers:
                forwarded_for = websocket.headers.get("X-Forwarded-For")
                if forwarded_for:
                    client_ip = forwarded_for.split(",")[0].strip()
                else:
                    real_ip = websocket.headers.get("X-Real-IP")
                    if real_ip:
                        client_ip = real_ip.strip()
        except Exception:
            pass

    if not client_ip or client_ip == "unknown":
        manager.logger.warning(f"[端口{port}] 无法获取客户端IP地址: remote_address={client_addr}")
        client_ip = "0.0.0.0"

    try:
        manager.client_ip_manager.record_connection(client_ip, port)
        manager.logger.info(f"[端口{port}] 客户端连接: {client_ip}")
    except Exception as exc:
        manager.logger.error(f"[端口{port}] 记录IP连接失败 {client_ip}: {exc}")

    client_info = (client_ip, client_addr)

    if not manager.client_ip_manager.check_ip_allowed(client_ip):
        manager.client_ip_manager.record_disconnection(client_ip, port)
        await websocket.close(code=1008, reason="Access denied")
        return

    if not manager.client_ip_manager.check_connection_limit(client_ip):
        manager.client_ip_manager.record_disconnection(client_ip, port)
        await websocket.close(code=1008, reason="Connection limit exceeded")
        return

    if port != get_eew_port():
        return
    clients, lock = manager.clients_5000, manager.lock_5000

    with lock:
        clients.add(websocket)

    try:
        current_data = manager.cache_mgr.get_fused_cache(port)
        initial_msg = json.dumps(
            {"shuju": current_data},
            ensure_ascii=False,
            separators=(",", ":"),
            check_circular=False,
        )
        await websocket.send(initial_msg)
        async for message in websocket:
            try:
                if isinstance(message, str):
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        continue
                    if data.get("type") == "ping":
                        await websocket.send(json.dumps({"type": "pong"}, ensure_ascii=False))
            except Exception as exc:
                manager.logger.debug(f"处理客户端消息失败: {exc}")
    except Exception as exc:
        manager.logger.error(f"[端口{port}] handle_client 异常: {exc}")
    finally:
        with lock:
            clients.discard(websocket)
        try:
            disconnect_ip, disconnect_addr = client_info
            manager.client_ip_manager.record_disconnection(disconnect_ip, port)
            manager.logger.info(f"[端口{port}] 客户端断开: {disconnect_addr}")
        except Exception as exc:
            manager.logger.error(f"[端口{port}] 记录断开连接失败: {exc}")


async def broadcast_async(manager: "WebSocketServerManager", message: str, port: int) -> None:
    if port != get_eew_port():
        return
    clients, lock = manager.clients_5000, manager.lock_5000
    with lock:
        if not clients:
            return
        clients_copy = tuple(clients)

    async def send_to_client(client):
        try:
            await client.send(message)
            return None
        except Exception:
            return client

    results = await asyncio.gather(*[send_to_client(c) for c in clients_copy], return_exceptions=True)
    disconnected = [r for r in results if r is not None and not isinstance(r, Exception)]
    if disconnected:
        with lock:
            for client in disconnected:
                clients.discard(client)


def broadcast(manager: "WebSocketServerManager", message: str, port: int) -> None:
    if not manager.broadcast_loop:
        return
    t_before_schedule = time.time()

    async def _broadcast_with_timing():
        t_async_start = time.time()
        await broadcast_async(manager, message, port)
        schedule_delay_ms = (t_async_start - t_before_schedule) * 1000
        send_ms = (time.time() - t_async_start) * 1000
        manager.logger.debug(
            f"[Broadcast] 端口{port} 调度延迟: {schedule_delay_ms:.1f}ms, 发送耗时: {send_ms:.1f}ms"
        )

    asyncio.run_coroutine_threadsafe(_broadcast_with_timing(), manager.broadcast_loop)


def start_ws_server(manager: "WebSocketServerManager", port: int) -> None:
    async def handler(websocket, path=None):
        await handle_client(manager, websocket, port)

    async def run():
        async with websockets.serve(handler, LOCAL_BIND, port, ping_interval=None):
            await asyncio.Future()

    def run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())

    threading.Thread(target=run_in_thread, daemon=True, name=f"WS-Server-{port}").start()
    manager.logger.debug(f"WebSocket服务器启动: 端口{port}")
