"""单实例锁：防止同一服务重复启动占用端口。"""

from __future__ import annotations

import os
import socket
from typing import Optional

_lock_socket: Optional[socket.socket] = None


def acquire_instance_lock(name: str, port: int) -> bool:
    """
    绑定本地端口作为单实例锁。成功返回 True；已有实例则返回 False。
    端口可通过环境变量 {NAME}_LOCK_PORT 覆盖。
    """
    global _lock_socket
    env_key = f"{name.upper()}_LOCK_PORT"
    port = int(os.environ.get(env_key, str(port)))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
    try:
        sock.bind(("127.0.0.1", port))
        sock.listen(1)
    except OSError:
        try:
            sock.close()
        except Exception:
            pass
        return False
    _lock_socket = sock
    return True


def release_instance_lock() -> None:
    global _lock_socket
    if _lock_socket is not None:
        try:
            _lock_socket.close()
        except Exception:
            pass
        _lock_socket = None
