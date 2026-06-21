"""HTTP / WebSocket 健康探活"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional

from PyQt5.QtCore import QThread, pyqtSignal

try:
    import requests
except ImportError:
    requests = None  # type: ignore

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore

from services.common.ports import LOCAL_BIND, get_eew_port, get_list_port


def build_default_checks() -> dict:
    eew = get_eew_port()
    lst = get_list_port()
    return {
        "http": {
            "list": f"http://{LOCAL_BIND}:{lst}/earthquakes",
        },
        "ws": {
            "eew": f"ws://{LOCAL_BIND}:{eew}",
        },
    }


def port_probe_order() -> tuple[str, ...]:
    return ("eew", "list")


DEFAULT_CHECKS = build_default_checks()
PORT_PROBE_ORDER = port_probe_order()


class HealthCheckWorker(QThread):
    finished = pyqtSignal(dict)

    def __init__(self, checks: dict):
        super().__init__()
        self._checks = checks

    def run(self):
        results: Dict[str, Any] = {"timestamp": time.time()}
        if requests:
            for key, url in self._checks.get("http", {}).items():
                t0 = time.perf_counter()
                try:
                    r = requests.get(url, timeout=5)
                    ms = int((time.perf_counter() - t0) * 1000)
                    results[key] = {
                        "ok": r.status_code == 200,
                        "status": r.status_code,
                        "latency_ms": ms,
                    }
                except Exception as e:
                    results[key] = {"ok": False, "error": str(e)}

        if websockets:

            async def _ws_ping(uri: str, key: str):
                try:
                    async with websockets.connect(uri, open_timeout=5, close_timeout=3) as ws:
                        t0 = time.perf_counter()
                        await ws.send(json.dumps({"type": "ping"}))
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=8)
                        except asyncio.TimeoutError:
                            msg = ""
                        ms = int((time.perf_counter() - t0) * 1000)
                        ok = "pong" in msg.lower() or "shuju" in msg
                        return key, {"ok": ok, "latency_ms": ms, "preview": msg[:80]}
                except Exception as e:
                    return key, {"ok": False, "error": str(e)}

            async def _run_ws():
                tasks = []
                for key, uri in self._checks.get("ws", {}).items():
                    tasks.append(_ws_ping(uri, key))
                if tasks:
                    for coro in asyncio.as_completed(tasks):
                        key, val = await coro
                        results[key] = val

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(_run_ws())
                loop.close()
            except Exception as e:
                results["ws_error"] = str(e)

        self.finished.emit(results)
