"""自定义数据源：HTTP 轮询或 WebSocket，解析后发布到 EEW 总线。"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any, Dict, Optional

import requests

from services.common.bus import get_event_bus
from services.common.custom_adapter import parse_custom_payload
from services.common.source_status import get_source_status_registry
from services.common.source_switches import get_custom_data_source_url

logger = logging.getLogger("internal.custom")

SOURCE_ID = "custom"
POLL_INTERVAL = 1.0
HTTP_TIMEOUT = 12

_stop = threading.Event()
_thread: Optional[threading.Thread] = None
_last_hash: Optional[str] = None
_last_http_ok = False
_ws_connected = False
_ws_connecting = False


def get_http_last_ok() -> bool:
    return _last_http_ok


def get_ws_status() -> str:
    if _ws_connected:
        return "connected"
    if _ws_connecting:
        return "connecting"
    return "disconnected"


def _content_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8", errors="replace")).hexdigest()


def _publish_raw(raw: Any) -> None:
    global _last_hash
    if isinstance(raw, (dict, list)):
        try:
            canonical = json.dumps(raw, sort_keys=True, ensure_ascii=False)
        except (TypeError, ValueError):
            canonical = str(raw)
    else:
        canonical = str(raw)
    h = _content_hash(canonical)
    if h == _last_hash:
        return
    parsed = parse_custom_payload(raw)
    if parsed is None:
        return
    _last_hash = h
    get_event_bus().publish(
        "eew",
        SOURCE_ID,
        {"type": "update", "Data": parsed.get("raw_data", raw)},
    )


def _poll_http(url: str) -> None:
    global _last_http_ok
    reg = get_source_status_registry()
    try:
        r = requests.get(
            url,
            timeout=HTTP_TIMEOUT,
            proxies={"http": None, "https": None},
            headers={"User-Agent": "FusedCore-Custom/1.0"},
        )
        r.raise_for_status()
        _last_http_ok = True
        reg.record_ok(SOURCE_ID)
        data = r.json()
        _publish_raw(data)
    except Exception as e:
        _last_http_ok = False
        reg.record_error(SOURCE_ID, str(e))


def _ws_loop(url: str) -> None:
    global _ws_connected, _ws_connecting
    try:
        import websocket
    except ImportError:
        logger.error("websocket-client 未安装，无法连接自定义 WS")
        return

    reg = get_source_status_registry()

    def on_message(_ws, message: str):
        try:
            data = json.loads(message)
            reg.record_ok(SOURCE_ID)
            _publish_raw(data)
        except Exception as e:
            reg.record_error(SOURCE_ID, str(e))

    def on_open(_ws):
        global _ws_connected, _ws_connecting
        _ws_connected = True
        _ws_connecting = False
        reg.set_connected(SOURCE_ID, True)

    def on_close(_ws, *_args):
        global _ws_connected, _ws_connecting
        _ws_connected = False
        _ws_connecting = False
        reg.set_connected(SOURCE_ID, False)

    def on_error(_ws, err):
        reg.record_error(SOURCE_ID, str(err))

    while not _stop.is_set():
        _ws_connecting = True
        _ws_connected = False
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_close=on_close,
                on_error=on_error,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            reg.record_error(SOURCE_ID, str(e))
        finally:
            _ws_connected = False
            _ws_connecting = False
            reg.set_connected(SOURCE_ID, False)
        if not _stop.is_set():
            time.sleep(3)


def _main_loop() -> None:
    reg = get_source_status_registry()
    reg.register(SOURCE_ID, "自定义数据源", "eew")
    while not _stop.is_set():
        url = get_custom_data_source_url()
        if not url:
            reg.set_connected(SOURCE_ID, False)
            time.sleep(1)
            continue
        low = url.lower()
        if low.startswith("http://") or low.startswith("https://"):
            reg.set_connected(SOURCE_ID, True)
            _poll_http(url)
            _stop.wait(POLL_INTERVAL)
        elif low.startswith("ws://") or low.startswith("wss://"):
            _ws_loop(url)
        else:
            reg.record_error(SOURCE_ID, "不支持的 URL 协议")
            time.sleep(2)


def start() -> threading.Thread:
    global _thread, _stop, _last_hash
    _stop.clear()
    _last_hash = None
    _thread = threading.Thread(target=_main_loop, name="CustomSource", daemon=True)
    _thread.start()
    return _thread


def stop() -> None:
    global _thread, _stop, _ws_connected, _ws_connecting, _last_http_ok
    _stop.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=5)
    _thread = None
    _ws_connected = False
    _ws_connecting = False
    _last_http_ok = False
    try:
        get_source_status_registry().set_connected(SOURCE_ID, False)
    except Exception:
        pass
