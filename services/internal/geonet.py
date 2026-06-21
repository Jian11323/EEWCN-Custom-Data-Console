"""GeoNet 速报轮询 → list channel。"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from typing import Optional

import requests

from services.common.bus import get_event_bus
from services.common.http_poll_intervals import get_poll_interval
from services.common.source_status import get_source_status_registry

logger = logging.getLogger("internal.geonet")

SOURCE_ID = "geonet"
API_URL = "https://api.geonet.org.nz/quake?MMI=-1"
HTTP_TIMEOUT = 12

_stop = threading.Event()
_latest_md5: Optional[str] = None


def _data_md5(data: dict) -> str:
    return hashlib.md5(json.dumps(data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def extract_first_item(raw: str) -> Optional[dict]:
    """从 API 响应取 features[0] GeoJSON Feature。"""
    try:
        obj = json.loads(raw)
        features = obj.get("features")
        if not features or not isinstance(features, list):
            return None
        first = features[0]
        return first if isinstance(first, dict) else None
    except (json.JSONDecodeError, IndexError, TypeError):
        return None


def fetch_api() -> Optional[str]:
    try:
        r = requests.get(
            API_URL,
            timeout=HTTP_TIMEOUT,
            proxies={"http": None, "https": None},
            headers={"User-Agent": "FusedCore-Internal/1.0"},
        )
        r.raise_for_status()
        return r.text
    except Exception as e:
        get_source_status_registry().record_error(SOURCE_ID, str(e))
        return None


def _poll_once() -> None:
    global _latest_md5
    reg = get_source_status_registry()
    raw = fetch_api()
    if raw is None:
        return
    data = extract_first_item(raw)
    if data is None:
        return
    reg.record_ok(SOURCE_ID)
    md5 = _data_md5(data)
    if md5 == _latest_md5:
        return
    _latest_md5 = md5
    get_event_bus().publish("list", SOURCE_ID, {"Data": data, "type": "update"})


def _loop() -> None:
    reg = get_source_status_registry()
    reg.register(SOURCE_ID, "GeoNet 速报", "list")
    reg.set_connected(SOURCE_ID, True)
    while not _stop.is_set():
        try:
            _poll_once()
        except Exception as e:
            logger.exception("GeoNet 轮询异常")
            reg.record_error(SOURCE_ID, str(e))
        _stop.wait(get_poll_interval("geonet"))


def start() -> threading.Thread:
    _stop.clear()
    t = threading.Thread(target=_loop, name="Internal-GeoNet", daemon=True)
    t.start()
    return t


def stop() -> None:
    _stop.set()
