"""各数据源采集状态注册表，供管理 WS 与控制台查询。"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


@dataclass
class SourceStatus:
    source_id: str
    label: str = ""
    channel: str = ""  # eew | list | fan | upstream
    connected: bool = False
    last_ok_at: Optional[float] = None
    last_error: str = ""
    last_event_at: Optional[float] = None
    message_count: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        for k in ("last_ok_at", "last_event_at"):
            if d[k] is not None:
                d[k] = int(d[k])
        return d


class SourceStatusRegistry:
    def __init__(self):
        self._lock = threading.RLock()
        self._sources: Dict[str, SourceStatus] = {}

    def register(self, source_id: str, label: str = "", channel: str = "") -> None:
        with self._lock:
            if source_id not in self._sources:
                self._sources[source_id] = SourceStatus(
                    source_id=source_id, label=label or source_id, channel=channel
                )

    def set_connected(self, source_id: str, connected: bool) -> None:
        with self._lock:
            st = self._sources.setdefault(source_id, SourceStatus(source_id=source_id))
            st.connected = connected
            if connected:
                st.last_ok_at = time.time()
                st.last_error = ""

    def record_ok(self, source_id: str) -> None:
        with self._lock:
            st = self._sources.setdefault(source_id, SourceStatus(source_id=source_id))
            st.connected = True
            st.last_ok_at = time.time()
            st.last_error = ""

    def record_error(self, source_id: str, error: str) -> None:
        with self._lock:
            st = self._sources.setdefault(source_id, SourceStatus(source_id=source_id))
            st.last_error = str(error)[:500]
            st.connected = False

    def record_event(self, source_id: str) -> None:
        with self._lock:
            st = self._sources.setdefault(source_id, SourceStatus(source_id=source_id))
            st.last_event_at = time.time()
            st.message_count += 1

    def set_extra(self, source_id: str, **kwargs: Any) -> None:
        with self._lock:
            st = self._sources.setdefault(source_id, SourceStatus(source_id=source_id))
            st.extra.update(kwargs)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "timestamp": int(time.time()),
                "sources": {k: v.to_dict() for k, v in sorted(self._sources.items())},
            }


_registry: Optional[SourceStatusRegistry] = None


def get_source_status_registry() -> SourceStatusRegistry:
    global _registry
    if _registry is None:
        _registry = SourceStatusRegistry()
    return _registry
