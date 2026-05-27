"""内部事件总线：采集模块 → EEW/List 融合管道。"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, List, Literal, Optional

from services.common.source_status import get_source_status_registry

logger = logging.getLogger("common.bus")

Channel = Literal["eew", "list"]

Handler = Callable[[str, Dict[str, Any]], None]


class InternalEventBus:
    """按 channel + source_id 分发内部事件。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._handlers: Dict[Channel, List[Handler]] = {"eew": [], "list": []}

    def subscribe(self, channel: Channel, handler: Handler) -> None:
        with self._lock:
            if handler not in self._handlers[channel]:
                self._handlers[channel].append(handler)

    def publish(self, channel: Channel, source_id: str, payload: Dict[str, Any]) -> None:
        reg = get_source_status_registry()
        reg.record_event(source_id)
        with self._lock:
            handlers = list(self._handlers[channel])
        for h in handlers:
            try:
                h(source_id, payload)
            except Exception as e:
                logger.error("bus handler [%s/%s] 异常: %s", channel, source_id, e, exc_info=True)


_bus: Optional[InternalEventBus] = None


def get_event_bus() -> InternalEventBus:
    global _bus
    if _bus is None:
        _bus = InternalEventBus()
    return _bus
