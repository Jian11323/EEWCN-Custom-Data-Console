"""融合服务启停顺序。"""

from __future__ import annotations

import logging

logger = logging.getLogger("fused.lifecycle")


def shutdown_all(fan_conn=None, eew_service=None) -> None:
    if fan_conn:
        try:
            fan_conn.stop()
        except Exception as e:
            logger.error(f"停止 Fan Studio: {e}")
    if eew_service:
        try:
            eew_service._graceful_shutdown()
        except Exception as e:
            logger.error(f"EEW 关闭: {e}")
