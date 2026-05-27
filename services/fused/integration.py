"""EEW / List 与共享 Fan Studio、internal bus 的接线。"""

from __future__ import annotations

import logging

logger = logging.getLogger("fused.integration")


def wire_services(eew_service, list_engine_module) -> None:
    from services.common.bus import get_event_bus
    from services.common.fanstudio import get_fanstudio_connection, get_fanstudio_router

    bus = get_event_bus()
    conn = get_fanstudio_connection()
    router = get_fanstudio_router()

    WebSocketHandler = list_engine_module.WebSocketHandler

    if hasattr(eew_service, "ws_client_mgr"):
        eew_service.ws_client_mgr.attach_shared_fanstudio(router, conn)
        eew_service.ws_client_mgr.attach_internal_bus(bus)

    WebSocketHandler.attach_shared_fanstudio(router, conn)
    WebSocketHandler.attach_internal_bus(bus)

    conn.start()
    if hasattr(eew_service, "ws_server") and eew_service.ws_server:
        eew_service.ws_server.set_list_engine_module(list_engine_module)
    logger.info("Fan Studio 共享连接已接线（List GeoNet/BMKG 改 HTTP 直连）")
