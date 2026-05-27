"""内部数据源采集（无对外 WebSocket 端口）。"""

from services.internal.runner import start_internal_fetchers, stop_internal_fetchers

__all__ = ["start_internal_fetchers", "stop_internal_fetchers"]
