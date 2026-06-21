from __future__ import annotations

from services.fused.list.fusion import FusionHandler
from services.fused.list.state import logger
from services.fused.list.utils import Utils


class DataSourceBase:
    """数据源基类：提供通用的处理流程"""

    @staticmethod
    def process(source_name, fetch_func, parse_func):
        """通用处理流程"""
        if Utils.check_circuit_breaker(source_name):
            return
        
        try:
            data = fetch_func()
            if data:
                parsed_data = parse_func(data if isinstance(data, list) else [data])
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                Utils.reset_circuit_breaker(source_name)
        except Exception as e:
            logger.error(f"处理 {source_name} 数据时发生错误: {e}")
            Utils.handle_fetch_error(source_name, e)

# ============================================================================
# JMA数据源
# ============================================================================
