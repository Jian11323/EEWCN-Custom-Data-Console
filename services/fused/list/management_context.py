"""List 管理端口显式上下文（替代 fl_module 鸭子类型）。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass
class ListManagementContext:
    """List 侧管理命令所需依赖。"""

    WebSocketHandler: Any
    FAN_STUDIO_SWITCH_CONFIG: dict
    fused_events: Any
    fused_data_lock: Any
    error_stats: dict
    cache_state: dict

    @classmethod
    def from_module(cls, fl_module) -> "ListManagementContext":
        return cls(
            WebSocketHandler=fl_module.WebSocketHandler,
            FAN_STUDIO_SWITCH_CONFIG=fl_module.FAN_STUDIO_SWITCH_CONFIG,
            fused_events=fl_module.fused_events,
            fused_data_lock=fl_module.fused_data_lock,
            error_stats=fl_module.error_stats,
            cache_state=fl_module.cache_state,
        )


def execute_list_command_with_context(
    command: str,
    ctx: ListManagementContext,
    params: Dict[str, Any] | None = None,
) -> Any:
    """通过显式上下文执行命令（包装 legacy execute_list_command）。"""
    from services.list import management_ws

    class _ModuleProxy:
        WebSocketHandler = ctx.WebSocketHandler
        FAN_STUDIO_SWITCH_CONFIG = ctx.FAN_STUDIO_SWITCH_CONFIG
        fused_events = ctx.fused_events
        fused_data_lock = ctx.fused_data_lock
        error_stats = ctx.error_stats
        cache_state = ctx.cache_state

    return management_ws.execute_list_command(command, _ModuleProxy(), params)
