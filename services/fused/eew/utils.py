from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from services.fused.common.numeric import (
    format_depth,
    format_magnitude,
    safe_float,
    safe_int,
)

class Utils:
    """工具类"""
    
    @staticmethod
    def safe_float(value: Any, default: float = 0.0) -> float:
        """安全转换为浮点数"""
        return safe_float(value, default)
    
    @staticmethod
    def safe_int(value: Any, default: int = 0) -> int:
        """安全转换为整数"""
        return safe_int(value, default)
    
    @staticmethod
    def format_magnitude(value: Any, default: float = 0.0) -> float:
        """格式化震级"""
        return format_magnitude(value, default)
    
    @staticmethod
    def format_depth(value: Any, default: int = 0) -> int:
        """格式化震源深度"""
        return format_depth(value, default)
    
    @staticmethod
    def format_epicenter_tts(epicenter: str) -> str:
        """格式化震中地名（用于TTS）"""
        return re.sub(r'\s*\([^)]*\)$', '', epicenter).strip()
    
    @staticmethod
    def format_o_time(timestamp_ms: int) -> str:
        """格式化发震时间"""
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S')
    
    @staticmethod
    def parse_time_utc_offset(time_str: str, utc_offset: int = 8) -> int:
        """解析时间字符串并转换为指定时区的毫秒时间戳"""
        if not time_str:
            return 0  # 返回0而不是当前时间，避免每次都生成新的时间戳
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            tz = timezone(timedelta(hours=utc_offset))
            dt_with_tz = dt.replace(tzinfo=tz)
            return int(dt_with_tz.timestamp() * 1000)
        except (ValueError, TypeError):
            return 0  # 返回0而不是当前时间

