"""数值与格式化工具。"""

from __future__ import annotations

from typing import Any


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def format_magnitude(value: Any, default: float = 0.0) -> float:
    return round(safe_float(value, default), 1)


def format_depth(value: Any, default: int = 0) -> int:
    return int(round(safe_float(value, default), 0))
