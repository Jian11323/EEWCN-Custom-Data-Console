"""Fan Studio 连接健康状态（供管理与自动切换）。"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Literal, Optional


@dataclass
class FanStudioHealth:
    primary_url: str
    backup_url: str
    current_url: str = ""
    manual_target: Optional[Literal["primary", "backup"]] = None
    is_using_backup: bool = False
    primary_fail_count: int = 0
    primary_fail_threshold: int = 20
    connection_quality: str = "unknown"
    lock: threading.RLock = field(default_factory=threading.RLock)

    def status_dict(self) -> dict:
        with self.lock:
            return {
                "current_url": self.current_url,
                "is_using_backup": self.is_using_backup,
                "manual_target": self.manual_target,
                "primary_fail_count": self.primary_fail_count,
                "connection_quality": self.connection_quality,
            }
