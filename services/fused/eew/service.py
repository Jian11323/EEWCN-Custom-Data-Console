"""EEW 子系统入口。"""

from __future__ import annotations

from services.fused.eew.engine import EEWService


class EEWModule:
    def __init__(self):
        self.service: EEWService | None = None

    def create(self) -> EEWService:
        self.service = EEWService()
        return self.service

    @property
    def ws_client_mgr(self):
        return self.service.ws_client_mgr if self.service else None
