from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

from services.fused.eew.cache import CacheManager
from services.fused.eew.config import Config
from services.fused.eew.translation import TranslationService

class DataSource:
    """数据源基类"""

    CWA_EEW_DISPLAY_NAME = "台湾气象署预警"
    CWA_EEW_MUTEX_KEYS = frozenset({"CWA_FS"})

    SOURCE_NAME_MAP = {
        "CUSTOM": "自定义数据源",
        "CEA_PR": "地震局预警",
        "CEA": "中国预警网",
        "CWA_FS": CWA_EEW_DISPLAY_NAME,
        "SA": "美国地质调查局预警",
        "KMA": "韩国气象厅预警",
        "JMA": "日本气象厅预警",
        "EARLY_EST": "Early-est 预警",
    }
    INTERNAL_REGISTRY_IDS = {
        "CUSTOM": "custom",
        "EARLY_EST": "early-est",
    }

    @staticmethod
    def resolve_connected(
        source_key: str,
        source: "DataSource",
        reg_sources: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """解析数据源连接状态（内部源以 status registry 为准）。"""
        reg_id = DataSource.INTERNAL_REGISTRY_IDS.get(source_key)
        if reg_id:
            if reg_sources is None:
                from services.common.source_status import get_source_status_registry
                reg_sources = get_source_status_registry().snapshot().get("sources", {})
            info = reg_sources.get(reg_id)
            if info is not None:
                return bool(info.get("connected"))
        return bool(getattr(source, "connected", False))
    
    def __init__(self, source_key: str, config: Config, logger: logging.Logger, 
                 cache_mgr: CacheManager, translator: TranslationService):
        self.source_key = source_key
        self.source_name = self.SOURCE_NAME_MAP.get(source_key, source_key)
        self.config = config
        self.logger = logger
        self.cache_mgr = cache_mgr
        self.translator = translator
        self.connected = False
        self.lock = threading.RLock()
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        """获取数据（需子类实现）"""
        raise NotImplementedError
    
    def get_target_ports(self) -> List[int]:
        """获取目标端口列表（预警 WebSocket）"""
        from services.common.ports import get_eew_port

        return [get_eew_port()]


