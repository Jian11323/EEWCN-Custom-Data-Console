from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from services.fused.eew.config import Config

class CacheManager:
    """缓存管理器"""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.lock = threading.RLock()
        
        # 内存缓存（源名称 -> 事件数据）
        self.memory_cache: Dict[str, Dict[str, Any]] = {}
        
        # 融合缓存（预警 WS 5000）
        self.fused_cache_5000: List[Dict] = []
        self.source_index_5000: Dict[str, int] = {}
        
        # MD5缓存（用于Fan Studio数据源去重）
        self.md5_cache: Dict[str, Dict[str, Any]] = {}
        
        # 文件缓存目录
        os.makedirs(config.CACHE_DIR, exist_ok=True)
    
    def save_source_cache(self, source_name: str, data: Dict[str, Any]):
        """保存源缓存到文件（异步）"""
        def _save():
            try:
                cache_file = os.path.join(self.config.CACHE_DIR, f"{source_name}_cache.json")
                cache_data = {
                    "data": data,
                    "last_update": time.time(),
                    "save_time": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                }
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error(f"保存{source_name}缓存失败: {e}")
        
        threading.Thread(target=_save, daemon=True, name=f"SaveCache-{source_name}").start()
    
    def load_source_cache(self, source_name: str) -> Optional[Dict[str, Any]]:
        """从文件加载源缓存"""
        try:
            cache_file = os.path.join(self.config.CACHE_DIR, f"{source_name}_cache.json")
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"加载{source_name}缓存失败: {e}")
        return None
    
    def update_memory_cache(self, source_name: str, data: Dict[str, Any]):
        """更新内存缓存"""
        with self.lock:
            self.memory_cache[source_name] = {
                'data': data,
                'last_update': time.time()
            }
    
    def get_memory_cache(self, source_name: str, max_age: int = None) -> Optional[Dict[str, Any]]:
        """获取内存缓存"""
        max_age = max_age or self.config.CACHE_MAX_AGE
        with self.lock:
            cache = self.memory_cache.get(source_name)
            if cache and (time.time() - cache['last_update']) <= max_age:
                return cache['data']
        return None
    
    def get_fused_cache(self, port: int = 5000) -> List[Dict]:
        """获取融合缓存（端口 5000）"""
        with self.lock:
            if port == 5000:
                return self.fused_cache_5000[:]
        return []
    
    def update_fused_cache(self, port: int, events: List[Dict], source_index: Dict[str, int]):
        """更新融合缓存（端口 5000）"""
        with self.lock:
            if port == 5000:
                self.fused_cache_5000 = events
                self.source_index_5000 = source_index

