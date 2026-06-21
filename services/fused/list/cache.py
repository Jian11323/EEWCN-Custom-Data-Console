from __future__ import annotations

import json
import os
from collections import deque
from datetime import datetime

from services.fused.list.config import Config, FANSTUDIO_NO_RAW_CACHE_SOURCES
from services.fused.list.fusion import FusionHandler
from services.fused.list.sources.processor_maps import (
    FAN_STUDIO_PARSERS,
    INTERNAL_WS_PARSERS,
)
from services.fused.list.state import fanstudio_cache_lock, fanstudio_raw_cache, logger
from services.fused.list.utils import Utils

class CacheManager:
    """缓存管理器"""
    
    @staticmethod
    def save_fanstudio_cache():
        """保存FanStudio缓存"""
        try:
            if not os.path.exists(Config.CACHE_DIR):
                os.makedirs(Config.CACHE_DIR, exist_ok=True)

            with fanstudio_cache_lock:
                for source, cache_deque in fanstudio_raw_cache.items():
                    if not cache_deque:
                        continue

                    cache_file = os.path.join(Config.CACHE_DIR, f'{source}_cache.json')
                    cache_list = list(cache_deque)

                    try:
                        with open(cache_file, 'w', encoding='utf-8') as f:
                            json.dump(cache_list, f, ensure_ascii=False, indent=2)
                        logger.debug(f"FanStudio: 保存 {source} 缓存 {len(cache_list)} 条原始数据到 {cache_file}")
                    except Exception as e:
                        logger.error(f"FanStudio: 保存 {source} 缓存文件失败: {e}，文件路径: {cache_file}")

        except Exception as e:
            logger.error(f"FanStudio: 保存缓存文件失败: {e}")
    
    @staticmethod
    def load_fanstudio_cache():
        """加载FanStudio缓存"""
        try:
            if not os.path.exists(Config.CACHE_DIR):
                logger.warning(f"FanStudio: 缓存目录不存在: {Config.CACHE_DIR}，跳过加载")
                return

            total_loaded = 0
            events_to_push = []

            with fanstudio_cache_lock:
                fanstudio_raw_cache.clear()

                # 遍历所有数据源的缓存文件（含 FanStudio 与内网 WS 的 bmkg/geonet）
                _parsers_all = dict(FAN_STUDIO_PARSERS)
                _parsers_all.update(INTERNAL_WS_PARSERS)

                for source_name in _parsers_all.keys():
                    if source_name in FANSTUDIO_NO_RAW_CACHE_SOURCES:
                        continue
                    cache_file = os.path.join(Config.CACHE_DIR, f'{source_name}_cache.json')

                    if not os.path.exists(cache_file):
                        continue

                    try:
                        file_size = os.path.getsize(cache_file)
                        if file_size == 0:
                            continue

                        with open(cache_file, 'r', encoding='utf-8') as f:
                            raw_data_list = json.load(f)

                        if not isinstance(raw_data_list, list) or not raw_data_list:
                            continue

                        # 保存原始数据到内存缓存
                        cache_deque = deque(raw_data_list, maxlen=Config.MAX_CACHE_PER_SOURCE)
                        fanstudio_raw_cache[source_name] = cache_deque

                        # 解析原始数据
                        parser = _parsers_all[source_name]
                        parsed_events = []
                        for raw_data in raw_data_list:
                            try:
                                event = parser(raw_data)
                                if event:
                                    parsed_events.append(event)
                            except Exception as e:
                                logger.warning(f"FanStudio: 解析 {source_name} 缓存数据失败: {e}")
                                continue

                        # 按时间排序并限制数量
                        sorted_events = sorted(parsed_events, key=lambda x: Utils.parse_time(x.get("O_TIME")) or datetime.min, reverse=True)
                        sorted_events = sorted_events[:Config.MAX_CACHE_PER_SOURCE]

                        logger.debug(f"FanStudio: 从 {source_name}_cache.json 加载并解析 {len(sorted_events)} 条数据")
                        total_loaded += len(sorted_events)
                        events_to_push.extend(sorted_events)

                    except json.JSONDecodeError as e:
                        logger.error(f"FanStudio: {source_name} 缓存文件JSON解析失败: {e}")
                    except Exception as e:
                        logger.error(f"FanStudio: 加载 {source_name} 缓存文件失败: {e}")

            if total_loaded > 0:
                logger.info(f"FanStudio: 成功加载并解析 {total_loaded} 条缓存数据")
                FusionHandler.add_events_to_fused_list(events_to_push, bulk_quiet_cenc_logs=True)
            else:
                logger.info("FanStudio: 未找到有效的缓存数据")

        except Exception as e:
            logger.error(f"FanStudio: 加载缓存失败: {e}")

