from __future__ import annotations

import time
from typing import Any, Dict, Optional

from services.fused.eew.sources.fanstudio_base import FanStudioSource
from services.fused.eew.utils import Utils

class CEAPRSource(FanStudioSource):
    """地震局预警数据源"""
    
    def __init__(self, *args, **kwargs):
        super().__init__("CEA_PR", "cea-pr", *args, **kwargs)
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            # 解析基础字段
            shock_time_str = data.get('shockTime', '')
            timestamp_ms = Utils.parse_time_utc_offset(shock_time_str, 8)
            
            # 如果时间无效，返回None（不推送无效数据）
            if timestamp_ms <= 0:
                self.logger.warning(f"CEA_PR数据时间无效，跳过: {data.get('eventId', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            updates = Utils.safe_int(data.get('updates', 1), 1)
            
            province = data.get('province', '')
            suffix = f"({province}地震局)" if province else "(地震局)"
            epicenter = f"{data.get('placeName', '未知地点')} {suffix}"
            
            raw_event_id = str(data.get('eventId', data.get('id', 'unknown')))
            event_id = f"{raw_event_id}-CEA"
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": event_id,
                "updates": updates,
                "report_number": updates,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": Utils.format_epicenter_tts(epicenter),
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": f"{province}地震局预警"
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理CEA_PR数据失败: {e}")
        return None


class CEASource(FanStudioSource):
    """中国预警网数据源"""
    
    def __init__(self, *args, **kwargs):
        super().__init__("CEA", "cea", *args, **kwargs)
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            # 解析基础字段
            shock_time_str = data.get('shockTime', '')
            timestamp_ms = Utils.parse_time_utc_offset(shock_time_str, 8)
            
            # 如果时间无效，返回None
            if timestamp_ms <= 0:
                self.logger.warning(f"CEA数据时间无效，跳过: {data.get('eventId', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            updates = Utils.safe_int(data.get('updates', 1), 1)
            
            epicenter = f"{data.get('placeName', '未知地点')} (CN)"
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": str(data.get('eventId', data.get('id', 'unknown'))),
                "updates": updates,
                "report_number": updates,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": Utils.format_epicenter_tts(epicenter),
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": "中国预警网预警"
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理CEA数据失败: {e}")
        return None


