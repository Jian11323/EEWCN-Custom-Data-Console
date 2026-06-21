from __future__ import annotations

import time
from typing import Any, Dict, Optional

from services.fused.eew.sources.fanstudio_base import (
    FanStudioSource,
    _cwa_origin_time_ms,
    _cwa_place_name_from_payload,
)
from services.fused.eew.utils import Utils

class CWAFanStudioSourceV2(FanStudioSource):
    """台湾气象署预警数据源（来自 Fan Studio /all）"""
    
    def __init__(self, *args, **kwargs):
        # 使用独立的 source_key，与经 1450 聚合的 CWA 区分
        super().__init__("CWA_FS", "cwa-eew", *args, **kwargs)
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            shock_time_str = data.get('shockTime', '') or ''
            timestamp_ms = Utils.parse_time_utc_offset(str(shock_time_str).strip(), 8) if shock_time_str else 0
            if timestamp_ms <= 0:
                timestamp_ms = _cwa_origin_time_ms(data)
            if timestamp_ms <= 0:
                self.logger.warning(
                    f"CWA(Fan Studio) 数据时间无效，跳过: {data.get('id', data.get('identifier', 'unknown'))}"
                )
                return None

            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            updates = Utils.safe_int(data.get('number', data.get('updates', 1)), 1)

            lat = Utils.safe_float(data.get('epicenterLat', data.get('latitude', 0)))
            lon = Utils.safe_float(data.get('epicenterLon', data.get('longitude', 0)))
            place_name = _cwa_place_name_from_payload(data, lat, lon, self.logger)

            epicenter = f"{place_name} (CWA)"
            epicenter_tts = Utils.format_epicenter_tts(place_name)

            event_data = {
                "eventId": str(data.get('identifier') or data.get('id') or data.get('eventId', 'unknown')),
                "updates": updates,
                "report_number": updates,
                "latitude": lat,
                "longitude": lon,
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": epicenter_tts,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name,
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理 CWA(Fan Studio) 数据失败: {e}")
        return None


