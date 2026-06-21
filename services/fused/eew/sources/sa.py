from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from services.fused.eew.sources.fanstudio_base import FanStudioSource
from services.fused.eew.utils import Utils

class SASource(FanStudioSource):
    """美国地质调查局数据源"""
    
    def __init__(self, *args, **kwargs):
        super().__init__("SA", "sa", *args, **kwargs)
        self.region_data = self._load_region_data()
    
    def _load_region_data(self) -> List[Dict]:
        """加载SA区域数据"""
        try:
            from services.common.regions import get_sa_regions
            regions = get_sa_regions()
            n = len(regions)
            if n:
                self.logger.info(f"已加载 {n} 个 USGS/SA 地名修正区域 (data/sa_region_data.json)")
            else:
                self.logger.warning("USGS/SA 地名修正为空，请检查 data/sa_region_data.json")
            return regions
        except Exception as e:
            self.logger.error(f"加载SA区域数据失败: {e}")
        return []

    def _apply_region_to_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """按坐标刷新 epicenter（用于缓存回放）。"""
        lat = Utils.safe_float(event.get("latitude", 0))
        lon = Utils.safe_float(event.get("longitude", 0))
        sa_region = self._get_region(lat, lon)
        if sa_region:
            event = dict(event)
            event["epicenter"] = f"{sa_region} (USGS)"
            event["epicenter_tts"] = sa_region
        return event
    
    def _get_region(self, lat: float, lon: float) -> str:
        """根据坐标获取区域名称"""
        for region in self.region_data:
            try:
                if (region.get('lat_min', -90) <= lat <= region.get('lat_max', 90) and
                    region.get('lon_min', -180) <= lon <= region.get('lon_max', 180)):
                    return region.get('name', '')
            except Exception:
                continue
        return ''
    
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
                self.logger.warning(f"SA数据时间无效，跳过: {data.get('eventId', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            
            lat = Utils.safe_float(data.get('latitude', 0))
            lon = Utils.safe_float(data.get('longitude', 0))
            
            # 根据坐标从sa_region_data.json中匹配区域（不使用百度翻译）
            sa_region = self._get_region(lat, lon)
            if sa_region:
                # 匹配成功，使用区域名称（已经是中文）
                epicenter = f"{sa_region} (USGS)"
                epicenter_tts = sa_region
            else:
                # 匹配失败，使用原始地名（不调用翻译API）
                original = data.get('placeName', '未知地点')
                epicenter = f"{original} (USGS)"
                epicenter_tts = Utils.format_epicenter_tts(epicenter)
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": str(data.get('eventId', data.get('id', 'unknown'))),
                "updates": 1,
                "report_number": 1,
                "latitude": lat,
                "longitude": lon,
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": epicenter_tts,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理SA数据失败: {e}")
        return None


