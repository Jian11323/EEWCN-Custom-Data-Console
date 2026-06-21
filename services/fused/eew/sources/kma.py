from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from services.fused.eew.sources.fanstudio_base import FanStudioSource
from services.fused.eew.utils import Utils

class KMASource(FanStudioSource):
    """韩国气象厅数据源"""

    def __init__(self, *args, **kwargs):
        super().__init__("KMA", "kma-eew", *args, **kwargs)
        self.korea_region_data = self._load_korea_region_data()

    def _load_korea_region_data(self) -> List[Dict]:
        """加载韩国区域数据"""
        try:
            from services.common.regions import get_korea_regions
            regions = get_korea_regions()
            n = len(regions)
            if n:
                self.logger.info(f"已加载 {n} 个 KMA 地名修正区域 (data/korea_region_data.json)")
            else:
                self.logger.warning("KMA 地名修正为空，请检查 data/korea_region_data.json")
            return regions
        except Exception as e:
            self.logger.error(f"加载韩国区域数据失败: {e}")
        return []

    def _apply_region_to_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        lat = Utils.safe_float(event.get("latitude", 0))
        lon = Utils.safe_float(event.get("longitude", 0))
        korea_region = self._get_korea_region(lat, lon)
        if korea_region:
            event = dict(event)
            event["epicenter"] = f"{korea_region} (KMA)"
            event["epicenter_tts"] = korea_region
        return event

    def _get_korea_region(self, lat: float, lon: float) -> str:
        """根据经纬度获取韩国地名"""
        for region in self.korea_region_data:
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
            # KMA时间为UTC+9，转换为UTC+8
            shock_time_str = data.get('shockTime', '')
            if shock_time_str:
                try:
                    naive_time = datetime.strptime(shock_time_str, "%Y-%m-%d %H:%M:%S")
                    korea_time = naive_time.replace(tzinfo=timezone(timedelta(hours=9)))
                    beijing_time = korea_time.astimezone(timezone(timedelta(hours=8)))
                    timestamp_ms = int(beijing_time.timestamp() * 1000)
                except Exception:
                    timestamp_ms = 0
            else:
                timestamp_ms = 0
            
            # 如果时间无效，返回None
            if timestamp_ms <= 0:
                self.logger.warning(f"KMA数据时间无效，跳过: {data.get('id', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))

            lat = Utils.safe_float(data.get('latitude', 0))
            lon = Utils.safe_float(data.get('longitude', 0))

            # 根据坐标从korea_region_data.json中匹配韩国地名（不使用翻译API）
            korea_region = self._get_korea_region(lat, lon)
            if korea_region:
                # 匹配成功，使用区域名称（已经是中文）
                location = korea_region
            else:
                # 匹配失败，使用原始地名（不调用翻译API）
                place_name = data.get('placeName', '未知地点')
                location = place_name

            epicenter = f"{location} (KMA)"
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": str(data.get('eventId', data.get('id', 'unknown'))),
                "updates": 1,
                "report_number": 1,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": location,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理KMA数据失败: {e}")
        return None

