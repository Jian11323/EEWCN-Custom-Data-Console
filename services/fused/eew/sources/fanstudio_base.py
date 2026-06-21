from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from services.fused.eew.sources.base import DataSource
from services.fused.eew.utils import Utils

class FanStudioSource(DataSource):
    """Fan Studio数据源基类"""
    
    def __init__(self, source_key: str, fan_key: str, *args, **kwargs):
        super().__init__(source_key, *args, **kwargs)
        self.fan_key = fan_key  # Fan Studio中的key名称
        self.raw_cache = None
        self.raw_cache_time = 0
        self.event_distributor = None  # 将在主服务中设置
    
    def on_message(self, raw_data: Dict[str, Any], md5: str):
        """处理Fan Studio消息"""
        # CWA 等源用 number 表示报数，与 fetch() 一致，避免 MD5 未变时误丢弃续报
        updates = Utils.safe_int(raw_data.get('number', raw_data.get('updates', 1)), 1)
        
        # MD5去重
        last_info = self.cache_mgr.md5_cache.get(self.source_key, {})
        if md5 == last_info.get('md5', '') and updates <= last_info.get('updates', 0):
            return False
        
        self.cache_mgr.md5_cache[self.source_key] = {'md5': md5, 'updates': updates}
        
        receive_time = time.time()  # 记录接收时间
        
        with self.lock:
            self.raw_cache = raw_data
            self.raw_cache_time = receive_time
            self.connected = True
        
        # 立即处理并推送（原脚本方式）
        try:
            event = self.fetch()
            t_after_fetch = time.time()
            if event and self.event_distributor:
                target_ports = self.get_target_ports()
                self.event_distributor.distribute(self.source_key, event, target_ports)
            t_after_distribute = time.time()
            if event and self.event_distributor:
                # 埋点：收到→fetch / 收到→distribute 结束（毫秒）
                fetch_ms = (t_after_fetch - receive_time) * 1000
                distribute_ms = (t_after_distribute - receive_time) * 1000
                self.logger.debug(f"{self.source_key} 延迟: fetch={fetch_ms:.1f}ms, distribute={distribute_ms:.1f}ms")
                push_delay = (t_after_distribute - receive_time) * 1000  # 毫秒
                self.logger.debug(f"{self.source_key}推送延迟: {push_delay:.1f}ms")

                # 异步保存缓存
                self.cache_mgr.save_source_cache(self.source_key, event)
        except Exception as e:
            self.logger.error(f"{self.source_key}即时推送失败: {e}")
        
        return True

# ============================================================================
# CWA(Fan Studio) 辅助解析
# ============================================================================

def _cwa_origin_time_ms(data: Dict[str, Any]) -> int:
    """shockTime 无效时，尝试其它时间字段（均为 UTC+8）。"""
    for key in ("originTime", "OriginTime", "otime", "createTime", "announcedTime", "AnnouncedTime"):
        val = data.get(key)
        if not val:
            continue
        s = str(val).strip()
        if not s:
            continue
        ms = Utils.parse_time_utc_offset(s, 8)
        if ms > 0:
            return ms
        try:
            if s.endswith("Z"):
                s = s[:-1].strip()
            naive = datetime.strptime(s, "%Y/%m/%d %H:%M:%S")
            utc_time = naive.replace(tzinfo=timezone.utc)
            beijing = utc_time.astimezone(timezone(timedelta(hours=8)))
            return int(beijing.timestamp() * 1000)
        except (ValueError, TypeError):
            continue
    return 0


def _cwa_place_name_from_bracket(raw: str) -> str:
    """从 CWA placeName 括号中提取地名，如「地震（位於花蓮縣光復鄉）」。"""
    if not raw or not isinstance(raw, str):
        return ""
    text = raw.strip()
    if not text:
        return ""
    bracket_match = re.search(r'\(([^)]+)\)', text)
    if bracket_match:
        loc = bracket_match.group(1).replace("位於", "").replace("位于", "")
        return re.sub(r'\s+', ' ', loc).strip()
    return text


def _cwa_place_name_from_payload(
    data: Dict[str, Any],
    lat: float,
    lon: float,
    logger: Optional[logging.Logger] = None,
) -> str:
    """优先使用报文地名，否则按坐标匹配 taiwan_region_data.json。"""
    for key in ("placeName", "epicenterName", "hypoCenter", "HypoCenter", "region", "location"):
        raw = data.get(key)
        if not raw:
            continue
        name = _cwa_place_name_from_bracket(str(raw))
        if name and name not in ("未知地区", "未知地点"):
            return name

    if lat or lon:
        try:
            from services.common.regions import get_taiwan_regions, match_region_by_coords
            regions = get_taiwan_regions()
            if regions:
                matched = match_region_by_coords(
                    regions,
                    lat,
                    lon,
                    bbox_keys=("lat_min", "lat_max", "lon_min", "lon_max", "name"),
                )
                if matched:
                    return matched
        except Exception as e:
            if logger:
                logger.debug(f"CWA 坐标地名匹配失败: lat={lat}, lon={lon}, {e}")

    return "未知地点"


# ============================================================================
# 具体Fan Studio数据源实现
# ============================================================================

