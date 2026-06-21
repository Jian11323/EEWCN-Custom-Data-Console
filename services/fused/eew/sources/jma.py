from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from services.fused.eew.sources.fanstudio_base import FanStudioSource
from services.fused.eew.utils import Utils

class JMAFanStudioSource(FanStudioSource):
    """日本气象厅数据源"""

    def __init__(self, *args, **kwargs):
        super().__init__("JMA", "jma", *args, **kwargs)

    def on_message(self, raw_data: Dict[str, Any], md5: str):
        """处理Fan Studio JMA消息（支持cancel撤销）"""
        if not raw_data or not isinstance(raw_data, dict):
            return False

        # cancel 报文：撤销该事件预警（同时从融合缓存列表移除）
        if raw_data.get('cancel') is True:
            event_id = str(raw_data.get('id', 'unknown'))
            updates = Utils.safe_int(raw_data.get('updates', 1), 1)
            self.cache_mgr.md5_cache[self.source_key] = {'md5': md5, 'updates': updates}
            with self.lock:
                self.raw_cache = raw_data
                self.raw_cache_time = time.time()
                self.connected = True

            if self.event_distributor:
                cancel_event = {
                    "type": "cancel",
                    "source": "JMA",
                    "eventId": event_id,
                    "updates": updates,
                    "timestamp": time.time()
                }
                self.event_distributor.distribute(self.source_key, cancel_event, self.get_target_ports())
            return True

        # 非cancel报文：走通用FanStudioSource逻辑（含MD5去重+即时推送）
        return super().on_message(raw_data, md5)

    def fetch(self) -> Optional[Dict[str, Any]]:
        """解析Fan Studio推送的JMA数据"""
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache

        try:
            if data.get('cancel') is True:
                return None

            # shockTime 为 UTC+9，转换为 UTC+8
            shock_time_str = data.get('shockTime', '')
            if shock_time_str:
                try:
                    naive = datetime.strptime(shock_time_str, "%Y-%m-%d %H:%M:%S")
                    tokyo = naive.replace(tzinfo=timezone(timedelta(hours=9)))
                    beijing = tokyo.astimezone(timezone(timedelta(hours=8)))
                    timestamp_ms = int(beijing.timestamp() * 1000)
                except Exception as e:
                    self.logger.warning(f"JMA时间解析失败: {shock_time_str}, {e}")
                    timestamp_ms = 0
            else:
                timestamp_ms = 0

            # 如果时间无效，返回None
            if timestamp_ms <= 0:
                self.logger.warning(f"JMA数据时间无效，跳过: {data.get('id', 'unknown')}")
                return None

            updates = Utils.safe_int(data.get('updates', 1), 1)
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))

            place_name = data.get('placeName', '未知地点')
            info_type = data.get('infoTypeName', '')  # '警報' / '予報'

            # 直接使用原始地名，不进行翻译
            # 保留"警报"处理方式：仅当 infoTypeName == '警報' 才添加前缀
            translated_place = place_name
            if info_type == '警報':
                translated_place = f"（警報）{translated_place}"

            epicenter = f"{translated_place} (JMA)"

            # 构建标准事件数据结构
            event_data = {
                "eventId": str(data.get('id', 'unknown')),
                "updates": updates,
                "report_number": updates,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": translated_place,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name
            }

            # 附加JMA特有字段
            event_data["final"] = bool(data.get("final", False))
            event_data["cancel"] = bool(data.get("cancel", False))
            event_data["epiIntensity"] = data.get("epiIntensity", "")
            event_data["infoTypeName"] = info_type
            event_data["createTime"] = data.get("createTime", "")

            return event_data
        except Exception as e:
            self.logger.error(f"解析JMA数据失败: {e}")
        return None


# ============================================================================
# Wolfx all_eew -> Fan Studio 形态（CEA / JMA）映射
# ============================================================================

