from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import datetime

import requests

from services.fused.list.config import API_URLS, Config
from services.fused.list.state import cache_state, error_stats, logger
from services.fused.list.fusion import FusionHandler
from services.fused.list.sources.processor_maps import DataSourceProcessor
from services.fused.list.state import logger
from services.fused.list.translation import TranslationService
from services.fused.list.utils import Utils

class INGVSource:
    """意大利国家地球物理与火山学研究数据源"""
    
    @staticmethod
    def process():
        """处理INGV数据源"""
        from services.common.source_switches import is_list_enabled
        if not is_list_enabled("INGV"):
            return
        source_name = "INGV"
        from services.common.source_status import get_source_status_registry
        reg = get_source_status_registry()
        if Utils.check_circuit_breaker(source_name):
            return

        try:
            data = INGVSource.fetch()
            reg.record_ok("ingv")
            if data:
                parsed_data = INGVSource.parse(data if isinstance(data, list) else [data])
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                    reg.record_event("ingv")
                Utils.reset_circuit_breaker(source_name)
        except Exception as e:
            logger.error(f"处理 {source_name} 数据时发生错误: {e}")
            reg.record_error("ingv", str(e))
            Utils.handle_fetch_error(source_name, e)
    
    @staticmethod
    def fetch():
        """获取INGV全部数据并全部解析"""
        def extract_events(data):
            if not isinstance(data, dict):
                return []
            payload = data.get("payload")
            if not isinstance(payload, list):
                return []
            return payload

        def get_id(first, last):
            first_id = str(first.get("properties", {}).get("eventId", ""))
            last_id = str(last.get("properties", {}).get("eventId", ""))
            return first_id if first is last else f"{first_id}_{last_id}"

        def get_time(event):
            return event.get("properties", {}).get("time", "")

        return INGVSource._fetch_and_check_cache(API_URLS['INGV'], "INGV", None, extract_events, get_id, get_time)
    
    @staticmethod
    def _fetch_and_check_cache(url, source_key, headers=None, extract_events_fn=None, get_id_fn=None, get_time_fn=None):
        """通用数据获取和缓存检查函数"""
        try:
            timeout = 10
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            event_list = extract_events_fn(data) if extract_events_fn else []
            if not event_list or len(event_list) == 0:
                return None

            event_count = len(event_list)
            data_id = get_id_fn(event_list[0], event_list[-1]) if get_id_fn else ""
            latest_event_id = get_id_fn(event_list[0], event_list[0]) if get_id_fn else ""
            latest_event_time = get_time_fn(event_list[0]) if get_time_fn else ""

            cache = cache_state[source_key]
            if (data_id == cache["id"] and event_count == cache["count"] and
                latest_event_id == cache["latest_id"] and str(latest_event_time) == cache["latest_time"]):
                return None

            cache["id"] = data_id
            cache["count"] = event_count
            cache["latest_id"] = latest_event_id
            cache["latest_time"] = str(latest_event_time)
            cache["last_success"] = datetime.now()

            return event_list
        except Exception:
            return None
    
    @staticmethod
    def parse(data):
        """解析INGV数据"""
        result = []
        for item in data:
            try:
                properties = item.get("properties") or {}
                geometry = item.get("geometry") or {}

                mag = properties.get("mag")
                if mag is None:
                    continue
                try:
                    mag = float(mag)
                except (TypeError, ValueError):
                    continue
                if mag <= 0:
                    continue

                event_time_str = properties.get("time", "")
                if not event_time_str:
                    continue

                event_time = Utils.parse_time(event_time_str)
                if not event_time:
                    continue

                if event_time.tzinfo is None:
                    event_time = pytz.UTC.localize(event_time)
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

                coords = geometry.get("coordinates")
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    try:
                        lon = float(coords[0]) if coords[0] is not None else 0.0
                    except (TypeError, ValueError):
                        lon = 0.0
                    try:
                        lat = float(coords[1]) if coords[1] is not None else 0.0
                    except (TypeError, ValueError):
                        lat = 0.0
                    try:
                        depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else 0.0
                    except (TypeError, ValueError):
                        depth = 0.0
                else:
                    lat, lon, depth = 0.0, 0.0, 0.0

                location_raw = properties.get("place", "未知地区")
                if not location_raw or not isinstance(location_raw, str):
                    location_raw = "未知地区"

                location = TranslationService.translate_location(location_raw, lat=lat, lon=lon, source='INGV')

                event_id = properties.get("eventId") or properties.get("originId") or ""
                if not event_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    event_id = f"ingv_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"
                else:
                    event_id = str(event_id)

                event = {
                    "id": event_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(lat),
                    "EPI_LON": str(lon),
                    "EPI_DEPTH": round(depth),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M",
                    "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (INGV)",
                    "epicenter_tts": location,
                    "INTENSITY": "",
                    "SOURCE": SOURCE_NAMES.get("INGV", "INGV"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("INGV", "INGV"),
                    "EVENT_ID": event_id,
                    "infoTypeName": "地震报告"
                }
                result.append(event)
            except Exception:
                continue
        return result

# ============================================================================
# 数据源处理器（保留用于FanStudio数据源解析器）
# ============================================================================
