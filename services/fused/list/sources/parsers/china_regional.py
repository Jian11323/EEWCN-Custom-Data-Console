from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta

import pytz

from services.common.source_filters import get_filter_registry, resolve_list_source_id
from services.fused.list.config import Config, EXCLUDED_SOURCES, NO_THRESHOLD_SOURCES, SOURCE_NAMES
from services.fused.list.state import logger
from services.fused.list.translation import TranslationService
from services.fused.list.utils import Utils

class ChinaRegionalParsers:
    @staticmethod
    def parse_ningxia_data(data):
        """解析宁夏地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            # 震级有可能为 None 或无法转换为数字，这里做更健壮的处理
            magnitude = data.get("magnitude")
            if magnitude is None:
                return None
            try:
                magnitude = float(magnitude)
            except (TypeError, ValueError):
                return None
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # NINGXIA 直接返回原始地名
            location = place_name
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"ningxia_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (宁夏)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("NINGXIA", "宁夏地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("NINGXIA", "宁夏地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析宁夏数据失败: {e}")
            return None

    @staticmethod
    def parse_guangxi_data(data):
        """解析广西地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # GUANGXI 直接返回原始地名
            location = place_name
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"guangxi_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (广西地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("GUANGXI", "广西地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("GUANGXI", "广西地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析广西数据失败: {e}")
            return None

    @staticmethod
    def parse_yunnan_data(data):
        """解析云南地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            # 云南局震级：优先使用 magnitude，缺失时回退到 magnitudel（ml）
            magnitude_raw = data.get("magnitude")
            if magnitude_raw in (None, "", " "):
                magnitude_raw = data.get("magnitudel")

            # 若仍然为空，视为无效事件
            if magnitude_raw is None:
                return None

            # 尝试将震级转换为浮点数，失败则视为无效
            try:
                magnitude = float(magnitude_raw)
            except (TypeError, ValueError):
                return None

            # 非正震级直接丢弃
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = place_name
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"yunnan_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (云南地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("YUNNAN", "云南地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("YUNNAN", "云南地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析云南数据失败: {e}")
            return None

    @staticmethod
    def parse_shanxi_data(data):
        """解析山西地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='SHANXI')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"shanxi_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (山西地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("SHANXI", "山西地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("SHANXI", "山西地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析山西数据失败: {e}")
            return None

    @staticmethod
    def parse_beijing_data(data):
        """解析北京地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='BEIJING')
            event_id = data.get("eventId") or data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"beijing_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (北京地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("BEIJING", "北京地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BEIJING", "北京地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析北京数据失败: {e}")
            return None

