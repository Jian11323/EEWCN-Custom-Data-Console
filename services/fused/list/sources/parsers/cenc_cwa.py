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

class CencCwaParsers:
    @staticmethod
    def parse_cenc_data(data):
        """解析CENC数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            try:
                magnitude = float(data.get("magnitude", 0))
            except (ValueError, TypeError):
                magnitude = 0

            if magnitude <= 0:
                return None

            try:
                latitude = float(data.get("latitude", 0))
            except (ValueError, TypeError):
                latitude = 0.0

            try:
                longitude = float(data.get("longitude", 0))
            except (ValueError, TypeError):
                longitude = 0.0

            try:
                depth = float(data.get("depth", 0))
            except (ValueError, TypeError):
                depth = 0.0

            place_name = data.get("placeName", "未知地区")

            info_type_name = data.get("infoTypeName", "")
            auto_flag = data.get("autoFlag", "")
            is_auto = False

            if "[自动测定]" in info_type_name or auto_flag == "I":
                is_auto = True
                flag = "[自动测定]"
            elif "[正式测定]" in info_type_name or auto_flag == "M":
                is_auto = False
                flag = "M"
            else:
                if auto_flag == "I":
                    is_auto = True
                    flag = "[自动测定]"
                else:
                    is_auto = False
                    flag = "M"

            # CENC 直接返回原始地名
            location = place_name
            event_id = data.get("eventId") or data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"cenc_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(depth),
                "AUTO_FLAG": flag,
                "EQ_TYPE": "M",
                "M": f"{magnitude:.1f}",
                "LOCATION_C": f"{location} (CENC)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("CENC", "中国地震台网中心"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("CENC", "中国地震台网中心"),
                "IS_AUTO": is_auto,
                "EVENT_ID": event_id,
                "infoTypeName": info_type_name if info_type_name else ("[自动测定]" if is_auto else "[正式测定]")
            }
        except Exception as e:
            logger.error(f"解析Fan Studio CENC数据失败: {e}")
            return None

    @staticmethod
    def parse_cwa_fanstudio_data(data):
        """解析 FanStudio WebSocket 中的 cwa.Data（速报/测定统一格式）"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None
            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None
            if event_time.tzinfo is None:
                event_time_utc8 = pytz.timezone('Asia/Taipei').localize(event_time).astimezone(pytz.timezone('Asia/Shanghai'))
            else:
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

            try:
                mag = float(data.get("magnitude", 0) or 0)
            except (ValueError, TypeError):
                mag = 0.0

            try:
                lat = float(data.get("latitude", 0))
                lon = float(data.get("longitude", 0))
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0

            try:
                depth = float(data.get("depth", 0))
            except (ValueError, TypeError):
                depth = 0.0

            place_name = data.get("placeName", "未知地区")
            if not place_name or not isinstance(place_name, str):
                place_name = "未知地区"
            bracket_match = re.search(r'\(([^)]+)\)', place_name)
            if bracket_match:
                location = bracket_match.group(1).replace("位於", "")
                location = re.sub(r'\s+', ' ', location).strip()
            else:
                location = "未知地区"
            event_id = str(data.get("eventId", "") or data.get("id", "") or "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"cwa_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"

            intensity = ""
            max_intensity = data.get("maxIntensity")
            if max_intensity and isinstance(max_intensity, str):
                m_int = re.search(r"(\d+)", max_intensity)
                if m_int:
                    intensity = m_int.group(1)

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(lat),
                "EPI_LON": str(lon),
                "EPI_DEPTH": round(depth),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{mag:.1f}",
                "LOCATION_C": location + " (CWA)",
                "epicenter_tts": location,
                "INTENSITY": intensity,
                "SOURCE": SOURCE_NAMES.get("CWA", "CWA"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("CWA", "CWA"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析FanStudio CWA数据失败: {e}")
            return None
