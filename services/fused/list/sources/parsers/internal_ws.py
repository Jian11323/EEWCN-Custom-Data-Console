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

class InternalWsParsers:
    @staticmethod
    def parse_bmkg_fanstudio_data(data):
        """解析 all_ws 1450 / bmkg_ws 的 bmkg.Data。
        历史与实时均为 BMKG TEWS 原结构：DateTime、Coordinates、Magnitude、Kedalaman、Wilayah 等，
        与 HTTP BMKGSource.parse 一致；不再使用 shockTime/placeName/扁平 latitude 等 FanStudio 旧字段。"""
        try:
            if not isinstance(data, dict):
                return None
            # 仍含 shockTime 的旧消息：尽量走 BMKG 官方字段；若无 DateTime/Coordinates 再回退 shockTime
            if data.get("shockTime") and not (data.get("DateTime") or data.get("Coordinates")):
                shock_time = data.get("shockTime")
                event_time = Utils.parse_time(shock_time)
                if not event_time:
                    return None
                if event_time.tzinfo is None:
                    event_time_utc8 = pytz.timezone('Asia/Jakarta').localize(event_time).astimezone(
                        pytz.timezone('Asia/Shanghai'))
                else:
                    event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))
                try:
                    mag = float(data.get("magnitude", 0))
                except (ValueError, TypeError):
                    mag = 0
                if mag <= 0:
                    return None
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
                try:
                    location = TranslationService.translate_location(place_name.strip(), lat=lat, lon=lon, source='BMKG')
                except Exception:
                    location = TranslationService.translate_location(place_name, lat=lat, lon=lon, source='BMKG')
                event_id = str(data.get("eventId", "") or "")
                if not event_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    event_id = f"bmkg_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"
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
                    "LOCATION_C": location + " (BMKG)",
                    "epicenter_tts": location,
                    "INTENSITY": "",
                    "SOURCE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "EVENT_ID": event_id,
                    "infoTypeName": "地震报告"
                }
            parsed = BMKGSource.parse([data])
            return parsed[0] if parsed else None
        except Exception as e:
            logger.error(f"解析 BMKG WebSocket 数据失败: {e}")
            return None

    @staticmethod
    def parse_geonet_fanstudio_data(data):
        """解析 all_ws 1450 / GeoNet_ws 的 geonet.Data。
        标准 GeoJSON Feature：geometry.coordinates、properties.time/publicID/magnitude/depth/locality，
        与 HTTP GEONETSource.parse 一致；不再使用 shockTime/placeName 等 FanStudio 旧字段。"""
        try:
            if not isinstance(data, dict):
                return None
            geom = data.get("geometry") if isinstance(data.get("geometry"), dict) else {}
            props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
            is_feature = data.get("type") == "Feature" or (
                "coordinates" in geom and isinstance(props, dict)
            )
            if is_feature:
                parsed = GEONETSource.parse([data])
                if parsed:
                    return parsed[0]
            if data.get("shockTime"):
                shock_time = data.get("shockTime")
                event_time = Utils.parse_time(shock_time)
                if not event_time:
                    return None
                if event_time.tzinfo is None:
                    event_time_utc8 = pytz.timezone('Pacific/Auckland').localize(event_time).astimezone(
                        pytz.timezone('Asia/Shanghai'))
                else:
                    event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))
                try:
                    mag = float(data.get("magnitude", 0))
                except (ValueError, TypeError):
                    mag = 0
                if mag <= 0:
                    return None
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
                try:
                    location = TranslationService.translate_location(place_name.strip(), lat=lat, lon=lon, source='GEONET')
                except Exception:
                    location = TranslationService.translate_location(place_name, lat=lat, lon=lon, source='GEONET')
                mmi = data.get("mmi")
                if mmi is None:
                    intensity = ""
                else:
                    try:
                        mmi_value = float(mmi)
                        intensity = str(int(mmi_value)) if mmi_value >= 0 else ""
                    except (ValueError, TypeError):
                        intensity = str(mmi) if mmi else ""
                public_id = str(data.get("eventId", "") or "")
                if not public_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    public_id = f"geonet_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"
                return {
                    "id": public_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(lat),
                    "EPI_LON": str(lon),
                    "EPI_DEPTH": round(depth),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M",
                    "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (GeoNet)",
                    "epicenter_tts": location,
                    "INTENSITY": intensity,
                    "SOURCE": SOURCE_NAMES.get("GEONET", "GEONET"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("GEONET", "GEONET"),
                    "EVENT_ID": public_id,
                    "infoTypeName": "地震报告"
                }
            return None
        except Exception as e:
            logger.error(f"解析 GeoNet WebSocket 数据失败: {e}")
            return None
