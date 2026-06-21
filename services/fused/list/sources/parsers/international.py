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

class InternationalParsers:
    @staticmethod
    def parse_hko_data(data):
        """解析香港天文台数据"""
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

            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='HKO')
            event_id = data.get("eventId") or data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"hko_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            verify = data.get("verify", "")
            if verify == "Y":
                auto_flag = "M"
                info_type_name = "已核实"
                is_auto = False
            else:
                auto_flag = "[自动测定]"
                info_type_name = "待核实"
                is_auto = True

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": auto_flag,
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (HKO)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("HKO", "香港天文台"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("HKO", "香港天文台"),
                "EVENT_ID": event_id,
                "IS_AUTO": is_auto,
                "infoTypeName": info_type_name
            }
        except Exception as e:
            logger.error(f"解析香港天文台数据失败: {e}")
            return None

    @staticmethod
    def parse_usgs_data(data):
        """解析USGS数据"""
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

            info_type_name = data.get("infoTypeName", "")
            if info_type_name == "automatic":
                auto_flag = "[自动测定]"
                result_info_type_name = "Automatic[自动测定]"
                is_auto = True
            else:
                auto_flag = "M"
                result_info_type_name = "Reviewed"
                is_auto = False

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='USGS')

            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"usgs_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": auto_flag,
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (USGS)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("USGS", "美国地质调查局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("USGS", "美国地质调查局"),
                "EVENT_ID": event_id,
                "IS_AUTO": is_auto,
                "infoTypeName": result_info_type_name
            }
        except Exception as e:
            logger.error(f"解析USGS数据失败: {e}")
            return None

    @staticmethod
    def parse_emsc_data(data):
        """解析EMSC数据"""
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

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='EMSC')

            # 确保地名不为空
            if not location or not location.strip():
                location = "未知地区"
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"emsc_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

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
                "LOCATION_C": f"{location} (EMSC)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("EMSC", "欧洲地中海地震中心"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("EMSC", "欧洲地中海地震中心"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析EMSC数据失败: {e}")
            return None

    @staticmethod
    def parse_fssn_data(data):
        """解析FSSN数据"""
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
            
            # FSSN 优先使用 placeName_zh，如果存在则直接使用，不进行地名修正
            place_name_zh = data.get("placeName_zh")
            if place_name_zh:
                location = place_name_zh
            else:
                place_name = data.get("placeName", "未知地区")
                # 使用新的地名处理优先级（FE fix -> 翻译API）
                location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='FSSN')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"fssn_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            info_type_name = data.get("infoTypeName", "")
            if "正式" in info_type_name or "已核实" in info_type_name:
                auto_flag = "M"
                result_info_type_name = "已核实"
                is_auto = False
            else:
                auto_flag = "[自动测定]"
                result_info_type_name = "已确认[自动测定]"
                is_auto = True

            result = {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": auto_flag,
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (FSSN)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("FSSN", "FSSN"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("FSSN", "FSSN"),
                "EVENT_ID": event_id,
                "IS_AUTO": is_auto,
                "infoTypeName": result_info_type_name
            }
            # 保存原始的 placeName_zh 用于台湾数据判断
            if place_name_zh:
                result["placeName_zh"] = place_name_zh
            return result
        except Exception as e:
            logger.error(f"解析FSSN数据失败: {e}")
            return None

    @staticmethod
    def parse_bcsf_data(data):
        """解析BCSF数据"""
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

            location = place_name

            if "near of" in location:
                parts = location.split("near of")
                if len(parts) > 1:
                    location = parts[1].strip()
                    if "(" in location:
                        location = location.split("(")[0].strip()

            location = re.sub(r'Quarry blast of magnitude \d+\.\d+,?\s*', '', location)
            location = re.sub(r',?\s*\([^)]*\)', '', location)
            location = location.strip()

            if not location:
                location = place_name

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(location, lat=latitude, lon=longitude, source='BCSF')

            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"bcsf_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

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
                "LOCATION_C": f"{location} (BCSF)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("BCSF", "法国中央地震研究所"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BCSF", "法国中央地震研究所"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析BCSF数据失败: {e}")
            return None

    @staticmethod
    def parse_gfz_data(data):
        """解析GFZ数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude")
            if magnitude is None or magnitude <= 0:
                return None

            latitude = data.get("latitude")
            if latitude is None:
                return None

            longitude = data.get("longitude")
            if longitude is None:
                return None

            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='GFZ')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"gfz_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

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
                "LOCATION_C": f"{location} (GFZ)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("GFZ", "德国地学研究中心"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("GFZ", "德国地学研究中心"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析GFZ数据失败: {e}")
            return None

    @staticmethod
    def parse_usp_data(data):
        """解析USP数据"""
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

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='USP')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"usp_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

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
                "LOCATION_C": f"{location} (USP)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("USP", "巴西圣保罗大学地震信息"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("USP", "巴西圣保罗大学地震信息"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析USP数据失败: {e}")
            return None

    @staticmethod
    def parse_kma_data(data):
        """解析KMA数据"""
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

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='KMA')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"kma_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

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
                "LOCATION_C": f"{location} (KMA)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("KMA", "韩国气象厅"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("KMA", "韩国气象厅"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析KMA数据失败: {e}")
            return None

