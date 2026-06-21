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

class BMKGSource:
    """印度尼西亚BMKG数据源"""
    
    @staticmethod
    def process():
        """处理BMKG数据源"""
        source_name = "BMKG"
        if Utils.check_circuit_breaker(source_name):
            return

        try:
            data = BMKGSource.fetch()
            if data:
                parsed_data = BMKGSource.parse(data if isinstance(data, list) else [data])
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                Utils.reset_circuit_breaker(source_name)
        except Exception as e:
            logger.error(f"处理 {source_name} 数据时发生错误: {e}")
            Utils.handle_fetch_error(source_name, e)

    @staticmethod
    def initial_load():
        """仅在启动时执行一次：HTTP 全量拉取 BMKG 并写入融合列表"""
        source_name = "BMKG"
        logger.info("BMKG: 启动初始化开始（HTTP 一次性拉取）")
        try:
            if Utils.check_circuit_breaker(source_name):
                logger.warning("BMKG: 熔断器已打开，跳过启动初始化")
                return
            response = requests.get(API_URLS['BMKG'], timeout=10)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or "Infogempa" not in data:
                logger.info("BMKG: 启动初始化响应格式异常")
                return
            event_list = data.get("Infogempa", {}).get("gempa", []) if isinstance(data.get("Infogempa"), dict) else []
            if not event_list:
                logger.info("BMKG: 启动初始化无列表数据")
                return
            parsed_data = BMKGSource.parse(event_list)
            if parsed_data:
                FusionHandler.add_events_to_fused_list(parsed_data)
            first_dt = event_list[0].get("DateTime", "")
            last_dt = event_list[-1].get("DateTime", "")
            data_id = first_dt if event_list[0] is event_list[-1] else f"{first_dt}_{last_dt}"
            cache_state["BMKG"]["id"] = data_id
            cache_state["BMKG"]["count"] = len(event_list)
            cache_state["BMKG"]["latest_id"] = first_dt
            cache_state["BMKG"]["latest_time"] = str(first_dt)
            cache_state["BMKG"]["last_success"] = datetime.now()
            Utils.reset_circuit_breaker(source_name)
            logger.info(f"BMKG: 启动初始化完成，加载 {len(parsed_data or [])} 条地震事件")
        except Exception as e:
            logger.error(f"BMKG: 启动初始化失败: {e}")
            Utils.handle_fetch_error(source_name, e)
    
    @staticmethod
    def fetch():
        """获取BMKG全部数据并全部解析"""
        def extract_events(data):
            if not isinstance(data, dict) or "Infogempa" not in data:
                return []
            infogempa = data.get("Infogempa", {})
            return infogempa.get("gempa", []) if isinstance(infogempa, dict) else []

        def get_id(first, last):
            first_dt = first.get("DateTime", "")
            return first_dt if first is last else f"{first_dt}_{last.get('DateTime', '')}"

        def get_time(event):
            return event.get("DateTime", "")

        return BMKGSource._fetch_and_check_cache(API_URLS['BMKG'], "BMKG", None, extract_events, get_id, get_time)
    
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
        """解析BMKG数据"""
        result = []
        for item in data:
            try:
                # 获取震级
                magnitude_str = item.get("Magnitude", "")
                if not magnitude_str:
                    continue
                try:
                    mag = float(magnitude_str)
                except (ValueError, TypeError):
                    continue
                if mag <= 0:
                    continue

                # 获取时间并转换为北京时间
                event_time_str = item.get("DateTime", "")
                if not event_time_str:
                    tanggal = item.get("Tanggal", "")
                    jam = item.get("Jam", "")
                    if not (tanggal and jam):
                        continue

                    try:
                        indonesian_months = {
                            "Jan": "Jan", "Feb": "Feb", "Mar": "Mar", "Apr": "Apr",
                            "Mei": "May", "Jun": "Jun", "Jul": "Jul", "Agu": "Aug",
                            "Sep": "Sep", "Okt": "Oct", "Nov": "Nov", "Des": "Dec"
                        }
                        tanggal_parts = tanggal.split()
                        if len(tanggal_parts) >= 3 and tanggal_parts[1] in indonesian_months:
                            tanggal_parts[1] = indonesian_months[tanggal_parts[1]]
                            tanggal = " ".join(tanggal_parts)

                        date_obj = datetime.strptime(tanggal, "%d %b %Y")
                        time_obj = datetime.strptime(jam.split()[0], "%H:%M:%S")
                        combined_time = date_obj.replace(hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
                        event_time_str = pytz.timezone('Asia/Jakarta').localize(combined_time).astimezone(pytz.UTC).isoformat()
                    except (ValueError, TypeError, IndexError):
                        continue

                event_time = Utils.parse_time(event_time_str)
                if not event_time:
                    continue

                if event_time.tzinfo is None:
                    event_time = pytz.timezone('Asia/Jakarta').localize(event_time)
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

                # 获取坐标
                try:
                    coordinates = item.get("Coordinates", "")
                    if coordinates:
                        coords = coordinates.split(",")
                        if len(coords) >= 2:
                            lat = float(coords[0].strip())
                            lon = float(coords[1].strip())
                        else:
                            lat, lon = 0.0, 0.0
                    else:
                        lintang = item.get("Lintang", "")
                        bujur = item.get("Bujur", "")
                        if lintang and bujur:
                            lat = float(lintang.split()[0])
                            if "LS" in lintang.upper():
                                lat = -abs(lat)
                            lon = float(bujur.split()[0])
                            if "BB" in bujur.upper():
                                lon = -abs(lon)
                        else:
                            lat, lon = 0.0, 0.0
                except (ValueError, TypeError, IndexError):
                    lat, lon = 0.0, 0.0

                # 获取深度
                try:
                    kedalaman = item.get("Kedalaman", "")
                    if kedalaman:
                        depth_match = re.search(r'(\d+(?:\.\d+)?)', kedalaman)
                        if depth_match:
                            depth = float(depth_match.group(1))
                        else:
                            depth = 0.0
                    else:
                        depth = 0.0
                except (ValueError, TypeError):
                    depth = 0.0

                # 获取位置信息
                wilayah = item.get("Wilayah", "未知地区")
                if not wilayah or not isinstance(wilayah, str):
                    wilayah = "未知地区"

                try:
                    location = wilayah.strip()
                    location = re.sub(r'^\d+\s*km\s+(BaratLaut|BaratDaya|Tenggara|TimurLaut|Barat|Timur|Utara|Selatan)\s+', '', location, flags=re.IGNORECASE).strip()
                    if not location:
                        location = wilayah.strip()
                    location = TranslationService.translate_location(location, lat=lat, lon=lon, source='BMKG')
                except Exception:
                    location = TranslationService.translate_location(wilayah, lat=lat, lon=lon, source='BMKG')

                # 获取事件ID
                event_id = item.get("DateTime", "")
                if not event_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    event_id = f"bmkg_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"

                # 构建事件字典
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
                    "LOCATION_C": location + " (BMKG)",
                    "epicenter_tts": location,
                    "INTENSITY": "",
                    "SOURCE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "EVENT_ID": event_id,
                    "infoTypeName": "地震报告"
                }
                result.append(event)
            except Exception:
                continue
        return result

# ============================================================================
# INGV数据源
# ============================================================================
