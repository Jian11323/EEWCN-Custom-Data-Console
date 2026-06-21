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

class GEONETSource:
    """新西兰GeoNet数据源"""
    
    @staticmethod
    def process():
        """处理GeoNet数据源"""
        source_name = "GEONET"
        start_time = time.time()
        
        if Utils.check_circuit_breaker(source_name):
            return

        try:
            fetch_start = time.time()
            data = GEONETSource.fetch()
            fetch_time = time.time() - fetch_start
            if fetch_time > 5:
                logger.debug(f"[{source_name}] 数据获取耗时: {fetch_time:.2f}秒")
            
            if data:
                parse_start = time.time()
                parsed_data = GEONETSource.parse(data if isinstance(data, list) else [data])
                parse_time = time.time() - parse_start
                if parse_time > 5:
                    logger.debug(f"[{source_name}] 数据解析耗时: {parse_time:.2f}秒，处理了 {len(data)} 条数据")
                
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                Utils.reset_circuit_breaker(source_name)
            
            total_time = time.time() - start_time
            if total_time > 10:
                logger.debug(f"[{source_name}] 总处理耗时: {total_time:.2f}秒")
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"处理 {source_name} 数据时发生错误 (耗时 {total_time:.2f}秒): {e}")
            Utils.handle_fetch_error(source_name, e)

    @staticmethod
    def initial_load():
        """仅在启动时执行一次：HTTP 全量拉取 GeoNet 并写入融合列表"""
        source_name = "GEONET"
        logger.info("GEONET: 启动初始化开始（HTTP 一次性拉取）")
        try:
            if Utils.check_circuit_breaker(source_name):
                logger.warning("GEONET: 熔断器已打开，跳过启动初始化")
                return
            headers = {"Accept": "application/vnd.geo+json;version=2", "Accept-Encoding": "gzip"}
            response = requests.get(API_URLS['GEONET'], headers=headers, timeout=35)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or "features" not in data:
                logger.info("GEONET: 启动初始化响应无 features")
                return
            event_list = [f for f in data.get("features", []) if isinstance(f, dict) and "properties" in f]
            if not event_list:
                logger.info("GEONET: 启动初始化无地震要素")
                return
            parsed_data = GEONETSource.parse(event_list)
            if parsed_data:
                FusionHandler.add_events_to_fused_list(parsed_data)
            first_id = event_list[0].get("properties", {}).get("publicID", "")
            last_id = event_list[-1].get("properties", {}).get("publicID", "")
            data_id = first_id if event_list[0] is event_list[-1] else f"{first_id}_{last_id}"
            cache_state["GEONET"]["id"] = data_id
            cache_state["GEONET"]["count"] = len(event_list)
            cache_state["GEONET"]["latest_id"] = first_id
            cache_state["GEONET"]["latest_time"] = str(event_list[0].get("properties", {}).get("time", ""))
            cache_state["GEONET"]["last_success"] = datetime.now()
            Utils.reset_circuit_breaker(source_name)
            logger.info(f"GEONET: 启动初始化完成，加载 {len(parsed_data or [])} 条地震事件")
        except Exception as e:
            logger.error(f"GEONET: 启动初始化失败: {e}")
            Utils.handle_fetch_error(source_name, e)
    
    @staticmethod
    def fetch():
        """获取GeoNet全部数据并全部解析"""
        def extract_events(data):
            if not isinstance(data, dict) or "features" not in data:
                return []
            features = data.get("features", [])
            return [f for f in features if isinstance(f, dict) and "properties" in f]

        def get_id(first, last):
            first_id = first.get("properties", {}).get("publicID", "")
            return first_id if first is last else f"{first_id}_{last.get('properties', {}).get('publicID', '')}"

        def get_time(event):
            return event.get("properties", {}).get("time", "")

        headers = {"Accept": "application/vnd.geo+json;version=2", "Accept-Encoding": "gzip"}
        return GEONETSource._fetch_and_check_cache(API_URLS['GEONET'], "GEONET", headers, extract_events, get_id, get_time)
    
    @staticmethod
    def _fetch_and_check_cache(url, source_key, headers=None, extract_events_fn=None, get_id_fn=None, get_time_fn=None):
        """通用数据获取和缓存检查函数"""
        try:
            timeout = 35 if source_key == "GEONET" else 10
            fetch_start = time.time()
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            fetch_time = time.time() - fetch_start
            
            parse_start = time.time()
            data = response.json()
            parse_time = time.time() - parse_start
            
            if source_key == "GEONET" and (fetch_time > 10 or parse_time > 5):
                logger.debug(f"[{source_key}] HTTP请求耗时: {fetch_time:.2f}秒, JSON解析耗时: {parse_time:.2f}秒")

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
        """解析GeoNet数据"""
        result = []
        for feature in data:
            try:
                properties = feature.get("properties", {})
                if properties.get("quality") == "deleted":
                    continue

                # 获取震级
                mag = properties.get("magnitude")
                if mag is None:
                    continue
                try:
                    mag = float(mag)
                except (ValueError, TypeError):
                    continue
                if mag <= 0:
                    continue

                # 获取时间并转换为北京时间
                event_time_str = properties.get("time")
                if not event_time_str:
                    continue

                event_time = Utils.parse_time(event_time_str)
                if not event_time:
                    continue

                if event_time.tzinfo is None:
                    event_time = pytz.UTC.localize(event_time)
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

                # 获取坐标
                try:
                    geometry = feature.get("geometry", {})
                    if isinstance(geometry, dict) and "coordinates" in geometry:
                        coords = geometry.get("coordinates", [])
                        if len(coords) >= 2:
                            lat = float(coords[1])
                            lon = float(coords[0])
                        else:
                            lat, lon = 0.0, 0.0
                    else:
                        lat, lon = 0.0, 0.0
                except (ValueError, TypeError, IndexError):
                    lat, lon = 0.0, 0.0

                # 获取深度
                try:
                    depth = float(properties.get("depth", 0)) if properties.get("depth") is not None else 0.0
                except (ValueError, TypeError):
                    depth = 0.0

                # 获取位置信息
                locality = properties.get("locality", "未知地区")
                if not locality or not isinstance(locality, str):
                    locality = "未知地区"

                try:
                    location = locality.strip()
                    location = re.sub(r'^\d+\s*km\s+(north|south|east|west|north-east|north-west|south-east|south-west)\s+of\s+', '', location, flags=re.IGNORECASE).strip()
                    if not location:
                        location = locality.strip()
                    location = TranslationService.translate_location(location, lat=lat, lon=lon, source='GEONET')
                except Exception:
                    location = TranslationService.translate_location(locality, lat=lat, lon=lon, source='GEONET')

                # 获取烈度
                mmi = properties.get("mmi")
                if mmi is None:
                    intensity = ""
                else:
                    try:
                        mmi_value = float(mmi)
                        intensity = str(int(mmi_value)) if mmi_value >= 0 else ""
                    except (ValueError, TypeError):
                        intensity = str(mmi) if mmi else ""

                # 获取事件ID
                public_id = properties.get("publicID", "")
                if not public_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    public_id = f"geonet_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"

                # 构建事件字典
                event = {
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
                result.append(event)
            except Exception:
                continue
        return result

# ============================================================================
# BMKG数据源
# ============================================================================
