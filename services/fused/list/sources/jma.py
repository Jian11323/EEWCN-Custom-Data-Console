from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta

import pytz
import requests

from services.fused.common.ws_client import WebSocketApp
from services.fused.list.config import API_URLS, Config, P2PQUAKE_WS_URL
from services.fused.list.fusion import FusionHandler
from services.fused.list.sources.processor_maps import DataSourceProcessor
from services.fused.list.state import cache_state, error_stats, logger
from services.fused.list.translation import TranslationService
from services.fused.list.utils import Utils

class JMASource:
    """日本气象厅数据源"""
    
    @staticmethod
    def process():
        """处理JMA数据源（兼容旧接口，当前仅用于启动时初始化）"""
        source_name = "JMA"
        if Utils.check_circuit_breaker(source_name):
            return

        try:
            data = JMASource.fetch()
            if data:
                parsed_data = JMASource.parse(data if isinstance(data, list) else [data])
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                Utils.reset_circuit_breaker(source_name)
        except Exception as e:
            logger.error(f"处理 {source_name} 数据时发生错误: {e}")
            Utils.handle_fetch_error(source_name, e)

    @staticmethod
    def prefetch_history(context="bootstrap"):
        """从 P2PQuake HTTP history API 拉取 code=551 情报并写入融合列表（不依赖缓存变更检测）"""
        from services.common.source_switches import is_list_enabled
        if not is_list_enabled("JMA"):
            if context == "bootstrap":
                logger.info("JMA(P2PQuake): 开关已关闭，跳过 HTTP 历史拉取")
            return 0
        source_name = "JMA"
        label_map = {
            "bootstrap": "启动",
            "reconnect": "重连前",
        }
        label = label_map.get(context, context)
        logger.info(f"JMA(P2PQuake): {label} HTTP 拉取历史 551 情报")
        try:
            if Utils.check_circuit_breaker(source_name):
                logger.warning(f"JMA(P2PQuake): 熔断器已打开，跳过{label}")
                return 0

            response = requests.get(API_URLS['JMA'], timeout=10)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list) or len(data) == 0:
                logger.info(f"JMA(P2PQuake): {label}未获取到历史数据")
                return 0

            parsed_data = JMASource.parse(data)
            if parsed_data:
                FusionHandler.add_events_to_fused_list(parsed_data)

            if len(data) == 1:
                data_id = data[0].get("id", "")
            else:
                data_id = f"{data[0].get('id', '')}_{data[-1].get('id', '')}"
            latest_item = data[0]
            cache_state["JMA"]["id"] = data_id
            cache_state["JMA"]["count"] = len(data)
            cache_state["JMA"]["latest_id"] = latest_item.get("id", "")
            cache_state["JMA"]["latest_time"] = str(latest_item.get("earthquake", {}).get("time", ""))
            cache_state["JMA"]["last_success"] = datetime.now()
            Utils.reset_circuit_breaker(source_name)
            count = len(parsed_data or [])
            logger.info(f"JMA(P2PQuake): {label} HTTP 拉取完成，加载 {count} 条地震事件")
            return count
        except Exception as e:
            logger.error(f"JMA(P2PQuake): {label} HTTP 拉取失败: {e}")
            Utils.handle_fetch_error(source_name, e)
            return 0

    @staticmethod
    def fetch():
        """获取JMA全部数据并全部解析"""
        try:
            response = requests.get(API_URLS['JMA'], timeout=10)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list) or len(data) == 0:
                return None

            data_id = ""
            if len(data) == 1:
                data_id = data[0].get("id", "")
            else:
                first_id = data[0].get("id", "")
                last_id = data[-1].get("id", "")
                data_id = f"{first_id}_{last_id}"

            event_count = len(data)

            latest_event_id = ""
            latest_event_time = ""
            if len(data) > 0:
                latest_item = data[0]
                latest_event_id = latest_item.get("id", "")
                eq_data = latest_item.get("earthquake", {})
                latest_event_time = eq_data.get("time", "")

            cached_id = cache_state["JMA"]["id"]
            cached_count = cache_state["JMA"]["count"]
            cached_latest_id = cache_state["JMA"]["latest_id"]
            cached_latest_time = cache_state["JMA"]["latest_time"]

            id_changed = (data_id != cached_id)
            count_changed = (event_count != cached_count)
            latest_id_changed = (latest_event_id != cached_latest_id)
            latest_time_changed = (str(latest_event_time) != cached_latest_time)

            if not (id_changed or count_changed or latest_id_changed or latest_time_changed):
                return None

            cache_state["JMA"]["id"] = data_id
            cache_state["JMA"]["count"] = event_count
            cache_state["JMA"]["latest_id"] = latest_event_id
            cache_state["JMA"]["latest_time"] = str(latest_event_time)
            cache_state["JMA"]["last_success"] = datetime.now()

            # 返回全部数据供解析
            return data
        except Exception:
            return None
    
    @staticmethod
    def parse(data):
        """解析JMA数据"""
        result = []
        for item in data:
            try:
                if item.get("code") != 551:
                    continue
                eq = item["earthquake"]
                hypo = eq["hypocenter"]
                try:
                    mag = float(hypo.get("magnitude", 0) or 0)
                except (TypeError, ValueError):
                    mag = 0.0

                issue_type = item.get("issue", {}).get("type", "")
                if issue_type == "Destination":
                    continue

                event_time = Utils.parse_time(eq["time"])
                if not event_time:
                    continue
                if event_time.tzinfo is None:
                    event_time_utc8 = pytz.timezone('Asia/Tokyo').localize(event_time).astimezone(pytz.timezone('Asia/Shanghai'))
                else:
                    event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

                # JMA 直接返回原始地名，不翻译
                location = hypo.get("name", "未知地区")

                max_scale = eq.get("maxScale", -1)
                intensity_str = ""
                if max_scale > 0:
                    intensity_str = Utils.get_intensity_string(max_scale)

                event_id = item.get("id", "")
                if not event_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    lat = hypo.get("latitude", 0)
                    lon = hypo.get("longitude", 0)
                    event_id = f"jma_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"

                event = {
                    "id": event_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(hypo.get("latitude", 0)),
                    "EPI_LON": str(hypo.get("longitude", 0)),
                    "EPI_DEPTH": round(hypo.get("depth", 0)),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M", "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (JMA)",
                    "epicenter_tts": location,
                    "INTENSITY": intensity_str,
                    "SOURCE": SOURCE_NAMES.get("JMA", "JMA"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("JMA", "JMA"),
                    "EVENT_ID": event_id,
                    "infoTypeName": "地震报告"
                }
                result.append(event)
            except Exception:
                continue
        return result

# ============================================================================
# GEONET数据源
# ============================================================================
