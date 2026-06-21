from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import datetime

from services.common.source_filters import get_filter_registry, resolve_list_source_id
from services.fused.list.config import Config, LIST_UNFILTERED_IDS, NO_THRESHOLD_SOURCES, SOURCE_NAMES
from services.fused.list.state import (
    event_dict_by_key,
    fused_data_lock,
    fused_events,
    logger,
)
from services.fused.list.utils import Utils

class FusionHandler:
    """数据融合处理器：负责地震事件的去重、更新和过滤"""

    @staticmethod
    def is_cenc_event_match(event1, event2, time_tolerance_seconds=60, lat_lon_tolerance=2.0):
        """检查两个CENC事件是否匹配（用于正式测定替换自动测定）

        根据提供的示例数据调整匹配参数：
        - 自动测定和正式测定的EVENT_ID通常不同
        - 时间差异可能较大（示例中相差17秒）
        - 位置可能有一定偏差（示例中经纬度都有差异）
        - 优先匹配时间和位置相似性
        """
        try:
            # 首先检查EVENT_ID是否相同（虽然通常不同，但如果相同则直接匹配）
            event_id1 = event1.get("EVENT_ID", "")
            event_id2 = event2.get("EVENT_ID", "")
            if event_id1 and event_id2 and event_id1 == event_id2:
                return True

            # 检查时间差异
            time1 = Utils.parse_time(event1.get("O_TIME", ""))
            time2 = Utils.parse_time(event2.get("O_TIME", ""))
            if not time1 or not time2:
                return False

            time_diff = abs((time1 - time2).total_seconds())
            if time_diff > time_tolerance_seconds:
                return False

            # 检查坐标差异
            try:
                lat1 = float(event1.get("EPI_LAT", 0))
                lon1 = float(event1.get("EPI_LON", 0))
                lat2 = float(event2.get("EPI_LAT", 0))
                lon2 = float(event2.get("EPI_LON", 0))

                lat_diff = abs(lat1 - lat2)
                lon_diff = abs(lon1 - lon2)

                if lat_diff > lat_lon_tolerance or lon_diff > lat_lon_tolerance:
                    return False
            except (ValueError, TypeError):
                return False

            # 检查震级是否相近（同一地震事件震级不应差异太大）
            try:
                mag1 = float(event1.get("M", 0))
                mag2 = float(event2.get("M", 0))
                mag_diff = abs(mag1 - mag2)
                if mag_diff > 1.0:  # 震级差异超过1.0级则不匹配
                    return False
            except (ValueError, TypeError):
                pass

            # 检查位置描述的相似性
            location1 = event1.get("epicenter_tts", "").strip()
            location2 = event2.get("epicenter_tts", "").strip()

            if not location1 and not location2:
                return True

            if not location1 or not location2:
                return True

            if location1 == location2:
                return True

            # 检查位置描述是否有重叠或包含关系
            if location1 in location2 or location2 in location1:
                return True

            return False
        except Exception:
            return False

    @staticmethod
    def _find_event_by_location_time(fused_events_list, source_name, event_time, lat, lon):
        """根据位置和时间查找事件"""
        if not event_time:
            return None
        time_key = event_time.replace(second=0, microsecond=0)
        try:
            lat = round(float(lat), 1)
            lon = round(float(lon), 1)
            location_key = (time_key, lat, lon)
            for e in fused_events_list:
                if e.get("SOURCE") == source_name:
                    e_time = Utils.parse_time(e.get("O_TIME", ""))
                    if e_time:
                        e_time_key = e_time.replace(second=0, microsecond=0)
                        try:
                            e_lat = round(float(e.get("EPI_LAT", 0)), 1)
                            e_lon = round(float(e.get("EPI_LON", 0)), 1)
                            if (e_time_key, e_lat, e_lon) == location_key:
                                return e
                        except (ValueError, TypeError):
                            continue
        except (ValueError, TypeError):
            pass
        return None

    @staticmethod
    def _find_cenc_event_by_id_or_match(fused_events_list, event, source_name, is_auto_filter=None):
        """根据ID或匹配查找CENC事件"""
        event_id = event.get("EVENT_ID", "")
        for e in fused_events_list:
            if e.get("SOURCE") != source_name:
                continue
            if is_auto_filter is not None and e.get("IS_AUTO", False) != is_auto_filter:
                continue
            e_event_id = e.get("EVENT_ID", "")
            if event_id and e_event_id and event_id == e_event_id:
                return e
            # 使用新的匹配逻辑，参数与正式测定替换自动测定的逻辑保持一致
            if FusionHandler.is_cenc_event_match(event, e, time_tolerance_seconds=120, lat_lon_tolerance=3.0):
                return e
        return None

    @staticmethod
    def _remove_and_update_dict(fused_events_list, event_dict, event_to_remove, new_event, key, log_msg=None):
        """移除旧事件并更新字典"""
        try:
            idx = fused_events_list.index(event_to_remove)
            fused_events_list[idx] = new_event
            old_key = Utils.get_event_key(event_to_remove)
            if old_key and old_key in event_dict:
                del event_dict[old_key]
            event_dict[key] = new_event
            if log_msg:
                logger.info(log_msg)
            return True
        except (ValueError, IndexError):
            return False

    @staticmethod
    def _update_fused_list(new_events, fused_events_list, event_dict, lock, log_additions=True):
        """更新融合事件列表"""
        if not new_events:
            return
        with lock:
            updated = False
            for event in new_events:
                key = Utils.get_event_key(event)
                if not key:
                    continue

                existing_event = event_dict.get(key)
                event_source = event.get("SOURCE")

                if not existing_event:
                    for source_key in ["JMA", "GUANGXI", "SHANXI"]:
                        source_name = SOURCE_NAMES.get(source_key)
                        if event_source == source_name:
                            event_time = Utils.parse_time(event.get("O_TIME", ""))
                            if event_time:
                                existing_event = FusionHandler._find_event_by_location_time(
                                    fused_events_list, source_name, event_time,
                                    event.get("EPI_LAT", 0), event.get("EPI_LON", 0)
                                )
                            break

                cenc_source_name = SOURCE_NAMES.get("CENC", "CENC")
                if not existing_event and event_source == cenc_source_name:
                    existing_event = FusionHandler._find_cenc_event_by_id_or_match(fused_events_list, event, cenc_source_name)

                if event_source == cenc_source_name:
                    is_auto = event.get("IS_AUTO", False)
                    event_id = event.get("EVENT_ID", "")

                    if not is_auto:
                        # 正式测定：查找并替换对应的自动测定
                        auto_events_to_remove = []
                        for e in list(fused_events_list):
                            if e.get("SOURCE") == cenc_source_name and e.get("IS_AUTO", False):
                                # 使用改进的匹配逻辑：基于时间、位置、震级相似性
                                if FusionHandler.is_cenc_event_match(event, e, time_tolerance_seconds=120, lat_lon_tolerance=3.0):
                                    auto_events_to_remove.append(e)

                        if existing_event and existing_event in auto_events_to_remove:
                            existing_event = None

                        for auto_event in auto_events_to_remove:
                            try:
                                fused_events_list.remove(auto_event)
                                old_key = Utils.get_event_key(auto_event)
                                if old_key and old_key in event_dict:
                                    del event_dict[old_key]
                                updated = True
                                if log_additions:
                                    # 计算匹配的置信度信息
                                    time1 = Utils.parse_time(event.get("O_TIME", ""))
                                    time2 = Utils.parse_time(auto_event.get("O_TIME", ""))
                                    time_diff = abs((time1 - time2).total_seconds()) if time1 and time2 else 0

                                    try:
                                        lat1, lon1 = float(event.get("EPI_LAT", 0)), float(event.get("EPI_LON", 0))
                                        lat2, lon2 = float(auto_event.get("EPI_LAT", 0)), float(auto_event.get("EPI_LON", 0))
                                        distance = ((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2) ** 0.5
                                    except (ValueError, TypeError):
                                        distance = 0

                                    logger.info(f"CENC: 正式测定替换自动测定 [时间差:{time_diff:.0f}秒,距离:{distance:.2f}°] - 自动测定:{auto_event.get('O_TIME')}, {auto_event.get('epicenter_tts')}, M{auto_event.get('M')}, ID:{auto_event.get('EVENT_ID')}; 正式测定:{event.get('O_TIME')}, {event.get('epicenter_tts')}, M{event.get('M')}, ID:{event.get('EVENT_ID')}")
                            except (ValueError, IndexError):
                                pass

                        existing_official = FusionHandler._find_cenc_event_by_id_or_match(fused_events_list, event, cenc_source_name, False)
                        if existing_official:
                            match_method = "EVENT_ID" if (event_id and existing_official.get("EVENT_ID") == event_id) else "时间和坐标"
                            if FusionHandler._remove_and_update_dict(fused_events_list, event_dict, existing_official, event, key,
                                log_additions and f"CENC: 正式测定替换已存在的正式测定 [{match_method}] - 旧时间:{existing_official.get('O_TIME')}, 新时间:{event.get('O_TIME')}, 地名:{event.get('epicenter_tts')}, 震级:{event.get('M')}, EVENT_ID:{event.get('EVENT_ID')}"):
                                updated = True
                        else:
                            fused_events_list.appendleft(event)
                            event_dict[key] = event
                            updated = True
                            if log_additions:
                                logger.info(f"CENC: 正式测定添加 - 时间:{event.get('O_TIME')}, 地名:{event.get('epicenter_tts')}, 震级:{event.get('M')}, EVENT_ID:{event.get('EVENT_ID')}")
                    else:
                        # 自动测定：检查是否已存在对应的正式测定
                        existing_official = None
                        for e in fused_events_list:
                            if e.get("SOURCE") == cenc_source_name and not e.get("IS_AUTO", False):
                                # 使用相同的匹配逻辑检查是否对应同一地震事件
                                if FusionHandler.is_cenc_event_match(event, e, time_tolerance_seconds=120, lat_lon_tolerance=3.0):
                                    existing_official = e
                                    break

                        if existing_official:
                            if log_additions:
                                # 计算匹配的置信度信息
                                time1 = Utils.parse_time(event.get("O_TIME", ""))
                                time2 = Utils.parse_time(existing_official.get("O_TIME", ""))
                                time_diff = abs((time1 - time2).total_seconds()) if time1 and time2 else 0

                                try:
                                    lat1, lon1 = float(event.get("EPI_LAT", 0)), float(event.get("EPI_LON", 0))
                                    lat2, lon2 = float(existing_official.get("EPI_LAT", 0)), float(existing_official.get("EPI_LON", 0))
                                    distance = ((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2) ** 0.5
                                except (ValueError, TypeError):
                                    distance = 0

                                logger.info(f"CENC: 自动测定已存在正式测定，不添加 [时间差:{time_diff:.0f}秒,距离:{distance:.2f}°] - 自动测定:{event.get('O_TIME')}, {event.get('epicenter_tts')}, M{event.get('M')}, ID:{event.get('EVENT_ID')}; 正式测定:{existing_official.get('O_TIME')}, {existing_official.get('epicenter_tts')}, M{existing_official.get('M')}, ID:{existing_official.get('EVENT_ID')}")
                        elif existing_event:
                            existing_time = Utils.parse_time(existing_event.get("O_TIME", ""))
                            current_time = Utils.parse_time(event.get("O_TIME", ""))
                            if existing_time and current_time and current_time > existing_time:
                                if FusionHandler._remove_and_update_dict(fused_events_list, event_dict, existing_event, event, key,
                                    log_additions and f"CENC: 自动测定替换已存在的自动测定（新时间优先） - 旧自动测定时间:{existing_event.get('O_TIME')}, 地名:{existing_event.get('epicenter_tts')}, 震级:{existing_event.get('M')}, EVENT_ID:{existing_event.get('EVENT_ID')}; 新自动测定时间:{event.get('O_TIME')}, 地名:{event.get('epicenter_tts')}, 震级:{event.get('M')}, EVENT_ID:{event.get('EVENT_ID')}"):
                                    updated = True
                        else:
                            fused_events_list.appendleft(event)
                            event_dict[key] = event
                            updated = True
                            if log_additions:
                                logger.info(f"CENC: 自动测定添加 - 时间:{event.get('O_TIME')}, 地名:{event.get('epicenter_tts')}, 震级:{event.get('M')}, EVENT_ID:{event.get('EVENT_ID')}")
                elif existing_event:
                    existing_is_auto = existing_event.get("IS_AUTO", False)
                    event_is_auto = event.get("IS_AUTO", False)

                    if existing_is_auto and not event_is_auto:
                        if FusionHandler._remove_and_update_dict(fused_events_list, event_dict, existing_event, event, key):
                            updated = True
                    elif not existing_is_auto and event_is_auto:
                        pass
                    else:
                        existing_time = Utils.parse_time(existing_event.get("O_TIME", ""))
                        current_time = Utils.parse_time(event.get("O_TIME", ""))
                        if existing_time and current_time and current_time > existing_time:
                            if FusionHandler._remove_and_update_dict(fused_events_list, event_dict, existing_event, event, key):
                                updated = True
                else:
                    fused_events_list.appendleft(event)
                    event_dict[key] = event
                    updated = True

            if updated:
                sorted_list = sorted(list(fused_events_list), key=lambda x: Utils.parse_time(x.get("O_TIME")) or datetime.min, reverse=True)
                fused_events_list.clear()
                fused_events_list.extend(sorted_list)
                event_dict.clear()
                for e in fused_events_list:
                    k = Utils.get_event_key(e)
                    if k:
                        event_dict[k] = e

    @staticmethod
    def _check_location_contains_taiwan(event):
        """检查事件地名是否包含台湾"""
        try:
            # 对于FSSN数据，优先检查 placeName_zh 是否包含台湾
            place_name_zh = event.get("placeName_zh", "")
            if isinstance(place_name_zh, str) and "台湾" in place_name_zh:
                return True
            
            # 检查其他地名字段是否包含台湾
            location_c = event.get("LOCATION_C", "")
            epicenter_tts = event.get("epicenter_tts", "")
            if isinstance(location_c, str) and "台湾" in location_c:
                return True
            if isinstance(epicenter_tts, str) and "台湾" in epicenter_tts:
                return True
            
            return False
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _check_fssn_location_in_china_or_japan(event):
        """检查FSSN事件是否来自国内或日本（参考台湾处理，不过滤）"""
        try:
            source = event.get("SOURCE", "")
            # 只对FSSN数据源进行判断
            if source != SOURCE_NAMES.get("FSSN", "FSSN"):
                return False
            
            # 优先检查 placeName_zh 是否包含中国或日本相关关键词
            place_name_zh = event.get("placeName_zh", "")
            if isinstance(place_name_zh, str):
                china_keywords = ["中国", "北京", "上海", "广东", "四川", "云南", "新疆", "西藏", "内蒙古", "台湾", "香港", "澳门"]
                japan_keywords = ["日本", "东京", "大阪", "北海道", "九州", "本州", "四国"]
                for keyword in china_keywords + japan_keywords:
                    if keyword in place_name_zh:
                        return True
            
            # 检查其他地名字段
            location_c = event.get("LOCATION_C", "")
            epicenter_tts = event.get("epicenter_tts", "")
            for location_field in [location_c, epicenter_tts]:
                if isinstance(location_field, str):
                    china_keywords = ["中国", "北京", "上海", "广东", "四川", "云南", "新疆", "西藏", "内蒙古", "台湾", "香港", "澳门"]
                    japan_keywords = ["日本", "东京", "大阪", "北海道", "九州", "本州", "四国"]
                    for keyword in china_keywords + japan_keywords:
                        if keyword in location_field:
                            return True
            
            # 通过经纬度范围判断
            try:
                lat = float(event.get("EPI_LAT", 0))
                lon = float(event.get("EPI_LON", 0))
                
                # 中国范围：纬度 18°N - 54°N，经度 73°E - 135°E
                if 18 <= lat <= 54 and 73 <= lon <= 135:
                    return True
                
                # 日本范围：纬度 24°N - 46°N，经度 123°E - 146°E
                if 24 <= lat <= 46 and 123 <= lon <= 146:
                    return True
            except (ValueError, TypeError):
                pass
            
            return False
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _event_skips_list_filter(event) -> bool:
        """CWA / JMA：不做震级阈值与地区过滤。"""
        sid = resolve_list_source_id(event.get("SOURCE", ""))
        return sid in LIST_UNFILTERED_IDS

    @staticmethod
    def _should_include_list_event(event) -> bool:
        """是否写入融合列表（震级阈值 + 可选地区过滤）。"""
        if FusionHandler._event_skips_list_filter(event):
            return True
        include, reason = get_filter_registry().should_include_list_event(event)
        if not include and reason:
            logger.debug(
                "List 过滤丢弃 [%s]: %s, M=%s",
                event.get("SOURCE"),
                reason,
                event.get("M"),
            )
        return include

    @staticmethod
    def _list_threshold_mag(event) -> float:
        sid = resolve_list_source_id(event.get("SOURCE", ""))
        if sid in LIST_FOREIGN_IDS:
            return get_filter_registry().get_list_threshold(sid)
        return Config.THRESHOLD_MAG

    @staticmethod
    def _list_push_log_suffix(event) -> str:
        """FanStudio/内部源推送日志后缀。"""
        if FusionHandler._event_skips_list_filter(event):
            return "已推送至8150"
        if not FusionHandler._check_event_has_threshold(event):
            return "无阈值，已推送至8150"
        include, reason = get_filter_registry().should_include_list_event(event)
        if include:
            thr = FusionHandler._list_threshold_mag(event)
            return f"高于阈值M{thr}，已推送至8150"
        if reason == "region_filter":
            return "非中台日地区，已过滤"
        if reason.startswith("threshold"):
            thr = FusionHandler._list_threshold_mag(event)
            return f"低于阈值M{thr}，已过滤"
        return "已过滤"

    @staticmethod
    def _filter_by_threshold(events):
        """根据阈值过滤事件（CWA/JMA 始终保留；fssnlist 等国外源仍过滤）。"""
        return [e for e in events if FusionHandler._should_include_list_event(e)]

    @staticmethod
    def _filter_by_source(events, allowed_sources):
        """根据数据源过滤事件"""
        return [event for event in events if event.get("SOURCE") in allowed_sources]

    @staticmethod
    def _check_event_passes_threshold(event):
        """检查事件是否通过阈值/地区过滤（与写入融合列表规则一致）。"""
        return FusionHandler._should_include_list_event(event)

    @staticmethod
    def _check_event_has_threshold(event):
        """检查事件是否有阈值限制"""
        try:
            if FusionHandler._check_location_contains_taiwan(event):
                return False
            if FusionHandler._check_fssn_location_in_china_or_japan(event):
                return False
            source = event.get("SOURCE")
            return source not in NO_THRESHOLD_SOURCES
        except (ValueError, TypeError):
            return True

    @staticmethod
    def add_events_to_fused_list(new_events, bulk_quiet_cenc_logs=False):
        """bulk_quiet_cenc_logs 为 True 时抑制 CENC 逐条 INFO（批量列表/缓存回放）。"""
        if not new_events:
            return

        filtered = FusionHandler._filter_by_threshold(new_events)
        if not filtered:
            return

        FusionHandler._update_fused_list(
            filtered,
            fused_events,
            event_dict_by_key,
            fused_data_lock,
            log_additions=not bulk_quiet_cenc_logs,
        )

# ============================================================================
# API服务模块
# ============================================================================
