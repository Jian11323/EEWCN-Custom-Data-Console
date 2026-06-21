from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from services.fused.eew.sources.base import DataSource
from services.fused.eew.utils import Utils

class CustomSource(DataSource):
    """用户配置的 HTTP/HTTPS 或 WS/WSS 自定义预警源。"""

    def __init__(self, *args, **kwargs):
        super().__init__("CUSTOM", *args, **kwargs)
        self.raw_cache: Optional[Dict[str, Any]] = None
        self.raw_cache_time: float = 0.0
        self.event_distributor = None

    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if self.raw_cache and (time.time() - self.raw_cache_time) <= self.config.CACHE_MAX_AGE:
                return dict(self.raw_cache)
            cached = self.cache_mgr.load_source_cache(self.source_key)
            if cached and isinstance(cached.get("data"), dict):
                data = cached["data"]
                if data.get("eventId"):
                    return data
        return None

    def on_raw_payload(self, payload: Dict[str, Any]) -> None:
        from services.common.custom_adapter import parse_custom_payload

        if not payload or not isinstance(payload, dict):
            return
        parsed = parse_custom_payload(payload)
        if not parsed:
            return
        try:
            event_data = self._build_event_data(parsed)
            if not event_data:
                return
            receive_time = time.time()
            with self.lock:
                self.raw_cache = event_data
                self.raw_cache_time = receive_time
                self.connected = True
            self.cache_mgr.update_memory_cache(self.source_key, event_data)
            if self.event_distributor:
                self.event_distributor.distribute(
                    self.source_key, event_data, self.get_target_ports()
                )
            self.cache_mgr.save_source_cache(self.source_key, event_data)
        except Exception as e:
            self.logger.error(f"自定义数据源处理失败: {e}")

    def _build_event_data(self, parsed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        place_name = parsed.get("place_name") or "未知地点"
        org = parsed.get("organization") or "自定义"
        magnitude = Utils.format_magnitude(parsed.get("magnitude", 0))
        depth = Utils.format_depth(parsed.get("depth", 0))
        shock_time = parsed.get("shock_time") or ""
        start_at_ms = Utils.parse_time_utc_offset(str(shock_time).strip(), 8)
        if start_at_ms <= 0:
            start_at_ms = int(time.time() * 1000)
        updates = parsed.get("updates")
        if updates is None:
            updates = 1
        else:
            updates = int(updates)
        event_id = (parsed.get("event_id") or "").strip()
        if not event_id:
            event_id = f"CUSTOM_{start_at_ms}"
        epicenter = f"{place_name} ({org})"
        return {
            "eventId": event_id,
            "updates": updates,
            "report_number": updates,
            "latitude": Utils.safe_float(parsed.get("latitude", 0)),
            "longitude": Utils.safe_float(parsed.get("longitude", 0)),
            "depth": depth,
            "epicenter": epicenter,
            "epicenter_tts": Utils.format_epicenter_tts(place_name),
            "startAt": start_at_ms,
            "O_TIME": Utils.format_o_time(start_at_ms),
            "magnitude": magnitude,
            "source": org,
        }


# ============================================================================
# Early-est 数据源
# ============================================================================

class EarlyEstSource(DataSource):
    """Early-est 预警数据源（WebSocket 推送）"""

    def __init__(self, *args, **kwargs):
        super().__init__("EARLY_EST", *args, **kwargs)
        self.raw_cache = None
        self.raw_cache_time = 0
        self.event_distributor = None  # 将在主服务中设置

    def on_message(self, payload: Dict[str, Any]):
        """处理 Early-est WebSocket 消息"""
        if not payload or not isinstance(payload, dict):
            self.logger.warning("Early-est 接收到无效数据")
            return

        msg_type = payload.get("type", "")
        if msg_type not in ("initial", "update"):
            # 仅处理 initial / update 报文，其余忽略
            return

        data = payload.get("data")
        if not isinstance(data, dict):
            self.logger.warning("Early-est data 字段不是字典，跳过")
            return

        # 取消报直接记录日志并跳过（当前分发器未对通用 cancel 事件做特殊处理）
        if data.get("isCancel"):
            event_id = data.get("eventID", "unknown")
            self.logger.info(f"Early-est 取消报，跳过处理: {event_id}")
            return

        receive_time = time.time()

        # 更新原始缓存
        with self.lock:
            self.raw_cache = data
            self.raw_cache_time = receive_time
            self.connected = True

        # 立即转换并分发
        try:
            event = self.fetch()
            if event and self.event_distributor:
                target_ports = self.get_target_ports()
                self.event_distributor.distribute(self.source_key, event, target_ports)
                # 异步保存缓存
                self.cache_mgr.save_source_cache("EARLY_EST", event)
        except Exception as e:
            self.logger.error(f"Early-est 即时推送失败: {e}")

    def fetch(self) -> Optional[Dict[str, Any]]:
        """获取 Early-est 当前事件（用于定时拉取 / 缓存恢复）"""
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache("EARLY_EST")
                if cached and cached.get("data"):
                    data = cached["data"]
                else:
                    return None
            else:
                data = self.raw_cache

        try:
            # 发震时间：all_ws 1450 为 otime（与 shockTime 同为 UTC 串）；转为 UTC+8 毫秒时间戳
            shock_time_str = str(data.get("otime") or data.get("shockTime", "") or "").strip()
            if shock_time_str:
                try:
                    if shock_time_str.endswith("Z"):
                        shock_time_str = shock_time_str[:-1].strip()
                    naive_time = datetime.strptime(shock_time_str, "%Y/%m/%d %H:%M:%S")
                    utc_time = naive_time.replace(tzinfo=timezone.utc)
                    beijing_time = utc_time.astimezone(timezone(timedelta(hours=8)))
                    timestamp_ms = int(beijing_time.timestamp() * 1000)
                except Exception:
                    timestamp_ms = 0
            else:
                timestamp_ms = 0

            if timestamp_ms <= 0:
                eid = data.get("identifier") or data.get("eventID", "unknown")
                self.logger.warning(f"Early-est 数据时间无效，跳过: {eid}")
                return None

            magnitude = Utils.format_magnitude(data.get("mag", data.get("magnitude", 0)))
            depth = Utils.format_depth(data.get("depth", 0))
            # all_ws：报序号为 locSeq；旧字段 reportNum / updates 仍兼容
            updates = Utils.safe_int(
                data.get("locSeq", data.get("locseq", data.get("reportNum", data.get("updates", 1)))),
                1,
            )

            latitude = Utils.safe_float(data.get("lat", data.get("latitude", 0)))
            longitude = Utils.safe_float(data.get("lon", data.get("longitude", 0)))
            place_name = data.get("region") or data.get("placeName", "未知地点")

            # 使用统一翻译服务将地名翻译为中文
            translated_place = place_name
            try:
                if place_name:
                    translated_place = self.translator.translate(place_name, quick_mode=False, skip_cache=False)
            except Exception as e:
                self.logger.error(f"Early-est 地名翻译失败: {place_name}, {e}")
                translated_place = place_name

            epicenter = f"{translated_place} (Early-est)"
            epicenter_tts = Utils.format_epicenter_tts(epicenter)

            event_id = str(data.get("identifier") or data.get("eventID", "unknown"))

            event_data = {
                "eventId": event_id,
                "updates": updates,
                "report_number": updates,
                "latitude": latitude,
                "longitude": longitude,
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": epicenter_tts,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name,
            }

            return event_data
        except Exception as e:
            self.logger.error(f"处理 Early-est 数据失败: {e}")
            return None


# ============================================================================
# Fan Studio数据源基类
# ============================================================================

