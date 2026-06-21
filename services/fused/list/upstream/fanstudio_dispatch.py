"""Fan Studio 消息分发（从 ws_handler 拆分）。"""

from __future__ import annotations

from collections import deque
from typing import Any, Type

from services.common.source_switches import is_list_enabled
from services.fused.list.cache import CacheManager
from services.fused.list.config import (
    Config,
    EXCLUDED_SOURCES,
    FANSTUDIO_LIST_CMD_STATE,
    FANSTUDIO_NO_RAW_CACHE_SOURCES,
    SOURCE_NAMES,
)
from services.fused.list.fusion import FusionHandler
from services.fused.list.sources.processor_maps import DataSourceProcessor, FAN_STUDIO_PARSERS
from services.fused.list.state import (
    _fanstudio_list_cmd_lock,
    fanstudio_cache_lock,
    fanstudio_raw_cache,
    logger,
)


class FanStudioDispatch:
    """Fan Studio /all 消息路由与入库。"""

    _SKIP_INSTITUTION_TYPES = frozenset({
        "heartbeat", "pong", "cenclist_response", "cwalist_response", "fssnlist_response",
    })

    @staticmethod
    def _cache_source_data(source: str, source_data: dict) -> None:
        if source in FANSTUDIO_NO_RAW_CACHE_SOURCES:
            return
        with fanstudio_cache_lock:
            if source not in fanstudio_raw_cache:
                fanstudio_raw_cache[source] = deque(maxlen=Config.MAX_CACHE_PER_SOURCE)
            cache_deque = fanstudio_raw_cache[source]
            if source_data not in cache_deque:
                cache_deque.append(source_data)
        CacheManager.save_fanstudio_cache()

    @staticmethod
    def _handle_source_update(source: str, source_data: dict, handler_cls: Type[Any]) -> None:
        if not source or source in EXCLUDED_SOURCES or source not in FAN_STUDIO_PARSERS:
            return
        if not is_list_enabled(source):
            return
        if not source_data:
            return

        FanStudioDispatch._cache_source_data(source, source_data)
        parser = FAN_STUDIO_PARSERS[source]
        event = parser(source_data)
        if not event:
            return

        if source == "cenc":
            FusionHandler.add_events_to_fused_list([event])
            logger.info(
                f"FanStudio更新数据 [{source}]: 解析到1个地震事件；"
                f"{FusionHandler._list_push_log_suffix(event)}"
            )
            return

        if source == "cwa":
            FusionHandler.add_events_to_fused_list([event])
            logger.info(f"FanStudio更新数据 [{source}]: 解析到1个地震事件（台湾气象署）；已推送至8150")
            return

        if not handler_cls.check_event_time(event, source):
            logger.debug(f"FanStudio更新数据 [{source}]: 事件时间不新，跳过推送")
            return

        FusionHandler.add_events_to_fused_list([event])
        logger.info(
            f"FanStudio更新数据 [{source}]: 解析到1个地震事件；"
            f"{FusionHandler._list_push_log_suffix(event)}"
        )

    @staticmethod
    def dispatch(data: dict, handler_cls: Type[Any]) -> None:
        try:
            data = handler_cls._expand_fan_v21(data)
            msg_type = data.get("type")

            if msg_type in ("initial_all", "start_all"):
                with fanstudio_cache_lock:
                    for source_name in FAN_STUDIO_PARSERS:
                        if source_name in EXCLUDED_SOURCES or source_name in FANSTUDIO_NO_RAW_CACHE_SOURCES:
                            continue
                        source_entry = handler_cls._get_source_entry(data, source_name)
                        if not source_entry:
                            continue
                        source_data = source_entry.get("Data", {})
                        if source_data:
                            FanStudioDispatch._cache_source_data(source_name, source_data)

                parsed_events = handler_cls.parse_fan_studio_data(data)
                if not parsed_events:
                    return

                filtered_events = []
                for event in parsed_events:
                    source_name = None
                    source_chinese = event.get("SOURCE", "")
                    for parser_source in FAN_STUDIO_PARSERS:
                        expected = SOURCE_NAMES.get(parser_source.upper(), "")
                        if source_chinese == expected:
                            source_name = parser_source
                            break
                    if source_name and handler_cls.check_event_time(event, source_name):
                        filtered_events.append(event)

                if not filtered_events:
                    logger.debug("FanStudio初始数据: 所有事件都已处理过或时间不新，跳过推送")
                    return

                total_parsed = len(filtered_events)
                FusionHandler.add_events_to_fused_list(filtered_events)
                pushed = [e for e in filtered_events if FusionHandler._should_include_list_event(e)]
                no_threshold_count = sum(1 for e in pushed if not FusionHandler._check_event_has_threshold(e))
                above_count = sum(1 for e in pushed if FusionHandler._check_event_has_threshold(e))
                dropped_count = total_parsed - len(pushed)

                log_parts = []
                if no_threshold_count > 0:
                    log_parts.append(f"{no_threshold_count}个无阈值已推送至8150")
                if above_count > 0:
                    log_parts.append(f"{above_count}个已推送至8150")
                if dropped_count > 0:
                    log_parts.append(f"{dropped_count}个已过滤")

                if log_parts:
                    logger.info(f"FanStudio初始数据: 解析到 {total_parsed} 个地震事件；{', '.join(log_parts)}")
                else:
                    logger.info(f"FanStudio初始数据: 解析到 {total_parsed} 个地震事件")
                return

            if msg_type == "update":
                source = handler_cls._normalize_institution_key(data.get("institution") or data.get("source"))
                FanStudioDispatch._handle_source_update(source, data.get("Data", {}), handler_cls)
                return

            if isinstance(msg_type, str) and msg_type not in FanStudioDispatch._SKIP_INSTITUTION_TYPES:
                source = handler_cls._normalize_institution_key(msg_type)
                FanStudioDispatch._handle_source_update(source, data.get("Data", {}), handler_cls)
                return

            if msg_type == "cenclist_response":
                FanStudioDispatch._handle_list_response(
                    data, handler_cls, "cenc", DataSourceProcessor.parse_cenc_data,
                    next_cmd="cwalist", phase_from=0, phase_to=1,
                    log_label="CENC列表", extra_log="CENC数据源无阈值，已推送至8150",
                )
                return

            if msg_type == "cwalist_response":
                FanStudioDispatch._handle_list_response(
                    data, handler_cls, "cwa", DataSourceProcessor.parse_cwa_fanstudio_data,
                    next_cmd="fssnlist", phase_from=1, phase_to=2,
                    log_label="CWA列表",
                )
                return

            if msg_type == "fssnlist_response":
                fssn_data_list = data.get("Data", [])
                if not isinstance(fssn_data_list, list):
                    logger.warning("FanStudio FSSN列表响应格式错误: Data不是列表类型")
                    return
                parsed_events = FanStudioDispatch._parse_list_items(fssn_data_list, DataSourceProcessor.parse_fssn_data)
                if parsed_events:
                    FusionHandler.add_events_to_fused_list(parsed_events, bulk_quiet_cenc_logs=True)
                    pushed = sum(1 for e in parsed_events if FusionHandler._should_include_list_event(e))
                    logger.info(
                        f"FanStudio FSSN列表: 解析到 {len(parsed_events)} 个地震事件；"
                        f"{pushed} 条已推送至8150（其余已过滤）"
                    )
                with _fanstudio_list_cmd_lock:
                    if FANSTUDIO_LIST_CMD_STATE["phase"] == 2:
                        FANSTUDIO_LIST_CMD_STATE["phase"] = 3
        except Exception:
            logger.exception("处理FanStudio WebSocket消息失败")

    @staticmethod
    def _parse_list_items(items: list, parser) -> list:
        parsed_events = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                event = parser(item)
                if event:
                    parsed_events.append(event)
            except Exception as exc:
                logger.error("解析FanStudio列表项失败: %s", exc)
        return parsed_events

    @staticmethod
    def _handle_list_response(
        data: dict,
        handler_cls: Type[Any],
        source_key: str,
        parser,
        *,
        next_cmd: str,
        phase_from: int,
        phase_to: int,
        log_label: str,
        extra_log: str = "已推送至8150",
    ) -> None:
        items = data.get("Data", [])
        if not isinstance(items, list):
            logger.warning(f"FanStudio {log_label}响应格式错误: Data不是列表类型")
            return
        parsed_events = FanStudioDispatch._parse_list_items(items, parser)
        if parsed_events:
            FusionHandler.add_events_to_fused_list(parsed_events, bulk_quiet_cenc_logs=True)
            logger.info(f"FanStudio {log_label}: 解析到 {len(parsed_events)} 个地震事件；{extra_log}")

        do_next = False
        with _fanstudio_list_cmd_lock:
            if FANSTUDIO_LIST_CMD_STATE["phase"] == phase_from:
                FANSTUDIO_LIST_CMD_STATE["phase"] = phase_to
                do_next = True
        if do_next:
            gap = handler_cls._fanstudio_list_cmd_gap(len(parsed_events))
            logger.info(f"FanStudio 列表：{source_key.upper()} {len(parsed_events)} 条已入库，{gap:g}s 后发送 {next_cmd}")
            handler_cls._fanstudio_schedule_list_cmd_after_delay(next_cmd, gap)
