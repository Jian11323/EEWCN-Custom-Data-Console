from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta

from services.fused.common.ws_client import WebSocketApp, ws_run_forever_kwargs
from services.fused.list.config import (
    Config,
    FANSTUDIO_LIST_CMD_GAP_MAX_SEC,
    FANSTUDIO_LIST_CMD_GAP_PER_100,
    FANSTUDIO_LIST_CMD_GAP_SEC,
    FANSTUDIO_LIST_CMD_STATE,
    FAN_STUDIO_SWITCH_CONFIG,
    FAN_STUDIO_WS_URL_BACKUP,
    FAN_STUDIO_WS_URL_PRIMARY,
    P2PQUAKE_WS_URL,
    EXCLUDED_SOURCES,
    FANSTUDIO_NO_RAW_CACHE_SOURCES,
)
from services.fused.list.fusion import FusionHandler
from services.fused.list.sources.jma import JMASource
from services.fused.list.sources.processor_maps import (
    DataSourceProcessor,
    FAN_STUDIO_PARSERS,
    INTERNAL_WS_PARSERS,
)
from services.fused.list import config as list_config
from services.fused.list.state import (
    _fanstudio_list_cmd_lock,
    fanstudio_cache_lock,
    fanstudio_raw_cache,
    logger,
)
from services.fused.list.utils import Utils

class WebSocketHandler:
    """WebSocket连接处理器：负责FanStudio WebSocket连接和消息处理"""

    @staticmethod
    def try_connect_primary_server():
        """测试主服务器连接"""
        test_result = {"connected": False, "lock": threading.Lock()}

        def on_open(ws):
            with test_result["lock"]:
                test_result["connected"] = True
            try:
                ws.close()
            except Exception:
                pass

        def on_error(ws, error):
            with test_result["lock"]:
                test_result["connected"] = False

        def on_close(ws, close_status_code, close_msg):
            pass

        try:
            test_ws = WebSocketApp(
                FAN_STUDIO_WS_URL_PRIMARY,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close
            )
            thread = threading.Thread(
                target=lambda: test_ws.run_forever(**ws_run_forever_kwargs()),
                daemon=True,
            )
            thread.start()
            thread.join(timeout=5)

            with test_result["lock"]:
                return test_result["connected"]
        except Exception as e:
            logger.debug(f"测试主服务器连接失败: {e}")
            return False

    @staticmethod
    def switch_to_backup_server():
        """切换到备用服务器"""
        if list_config._shared_fan_conn is not None:
            list_config._shared_fan_conn.switch_to_backup()
        with FAN_STUDIO_SWITCH_CONFIG["lock"]:
            if not FAN_STUDIO_SWITCH_CONFIG["is_using_backup"]:
                if FAN_STUDIO_SWITCH_CONFIG["ws_instance"]:
                    WebSocketHandler._fanstudio_cancel_list_cmd_timer()
                    try:
                        FAN_STUDIO_SWITCH_CONFIG["ws_instance"].close()
                        FAN_STUDIO_SWITCH_CONFIG["ws_instance"] = None
                    except Exception:
                        pass

                FAN_STUDIO_SWITCH_CONFIG["current_url"] = FAN_STUDIO_WS_URL_BACKUP
                FAN_STUDIO_SWITCH_CONFIG["is_using_backup"] = True
                FAN_STUDIO_SWITCH_CONFIG["switch_to_backup_time"] = datetime.now()
                FAN_STUDIO_SWITCH_CONFIG["primary_fail_count"] = 0
                logger.warning(f"FanStudio: 切换到备用服务器 {FAN_STUDIO_WS_URL_BACKUP}（主服务器连续失败{FAN_STUDIO_SWITCH_CONFIG['primary_fail_threshold']}次）")

    @staticmethod
    def switch_to_primary_server():
        """切换回主服务器"""
        if list_config._shared_fan_conn is not None:
            list_config._shared_fan_conn.switch_to_primary()
        with FAN_STUDIO_SWITCH_CONFIG["lock"]:
            if FAN_STUDIO_SWITCH_CONFIG["is_using_backup"]:
                if FAN_STUDIO_SWITCH_CONFIG["ws_instance"]:
                    WebSocketHandler._fanstudio_cancel_list_cmd_timer()
                    try:
                        FAN_STUDIO_SWITCH_CONFIG["ws_instance"].close()
                        FAN_STUDIO_SWITCH_CONFIG["ws_instance"] = None
                    except Exception:
                        pass

                FAN_STUDIO_SWITCH_CONFIG["current_url"] = FAN_STUDIO_WS_URL_PRIMARY
                FAN_STUDIO_SWITCH_CONFIG["is_using_backup"] = False
                FAN_STUDIO_SWITCH_CONFIG["primary_fail_count"] = 0
                FAN_STUDIO_SWITCH_CONFIG["switch_to_backup_time"] = None
                FAN_STUDIO_SWITCH_CONFIG["last_backup_check"] = None
                logger.info(f"FanStudio: 切换回主服务器 {FAN_STUDIO_WS_URL_PRIMARY}")

    @staticmethod
    def check_event_time(event, source_name):
        """检查事件时间是否为新事件。

        解析失败时保守返回 True（允许入库），避免静默丢弃可能的新事件。
        """
        try:
            event_time_str = event.get("O_TIME", "")
            if not event_time_str:
                return True

            event_time = Utils.parse_time(event_time_str)
            if not event_time:
                return True

            with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                last_time = FAN_STUDIO_SWITCH_CONFIG["last_event_times"].get(source_name)
                if last_time and event_time <= last_time:
                    return False

                FAN_STUDIO_SWITCH_CONFIG["last_event_times"][source_name] = event_time
                return True
        except Exception as exc:
            logger.warning("check_event_time 失败，保守视为新事件: source=%s err=%s", source_name, exc)
            return True

    @staticmethod
    def _normalize_institution_key(key: str) -> str:
        """将 institution 前缀键规范化为原始 source 名。"""
        if not isinstance(key, str):
            return key
        if key.startswith("institution:"):
            return key[len("institution:"):]
        if key.startswith("institution："):
            return key[len("institution："):]
        return key

    @staticmethod
    def _get_source_entry(data: dict, source_name: str):
        """同时兼容旧键名与 institution 前缀键名。"""
        if not isinstance(data, dict):
            return None
        return (
            data.get(source_name)
            or data.get(f"institution:{source_name}")
            or data.get(f"institution：{source_name}")
        )

    @staticmethod
    def _fanstudio_cancel_list_cmd_timer():
        """取消待发送的下一条列表命令定时器（重连或关闭时调用）。"""
        with _fanstudio_list_cmd_lock:
            t = FANSTUDIO_LIST_CMD_STATE["timer"]
            if t is not None:
                try:
                    t.cancel()
                except Exception:
                    pass
                FANSTUDIO_LIST_CMD_STATE["timer"] = None

    @staticmethod
    def _fanstudio_list_cmd_gap(parsed_count: int = 0) -> float:
        """根据上一条列表响应条数计算发送下一条前的等待秒数。"""
        extra = (max(0, parsed_count) / 100.0) * FANSTUDIO_LIST_CMD_GAP_PER_100
        return min(FANSTUDIO_LIST_CMD_GAP_MAX_SEC, FANSTUDIO_LIST_CMD_GAP_SEC + extra)

    @staticmethod
    def _fanstudio_send_list_cmd(cmd: str) -> bool:
        """发送列表拉取命令（List 独立 WS 或融合共享连接）。"""
        with FAN_STUDIO_SWITCH_CONFIG["lock"]:
            inst = FAN_STUDIO_SWITCH_CONFIG.get("ws_instance")
        if inst is not None:
            try:
                inst.send(cmd)
                return True
            except Exception as e:
                logger.error(f"发送 Fan Studio 列表命令 {cmd!r} 失败: {e}")
                return False
        if list_config._shared_fan_conn is not None and list_config._shared_fan_conn.send_text(cmd):
            return True
        logger.warning(f"无可用 Fan Studio WebSocket，无法发送列表命令 {cmd!r}")
        return False

    @staticmethod
    def _fanstudio_schedule_list_cmd_after_delay(cmd: str, delay_sec: float):
        """在 delay_sec 秒后发送一条 FanStudio 文本命令（不阻塞 WebSocket 线程）。"""
        def _send():
            try:
                if WebSocketHandler._fanstudio_send_list_cmd(cmd):
                    logger.info(
                        f"已发送 {cmd}（距上一条列表响应处理完成等待 {delay_sec:g}s）"
                    )
            finally:
                with _fanstudio_list_cmd_lock:
                    FANSTUDIO_LIST_CMD_STATE["timer"] = None

        with _fanstudio_list_cmd_lock:
            old = FANSTUDIO_LIST_CMD_STATE["timer"]
            if old is not None:
                try:
                    old.cancel()
                except Exception:
                    pass
            FANSTUDIO_LIST_CMD_STATE["timer"] = threading.Timer(delay_sec, _send)
            FANSTUDIO_LIST_CMD_STATE["timer"].daemon = True
            FANSTUDIO_LIST_CMD_STATE["timer"].start()

    @staticmethod
    def parse_fan_studio_data(data):
        """解析FanStudio数据"""
        result = []

        from services.common.source_switches import is_list_enabled
        for source, parser in FAN_STUDIO_PARSERS.items():
            if source in EXCLUDED_SOURCES:
                continue
            if not is_list_enabled(source):
                continue
            if source in FANSTUDIO_NO_RAW_CACHE_SOURCES:
                continue

            source_entry = WebSocketHandler._get_source_entry(data, source)
            if source_entry:
                source_data = source_entry.get('Data', {})
                if not source_data:
                    continue

                try:
                    event = parser(source_data)
                    if event:
                        result.append(event)
                except Exception as e:
                    logger.error(f"解析FanStudio {source} 数据失败: {e}")
                    continue

        return result

    @staticmethod
    def _expand_fan_v21(data: dict) -> dict:
        """将 v2.1 initial/update 嵌套 data 展开为旧版 start_all/update 形态。"""
        msg_type = data.get('type')
        if msg_type not in ('initial', 'update') or not isinstance(data.get('data'), dict):
            return data
        nested = data['data']
        if msg_type == 'initial':
            expanded = {'type': 'start_all'}
            for k, v in nested.items():
                expanded[k] = v
            return expanded
        if len(nested) == 1:
            sk, entry = next(iter(nested.items()))
            inner = entry.get('Data', {}) if isinstance(entry, dict) else entry
            md5 = entry.get('md5', '') if isinstance(entry, dict) else ''
            return {
                'type': 'update',
                'source': sk,
                'institution': sk,
                'Data': inner,
                'md5': md5,
            }
        return data

    @staticmethod
    def dispatch_fanstudio_message(data):
        """处理 Fan Studio JSON（共享 /all 或独立连接，兼容 v2.1）。"""
        from services.fused.list.upstream.fanstudio_dispatch import FanStudioDispatch
        FanStudioDispatch.dispatch(data, WebSocketHandler)

    @staticmethod
    def fanstudio_shared_on_open(ws):
        logger.info(f"FanStudio 共享连接已建立: {FAN_STUDIO_SWITCH_CONFIG.get('current_url')}")
        Utils.reset_circuit_breaker("FAN_STUDIO")
        WebSocketHandler._fanstudio_cancel_list_cmd_timer()
        with _fanstudio_list_cmd_lock:
            FANSTUDIO_LIST_CMD_STATE["phase"] = 0
        if not WebSocketHandler._fanstudio_send_list_cmd("cenclist"):
            try:
                ws.send("cenclist")
            except Exception as e:
                logger.error(f"发送 cenclist 失败: {e}")
        else:
            logger.info(
                f"已发送 cenclist；收到响应并入库后间隔 "
                f"{FANSTUDIO_LIST_CMD_GAP_SEC:g}–{FANSTUDIO_LIST_CMD_GAP_MAX_SEC:g}s（随条数增加）再发 cwalist / fssnlist"
            )

    @staticmethod
    def attach_shared_fanstudio(router, conn):
        list_config._shared_fan_conn = conn
        router.register_message(WebSocketHandler.dispatch_fanstudio_message)
        router.register_open(WebSocketHandler.fanstudio_shared_on_open)

    @staticmethod
    def process_fan_studio_ws():
        """处理FanStudio WebSocket连接（非融合模式独立线程）"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                return
            WebSocketHandler.dispatch_fanstudio_message(data)

        def on_error(ws, error):
            logger.error(f"FanStudio WebSocket错误: {error}")
            Utils.handle_fetch_error("FAN_STUDIO", error)

        def on_close(ws, close_status_code, close_msg):
            WebSocketHandler._fanstudio_cancel_list_cmd_timer()
            with _fanstudio_list_cmd_lock:
                FANSTUDIO_LIST_CMD_STATE["phase"] = 0
            logger.warning(f"FanStudio WebSocket连接关闭: {close_status_code} - {close_msg}")
            with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                if not FAN_STUDIO_SWITCH_CONFIG["is_using_backup"]:
                    FAN_STUDIO_SWITCH_CONFIG["primary_fail_count"] += 1
                    logger.warning(f"FanStudio主服务器连接失败，失败次数: {FAN_STUDIO_SWITCH_CONFIG['primary_fail_count']}/{FAN_STUDIO_SWITCH_CONFIG['primary_fail_threshold']}")

        def on_open(ws):
            current_url = FAN_STUDIO_SWITCH_CONFIG["current_url"]
            server_name = "备用服务器" if FAN_STUDIO_SWITCH_CONFIG["is_using_backup"] else "主服务器"
            logger.info(f"FanStudio WebSocket连接已建立 ({server_name}: {current_url})")
            Utils.reset_circuit_breaker("FAN_STUDIO")

            with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                if not FAN_STUDIO_SWITCH_CONFIG["is_using_backup"]:
                    FAN_STUDIO_SWITCH_CONFIG["primary_fail_count"] = 0

            WebSocketHandler._fanstudio_cancel_list_cmd_timer()
            with _fanstudio_list_cmd_lock:
                FANSTUDIO_LIST_CMD_STATE["phase"] = 0
            gap = FANSTUDIO_LIST_CMD_GAP_SEC
            logger.info(
                f"FanStudio 连接就绪，{gap:g}s 后发送 cenclist；"
                f"各列表响应入库后再间隔 {gap:g}–{FANSTUDIO_LIST_CMD_GAP_MAX_SEC:g}s（随条数增加）发送下一条"
            )
            WebSocketHandler._fanstudio_schedule_list_cmd_after_delay("cenclist", gap)

        while True:
            try:
                with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                    is_using_backup = FAN_STUDIO_SWITCH_CONFIG["is_using_backup"]
                    primary_fail_count = FAN_STUDIO_SWITCH_CONFIG["primary_fail_count"]
                    manual_lock = FAN_STUDIO_SWITCH_CONFIG.get("manual_lock", False)
                    should_use_backup = is_using_backup or (
                        not manual_lock
                        and primary_fail_count >= FAN_STUDIO_SWITCH_CONFIG["primary_fail_threshold"]
                    )

                if is_using_backup and not manual_lock:
                    with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                        now = datetime.now()
                        last_check = FAN_STUDIO_SWITCH_CONFIG["last_backup_check"]
                        switch_time = FAN_STUDIO_SWITCH_CONFIG["switch_to_backup_time"]

                        should_check = False
                        if last_check is None:
                            if switch_time and (now - switch_time).total_seconds() >= FAN_STUDIO_SWITCH_CONFIG["backup_check_interval"]:
                                should_check = True
                        else:
                            if (now - last_check).total_seconds() >= FAN_STUDIO_SWITCH_CONFIG["backup_check_interval"]:
                                should_check = True

                        if should_check:
                            FAN_STUDIO_SWITCH_CONFIG["last_backup_check"] = now
                            logger.info("FanStudio: 正在检查主服务器是否恢复...")
                            if WebSocketHandler.try_connect_primary_server():
                                logger.info("FanStudio: 主服务器已恢复，立即切换回主服务器")
                                WebSocketHandler.switch_to_primary_server()
                                with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                                    should_use_backup = False
                            else:
                                logger.info("FanStudio: 主服务器仍未恢复，继续使用备用服务器")

                if not is_using_backup and should_use_backup and not manual_lock:
                    WebSocketHandler.switch_to_backup_server()
                    with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                        should_use_backup = True

                with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                    current_url = FAN_STUDIO_WS_URL_BACKUP if should_use_backup else FAN_STUDIO_WS_URL_PRIMARY

                with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                    old_ws = FAN_STUDIO_SWITCH_CONFIG["ws_instance"]
                    if old_ws:
                        WebSocketHandler._fanstudio_cancel_list_cmd_timer()
                        FAN_STUDIO_SWITCH_CONFIG["ws_instance"] = None
                        try:
                            old_ws.close()
                        except Exception:
                            pass

                ws = WebSocketApp(
                    current_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_open=on_open
                )

                with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                    FAN_STUDIO_SWITCH_CONFIG["ws_instance"] = ws

                ws.run_forever(**ws_run_forever_kwargs())

            except Exception as e:
                logger.error(f"FanStudio WebSocket连接异常: {e}")
                Utils.handle_fetch_error("FAN_STUDIO", e)

                with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                    if not FAN_STUDIO_SWITCH_CONFIG["is_using_backup"]:
                        FAN_STUDIO_SWITCH_CONFIG["primary_fail_count"] += 1
                        logger.warning(f"FanStudio主服务器连接异常，失败次数: {FAN_STUDIO_SWITCH_CONFIG['primary_fail_count']}/{FAN_STUDIO_SWITCH_CONFIG['primary_fail_threshold']}")

                with FAN_STUDIO_SWITCH_CONFIG["lock"]:
                    retry_interval = FAN_STUDIO_SWITCH_CONFIG["primary_retry_interval"] if not FAN_STUDIO_SWITCH_CONFIG["is_using_backup"] else 10

                time.sleep(retry_interval)

    @staticmethod
    def _parse_internal_agency_ws_initial(data):
        """从内网 WebSocket 的 initial_all/start_all 中解析 bmkg / geonet（历史快照字段，用于校验日志）。"""
        result = []
        for source, parser in INTERNAL_WS_PARSERS.items():
            source_entry = WebSocketHandler._get_source_entry(data, source)
            if not source_entry:
                continue
            source_data = source_entry.get('Data', {})
            if not source_data:
                continue
            try:
                event = parser(source_data)
                if event:
                    result.append(event)
            except Exception as e:
                logger.error(f"解析内网 WebSocket {source} 数据失败: {e}")
        return result

    @staticmethod
    def _handle_internal_list_update(source_id: str, payload: dict) -> None:
        """处理 internal bus 推送的 BMKG/GeoNet 速报。"""
        from services.common.source_switches import is_internal_list_enabled
        if not is_internal_list_enabled(source_id):
            return
        source = WebSocketHandler._normalize_institution_key(source_id)
        if not source or source not in INTERNAL_WS_PARSERS:
            return
        source_data = payload.get('Data', payload)
        if not source_data or not isinstance(source_data, dict):
            return

        with fanstudio_cache_lock:
            if source not in fanstudio_raw_cache:
                fanstudio_raw_cache[source] = deque(maxlen=Config.MAX_CACHE_PER_SOURCE)
            cache_deque = fanstudio_raw_cache[source]
            if source_data not in cache_deque:
                cache_deque.append(source_data)

        CacheManager.save_fanstudio_cache()

        parser = INTERNAL_WS_PARSERS[source]
        event = parser(source_data)
        if not event:
            return
        if not WebSocketHandler.check_event_time(event, source):
            logger.debug(f"内部源 [{source}]: 事件时间不新，跳过")
            return

        FusionHandler.add_events_to_fused_list([event])
        logger.info(f"内部源 [{source}]: {FusionHandler._list_push_log_suffix(event)}")

    @staticmethod
    def attach_internal_bus(bus) -> None:
        """订阅 internal event bus（BMKG/GeoNet，替代 1450 WS）。"""
        bus.subscribe("list", WebSocketHandler._handle_internal_list_update)
        logger.info("List 已订阅内部 list 事件总线")

    @staticmethod
    def process_internal_agency_ws():
        """已废弃：内网机构数据改经 internal event bus。"""
        logger.info("内网机构 WS 客户端已禁用，使用 internal event bus")
        while True:
            time.sleep(3600)

    @staticmethod
    def process_p2pquake_ws():
        """P2PQuake：先 HTTP 拉取历史 551 情报，完成后再连接 WebSocket 接收推送"""
        from services.common.source_switches import is_list_enabled
        from services.common.source_status import get_source_status_registry
        reg = get_source_status_registry()
        http_bootstrapped = False

        def on_message(ws, message):
            from services.common.source_switches import is_list_enabled
            if not is_list_enabled("JMA"):
                return
            try:
                payload = json.loads(message)
                items = payload if isinstance(payload, list) else [payload]
                parsed_all = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if item.get("code") != 551:
                        continue
                    parsed_all.extend(JMASource.parse([item]))
                reg.record_ok("p2p_jma")
                if parsed_all:
                    FusionHandler.add_events_to_fused_list(parsed_all)
                    reg.record_event("p2p_jma")
                    logger.info(f"P2PQuake WebSocket: 融合 {len(parsed_all)} 条 code=551 地震情报")
            except Exception as e:
                logger.error(f"P2PQuake WebSocket消息处理失败: {e}")
                reg.record_error("p2p_jma", str(e))

        def on_error(ws, error):
            logger.error(f"P2PQuake WebSocket错误: {error}")
            reg.record_error("p2p_jma", str(error))
            Utils.handle_fetch_error("P2PQUAKE", error)

        def on_close(ws, close_status_code, close_msg):
            reg.set_connected("p2p_jma", False)
            logger.warning(f"P2PQuake WebSocket连接关闭: {close_status_code} - {close_msg}")

        def on_open(ws):
            reg.set_connected("p2p_jma", True)
            reg.record_ok("p2p_jma")
            logger.info(f"P2PQuake WebSocket已连接: {P2PQUAKE_WS_URL}")
            Utils.reset_circuit_breaker("P2PQUAKE")

        while True:
            try:
                if not is_list_enabled("JMA"):
                    reg.set_connected("p2p_jma", False)
                    time.sleep(5)
                    continue

                ctx = "bootstrap" if not http_bootstrapped else "reconnect"
                JMASource.prefetch_history(context=ctx)
                http_bootstrapped = True
                logger.info("P2PQuake: HTTP 拉取已完成，开始连接 WebSocket")

                ws_app = WebSocketApp(
                    P2PQUAKE_WS_URL,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_open=on_open
                )
                ws_app.run_forever(**ws_run_forever_kwargs())
            except Exception as e:
                logger.error(f"P2PQuake WebSocket连接异常: {e}")
                Utils.handle_fetch_error("P2PQUAKE", e)
            time.sleep(20)

# ============================================================================
# 数据融合和存储模块
# ============================================================================
