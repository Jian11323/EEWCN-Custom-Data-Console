from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from typing import Any, Dict, Literal, Optional

from services.fused.common.ws_client import FanStudioWebSocketApp, ws_run_forever_kwargs
from services.fused.eew.config import Config
from services.fused.eew.sources.base import DataSource
from services.fused.eew.sources.custom import CustomSource, EarlyEstSource
from services.fused.eew.sources.cea import CEASource
from services.fused.eew.sources.fanstudio_base import FanStudioSource
from services.fused.eew.sources.jma import JMAFanStudioSource
from services.fused.eew.sources.wolfx import (
    _wolfx_cenc_eew_to_cea_raw,
    _wolfx_jma_eew_to_jma_raw,
)

class WebSocketClientManager:
    """WebSocket客户端管理器（连接上游数据源）"""

    def __init__(self, config: Config, logger: logging.Logger, sources: Dict[str, DataSource]):
        self.config = config
        self.logger = logger
        self.sources = sources

        # Fan Studio 服务器配置
        self.primary_url = self.config.ALL_WS_PRIMARY
        self.backup_url = self.config.ALL_WS_BACKUP
        self.current_server_url = self.primary_url  # 默认从主服务器开始

        # 连接控制
        self.fanstudio_thread = None
        self.fanstudio_stop_event = threading.Event()
        self.fanstudio_ws = None
        self.fanstudio_lock = threading.RLock()

        # 连接状态和健康监控
        self.connection_health = {
            'primary_server_health': 0.0,  # 健康度 0-1
            'backup_server_health': 0.0,
            'current_server': 'primary',  # 'primary' or 'backup'
            'last_health_check': 0,
            'connection_quality': 'unknown'  # 'good', 'fair', 'poor', 'unknown'
        }

        # 连接统计
        self.connection_stats = {
            'total_attempts': 0,
            'successful_connections': 0,
            'failed_connections': 0,
            'last_successful_connection': None,
            'current_fail_streak': 0,
            'max_fail_streak': 0,
            'server_switch_count': 0,
            'last_server_switch': None
        }
        self.stats_lock = threading.RLock()

        # 连接质量监控
        self.quality_monitor = {
            'recent_errors': [],  # 最近错误记录
            'error_window': 300,  # 5分钟错误窗口
            'error_threshold': 5,  # 5分钟内允许的最大错误数
            'last_quality_log': 0
        }

        # Fan Studio 管理端手动指定主/备（非 None 时禁止 should_switch_server 自动改 URL）
        self.fanstudio_manual_target: Optional[Literal['primary', 'backup']] = None
        self._fanstudio_skip_reconnect_delay = False
        self._fanstudio_intentional_close = False

        # CEA(CENC)+JMA 上游二选一：fanstudio=/all；wolfx=all_eew（Wolfx 模式时整路 Fan Studio 断开，CEA_PR/CWA_FS/SA/KMA 无新数据）
        self.cea_jma_upstream: Literal['fanstudio', 'wolfx'] = 'fanstudio'
        self.wolfx_thread = None
        self.wolfx_stop_event = threading.Event()
        self.wolfx_ws = None
        self.wolfx_lock = threading.RLock()
        self._wolfx_skip_reconnect_delay = False
        self._wolfx_intentional_close = False

    def switch_fanstudio_to_backup(self) -> None:
        """断开当前 Fan Studio 连接并锁定使用备用服务器，直至 fanstudio_resume_auto_switch。"""
        if os.environ.get("FUSED_SHARED_FAN", "").strip() in ("1", "true", "yes"):
            try:
                from services.common.fanstudio import get_fanstudio_connection
                get_fanstudio_connection().switch_to_backup()
            except ImportError:
                pass
            with self.fanstudio_lock:
                self.fanstudio_manual_target = 'backup'
                self.current_server_url = self.backup_url
            return
        ws_to_close = None
        with self.fanstudio_lock:
            self.fanstudio_manual_target = 'backup'
            self.current_server_url = self.backup_url
            self.connection_health['current_server'] = 'backup'
            self._fanstudio_skip_reconnect_delay = True
            ws_to_close = self.fanstudio_ws
        if ws_to_close:
            self._fanstudio_intentional_close = True
            try:
                ws_to_close.close()
            except Exception as e:
                self._fanstudio_intentional_close = False
                self.logger.debug(f"[Fan Studio] 管理切换关闭 WebSocket 时异常: {e}")
        with self.stats_lock:
            self.connection_stats['server_switch_count'] += 1
            self.connection_stats['last_server_switch'] = time.time()
            self.connection_stats['current_fail_streak'] = 0
        self.logger.info("[Fan Studio] 已切换为备用服务器（手动锁定，自动切换已暂停）")

    def switch_fanstudio_to_primary(self) -> None:
        """断开当前连接并锁定使用主服务器，直至 fanstudio_resume_auto_switch。"""
        if os.environ.get("FUSED_SHARED_FAN", "").strip() in ("1", "true", "yes"):
            try:
                from services.common.fanstudio import get_fanstudio_connection
                get_fanstudio_connection().switch_to_primary()
            except ImportError:
                pass
            with self.fanstudio_lock:
                self.fanstudio_manual_target = 'primary'
                self.current_server_url = self.primary_url
            return
        ws_to_close = None
        with self.fanstudio_lock:
            self.fanstudio_manual_target = 'primary'
            self.current_server_url = self.primary_url
            self.connection_health['current_server'] = 'primary'
            self._fanstudio_skip_reconnect_delay = True
            ws_to_close = self.fanstudio_ws
        if ws_to_close:
            self._fanstudio_intentional_close = True
            try:
                ws_to_close.close()
            except Exception as e:
                self._fanstudio_intentional_close = False
                self.logger.debug(f"[Fan Studio] 管理切换关闭 WebSocket 时异常: {e}")
        with self.stats_lock:
            self.connection_stats['server_switch_count'] += 1
            self.connection_stats['last_server_switch'] = time.time()
            self.connection_stats['current_fail_streak'] = 0
        self.logger.info("[Fan Studio] 已切换为主服务器（手动锁定，自动切换已暂停）")

    def fanstudio_resume_auto_switch(self) -> None:
        """清除手动锁定，恢复按健康度的自动切换策略（不断开当前连接）。"""
        if os.environ.get("FUSED_SHARED_FAN", "").strip() in ("1", "true", "yes"):
            try:
                from services.common.fanstudio import get_fanstudio_connection
                get_fanstudio_connection().resume_auto_switch()
            except ImportError:
                pass
        with self.fanstudio_lock:
            self.fanstudio_manual_target = None
        self.logger.info("[Fan Studio] 已恢复自动切换策略")

    def switch_cea_jma_to_wolfx(self) -> None:
        """CEA/JMA 改走 Wolfx all_eew：断开 Fan Studio /all（其它 Fan 源暂停直至切回）。"""
        self.cea_jma_upstream = 'wolfx'
        if os.environ.get("FUSED_SHARED_FAN", "").strip() in ("1", "true", "yes"):
            try:
                from services.common.fanstudio import get_fanstudio_connection
                get_fanstudio_connection().set_disabled(True)
            except ImportError:
                pass
        ws_to_close = None
        with self.fanstudio_lock:
            ws_to_close = self.fanstudio_ws
        if ws_to_close:
            self._fanstudio_intentional_close = True
            try:
                ws_to_close.close()
            except Exception as e:
                self._fanstudio_intentional_close = False
                self.logger.debug(f"[Fan Studio] Wolfx 切换关闭 WebSocket 时异常: {e}")
        self._wolfx_skip_reconnect_delay = True
        self.logger.info(
            "[CEA/JMA] 已切换为 Wolfx all_eew；Fan Studio /all 已断开"
            "（CEA_PR/CWA_FS/SA/KMA 等无推送直至执行「切换fan studio服务器」）"
        )

    def switch_cea_jma_to_fanstudio(self) -> None:
        """CEA/JMA 改回 Fan Studio /all：断开 Wolfx，尽快重连 Fan Studio。"""
        self.cea_jma_upstream = 'fanstudio'
        if os.environ.get("FUSED_SHARED_FAN", "").strip() in ("1", "true", "yes"):
            try:
                from services.common.fanstudio import get_fanstudio_connection
                get_fanstudio_connection().set_disabled(False)
            except ImportError:
                pass
        ws_to_close = None
        with self.wolfx_lock:
            ws_to_close = self.wolfx_ws
        if ws_to_close:
            self._wolfx_intentional_close = True
            try:
                ws_to_close.close()
            except Exception as e:
                self._wolfx_intentional_close = False
                self.logger.debug(f"[Wolfx] 切回 Fan Studio 关闭 WebSocket 时异常: {e}")
        self._fanstudio_skip_reconnect_delay = True
        self.logger.info("[CEA/JMA] 已切换回 Fan Studio /all")

    def _handle_wolfx_payload(self, data: Any) -> None:
        """解析 Wolfx all_eew 单条 JSON（或列表），分发 CENC->CEA、JMA->JMA。"""
        if isinstance(data, list):
            for item in data:
                self._handle_wolfx_payload(item)
            return
        if not isinstance(data, dict):
            return
        msg_type = data.get("type")
        if msg_type in ("heartbeat", "pong"):
            return
        if msg_type == "cenc_eew":
            raw = _wolfx_cenc_eew_to_cea_raw(data)
            md5 = _wolfx_synthetic_md5(
                {"t": "cenc_eew", "e": data.get("EventID"), "n": data.get("ReportNum"), "id": data.get("ID")}
            )
            cea = self.sources.get("CEA")
            if isinstance(cea, CEASource):
                cea.on_message(raw, md5)
            return
        if msg_type == "jma_eew":
            raw = _wolfx_jma_eew_to_jma_raw(data)
            md5 = _wolfx_synthetic_md5(
                {
                    "t": "jma_eew",
                    "e": data.get("EventID"),
                    "s": data.get("Serial"),
                    "c": data.get("isCancel"),
                }
            )
            jma = self.sources.get("JMA")
            if isinstance(jma, JMAFanStudioSource):
                jma.on_message(raw, md5)
            return

    def _route_all_ws_full_source(self, source_name: str, data: Dict[str, Any]) -> None:
        """将 all_ws 1450 单源的 inner Data 分发给对应 EEW Source。"""
        try:
            if source_name == "early-est":
                early = self.sources.get("EARLY_EST")
                if isinstance(early, EarlyEstSource):
                    early.on_message({"type": "update", "data": data})
                return
        except Exception as e:
            self.logger.error(f"[ALL_WS_FULL] 分发源 {source_name} 失败: {e}")

    def _set_all_ws_full_upstream_connected(self, connected: bool) -> None:
        early = self.sources.get("EARLY_EST")
        if isinstance(early, EarlyEstSource):
            early.connected = connected

    def _handle_all_ws_full_json(self, payload: Dict[str, Any]) -> None:
        msg_type = payload.get("type")
        if msg_type == "heartbeat":
            return
        if msg_type == "start_all":
            for key, entry in payload.items():
                if key in ("type", "institution"):
                    continue
                if not isinstance(entry, dict):
                    continue
                inner = entry.get("Data")
                if not isinstance(inner, dict):
                    continue
                if isinstance(key, str):
                    if key.startswith("institution:"):
                        source_key = key[len("institution:"):]
                    elif key.startswith("institution："):
                        source_key = key[len("institution："):]
                    else:
                        source_key = key
                else:
                    source_key = key
                self._route_all_ws_full_source(source_key, inner)
            return
        inner = payload.get("Data")
        if not isinstance(inner, dict):
            return

        # 兼容旧格式：type=update + institution/source
        if msg_type == "update":
            src_name = payload.get("institution") or payload.get("source")
            if isinstance(src_name, str):
                self._route_all_ws_full_source(src_name, inner)
            return

        # 新格式：type 直接为机构名（如 early-est / custom ...）
        if isinstance(msg_type, str):
            self._route_all_ws_full_source(msg_type, inner)

    def start_all_ws_full_client(self):
        """已废弃：内部源经 event bus 接入，不再连接 1450。"""
        self.logger.info("ALL_WS_FULL 客户端已禁用，使用 internal event bus")

    def dispatch_fanstudio_payload(self, data: dict, fan_sources: Optional[Dict[str, Any]] = None) -> None:
        """处理 Fan Studio JSON（共享连接或独立 WS，兼容 v2.1 initial/update）。"""
        from services.common.fanstudio.normalize import is_fan_control_message, iter_fan_sources
        from services.common.source_status import get_source_status_registry
        if fan_sources is None:
            fan_sources = {k: v for k, v in self.sources.items() if isinstance(v, FanStudioSource)}
        try:
            if not data:
                return
            if is_fan_control_message(data):
                if data.get('type') == 'heartbeat':
                    self.logger.debug(f"[Fan Studio] 收到服务器心跳: ver={data.get('ver')}, id={data.get('id')}")
                return
            msg_type = data.get('type')
            if msg_type in ('start_all', 'initial_all', 'initial'):
                self.logger.info("[Fan Studio] 收到初始数据")
            from services.common.source_switches import is_fan_eew_enabled, is_eew_enabled
            fan_by_key = {v.fan_key: v for v in fan_sources.values()}
            for source_key, inner, md5 in iter_fan_sources(data):
                if not is_fan_eew_enabled(source_key):
                    continue
                src = fan_by_key.get(source_key)
                if src and inner:
                    src.on_message(inner, md5)
                    continue
                sk = str(source_key).lower()
                if sk in ("early-est", "earlyest") and inner and is_eew_enabled("EARLY_EST"):
                    early = self.sources.get("EARLY_EST")
                    if isinstance(early, EarlyEstSource):
                        early.on_message({"type": "update", "data": inner})
            get_source_status_registry().record_ok("fanstudio")
        except Exception as e:
            self.logger.debug(f"Fan Studio消息处理异常: {e}")

    def _sync_internal_source_connected(self) -> None:
        """将 internal 采集器的 registry 连接状态同步到 EEW 数据源对象。"""
        from services.common.source_status import get_source_status_registry
        reg_sources = get_source_status_registry().snapshot().get("sources", {})
        for source_key, reg_id in DataSource.INTERNAL_REGISTRY_IDS.items():
            src = self.sources.get(source_key)
            info = reg_sources.get(reg_id)
            if src is not None and info is not None:
                src.connected = bool(info.get("connected"))

    def attach_internal_bus(self, bus) -> None:
        """订阅内部 event bus（自定义 / Early-est 等内部采集源）。"""
        def _on_eew(source_id: str, payload: dict) -> None:
            from services.common.source_switches import is_internal_eew_enabled
            if not is_internal_eew_enabled(source_id):
                return
            sid = source_id.lower()
            try:
                if sid == "custom":
                    custom_src = self.sources.get("CUSTOM")
                    if isinstance(custom_src, CustomSource):
                        inner = payload.get("Data", payload)
                        custom_src.on_raw_payload(inner)
                elif sid in ("early-est", "earlyest"):
                    early = self.sources.get("EARLY_EST")
                    if isinstance(early, EarlyEstSource):
                        if "data" in payload:
                            early.on_message(payload)
                        else:
                            early.on_message({"type": "update", "data": payload.get("Data", payload)})
                self._sync_internal_source_connected()
            except Exception as e:
                self.logger.error(f"[InternalBus] 分发 {source_id} 失败: {e}")

        bus.subscribe("eew", _on_eew)
        self._sync_internal_source_connected()
        self.logger.info("已订阅内部 EEW 事件总线")

    def attach_shared_fanstudio(self, router, conn) -> None:
        """融合模式：注册到全局 Fan Studio 连接。"""
        self._shared_fan_conn = conn
        fan_sources = {k: v for k, v in self.sources.items() if isinstance(v, FanStudioSource)}
        router.register_message(lambda d: self.dispatch_fanstudio_payload(d, fan_sources))

        def _on_open(ws):
            self.logger.info(f"[Fan Studio] 连接成功(共享): {conn.health.current_url}")
            for source in fan_sources.values():
                source.connected = True
            try:
                from services.common.source_status import get_source_status_registry
                reg = get_source_status_registry()
                reg.set_connected("fanstudio", True)
                reg.set_extra("fanstudio", url=conn.health.current_url)
            except Exception:
                pass

        router.register_open(_on_open)

        def _on_close(ws, code, msg):
            for source in fan_sources.values():
                source.connected = False
            try:
                from services.common.source_status import get_source_status_registry
                get_source_status_registry().set_connected("fanstudio", False)
            except Exception:
                pass

        router.register_close(_on_close)

    def start_fanstudio_client(self):
        """启动Fan Studio WebSocket客户端（智能连接管理）"""
        if os.environ.get("FUSED_SHARED_FAN", "").strip() in ("1", "true", "yes"):
            self.logger.info("[Fan Studio] 融合模式：使用共享连接，跳过独立客户端线程")
            return
        with self.fanstudio_lock:
            # 如果已有连接线程在运行，先停止
            if self.fanstudio_thread and self.fanstudio_thread.is_alive():
                self.logger.info("重启Fan Studio连接...")
                self.fanstudio_stop_event.set()
                if self.fanstudio_ws:
                    try:
                        self.fanstudio_ws.close()
                    except Exception as e:
                        self.logger.debug(f"[Fan Studio] 关闭 WebSocket 时异常: {e}")
                    self.fanstudio_ws = None

                # 等待线程停止
                for _ in range(50):  # 增加等待时间
                    if not self.fanstudio_thread.is_alive():
                        break
                    time.sleep(0.1)
                self.fanstudio_stop_event.clear()

        fan_sources = {k: v for k, v in self.sources.items() if isinstance(v, FanStudioSource)}

        def record_error(error_type: str, error_msg: str):
            """记录错误并评估连接质量"""
            current_time = time.time()

            with self.stats_lock:
                self.quality_monitor['recent_errors'].append({
                    'time': current_time,
                    'type': error_type,
                    'message': error_msg
                })

                # 清理过期错误
                cutoff_time = current_time - self.quality_monitor['error_window']
                self.quality_monitor['recent_errors'] = [
                    err for err in self.quality_monitor['recent_errors']
                    if err['time'] > cutoff_time
                ]

                # 更新连接质量
                error_count = len(self.quality_monitor['recent_errors'])
                if error_count == 0:
                    self.connection_health['connection_quality'] = 'good'
                elif error_count <= self.quality_monitor['error_threshold']:
                    self.connection_health['connection_quality'] = 'fair'
                else:
                    self.connection_health['connection_quality'] = 'poor'

        def on_message(ws, message):
            try:
                data = json.loads(message)
                if data is None:
                    return
                self.dispatch_fanstudio_payload(data, fan_sources)
            except Exception as e:
                self.logger.debug(f"Fan Studio消息处理异常: {e}")

        def send_ping():
            """发送ping心跳的线程函数"""
            while not self.fanstudio_stop_event.is_set():
                try:
                    if self.fanstudio_ws and hasattr(self.fanstudio_ws, 'send'):
                        ping_msg = json.dumps({
                            "type": "ping",
                            "timestamp": int(time.time() * 1000)
                        })
                        self.fanstudio_ws.send(ping_msg)
                        self.logger.debug("[Fan Studio] 发送ping心跳")
                except Exception as e:
                    self.logger.debug(f"[Fan Studio] 发送ping失败: {e}")

                # 每10分钟发送一次ping
                for _ in range(1800):  # 30分钟 = 1800秒
                    if self.fanstudio_stop_event.is_set():
                        break
                    time.sleep(1)

        def on_open(ws):
            server_name = "主服务器" if self.current_server_url == self.primary_url else "备用服务器"
            print(f"[OK] Fan Studio连接成功 ({server_name})")
            self.logger.info(f"[Fan Studio] 连接成功: {self.current_server_url}")

            # 更新连接状态
            for source in fan_sources.values():
                source.connected = True

            with self.stats_lock:
                self.connection_stats['successful_connections'] += 1
                self.connection_stats['last_successful_connection'] = time.time()
                self.connection_stats['current_fail_streak'] = 0

                # 更新服务器健康度
                if self.current_server_url == self.primary_url:
                    self.connection_health['primary_server_health'] = min(1.0, self.connection_health['primary_server_health'] + 0.1)
                else:
                    self.connection_health['backup_server_health'] = min(1.0, self.connection_health['backup_server_health'] + 0.1)

            # 启动ping心跳线程
            ping_thread = threading.Thread(target=send_ping, daemon=True, name="FanStudio-Ping")
            ping_thread.start()
            self.logger.debug("[Fan Studio] ping心跳线程已启动")

        def on_close(ws, code, msg):
            server_name = "主服务器" if self.current_server_url == self.primary_url else "备用服务器"
            print(f"[X] Fan Studio断开 ({server_name})")
            self.logger.info(f"[Fan Studio] 连接断开: {self.current_server_url}, code={code}, msg={msg}")

            for source in fan_sources.values():
                source.connected = False

            intentional = False
            if self._fanstudio_intentional_close:
                intentional = True
                self._fanstudio_intentional_close = False

            # 只有非主动断开时才记录错误
            if not self.fanstudio_stop_event.is_set() and not intentional:
                record_error('disconnect', f'code={code}, msg={msg}')

        def on_error(ws, error):
            error_str = str(error)

            # 分类错误并记录
            if '502' in error_str or 'Bad Gateway' in error_str:
                record_error('bad_gateway', error_str)
                self.logger.debug(f"[Fan Studio] 网关错误: {self.current_server_url}")
            elif 'Connection refused' in error_str:
                record_error('connection_refused', error_str)
                self.logger.debug(f"[Fan Studio] 连接被拒绝: {self.current_server_url}")
            elif '1013' in error_str or 'Server is warming up' in error_str:
                record_error('server_warming', error_str)
                self.logger.debug(f"[Fan Studio] 服务器预热中: {self.current_server_url}")
            else:
                record_error('other', error_str)
                self.logger.debug(f"[Fan Studio] 连接错误: {error_str}")

            for source in fan_sources.values():
                source.connected = False

            # 更新失败统计
            with self.stats_lock:
                self.connection_stats['failed_connections'] += 1
                self.connection_stats['current_fail_streak'] += 1
                if self.connection_stats['current_fail_streak'] > self.connection_stats['max_fail_streak']:
                    self.connection_stats['max_fail_streak'] = self.connection_stats['current_fail_streak']

                # 降低当前服务器健康度
                if self.current_server_url == self.primary_url:
                    self.connection_health['primary_server_health'] = max(0.0, self.connection_health['primary_server_health'] - 0.2)
                else:
                    self.connection_health['backup_server_health'] = max(0.0, self.connection_health['backup_server_health'] - 0.2)

        def should_switch_server():
            """判断是否需要切换服务器"""
            if self.cea_jma_upstream == 'wolfx':
                return False
            if self.fanstudio_manual_target is not None:
                return False

            current_time = time.time()

            with self.stats_lock:
                # 定期健康检查（每5分钟）
                if current_time - self.connection_health['last_health_check'] > 300:
                    self.connection_health['last_health_check'] = current_time

                    # 如果当前服务器健康度过低，且另一个服务器相对健康，则切换
                    current_health = (self.connection_health['primary_server_health']
                                    if self.current_server_url == self.primary_url
                                    else self.connection_health['backup_server_health'])

                    other_health = (self.connection_health['backup_server_health']
                                  if self.current_server_url == self.primary_url
                                  else self.connection_health['primary_server_health'])

                    if current_health < 0.3 and other_health > current_health + 0.2:
                        return True

                # 如果连接质量差且失败次数过多，尝试切换
                if (self.connection_health['connection_quality'] == 'poor' and
                    self.connection_stats['current_fail_streak'] >= 3):
                    return True

            return False

        def perform_server_switch():
            """执行服务器切换"""
            old_url = self.current_server_url
            old_server = "主服务器" if old_url == self.primary_url else "备用服务器"

            # 切换到另一个服务器
            if self.current_server_url == self.primary_url:
                self.current_server_url = self.backup_url
                new_server = "备用服务器"
            else:
                self.current_server_url = self.primary_url
                new_server = "主服务器"

            with self.stats_lock:
                self.connection_stats['server_switch_count'] += 1
                self.connection_stats['last_server_switch'] = time.time()
                self.connection_stats['current_fail_streak'] = 0  # 重置失败计数

            self.logger.info(f"[Fan Studio] 自动切换服务器: {old_server} -> {new_server}")
            print(f"🔄 自动切换到{new_server}")

        def calculate_reconnect_delay():
            """智能重连延迟计算"""
            with self.stats_lock:
                fail_streak = self.connection_stats['current_fail_streak']
                quality = self.connection_health['connection_quality']

            # 基础延迟
            base_delay = 3

            # 根据失败次数增加延迟
            if fail_streak > 0:
                base_delay += min(fail_streak * 2, 30)

            # 根据连接质量调整延迟
            if quality == 'poor':
                base_delay = min(base_delay * 1.5, 60)
            elif quality == 'fair':
                base_delay = min(base_delay * 1.2, 45)

            # 增加随机性避免同时重连
            base_delay += random.uniform(0, 3)

            return max(1, int(base_delay))

        def run():
            consecutive_failures = 0

            while not self.fanstudio_stop_event.is_set():
                while self.cea_jma_upstream == 'wolfx' and not self.fanstudio_stop_event.is_set():
                    time.sleep(0.2)
                if self.fanstudio_stop_event.is_set():
                    break
                try:
                    # 检查是否需要切换服务器
                    if should_switch_server():
                        perform_server_switch()
                        consecutive_failures = 0  # 重置连续失败计数

                    with self.stats_lock:
                        self.connection_stats['total_attempts'] += 1

                    # 创建WebSocket连接
                    ws = FanStudioWebSocketApp(
                        self.current_server_url,
                        on_message=on_message,
                        on_open=on_open,
                        on_close=on_close,
                        on_error=on_error
                    )

                    with self.fanstudio_lock:
                        self.fanstudio_ws = ws

                    if self.fanstudio_stop_event.is_set():
                        break

                    # 运行连接，禁用自动ping并忽略SSL证书验证
                    ws.run_forever(ping_interval=None, **ws_run_forever_kwargs())

                    # 连接断开后清理
                    with self.fanstudio_lock:
                        if self.fanstudio_ws == ws:
                            self.fanstudio_ws = None

                    if self.fanstudio_stop_event.is_set():
                        break

                    consecutive_failures += 1

                except Exception as e:
                    if not self.fanstudio_stop_event.is_set():
                        self.logger.debug(f"Fan Studio连接异常: {e}")
                        consecutive_failures += 1

                if self.fanstudio_stop_event.is_set():
                    break

                # 计算重连延迟
                reconnect_delay = calculate_reconnect_delay()
                if self._fanstudio_skip_reconnect_delay:
                    reconnect_delay = 0
                    self._fanstudio_skip_reconnect_delay = False

                # 定期输出连接质量状态
                current_time = time.time()
                if current_time - self.quality_monitor['last_quality_log'] > 300:  # 5分钟
                    self.quality_monitor['last_quality_log'] = current_time
                    quality = self.connection_health['connection_quality']
                    fail_streak = self.connection_stats['current_fail_streak']
                    self.logger.info(f"[Fan Studio] 连接状态: 质量={quality}, 连续失败={fail_streak}, 重连延迟={reconnect_delay}s")

                # 等待重连，期间检查停止事件
                elapsed = 0
                while elapsed < reconnect_delay and not self.fanstudio_stop_event.is_set():
                    sleep_time = min(0.2, reconnect_delay - elapsed)
                    time.sleep(sleep_time)
                    elapsed += sleep_time

        # 启动连接线程
        with self.fanstudio_lock:
            self.fanstudio_thread = threading.Thread(target=run, daemon=True, name="FanStudio-WS")
            self.fanstudio_thread.start()
            self.logger.info(f"Fan Studio智能连接已启动: {self.current_server_url}")

    def start_wolfx_all_eew_client(self) -> None:
        """Wolfx all_eew：仅在 cea_jma_upstream==wolfx 时连接；分发 cenc_eew / jma_eew 至 CEA、JMA。"""
        with self.wolfx_lock:
            if self.wolfx_thread and self.wolfx_thread.is_alive():
                return

        def run():
            while not self.wolfx_stop_event.is_set():
                try:
                    while self.cea_jma_upstream != 'wolfx' and not self.wolfx_stop_event.is_set():
                        time.sleep(0.2)
                    if self.wolfx_stop_event.is_set():
                        break

                    def on_message(ws, message):
                        try:
                            msg = message.decode("utf-8") if isinstance(message, (bytes, bytearray)) else message
                            if isinstance(msg, str) and msg.strip().lower() == "ping":
                                return
                            data = json.loads(msg)
                            self._handle_wolfx_payload(data)
                        except json.JSONDecodeError:
                            pass
                        except Exception as e:
                            self.logger.debug(f"[Wolfx] 消息处理异常: {e}")

                    def on_open(ws):
                        print("[OK] Wolfx all_eew 连接成功")
                        self.logger.info(f"[Wolfx] 已连接 {self.config.WOLFX_ALL_EEW_URL}")
                        cea = self.sources.get("CEA")
                        if isinstance(cea, FanStudioSource):
                            cea.connected = True
                        jma = self.sources.get("JMA")
                        if isinstance(jma, FanStudioSource):
                            jma.connected = True

                    def on_close(ws, code, msg):
                        print(f"[X] Wolfx all_eew 断开 ({code})")
                        self.logger.info(f"[Wolfx] 连接断开: code={code}, msg={msg}")
                        cea = self.sources.get("CEA")
                        if isinstance(cea, FanStudioSource):
                            cea.connected = False
                        jma = self.sources.get("JMA")
                        if isinstance(jma, FanStudioSource):
                            jma.connected = False
                        if self._wolfx_intentional_close:
                            self._wolfx_intentional_close = False

                    def on_error(ws, error):
                        self.logger.debug(f"[Wolfx] 连接错误: {error}")
                        cea = self.sources.get("CEA")
                        if isinstance(cea, FanStudioSource):
                            cea.connected = False
                        jma = self.sources.get("JMA")
                        if isinstance(jma, FanStudioSource):
                            jma.connected = False

                    ws = FanStudioWebSocketApp(
                        self.config.WOLFX_ALL_EEW_URL,
                        on_message=on_message,
                        on_open=on_open,
                        on_close=on_close,
                        on_error=on_error,
                    )
                    with self.wolfx_lock:
                        self.wolfx_ws = ws
                    if self.wolfx_stop_event.is_set():
                        break
                    ws.run_forever(ping_interval=None, **ws_run_forever_kwargs())
                    with self.wolfx_lock:
                        if self.wolfx_ws == ws:
                            self.wolfx_ws = None
                except Exception as e:
                    if not self.wolfx_stop_event.is_set():
                        self.logger.debug(f"[Wolfx] 连接异常: {e}")
                if self.wolfx_stop_event.is_set():
                    break
                reconnect_delay = 3
                if self._wolfx_skip_reconnect_delay:
                    reconnect_delay = 0
                    self._wolfx_skip_reconnect_delay = False
                elapsed = 0.0
                while elapsed < reconnect_delay and not self.wolfx_stop_event.is_set():
                    st = min(0.2, reconnect_delay - elapsed)
                    time.sleep(st)
                    elapsed += st

        with self.wolfx_lock:
            self.wolfx_stop_event.clear()
            self.wolfx_thread = threading.Thread(target=run, daemon=True, name="Wolfx-ALL-EEW")
            self.wolfx_thread.start()
        self.logger.info("[Wolfx] all_eew 客户端线程已启动（仅 wolfx 模式时建立连接）")

    def start_cwa_client(self) -> None:
        """CWA 由 ALL_WS_FULL（1450）聚合推送，此处保留空实现以兼容旧调用。"""
        pass


# ============================================================================
# 客户端IP管理器
# ============================================================================

