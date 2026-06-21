from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Set

from services.fused.eew.cache import CacheManager
from services.fused.eew.config import Config
from services.fused.eew.sources.base import DataSource
from services.fused.eew.server.ws_server import WebSocketServerManager

class EventDistributor:
    """事件分发器"""
    
    # 来自 Fan Studio /all 的数据源（只在这些源上应用“过期不推 updated_event”规则）
    FANSTUDIO_SOURCES = {"CEA", "CEA_PR", "CWA_FS", "SA", "KMA", "JMA"}
    # 历史上 Fan 通道曾使用独立 source 名，升级后需从融合列表移除以免 TTS 重复槽位
    _LEGACY_CWA_EEW_SOURCE_LABELS = (
        "台湾气象署预警(Fan)",
        "台湾气象署预警（Fan Studio）",
    )
    # 两路 CWA 互斥但共用显示名，去重指纹亦共用，避免切换时重复/误拦推送
    CWA_EEW_DEDUP_KEY = "__CWA_EEW__"
    CWA_EEW_FUSED_LABELS = frozenset(
        {DataSource.CWA_EEW_DISPLAY_NAME, *_LEGACY_CWA_EEW_SOURCE_LABELS}
    )

    def _dedup_key(self, source_key: str) -> str:
        if source_key in DataSource.CWA_EEW_MUTEX_KEYS:
            return self.CWA_EEW_DEDUP_KEY
        return source_key

    def clear_dedup(self, source_key: str) -> None:
        with self.data_hash_lock:
            self.data_hash_cache.pop(self._dedup_key(source_key), None)

    def _normalize_fused_source_label(self, src: Optional[str]) -> Optional[str]:
        if src and src in self.CWA_EEW_FUSED_LABELS:
            return DataSource.CWA_EEW_DISPLAY_NAME
        return src

    def __init__(self, config: Config, logger: logging.Logger, cache_mgr: CacheManager, 
                 ws_server: WebSocketServerManager):
        self.config = config
        self.logger = logger
        self.cache_mgr = cache_mgr
        self.ws_server = ws_server
        
        # 去重缓存
        self.log_dedup_cache: Dict[str, float] = {}
        self.log_lock = threading.Lock()
        
        # 首次加载标志
        self.is_first_load = True
        self.first_load_lock = threading.Lock()
        
        # 数据变化检测缓存（用于防止重复推送相同数据）
        self.data_hash_cache: Dict[str, str] = {}
        self.data_hash_lock = threading.Lock()
        
        # 广播时间状态（按端口记录最近一次实际广播时间，用于30秒保活判断）
        from services.common.ports import get_eew_port

        self.last_broadcast_time: Dict[int, float] = {get_eew_port(): 0.0}
        self.broadcast_state_lock = threading.Lock()
    
    def distribute(self, source_key: str, event_data: Dict[str, Any], target_ports: List[int]):
        """分发事件到各个端口"""
        from services.common.source_switches import is_active_eew_source
        from services.common.source_filters import (
            EEW_FOREIGN_IDS,
            get_filter_registry,
        )
        if not is_active_eew_source(source_key):
            return
        t_enter = time.time()
        if not event_data:
            return

        if event_data.get("type") != "cancel" and source_key in EEW_FOREIGN_IDS:
            reg = get_filter_registry()
            include, reason = reg.should_include_eew_event(source_key, event_data)
            if not include:
                self.logger.debug(
                    "%s 过滤丢弃(%s): %s",
                    source_key,
                    reason,
                    event_data.get("eventId", "unknown"),
                )
                return
        
        # 验证 startAt 字段（在分发前验证）
        start_at = event_data.get('startAt')
        if not start_at or not isinstance(start_at, (int, float)) or start_at <= 0:
            self.logger.warning(f"{source_key}事件startAt无效，跳过分发: 事件ID={event_data.get('eventId', 'unknown')}, startAt={start_at}")
            return
        
        # 检查数据是否真正变化（防止重复推送相同数据触发TTS）
        has_changed = self._check_data_changed(source_key, event_data)
        if not has_changed:
            # 数据未变化，不推送
            return
        
        event_data['last_updated'] = time.time()
        
        # 日志去重
        should_log = self._should_log(source_key, event_data)
        # 仅对 target_ports 中第一个端口记录数据更新日志，避免多端口重复
        for port in target_ports:
            self._update_port_cache(port, source_key, event_data, should_log and port == target_ports[0])
        t_leave = time.time()
        self.logger.debug(f"[Distribute] {source_key} 耗时: {(t_leave - t_enter) * 1000:.1f}ms")

    def evict_source(self, source_key: str, port: Optional[int] = None) -> None:
        """从融合推送列表移除指定源（保留磁盘缓存）。"""
        from services.common.ports import get_eew_port

        if port is None:
            port = get_eew_port()
        chinese_name = DataSource.SOURCE_NAME_MAP.get(source_key, source_key)
        norm_name = self._normalize_fused_source_label(chinese_name) or chinese_name
        current_cache = self.cache_mgr.get_fused_cache(port)
        final_events = [
            e for e in current_cache
            if self._normalize_fused_source_label(e.get("source")) != norm_name
        ]
        source_index = {evt["source"]: idx for idx, evt in enumerate(final_events) if evt.get("source")}
        self.cache_mgr.update_fused_cache(port, final_events, source_index)
        message = json.dumps(
            {"shuju": final_events},
            ensure_ascii=False,
            separators=(",", ":"),
            check_circular=False,
        )
        try:
            self.ws_server.broadcast(message, port)
        except Exception as e:
            self.logger.error(f"[Evict] 端口{port} 广播失败: {e}")
    
    def _check_data_changed(self, source_key: str, event_data: Dict[str, Any]) -> bool:
        """检查数据是否真正变化（只使用完全稳定的字段）"""
        # 只使用绝对稳定的核心字段（不包含可能动态生成的字段）
        event_id = event_data.get('eventId', 'unknown')
        updates = event_data.get('updates', 1)
        magnitude = event_data.get('magnitude', 0)
        latitude = event_data.get('latitude', 0)
        longitude = event_data.get('longitude', 0)
        depth = event_data.get('depth', 0)
        
        # 构建数据指纹（不包含startAt，因为可能使用当前时间作为默认值）
        # 不包含epicenter，因为翻译会导致变化
        data_fingerprint = f"{event_id}:{updates}:{magnitude}:{latitude}:{longitude}:{depth}"
        
        dedup_key = self._dedup_key(source_key)
        with self.data_hash_lock:
            last_fingerprint = self.data_hash_cache.get(dedup_key, '')
            
            if data_fingerprint == last_fingerprint:
                # 数据完全相同，未变化
                self.logger.debug(f"{source_key}数据未变化，跳过推送: 事件ID={event_id}, 报数={updates}")
                return False
            
            # 数据有变化，更新缓存
            self.data_hash_cache[dedup_key] = data_fingerprint
            self.logger.debug(f"{source_key}数据已变化，允许推送: 事件ID={event_id}, 报数={updates}, M{magnitude}")
            return True
    
    def _should_log(self, source_key: str, event_data: Dict[str, Any]) -> bool:
        """判断是否应该记录日志（优化版：最小化锁时间）"""
        event_id = event_data.get('eventId', 'unknown')
        updates = event_data.get('updates', 1)
        log_key = f"{source_key}:{event_id}:{updates}"
        
        current_time = time.time()
        
        with self.log_lock:
            # 快速检查（延迟清理，不在关键路径上清理）
            last_log_time = self.log_dedup_cache.get(log_key, 0)
            if current_time - last_log_time >= self.config.DEDUP_TTL:
                self.log_dedup_cache[log_key] = current_time
                
                # 只在缓存过大时才清理（避免每次都检查）
                if len(self.log_dedup_cache) > 2000:
                    # 异步清理（不阻塞推送）
                    def async_cleanup():
                        with self.log_lock:
                            expired = [k for k, t in self.log_dedup_cache.items() if current_time - t > 180]
                            for k in expired:
                                del self.log_dedup_cache[k]
                    threading.Thread(target=async_cleanup, daemon=True, name="LogCleanup").start()
                
                return True
        
        return False
    
    def _update_port_cache(self, port: int, source_name: str, event_data: Dict[str, Any], should_log: bool):
        """更新指定端口的缓存（优化版：最小化锁持有时间）"""
        chinese_name = DataSource.SOURCE_NAME_MAP.get(source_name, source_name)
        event_data['source'] = chinese_name
        
        # 验证 startAt 字段（必须存在且大于0）
        start_at = event_data.get('startAt')
        if not start_at or not isinstance(start_at, (int, float)) or start_at <= 0:
            self.logger.warning(f"[端口{port}] {source_name}事件startAt无效，跳过更新: 事件ID={event_data.get('eventId', 'unknown')}, startAt={start_at}")
            return
        
        # 快速获取当前缓存（最小化锁时间）
        current_cache = self.cache_mgr.get_fused_cache(port)
        
        # 构建源->事件映射（无锁操作）；CWA 两路共用显示名，合并历史/重复槽位
        source_to_event: Dict[str, Dict] = {}
        for evt in current_cache:
            src = evt.get('source')
            norm_src = self._normalize_fused_source_label(src)
            if norm_src and evt.get('startAt', 0) > 0:
                existing = source_to_event.get(norm_src)
                if not existing or evt.get('updates', 0) > existing.get('updates', 0):
                    source_to_event[norm_src] = evt
        
        # 更新事件
        source_to_event[chinese_name] = event_data

        # 按时间排序（使用快速排序）
        final_events = sorted(source_to_event.values(), key=lambda e: e.get('startAt', 0), reverse=True)
        
        # 构建源索引
        source_index = {evt['source']: idx for idx, evt in enumerate(final_events) if evt.get('source')}
        
        # 原子更新缓存（最小化锁时间）
        self.cache_mgr.update_fused_cache(port, final_events, source_index)

        # 立即广播：有更新立刻推送，过期 Fan Studio 报只推融合列表
        is_fan_source = source_name in self.FANSTUDIO_SOURCES
        is_expired = False
        now = time.time()
        if is_fan_source:
            try:
                # startAt 为毫秒时间戳，这里按 5 分钟阈值判断是否为过期数据
                now_ms = now * 1000
                is_expired = (now_ms - start_at) > 5 * 60 * 1000
            except Exception:
                is_expired = False

        payload: Dict[str, Any] = {"shuju": final_events}
        if (not is_fan_source) or (not is_expired):
            # 非 Fan Studio 或未过期的 Fan Studio 报文，都带 updated_event
            payload["updated_event"] = event_data

        message = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(',', ':'),
            check_circular=False
        )

        # 更新端口最近广播时间，用于30秒保活判断
        with self.broadcast_state_lock:
            self.last_broadcast_time[port] = now

        try:
            self.ws_server.broadcast(message, port)
        except Exception as e:
            self.logger.error(f"[Broadcast] 端口{port} 实时广播失败: {e}")
        
        # 记录日志
        if should_log:
            with self.first_load_lock:
                is_first = self.is_first_load
            
            magnitude = event_data.get('magnitude', 0)
            epicenter = event_data.get('epicenter', '未知')
            event_id = event_data.get('eventId', 'unknown')
            updates = event_data.get('updates', 1)
            
            # 控制台和文件都显示数据更新（控制台会自动过滤）
            self.logger.info(f"[{chinese_name}] 数据更新: 事件ID={event_id}, 报数={updates}, M{magnitude}, {epicenter}")
            
            # 文件记录详细信息
            if not is_first:
                self.logger.debug(f"[{chinese_name}] 详细: 坐标=({event_data.get('latitude')}, {event_data.get('longitude')}), 深度={event_data.get('depth')}km")
    
    def flush_pending_broadcasts(self):
        """检查并执行30秒保活广播：无新数据时定期推送当前融合列表"""
        from services.common.ports import get_eew_port

        ports = (get_eew_port(),)
        to_send: List[Tuple[int, str]] = []
        now = time.time()

        for port in ports:
            # 当前端口的融合列表为空则无需保活
            events = self.cache_mgr.get_fused_cache(port)
            if not events:
                continue

            with self.broadcast_state_lock:
                last_time = self.last_broadcast_time.get(port, 0.0)
                # 距离上一次真实广播未超过30秒，不需要保活
                if now - last_time < 30.0:
                    continue
                # 达到保活间隔，更新最近广播时间
                self.last_broadcast_time[port] = now

            # 保活报文只包含当前融合列表，不携带 updated_event
            payload: Dict[str, Any] = {"shuju": events}
            message = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(',', ':'),
                check_circular=False
            )
            to_send.append((port, message))

        # 在锁外执行实际网络发送，避免阻塞其它更新
        for port, message in to_send:
            try:
                self.ws_server.broadcast(message, port)
            except Exception as e:
                self.logger.error(f"[Broadcast] 端口{port} 保活广播失败: {e}")
    
    def set_first_load_complete(self):
        """设置首次加载完成"""
        with self.first_load_lock:
            self.is_first_load = False


# ============================================================================
# 主服务类
# ============================================================================
