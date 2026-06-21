from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False

from services.fused.eew.cache import CacheManager
from services.fused.eew.config import Config
from services.fused.eew.distributor import EventDistributor
from services.fused.eew.logging_mgr import LogManager
from services.fused.eew.sources.base import DataSource
from services.fused.eew.sources.cea import CEAPRSource, CEASource
from services.fused.eew.sources.custom import CustomSource, EarlyEstSource
from services.fused.eew.sources.cwa import CWAFanStudioSourceV2
from services.fused.eew.sources.jma import JMAFanStudioSource
from services.fused.eew.sources.kma import KMASource
from services.fused.eew.sources.sa import SASource
from services.fused.eew.server.ws_server import WebSocketServerManager
from services.fused.eew.translation import TranslationService
from services.fused.eew.upstream.ws_client_mgr import WebSocketClientManager

class EEWService:
    """地震预警融合服务"""
    
    def __init__(self):
        self.config = Config()

        # 创建目录
        for directory in [self.config.LOG_DIR, self.config.CACHE_DIR, self.config.TRANSLATION_CACHE_DIR]:
            os.makedirs(directory, exist_ok=True)

        # 初始化组件
        self.log_mgr = LogManager(self.config)
        self.cache_mgr = CacheManager(self.config, self.log_mgr.get_logger('data'))
        self.translator = TranslationService(self.config, self.log_mgr.get_logger('error'))

        # 初始化数据源
        self.sources = self._init_sources()

        # WebSocket客户端管理器
        self.ws_client_mgr = WebSocketClientManager(self.config, self.log_mgr.get_logger('connection'), self.sources)

        # 初始化WebSocket服务器（传入客户端管理器和自身引用）
        self.ws_server = WebSocketServerManager(self.config, self.log_mgr.get_logger('connection'), self.cache_mgr, self.ws_client_mgr, self)

        # 初始化分发器
        self.distributor = EventDistributor(self.config, self.log_mgr.get_logger('data'), self.cache_mgr, self.ws_server)

        # 线程池管理
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        # 使用RLock避免在同一线程内的嵌套调用导致死锁（如restart_thread_pool内部调用get_thread_pool_status）
        self.thread_pool_lock = threading.RLock()
        self.thread_pool_created_time: Optional[float] = None
        self.thread_pool_task_count: int = 0
        self.scheduler = None  # APScheduler 实例，用于优雅关闭时停止定时任务
        self._init_thread_pool()

        # 确保IP管理器已初始化
        self.log_mgr.get_logger('data').info("IP连接管理器已初始化")
    
    def _init_sources(self) -> Dict[str, DataSource]:
        """初始化所有数据源"""
        common_args = (self.config, self.log_mgr.get_logger('data'), self.cache_mgr, self.translator)
        
        return {
            "CUSTOM": CustomSource(*common_args),
            "CEA_PR": CEAPRSource(*common_args),
            "CEA": CEASource(*common_args),
            "CWA_FS": CWAFanStudioSourceV2(*common_args),
            "SA": SASource(*common_args),
            "KMA": KMASource(*common_args),
            "JMA": JMAFanStudioSource(*common_args),
            "EARLY_EST": EarlyEstSource(*common_args),
        }
    
    def load_caches(self):
        """加载所有缓存（不检查过期时间，保留所有历史数据）"""
        data_logger = self.log_mgr.get_logger('data')
        data_logger.info("开始加载缓存数据...")

        loaded_count = 0
        from services.common.source_switches import is_active_eew_source

        for source_key, source in self.sources.items():
            try:
                if not is_active_eew_source(source_key):
                    data_logger.debug(f"[缓存加载] {source_key}: 未生效或已关闭，跳过")
                    continue
                cache_key = source_key
                cached = self.cache_mgr.load_source_cache(cache_key)

                if cached and cached.get('data'):
                    event = None

                    if isinstance(source, SASource):
                        event = source._apply_region_to_event(cached["data"])
                    elif isinstance(source, KMASource):
                        event = source._apply_region_to_event(cached["data"])
                    else:
                        event = cached['data']

                    if event and isinstance(event, dict):
                        # 验证事件数据的基本字段
                        if all(key in event for key in ['eventId', 'updates', 'epicenter']):
                            data_logger.info(f"[缓存加载] {source_key}: 事件ID={event.get('eventId')}, 报数={event.get('updates')}, 震源={event.get('epicenter')}")
                            target_ports = source.get_target_ports()
                            self.distributor.distribute(source_key, event, target_ports)
                            loaded_count += 1
                        else:
                            data_logger.warning(f"[缓存加载] {source_key}: 数据字段不完整，跳过")
                    else:
                        data_logger.warning(f"[缓存加载] {source_key}: 无效的事件数据")
                else:
                    data_logger.debug(f"[缓存加载] {source_key}: 无缓存数据")

            except Exception as e:
                error_logger = self.log_mgr.get_logger('error')
                error_logger.error(f"加载{source_key}缓存失败: {e}")

        data_logger.info(f"缓存加载完成，共加载 {loaded_count} 个数据源")

    def republish_source_cache(self, source_key: str, *, force: bool = False) -> bool:
        """将指定源缓存重新打入融合列表（CWA 互斥切换时保持「台湾气象署预警」槽位）。"""
        from services.common.source_switches import is_active_eew_source
        if not is_active_eew_source(source_key):
            return False
        source = self.sources.get(source_key)
        if not source:
            return False
        if force:
            self.distributor.clear_dedup(source_key)
        try:
            event = source.fetch()
            if not event or not isinstance(event, dict):
                cached = self.cache_mgr.load_source_cache(source_key)
                if cached and isinstance(cached.get("data"), dict):
                    event = cached["data"]
            if not event or not all(k in event for k in ("eventId", "updates", "epicenter")):
                return False
            start_at = event.get("startAt")
            if not start_at or not isinstance(start_at, (int, float)) or start_at <= 0:
                return False
            self.distributor.distribute(source_key, event, source.get_target_ports())
            return True
        except Exception as e:
            self.log_mgr.get_logger("error").error(f"重推{source_key}缓存失败: {e}")
            return False

    def fetch_all_sources(self):
        """获取所有数据源（除每秒更新的数据源外）"""
        fast_update_sources = {"EARLY_EST"}
        results = {}

        def fetch_one(source_key: str, source: DataSource):
            try:
                event = source.fetch()
                results[source_key] = event
            except Exception as e:
                error_logger = self.log_mgr.get_logger('error')
                error_logger.error(f"获取{source_key}失败: {e}")
                results[source_key] = None

        # 只获取非快速更新的数据源
        sources_to_fetch = {k: v for k, v in self.sources.items() if k not in fast_update_sources}

        # 使用线程池实例
        executor = self._get_thread_pool()
        if executor:
            futures = [executor.submit(fetch_one, key, src) for key, src in sources_to_fetch.items()]
            for future in futures:
                future.result()
            with self.thread_pool_lock:
                self.thread_pool_task_count += len(futures)
        
        # 分发事件
        for source_key, event in results.items():
            if event:
                source = self.sources[source_key]
                target_ports = source.get_target_ports()
                self.distributor.distribute(source_key, event, target_ports)
                
                self.cache_mgr.save_source_cache(source_key, event)
    
    def _init_thread_pool(self):
        """初始化线程池"""
        with self.thread_pool_lock:
            if self.thread_pool is None:
                self.thread_pool = ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS, thread_name_prefix="EEW-Fetch")
                self.thread_pool_created_time = time.time()
                self.thread_pool_task_count = 0
                self.log_mgr.get_logger('data').info(f"线程池已初始化，最大工作线程数: {self.config.MAX_WORKERS}")
    
    def _get_thread_pool(self) -> Optional[ThreadPoolExecutor]:
        """获取线程池实例，如果不存在则创建"""
        with self.thread_pool_lock:
            if self.thread_pool is None or self.thread_pool._shutdown:
                self._init_thread_pool()
            return self.thread_pool
    
    def get_thread_pool_status(self) -> Dict[str, Any]:
        """获取线程池运行状态"""
        with self.thread_pool_lock:
            if self.thread_pool is None:
                return {
                    "状态": "未初始化",
                    "最大工作线程数": self.config.MAX_WORKERS,
                    "活动线程数": 0,
                    "队列大小": 0,
                    "创建时间": None,
                    "运行时间秒数": 0,
                    "总任务数": 0
                }
            
            try:
                # 以下使用 ThreadPoolExecutor 未文档化的私有属性，仅用于监控状态；标准库无公开 API 可替代。依赖 CPython 实现，仅用于监控。
                active_threads = len([t for t in self.thread_pool._threads if t.is_alive()]) if hasattr(self.thread_pool, '_threads') else 0
                queue_size = self.thread_pool._work_queue.qsize() if hasattr(self.thread_pool, '_work_queue') else 0
                uptime_seconds = int(time.time() - self.thread_pool_created_time) if self.thread_pool_created_time else 0
                status = "运行中"
                if getattr(self.thread_pool, '_shutdown', True):
                    status = "已关闭"
                elif active_threads == 0:
                    status = "空闲"
                
                return {
                    "状态": status,
                    "最大工作线程数": getattr(self.thread_pool, '_max_workers', self.config.MAX_WORKERS),
                    "活动线程数": active_threads,
                    "队列大小": queue_size,
                    "创建时间": datetime.fromtimestamp(self.thread_pool_created_time).strftime("%Y/%m/%d %H:%M:%S") if self.thread_pool_created_time else None,
                    "运行时间秒数": uptime_seconds,
                    "运行时间": f"{uptime_seconds // 3600}小时{(uptime_seconds % 3600) // 60}分钟{uptime_seconds % 60}秒",
                    "总任务数": self.thread_pool_task_count
                }
            except Exception as e:
                self.log_mgr.get_logger('error').error(f"获取线程池状态失败: {e}")
                return {
                    "状态": "错误",
                    "错误": str(e),
                    "最大工作线程数": self.config.MAX_WORKERS
                }
    
    def check_thread_pool(self) -> Dict[str, Any]:
        """检查线程池健康状态"""
        status = self.get_thread_pool_status()
        issues = []
        warnings = []
        
        # 检查线程池是否关闭
        if status.get("状态") == "已关闭":
            issues.append("线程池已关闭，需要重启")
        
        # 检查活动线程数是否异常
        max_workers = status.get("最大工作线程数", 0)
        active_threads = status.get("活动线程数", 0)
        if active_threads > max_workers * 1.5:
            issues.append(f"活动线程数异常: {active_threads} (最大: {max_workers})")
        elif active_threads > max_workers:
            warnings.append(f"活动线程数略高: {active_threads} (最大: {max_workers})")
        
        # 检查队列积压
        queue_size = status.get("队列大小", 0)
        if queue_size > 10:
            issues.append(f"队列积压严重: {queue_size} 个待处理任务")
        elif queue_size > 5:
            warnings.append(f"队列有积压: {queue_size} 个待处理任务")
        
        # 检查运行时间（超过24小时建议重启）
        uptime_seconds = status.get("运行时间秒数", 0)
        if uptime_seconds > 86400:
            warnings.append(f"线程池运行时间较长: {status.get('运行时间')}，建议考虑重启")
        
        # 检查任务数量（超过10万建议重启）
        total_tasks = status.get("总任务数", 0)
        if total_tasks > 100000:
            warnings.append(f"线程池已处理大量任务: {total_tasks}，建议考虑重启")
        
        health_status = "健康"
        if issues:
            health_status = "异常"
        elif warnings:
            health_status = "警告"
        
        return {
            "健康状态": health_status,
            "状态": status,
            "异常问题": issues,
            "警告信息": warnings,
            "时间戳": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        }
    
    def restart_thread_pool(self, force: bool = False) -> Dict[str, Any]:
        """安全重启线程池（不会影响上游服务器连接和数据推送）
        
        注意：
        - 线程池只用于 fetch_all_sources 的数据获取任务
        - 上游WebSocket客户端连接在独立线程中运行，不受影响
        - 数据推送在 EventDistributor 中执行，也不在线程池中
        - 重启时会等待正在执行的任务完成，确保数据完整性
        
        Args:
            force: 是否强制重启（即使有正在执行的任务），默认为False，会等待任务完成
        同时带有“自动恢复”能力：
        - 即使重启过程出现异常，也会尽量保证最终线程池处于“可用”状态
        """
        try:
            with self.thread_pool_lock:
                old_status = self.get_thread_pool_status()
                
                if self.thread_pool is None:
                    return {
                        "成功": False,
                        "消息": "线程池未初始化，无需重启",
                        "时间戳": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                    }
                
                # 检查是否有正在执行的任务
                active_threads = old_status.get("活动线程数", 0)
                queue_size = old_status.get("队列大小", 0)
                
                if not force and (active_threads > 0 or queue_size > 0):
                    # 有正在执行的任务，等待完成
                    data_logger = self.log_mgr.get_logger('data')
                    data_logger.info(f"线程池重启：等待任务完成（活动线程: {active_threads}, 队列: {queue_size}）...")
                    
                    # 等待最多30秒，让正在执行的任务完成
                    max_wait_time = 30
                    wait_interval = 1
                    waited = 0
                    
                    while waited < max_wait_time:
                        time.sleep(wait_interval)
                        waited += wait_interval
                        current_status = self.get_thread_pool_status()
                        current_active = current_status.get("活动线程数", 0)
                        current_queue = current_status.get("队列大小", 0)
                        
                        if current_active == 0 and current_queue == 0:
                            data_logger.info(f"线程池任务已完成，可以安全重启（等待了 {waited} 秒）")
                            break
                    
                    # 再次检查状态
                    final_status = self.get_thread_pool_status()
                    final_active = final_status.get("活动线程数", 0)
                    final_queue = final_status.get("队列大小", 0)
                    
                    if final_active > 0 or final_queue > 0:
                        data_logger.warning(f"线程池仍有任务在执行（活动线程: {final_active}, 队列: {final_queue}），强制关闭")
                
                # 关闭旧线程池（等待任务完成），出现异常也不中断后续恢复
                data_logger = self.log_mgr.get_logger('data')
                try:
                    data_logger.info("正在关闭旧线程池（等待任务完成）...")
                    self.thread_pool.shutdown(wait=True)
                except Exception as e_shutdown:
                    data_logger.error(f"关闭旧线程池时发生错误，将继续尝试创建新线程池: {e_shutdown}")
                finally:
                    # 无论关闭是否成功，都丢弃旧实例，重新初始化一个干净的线程池
                    self.thread_pool = None
                
                # 创建新线程池
                self._init_thread_pool()
                new_status = self.get_thread_pool_status()
                
                data_logger.info("线程池已成功重启（上游连接和数据推送未受影响）")
                
                return {
                    "成功": True,
                    "消息": "线程池已成功重启",
                    "旧状态": old_status,
                    "新状态": new_status,
                    "时间戳": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                }
        except Exception as e:
            # 到这里说明整个重启流程出现了严重异常，进行一次“兜底恢复”尝试
            error_logger = self.log_mgr.get_logger('error')
            error_msg = f"重启线程池失败: {e}"
            error_logger.error(error_msg)
            
            try:
                with self.thread_pool_lock:
                    # 如果当前线程池不可用，则尝试重新初始化一个新的线程池
                    if self.thread_pool is None or getattr(self.thread_pool, "_shutdown", False):
                        error_logger.warning("检测到线程池处于不可用状态，尝试自动重新初始化以恢复服务...")
                        self._init_thread_pool()
                        error_logger.info("自动重新初始化线程池成功，服务已恢复到可用状态")
            except Exception as recover_err:
                error_logger.error(f"自动恢复线程池失败: {recover_err}")
            
            return {
                "成功": False,
                "消息": error_msg,
                "错误": str(e),
                "时间戳": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            }
    
    def _check_and_auto_restart_thread_pool(self):
        """检查线程池运行时间，超过48小时则自动重启（不影响上游连接和数据推送）
        
        安全性说明：
        - 重启线程池不会影响上游服务器连接（WebSocket客户端在独立线程中）
        - 数据推送不受影响（推送在EventDistributor中执行，不在线程池中）
        - 线程池只用于fetch_all_sources的数据获取任务
        - 重启时会等待正在执行的任务完成，确保数据完整性
        """
        try:
            status = self.get_thread_pool_status()
            uptime_seconds = status.get("运行时间秒数", 0)
            
            # 48小时 = 172800秒
            max_uptime_seconds = 48 * 3600
            
            if uptime_seconds > max_uptime_seconds:
                data_logger = self.log_mgr.get_logger('data')
                uptime_formatted = status.get("运行时间", "未知")
                data_logger.info(f"线程池运行时间已超过48小时（{uptime_formatted}），执行自动重启...")
                
                # 执行安全重启（不强制，等待任务完成）
                # 注意：重启线程池不会影响上游服务器连接（WebSocket客户端）和数据推送
                # 因为上游连接和数据推送都不在线程池中运行
                restart_result = self.restart_thread_pool(force=False)
                
                if restart_result.get("成功"):
                    data_logger.info("线程池自动重启成功（上游连接和数据推送未受影响）")
                else:
                    error_logger = self.log_mgr.get_logger('error')
                    error_logger.error(f"线程池自动重启失败: {restart_result.get('消息', '未知错误')}")
            else:
                # 记录日志，显示距离重启还有多长时间
                remaining_hours = (max_uptime_seconds - uptime_seconds) / 3600
                if remaining_hours < 1:  # 距离重启不到1小时时记录日志
                    data_logger = self.log_mgr.get_logger('data')
                    data_logger.debug(f"线程池运行时间: {status.get('运行时间', '未知')}，距离自动重启还有 {remaining_hours:.1f} 小时")
        except Exception as e:
            error_logger = self.log_mgr.get_logger('error')
            error_logger.error(f"检查线程池运行时间失败: {e}")
    
    def flush_broadcasts(self):
        """触发一次融合数据广播刷新（供定时任务调用）"""
        try:
            self.distributor.flush_pending_broadcasts()
        except Exception as e:
            error_logger = self.log_mgr.get_logger('error')
            error_logger.error(f"执行融合数据广播刷新失败: {e}")
    
    def start_scheduler(self):
        """启动定时任务"""
        if APSCHEDULER_AVAILABLE:
            # 使用APScheduler
            scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

            # 融合数据广播刷新（1秒，统一节流控制）
            scheduler.add_job(
                self.flush_broadcasts,
                'interval',
                seconds=1.0,
                id='flush_broadcasts',
                max_instances=1,
                coalesce=True
            )

            # 全部数据源更新（5秒）
            scheduler.add_job(
                self.fetch_all_sources,
                'interval',
                seconds=5,
                id='update_all',
                max_instances=1,
                coalesce=True
            )

            # 保存翻译缓存（60秒）
            scheduler.add_job(
                self.translator.save_cache,
                'interval',
                seconds=60,
                id='save_translation',
                max_instances=1
            )

            # 清理日志（每天凌晨1点）
            scheduler.add_job(
                self.log_mgr.cleanup_old_logs,
                'cron',
                hour=1,
                minute=0,
                id='cleanup_logs'
            )

            # 检查线程池运行时间（每1小时检查一次，超过48小时自动重启）
            scheduler.add_job(
                self._check_and_auto_restart_thread_pool,
                'interval',
                hours=1,
                id='check_thread_pool_uptime',
                max_instances=1
            )

            self.scheduler = scheduler
            scheduler.start()
            data_logger = self.log_mgr.get_logger('data')
            data_logger.debug("APScheduler定时任务已启动")
        else:
            self.scheduler = None
            # 使用基本的threading实现定时任务
            error_logger = self.log_mgr.get_logger('error')
            def schedule_task(func, interval):
                """调度重复任务"""
                def wrapper():
                    try:
                        func()
                    except Exception as e:
                        error_logger.error(f"定时任务执行失败: {e}")
                    finally:
                        # 重新调度下一次执行
                        threading.Timer(interval, wrapper).start()
                # 启动第一次执行
                threading.Timer(interval, wrapper).start()

            # 融合数据广播刷新（1秒）
            schedule_task(self.flush_broadcasts, 1.0)

            # 全部数据源更新（5秒）
            schedule_task(self.fetch_all_sources, 5.0)

            # 保存翻译缓存（60秒）
            schedule_task(self.translator.save_cache, 60.0)

            # 检查线程池运行时间（每1小时 = 3600秒）
            schedule_task(self._check_and_auto_restart_thread_pool, 3600.0)

            # 清理日志（每天 = 86400秒，从现在开始计算到凌晨1点）
            def cleanup_logs_daily():
                self.log_mgr.cleanup_old_logs()
                # 重新调度到明天同一时间
                threading.Timer(86400, cleanup_logs_daily).start()

            # 计算到明天凌晨1点的时间
            now = datetime.now()
            tomorrow = now + timedelta(days=1)
            tomorrow_1am = tomorrow.replace(hour=1, minute=0, second=0, microsecond=0)
            seconds_until_1am = (tomorrow_1am - now).total_seconds()
            threading.Timer(seconds_until_1am, cleanup_logs_daily).start()

            data_logger = self.log_mgr.get_logger('data')
            data_logger.debug("基础threading定时任务已启动")
    
    def _update_single_source(self, source_key: str):
        """更新单个数据源"""
        try:
            from services.common.source_switches import is_active_eew_source
            if not is_active_eew_source(source_key):
                return
            source = self.sources.get(source_key)
            if source:
                event = source.fetch()
                if event:
                    target_ports = source.get_target_ports()
                    self.distributor.distribute(source_key, event, target_ports)
        except Exception as e:
            error_logger = self.log_mgr.get_logger('error')
            error_logger.error(f"更新{source_key}失败: {e}")
    
    def _graceful_shutdown(self):
        """优雅关闭：停止定时任务、广播循环、线程池。WS 服务端为 daemon 线程，进程退出时自动结束。"""
        data_logger = self.log_mgr.get_logger('data')
        error_logger = self.log_mgr.get_logger('error')
        # 1. 停止定时任务
        if self.scheduler is not None:
            try:
                self.scheduler.shutdown(wait=False)
                data_logger.info("定时任务已停止")
            except Exception as e:
                error_logger.error(f"停止定时任务时异常: {e}")
            self.scheduler = None
        # 2. 停止广播用事件循环
        if self.ws_server and getattr(self.ws_server, 'broadcast_loop', None):
            try:
                loop = self.ws_server.broadcast_loop
                if loop.is_running():
                    loop.call_soon_threadsafe(loop.stop)
                data_logger.info("广播事件循环已停止")
            except Exception as e:
                error_logger.error(f"停止广播循环时异常: {e}")
        # 3. 关闭线程池（等待任务完成）
        try:
            with self.thread_pool_lock:
                pool = self.thread_pool
                self.thread_pool = None
            if pool is not None and not getattr(pool, '_shutdown', True):
                pool.shutdown(wait=True)
                data_logger.info("线程池已关闭")
        except Exception as e:
            error_logger.error(f"关闭线程池时异常: {e}")

    def run(self):
        """运行服务"""
        print("=" * 60)
        print("地震预警融合API服务 v2.0")
        print("=" * 60)
        
        # 为所有数据源设置事件分发器和WebSocket服务器（用于即时推送）
        for source in self.sources.values():
            if hasattr(source, 'event_distributor'):
                source.event_distributor = self.distributor
            if hasattr(source, 'ws_server'):
                source.ws_server = self.ws_server
        
        # 加载缓存
        data_logger = self.log_mgr.get_logger('data')
        data_logger.info("开始加载缓存数据...")
        self.load_caches()
        data_logger.info("缓存数据加载完成")
        
        # 标记首次加载完成
        self.distributor.set_first_load_complete()
        
        # 启动WebSocket客户端
        connection_logger = self.log_mgr.get_logger('connection')
        connection_logger.debug("启动WebSocket客户端...")

        # 内部数据源经 event bus 接入（见 attach_internal_bus），不再连接 1450
        self.ws_client_mgr.start_fanstudio_client()
        self.ws_client_mgr.start_wolfx_all_eew_client()

        # 启动WebSocket服务器
        connection_logger.debug("启动WebSocket服务器...")
        from services.common.ports import get_eew_port, LOCAL_BIND

        eew_port = get_eew_port()
        self.ws_server.start_server(eew_port)

        # 控制台 IPC（替代 2050 管理 WebSocket）
        from services.fused.console_ipc import start_console_ipc

        start_console_ipc(self.ws_server)


        # 启动定时任务
        self.start_scheduler()

        # 后台更新一次
        def startup_update():
            time.sleep(2)
            data_logger.debug("启动后台数据更新...")
            self.fetch_all_sources()
            data_logger.debug("后台更新完成")
        
        threading.Thread(target=startup_update, daemon=True, name="Startup-Update").start()
        
        print("\n[OK] 服务启动成功")
        print(f"  - 端口 {eew_port}: 地震预警 WebSocket 推送 ({LOCAL_BIND})")
        print(f"\n正在监听数据源更新...\n")
        
        # 主循环
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n服务正在关闭...")
            data_logger.info("服务正在关闭...")
            self._graceful_shutdown()
            # 保存翻译缓存
            try:
                print("保存翻译缓存...")
                self.translator.save_cache()
                print("翻译缓存保存完成")
            except Exception as e:
                print(f"保存翻译缓存失败: {e}")

