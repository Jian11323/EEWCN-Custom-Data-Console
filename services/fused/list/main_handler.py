from __future__ import annotations

import os
import signal
import sys
import threading
import time
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed

from waitress import serve

from services.fused.list.cache import CacheManager
from services.fused.list.config import Config
from services.fused.list.logging_mgr import LogManager
from services.fused.list.pool import ThreadPoolManager
from services.fused.list.sources.bmkg import BMKGSource
from services.fused.list.sources.geonet import GEONETSource
from services.fused.list.sources.ingv import INGVSource
from services.fused.list.sources.jma import JMASource
from services.fused.list.state import app, logger
from services.fused.list.upstream.ws_handler import WebSocketHandler
from services.fused.list.utils import Utils
from services.common.http_poll_intervals import get_poll_interval

class MainHandler:
    """主程序处理器：负责程序初始化和启动"""

    @staticmethod
    def update_loop():
        """HTTP 数据源轮询（仅 INGV；GeoNet/BMKG 为启动时 HTTP 全量 + 内网 WebSocket 更新）"""
        data_processors = [
            INGVSource.process,
        ]
        consecutive_failures = 0
        max_consecutive_failures = 10

        while True:
            try:
                # 获取或创建线程池（动态调整，最低4，最大15）
                executor = ThreadPoolManager.get_http_thread_pool()
                if executor is None:
                    logger.error("无法获取HTTP线程池，等待重试")
                    time.sleep(5.0)
                    continue

                # 监控线程池健康状态
                if not ThreadPoolManager.monitor_thread_pool_health():
                    logger.warning("HTTP线程池健康检查失败，将重新创建")
                    ThreadPoolManager.shutdown_http_thread_pool()
                    continue

                # 记录线程池状态
                ThreadPoolManager.log_pool_status()

                # 检查是否有严重的队列积压
                if hasattr(executor, '_work_queue') and executor._work_queue.qsize() > 10:
                    logger.warning(f"线程池队列开始积压: {executor._work_queue.qsize()} 个待处理任务")
                    time.sleep(0.5)  # 短暂延迟，让队列消化一下

                # 提交任务到线程池（添加小延迟以错开请求）
                futures = {}
                for i, processor in enumerate(data_processors):
                    try:
                        # 轻微延迟以减少同时请求的压力
                        if i > 0:
                            time.sleep(0.1)  # 100ms延迟
                        future = executor.submit(processor)
                        futures[future] = processor.__name__
                        # 增加任务计数
                        ThreadPoolManager.increment_task_count()
                    except Exception as e:
                        logger.error(f"提交任务失败 [{processor.__name__}]: {e}")
                        continue

                # 等待任务完成（增加超时时间以适应网络延迟，特别是GEONET等可能较慢的数据源）
                completed_count = 0
                failed_tasks = []
                task_start_times = {name: time.time() for name in futures.values()}
                try:
                    # 增加整体超时到60秒，给慢速数据源（如GEONET）更多时间
                    for future in as_completed(futures, timeout=60):
                        processor_name = futures[future]
                        completed_count += 1
                        task_duration = time.time() - task_start_times.get(processor_name, time.time())
                        try:
                            # GEONET需要更长的超时时间，因为数据量大且处理复杂
                            task_timeout = 50 if processor_name == 'process_geonet_data' else 30
                            result = future.result(timeout=task_timeout)
                            # 任务成功，重置连续失败计数
                            consecutive_failures = 0
                            if task_duration > 15:
                                logger.debug(f"HTTP数据源处理任务成功 [{processor_name}]，耗时: {task_duration:.2f}秒")
                        except (TimeoutError, FuturesTimeoutError):
                            logger.warning(f"HTTP数据源处理任务超时 [{processor_name}] (耗时 {task_duration:.2f}秒)，但继续等待其他任务")
                            failed_tasks.append(processor_name)
                            # 不立即取消，给任务更多完成机会
                        except Exception as e:
                            logger.error(f"HTTP数据源处理任务异常 [{processor_name}] (耗时 {task_duration:.2f}秒): {e}")
                            failed_tasks.append(processor_name)
                            consecutive_failures += 1

                except (TimeoutError, FuturesTimeoutError) as e:
                    unfinished_count = len(futures) - completed_count
                    unfinished_tasks = [futures[f] for f in futures if not f.done()]
                    
                    # 记录未完成任务已运行的时间
                    unfinished_durations = {}
                    for future in futures:
                        if not future.done():
                            task_name = futures.get(future, 'unknown')
                            duration = time.time() - task_start_times.get(task_name, time.time())
                            unfinished_durations[task_name] = duration
                    
                    # 检查是否有任务在超时后完成
                    if unfinished_count > 0:
                        logger.warning(f"HTTP线程池整体超时，{unfinished_count} 个任务未完成: {unfinished_tasks}")
                        if unfinished_durations:
                            logger.warning(f"未完成任务运行时间: {unfinished_durations}")
                        # 给未完成任务额外3秒时间，可能它们即将完成（特别是GEONET）
                        logger.warning("等待额外3秒...")
                        time.sleep(3.0)
                        
                        # 再次检查未完成的任务
                        still_unfinished = [futures[f] for f in futures if not f.done()]
                        if still_unfinished:
                            final_durations = {}
                            for future in futures:
                                if not future.done():
                                    task_name = futures.get(future, 'unknown')
                                    duration = time.time() - task_start_times.get(task_name, time.time())
                                    final_durations[task_name] = duration
                            
                            logger.error(f"仍有 {len(still_unfinished)} 个任务未完成: {still_unfinished}")
                            if final_durations:
                                logger.error(f"最终未完成任务运行时间: {final_durations}")
                            logger.error("取消这些任务")
                            # 只取消仍然未完成的任务
                            cancelled_count = 0
                            for future in futures:
                                if not future.done():
                                    try:
                                        future.cancel()
                                        cancelled_count += 1
                                    except Exception as cancel_e:
                                        logger.warning(f"取消任务失败 [{futures.get(future, 'unknown')}]: {cancel_e}")
                            logger.info(f"已取消 {cancelled_count} 个未完成的任务")
                            
                            # 如果有部分任务完成，不增加失败计数
                            if completed_count > 0:
                                logger.info(f"部分任务完成 ({completed_count}/{len(futures)})，不增加失败计数")
                            else:
                                consecutive_failures += 1
                        else:
                            logger.info(f"所有任务在额外等待后完成")
                    else:
                        logger.info(f"所有任务已完成")
                
                # 检查是否有任务失败但未超时
                if failed_tasks and completed_count == 0:
                    # 所有任务都失败了，增加失败计数
                    consecutive_failures += 1
                elif failed_tasks and completed_count > 0:
                    # 部分任务失败，记录但不增加失败计数（部分成功）
                    logger.warning(f"部分任务失败: {failed_tasks}，但 {completed_count} 个任务成功完成")

                # 检查连续失败次数
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"HTTP线程池连续失败 {consecutive_failures} 次，重新创建线程池")
                    ThreadPoolManager.shutdown_http_thread_pool()
                    consecutive_failures = 0
                    time.sleep(2.0)  # 等待更长时间后再重试
                    continue

            except Exception as e:
                # 检查是否是futures未完成的错误
                error_msg = str(e)
                if "futures unfinished" in error_msg or isinstance(e, (TimeoutError, FuturesTimeoutError)):
                    # 这是超时错误，应该已经被内部处理了，但如果没有，这里补充处理
                    if 'futures' in locals():
                        unfinished_count = sum(1 for f in futures if not f.done())
                        if unfinished_count > 0:
                            unfinished_tasks = [futures[f] for f in futures if not f.done()]
                            logger.warning(f"检测到未完成的futures ({unfinished_count}个): {unfinished_tasks}，尝试取消")
                            for future in futures:
                                if not future.done():
                                    try:
                                        future.cancel()
                                    except Exception:
                                        pass
                
                logger.error(f"HTTP数据源轮询循环出现严重错误: {e}", exc_info=True)
                consecutive_failures += 1
                # 根据连续失败次数调整等待时间
                sleep_time = min(1.0 + consecutive_failures * 0.5, 5.0)
                time.sleep(sleep_time)
            else:
                # 正常完成，重置失败计数
                consecutive_failures = 0
                time.sleep(get_poll_interval("ingv"))

    @staticmethod
    def initialize():
        """初始化程序"""
        LogManager.setup_logging()

        logger.info("=== 地震数据聚合服务启动 ===")
        logger.info(f"基础目录: {Config.BASE_DIR}")

        # 加载地名修正数据
        Utils.load_location_fix_data()

        # 加载FanStudio缓存数据
        CacheManager.load_fanstudio_cache()

        # 启动时仅执行一次：GeoNet / BMKG HTTP 全量拉取（运行期内网 WS；CWA 由 FanStudio cwalist_response 提供）
        from services.common.source_switches import is_list_enabled
        for _label, _fn, _sid in (
            ("GEONET", GEONETSource.initial_load, "geonet"),
            ("BMKG", BMKGSource.initial_load, "bmkg"),
        ):
            if not is_list_enabled(_sid):
                logger.info("程序初始化: %s 开关已关闭，跳过启动加载", _label)
                continue
            try:
                _fn()
            except Exception as e:
                logger.error(f"程序初始化阶段 {_label} 启动加载失败: {e}")

        # JMA(P2PQuake) 由 P2PQuake-WS 线程内先 HTTP 拉取历史，再连接 WebSocket

        # 注册退出时的清理函数
        import atexit
        atexit.register(ThreadPoolManager.shutdown_http_thread_pool)

    @staticmethod
    def start_threads():
        """启动所有工作线程"""
        from services.common.source_status import get_source_status_registry
        reg = get_source_status_registry()
        reg.register("ingv", "INGV 速报", "list")
        reg.register("p2p_jma", "P2P JMA", "list")

        # 启动 HTTP 数据源轮询线程（当前仅 INGV）
        logger.info("启动HTTP数据源轮询线程...")
        threading.Thread(target=MainHandler.update_loop, name="HTTP-Polling-Thread", daemon=True).start()

        if os.environ.get("FUSED_SHARED_FAN", "").strip() not in ("1", "true", "yes"):
            logger.info("启动FanStudio WebSocket线程...")
            threading.Thread(target=WebSocketHandler.process_fan_studio_ws, name="FanStudio-WebSocket-Thread", daemon=True).start()
        else:
            logger.info("Fan Studio 使用进程内共享连接，跳过 List 独立 WebSocket 线程")

        logger.info("内部 BMKG/GeoNet 经 event bus 接入（无需 1450 WS 线程）")

        logger.info("启动 P2PQuake 线程（先 HTTP 历史拉取，再 WebSocket code=551）...")
        threading.Thread(target=WebSocketHandler.process_p2pquake_ws, name="P2PQuake-WS-Thread", daemon=True).start()

    @staticmethod
    def start_servers():
        """启动Flask服务器"""
        from services.common.ports import LOCAL_BIND, get_list_port

        list_port = get_list_port()
        logger.info("Flask Web服务器已启动: %s/%s/earthquakes", LOCAL_BIND, list_port)
        threading.Thread(
            target=lambda: serve(app, host=LOCAL_BIND, port=list_port),
            name=f"Flask-{list_port}-Thread",
            daemon=True,
        ).start()

    @staticmethod
    def main():
        """主程序入口"""
        # 忽略SIGINT信号，由子线程处理
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # 初始化
        MainHandler.initialize()

        # 启动工作线程
        MainHandler.start_threads()

        # 启动服务器
        MainHandler.start_servers()

        try:
            _list_dir = os.path.dirname(os.path.abspath(__file__))
            if _list_dir not in sys.path:
                sys.path.insert(0, _list_dir)
            from management_ws import start_list_management_server
            start_list_management_server(sys.modules[__name__])
        except Exception as e:
            logger.error(f"启动 List 管理端口失败: {e}")

        while True:
            time.sleep(1)
