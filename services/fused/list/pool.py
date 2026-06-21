from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from services.fused.list import state


class ThreadPoolManager:
    """线程池管理器：防止内存泄漏和线程堵塞，支持动态调整线程数"""

    MIN_WORKERS = 4
    MAX_WORKERS = 15
    MAX_POOL_LIFETIME = 86400
    MAX_TASKS_PER_POOL = 100000
    HEALTH_CHECK_INTERVAL = 300
    MAX_QUEUE_SIZE = 20
    MEMORY_CHECK_INTERVAL = 900
    QUEUE_THRESHOLD_FOR_SCALE_UP = 5
    QUEUE_THRESHOLD_FOR_SCALE_DOWN = 2

    @staticmethod
    def get_http_thread_pool(requested_workers=None):
        with state._http_thread_pool_lock:
            current_time = time.time()
            target_workers = ThreadPoolManager._calculate_target_workers(requested_workers)
            need_restart = (
                state._http_thread_pool is None
                or state._http_thread_pool._shutdown
                or (state._http_thread_pool_task_count > ThreadPoolManager.MAX_TASKS_PER_POOL)
                or (state._http_thread_pool_current_workers != target_workers)
            )
            if need_restart:
                old_workers = state._http_thread_pool_current_workers if state._http_thread_pool else None
                ThreadPoolManager._shutdown_current_pool()
                try:
                    state._http_thread_pool = ThreadPoolExecutor(
                        max_workers=target_workers,
                        thread_name_prefix="HTTP-Processor",
                    )
                    state._http_thread_pool_created_time = current_time
                    state._http_thread_pool_task_count = 0
                    state._http_thread_pool_current_workers = target_workers
                    if old_workers is not None and old_workers != target_workers:
                        state.logger.info(f"动态调整HTTP线程池: {old_workers} -> {target_workers} 个线程")
                    else:
                        state.logger.info(f"创建新的HTTP线程池，工作线程数: {target_workers}")
                    ThreadPoolManager._start_health_check_thread()
                except Exception as e:
                    state.logger.error(f"创建HTTP线程池失败: {e}")
                    return None
        return state._http_thread_pool

    @staticmethod
    def _calculate_target_workers(requested_workers=None):
        if requested_workers is not None:
            return max(
                ThreadPoolManager.MIN_WORKERS,
                min(requested_workers, ThreadPoolManager.MAX_WORKERS),
            )
        base_workers = ThreadPoolManager.MIN_WORKERS
        if state._http_thread_pool is None:
            return base_workers
        if state._http_thread_pool_current_workers < ThreadPoolManager.MIN_WORKERS:
            state._http_thread_pool_current_workers = base_workers
        try:
            queue_size = 0
            if hasattr(state._http_thread_pool, "_work_queue"):
                queue_size = state._http_thread_pool._work_queue.qsize()
            active_threads = 0
            if hasattr(state._http_thread_pool, "_threads"):
                active_threads = len([t for t in state._http_thread_pool._threads if t.is_alive()])
            current_workers = state._http_thread_pool_current_workers
            if queue_size > ThreadPoolManager.QUEUE_THRESHOLD_FOR_SCALE_UP:
                if current_workers < ThreadPoolManager.MAX_WORKERS:
                    additional_workers = min(
                        (queue_size // ThreadPoolManager.QUEUE_THRESHOLD_FOR_SCALE_UP),
                        ThreadPoolManager.MAX_WORKERS - current_workers,
                    )
                    return min(current_workers + additional_workers, ThreadPoolManager.MAX_WORKERS)
            elif queue_size <= ThreadPoolManager.QUEUE_THRESHOLD_FOR_SCALE_DOWN:
                if current_workers > ThreadPoolManager.MIN_WORKERS and active_threads < current_workers * 0.5:
                    return max(current_workers - 1, ThreadPoolManager.MIN_WORKERS)
            return current_workers
        except Exception as e:
            state.logger.debug(f"计算目标线程数时出错: {e}，使用默认值 {base_workers}")
            return base_workers

    @staticmethod
    def _shutdown_current_pool():
        if state._http_thread_pool is not None and not state._http_thread_pool._shutdown:
            try:
                state._http_thread_pool.shutdown(wait=True, timeout=10)
                state.logger.info("HTTP线程池已关闭")
            except Exception as e:
                state.logger.error(f"关闭HTTP线程池时出错: {e}")
            finally:
                state._http_thread_pool = None
                state._http_thread_pool_created_time = None
                state._http_thread_pool_task_count = 0

    @staticmethod
    def increment_task_count():
        with state._http_thread_pool_lock:
            state._http_thread_pool_task_count += 1

    @staticmethod
    def _start_health_check_thread():
        if state._http_thread_pool_health_check_thread and state._http_thread_pool_health_check_thread.is_alive():
            return
        state._http_thread_pool_health_check_stop.clear()

        def health_check_worker():
            while not state._http_thread_pool_health_check_stop.is_set():
                try:
                    ThreadPoolManager._perform_health_check()
                except Exception as e:
                    state.logger.error(f"健康检查线程异常: {e}")
                if state._http_thread_pool_health_check_stop.wait(ThreadPoolManager.HEALTH_CHECK_INTERVAL):
                    break

        state._http_thread_pool_health_check_thread = threading.Thread(
            target=health_check_worker,
            name="ThreadPool-HealthCheck",
            daemon=True,
        )
        state._http_thread_pool_health_check_thread.start()
        state.logger.debug("线程池健康检查线程已启动")

    @staticmethod
    def _perform_health_check():
        try:
            ThreadPoolManager._check_memory_usage()
            ThreadPoolManager._check_pool_status()
            ThreadPoolManager._check_queue_backlog()
        except Exception as e:
            state.logger.error(f"执行健康检查时出错: {e}")

    @staticmethod
    def _check_memory_usage():
        try:
            import psutil

            memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
            if memory_mb > 500:
                state.logger.warning(f"内存使用过高: {memory_mb:.1f}MB")
            if memory_mb > 1024:
                state.logger.error(f"内存使用严重超标: {memory_mb:.1f}MB，强制重启线程池")
                ThreadPoolManager._force_restart_pool("内存使用严重超标")
        except ImportError:
            pass
        except Exception as e:
            state.logger.debug(f"检查内存使用时出错: {e}")

    @staticmethod
    def _check_pool_status():
        with state._http_thread_pool_lock:
            if state._http_thread_pool is None:
                return
            try:
                current_time = time.time()
                if (
                    state._http_thread_pool_created_time
                    and current_time - state._http_thread_pool_created_time > ThreadPoolManager.MAX_POOL_LIFETIME
                ):
                    state.logger.info(
                        f"线程池已运行 {int((current_time - state._http_thread_pool_created_time) / 3600)} 小时，建议考虑重启"
                    )
                elif state._http_thread_pool_task_count > ThreadPoolManager.MAX_TASKS_PER_POOL:
                    state.logger.info(f"线程池已处理 {state._http_thread_pool_task_count} 个任务，建议考虑重启")
                elif hasattr(state._http_thread_pool, "_threads"):
                    active_threads = len([t for t in state._http_thread_pool._threads if t.is_alive()])
                    if active_threads > state._http_thread_pool._max_workers * 2:
                        state.logger.warning(f"线程池线程数严重异常: {active_threads}，强制重启")
                        ThreadPoolManager._force_restart_pool("线程数严重异常")
            except Exception as e:
                state.logger.error(f"检查线程池状态时出错: {e}")

    @staticmethod
    def _check_queue_backlog():
        with state._http_thread_pool_lock:
            if state._http_thread_pool is None:
                return
            try:
                if hasattr(state._http_thread_pool, "_work_queue"):
                    queue_size = state._http_thread_pool._work_queue.qsize()
                    if queue_size > ThreadPoolManager.MAX_QUEUE_SIZE:
                        state.logger.warning(f"线程池队列积压: {queue_size} 个任务待处理")
                        if queue_size > ThreadPoolManager.MAX_QUEUE_SIZE * 5:
                            state.logger.error(f"线程池队列严重积压: {queue_size} 个任务，强制重启")
                            ThreadPoolManager._force_restart_pool("队列严重积压")
            except Exception as e:
                state.logger.debug(f"检查队列积压时出错: {e}")

    @staticmethod
    def _force_restart_pool(reason):
        state.logger.info(f"强制重启线程池，原因: {reason}")
        ThreadPoolManager._shutdown_current_pool()

    @staticmethod
    def shutdown_http_thread_pool():
        state._http_thread_pool_health_check_stop.set()
        ThreadPoolManager._shutdown_current_pool()
        if state._http_thread_pool_health_check_thread:
            state._http_thread_pool_health_check_thread.join(timeout=5)

    @staticmethod
    def monitor_thread_pool_health():
        with state._http_thread_pool_lock:
            if state._http_thread_pool is None:
                return False
            try:
                if state._http_thread_pool._shutdown:
                    state.logger.warning("HTTP线程池已关闭，将重新创建")
                    state._http_thread_pool = None
                    return False
                active_threads = (
                    len(state._http_thread_pool._threads)
                    if hasattr(state._http_thread_pool, "_threads")
                    else 0
                )
                if active_threads > state._http_thread_pool._max_workers * 1.2:
                    state.logger.warning(
                        f"HTTP线程池活动线程数偏高: {active_threads} > {state._http_thread_pool._max_workers}"
                    )
                return True
            except Exception as e:
                state.logger.error(f"检查HTTP线程池健康状态时出错: {e}")
                return False

    @staticmethod
    def log_pool_status():
        try:
            if state._http_thread_pool and hasattr(state._http_thread_pool, "_threads"):
                active_threads = len([t for t in state._http_thread_pool._threads if t.is_alive()])
                max_workers = state._http_thread_pool._max_workers
                queue_size = (
                    state._http_thread_pool._work_queue.qsize()
                    if hasattr(state._http_thread_pool, "_work_queue")
                    else 0
                )
                state.logger.debug(
                    f"线程池状态 - 活跃线程: {active_threads}/{max_workers} "
                    f"(当前配置: {state._http_thread_pool_current_workers}), 队列长度: {queue_size}"
                )
        except Exception as e:
            state.logger.debug(f"记录线程池状态时出错: {e}")
