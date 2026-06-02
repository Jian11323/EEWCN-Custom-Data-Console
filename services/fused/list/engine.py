"""
地震数据聚合服务器
整合多个地震数据源(JMA、CWA、CENC、GEONET、BMKG、FanStudio等)的地震信息
提供统一的API接口供前端调用
"""

# ============================================================================
# 导入模块
# ============================================================================
import requests
import threading
import time
from flask import Flask, jsonify
from datetime import datetime, timedelta
import re
import pytz
import json
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
import logging
import hashlib
import os
import sys
from collections import deque
from waitress import serve
import ssl
import signal

# 同步 WS 客户端：必须用 PyPI 的 websocket-client（提供 websocket._app.WebSocketApp）。
# 仅 `import websocket` 可能加载到错误的同名包或残留安装；优先从子模块导入。
WebSocketApp = None
try:
    from websocket._app import WebSocketApp
except ImportError:
    try:
        import websocket as _ws_sync_mod

        WebSocketApp = getattr(_ws_sync_mod, "WebSocketApp", None)
    except ImportError:
        WebSocketApp = None

if WebSocketApp is None:
    raise ImportError(
        "无法加载 websocket-client 的 WebSocketApp（Fan Studio / 内网机构 WS 需要）。\n"
        "请使用与本脚本相同的解释器安装依赖（Windows 上 `Python` 与 `pip` 可能不是同一环境）：\n"
        f"  {sys.executable} -m pip uninstall websocket -y\n"
        f"  {sys.executable} -m pip install websocket-client\n"
        "若脚本目录下有 websocket.py 或与包同名的文件夹，请改名以免遮挡 site-packages。"
    )

# ============================================================================
# 配置类
# ============================================================================
def _default_list_base_dir() -> str:
    env = os.environ.get("LIST_BASE_DIR")
    if env:
        return env
    if os.environ.get("FUSED_MODE", "").strip() in ("1", "true", "yes"):
        try:
            from services.common.paths import get_cache_dir
            return str(get_cache_dir() / "list")
        except ImportError:
            pass
    if os.name == "nt":
        return os.path.join(
            os.environ.get("APPDATA", os.path.expanduser("~")),
            "custom-datasource-console", "list",
        )
    return "/opt/eew/fused_list"


class Config:
    """配置类：包含所有配置项和常量"""

    # 基础目录配置
    BASE_DIR = _default_list_base_dir()
    LOG_DIR = os.environ.get("LIST_LOG_DIR", os.path.join(BASE_DIR, 'logs'))
    TRANSLATION_DIR = os.environ.get("LIST_TRANSLATION_DIR", os.path.join(BASE_DIR, 'translation_cache'))
    CACHE_DIR = os.environ.get("LIST_CACHE_DIR", os.path.join(BASE_DIR, 'cache'))

    # 日志保留天数
    LOG_RETENTION_DAYS = 7

    # 最大融合事件数量
    MAX_FUSED_EVENTS = 300

    # 熔断器配置
    CIRCUIT_BREAKER_THRESHOLD = 5
    BACKOFF_BASE_DELAY = 30
    BACKOFF_MAX_DELAY = 300

    # 阈值配置
    THRESHOLD_MAG = 4.5

    # 缓存配置
    MAX_CACHE_PER_SOURCE = 25

# 外部数据源API URL配置
API_URLS = {
    'JMA': "https://api.p2pquake.net/v2/history?codes=551&limit=30",  # 日本气象厅 - 获取全部数据
    'GEONET': "https://api.geonet.org.nz/quake?MMI=-1",  # 新西兰GeoNet - 获取全部数据
    'BMKG': "https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json",  # 印度尼西亚BMKG - 获取全部数据
    'INGV': "https://api.terraquakeapi.com/v1/earthquakes/recent?limit=50",  # 意大利国家地球物理与火山学研究 - TerraQuake API / recent
}

# FanStudio WebSocket配置
FAN_STUDIO_WS_URL_PRIMARY = "wss://ws.fanstudio.tech/all"  # 主服务器
FAN_STUDIO_WS_URL_BACKUP = "wss://ws.fanstudio.hk/all"  # 备用服务器

# P2PQuake WebSocket配置（仅地震情报 code 551）
P2PQUAKE_WS_URL = "wss://api.p2pquake.net/v2/ws"

# 内网聚合 WebSocket（BMKG / GeoNet，消息格式同 FanStudio 文档）
# 内网机构数据源改经 internal event bus 接入（不再使用 1450 WS）

_shared_fan_conn = None

FAN_STUDIO_SWITCH_CONFIG = {
    "current_url": FAN_STUDIO_WS_URL_PRIMARY,
    "is_using_backup": False,
    "primary_fail_count": 0,
    "primary_fail_threshold": 20,
    "primary_retry_interval": 30,
    "switch_to_backup_time": None,
    "backup_check_interval": 1800,
    "last_backup_check": None,
    "ws_instance": None,
    "last_event_times": {},
    "manual_lock": False,
    "lock": threading.Lock()
}

# 数据源名称映射表
SOURCE_NAMES = {
    "JMA": "日本气象厅",
    "CWA": "台湾气象署",
    "CENC": "中国地震台网中心",
    "NINGXIA": "宁夏地震局",
    "GUANGXI": "广西地震局",
    "SHANXI": "山西地震局",
    "BEIJING": "北京地震局",
    "YUNNAN": "云南地震局",
    "HKO": "香港天文台",
    "USGS": "美国地质调查局",
    "EMSC": "欧洲地中海地震中心",
    "BCSF": "法国中央地震研究所",
    "GFZ": "德国波茨坦地球科学研究中心",
    "USP": "巴西圣保罗大学地震信息",
    "KMA": "韩国气象厅",
    "FSSN": "FSSN",
    "GEONET": "新西兰GeoNet",
    "BMKG": "印度尼西亚气象气候和地球物理局",
    "INGV": "意大利国家地球物理与火山学研究",
}

# 百度翻译API配置
BAIDU_TRANSLATE_CONFIG = {
    'APP_ID': "20251017002477309",
    'SECRET_KEY': "xIeqBl_hNBbaXTevSkyl"
}

# 排除的数据源：非速报条目，或改由 EEW 融合（cwa-eew/kma-eew）处理
# cwa 速报仍由 cwa / cwalist_response 提供
EXCLUDED_SOURCES = [
    'weatheralarm', 'tsunami', 'cea', 'cea-pr', 'cwa-test', 'cwa-eew', 'kma-eew',
    'fssn-cmt', 'sichuan', 'sa',
]
# 无阈值数据源（显示名，与 LIST_NO_THRESHOLD_IDS 对应）
NO_THRESHOLD_SOURCES = [
    SOURCE_NAMES.get("CENC"),
    SOURCE_NAMES.get("JMA"),
    SOURCE_NAMES.get("CWA"),
    SOURCE_NAMES.get("YUNNAN"),
    "宁夏地震局",
    "广西地震局",
    "山西地震局",
    "北京地震局",
]

from services.common.source_filters import (
    LIST_FOREIGN_IDS,
    LIST_UNFILTERED_IDS,
    get_filter_registry,
    register_list_display_aliases,
    resolve_list_source_id,
)

_register_aliases = {}
for _k, _v in SOURCE_NAMES.items():
    _sid = _k if _k in ("JMA", "INGV") else _k.lower()
    _register_aliases[_v] = _sid
register_list_display_aliases(_register_aliases)

# ============================================================================
# 全局变量和存储
# ============================================================================
# Flask应用实例（历史 HTTP 8150）
app = Flask(__name__)

# 地震事件数据存储
fused_events = deque(maxlen=Config.MAX_FUSED_EVENTS)
event_dict_by_key = {}
fused_data_lock = threading.Lock()
# 兼容旧变量名
fused_events_no_threshold = fused_events
event_dict_by_key_no_threshold = event_dict_by_key
fused_data_lock_no_threshold = fused_data_lock
app_no_threshold = app

# 缓存相关
translation_cache = {}
location_regions = None
cache_lock = threading.Lock()

# FanStudio原始数据缓存（按数据源分别缓存）
fanstudio_raw_cache = {}  # {source_name: deque}
fanstudio_cache_lock = threading.Lock()

# 错误统计
error_stats = {
    "JMA": {"fetch_errors": 0, "parse_errors": 0, "last_error": None, "consecutive_failures": 0, "backoff_until": None},
    "GEONET": {"fetch_errors": 0, "parse_errors": 0, "last_error": None, "consecutive_failures": 0, "backoff_until": None},
    "BMKG": {"fetch_errors": 0, "parse_errors": 0, "last_error": None, "consecutive_failures": 0, "backoff_until": None},
    "INGV": {"fetch_errors": 0, "parse_errors": 0, "last_error": None, "consecutive_failures": 0, "backoff_until": None},
    "FAN_STUDIO": {"fetch_errors": 0, "parse_errors": 0, "last_error": None, "consecutive_failures": 0, "backoff_until": None},
    "P2PQUAKE": {"fetch_errors": 0, "parse_errors": 0, "last_error": None, "consecutive_failures": 0, "backoff_until": None},
    "INTERNAL_WS": {"fetch_errors": 0, "parse_errors": 0, "last_error": None, "consecutive_failures": 0, "backoff_until": None},
}

# 缓存状态
cache_state = {
    "JMA": {"id": "", "count": 0, "latest_id": "", "latest_time": "", "last_success": None},
    "GEONET": {"id": "", "count": 0, "latest_id": "", "latest_time": "", "last_success": None},
    "BMKG": {"id": "", "count": 0, "latest_id": "", "latest_time": "", "last_success": None},
    "INGV": {"id": "", "count": 0, "latest_id": "", "latest_time": "", "last_success": None},
}

# CENC/CWA/FSSN：不写入 fanstudio 原始磁盘/内存 deque；initial_all 不解析进融合；连接后 cenclist/cwalist/fssnlist 拉历史
FANSTUDIO_NO_RAW_CACHE_SOURCES = frozenset({'cenc', 'cwa', 'fssn'})

# FanStudio 列表命令：收到上一条响应并完成本地入库后，再间隔若干秒发下一条（定时器触发，不阻塞 WS）
def _fanstudio_list_cmd_gap_from_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


FANSTUDIO_LIST_CMD_GAP_SEC = _fanstudio_list_cmd_gap_from_env("FANSTUDIO_LIST_CMD_GAP_SEC", 3.0)
FANSTUDIO_LIST_CMD_GAP_PER_100 = _fanstudio_list_cmd_gap_from_env("FANSTUDIO_LIST_CMD_GAP_PER_100", 2.0)
FANSTUDIO_LIST_CMD_GAP_MAX_SEC = _fanstudio_list_cmd_gap_from_env("FANSTUDIO_LIST_CMD_GAP_MAX_SEC", 8.0)
FANSTUDIO_LIST_CMD_STATE = {"timer": None, "phase": 0}  # phase: 0 cenclist 已发 → 1 cwalist 已发 → 2 fssnlist 已发 → 3 完成
_fanstudio_list_cmd_lock = threading.Lock()

# 日志记录器
logger = None

# 全局线程池实例（重用以防止内存泄漏）
_http_thread_pool = None
_http_thread_pool_lock = threading.Lock()
_http_thread_pool_created_time = None
_http_thread_pool_task_count = 0
_http_thread_pool_health_check_thread = None
_http_thread_pool_health_check_stop = threading.Event()
_http_thread_pool_current_workers = 4  # 当前线程数，默认4

# ============================================================================
# 线程池管理器
# ============================================================================
class ThreadPoolManager:
    """线程池管理器：防止内存泄漏和线程堵塞，支持动态调整线程数"""

    # 线程池配置常量
    MIN_WORKERS = 4  # 最小线程数
    MAX_WORKERS = 15  # 最大线程数
    MAX_POOL_LIFETIME = 86400  # 24小时后检查是否需要重启（非强制）
    MAX_TASKS_PER_POOL = 100000  # 每个线程池最多处理的任务数（大幅增加）
    HEALTH_CHECK_INTERVAL = 300  # 每5分钟进行一次健康检查（增加频率）
    MAX_QUEUE_SIZE = 20  # 最大队列长度（降低阈值，更敏感）
    MEMORY_CHECK_INTERVAL = 900  # 每15分钟检查一次内存使用（增加频率）
    QUEUE_THRESHOLD_FOR_SCALE_UP = 5  # 队列长度超过此值时考虑增加线程
    QUEUE_THRESHOLD_FOR_SCALE_DOWN = 2  # 队列长度低于此值时考虑减少线程

    @staticmethod
    def get_http_thread_pool(requested_workers=None):
        """获取或创建HTTP线程池（动态调整线程数，最低4，最大15）"""
        global _http_thread_pool, _http_thread_pool_created_time, _http_thread_pool_task_count, _http_thread_pool_current_workers
        with _http_thread_pool_lock:
            current_time = time.time()

            # 计算需要的线程数（动态调整）
            target_workers = ThreadPoolManager._calculate_target_workers(requested_workers)

            # 检查是否需要重新创建线程池
            need_restart = (
                _http_thread_pool is None or
                _http_thread_pool._shutdown or
                (_http_thread_pool_task_count > ThreadPoolManager.MAX_TASKS_PER_POOL) or
                (_http_thread_pool_current_workers != target_workers)  # 线程数需要调整
            )

            if need_restart:
                old_workers = _http_thread_pool_current_workers if _http_thread_pool else None
                ThreadPoolManager._shutdown_current_pool()
                try:
                    _http_thread_pool = ThreadPoolExecutor(
                        max_workers=target_workers,
                        thread_name_prefix="HTTP-Processor"
                    )
                    _http_thread_pool_created_time = current_time
                    _http_thread_pool_task_count = 0
                    _http_thread_pool_current_workers = target_workers
                    if old_workers is not None and old_workers != target_workers:
                        logger.info(f"动态调整HTTP线程池: {old_workers} -> {target_workers} 个线程")
                    else:
                        logger.info(f"创建新的HTTP线程池，工作线程数: {target_workers}")

                    # 启动健康检查线程
                    ThreadPoolManager._start_health_check_thread()

                except Exception as e:
                    logger.error(f"创建HTTP线程池失败: {e}")
                    return None

        return _http_thread_pool

    @staticmethod
    def _calculate_target_workers(requested_workers=None):
        """计算目标线程数（动态调整，最低4，最大15）"""
        global _http_thread_pool, _http_thread_pool_current_workers
        
        # 如果明确指定了线程数，使用指定值（但限制在范围内）
        if requested_workers is not None:
            return max(ThreadPoolManager.MIN_WORKERS, 
                      min(requested_workers, ThreadPoolManager.MAX_WORKERS))
        
        # 动态计算：基于当前队列长度和负载
        base_workers = ThreadPoolManager.MIN_WORKERS  # 基础线程数
        
        if _http_thread_pool is None:
            return base_workers
        
        # 如果当前线程数未初始化，使用基础值
        if _http_thread_pool_current_workers < ThreadPoolManager.MIN_WORKERS:
            _http_thread_pool_current_workers = base_workers
        
        try:
            # 获取队列长度
            queue_size = 0
            if hasattr(_http_thread_pool, '_work_queue'):
                queue_size = _http_thread_pool._work_queue.qsize()
            
            # 获取当前活跃线程数
            active_threads = 0
            if hasattr(_http_thread_pool, '_threads'):
                active_threads = len([t for t in _http_thread_pool._threads if t.is_alive()])
            
            # 动态调整策略
            current_workers = _http_thread_pool_current_workers
            
            # 如果队列积压严重，增加线程
            if queue_size > ThreadPoolManager.QUEUE_THRESHOLD_FOR_SCALE_UP:
                # 队列积压，尝试增加线程（但不超过最大值）
                if current_workers < ThreadPoolManager.MAX_WORKERS:
                    # 根据队列长度计算需要增加的线程数
                    additional_workers = min(
                        (queue_size // ThreadPoolManager.QUEUE_THRESHOLD_FOR_SCALE_UP),
                        ThreadPoolManager.MAX_WORKERS - current_workers
                    )
                    new_workers = min(current_workers + additional_workers, ThreadPoolManager.MAX_WORKERS)
                    logger.debug(f"队列积压({queue_size})，建议增加线程: {current_workers} -> {new_workers}")
                    return new_workers
            
            # 如果队列很空且活跃线程少，减少线程（但不少于最小值）
            elif queue_size <= ThreadPoolManager.QUEUE_THRESHOLD_FOR_SCALE_DOWN:
                if current_workers > ThreadPoolManager.MIN_WORKERS and active_threads < current_workers * 0.5:
                    # 活跃线程少于当前线程数的一半，可以适当减少
                    new_workers = max(current_workers - 1, ThreadPoolManager.MIN_WORKERS)
                    logger.debug(f"队列空闲({queue_size})，活跃线程少({active_threads})，建议减少线程: {current_workers} -> {new_workers}")
                    return new_workers
            
            # 保持当前线程数
            return current_workers
            
        except Exception as e:
            logger.debug(f"计算目标线程数时出错: {e}，使用默认值 {base_workers}")
            return base_workers

    @staticmethod
    def _shutdown_current_pool():
        """关闭当前线程池"""
        global _http_thread_pool, _http_thread_pool_created_time, _http_thread_pool_task_count, _http_thread_pool_current_workers
        if _http_thread_pool is not None and not _http_thread_pool._shutdown:
            try:
                _http_thread_pool.shutdown(wait=True, timeout=10)
                logger.info("HTTP线程池已关闭")
            except Exception as e:
                logger.error(f"关闭HTTP线程池时出错: {e}")
            finally:
                _http_thread_pool = None
                _http_thread_pool_created_time = None
                _http_thread_pool_task_count = 0
                # 注意：不重置 _http_thread_pool_current_workers，因为新池会设置它

    @staticmethod
    def increment_task_count():
        """增加任务计数"""
        global _http_thread_pool_task_count
        with _http_thread_pool_lock:
            _http_thread_pool_task_count += 1

    @staticmethod
    def _start_health_check_thread():
        """启动健康检查线程"""
        global _http_thread_pool_health_check_thread, _http_thread_pool_health_check_stop

        if _http_thread_pool_health_check_thread and _http_thread_pool_health_check_thread.is_alive():
            return

        _http_thread_pool_health_check_stop.clear()

        def health_check_worker():
            while not _http_thread_pool_health_check_stop.is_set():
                try:
                    ThreadPoolManager._perform_health_check()
                except Exception as e:
                    logger.error(f"健康检查线程异常: {e}")

                # 等待下次检查或停止信号
                if _http_thread_pool_health_check_stop.wait(ThreadPoolManager.HEALTH_CHECK_INTERVAL):
                    break

        _http_thread_pool_health_check_thread = threading.Thread(
            target=health_check_worker,
            name="ThreadPool-HealthCheck",
            daemon=True
        )
        _http_thread_pool_health_check_thread.start()
        logger.debug("线程池健康检查线程已启动")

    @staticmethod
    def _perform_health_check():
        """执行健康检查"""
        try:
            # 检查内存使用
            ThreadPoolManager._check_memory_usage()

            # 检查线程池状态
            ThreadPoolManager._check_pool_status()

            # 检查队列积压
            ThreadPoolManager._check_queue_backlog()

        except Exception as e:
            logger.error(f"执行健康检查时出错: {e}")

    @staticmethod
    def _check_memory_usage():
        """检查内存使用情况"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            # 如果内存使用超过500MB，记录警告
            if memory_mb > 500:
                logger.warning(f"内存使用过高: {memory_mb:.1f}MB")

            # 如果内存使用超过1GB，强制重启线程池
            if memory_mb > 1024:
                logger.error(f"内存使用严重超标: {memory_mb:.1f}MB，强制重启线程池")
                ThreadPoolManager._force_restart_pool("内存使用严重超标")

        except ImportError:
            # 如果没有psutil，跳过内存检查
            pass
        except Exception as e:
            logger.debug(f"检查内存使用时出错: {e}")

    @staticmethod
    def _check_pool_status():
        """检查线程池状态"""
        global _http_thread_pool, _http_thread_pool_created_time, _http_thread_pool_task_count
        with _http_thread_pool_lock:
            if _http_thread_pool is None:
                return

            try:
                # 检查生命周期（仅记录日志，不强制重启）
                current_time = time.time()
                if _http_thread_pool_created_time and current_time - _http_thread_pool_created_time > ThreadPoolManager.MAX_POOL_LIFETIME:
                    logger.info(f"线程池已运行 {int((current_time - _http_thread_pool_created_time) / 3600)} 小时，建议考虑重启")

                # 检查任务数量（仅记录日志，不强制重启）
                elif _http_thread_pool_task_count > ThreadPoolManager.MAX_TASKS_PER_POOL:
                    logger.info(f"线程池已处理 {_http_thread_pool_task_count} 个任务，建议考虑重启")

                # 检查线程状态（严重异常时才重启）
                elif hasattr(_http_thread_pool, '_threads'):
                    active_threads = len([t for t in _http_thread_pool._threads if t.is_alive()])
                    if active_threads > _http_thread_pool._max_workers * 2:  # 线程数严重异常
                        logger.warning(f"线程池线程数严重异常: {active_threads}，强制重启")
                        ThreadPoolManager._force_restart_pool("线程数严重异常")

            except Exception as e:
                logger.error(f"检查线程池状态时出错: {e}")

    @staticmethod
    def _check_queue_backlog():
        """检查队列积压情况"""
        global _http_thread_pool
        with _http_thread_pool_lock:
            if _http_thread_pool is None:
                return

            try:
                # 检查待处理任务队列
                if hasattr(_http_thread_pool, '_work_queue'):
                    queue_size = _http_thread_pool._work_queue.qsize()
                    if queue_size > ThreadPoolManager.MAX_QUEUE_SIZE:
                        logger.warning(f"线程池队列积压: {queue_size} 个任务待处理")
                        # 如果队列积压严重过多，才强制重启
                        if queue_size > ThreadPoolManager.MAX_QUEUE_SIZE * 5:
                            logger.error(f"线程池队列严重积压: {queue_size} 个任务，强制重启")
                            ThreadPoolManager._force_restart_pool("队列严重积压")

            except Exception as e:
                logger.debug(f"检查队列积压时出错: {e}")

    @staticmethod
    def _force_restart_pool(reason):
        """强制重启线程池"""
        logger.info(f"强制重启线程池，原因: {reason}")
        ThreadPoolManager._shutdown_current_pool()

    @staticmethod
    def shutdown_http_thread_pool():
        """关闭HTTP线程池"""
        global _http_thread_pool_health_check_stop
        _http_thread_pool_health_check_stop.set()

        ThreadPoolManager._shutdown_current_pool()

        if _http_thread_pool_health_check_thread:
            _http_thread_pool_health_check_thread.join(timeout=5)

    @staticmethod
    def monitor_thread_pool_health():
        """监控线程池健康状态（简化版，用于轮询循环中）"""
        global _http_thread_pool
        with _http_thread_pool_lock:
            if _http_thread_pool is None:
                return False

            try:
                # 快速检查线程池是否正常
                if _http_thread_pool._shutdown:
                    logger.warning("HTTP线程池已关闭，将重新创建")
                    _http_thread_pool = None
                    return False

                # 检查活动线程数
                active_threads = len(_http_thread_pool._threads) if hasattr(_http_thread_pool, '_threads') else 0
                if active_threads > _http_thread_pool._max_workers * 1.2:  # 稍微放宽限制
                    logger.warning(f"HTTP线程池活动线程数偏高: {active_threads} > {_http_thread_pool._max_workers}")

                return True
            except Exception as e:
                logger.error(f"检查HTTP线程池健康状态时出错: {e}")
                return False

    @staticmethod
    def log_pool_status():
        """记录线程池当前状态"""
        global _http_thread_pool, _http_thread_pool_current_workers
        try:
            if _http_thread_pool and hasattr(_http_thread_pool, '_threads'):
                active_threads = len([t for t in _http_thread_pool._threads if t.is_alive()])
                max_workers = _http_thread_pool._max_workers
                queue_size = _http_thread_pool._work_queue.qsize() if hasattr(_http_thread_pool, '_work_queue') else 0
                logger.debug(f"线程池状态 - 活跃线程: {active_threads}/{max_workers} (当前配置: {_http_thread_pool_current_workers}), 队列长度: {queue_size}")
        except Exception as e:
            logger.debug(f"记录线程池状态时出错: {e}")

# ============================================================================
# 日志管理
# ============================================================================
class LogManager:
    """日志管理器"""
    
    @staticmethod
    def setup_logging():
        """配置日志记录器"""
        global logger

        from services.common.logging_setup import Utf8StdoutHandler, ensure_stdio_utf8

        ensure_stdio_utf8()

        # 创建日志目录
        os.makedirs(Config.LOG_DIR, exist_ok=True)

        # 配置日志记录器
        console = Utf8StdoutHandler()
        console.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
            )
        )
        logging.basicConfig(
            level=logging.INFO,
            handlers=[
                console,
                logging.FileHandler(
                    os.path.join(Config.LOG_DIR, f'earthquake_api_{datetime.now().strftime("%Y-%m-%d")}.log'),
                    encoding='utf-8'
                )
            ]
        )
        logger = logging.getLogger(__name__)
        return logger

# ============================================================================
# 翻译服务
# ============================================================================
class TranslationService:
    """翻译服务"""
    
    @staticmethod
    def translate_location(location, lat=None, lon=None, source=None):
        """翻译地名"""
        # JMA、CWA 和 HKO 直接返回原始地名
        if source in ['JMA', 'CWA', 'HKO']:
            return location
        
        # 如果有经纬度，先尝试 FE fix
        if lat is not None and lon is not None:
            try:
                fe_location = Utils.get_fixed_location(lat, lon)
                if fe_location:
                    return fe_location
            except Exception:
                pass
        
        # 检查缓存
        with cache_lock:
            if location in translation_cache:
                return translation_cache[location]

        # 如果没有翻译配置，直接返回
        if not BAIDU_TRANSLATE_CONFIG['APP_ID'] or not BAIDU_TRANSLATE_CONFIG['SECRET_KEY']:
            return location

        # 如果已经是简体中文，直接返回
        if re.search(r'[\u4e00-\u9fa5]', location) and not TranslationService.contains_traditional_chinese(location):
            return location

        # 使用百度翻译API
        api_url = 'http://api.fanyi.baidu.com/api/trans/vip/translate'
        salt = str(time.time())
        sign = hashlib.md5((BAIDU_TRANSLATE_CONFIG['APP_ID'] + location + salt + BAIDU_TRANSLATE_CONFIG['SECRET_KEY']).encode('utf-8')).hexdigest()
        params = {
            'q': location,
            'from': 'auto',
            'to': 'zh',
            'appid': BAIDU_TRANSLATE_CONFIG['APP_ID'],
            'salt': salt,
            'sign': sign
        }

        try:
            response = requests.get(api_url, params=params, timeout=5)
            result = response.json()
            if 'trans_result' in result and result['trans_result']:
                translated_text = result['trans_result'][0]['dst']
                with cache_lock:
                    translation_cache[location] = translated_text
                return translated_text
        except Exception:
            pass
        return location
    
    @staticmethod
    def contains_traditional_chinese(text):
        """检查是否包含繁体中文"""
        try:
            text.encode('gb2312')
            return False
        except UnicodeEncodeError:
            return True
    
    @staticmethod
    def convert_traditional_to_simplified(location):
        """将繁体中文转换为简体中文"""
        return TranslationService.translate_location(location)

# ============================================================================
# 缓存管理器
# ============================================================================
class CacheManager:
    """缓存管理器"""
    
    @staticmethod
    def save_fanstudio_cache():
        """保存FanStudio缓存"""
        try:
            if not os.path.exists(Config.CACHE_DIR):
                os.makedirs(Config.CACHE_DIR, exist_ok=True)

            with fanstudio_cache_lock:
                for source, cache_deque in fanstudio_raw_cache.items():
                    if not cache_deque:
                        continue

                    cache_file = os.path.join(Config.CACHE_DIR, f'{source}_cache.json')
                    cache_list = list(cache_deque)

                    try:
                        with open(cache_file, 'w', encoding='utf-8') as f:
                            json.dump(cache_list, f, ensure_ascii=False, indent=2)
                        logger.debug(f"FanStudio: 保存 {source} 缓存 {len(cache_list)} 条原始数据到 {cache_file}")
                    except Exception as e:
                        logger.error(f"FanStudio: 保存 {source} 缓存文件失败: {e}，文件路径: {cache_file}")

        except Exception as e:
            logger.error(f"FanStudio: 保存缓存文件失败: {e}")
    
    @staticmethod
    def load_fanstudio_cache():
        """加载FanStudio缓存"""
        try:
            if not os.path.exists(Config.CACHE_DIR):
                logger.warning(f"FanStudio: 缓存目录不存在: {Config.CACHE_DIR}，跳过加载")
                return

            total_loaded = 0
            events_to_push = []

            with fanstudio_cache_lock:
                fanstudio_raw_cache.clear()

                # 遍历所有数据源的缓存文件（含 FanStudio 与内网 WS 的 bmkg/geonet）
                _parsers_all = dict(FAN_STUDIO_PARSERS)
                _parsers_all.update(INTERNAL_WS_PARSERS)

                for source_name in _parsers_all.keys():
                    if source_name in FANSTUDIO_NO_RAW_CACHE_SOURCES:
                        continue
                    cache_file = os.path.join(Config.CACHE_DIR, f'{source_name}_cache.json')

                    if not os.path.exists(cache_file):
                        continue

                    try:
                        file_size = os.path.getsize(cache_file)
                        if file_size == 0:
                            continue

                        with open(cache_file, 'r', encoding='utf-8') as f:
                            raw_data_list = json.load(f)

                        if not isinstance(raw_data_list, list) or not raw_data_list:
                            continue

                        # 保存原始数据到内存缓存
                        cache_deque = deque(raw_data_list, maxlen=Config.MAX_CACHE_PER_SOURCE)
                        fanstudio_raw_cache[source_name] = cache_deque

                        # 解析原始数据
                        parser = _parsers_all[source_name]
                        parsed_events = []
                        for raw_data in raw_data_list:
                            try:
                                event = parser(raw_data)
                                if event:
                                    parsed_events.append(event)
                            except Exception as e:
                                logger.warning(f"FanStudio: 解析 {source_name} 缓存数据失败: {e}")
                                continue

                        # 按时间排序并限制数量
                        sorted_events = sorted(parsed_events, key=lambda x: Utils.parse_time(x.get("O_TIME")) or datetime.min, reverse=True)
                        sorted_events = sorted_events[:Config.MAX_CACHE_PER_SOURCE]

                        logger.debug(f"FanStudio: 从 {source_name}_cache.json 加载并解析 {len(sorted_events)} 条数据")
                        total_loaded += len(sorted_events)
                        events_to_push.extend(sorted_events)

                    except json.JSONDecodeError as e:
                        logger.error(f"FanStudio: {source_name} 缓存文件JSON解析失败: {e}")
                    except Exception as e:
                        logger.error(f"FanStudio: 加载 {source_name} 缓存文件失败: {e}")

            if total_loaded > 0:
                logger.info(f"FanStudio: 成功加载并解析 {total_loaded} 条缓存数据")
                FusionHandler.add_events_to_fused_list(events_to_push, bulk_quiet_cenc_logs=True)
            else:
                logger.info("FanStudio: 未找到有效的缓存数据")

        except Exception as e:
            logger.error(f"FanStudio: 加载缓存失败: {e}")

# ============================================================================
# 工具函数
# ============================================================================
class Utils:
    """工具函数类：包含所有通用工具函数"""

    @staticmethod
    def load_location_fix_data():

        global location_regions
        try:
            from services.common.regions import get_fe_fix_regions
            regions = get_fe_fix_regions()
            if regions:
                location_regions = regions
                logger.info(f"已加载 {len(location_regions)} 个地名修正区域规则 (data/fe_fix_region_data.json)")
                return
            logger.warning("未找到地名修正文件 data/fe_fix_region_data.json，将回退到API翻译。")
        except Exception as e:
            location_regions = None
            logger.error(f"加载地名修正文件失败: {e}")

    @staticmethod
    def get_fixed_location(lat, lon):
        if location_regions is None:
            return None

        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            return None

        best_name = None
        best_area = None

        for region in location_regions:
            try:
                lat_min = region.get('lat_min', -90)
                lat_max = region.get('lat_max', 90)
                lon_min = region.get('lon_min', -180)
                lon_max = region.get('lon_max', 180)

                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    area = (lat_max - lat_min) * (lon_max - lon_min)
                    if best_area is None or area < best_area:
                        best_area = area
                        best_name = region.get('name', '')
            except Exception:
                continue

        return best_name or None


    @staticmethod
    def get_intensity_string(scale_value):

        return {10:"1", 20:"2", 30:"3", 40:"4", 45:"5-", 50:"5+", 55:"6-", 60:"6+", 70:"7"}.get(scale_value, str(scale_value))

    @staticmethod
    def parse_time(time_str):

        if not time_str:
            return None

        formats = ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]

        for fmt in formats:
            try:
                return datetime.strptime(str(time_str), fmt)
            except (ValueError, TypeError):
                continue

        try:
            if str(time_str).endswith('Z'):
                time_str = str(time_str)[:-1] + '+00:00'
            return datetime.fromisoformat(time_str)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def get_event_key(event):

        try:
            if "EVENT_ID" in event and event["EVENT_ID"]:
                return (event["SOURCE"], event["EVENT_ID"])

            event_time = Utils.parse_time(event["O_TIME"])
            if not event_time:
                return None
            return (event_time.replace(second=0, microsecond=0), round(float(event["EPI_LAT"]), 1), round(float(event["EPI_LON"]), 1))
        except (ValueError, KeyError, TypeError):
            return None

    @staticmethod
    def check_circuit_breaker(source):

        if source not in error_stats:
            return False
        stats = error_stats[source]
        if stats["backoff_until"] and datetime.now() < stats["backoff_until"]:
            return True
        return False

    @staticmethod
    def handle_fetch_error(source, error):

        if source not in error_stats:
            return
        stats = error_stats[source]
        stats["fetch_errors"] += 1
        stats["last_error"] = str(error)
        stats["consecutive_failures"] += 1

        backoff_delay = min(Config.BACKOFF_BASE_DELAY * (2 ** (stats["consecutive_failures"] - 1)), Config.BACKOFF_MAX_DELAY)
        stats["backoff_until"] = datetime.now() + timedelta(seconds=backoff_delay)
        logger.warning(f"{source}: 获取失败 (连续 {stats['consecutive_failures']} 次)，将退避 {backoff_delay} 秒")

    @staticmethod
    def reset_circuit_breaker(source):

        if source not in error_stats:
            return
        stats = error_stats[source]
        if stats["consecutive_failures"] > 0:
            stats["consecutive_failures"] = 0
            stats["backoff_until"] = None


# ============================================================================
# 数据源基类
# ============================================================================
class DataSourceBase:
    """数据源基类：提供通用的处理流程"""

    @staticmethod
    def process(source_name, fetch_func, parse_func):
        """通用处理流程"""
        if Utils.check_circuit_breaker(source_name):
            return
        
        try:
            data = fetch_func()
            if data:
                parsed_data = parse_func(data if isinstance(data, list) else [data])
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                Utils.reset_circuit_breaker(source_name)
        except Exception as e:
            logger.error(f"处理 {source_name} 数据时发生错误: {e}")
            Utils.handle_fetch_error(source_name, e)

# ============================================================================
# JMA数据源
# ============================================================================
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
class GEONETSource:
    """新西兰GeoNet数据源"""
    
    @staticmethod
    def process():
        """处理GeoNet数据源"""
        source_name = "GEONET"
        start_time = time.time()
        
        if Utils.check_circuit_breaker(source_name):
            return

        try:
            fetch_start = time.time()
            data = GEONETSource.fetch()
            fetch_time = time.time() - fetch_start
            if fetch_time > 5:
                logger.debug(f"[{source_name}] 数据获取耗时: {fetch_time:.2f}秒")
            
            if data:
                parse_start = time.time()
                parsed_data = GEONETSource.parse(data if isinstance(data, list) else [data])
                parse_time = time.time() - parse_start
                if parse_time > 5:
                    logger.debug(f"[{source_name}] 数据解析耗时: {parse_time:.2f}秒，处理了 {len(data)} 条数据")
                
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                Utils.reset_circuit_breaker(source_name)
            
            total_time = time.time() - start_time
            if total_time > 10:
                logger.debug(f"[{source_name}] 总处理耗时: {total_time:.2f}秒")
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"处理 {source_name} 数据时发生错误 (耗时 {total_time:.2f}秒): {e}")
            Utils.handle_fetch_error(source_name, e)

    @staticmethod
    def initial_load():
        """仅在启动时执行一次：HTTP 全量拉取 GeoNet 并写入融合列表"""
        source_name = "GEONET"
        logger.info("GEONET: 启动初始化开始（HTTP 一次性拉取）")
        try:
            if Utils.check_circuit_breaker(source_name):
                logger.warning("GEONET: 熔断器已打开，跳过启动初始化")
                return
            headers = {"Accept": "application/vnd.geo+json;version=2", "Accept-Encoding": "gzip"}
            response = requests.get(API_URLS['GEONET'], headers=headers, timeout=35)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or "features" not in data:
                logger.info("GEONET: 启动初始化响应无 features")
                return
            event_list = [f for f in data.get("features", []) if isinstance(f, dict) and "properties" in f]
            if not event_list:
                logger.info("GEONET: 启动初始化无地震要素")
                return
            parsed_data = GEONETSource.parse(event_list)
            if parsed_data:
                FusionHandler.add_events_to_fused_list(parsed_data)
            first_id = event_list[0].get("properties", {}).get("publicID", "")
            last_id = event_list[-1].get("properties", {}).get("publicID", "")
            data_id = first_id if event_list[0] is event_list[-1] else f"{first_id}_{last_id}"
            cache_state["GEONET"]["id"] = data_id
            cache_state["GEONET"]["count"] = len(event_list)
            cache_state["GEONET"]["latest_id"] = first_id
            cache_state["GEONET"]["latest_time"] = str(event_list[0].get("properties", {}).get("time", ""))
            cache_state["GEONET"]["last_success"] = datetime.now()
            Utils.reset_circuit_breaker(source_name)
            logger.info(f"GEONET: 启动初始化完成，加载 {len(parsed_data or [])} 条地震事件")
        except Exception as e:
            logger.error(f"GEONET: 启动初始化失败: {e}")
            Utils.handle_fetch_error(source_name, e)
    
    @staticmethod
    def fetch():
        """获取GeoNet全部数据并全部解析"""
        def extract_events(data):
            if not isinstance(data, dict) or "features" not in data:
                return []
            features = data.get("features", [])
            return [f for f in features if isinstance(f, dict) and "properties" in f]

        def get_id(first, last):
            first_id = first.get("properties", {}).get("publicID", "")
            return first_id if first is last else f"{first_id}_{last.get('properties', {}).get('publicID', '')}"

        def get_time(event):
            return event.get("properties", {}).get("time", "")

        headers = {"Accept": "application/vnd.geo+json;version=2", "Accept-Encoding": "gzip"}
        return GEONETSource._fetch_and_check_cache(API_URLS['GEONET'], "GEONET", headers, extract_events, get_id, get_time)
    
    @staticmethod
    def _fetch_and_check_cache(url, source_key, headers=None, extract_events_fn=None, get_id_fn=None, get_time_fn=None):
        """通用数据获取和缓存检查函数"""
        try:
            timeout = 35 if source_key == "GEONET" else 10
            fetch_start = time.time()
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            fetch_time = time.time() - fetch_start
            
            parse_start = time.time()
            data = response.json()
            parse_time = time.time() - parse_start
            
            if source_key == "GEONET" and (fetch_time > 10 or parse_time > 5):
                logger.debug(f"[{source_key}] HTTP请求耗时: {fetch_time:.2f}秒, JSON解析耗时: {parse_time:.2f}秒")

            event_list = extract_events_fn(data) if extract_events_fn else []
            if not event_list or len(event_list) == 0:
                return None

            event_count = len(event_list)
            data_id = get_id_fn(event_list[0], event_list[-1]) if get_id_fn else ""
            latest_event_id = get_id_fn(event_list[0], event_list[0]) if get_id_fn else ""
            latest_event_time = get_time_fn(event_list[0]) if get_time_fn else ""

            cache = cache_state[source_key]
            if (data_id == cache["id"] and event_count == cache["count"] and
                latest_event_id == cache["latest_id"] and str(latest_event_time) == cache["latest_time"]):
                return None

            cache["id"] = data_id
            cache["count"] = event_count
            cache["latest_id"] = latest_event_id
            cache["latest_time"] = str(latest_event_time)
            cache["last_success"] = datetime.now()

            return event_list
        except Exception:
            return None
    
    @staticmethod
    def parse(data):
        """解析GeoNet数据"""
        result = []
        for feature in data:
            try:
                properties = feature.get("properties", {})
                if properties.get("quality") == "deleted":
                    continue

                # 获取震级
                mag = properties.get("magnitude")
                if mag is None:
                    continue
                try:
                    mag = float(mag)
                except (ValueError, TypeError):
                    continue
                if mag <= 0:
                    continue

                # 获取时间并转换为北京时间
                event_time_str = properties.get("time")
                if not event_time_str:
                    continue

                event_time = Utils.parse_time(event_time_str)
                if not event_time:
                    continue

                if event_time.tzinfo is None:
                    event_time = pytz.UTC.localize(event_time)
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

                # 获取坐标
                try:
                    geometry = feature.get("geometry", {})
                    if isinstance(geometry, dict) and "coordinates" in geometry:
                        coords = geometry.get("coordinates", [])
                        if len(coords) >= 2:
                            lat = float(coords[1])
                            lon = float(coords[0])
                        else:
                            lat, lon = 0.0, 0.0
                    else:
                        lat, lon = 0.0, 0.0
                except (ValueError, TypeError, IndexError):
                    lat, lon = 0.0, 0.0

                # 获取深度
                try:
                    depth = float(properties.get("depth", 0)) if properties.get("depth") is not None else 0.0
                except (ValueError, TypeError):
                    depth = 0.0

                # 获取位置信息
                locality = properties.get("locality", "未知地区")
                if not locality or not isinstance(locality, str):
                    locality = "未知地区"

                try:
                    location = locality.strip()
                    location = re.sub(r'^\d+\s*km\s+(north|south|east|west|north-east|north-west|south-east|south-west)\s+of\s+', '', location, flags=re.IGNORECASE).strip()
                    if not location:
                        location = locality.strip()
                    location = TranslationService.translate_location(location, lat=lat, lon=lon, source='GEONET')
                except Exception:
                    location = TranslationService.translate_location(locality, lat=lat, lon=lon, source='GEONET')

                # 获取烈度
                mmi = properties.get("mmi")
                if mmi is None:
                    intensity = ""
                else:
                    try:
                        mmi_value = float(mmi)
                        intensity = str(int(mmi_value)) if mmi_value >= 0 else ""
                    except (ValueError, TypeError):
                        intensity = str(mmi) if mmi else ""

                # 获取事件ID
                public_id = properties.get("publicID", "")
                if not public_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    public_id = f"geonet_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"

                # 构建事件字典
                event = {
                    "id": public_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(lat),
                    "EPI_LON": str(lon),
                    "EPI_DEPTH": round(depth),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M",
                    "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (GeoNet)",
                    "epicenter_tts": location,
                    "INTENSITY": intensity,
                    "SOURCE": SOURCE_NAMES.get("GEONET", "GEONET"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("GEONET", "GEONET"),
                    "EVENT_ID": public_id,
                    "infoTypeName": "地震报告"
                }
                result.append(event)
            except Exception:
                continue
        return result

# ============================================================================
# BMKG数据源
# ============================================================================
class BMKGSource:
    """印度尼西亚BMKG数据源"""
    
    @staticmethod
    def process():
        """处理BMKG数据源"""
        source_name = "BMKG"
        if Utils.check_circuit_breaker(source_name):
            return

        try:
            data = BMKGSource.fetch()
            if data:
                parsed_data = BMKGSource.parse(data if isinstance(data, list) else [data])
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                Utils.reset_circuit_breaker(source_name)
        except Exception as e:
            logger.error(f"处理 {source_name} 数据时发生错误: {e}")
            Utils.handle_fetch_error(source_name, e)

    @staticmethod
    def initial_load():
        """仅在启动时执行一次：HTTP 全量拉取 BMKG 并写入融合列表"""
        source_name = "BMKG"
        logger.info("BMKG: 启动初始化开始（HTTP 一次性拉取）")
        try:
            if Utils.check_circuit_breaker(source_name):
                logger.warning("BMKG: 熔断器已打开，跳过启动初始化")
                return
            response = requests.get(API_URLS['BMKG'], timeout=10)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or "Infogempa" not in data:
                logger.info("BMKG: 启动初始化响应格式异常")
                return
            event_list = data.get("Infogempa", {}).get("gempa", []) if isinstance(data.get("Infogempa"), dict) else []
            if not event_list:
                logger.info("BMKG: 启动初始化无列表数据")
                return
            parsed_data = BMKGSource.parse(event_list)
            if parsed_data:
                FusionHandler.add_events_to_fused_list(parsed_data)
            first_dt = event_list[0].get("DateTime", "")
            last_dt = event_list[-1].get("DateTime", "")
            data_id = first_dt if event_list[0] is event_list[-1] else f"{first_dt}_{last_dt}"
            cache_state["BMKG"]["id"] = data_id
            cache_state["BMKG"]["count"] = len(event_list)
            cache_state["BMKG"]["latest_id"] = first_dt
            cache_state["BMKG"]["latest_time"] = str(first_dt)
            cache_state["BMKG"]["last_success"] = datetime.now()
            Utils.reset_circuit_breaker(source_name)
            logger.info(f"BMKG: 启动初始化完成，加载 {len(parsed_data or [])} 条地震事件")
        except Exception as e:
            logger.error(f"BMKG: 启动初始化失败: {e}")
            Utils.handle_fetch_error(source_name, e)
    
    @staticmethod
    def fetch():
        """获取BMKG全部数据并全部解析"""
        def extract_events(data):
            if not isinstance(data, dict) or "Infogempa" not in data:
                return []
            infogempa = data.get("Infogempa", {})
            return infogempa.get("gempa", []) if isinstance(infogempa, dict) else []

        def get_id(first, last):
            first_dt = first.get("DateTime", "")
            return first_dt if first is last else f"{first_dt}_{last.get('DateTime', '')}"

        def get_time(event):
            return event.get("DateTime", "")

        return BMKGSource._fetch_and_check_cache(API_URLS['BMKG'], "BMKG", None, extract_events, get_id, get_time)
    
    @staticmethod
    def _fetch_and_check_cache(url, source_key, headers=None, extract_events_fn=None, get_id_fn=None, get_time_fn=None):
        """通用数据获取和缓存检查函数"""
        try:
            timeout = 10
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            event_list = extract_events_fn(data) if extract_events_fn else []
            if not event_list or len(event_list) == 0:
                return None

            event_count = len(event_list)
            data_id = get_id_fn(event_list[0], event_list[-1]) if get_id_fn else ""
            latest_event_id = get_id_fn(event_list[0], event_list[0]) if get_id_fn else ""
            latest_event_time = get_time_fn(event_list[0]) if get_time_fn else ""

            cache = cache_state[source_key]
            if (data_id == cache["id"] and event_count == cache["count"] and
                latest_event_id == cache["latest_id"] and str(latest_event_time) == cache["latest_time"]):
                return None

            cache["id"] = data_id
            cache["count"] = event_count
            cache["latest_id"] = latest_event_id
            cache["latest_time"] = str(latest_event_time)
            cache["last_success"] = datetime.now()

            return event_list
        except Exception:
            return None
    
    @staticmethod
    def parse(data):
        """解析BMKG数据"""
        result = []
        for item in data:
            try:
                # 获取震级
                magnitude_str = item.get("Magnitude", "")
                if not magnitude_str:
                    continue
                try:
                    mag = float(magnitude_str)
                except (ValueError, TypeError):
                    continue
                if mag <= 0:
                    continue

                # 获取时间并转换为北京时间
                event_time_str = item.get("DateTime", "")
                if not event_time_str:
                    tanggal = item.get("Tanggal", "")
                    jam = item.get("Jam", "")
                    if not (tanggal and jam):
                        continue

                    try:
                        indonesian_months = {
                            "Jan": "Jan", "Feb": "Feb", "Mar": "Mar", "Apr": "Apr",
                            "Mei": "May", "Jun": "Jun", "Jul": "Jul", "Agu": "Aug",
                            "Sep": "Sep", "Okt": "Oct", "Nov": "Nov", "Des": "Dec"
                        }
                        tanggal_parts = tanggal.split()
                        if len(tanggal_parts) >= 3 and tanggal_parts[1] in indonesian_months:
                            tanggal_parts[1] = indonesian_months[tanggal_parts[1]]
                            tanggal = " ".join(tanggal_parts)

                        date_obj = datetime.strptime(tanggal, "%d %b %Y")
                        time_obj = datetime.strptime(jam.split()[0], "%H:%M:%S")
                        combined_time = date_obj.replace(hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
                        event_time_str = pytz.timezone('Asia/Jakarta').localize(combined_time).astimezone(pytz.UTC).isoformat()
                    except (ValueError, TypeError, IndexError):
                        continue

                event_time = Utils.parse_time(event_time_str)
                if not event_time:
                    continue

                if event_time.tzinfo is None:
                    event_time = pytz.timezone('Asia/Jakarta').localize(event_time)
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

                # 获取坐标
                try:
                    coordinates = item.get("Coordinates", "")
                    if coordinates:
                        coords = coordinates.split(",")
                        if len(coords) >= 2:
                            lat = float(coords[0].strip())
                            lon = float(coords[1].strip())
                        else:
                            lat, lon = 0.0, 0.0
                    else:
                        lintang = item.get("Lintang", "")
                        bujur = item.get("Bujur", "")
                        if lintang and bujur:
                            lat = float(lintang.split()[0])
                            if "LS" in lintang.upper():
                                lat = -abs(lat)
                            lon = float(bujur.split()[0])
                            if "BB" in bujur.upper():
                                lon = -abs(lon)
                        else:
                            lat, lon = 0.0, 0.0
                except (ValueError, TypeError, IndexError):
                    lat, lon = 0.0, 0.0

                # 获取深度
                try:
                    kedalaman = item.get("Kedalaman", "")
                    if kedalaman:
                        depth_match = re.search(r'(\d+(?:\.\d+)?)', kedalaman)
                        if depth_match:
                            depth = float(depth_match.group(1))
                        else:
                            depth = 0.0
                    else:
                        depth = 0.0
                except (ValueError, TypeError):
                    depth = 0.0

                # 获取位置信息
                wilayah = item.get("Wilayah", "未知地区")
                if not wilayah or not isinstance(wilayah, str):
                    wilayah = "未知地区"

                try:
                    location = wilayah.strip()
                    location = re.sub(r'^\d+\s*km\s+(BaratLaut|BaratDaya|Tenggara|TimurLaut|Barat|Timur|Utara|Selatan)\s+', '', location, flags=re.IGNORECASE).strip()
                    if not location:
                        location = wilayah.strip()
                    location = TranslationService.translate_location(location, lat=lat, lon=lon, source='BMKG')
                except Exception:
                    location = TranslationService.translate_location(wilayah, lat=lat, lon=lon, source='BMKG')

                # 获取事件ID
                event_id = item.get("DateTime", "")
                if not event_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    event_id = f"bmkg_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"

                # 构建事件字典
                event = {
                    "id": event_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(lat),
                    "EPI_LON": str(lon),
                    "EPI_DEPTH": round(depth),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M",
                    "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (BMKG)",
                    "epicenter_tts": location,
                    "INTENSITY": "",
                    "SOURCE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "EVENT_ID": event_id,
                    "infoTypeName": "地震报告"
                }
                result.append(event)
            except Exception:
                continue
        return result

# ============================================================================
# INGV数据源
# ============================================================================
class INGVSource:
    """意大利国家地球物理与火山学研究数据源"""
    
    @staticmethod
    def process():
        """处理INGV数据源"""
        from services.common.source_switches import is_list_enabled
        if not is_list_enabled("INGV"):
            return
        source_name = "INGV"
        from services.common.source_status import get_source_status_registry
        reg = get_source_status_registry()
        if Utils.check_circuit_breaker(source_name):
            return

        try:
            data = INGVSource.fetch()
            reg.record_ok("ingv")
            if data:
                parsed_data = INGVSource.parse(data if isinstance(data, list) else [data])
                if parsed_data:
                    FusionHandler.add_events_to_fused_list(parsed_data)
                    reg.record_event("ingv")
                Utils.reset_circuit_breaker(source_name)
        except Exception as e:
            logger.error(f"处理 {source_name} 数据时发生错误: {e}")
            reg.record_error("ingv", str(e))
            Utils.handle_fetch_error(source_name, e)
    
    @staticmethod
    def fetch():
        """获取INGV全部数据并全部解析"""
        def extract_events(data):
            if not isinstance(data, dict):
                return []
            payload = data.get("payload")
            if not isinstance(payload, list):
                return []
            return payload

        def get_id(first, last):
            first_id = str(first.get("properties", {}).get("eventId", ""))
            last_id = str(last.get("properties", {}).get("eventId", ""))
            return first_id if first is last else f"{first_id}_{last_id}"

        def get_time(event):
            return event.get("properties", {}).get("time", "")

        return INGVSource._fetch_and_check_cache(API_URLS['INGV'], "INGV", None, extract_events, get_id, get_time)
    
    @staticmethod
    def _fetch_and_check_cache(url, source_key, headers=None, extract_events_fn=None, get_id_fn=None, get_time_fn=None):
        """通用数据获取和缓存检查函数"""
        try:
            timeout = 10
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            event_list = extract_events_fn(data) if extract_events_fn else []
            if not event_list or len(event_list) == 0:
                return None

            event_count = len(event_list)
            data_id = get_id_fn(event_list[0], event_list[-1]) if get_id_fn else ""
            latest_event_id = get_id_fn(event_list[0], event_list[0]) if get_id_fn else ""
            latest_event_time = get_time_fn(event_list[0]) if get_time_fn else ""

            cache = cache_state[source_key]
            if (data_id == cache["id"] and event_count == cache["count"] and
                latest_event_id == cache["latest_id"] and str(latest_event_time) == cache["latest_time"]):
                return None

            cache["id"] = data_id
            cache["count"] = event_count
            cache["latest_id"] = latest_event_id
            cache["latest_time"] = str(latest_event_time)
            cache["last_success"] = datetime.now()

            return event_list
        except Exception:
            return None
    
    @staticmethod
    def parse(data):
        """解析INGV数据"""
        result = []
        for item in data:
            try:
                properties = item.get("properties") or {}
                geometry = item.get("geometry") or {}

                mag = properties.get("mag")
                if mag is None:
                    continue
                try:
                    mag = float(mag)
                except (TypeError, ValueError):
                    continue
                if mag <= 0:
                    continue

                event_time_str = properties.get("time", "")
                if not event_time_str:
                    continue

                event_time = Utils.parse_time(event_time_str)
                if not event_time:
                    continue

                if event_time.tzinfo is None:
                    event_time = pytz.UTC.localize(event_time)
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

                coords = geometry.get("coordinates")
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    try:
                        lon = float(coords[0]) if coords[0] is not None else 0.0
                    except (TypeError, ValueError):
                        lon = 0.0
                    try:
                        lat = float(coords[1]) if coords[1] is not None else 0.0
                    except (TypeError, ValueError):
                        lat = 0.0
                    try:
                        depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else 0.0
                    except (TypeError, ValueError):
                        depth = 0.0
                else:
                    lat, lon, depth = 0.0, 0.0, 0.0

                location_raw = properties.get("place", "未知地区")
                if not location_raw or not isinstance(location_raw, str):
                    location_raw = "未知地区"

                location = TranslationService.translate_location(location_raw, lat=lat, lon=lon, source='INGV')

                event_id = properties.get("eventId") or properties.get("originId") or ""
                if not event_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    event_id = f"ingv_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"
                else:
                    event_id = str(event_id)

                event = {
                    "id": event_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(lat),
                    "EPI_LON": str(lon),
                    "EPI_DEPTH": round(depth),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M",
                    "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (INGV)",
                    "epicenter_tts": location,
                    "INTENSITY": "",
                    "SOURCE": SOURCE_NAMES.get("INGV", "INGV"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("INGV", "INGV"),
                    "EVENT_ID": event_id,
                    "infoTypeName": "地震报告"
                }
                result.append(event)
            except Exception:
                continue
        return result

# ============================================================================
# 数据源处理器（保留用于FanStudio数据源解析器）
# ============================================================================
class DataSourceProcessor:
    """数据源处理器：保留用于FanStudio数据源解析器"""
    
    @staticmethod
    def parse_ningxia_data(data):
        """解析宁夏地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            # 震级有可能为 None 或无法转换为数字，这里做更健壮的处理
            magnitude = data.get("magnitude")
            if magnitude is None:
                return None
            try:
                magnitude = float(magnitude)
            except (TypeError, ValueError):
                return None
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # NINGXIA 直接返回原始地名
            location = place_name
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"ningxia_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (宁夏)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("NINGXIA", "宁夏地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("NINGXIA", "宁夏地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析宁夏数据失败: {e}")
            return None

    @staticmethod
    def parse_guangxi_data(data):
        """解析广西地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # GUANGXI 直接返回原始地名
            location = place_name
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"guangxi_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (广西地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("GUANGXI", "广西地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("GUANGXI", "广西地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析广西数据失败: {e}")
            return None

    @staticmethod
    def parse_yunnan_data(data):
        """解析云南地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            # 云南局震级：优先使用 magnitude，缺失时回退到 magnitudel（ml）
            magnitude_raw = data.get("magnitude")
            if magnitude_raw in (None, "", " "):
                magnitude_raw = data.get("magnitudel")

            # 若仍然为空，视为无效事件
            if magnitude_raw is None:
                return None

            # 尝试将震级转换为浮点数，失败则视为无效
            try:
                magnitude = float(magnitude_raw)
            except (TypeError, ValueError):
                return None

            # 非正震级直接丢弃
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = place_name
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"yunnan_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (云南地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("YUNNAN", "云南地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("YUNNAN", "云南地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析云南数据失败: {e}")
            return None

    @staticmethod
    def parse_shanxi_data(data):
        """解析山西地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='SHANXI')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"shanxi_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (山西地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("SHANXI", "山西地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("SHANXI", "山西地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析山西数据失败: {e}")
            return None

    @staticmethod
    def parse_beijing_data(data):
        """解析北京地震局数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='BEIJING')
            event_id = data.get("eventId") or data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"beijing_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (北京地震局)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("BEIJING", "北京地震局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BEIJING", "北京地震局"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析北京数据失败: {e}")
            return None

    @staticmethod
    def parse_hko_data(data):
        """解析香港天文台数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='HKO')
            event_id = data.get("eventId") or data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"hko_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            verify = data.get("verify", "")
            if verify == "Y":
                auto_flag = "M"
                info_type_name = "已核实"
                is_auto = False
            else:
                auto_flag = "[自动测定]"
                info_type_name = "待核实"
                is_auto = True

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": auto_flag,
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (HKO)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("HKO", "香港天文台"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("HKO", "香港天文台"),
                "EVENT_ID": event_id,
                "IS_AUTO": is_auto,
                "infoTypeName": info_type_name
            }
        except Exception as e:
            logger.error(f"解析香港天文台数据失败: {e}")
            return None

    @staticmethod
    def parse_usgs_data(data):
        """解析USGS数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            info_type_name = data.get("infoTypeName", "")
            if info_type_name == "automatic":
                auto_flag = "[自动测定]"
                result_info_type_name = "Automatic[自动测定]"
                is_auto = True
            else:
                auto_flag = "M"
                result_info_type_name = "Reviewed"
                is_auto = False

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='USGS')

            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"usgs_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": auto_flag,
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (USGS)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("USGS", "美国地质调查局"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("USGS", "美国地质调查局"),
                "EVENT_ID": event_id,
                "IS_AUTO": is_auto,
                "infoTypeName": result_info_type_name
            }
        except Exception as e:
            logger.error(f"解析USGS数据失败: {e}")
            return None

    @staticmethod
    def parse_emsc_data(data):
        """解析EMSC数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='EMSC')

            # 确保地名不为空
            if not location or not location.strip():
                location = "未知地区"
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"emsc_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (EMSC)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("EMSC", "欧洲地中海地震中心"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("EMSC", "欧洲地中海地震中心"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析EMSC数据失败: {e}")
            return None

    @staticmethod
    def parse_fssn_data(data):
        """解析FSSN数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            
            # FSSN 优先使用 placeName_zh，如果存在则直接使用，不进行地名修正
            place_name_zh = data.get("placeName_zh")
            if place_name_zh:
                location = place_name_zh
            else:
                place_name = data.get("placeName", "未知地区")
                # 使用新的地名处理优先级（FE fix -> 翻译API）
                location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='FSSN')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"fssn_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            info_type_name = data.get("infoTypeName", "")
            if "正式" in info_type_name or "已核实" in info_type_name:
                auto_flag = "M"
                result_info_type_name = "已核实"
                is_auto = False
            else:
                auto_flag = "[自动测定]"
                result_info_type_name = "已确认[自动测定]"
                is_auto = True

            result = {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": auto_flag,
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (FSSN)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("FSSN", "FSSN"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("FSSN", "FSSN"),
                "EVENT_ID": event_id,
                "IS_AUTO": is_auto,
                "infoTypeName": result_info_type_name
            }
            # 保存原始的 placeName_zh 用于台湾数据判断
            if place_name_zh:
                result["placeName_zh"] = place_name_zh
            return result
        except Exception as e:
            logger.error(f"解析FSSN数据失败: {e}")
            return None

    @staticmethod
    def parse_bcsf_data(data):
        """解析BCSF数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            location = place_name

            if "near of" in location:
                parts = location.split("near of")
                if len(parts) > 1:
                    location = parts[1].strip()
                    if "(" in location:
                        location = location.split("(")[0].strip()

            location = re.sub(r'Quarry blast of magnitude \d+\.\d+,?\s*', '', location)
            location = re.sub(r',?\s*\([^)]*\)', '', location)
            location = location.strip()

            if not location:
                location = place_name

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(location, lat=latitude, lon=longitude, source='BCSF')

            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"bcsf_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (BCSF)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("BCSF", "法国中央地震研究所"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BCSF", "法国中央地震研究所"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析BCSF数据失败: {e}")
            return None

    @staticmethod
    def parse_gfz_data(data):
        """解析GFZ数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude")
            if magnitude is None or magnitude <= 0:
                return None

            latitude = data.get("latitude")
            if latitude is None:
                return None

            longitude = data.get("longitude")
            if longitude is None:
                return None

            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='GFZ')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"gfz_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (GFZ)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("GFZ", "德国地学研究中心"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("GFZ", "德国地学研究中心"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析GFZ数据失败: {e}")
            return None

    @staticmethod
    def parse_usp_data(data):
        """解析USP数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='USP')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"usp_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (USP)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("USP", "巴西圣保罗大学地震信息"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("USP", "巴西圣保罗大学地震信息"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析USP数据失败: {e}")
            return None

    @staticmethod
    def parse_kma_data(data):
        """解析KMA数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            magnitude = data.get("magnitude", 0)
            if magnitude <= 0:
                return None

            latitude = data.get("latitude", 0)
            longitude = data.get("longitude", 0)
            depth = data.get("depth", 0)
            if depth is None:
                depth = 0
            place_name = data.get("placeName", "未知地区")

            # 使用新的地名处理优先级（FE fix -> 翻译API）
            location = TranslationService.translate_location(place_name, lat=latitude, lon=longitude, source='KMA')
            event_id = data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"kma_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(float(depth)),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{float(magnitude):.1f}",
                "LOCATION_C": f"{location} (KMA)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("KMA", "韩国气象厅"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("KMA", "韩国气象厅"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析KMA数据失败: {e}")
            return None

    @staticmethod
    def parse_cenc_data(data):
        """解析CENC数据"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None

            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None

            event_time_utc8 = event_time

            try:
                magnitude = float(data.get("magnitude", 0))
            except (ValueError, TypeError):
                magnitude = 0

            if magnitude <= 0:
                return None

            try:
                latitude = float(data.get("latitude", 0))
            except (ValueError, TypeError):
                latitude = 0.0

            try:
                longitude = float(data.get("longitude", 0))
            except (ValueError, TypeError):
                longitude = 0.0

            try:
                depth = float(data.get("depth", 0))
            except (ValueError, TypeError):
                depth = 0.0

            place_name = data.get("placeName", "未知地区")

            info_type_name = data.get("infoTypeName", "")
            auto_flag = data.get("autoFlag", "")
            is_auto = False

            if "[自动测定]" in info_type_name or auto_flag == "I":
                is_auto = True
                flag = "[自动测定]"
            elif "[正式测定]" in info_type_name or auto_flag == "M":
                is_auto = False
                flag = "M"
            else:
                if auto_flag == "I":
                    is_auto = True
                    flag = "[自动测定]"
                else:
                    is_auto = False
                    flag = "M"

            # CENC 直接返回原始地名
            location = place_name
            event_id = data.get("eventId") or data.get("id", "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"cenc_{event_timestamp}_{int(latitude*10)}_{int(longitude*10)}_{int(magnitude*10)}"

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(latitude),
                "EPI_LON": str(longitude),
                "EPI_DEPTH": round(depth),
                "AUTO_FLAG": flag,
                "EQ_TYPE": "M",
                "M": f"{magnitude:.1f}",
                "LOCATION_C": f"{location} (CENC)",
                "epicenter_tts": location,
                "INTENSITY": "",
                "SOURCE": SOURCE_NAMES.get("CENC", "中国地震台网中心"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("CENC", "中国地震台网中心"),
                "IS_AUTO": is_auto,
                "EVENT_ID": event_id,
                "infoTypeName": info_type_name if info_type_name else ("[自动测定]" if is_auto else "[正式测定]")
            }
        except Exception as e:
            logger.error(f"解析Fan Studio CENC数据失败: {e}")
            return None

    @staticmethod
    def parse_cwa_fanstudio_data(data):
        """解析 FanStudio WebSocket 中的 cwa.Data（速报/测定统一格式）"""
        try:
            shock_time = data.get("shockTime")
            if not shock_time:
                return None
            event_time = Utils.parse_time(shock_time)
            if not event_time:
                return None
            if event_time.tzinfo is None:
                event_time_utc8 = pytz.timezone('Asia/Taipei').localize(event_time).astimezone(pytz.timezone('Asia/Shanghai'))
            else:
                event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))

            try:
                mag = float(data.get("magnitude", 0) or 0)
            except (ValueError, TypeError):
                mag = 0.0

            try:
                lat = float(data.get("latitude", 0))
                lon = float(data.get("longitude", 0))
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0

            try:
                depth = float(data.get("depth", 0))
            except (ValueError, TypeError):
                depth = 0.0

            place_name = data.get("placeName", "未知地区")
            if not place_name or not isinstance(place_name, str):
                place_name = "未知地区"
            bracket_match = re.search(r'\(([^)]+)\)', place_name)
            if bracket_match:
                location = bracket_match.group(1).replace("位於", "")
                location = re.sub(r'\s+', ' ', location).strip()
            else:
                location = "未知地区"
            event_id = str(data.get("eventId", "") or data.get("id", "") or "")
            if not event_id:
                event_timestamp = int(event_time_utc8.timestamp())
                event_id = f"cwa_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"

            intensity = ""
            max_intensity = data.get("maxIntensity")
            if max_intensity and isinstance(max_intensity, str):
                m_int = re.search(r"(\d+)", max_intensity)
                if m_int:
                    intensity = m_int.group(1)

            return {
                "id": event_id,
                "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                "EPI_LAT": str(lat),
                "EPI_LON": str(lon),
                "EPI_DEPTH": round(depth),
                "AUTO_FLAG": "M",
                "EQ_TYPE": "M",
                "M": f"{mag:.1f}",
                "LOCATION_C": location + " (CWA)",
                "epicenter_tts": location,
                "INTENSITY": intensity,
                "SOURCE": SOURCE_NAMES.get("CWA", "CWA"),
                "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("CWA", "CWA"),
                "EVENT_ID": event_id,
                "infoTypeName": "地震报告"
            }
        except Exception as e:
            logger.error(f"解析FanStudio CWA数据失败: {e}")
            return None

    @staticmethod
    def parse_bmkg_fanstudio_data(data):
        """解析 all_ws 1450 / bmkg_ws 的 bmkg.Data。
        历史与实时均为 BMKG TEWS 原结构：DateTime、Coordinates、Magnitude、Kedalaman、Wilayah 等，
        与 HTTP BMKGSource.parse 一致；不再使用 shockTime/placeName/扁平 latitude 等 FanStudio 旧字段。"""
        try:
            if not isinstance(data, dict):
                return None
            # 仍含 shockTime 的旧消息：尽量走 BMKG 官方字段；若无 DateTime/Coordinates 再回退 shockTime
            if data.get("shockTime") and not (data.get("DateTime") or data.get("Coordinates")):
                shock_time = data.get("shockTime")
                event_time = Utils.parse_time(shock_time)
                if not event_time:
                    return None
                if event_time.tzinfo is None:
                    event_time_utc8 = pytz.timezone('Asia/Jakarta').localize(event_time).astimezone(
                        pytz.timezone('Asia/Shanghai'))
                else:
                    event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))
                try:
                    mag = float(data.get("magnitude", 0))
                except (ValueError, TypeError):
                    mag = 0
                if mag <= 0:
                    return None
                try:
                    lat = float(data.get("latitude", 0))
                    lon = float(data.get("longitude", 0))
                except (ValueError, TypeError):
                    lat, lon = 0.0, 0.0
                try:
                    depth = float(data.get("depth", 0))
                except (ValueError, TypeError):
                    depth = 0.0
                place_name = data.get("placeName", "未知地区")
                if not place_name or not isinstance(place_name, str):
                    place_name = "未知地区"
                try:
                    location = TranslationService.translate_location(place_name.strip(), lat=lat, lon=lon, source='BMKG')
                except Exception:
                    location = TranslationService.translate_location(place_name, lat=lat, lon=lon, source='BMKG')
                event_id = str(data.get("eventId", "") or "")
                if not event_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    event_id = f"bmkg_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"
                return {
                    "id": event_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(lat),
                    "EPI_LON": str(lon),
                    "EPI_DEPTH": round(depth),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M",
                    "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (BMKG)",
                    "epicenter_tts": location,
                    "INTENSITY": "",
                    "SOURCE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("BMKG", "BMKG"),
                    "EVENT_ID": event_id,
                    "infoTypeName": "地震报告"
                }
            parsed = BMKGSource.parse([data])
            return parsed[0] if parsed else None
        except Exception as e:
            logger.error(f"解析 BMKG WebSocket 数据失败: {e}")
            return None

    @staticmethod
    def parse_geonet_fanstudio_data(data):
        """解析 all_ws 1450 / GeoNet_ws 的 geonet.Data。
        标准 GeoJSON Feature：geometry.coordinates、properties.time/publicID/magnitude/depth/locality，
        与 HTTP GEONETSource.parse 一致；不再使用 shockTime/placeName 等 FanStudio 旧字段。"""
        try:
            if not isinstance(data, dict):
                return None
            geom = data.get("geometry") if isinstance(data.get("geometry"), dict) else {}
            props = data.get("properties") if isinstance(data.get("properties"), dict) else {}
            is_feature = data.get("type") == "Feature" or (
                "coordinates" in geom and isinstance(props, dict)
            )
            if is_feature:
                parsed = GEONETSource.parse([data])
                if parsed:
                    return parsed[0]
            if data.get("shockTime"):
                shock_time = data.get("shockTime")
                event_time = Utils.parse_time(shock_time)
                if not event_time:
                    return None
                if event_time.tzinfo is None:
                    event_time_utc8 = pytz.timezone('Pacific/Auckland').localize(event_time).astimezone(
                        pytz.timezone('Asia/Shanghai'))
                else:
                    event_time_utc8 = event_time.astimezone(pytz.timezone('Asia/Shanghai'))
                try:
                    mag = float(data.get("magnitude", 0))
                except (ValueError, TypeError):
                    mag = 0
                if mag <= 0:
                    return None
                try:
                    lat = float(data.get("latitude", 0))
                    lon = float(data.get("longitude", 0))
                except (ValueError, TypeError):
                    lat, lon = 0.0, 0.0
                try:
                    depth = float(data.get("depth", 0))
                except (ValueError, TypeError):
                    depth = 0.0
                place_name = data.get("placeName", "未知地区")
                if not place_name or not isinstance(place_name, str):
                    place_name = "未知地区"
                try:
                    location = TranslationService.translate_location(place_name.strip(), lat=lat, lon=lon, source='GEONET')
                except Exception:
                    location = TranslationService.translate_location(place_name, lat=lat, lon=lon, source='GEONET')
                mmi = data.get("mmi")
                if mmi is None:
                    intensity = ""
                else:
                    try:
                        mmi_value = float(mmi)
                        intensity = str(int(mmi_value)) if mmi_value >= 0 else ""
                    except (ValueError, TypeError):
                        intensity = str(mmi) if mmi else ""
                public_id = str(data.get("eventId", "") or "")
                if not public_id:
                    event_timestamp = int(event_time_utc8.timestamp())
                    public_id = f"geonet_{event_timestamp}_{int(lat*10)}_{int(lon*10)}_{int(mag*10)}"
                return {
                    "id": public_id,
                    "O_TIME": event_time_utc8.strftime('%Y-%m-%d %H:%M:%S'),
                    "O_TIME_TTS": event_time_utc8.strftime('%H:%M:%S'),
                    "EPI_LAT": str(lat),
                    "EPI_LON": str(lon),
                    "EPI_DEPTH": round(depth),
                    "AUTO_FLAG": "M",
                    "EQ_TYPE": "M",
                    "M": f"{mag:.1f}",
                    "LOCATION_C": location + " (GeoNet)",
                    "epicenter_tts": location,
                    "INTENSITY": intensity,
                    "SOURCE": SOURCE_NAMES.get("GEONET", "GEONET"),
                    "SOURCE_NAME_CHINESE": SOURCE_NAMES.get("GEONET", "GEONET"),
                    "EVENT_ID": public_id,
                    "infoTypeName": "地震报告"
                }
            return None
        except Exception as e:
            logger.error(f"解析 GeoNet WebSocket 数据失败: {e}")
            return None

# FanStudio数据源解析器映射
FAN_STUDIO_PARSERS = {
    'cenc': DataSourceProcessor.parse_cenc_data,
    'cwa': DataSourceProcessor.parse_cwa_fanstudio_data,
    'ningxia': DataSourceProcessor.parse_ningxia_data,
    'guangxi': DataSourceProcessor.parse_guangxi_data,
    'yunnan': DataSourceProcessor.parse_yunnan_data,
    'shanxi': DataSourceProcessor.parse_shanxi_data,
    'beijing': DataSourceProcessor.parse_beijing_data,
    'hko': DataSourceProcessor.parse_hko_data,
    'usgs': DataSourceProcessor.parse_usgs_data,
    'emsc': DataSourceProcessor.parse_emsc_data,
    'bcsf': DataSourceProcessor.parse_bcsf_data,  # BCSF - 法国中央地震研究所
    'gfz': DataSourceProcessor.parse_gfz_data,    # GFZ - 德国地学研究中心
    'usp': DataSourceProcessor.parse_usp_data,    # USP - 巴西圣保罗大学地震信息
    'kma': DataSourceProcessor.parse_kma_data,    # KMA - 韩国气象厅
    'fssn': DataSourceProcessor.parse_fssn_data,  # FSSN数据解析
}

# 内网 ws://172.25.16.104:1450：bmkg 为 TEWS 原字段，geonet 为 GeoJSON Feature（与 bmkg_ws / GeoNet_ws 及 start_all 一致）
INTERNAL_WS_PARSERS = {
    'bmkg': DataSourceProcessor.parse_bmkg_fanstudio_data,
    'geonet': DataSourceProcessor.parse_geonet_fanstudio_data,
}

# ============================================================================
# WebSocket处理模块
# ============================================================================
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
            thread = threading.Thread(target=lambda: test_ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}), daemon=True)
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
        global _shared_fan_conn
        if _shared_fan_conn is not None:
            _shared_fan_conn.switch_to_backup()
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
        global _shared_fan_conn
        if _shared_fan_conn is not None:
            _shared_fan_conn.switch_to_primary()
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
        """检查事件时间是否为新事件"""
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
        except Exception:
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
        if _shared_fan_conn is not None and _shared_fan_conn.send_text(cmd):
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
        try:
            from services.common.source_status import get_source_status_registry
            data = WebSocketHandler._expand_fan_v21(data)
            msg_type = data.get('type')

            if msg_type in ('initial_all', 'start_all'):
                # 保存原始数据到缓存
                with fanstudio_cache_lock:
                    for source_name, parser in FAN_STUDIO_PARSERS.items():
                        if source_name in EXCLUDED_SOURCES:
                            continue
                        if source_name in FANSTUDIO_NO_RAW_CACHE_SOURCES:
                            continue

                        source_entry = WebSocketHandler._get_source_entry(data, source_name)
                        if source_entry:
                            source_data = source_entry.get('Data', {})
                            if not source_data:
                                continue

                            if source_name not in fanstudio_raw_cache:
                                fanstudio_raw_cache[source_name] = deque(maxlen=Config.MAX_CACHE_PER_SOURCE)

                            cache_deque = fanstudio_raw_cache[source_name]
                            # 检查是否已存在相同数据（简单的检查）
                            if source_data not in cache_deque:
                                cache_deque.append(source_data)

                CacheManager.save_fanstudio_cache()

                # 解析并推送事件
                parsed_events = WebSocketHandler.parse_fan_studio_data(data)
                if parsed_events:
                    filtered_events = []
                    for event in parsed_events:
                        source_name = None
                        source_chinese = event.get("SOURCE", "")
                        for parser_source, parser_func in FAN_STUDIO_PARSERS.items():
                            expected_source_name = SOURCE_NAMES.get(parser_source.upper(), "")
                            if source_chinese == expected_source_name:
                                source_name = parser_source
                                break

                        if source_name and WebSocketHandler.check_event_time(event, source_name):
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

            elif msg_type == 'update':
                source = data.get('institution') or data.get('source')
                source = WebSocketHandler._normalize_institution_key(source)
                from services.common.source_switches import is_list_enabled
                if source and source not in EXCLUDED_SOURCES and source in FAN_STUDIO_PARSERS:
                    if not is_list_enabled(source):
                        return
                    source_data = data.get('Data', {})
                    if source_data:
                        if source not in FANSTUDIO_NO_RAW_CACHE_SOURCES:
                            with fanstudio_cache_lock:
                                if source not in fanstudio_raw_cache:
                                    fanstudio_raw_cache[source] = deque(maxlen=Config.MAX_CACHE_PER_SOURCE)

                                cache_deque = fanstudio_raw_cache[source]
                                if source_data not in cache_deque:
                                    cache_deque.append(source_data)

                            CacheManager.save_fanstudio_cache()

                        # 解析并推送事件
                        parser = FAN_STUDIO_PARSERS[source]
                        event = parser(source_data)
                        if event:
                            if source == 'cenc':
                                FusionHandler.add_events_to_fused_list([event])

                                logger.info(
                                    f"FanStudio更新数据 [{source}]: 解析到1个地震事件；"
                                    f"{FusionHandler._list_push_log_suffix(event)}"
                                )
                                return

                            if source == 'cwa':
                                FusionHandler.add_events_to_fused_list([event])
                                logger.info(f"FanStudio更新数据 [{source}]: 解析到1个地震事件（台湾气象署）；已推送至8150")
                                return

                            if not WebSocketHandler.check_event_time(event, source):
                                logger.debug(f"FanStudio更新数据 [{source}]: 事件时间不新，跳过推送")
                                return

                            FusionHandler.add_events_to_fused_list([event])
                            logger.info(
                                f"FanStudio更新数据 [{source}]: 解析到1个地震事件；"
                                f"{FusionHandler._list_push_log_suffix(event)}"
                            )

            # 兼容 all_ws 新格式：type 直接为机构名（无 update/institution）
            elif isinstance(msg_type, str) and msg_type not in (
                'heartbeat', 'pong', 'cenclist_response', 'cwalist_response', 'fssnlist_response',
            ):
                source = WebSocketHandler._normalize_institution_key(msg_type)
                from services.common.source_switches import is_list_enabled
                if source and source not in EXCLUDED_SOURCES and source in FAN_STUDIO_PARSERS:
                    if not is_list_enabled(source):
                        return
                    source_data = data.get('Data', {})
                    if source_data:
                        if source not in FANSTUDIO_NO_RAW_CACHE_SOURCES:
                            with fanstudio_cache_lock:
                                if source not in fanstudio_raw_cache:
                                    fanstudio_raw_cache[source] = deque(maxlen=Config.MAX_CACHE_PER_SOURCE)
                                cache_deque = fanstudio_raw_cache[source]
                                if source_data not in cache_deque:
                                    cache_deque.append(source_data)
                            CacheManager.save_fanstudio_cache()

                        parser = FAN_STUDIO_PARSERS[source]
                        event = parser(source_data)
                        if event:
                            if source == 'cenc':
                                FusionHandler.add_events_to_fused_list([event])
                                logger.info(
                                    f"FanStudio更新数据 [{source}]: 解析到1个地震事件；"
                                    f"{FusionHandler._list_push_log_suffix(event)}"
                                )
                                return

                            if source == 'cwa':
                                FusionHandler.add_events_to_fused_list([event])
                                logger.info(f"FanStudio更新数据 [{source}]: 解析到1个地震事件（台湾气象署）；已推送至8150")
                                return

                            if not WebSocketHandler.check_event_time(event, source):
                                logger.debug(f"FanStudio更新数据 [{source}]: 事件时间不新，跳过推送")
                                return

                            FusionHandler.add_events_to_fused_list([event])
                            logger.info(
                                f"FanStudio更新数据 [{source}]: 解析到1个地震事件；"
                                f"{FusionHandler._list_push_log_suffix(event)}"
                            )

            elif msg_type == 'cenclist_response':
                cenc_data_list = data.get('Data', [])
                if not isinstance(cenc_data_list, list):
                    logger.warning(f"FanStudio CENC列表响应格式错误: Data不是列表类型")
                    return

                parsed_events = []
                for item in cenc_data_list:
                    if not isinstance(item, dict):
                        continue
                    try:
                        event = DataSourceProcessor.parse_cenc_data(item)
                        if event:
                            parsed_events.append(event)
                    except Exception as e:
                        logger.error(f"解析FanStudio CENC列表项失败: {e}")
                        continue

                if parsed_events:
                    FusionHandler.add_events_to_fused_list(parsed_events, bulk_quiet_cenc_logs=True)

                    logger.info(f"FanStudio CENC列表: 解析到 {len(parsed_events)} 个地震事件；CENC数据源无阈值，已推送至8150")

                do_next = False
                with _fanstudio_list_cmd_lock:
                    if FANSTUDIO_LIST_CMD_STATE["phase"] == 0:
                        FANSTUDIO_LIST_CMD_STATE["phase"] = 1
                        do_next = True
                if do_next:
                    gap = WebSocketHandler._fanstudio_list_cmd_gap(len(parsed_events))
                    logger.info(
                        f"FanStudio 列表：CENC {len(parsed_events)} 条已入库，{gap:g}s 后发送 cwalist"
                    )
                    WebSocketHandler._fanstudio_schedule_list_cmd_after_delay("cwalist", gap)

            elif msg_type == 'cwalist_response':
                cwa_data_list = data.get('Data', [])
                if not isinstance(cwa_data_list, list):
                    logger.warning("FanStudio CWA列表响应格式错误: Data不是列表类型")
                    return
                parsed_events = []
                for item in cwa_data_list:
                    if not isinstance(item, dict):
                        continue
                    try:
                        event = DataSourceProcessor.parse_cwa_fanstudio_data(item)
                        if event:
                            parsed_events.append(event)
                    except Exception as e:
                        logger.error(f"解析FanStudio CWA列表项失败: {e}")
                        continue
                if parsed_events:
                    FusionHandler.add_events_to_fused_list(parsed_events, bulk_quiet_cenc_logs=True)
                    logger.info(f"FanStudio CWA列表: 解析到 {len(parsed_events)} 个地震事件；已推送至8150")

                do_next = False
                with _fanstudio_list_cmd_lock:
                    if FANSTUDIO_LIST_CMD_STATE["phase"] == 1:
                        FANSTUDIO_LIST_CMD_STATE["phase"] = 2
                        do_next = True
                if do_next:
                    gap = WebSocketHandler._fanstudio_list_cmd_gap(len(parsed_events))
                    logger.info(
                        f"FanStudio 列表：CWA {len(parsed_events)} 条已入库，{gap:g}s 后发送 fssnlist"
                    )
                    WebSocketHandler._fanstudio_schedule_list_cmd_after_delay("fssnlist", gap)

            elif msg_type == 'fssnlist_response':
                fssn_data_list = data.get('Data', [])
                if not isinstance(fssn_data_list, list):
                    logger.warning("FanStudio FSSN列表响应格式错误: Data不是列表类型")
                    return
                parsed_events = []
                for item in fssn_data_list:
                    if not isinstance(item, dict):
                        continue
                    try:
                        event = DataSourceProcessor.parse_fssn_data(item)
                        if event:
                            parsed_events.append(event)
                    except Exception as e:
                        logger.error(f"解析FanStudio FSSN列表项失败: {e}")
                        continue
                if parsed_events:
                    # fssnlist：仍应用震级阈值与地区过滤
                    FusionHandler.add_events_to_fused_list(parsed_events, bulk_quiet_cenc_logs=True)
                    pushed = sum(
                        1 for e in parsed_events if FusionHandler._should_include_list_event(e)
                    )
                    logger.info(
                        f"FanStudio FSSN列表: 解析到 {len(parsed_events)} 个地震事件；"
                        f"{pushed} 条已推送至8150（其余已过滤）"
                    )

                with _fanstudio_list_cmd_lock:
                    if FANSTUDIO_LIST_CMD_STATE["phase"] == 2:
                        FANSTUDIO_LIST_CMD_STATE["phase"] = 3

        except Exception as e:
            logger.error(f"处理FanStudio WebSocket消息失败: {e}")

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
        global _shared_fan_conn
        _shared_fan_conn = conn
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

                ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

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
                ws_app.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                logger.error(f"P2PQuake WebSocket连接异常: {e}")
                Utils.handle_fetch_error("P2PQUAKE", e)
            time.sleep(20)

# ============================================================================
# 数据融合和存储模块
# ============================================================================
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

            return True
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
                                    except:
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
                                except:
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
class APIHandler:
    """API处理器：负责Flask应用的路由和API端点"""

    @staticmethod
    @app.route("/earthquakes")
    def earthquakes():
        """地震数据 API（端口 8150）"""
        with fused_data_lock:
            return jsonify({"shuju": list(fused_events)})

# ============================================================================
# 主程序模块
# ============================================================================
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
                time.sleep(1.0)

    @staticmethod
    def initialize():
        """初始化程序"""
        global logger

        # 设置日志
        logger = LogManager.setup_logging()

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
        logger.info("Flask Web服务器已启动: 8150/earthquakes")
        threading.Thread(
            target=lambda: serve(app, host="0.0.0.0", port=8150),
            name="Flask-8150-Thread",
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

# ============================================================================
# 程序入口
# ============================================================================
if __name__ == "__main__":
    MainHandler.main()
