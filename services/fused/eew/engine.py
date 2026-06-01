import time
import json
import random
import hashlib
import logging
import logging.handlers
import threading
import re
import os
import sys
import ssl
import asyncio
import warnings
import websockets
import requests

# 同步 WS 客户端：必须用 PyPI 的 websocket-client（提供 websocket._app.WebSocketApp）。
# 仅 `import websocket` 可能加载到错误的同名包或残留安装；优先从子模块导入。
FanStudioWebSocketApp = None
try:
    from websocket._app import WebSocketApp as FanStudioWebSocketApp
except ImportError:
    try:
        import websocket as _ws_sync_mod

        FanStudioWebSocketApp = getattr(_ws_sync_mod, "WebSocketApp", None)
    except ImportError:
        FanStudioWebSocketApp = None

if FanStudioWebSocketApp is None:
    raise ImportError(
        "无法加载 websocket-client 的 WebSocketApp（Fan Studio / ALL_WS_FULL 需要）。\n"
        "请使用与本脚本相同的解释器安装依赖（Windows 上 `Python` 与 `pip` 可能不是同一环境）：\n"
        f"  {sys.executable} -m pip uninstall websocket -y\n"
        f"  {sys.executable} -m pip install websocket-client\n"
        "若脚本目录下有 websocket.py 或与包同名的文件夹，请改名以免遮挡 site-packages。"
    )

warnings.filterwarnings('ignore', message='.*InsecureRequestWarning.*')
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Optional, Any, Callable, Tuple, Literal
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False
    logging.getLogger(__name__).warning("APScheduler not available, using basic threading for scheduling")

# ============================================================================
# 配置类
# ============================================================================

def _default_eew_base_dir() -> str:
    env = os.environ.get("EEW_BASE_DIR")
    if env:
        return env
    if os.environ.get("FUSED_MODE", "").strip() in ("1", "true", "yes"):
        try:
            from services.common.paths import get_cache_dir
            return str(get_cache_dir() / "eew")
        except ImportError:
            pass
    if os.name == "nt":
        return os.path.join(
            os.environ.get("APPDATA", os.path.expanduser("~")),
            "custom-datasource-console", "eew",
        )
    return "/opt/eew/fused_eew_api"


def _eew_subdir(name: str) -> str:
    return os.path.join(_default_eew_base_dir(), name)


@dataclass
class Config:
    """全局配置"""
    # WebSocket 上游地址
    ALL_WS_PRIMARY: str = "wss://ws.fanstudio.tech/all"
    ALL_WS_BACKUP: str = "wss://ws.fanstudio.hk/all"
    # Wolfx 聚合 EEW（CEA/CENC + JMA 上游切换时使用，见 WebSocketClientManager）
    WOLFX_ALL_EEW_URL: str = field(default_factory=lambda: os.environ.get(
        "WOLFX_ALL_EEW_URL", "wss://ws-api.wolfx.jp/all_eew"
    ))

    # 百度翻译配置（生产环境建议通过环境变量 BAIDU_APP_ID / BAIDU_SECRET_KEY 覆盖）
    BAIDU_APP_ID: str = field(default_factory=lambda: os.environ.get("BAIDU_APP_ID", "20251017002477309"))
    BAIDU_SECRET_KEY: str = field(default_factory=lambda: os.environ.get("BAIDU_SECRET_KEY", "xIeqBl_hNBbaXTevSkyl"))
    
    # 目录配置（融合模式默认使用软件根目录 cache/logs）
    LOG_DIR: str = field(default_factory=lambda: os.environ.get("EEW_LOG_DIR", _eew_subdir("logs")))
    CACHE_DIR: str = field(default_factory=lambda: os.environ.get("EEW_CACHE_DIR", _eew_subdir("eew_cache")))
    TRANSLATION_CACHE_DIR: str = field(default_factory=lambda: os.environ.get(
        "EEW_TRANSLATION_DIR", _eew_subdir("translation")
    ))
    
    # 日志配置
    LOG_LEVEL: int = logging.INFO
    LOG_MAX_DAYS: int = 7
    
    # 缓存配置
    CACHE_MAX_AGE: int = 600  # 秒
    DEDUP_TTL: float = 60.0  # 秒
    
    # 性能配置
    MAX_WORKERS: int = 8
    FETCH_TIMEOUT: int = 5

# ============================================================================
# 工具函数
# ============================================================================

class Utils:
    """工具类"""
    
    @staticmethod
    def safe_float(value: Any, default: float = 0.0) -> float:
        """安全转换为浮点数"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def safe_int(value: Any, default: int = 0) -> int:
        """安全转换为整数"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def format_magnitude(value: Any, default: float = 0.0) -> float:
        """格式化震级"""
        return round(Utils.safe_float(value, default), 1)
    
    @staticmethod
    def format_depth(value: Any, default: int = 0) -> int:
        """格式化震源深度"""
        return int(round(Utils.safe_float(value, default), 0))
    
    @staticmethod
    def format_epicenter_tts(epicenter: str) -> str:
        """格式化震中地名（用于TTS）"""
        return re.sub(r'\s*\([^)]*\)$', '', epicenter).strip()
    
    @staticmethod
    def format_o_time(timestamp_ms: int) -> str:
        """格式化发震时间"""
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S')
    
    @staticmethod
    def parse_time_utc_offset(time_str: str, utc_offset: int = 8) -> int:
        """解析时间字符串并转换为指定时区的毫秒时间戳"""
        if not time_str:
            return 0  # 返回0而不是当前时间，避免每次都生成新的时间戳
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            tz = timezone(timedelta(hours=utc_offset))
            dt_with_tz = dt.replace(tzinfo=tz)
            return int(dt_with_tz.timestamp() * 1000)
        except (ValueError, TypeError):
            return 0  # 返回0而不是当前时间


# ============================================================================
# 日志管理
# ============================================================================

class LogManager:
    """日志管理器 - 分为data数据更新，connections链接记录，error运行错误"""

    def __init__(self, config: Config):
        self.config = config
        # 日志保留天数（仅本类使用，不修改 config 以免影响其他组件）
        self._log_max_days = 5
        self.data_logger = None
        self.connection_logger = None
        self.error_logger = None
        self._setup_loggers()

    def _setup_loggers(self):
        """设置分类型日志记录器"""
        from services.common.logging_setup import ensure_stdio_utf8
        import sys as _sys

        ensure_stdio_utf8()
        os.makedirs(self.config.LOG_DIR, exist_ok=True)

        # 统一的格式器
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_formatter = logging.Formatter(
            '%(asctime)s - %(message)s',
            datefmt='%H:%M:%S'
        )

        # 1. 数据日志记录器 - 记录数据更新和推送事件
        data_handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(self.config.LOG_DIR, 'data.log'),
            when='midnight',
            interval=1,
            backupCount=self._log_max_days,
            encoding='utf-8'
        )
        data_handler.setFormatter(formatter)
        data_handler.setLevel(logging.INFO)

        self.data_logger = logging.getLogger('eew_api.data')
        self.data_logger.setLevel(logging.DEBUG)
        self.data_logger.addHandler(data_handler)
        self.data_logger.propagate = False  # 不向父logger传播

        # 2. 连接日志记录器 - 记录所有连接相关事件
        connection_handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(self.config.LOG_DIR, 'connections.log'),
            when='midnight',
            interval=1,
            backupCount=self._log_max_days,
            encoding='utf-8'
        )
        connection_handler.setFormatter(formatter)
        connection_handler.setLevel(logging.INFO)

        self.connection_logger = logging.getLogger('eew_api.connection')
        self.connection_logger.setLevel(logging.DEBUG)
        self.connection_logger.addHandler(connection_handler)
        self.connection_logger.propagate = False  # 不向父logger传播

        # 3. 错误日志记录器 - 记录所有错误和异常
        error_handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(self.config.LOG_DIR, 'errors.log'),
            when='midnight',
            interval=1,
            backupCount=self._log_max_days,
            encoding='utf-8'
        )
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.WARNING)

        self.error_logger = logging.getLogger('eew_api.error')
        self.error_logger.setLevel(logging.DEBUG)
        self.error_logger.addHandler(error_handler)
        self.error_logger.propagate = False  # 不向父logger传播

        # 控制台处理器 - 只显示关键信息（UTF-8 直写 buffer，避免 Windows 管道乱码）
        from services.common.logging_setup import Utf8StdoutHandler
        console_handler = Utf8StdoutHandler()
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        # 控制台过滤器 - 只显示关键消息
        class ConsoleFilter(logging.Filter):
            def filter(self, record):
                msg = record.getMessage()

                # 排除的详细日志
                excluded_patterns = [
                    'Adding job', 'Added job', 'Scheduler started', 'Running job',
                    'executed successfully', 'skipped: maximum', 'Websocket connected',
                    '翻译成功', '已加载', '开始加载', '加载完成', '定时任务',
                    '启动后台', '后台更新', '正在连接', '连接线程已启动',
                    '收到服务器心跳', '发送ping心跳', '收到pong响应'
                ]

                if any(pattern in msg for pattern in excluded_patterns):
                    return False

                # 允许的关键消息
                allowed_patterns = [
                    '数据更新:', '客户端连接:', '客户端断开:',
                    'WebSocket连接成功', 'WebSocket断开', '已向客户端推送',
                    '✓', '✗', '自动切换到', '服务正在关闭',
                    '连接成功', '连接断开'
                ]

                if any(pattern in msg for pattern in allowed_patterns):
                    return True

                # WARNING和ERROR级别总是显示
                return record.levelno >= logging.WARNING

        console_handler.addFilter(ConsoleFilter())

        # 为所有logger添加控制台处理器
        for logger in [self.data_logger, self.connection_logger, self.error_logger]:
            logger.addHandler(console_handler)

        # 抑制第三方库日志
        for lib in ['urllib3', 'requests', 'websockets.server', 'websocket', 'apscheduler']:
            logging.getLogger(lib).setLevel(logging.ERROR)

        # 设置根logger级别
        logging.getLogger().setLevel(logging.INFO)

    def get_logger(self, category: str) -> logging.Logger:
        """获取指定类型的logger"""
        if category == 'data':
            return self.data_logger
        elif category == 'connection':
            return self.connection_logger
        elif category == 'error':
            return self.error_logger
        else:
            # 默认返回数据logger
            return self.data_logger

    def cleanup_old_logs(self):
        """清理过期日志（保留天数由 _log_max_days 决定）"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self._log_max_days)
            log_files = []

            # 收集所有日志文件
            for filename in os.listdir(self.config.LOG_DIR):
                if filename.endswith('.log'):
                    file_path = os.path.join(self.config.LOG_DIR, filename)
                    try:
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_mtime < cutoff_date:
                            log_files.append(file_path)
                    except OSError:
                        continue

            # 删除过期文件
            for file_path in log_files:
                try:
                    os.remove(file_path)
                    filename = os.path.basename(file_path)
                    print(f"已删除过期日志: {filename}")
                except OSError as e:
                    print(f"删除日志失败 {filename}: {e}")

        except Exception as e:
            print(f"清理日志失败: {e}")

# ============================================================================
# 翻译服务
# ============================================================================

class TranslationService:
    """翻译服务"""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.cache: Dict[str, str] = {}
        self.cache_file = os.path.join(config.TRANSLATION_CACHE_DIR, "translation_cache_eew.json")
        self.lock = threading.Lock()
        self._load_cache()
    
    def _normalize_key(self, text: str) -> str:
        """规范化缓存键（去除多余空格、统一处理）"""
        if not text:
            return text
        # 去除首尾空格，将多个连续空格替换为单个空格
        normalized = ' '.join(text.split())
        return normalized
    
    def _load_cache(self):
        """加载翻译缓存（自动去重）"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    raw_cache = json.load(f)
                
                # 规范化并去重：使用规范化后的键，如果有重复则保留最后一个
                normalized_cache = {}
                duplicates_removed = 0
                
                for key, value in raw_cache.items():
                    normalized_key = self._normalize_key(key)
                    if normalized_key in normalized_cache:
                        duplicates_removed += 1
                    normalized_cache[normalized_key] = value
                
                self.cache = normalized_cache
                
                if duplicates_removed > 0:
                    self.logger.info(f"加载翻译缓存时发现并移除了 {duplicates_removed} 个重复项")
                    # 立即保存去重后的缓存
                    self._async_save_cache()
                
                self.logger.debug(f"已加载 {len(self.cache)} 条翻译缓存")
            except Exception as e:
                self.logger.error(f"加载翻译缓存失败: {e}")
        else:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
    
    def save_cache(self):
        """保存翻译缓存（自动去重）"""
        try:
            with self.lock:
                # 确保缓存键都已规范化（去重处理）
                normalized_cache = {}
                for key, value in self.cache.items():
                    normalized_key = self._normalize_key(key)
                    normalized_cache[normalized_key] = value
                
                # 更新缓存为去重后的版本
                self.cache = normalized_cache
                
                # 保存到文件
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存翻译缓存失败: {e}")
    
    def _async_save_cache(self):
        """异步保存翻译缓存到文件（无阻塞，自动去重）"""
        def _save():
            try:
                with self.lock:
                    # 确保缓存键都已规范化（去重处理）
                    normalized_cache = {}
                    for key, value in self.cache.items():
                        normalized_key = self._normalize_key(key)
                        normalized_cache[normalized_key] = value
                    
                    # 更新缓存为去重后的版本
                    self.cache = normalized_cache
                    cache_copy = self.cache.copy()  # 复制缓存，减少锁持有时间
                
                # 在锁外进行文件写入
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_copy, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error(f"异步保存翻译缓存失败: {e}")
        
        threading.Thread(target=_save, daemon=True, name="SaveTranslationCache").start()
    
    def translate(self, text: str, force_lang: Optional[str] = None, quick_mode: bool = False, skip_cache: bool = False) -> str:
        """翻译文本（支持日语、韩语、英语到中文）"""
        if not text or text == '未知地点':
            return text
        
        # 规范化缓存键（去除多余空格，确保不重复）
        normalized_text = self._normalize_key(text)
        
        # 跳过缓存模式：直接调用API，不检查缓存（加快翻译速度，降低推送延迟）
        if not skip_cache:
            # 检查缓存（使用规范化后的键）
            if normalized_text in self.cache:
                return self.cache[normalized_text]
            
            # 快速模式：缓存未命中直接返回
            if quick_mode:
                return text
        
        # 检测语言
        has_korean = bool(re.search(r'[가-힣]', text))
        has_japanese = bool(re.search(r'[ひらがなカタカナ一-龯]', text))
        has_english = bool(re.search(r'[a-zA-Z]', text))
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', text))
        
        if has_chinese and not (has_korean or has_japanese or has_english):
            return text
        
        if force_lang:
            from_lang = force_lang
        elif has_korean:
            from_lang = 'kor'
        elif has_japanese:
            from_lang = 'jp'
        elif has_english:
            from_lang = 'auto'
        else:
            return text
        
        # 调用百度翻译API（使用更短的超时时间以加快响应）
        try:
            api_url = 'http://api.fanyi.baidu.com/api/trans/vip/translate'
            salt = str(random.randint(32768, 65536))
            sign_str = self.config.BAIDU_APP_ID + text + salt + self.config.BAIDU_SECRET_KEY
            sign = hashlib.md5(sign_str.encode('utf-8')).hexdigest()
            
            params = {
                'q': text,
                'from': from_lang,
                'to': 'zh',
                'appid': self.config.BAIDU_APP_ID,
                'salt': salt,
                'sign': sign
            }
            
            # 使用更短的超时时间（2秒）以降低推送延迟
            response = requests.get(api_url, params=params, timeout=2)
            response.raise_for_status()
            result = response.json()
            
            if 'trans_result' in result and result['trans_result']:
                translated = result['trans_result'][0]['dst']
                # 保存到内存缓存（使用规范化后的键，确保不重复）
                with self.lock:
                    self.cache[normalized_text] = translated
                # 立即异步保存到文件（确保缓存持久化，无上限）
                self._async_save_cache()
                self.logger.debug(f"翻译成功: '{text}' -> '{translated}'")
                return translated
            else:
                self.logger.error(f"翻译API错误: {result.get('error_msg', '未知错误')}")
                return text
        except requests.Timeout:
            # 超时情况：返回原文，避免阻塞
            self.logger.warning(f"翻译超时: '{text}'，返回原文")
            return text
        except Exception as e:
            self.logger.error(f"翻译异常: {e}")
            return text
    
    def translate_async(self, text: str, force_lang: Optional[str] = None):
        """异步翻译（后台任务）"""
        def _translate():
            try:
                normalized_text = self._normalize_key(text)
                if normalized_text not in self.cache:
                    self.translate(text, force_lang=force_lang, quick_mode=False)
            except Exception as e:
                self.logger.debug(f"异步翻译失败: {text}, {e}")
        
        threading.Thread(target=_translate, daemon=True, name="AsyncTranslate").start()


# ============================================================================
# 缓存管理器
# ============================================================================

class CacheManager:
    """缓存管理器"""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.lock = threading.RLock()
        
        # 内存缓存（源名称 -> 事件数据）
        self.memory_cache: Dict[str, Dict[str, Any]] = {}
        
        # 融合缓存（预警 WS 5000）
        self.fused_cache_5000: List[Dict] = []
        self.source_index_5000: Dict[str, int] = {}
        
        # MD5缓存（用于Fan Studio数据源去重）
        self.md5_cache: Dict[str, Dict[str, Any]] = {}
        
        # 文件缓存目录
        os.makedirs(config.CACHE_DIR, exist_ok=True)
    
    def save_source_cache(self, source_name: str, data: Dict[str, Any]):
        """保存源缓存到文件（异步）"""
        def _save():
            try:
                cache_file = os.path.join(self.config.CACHE_DIR, f"{source_name}_cache.json")
                cache_data = {
                    "data": data,
                    "last_update": time.time(),
                    "save_time": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                }
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error(f"保存{source_name}缓存失败: {e}")
        
        threading.Thread(target=_save, daemon=True, name=f"SaveCache-{source_name}").start()
    
    def load_source_cache(self, source_name: str) -> Optional[Dict[str, Any]]:
        """从文件加载源缓存"""
        try:
            cache_file = os.path.join(self.config.CACHE_DIR, f"{source_name}_cache.json")
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"加载{source_name}缓存失败: {e}")
        return None
    
    def update_memory_cache(self, source_name: str, data: Dict[str, Any]):
        """更新内存缓存"""
        with self.lock:
            self.memory_cache[source_name] = {
                'data': data,
                'last_update': time.time()
            }
    
    def get_memory_cache(self, source_name: str, max_age: int = None) -> Optional[Dict[str, Any]]:
        """获取内存缓存"""
        max_age = max_age or self.config.CACHE_MAX_AGE
        with self.lock:
            cache = self.memory_cache.get(source_name)
            if cache and (time.time() - cache['last_update']) <= max_age:
                return cache['data']
        return None
    
    def get_fused_cache(self, port: int = 5000) -> List[Dict]:
        """获取融合缓存（端口 5000）"""
        with self.lock:
            if port == 5000:
                return self.fused_cache_5000[:]
        return []
    
    def update_fused_cache(self, port: int, events: List[Dict], source_index: Dict[str, int]):
        """更新融合缓存（端口 5000）"""
        with self.lock:
            if port == 5000:
                self.fused_cache_5000 = events
                self.source_index_5000 = source_index


# ============================================================================
# 数据源基类
# ============================================================================

class DataSource:
    """数据源基类"""

    CWA_EEW_DISPLAY_NAME = "台湾气象署预警"
    CWA_EEW_MUTEX_KEYS = frozenset({"CWA_FS"})

    SOURCE_NAME_MAP = {
        "CUSTOM": "自定义数据源",
        "CEA_PR": "地震局预警",
        "CEA": "中国预警网",
        "CWA_FS": CWA_EEW_DISPLAY_NAME,
        "SA": "美国地质调查局预警",
        "KMA": "韩国气象厅预警",
        "JMA": "日本气象厅预警",
        "EARLY_EST": "Early-est 预警",
    }
    INTERNAL_REGISTRY_IDS = {
        "CUSTOM": "custom",
        "EARLY_EST": "early-est",
    }

    @staticmethod
    def resolve_connected(
        source_key: str,
        source: "DataSource",
        reg_sources: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """解析数据源连接状态（内部源以 status registry 为准）。"""
        reg_id = DataSource.INTERNAL_REGISTRY_IDS.get(source_key)
        if reg_id:
            if reg_sources is None:
                from services.common.source_status import get_source_status_registry
                reg_sources = get_source_status_registry().snapshot().get("sources", {})
            info = reg_sources.get(reg_id)
            if info is not None:
                return bool(info.get("connected"))
        return bool(getattr(source, "connected", False))
    
    def __init__(self, source_key: str, config: Config, logger: logging.Logger, 
                 cache_mgr: CacheManager, translator: TranslationService):
        self.source_key = source_key
        self.source_name = self.SOURCE_NAME_MAP.get(source_key, source_key)
        self.config = config
        self.logger = logger
        self.cache_mgr = cache_mgr
        self.translator = translator
        self.connected = False
        self.lock = threading.RLock()
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        """获取数据（需子类实现）"""
        raise NotImplementedError
    
    def get_target_ports(self) -> List[int]:
        """获取目标端口列表（预警统一 5000）"""
        return [5000]


# ============================================================================
# 自定义数据源
# ============================================================================

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

class FanStudioSource(DataSource):
    """Fan Studio数据源基类"""
    
    def __init__(self, source_key: str, fan_key: str, *args, **kwargs):
        super().__init__(source_key, *args, **kwargs)
        self.fan_key = fan_key  # Fan Studio中的key名称
        self.raw_cache = None
        self.raw_cache_time = 0
        self.event_distributor = None  # 将在主服务中设置
    
    def on_message(self, raw_data: Dict[str, Any], md5: str):
        """处理Fan Studio消息"""
        # CWA 等源用 number 表示报数，与 fetch() 一致，避免 MD5 未变时误丢弃续报
        updates = Utils.safe_int(raw_data.get('number', raw_data.get('updates', 1)), 1)
        
        # MD5去重
        last_info = self.cache_mgr.md5_cache.get(self.source_key, {})
        if md5 == last_info.get('md5', '') and updates <= last_info.get('updates', 0):
            return False
        
        self.cache_mgr.md5_cache[self.source_key] = {'md5': md5, 'updates': updates}
        
        receive_time = time.time()  # 记录接收时间
        
        with self.lock:
            self.raw_cache = raw_data
            self.raw_cache_time = receive_time
            self.connected = True
        
        # 立即处理并推送（原脚本方式）
        try:
            event = self.fetch()
            t_after_fetch = time.time()
            if event and self.event_distributor:
                target_ports = self.get_target_ports()
                self.event_distributor.distribute(self.source_key, event, target_ports)
            t_after_distribute = time.time()
            if event and self.event_distributor:
                # 埋点：收到→fetch / 收到→distribute 结束（毫秒）
                fetch_ms = (t_after_fetch - receive_time) * 1000
                distribute_ms = (t_after_distribute - receive_time) * 1000
                self.logger.debug(f"{self.source_key} 延迟: fetch={fetch_ms:.1f}ms, distribute={distribute_ms:.1f}ms")
                push_delay = (t_after_distribute - receive_time) * 1000  # 毫秒
                self.logger.debug(f"{self.source_key}推送延迟: {push_delay:.1f}ms")

                # 异步保存缓存
                self.cache_mgr.save_source_cache(self.source_key, event)
        except Exception as e:
            self.logger.error(f"{self.source_key}即时推送失败: {e}")
        
        return True

# ============================================================================
# CWA(Fan Studio) 辅助解析
# ============================================================================

def _cwa_origin_time_ms(data: Dict[str, Any]) -> int:
    """shockTime 无效时，尝试其它时间字段（均为 UTC+8）。"""
    for key in ("originTime", "OriginTime", "otime", "createTime", "announcedTime", "AnnouncedTime"):
        val = data.get(key)
        if not val:
            continue
        s = str(val).strip()
        if not s:
            continue
        ms = Utils.parse_time_utc_offset(s, 8)
        if ms > 0:
            return ms
        try:
            if s.endswith("Z"):
                s = s[:-1].strip()
            naive = datetime.strptime(s, "%Y/%m/%d %H:%M:%S")
            utc_time = naive.replace(tzinfo=timezone.utc)
            beijing = utc_time.astimezone(timezone(timedelta(hours=8)))
            return int(beijing.timestamp() * 1000)
        except (ValueError, TypeError):
            continue
    return 0


def _cwa_place_name_from_bracket(raw: str) -> str:
    """从 CWA placeName 括号中提取地名，如「地震（位於花蓮縣光復鄉）」。"""
    if not raw or not isinstance(raw, str):
        return ""
    text = raw.strip()
    if not text:
        return ""
    bracket_match = re.search(r'\(([^)]+)\)', text)
    if bracket_match:
        loc = bracket_match.group(1).replace("位於", "").replace("位于", "")
        return re.sub(r'\s+', ' ', loc).strip()
    return text


def _cwa_place_name_from_payload(
    data: Dict[str, Any],
    lat: float,
    lon: float,
    logger: Optional[logging.Logger] = None,
) -> str:
    """优先使用报文地名，否则按坐标匹配 taiwan_region_data.json。"""
    for key in ("placeName", "epicenterName", "hypoCenter", "HypoCenter", "region", "location"):
        raw = data.get(key)
        if not raw:
            continue
        name = _cwa_place_name_from_bracket(str(raw))
        if name and name not in ("未知地区", "未知地点"):
            return name

    if lat or lon:
        try:
            from services.common.regions import get_taiwan_regions, match_region_by_coords
            regions = get_taiwan_regions()
            if regions:
                matched = match_region_by_coords(
                    regions,
                    lat,
                    lon,
                    bbox_keys=("lat_min", "lat_max", "lon_min", "lon_max", "name"),
                )
                if matched:
                    return matched
        except Exception as e:
            if logger:
                logger.debug(f"CWA 坐标地名匹配失败: lat={lat}, lon={lon}, {e}")

    return "未知地点"


# ============================================================================
# 具体Fan Studio数据源实现
# ============================================================================

class CWAFanStudioSourceV2(FanStudioSource):
    """台湾气象署预警数据源（来自 Fan Studio /all）"""
    
    def __init__(self, *args, **kwargs):
        # 使用独立的 source_key，与经 1450 聚合的 CWA 区分
        super().__init__("CWA_FS", "cwa-eew", *args, **kwargs)
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            shock_time_str = data.get('shockTime', '') or ''
            timestamp_ms = Utils.parse_time_utc_offset(str(shock_time_str).strip(), 8) if shock_time_str else 0
            if timestamp_ms <= 0:
                timestamp_ms = _cwa_origin_time_ms(data)
            if timestamp_ms <= 0:
                self.logger.warning(
                    f"CWA(Fan Studio) 数据时间无效，跳过: {data.get('id', data.get('identifier', 'unknown'))}"
                )
                return None

            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            updates = Utils.safe_int(data.get('number', data.get('updates', 1)), 1)

            lat = Utils.safe_float(data.get('epicenterLat', data.get('latitude', 0)))
            lon = Utils.safe_float(data.get('epicenterLon', data.get('longitude', 0)))
            place_name = _cwa_place_name_from_payload(data, lat, lon, self.logger)

            epicenter = f"{place_name} (CWA)"
            epicenter_tts = Utils.format_epicenter_tts(place_name)

            event_data = {
                "eventId": str(data.get('identifier') or data.get('id') or data.get('eventId', 'unknown')),
                "updates": updates,
                "report_number": updates,
                "latitude": lat,
                "longitude": lon,
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
            self.logger.error(f"处理 CWA(Fan Studio) 数据失败: {e}")
        return None


class CEAPRSource(FanStudioSource):
    """地震局预警数据源"""
    
    def __init__(self, *args, **kwargs):
        super().__init__("CEA_PR", "cea-pr", *args, **kwargs)
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            # 解析基础字段
            shock_time_str = data.get('shockTime', '')
            timestamp_ms = Utils.parse_time_utc_offset(shock_time_str, 8)
            
            # 如果时间无效，返回None（不推送无效数据）
            if timestamp_ms <= 0:
                self.logger.warning(f"CEA_PR数据时间无效，跳过: {data.get('eventId', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            updates = Utils.safe_int(data.get('updates', 1), 1)
            
            province = data.get('province', '')
            suffix = f"({province}地震局)" if province else "(地震局)"
            epicenter = f"{data.get('placeName', '未知地点')} {suffix}"
            
            raw_event_id = str(data.get('eventId', data.get('id', 'unknown')))
            event_id = f"{raw_event_id}-CEA"
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": event_id,
                "updates": updates,
                "report_number": updates,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": Utils.format_epicenter_tts(epicenter),
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": f"{province}地震局预警"
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理CEA_PR数据失败: {e}")
        return None


class CEASource(FanStudioSource):
    """中国预警网数据源"""
    
    def __init__(self, *args, **kwargs):
        super().__init__("CEA", "cea", *args, **kwargs)
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            # 解析基础字段
            shock_time_str = data.get('shockTime', '')
            timestamp_ms = Utils.parse_time_utc_offset(shock_time_str, 8)
            
            # 如果时间无效，返回None
            if timestamp_ms <= 0:
                self.logger.warning(f"CEA数据时间无效，跳过: {data.get('eventId', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            updates = Utils.safe_int(data.get('updates', 1), 1)
            
            epicenter = f"{data.get('placeName', '未知地点')} (CN)"
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": str(data.get('eventId', data.get('id', 'unknown'))),
                "updates": updates,
                "report_number": updates,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": Utils.format_epicenter_tts(epicenter),
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": "中国预警网预警"
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理CEA数据失败: {e}")
        return None


class SASource(FanStudioSource):
    """美国地质调查局数据源"""
    
    def __init__(self, *args, **kwargs):
        super().__init__("SA", "sa", *args, **kwargs)
        self.region_data = self._load_region_data()
    
    def _load_region_data(self) -> List[Dict]:
        """加载SA区域数据"""
        try:
            from services.common.regions import get_sa_regions
            regions = get_sa_regions()
            n = len(regions)
            if n:
                self.logger.info(f"已加载 {n} 个 USGS/SA 地名修正区域 (data/sa_region_data.json)")
            else:
                self.logger.warning("USGS/SA 地名修正为空，请检查 data/sa_region_data.json")
            return regions
        except Exception as e:
            self.logger.error(f"加载SA区域数据失败: {e}")
        return []

    def _apply_region_to_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """按坐标刷新 epicenter（用于缓存回放）。"""
        lat = Utils.safe_float(event.get("latitude", 0))
        lon = Utils.safe_float(event.get("longitude", 0))
        sa_region = self._get_region(lat, lon)
        if sa_region:
            event = dict(event)
            event["epicenter"] = f"{sa_region} (USGS)"
            event["epicenter_tts"] = sa_region
        return event
    
    def _get_region(self, lat: float, lon: float) -> str:
        """根据坐标获取区域名称"""
        for region in self.region_data:
            try:
                if (region.get('lat_min', -90) <= lat <= region.get('lat_max', 90) and
                    region.get('lon_min', -180) <= lon <= region.get('lon_max', 180)):
                    return region.get('name', '')
            except Exception:
                continue
        return ''
    
    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            # 解析基础字段
            shock_time_str = data.get('shockTime', '')
            timestamp_ms = Utils.parse_time_utc_offset(shock_time_str, 8)
            
            # 如果时间无效，返回None
            if timestamp_ms <= 0:
                self.logger.warning(f"SA数据时间无效，跳过: {data.get('eventId', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))
            
            lat = Utils.safe_float(data.get('latitude', 0))
            lon = Utils.safe_float(data.get('longitude', 0))
            
            # 根据坐标从sa_region_data.json中匹配区域（不使用百度翻译）
            sa_region = self._get_region(lat, lon)
            if sa_region:
                # 匹配成功，使用区域名称（已经是中文）
                epicenter = f"{sa_region} (USGS)"
                epicenter_tts = sa_region
            else:
                # 匹配失败，使用原始地名（不调用翻译API）
                original = data.get('placeName', '未知地点')
                epicenter = f"{original} (USGS)"
                epicenter_tts = Utils.format_epicenter_tts(epicenter)
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": str(data.get('eventId', data.get('id', 'unknown'))),
                "updates": 1,
                "report_number": 1,
                "latitude": lat,
                "longitude": lon,
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": epicenter_tts,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理SA数据失败: {e}")
        return None


class KMASource(FanStudioSource):
    """韩国气象厅数据源"""

    def __init__(self, *args, **kwargs):
        super().__init__("KMA", "kma-eew", *args, **kwargs)
        self.korea_region_data = self._load_korea_region_data()

    def _load_korea_region_data(self) -> List[Dict]:
        """加载韩国区域数据"""
        try:
            from services.common.regions import get_korea_regions
            regions = get_korea_regions()
            n = len(regions)
            if n:
                self.logger.info(f"已加载 {n} 个 KMA 地名修正区域 (data/korea_region_data.json)")
            else:
                self.logger.warning("KMA 地名修正为空，请检查 data/korea_region_data.json")
            return regions
        except Exception as e:
            self.logger.error(f"加载韩国区域数据失败: {e}")
        return []

    def _apply_region_to_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        lat = Utils.safe_float(event.get("latitude", 0))
        lon = Utils.safe_float(event.get("longitude", 0))
        korea_region = self._get_korea_region(lat, lon)
        if korea_region:
            event = dict(event)
            event["epicenter"] = f"{korea_region} (KMA)"
            event["epicenter_tts"] = korea_region
        return event

    def _get_korea_region(self, lat: float, lon: float) -> str:
        """根据经纬度获取韩国地名"""
        for region in self.korea_region_data:
            try:
                if (region.get('lat_min', -90) <= lat <= region.get('lat_max', 90) and
                    region.get('lon_min', -180) <= lon <= region.get('lon_max', 180)):
                    return region.get('name', '')
            except Exception:
                continue
        return ''

    def fetch(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache
        
        try:
            # KMA时间为UTC+9，转换为UTC+8
            shock_time_str = data.get('shockTime', '')
            if shock_time_str:
                try:
                    naive_time = datetime.strptime(shock_time_str, "%Y-%m-%d %H:%M:%S")
                    korea_time = naive_time.replace(tzinfo=timezone(timedelta(hours=9)))
                    beijing_time = korea_time.astimezone(timezone(timedelta(hours=8)))
                    timestamp_ms = int(beijing_time.timestamp() * 1000)
                except Exception:
                    timestamp_ms = 0
            else:
                timestamp_ms = 0
            
            # 如果时间无效，返回None
            if timestamp_ms <= 0:
                self.logger.warning(f"KMA数据时间无效，跳过: {data.get('id', 'unknown')}")
                return None
            
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))

            lat = Utils.safe_float(data.get('latitude', 0))
            lon = Utils.safe_float(data.get('longitude', 0))

            # 根据坐标从korea_region_data.json中匹配韩国地名（不使用翻译API）
            korea_region = self._get_korea_region(lat, lon)
            if korea_region:
                # 匹配成功，使用区域名称（已经是中文）
                location = korea_region
            else:
                # 匹配失败，使用原始地名（不调用翻译API）
                place_name = data.get('placeName', '未知地点')
                location = place_name

            epicenter = f"{location} (KMA)"
            
            # 字段顺序与旧脚本保持一致
            event_data = {
                "eventId": str(data.get('eventId', data.get('id', 'unknown'))),
                "updates": 1,
                "report_number": 1,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": location,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name
            }
            
            return event_data
        except Exception as e:
            self.logger.error(f"处理KMA数据失败: {e}")
        return None

class JMAFanStudioSource(FanStudioSource):
    """日本气象厅数据源"""

    def __init__(self, *args, **kwargs):
        super().__init__("JMA", "jma", *args, **kwargs)

    def on_message(self, raw_data: Dict[str, Any], md5: str):
        """处理Fan Studio JMA消息（支持cancel撤销）"""
        if not raw_data or not isinstance(raw_data, dict):
            return False

        # cancel 报文：撤销该事件预警（同时从融合缓存列表移除）
        if raw_data.get('cancel') is True:
            event_id = str(raw_data.get('id', 'unknown'))
            updates = Utils.safe_int(raw_data.get('updates', 1), 1)
            self.cache_mgr.md5_cache[self.source_key] = {'md5': md5, 'updates': updates}
            with self.lock:
                self.raw_cache = raw_data
                self.raw_cache_time = time.time()
                self.connected = True

            if self.event_distributor:
                cancel_event = {
                    "type": "cancel",
                    "source": "JMA",
                    "eventId": event_id,
                    "updates": updates,
                    "timestamp": time.time()
                }
                self.event_distributor.distribute(self.source_key, cancel_event, self.get_target_ports())
            return True

        # 非cancel报文：走通用FanStudioSource逻辑（含MD5去重+即时推送）
        return super().on_message(raw_data, md5)

    def fetch(self) -> Optional[Dict[str, Any]]:
        """解析Fan Studio推送的JMA数据"""
        with self.lock:
            if not self.raw_cache or (time.time() - self.raw_cache_time) > self.config.CACHE_MAX_AGE:
                cached = self.cache_mgr.load_source_cache(self.source_key)
                if cached and cached.get('data'):
                    return cached['data']
                return None
            data = self.raw_cache

        try:
            if data.get('cancel') is True:
                return None

            # shockTime 为 UTC+9，转换为 UTC+8
            shock_time_str = data.get('shockTime', '')
            if shock_time_str:
                try:
                    naive = datetime.strptime(shock_time_str, "%Y-%m-%d %H:%M:%S")
                    tokyo = naive.replace(tzinfo=timezone(timedelta(hours=9)))
                    beijing = tokyo.astimezone(timezone(timedelta(hours=8)))
                    timestamp_ms = int(beijing.timestamp() * 1000)
                except Exception as e:
                    self.logger.warning(f"JMA时间解析失败: {shock_time_str}, {e}")
                    timestamp_ms = 0
            else:
                timestamp_ms = 0

            # 如果时间无效，返回None
            if timestamp_ms <= 0:
                self.logger.warning(f"JMA数据时间无效，跳过: {data.get('id', 'unknown')}")
                return None

            updates = Utils.safe_int(data.get('updates', 1), 1)
            magnitude = Utils.format_magnitude(data.get('magnitude', 0))
            depth = Utils.format_depth(data.get('depth', 0))

            place_name = data.get('placeName', '未知地点')
            info_type = data.get('infoTypeName', '')  # '警報' / '予報'

            # 直接使用原始地名，不进行翻译
            # 保留"警报"处理方式：仅当 infoTypeName == '警報' 才添加前缀
            translated_place = place_name
            if info_type == '警報':
                translated_place = f"（警報）{translated_place}"

            epicenter = f"{translated_place} (JMA)"

            # 构建标准事件数据结构
            event_data = {
                "eventId": str(data.get('id', 'unknown')),
                "updates": updates,
                "report_number": updates,
                "latitude": Utils.safe_float(data.get('latitude', 0)),
                "longitude": Utils.safe_float(data.get('longitude', 0)),
                "depth": depth,
                "epicenter": epicenter,
                "epicenter_tts": translated_place,
                "startAt": timestamp_ms,
                "O_TIME": Utils.format_o_time(timestamp_ms),
                "magnitude": magnitude,
                "source": self.source_name
            }

            # 附加JMA特有字段
            event_data["final"] = bool(data.get("final", False))
            event_data["cancel"] = bool(data.get("cancel", False))
            event_data["epiIntensity"] = data.get("epiIntensity", "")
            event_data["infoTypeName"] = info_type
            event_data["createTime"] = data.get("createTime", "")

            return event_data
        except Exception as e:
            self.logger.error(f"解析JMA数据失败: {e}")
        return None


# ============================================================================
# Wolfx all_eew -> Fan Studio 形态（CEA / JMA）映射
# ============================================================================

def _wolfx_synthetic_md5(parts: Dict[str, Any]) -> str:
    """Wolfx 报文无 md5 时用稳定字段生成去重键。"""
    try:
        s = json.dumps(parts, sort_keys=True, ensure_ascii=False, default=str)
    except TypeError:
        s = str(parts)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _wolfx_cenc_eew_to_cea_raw(wolfx: Dict[str, Any]) -> Dict[str, Any]:
    """cenc_eew -> CEASource.fetch 所读字段（与 Fan Studio cea 对齐）。"""
    depth = wolfx.get("Depth")
    return {
        "shockTime": str(wolfx.get("OriginTime", "")).strip(),
        "placeName": str(wolfx.get("HypoCenter") or wolfx.get("Hypocenter") or ""),
        "updates": Utils.safe_int(wolfx.get("ReportNum"), 1),
        "eventId": str(wolfx.get("EventID") or wolfx.get("ID") or ""),
        "magnitude": wolfx.get("Magnitude"),
        "depth": int(depth) if depth is not None and depth != "" else 0,
        "latitude": Utils.safe_float(wolfx.get("Latitude")),
        "longitude": Utils.safe_float(wolfx.get("Longitude")),
    }


def _wolfx_jma_eew_to_jma_raw(wolfx: Dict[str, Any]) -> Dict[str, Any]:
    """jma_eew -> JMAFanStudioSource 所读字段（与 Fan Studio jma 对齐）；取消报走 cancel 分支。"""
    if wolfx.get("isCancel"):
        return {
            "cancel": True,
            "id": str(wolfx.get("EventID", "unknown")),
            "updates": Utils.safe_int(wolfx.get("Serial"), 1),
        }
    origin = str(wolfx.get("OriginTime", "")).strip()
    hyp = wolfx.get("Hypocenter") or wolfx.get("HypoCenter") or "未知地点"
    mag = wolfx.get("Magunitude")
    if mag is None:
        mag = wolfx.get("Magnitude")
    warn_area = wolfx.get("WarnArea")
    info_type = ""
    if isinstance(warn_area, dict):
        info_type = str(warn_area.get("Type") or "")
    if not info_type:
        info_type = "警報" if wolfx.get("isWarn") else "予報"
    return {
        "shockTime": origin,
        "id": str(wolfx.get("EventID", "unknown")),
        "updates": Utils.safe_int(wolfx.get("Serial"), 1),
        "placeName": hyp,
        "latitude": Utils.safe_float(wolfx.get("Latitude")),
        "longitude": Utils.safe_float(wolfx.get("Longitude")),
        "magnitude": mag,
        "depth": wolfx.get("Depth"),
        "infoTypeName": info_type,
        "final": bool(wolfx.get("isFinal")),
        "cancel": False,
        "epiIntensity": wolfx.get("MaxIntensity", ""),
        "createTime": str(wolfx.get("AnnouncedTime", "")),
    }


# ============================================================================
# WebSocket客户端管理器
# ============================================================================

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
                    ws.run_forever(ping_interval=None, sslopt={"cert_reqs": ssl.CERT_NONE})

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
                    ws.run_forever(ping_interval=None, sslopt={"cert_reqs": ssl.CERT_NONE})
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

class ClientIPManager:
    """客户端IP管理器"""

    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger

        # IP连接统计（仅保存当前仍在线的连接）
        self.ip_connections: Dict[str, Dict] = {}  # ip -> {connections: int, first_seen: float, last_seen: float, ports: Set[int]}

        # 内存中的历史连接记录（仅保留每个IP的最新一条，供“历史记录”命令快速查询）
        self.connection_history: List[Dict[str, Any]] = []

        # 历史连接记录持久化文件（完整历史）
        self.history_file = os.path.join(self.config.CACHE_DIR, "connection_history.jsonl")

        # 黑名单
        self.blacklist: Dict[str, float] = {}  # ip -> 过期时间戳，0表示永久封禁

        # 配置
        self.max_connections_per_ip = 20  # 每个IP最大连接数
        self.connection_timeout = 1800  # 连接超时时间（秒）- 30分钟

        # 文件路径
        self.blacklist_file = os.path.join(self.config.CACHE_DIR, "blacklist.json")

        # 锁
        self.lock = threading.RLock()

        # 清理过期连接的定时器
        self.cleanup_timer = None
        self.start_cleanup_timer()

        # 加载IP配置
        self.load_ip_config()

    def load_ip_config(self):
        """从文件加载IP配置"""
        try:
            # 加载黑名单
            if os.path.exists(self.blacklist_file):
                with open(self.blacklist_file, 'r', encoding='utf-8') as f:
                    blacklist_data = json.load(f)
                with self.lock:
                    self.blacklist = blacklist_data
                self.logger.info(f"已从 {self.blacklist_file} 加载黑名单: {len(self.blacklist)} 个IP")
            else:
                self.logger.info(f"黑名单配置文件 {self.blacklist_file} 不存在，使用空黑名单")


        except Exception as e:
            self.logger.error(f"加载IP配置文件失败: {e}")

        # 历史记录文件不在启动时整体加载，只在需要时按需读取，避免内存占用过大

    def append_history_file(self, history_entry: Dict[str, Any]):
        """将单条历史记录追加写入到独立文件（JSON Lines），用于完整追溯

        注意：只在连接完全断开时写入一次，长期累积形成完整历史。
        """
        try:
            os.makedirs(self.config.CACHE_DIR, exist_ok=True)
            with open(self.history_file, 'a', encoding='utf-8') as f:
                json.dump(history_entry, f, ensure_ascii=False)
                f.write("\n")
        except Exception as e:
            self.logger.error(f"追加写入历史连接文件失败: {e}")

    def load_full_history(self, ip: str = None) -> List[Dict[str, Any]]:
        """从文件读取完整历史记录（可按IP过滤）

        返回值：列表中按文件顺序（时间顺序）排列的原始记录。
        """
        records: List[Dict[str, Any]] = []
        try:
            if not os.path.exists(self.history_file):
                return records
            with open(self.history_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if ip is None or obj.get("IP地址") == ip:
                            records.append(obj)
                    except Exception:
                        # 单条解析失败不影响整体
                        continue
        except Exception as e:
            self.logger.error(f"读取历史连接文件失败: {e}")
        return records

    def save_blacklist(self):
        """保存黑名单到独立文件"""
        try:
            with open(self.blacklist_file, 'w', encoding='utf-8') as f:
                json.dump(self.blacklist, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"黑名单已保存到 {self.blacklist_file}")
        except Exception as e:
            self.logger.error(f"保存黑名单文件失败: {e}")

    def save_ip_config(self):
        """保存IP配置到文件（兼容旧接口）"""
        self.save_blacklist()

    def start_cleanup_timer(self):
        """启动定时任务（当前仅用于保留结构，不再自动清理任何记录）"""
        def cleanup_expired():
            # 按当前需求：不对连接记录和黑名单做任何自动清理，
            # 所有变更仅通过显式管理命令完成
            with self.lock:
                pass

            # 重新调度
            self.cleanup_timer = threading.Timer(300, cleanup_expired)  # 5分钟清理一次
            self.cleanup_timer.daemon = True
            self.cleanup_timer.start()

        cleanup_expired()

    def check_ip_allowed(self, client_ip: str) -> bool:
        """检查IP是否被允许连接"""
        with self.lock:
            # 检查黑名单（包含过期检查）
            if client_ip in self.blacklist:
                expiry = self.blacklist[client_ip]
                if expiry == 0 or time.time() < expiry:  # 永久封禁或未过期
                    return False
                else:  # 已过期，自动移除
                    del self.blacklist[client_ip]
                    self.logger.info(f"IP {client_ip} 黑名单封禁已过期，自动移除")
                    self.save_blacklist()

            return True

    def record_connection(self, client_ip: str, port: int):
        """记录客户端连接"""
        current_time = time.time()

        with self.lock:
            if client_ip not in self.ip_connections:
                self.ip_connections[client_ip] = {
                    'connections': 0,
                    'first_seen': current_time,
                    'last_seen': current_time,
                    'ports': set()
                }

            info = self.ip_connections[client_ip]
            info['connections'] += 1
            info['last_seen'] = current_time
            info['ports'].add(port)

    def record_disconnection(self, client_ip: str, port: int):
        """记录客户端断开"""
        with self.lock:
            if client_ip in self.ip_connections:
                info = self.ip_connections[client_ip]
                # 在修改前先快照端口信息，便于写入历史记录
                ports_snapshot = set(info.get('ports', set()))

                info['connections'] = max(0, info['connections'] - 1)
                if port in info['ports']:
                    info['ports'].discard(port)

                # 更新最后活动时间为断开时间
                disconnect_time = time.time()
                info['last_seen'] = disconnect_time

                # 如果该IP已无任何连接，则将其移入历史记录并从当前连接表中移除
                if info['connections'] == 0:
                    history_entry = {
                        "IP地址": client_ip,
                        "首次连接时间": info.get("first_seen", disconnect_time),
                        "最后活动时间": info.get("last_seen", disconnect_time),
                        "连接端口": sorted(list(ports_snapshot)) if ports_snapshot else [],
                        "断开时间": disconnect_time,
                    }
                    # 1) 内存中只保留该IP最新一条历史，用于“历史记录”命令
                    # 先移除旧记录，再追加新记录，确保每个IP只有一条最新记录
                    self.connection_history = [
                        h for h in self.connection_history if h.get("IP地址") != client_ip
                    ]
                    self.connection_history.append(history_entry)

                    # 2) 追加写入到历史文件，形成完整追溯
                    self.append_history_file(history_entry)
                    # 从当前连接表中移除该IP
                    del self.ip_connections[client_ip]
                    self.logger.debug(f"IP {client_ip} 已断开，移动到历史记录")

    def get_connection_history(self, ip: str = None) -> List[Dict[str, Any]]:
        """获取历史连接记录（每个IP仅保留最新一条，用于快速查看）

        Args:
            ip: 可选，指定IP时仅返回该IP的历史记录
        """
        with self.lock:
            if ip is None:
                return list(self.connection_history)
            return [h for h in self.connection_history if h.get("IP地址") == ip]

    def check_connection_limit(self, client_ip: str) -> bool:
        """检查连接数限制"""
        with self.lock:
            if client_ip in self.ip_connections:
                return self.ip_connections[client_ip]['connections'] < self.max_connections_per_ip
            return True

    @staticmethod
    def parse_duration(duration_str: str) -> int:
        """解析时间字符串为秒数
        
        Args:
            duration_str: 时间字符串，支持格式：30S, 5m, 2h, 1Y
                         支持单位：S(秒), m(分钟), h(小时), Y(年)
        
        Returns:
            秒数，如果解析失败或超出范围则返回None
        
        Raises:
            ValueError: 如果时间字符串格式不正确或超出范围
        """
        if not duration_str or not isinstance(duration_str, str):
            return None
        
        duration_str = duration_str.strip().upper()
        
        # 匹配数字和单位（支持大小写，已转换为大写）
        match = re.match(r'^(\d+)([SMHY])$', duration_str)
        if not match:
            raise ValueError(f"时间格式错误，支持格式：30S, 5m, 2h, 1Y（单位支持大小写）")
        
        value = int(match.group(1))
        unit = match.group(2)
        
        # 转换为秒数
        if unit == 'S':
            seconds = value
        elif unit == 'M':
            seconds = value * 60
        elif unit == 'H':
            seconds = value * 3600
        elif unit == 'Y':
            seconds = value * 365 * 24 * 3600
        else:
            raise ValueError(f"不支持的时间单位: {unit}，支持单位：S/s(秒), m/M(分钟), h/H(小时), Y/y(年)")
        
        # 验证范围：最低30秒，最高1年
        min_seconds = 30
        max_seconds = 365 * 24 * 3600  # 1年
        
        if seconds < min_seconds:
            raise ValueError(f"封禁时长不能低于30秒，当前值: {duration_str}")
        if seconds > max_seconds:
            raise ValueError(f"封禁时长不能超过1年，当前值: {duration_str}")
        
        return seconds

    def add_to_blacklist(self, ip: str, duration: Any = 0):
        """添加到黑名单

        Args:
            ip: IP地址
            duration: 封禁时长，支持以下格式：
                     - 0 或 None: 永久封禁
                     - 整数（秒数）: 直接指定秒数
                     - 字符串: 时间字符串，如 "30S", "5m", "2h", "1Y"
                              支持单位：S(秒), m(分钟), h(小时), Y(年)
                              限制：最低30秒，最高1年
        """
        with self.lock:
            duration_seconds = None
            
            # 处理永久封禁
            if duration == 0 or duration is None:
                self.blacklist[ip] = 0  # 永久封禁
                duration_str = "永久"
            else:
                # 尝试解析时间字符串
                if isinstance(duration, str):
                    try:
                        duration_seconds = self.parse_duration(duration)
                    except ValueError as e:
                        raise ValueError(f"时间解析失败: {e}")
                elif isinstance(duration, (int, float)):
                    # 兼容旧接口：如果是整数，假设是分钟数（向后兼容）
                    # 但如果值很大（>10000），可能是秒数
                    if duration > 10000:
                        duration_seconds = int(duration)
                    else:
                        duration_seconds = int(duration) * 60
                else:
                    raise ValueError(f"不支持的时间格式: {type(duration)}")
                
                # 验证范围
                if duration_seconds is not None:
                    min_seconds = 30
                    max_seconds = 365 * 24 * 3600  # 1年
                    
                    if duration_seconds < min_seconds:
                        raise ValueError(f"封禁时长不能低于30秒")
                    if duration_seconds > max_seconds:
                        raise ValueError(f"封禁时长不能超过1年")
                    
                    expiry = time.time() + duration_seconds
                    self.blacklist[ip] = expiry
                    
                    # 格式化显示时长
                    if duration_seconds < 60:
                        duration_str = f"{duration_seconds}秒"
                    elif duration_seconds < 3600:
                        duration_str = f"{duration_seconds // 60}分钟"
                    elif duration_seconds < 86400:
                        duration_str = f"{duration_seconds // 3600}小时"
                    elif duration_seconds < 365 * 24 * 3600:
                        duration_str = f"{duration_seconds // 86400}天"
                    else:
                        duration_str = f"{duration_seconds // (365 * 24 * 3600)}年"

            self.logger.info(f"IP {ip} 已添加到黑名单，封禁时长: {duration_str}")
            self.save_blacklist()

    def remove_from_blacklist(self, ip: str):
        """从黑名单移除"""
        with self.lock:
            if ip in self.blacklist:
                del self.blacklist[ip]
                self.logger.info(f"IP {ip} 已从黑名单移除")
                self.save_blacklist()

    def get_connection_stats(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        with self.lock:
            total_connections = sum(info['connections'] for info in self.ip_connections.values())
            active_ips = len([ip for ip, info in self.ip_connections.items() if info['connections'] > 0])

            return {
                '总IP数': len(self.ip_connections),
                '活跃IP数': active_ips,
                '总连接数': total_connections,
                '黑名单IP数': len(self.blacklist),
                '每IP最大连接数': self.max_connections_per_ip
            }

    def get_ip_details(self, ip: str = None) -> Dict[str, Any]:
        """获取IP详情"""
        with self.lock:
            if ip:
                if ip in self.ip_connections:
                    return self.ip_connections[ip].copy()
                else:
                    return {}
            else:
                return self.ip_connections.copy()


# ============================================================================
# WebSocket服务器管理器
# ============================================================================

class WebSocketServerManager:
    """WebSocket服务器管理器（向客户端推送数据）"""

    def __init__(self, config: Config, logger: logging.Logger, cache_mgr: CacheManager, ws_client_mgr=None, eew_service=None):
        self.config = config
        self.logger = logger
        self.cache_mgr = cache_mgr
        self.ws_client_mgr = ws_client_mgr
        self.eew_service = eew_service  # EEWService实例，用于访问线程池管理功能
        self.list_engine_module = None  # fused list engine，channel=list 时路由

        # 客户端集合（预警 WS 5000）
        self.clients_5000: Set[Any] = set()
        self.lock_5000 = threading.Lock()

        # 客户端IP管理
        self.client_ip_manager = ClientIPManager(config, logger if 'connection' in str(logger) else logger)
        
        # 广播事件循环
        self.broadcast_loop: Optional[asyncio.AbstractEventLoop] = None
        self._setup_broadcast_loop()

    def get_available_commands(self) -> Dict[str, Any]:
        """获取所有可用管理命令列表"""
        commands = {
            "stats": {
                "description": "获取连接统计信息",
                "json_command": {"command": "stats"},
                "text_commands": ["统计", "STATS", "stats"]
            },
            "history": {
                "description": "获取历史连接记录（每个IP仅保留最新一条）",
                "json_command": {"command": "history", "ip": "可选IP地址"},
                "text_commands": ["历史记录", "HISTORY", "history"]
            },
            "full_history": {
                "description": "获取完整历史连接记录（包含所有连接记录）",
                "json_command": {"command": "full_history", "ip": "可选IP地址"},
                "text_commands": ["完整历史记录", "FULL_HISTORY", "full_history"]
            },
            "ip_details": {
                "description": "获取IP连接详情",
                "json_command": {"command": "ip_details", "ip": "可选IP地址"},
                "text_commands": ["IP详情", "IP_DETAILS", "ip_details"]
            },
            "blacklist_add": {
                "description": "将IP添加到黑名单，支持时间单位：S(秒), m(分钟), h(小时), Y(年)，最低30秒，最高1年",
                "json_command": {"command": "blacklist_add", "ip": "IP地址", "duration": "时间字符串，如30S/5m/2h/1Y，或0表示永久封禁"},
                "text_commands": ["加入黑名单", "BLACKLIST_ADD", "blacklist_add"]
            },
            "blacklist_remove": {
                "description": "从黑名单移除IP",
                "json_command": {"command": "blacklist_remove", "ip": "IP地址"},
                "text_commands": ["移除黑名单", "BLACKLIST_REMOVE", "blacklist_remove"]
            },
            "blacklist_list": {
                "description": "显示黑名单中的所有IP",
                "json_command": {"command": "blacklist_list"},
                "text_commands": ["黑名单列表", "BLACKLIST_LIST", "blacklist_list"]
            },
            "fanstudio_status": {
                "description": "显示Fan Studio连接状态",
                "json_command": {"command": "fanstudio_status"},
                "text_commands": ["服务器状态", "FANSTUDIO_STATUS", "fanstudio_status"]
            },
            "fanstudio_use_backup": {
                "description": "切换至Fan Studio备用服务器并锁定，直至恢复自动切换",
                "json_command": {"command": "fanstudio_use_backup"},
                "text_commands": ["切换备用服务器", "FANSTUDIO_USE_BACKUP", "fanstudio_use_backup"]
            },
            "fanstudio_use_primary": {
                "description": "切换至Fan Studio主服务器并锁定，直至恢复自动切换",
                "json_command": {"command": "fanstudio_use_primary"},
                "text_commands": ["切换主服务器", "FANSTUDIO_USE_PRIMARY", "fanstudio_use_primary"]
            },
            "fanstudio_resume_auto": {
                "description": "清除Fan Studio手动锁定，恢复按健康度自动切换",
                "json_command": {"command": "fanstudio_resume_auto"},
                "text_commands": ["恢复FanStudio自动切换", "FANSTUDIO_RESUME_AUTO", "fanstudio_resume_auto"]
            },
            "cea_jma_wolfx": {
                "description": "CEA(CENC)+JMA 上游切换为 Wolfx all_eew（断开 Fan Studio /all；CEA_PR/CWA_FS/SA/KMA 无新数据直至切回）",
                "json_command": {"command": "cea_jma_wolfx"},
                "text_commands": ["切换wolfx服务器", "WOLFX_UPSTREAM", "wolfx_upstream", "cea_jma_wolfx"]
            },
            "cea_jma_fanstudio": {
                "description": "CEA+JMA 上游切回 Fan Studio /all（断开 Wolfx，恢复全量 Fan 源）",
                "json_command": {"command": "cea_jma_fanstudio"},
                "text_commands": ["切换fan studio服务器", "CEA_JMA_FANSTUDIO_UPSTREAM", "cea_jma_fanstudio"]
            },
            "set_connection_limits": {
                "description": "设置连接数限制参数",
                "json_command": {"command": "set_connection_limits", "max_connections": 20, "timeout": 1800},
                "text_commands": ["设置连接限制", "SET_CONNECTION_LIMITS", "set_connection_limits"]
            },
            "auto_check": {
                "description": "自动检查所有模块状态",
                "json_command": {"command": "auto_check"},
                "text_commands": ["自动检查", "AUTO_CHECK", "auto_check"]
            },
            "source_switches_get": {
                "description": "获取数据源开关状态",
                "json_command": {"command": "source_switches_get", "channel": "eew"},
                "text_commands": ["数据源开关", "SOURCE_SWITCHES_GET", "source_switches_get"]
            },
            "source_switches_set": {
                "description": "设置数据源开关（热更新）",
                "json_command": {"command": "source_switches_set", "channel": "eew", "patch": {"CUSTOM": True}},
                "text_commands": ["设置数据源开关", "SOURCE_SWITCHES_SET", "source_switches_set"]
            },
            "source_filters_get": {
                "description": "获取国外源阈值与地区过滤配置",
                "json_command": {"command": "source_filters_get"},
                "text_commands": ["SOURCE_FILTERS_GET", "source_filters_get"]
            },
            "source_filters_set": {
                "description": "设置国外源阈值与地区过滤（热更新）",
                "json_command": {
                    "command": "source_filters_set",
                    "list_source_threshold": {"usgs": 4.5},
                },
                "text_commands": ["SOURCE_FILTERS_SET", "source_filters_set"]
            },
            "thread_pool_status": {
                "description": "获取线程池运行状态",
                "json_command": {"command": "thread_pool_status"},
                "text_commands": ["线程池实况", "THREAD_POOL_STATUS", "thread_pool_status"]
            },
            "thread_pool_check": {
                "description": "执行线程池健康检查",
                "json_command": {"command": "thread_pool_check"},
                "text_commands": ["线程池检查", "THREAD_POOL_CHECK", "thread_pool_check"]
            },
            "thread_pool_restart": {
                "description": "重启线程池",
                "json_command": {"command": "thread_pool_restart"},
                "text_commands": ["线程池重启", "THREAD_POOL_RESTART", "thread_pool_restart"]
            },
            "all_commands": {
                "description": "显示所有可用管理命令",
                "json_command": {"command": "all_commands"},
                "text_commands": ["全部命令", "ALL_COMMANDS", "all_commands", "命令列表", "帮助", "help"]
            },
            "logout": {
                "description": "退出管理员模式",
                "json_command": {"type": "logout"},
                "text_commands": ["退出", "LOGOUT", "logout", "exit"]
            }
        }
        return commands

    def _resolve_plain_management_command(self, text: str) -> Tuple[str, Dict[str, Any]]:
        """解析管理端口纯文本为 (command, params)。

        已注册的完整文本命令（含空格，如「切换fan studio服务器」）按整行匹配；
        否则按首词为命令、其余为 args（如「加入黑名单 1.2.3.4 30m」）。
        整行匹配使用 casefold，以兼容「切换Fan studio服务器」等大小写变体。
        """
        stripped = (text or "").strip()
        if not stripped:
            return "", {"args": []}
        cf_to_canonical: Dict[str, str] = {}
        for cmd_info in self.get_available_commands().values():
            for tc in cmd_info.get("text_commands", ()):
                ck = tc.casefold()
                if ck not in cf_to_canonical:
                    cf_to_canonical[ck] = tc
        whole_cf = stripped.casefold()
        if whole_cf in cf_to_canonical:
            return cf_to_canonical[whole_cf], {"args": []}
        parts = stripped.split()
        cmd = parts[0] if parts else ""
        return cmd, {"args": parts[1:] if len(parts) > 1 else []}

    async def _send_available_commands(self, websocket, is_json: bool = True):
        """向客户端发送可用命令列表"""
        try:
            commands = self.get_available_commands()

            if is_json:
                # JSON格式：发送结构化的命令列表
                await websocket.send(json.dumps({
                    "type": "available_commands",
                    "message": "以下是可用的管理命令：",
                    "commands": commands
                }))
            else:
                # 纯文本格式：发送格式化的文本列表
                response = "=== 可用管理命令 ===\n\n"
                for cmd_key, cmd_info in commands.items():
                    response += f"{cmd_info['description']}:\n"
                    response += f"  文本命令: {', '.join(cmd_info['text_commands'])}\n"
                    response += f"  JSON示例: {cmd_info['json_command']}\n\n"

                response += "提示：发送对应命令即可执行，JSON格式使用 {\"command\": \"命令名\"} 结构"
                await websocket.send(response)

        except Exception as e:
            self.logger.debug(f"发送可用命令列表失败: {e}")

    def _setup_broadcast_loop(self):
        """设置广播事件循环"""
        def run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.broadcast_loop = loop
            loop.run_forever()
        
        threading.Thread(target=run_loop, daemon=True, name="Broadcast-Loop").start()
        
        # 等待循环启动
        for _ in range(100):
            if self.broadcast_loop:
                break
            time.sleep(0.01)
    
    async def handle_client(self, websocket, port: int):
        """处理客户端连接"""
        # 获取客户端IP地址，增强容错性
        client_addr = websocket.remote_address
        client_ip = 'unknown'

        if client_addr:
            if isinstance(client_addr, tuple) and len(client_addr) > 0:
                client_ip = client_addr[0]
            elif isinstance(client_addr, str):
                # 如果直接是字符串，尝试提取IP
                client_ip = client_addr.split(':')[0] if ':' in client_addr else client_addr
            else:
                # 其他格式，转换为字符串
                client_ip = str(client_addr)
        else:
            # 如果remote_address为None，尝试其他方法获取IP
            try:
                # 检查websocket是否有headers或其他属性
                if hasattr(websocket, 'headers') and websocket.headers:
                    forwarded_for = websocket.headers.get('X-Forwarded-For')
                    if forwarded_for:
                        client_ip = forwarded_for.split(',')[0].strip()
                    else:
                        real_ip = websocket.headers.get('X-Real-IP')
                        if real_ip:
                            client_ip = real_ip.strip()
            except Exception:
                pass

        # 最后确保有有效的IP格式
        if not client_ip or client_ip == 'unknown':
            self.logger.warning(f"[端口{port}] 无法获取客户端IP地址: remote_address={client_addr}")
            client_ip = '0.0.0.0'  # 使用占位符IP确保记录

        # 确保连接被记录（在任何情况下都要记录）
        try:
            self.client_ip_manager.record_connection(client_ip, port)
            self.logger.info(f"[端口{port}] 客户端连接: {client_ip}")

            # 立即验证记录是否成功
            ip_details = self.client_ip_manager.get_ip_details(client_ip)
            if not ip_details:
                self.logger.warning(f"[端口{port}] IP记录验证失败: {client_ip}")
            else:
                self.logger.debug(f"[端口{port}] IP记录验证成功: {client_ip} -> {ip_details.get('connections', 0)}连接")

        except Exception as e:
            self.logger.error(f"[端口{port}] 记录IP连接失败 {client_ip}: {e}")

        # 存储客户端信息以便在finally块中使用
        client_info = (client_ip, client_addr)

        # IP管理检查
        if not self.client_ip_manager.check_ip_allowed(client_ip):
            self.logger.warning(f"[端口{port}] 客户端IP被拒绝: {client_ip}")
            # 被拒绝的连接也要记录断开，以保持计数准确
            self.client_ip_manager.record_disconnection(client_ip, port)
            await websocket.close(code=1008, reason="Access denied")  # 1008 = Policy Violation
            return

        if not self.client_ip_manager.check_connection_limit(client_ip):
            self.logger.warning(f"[端口{port}] 客户端IP连接数超限: {client_ip}")
            # 超限的连接也要记录断开，以保持计数准确
            self.client_ip_manager.record_disconnection(client_ip, port)
            await websocket.close(code=1008, reason="Connection limit exceeded")
            return

        if port != 5000:
            return
        clients, lock = self.clients_5000, self.lock_5000

        # 添加客户端
        with lock:
            clients.add(websocket)

        self.logger.info(f"[端口{port}] 客户端连接: {client_addr}")

        try:
            # 发送初始数据（格式与旧脚本一致）
            current_data = self.cache_mgr.get_fused_cache(port)
            initial_msg = json.dumps(
                {"shuju": current_data},
                ensure_ascii=False,
                separators=(',', ':'),
                check_circular=False
            )
            await websocket.send(initial_msg)
            self.logger.info(f"[端口{port}] 已向客户端推送 {len(current_data)} 条初始数据")
            self.logger.debug(f"[端口{port}] 客户端 {client_addr} 初始数据推送完成")

            # 处理客户端消息
            async for message in websocket:
                try:
                    if isinstance(message, str):    
                        # 尝试解析JSON
                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            # 如果不是JSON，忽略
                            continue

                        # 处理ping消息
                        if data.get("type") == "ping":
                            pong_msg = json.dumps(
                                {"type": "pong"},
                                ensure_ascii=False,
                                separators=(',', ':'),
                                check_circular=False
                            )
                            await websocket.send(pong_msg)

                except Exception as e:
                    self.logger.debug(f"处理客户端消息失败: {e}")
                    pass
        except Exception as e:
            self.logger.error(f"[端口{port}] handle_client 异常: {e}")
        finally:
            with lock:
                clients.discard(websocket)
            # 记录断开连接
            try:
                disconnect_ip, disconnect_addr = client_info
                self.client_ip_manager.record_disconnection(disconnect_ip, port)
                self.logger.info(f"[端口{port}] 客户端断开: {disconnect_addr}")
            except Exception as e:
                self.logger.error(f"[端口{port}] 记录断开连接失败: {e}")
    
    async def broadcast_async(self, message: str, port: int):
        """异步广播消息（优化版：并行发送，最小延迟）"""
        if port != 5000:
            return
        clients, lock = self.clients_5000, self.lock_5000
        
        # 快速复制客户端列表（最小化锁时间）
        with lock:
            if not clients:
                return
            clients_copy = tuple(clients)
        
        # 并行发送到所有客户端（无锁操作，最大化速度）
        async def send_to_client(client):
            try:
                await client.send(message)
                return None
            except Exception:
                return client
        
        # 使用gather并行发送（比循环发送快得多）
        results = await asyncio.gather(*[send_to_client(c) for c in clients_copy], return_exceptions=True)
        
        # 快速清理断开的客户端
        disconnected = [r for r in results if r is not None and not isinstance(r, Exception)]
        if disconnected:
            with lock:
                for client in disconnected:
                    clients.discard(client)
    

    def broadcast(self, message: str, port: int):
        """广播消息（线程安全）"""
        if self.broadcast_loop:
            t_before_schedule = time.time()
            async def _broadcast_with_timing():
                t_async_start = time.time()
                await self.broadcast_async(message, port)
                t_async_end = time.time()
                schedule_delay_ms = (t_async_start - t_before_schedule) * 1000
                send_ms = (t_async_end - t_async_start) * 1000
                self.logger.debug(f"[Broadcast] 端口{port} 调度延迟: {schedule_delay_ms:.1f}ms, 发送耗时: {send_ms:.1f}ms")
            asyncio.run_coroutine_threadsafe(
                _broadcast_with_timing(),
                self.broadcast_loop
            )
    
    def start_server(self, port: int):
        """启动WebSocket服务器"""
        async def handler(websocket, path=None):
            await self.handle_client(websocket, port)

        async def run():
            async with websockets.serve(handler, "0.0.0.0", port, ping_interval=None):
                await asyncio.Future()

        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(run())

        threading.Thread(target=run_in_thread, daemon=True, name=f"WS-Server-{port}").start()
        self.logger.debug(f"WebSocket服务器启动: 端口{port}")

    def set_list_engine_module(self, fl_module) -> None:
        self.list_engine_module = fl_module

    async def _send_mgmt_json(self, websocket, payload: dict, **json_kw) -> None:
        from services.common.mgmt_locale import localize_mgmt_envelope
        kw = {"ensure_ascii": False, "default": str}
        kw.update(json_kw)
        await websocket.send(json.dumps(localize_mgmt_envelope(payload), **kw))

    _MGMT_FANSTUDIO_COMMANDS = frozenset({
        "fanstudio_status", "服务器状态", "FANSTUDIO_STATUS",
        "fanstudio_use_backup", "切换备用服务器", "切换副服务器", "FANSTUDIO_USE_BACKUP",
        "fanstudio_use_primary", "切换主服务器", "FANSTUDIO_USE_PRIMARY",
        "fanstudio_resume_auto", "恢复FanStudio自动切换", "FANSTUDIO_RESUME_AUTO",
    })

    async def _execute_list_channel(self, websocket, command, params, is_json: bool) -> None:
        if not self.list_engine_module:
            msg = "List 模块未挂载"
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
            else:
                await websocket.send(f"ERROR:{msg}")
            return
        from services.list.management_ws import execute_list_command
        try:
            result = await asyncio.to_thread(
                execute_list_command, command, self.list_engine_module, params,
            )
            if is_json:
                await self._send_mgmt_json(websocket, {
                    "type": "result", "command": command, "channel": "list", "data": result,
                })
            else:
                await websocket.send(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        except ValueError as e:
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "error", "message": str(e)})
            else:
                await websocket.send(f"ERROR:{str(e)}")

    async def _execute_both_channel(self, websocket, command, params, is_json: bool) -> None:
        if command in self._MGMT_FANSTUDIO_COMMANDS:
            eew_params = {k: v for k, v in params.items() if k != "channel"}
            await self._execute_management_command(websocket, command, eew_params, is_json)
            return
        if command in ("stats", "统计", "STATS"):
            eew_stats = self.client_ip_manager.get_connection_stats()
            from services.list.management_ws import execute_list_command
            list_stats = await asyncio.to_thread(
                execute_list_command, "stats", self.list_engine_module, {},
            )
            data = {"eew": eew_stats, "list": list_stats}
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "stats", "channel": "both", "data": data})
            else:
                await websocket.send(json.dumps(data, ensure_ascii=False, indent=2))
            return
        if command in ("auto_check", "自动检查", "AUTO_CHECK"):
            eew_check = await self._perform_auto_check()
            from services.list.management_ws import execute_list_command
            list_check = await asyncio.to_thread(
                execute_list_command, "auto_check", self.list_engine_module, {},
            )
            data = {"eew": eew_check, "list": list_check}
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "auto_check", "channel": "both", "data": data})
            else:
                await websocket.send(self._format_check_result_text(eew_check) + "\n\n--- List ---\n" + json.dumps(list_check, ensure_ascii=False, indent=2))
            return
        if command in ("source_status", "SOURCE_STATUS", "数据源状态", "各数据源采集状态"):
            from services.common.source_status import get_source_status_registry
            snap = get_source_status_registry().snapshot()
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "source_status", "channel": "both", "data": snap})
            else:
                lines = ["=== 数据源采集状态 ==="]
                for sid, info in snap.get("sources", {}).items():
                    conn = "已连接" if info.get("connected") else "断开"
                    lines.append(f"{info.get('label', sid)} [{sid}]: {conn}")
                await websocket.send("\n".join(lines))
            return
        msg = f"channel=both 不支持命令: {command}"
        if is_json:
            await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
        else:
            await websocket.send(f"ERROR:{msg}")

    async def _execute_management_command(self, websocket, command, params, is_json=True, is_admin2580=False):
        """执行管理命令"""
        try:
            channel = "eew"
            if is_json:
                channel = (params.get("channel") or "eew").lower()
            if channel == "list":
                await self._execute_list_channel(websocket, command, params, is_json)
                return
            if channel == "both":
                await self._execute_both_channel(websocket, command, params, is_json)
                return

            # 获取参数
            if is_json:
                ip = params.get('ip')
                enabled = params.get('enabled', False)
            else:
                args = params.get('args', [])
                ip = args[0] if args else None
                enabled = len(args) > 0 and args[0].lower() in ('true', '1', 'enable', 'enabled')

            # 执行命令
            if command in ('统计', 'STATS', 'stats'):
                # 获取连接统计信息 - 显示所有客户端IP的连接情况
                stats = self.client_ip_manager.get_connection_stats()
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "stats", "data": stats})
                else:
                    # 纯文本格式化输出
                    response = "=== 连接统计 ===\n"
                    response += f"总IP数: {stats.get('总IP数', 'N/A')}\n"
                    response += f"活跃IP数: {stats.get('活跃IP数', 'N/A')}\n"
                    response += f"总连接数: {stats.get('总连接数', 'N/A')}\n"
                    response += f"黑名单IP数: {stats.get('黑名单IP数', 'N/A')}\n"
                    response += f"每IP最大连接数: {stats.get('每IP最大连接数', 'N/A')}"
                    await websocket.send(response)

            elif command in ('历史记录', 'HISTORY', 'history'):
                # 获取历史连接记录 - 显示已断开的IP连接信息（每个IP仅保留最新一条）
                if is_json:
                    ip_filter = params.get('ip')
                else:
                    args = params.get('args', [])
                    ip_filter = args[0] if args else None

                history_raw = self.client_ip_manager.get_connection_history(ip_filter)

                # 在管理端口返回前，把时间戳统一格式化为 YYYY/MM/DD HH:MM:SS
                def _fmt(ts):
                    try:
                        return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")
                    except Exception:
                        return ts

                history = []
                for item in history_raw:
                    new_item = dict(item)
                    if isinstance(new_item.get("首次连接时间"), (int, float)):
                        new_item["首次连接时间"] = _fmt(new_item["首次连接时间"])
                    if isinstance(new_item.get("最后活动时间"), (int, float)):
                        new_item["最后活动时间"] = _fmt(new_item["最后活动时间"])
                    if isinstance(new_item.get("断开时间"), (int, float)):
                        new_item["断开时间"] = _fmt(new_item["断开时间"])
                    history.append(new_item)

                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "history", "data": history})
                else:
                    if not history:
                        response = "=== 历史连接记录 ===\n(无历史记录)\n"
                    else:
                        response_lines = ["=== 历史连接记录 ==="]
                        # 按断开时间倒序显示，最近断开的在前
                        for item in sorted(history, key=lambda x: x.get("断开时间", 0), reverse=True):
                            ip_addr = item.get("IP地址", "未知IP")
                            first_seen = item.get("首次连接时间", "N/A")
                            last_seen = item.get("最后活动时间", "N/A")
                            disconnected_at = item.get("断开时间", "N/A")
                            ports = item.get("连接端口", [])
                            response_lines.append(f"IP: {ip_addr}")
                            response_lines.append(f"  首次连接时间: {first_seen}")
                            response_lines.append(f"  最后活动时间: {last_seen}")
                            response_lines.append(f"  断开时间: {disconnected_at}")
                            response_lines.append(f"  连接端口: {ports}")
                            response_lines.append("")  # 空行分隔
                        response = "\n".join(response_lines)

                    await websocket.send(response)

            elif command in ('完整历史记录', 'FULL_HISTORY', 'full_history'):
                # 获取完整历史连接记录 - 从独立文件中读取所有连接记录
                if is_json:
                    ip_filter = params.get('ip')
                else:
                    args = params.get('args', [])
                    ip_filter = args[0] if args else None

                history_raw = self.client_ip_manager.load_full_history(ip_filter)

                # 在管理端口返回前，把时间戳统一格式化为 YYYY/MM/DD HH:MM:SS
                def _fmt_full(ts):
                    try:
                        return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")
                    except Exception:
                        return ts

                history = []
                for item in history_raw:
                    new_item = dict(item)
                    if isinstance(new_item.get("首次连接时间"), (int, float)):
                        new_item["首次连接时间"] = _fmt_full(new_item["首次连接时间"])
                    if isinstance(new_item.get("最后活动时间"), (int, float)):
                        new_item["最后活动时间"] = _fmt_full(new_item["最后活动时间"])
                    if isinstance(new_item.get("断开时间"), (int, float)):
                        new_item["断开时间"] = _fmt_full(new_item["断开时间"])
                    history.append(new_item)

                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "full_history", "data": history})
                else:
                    if not history:
                        response = "=== 完整历史记录 ===\n(无历史记录)\n"
                    else:
                        response_lines = ["=== 完整历史记录 ==="]
                        # 按断开时间倒序显示，最近断开的在前
                        for item in sorted(history, key=lambda x: x.get("断开时间", 0), reverse=True):
                            ip_addr = item.get("IP地址", "未知IP")
                            first_seen = item.get("首次连接时间", "N/A")
                            last_seen = item.get("最后活动时间", "N/A")
                            disconnected_at = item.get("断开时间", "N/A")
                            ports = item.get("连接端口", [])
                            response_lines.append(f"IP: {ip_addr}")
                            response_lines.append(f"  首次连接时间: {first_seen}")
                            response_lines.append(f"  最后活动时间: {last_seen}")
                            response_lines.append(f"  断开时间: {disconnected_at}")
                            response_lines.append(f"  连接端口: {ports}")
                            response_lines.append("")  # 空行分隔
                        response = "\n".join(response_lines)

                    await websocket.send(response)

            elif command in ('IP详情', 'IP_DETAILS', 'ip_details'):
                details_raw = self.client_ip_manager.get_ip_details(ip)

                # 仅在管理端口返回时格式化时间
                def _fmt(ts):
                    try:
                        return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")
                    except Exception:
                        return ts

                if ip:
                    details = dict(details_raw) if details_raw else {}
                    if isinstance(details.get('first_seen'), (int, float)):
                        details['first_seen'] = _fmt(details['first_seen'])
                    if isinstance(details.get('last_seen'), (int, float)):
                        details['last_seen'] = _fmt(details['last_seen'])
                else:
                    # 所有IP时，保持结构不变，只在文本输出里做格式化
                    details = details_raw if details_raw else {}

                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "ip_details", "data": details})
                else:
                    if ip and details:
                        response = f"=== IP {ip} 详情 ===\n"
                        first_seen = details.get('first_seen', 'N/A')
                        last_seen = details.get('last_seen', 'N/A')
                        if isinstance(first_seen, (int, float)):
                            first_seen = _fmt(first_seen)
                        if isinstance(last_seen, (int, float)):
                            last_seen = _fmt(last_seen)
                        response += f"连接数: {details.get('connections', 0)}\n"
                        response += f"首次连接: {first_seen}\n"
                        response += f"最后连接: {last_seen}\n"
                        response += f"连接端口: {list(details.get('ports', []))}"
                    elif not ip:
                        response = "=== 所有IP详情 ===\n"
                        # 按端口分类显示IP
                        port_ips = {5000: []}
                        # 确保details是字典类型
                        if isinstance(details, dict):
                            for ip_addr, info in details.items():
                                if isinstance(info, dict):
                                    ports = info.get('ports', set())
                                    # ports可能是set或list，统一处理
                                    if isinstance(ports, set):
                                        ports = list(ports)
                                    for port in ports:
                                        if port in port_ips:
                                            port_ips[port].append(ip_addr)

                        for port in [5000]:
                            response += f"[{port}]端口\n"
                            if port_ips[port]:
                                for ip_addr in sorted(port_ips[port]):
                                    response += f"{ip_addr}\n"
                            else:
                                response += "(无连接)\n"
                            response += "\n"  # 端口间空行分隔
                    else:
                        response = f"IP {ip} 未找到"
                    await websocket.send(response)

            elif command in ('加入黑名单', 'BLACKLIST_ADD', 'blacklist_add'):
                try:
                    if is_json:
                        ip = params.get('ip')
                        duration = params.get('duration', 0)  # 默认为永久封禁，支持字符串格式如 "30S", "5m", "2h", "1Y"
                    else:
                        args = params.get('args', [])
                        ip = args[0] if len(args) > 0 else None
                        # 第二个参数是时间，可能是字符串格式如 "30S" 或数字
                        if len(args) > 1:
                            duration_str = str(args[1])
                            # 尝试解析为整数（兼容旧格式）
                            try:
                                duration = int(duration_str)
                            except ValueError:
                                # 如果不是纯数字，当作时间字符串处理
                                duration = duration_str
                        else:
                            duration = 0

                    if ip:
                        self.client_ip_manager.add_to_blacklist(ip, duration)
                        # 断开该IP的所有现有连接
                        disconnected_count = await self.disconnect_ip(ip)
                        
                        # 格式化消息
                        if duration == 0 or duration is None:
                            msg = f"IP {ip} 已添加到黑名单，永久封禁"
                        else:
                            # 显示原始输入的时间格式
                            if isinstance(duration, str):
                                msg = f"IP {ip} 已添加到黑名单，封禁 {duration}"
                            else:
                                # 兼容旧格式：如果是数字，显示为分钟
                                if duration > 10000:
                                    msg = f"IP {ip} 已添加到黑名单，封禁 {duration} 秒"
                                else:
                                    msg = f"IP {ip} 已添加到黑名单，封禁 {duration} 分钟"
                        
                        if disconnected_count > 0:
                            msg += f"，已断开 {disconnected_count} 个现有连接"
                    else:
                        msg = "需要指定IP地址"
                    
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": bool(ip), "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                except ValueError as e:
                    error_msg = f"添加黑名单失败: {str(e)}"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": error_msg})
                    else:
                        await websocket.send(f"ERROR:{error_msg}")
                except Exception as e:
                    error_msg = f"添加黑名单时发生错误: {str(e)}"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": error_msg})
                    else:
                        await websocket.send(f"ERROR:{error_msg}")

            elif command in ('移除黑名单', 'BLACKLIST_REMOVE', 'blacklist_remove'):
                if ip:
                    self.client_ip_manager.remove_from_blacklist(ip)
                    msg = f"IP {ip} 已从黑名单移除，现在可以正常连接"
                else:
                    msg = "需要指定IP地址"
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "result", "success": bool(ip), "message": msg})
                else:
                    await websocket.send(f"RESULT:{msg}")

            elif command in ('黑名单列表', 'BLACKLIST_LIST', 'blacklist_list'):
                blacklist = self.client_ip_manager.blacklist
                if is_json:
                    # 转换格式：包含过期时间信息
                    data = {}
                    for ip, expiry in blacklist.items():
                        if expiry == 0:
                            data[ip] = {"type": "permanent"}
                        else:
                            remaining_minutes = max(0, int((expiry - time.time()) / 60))
                            data[ip] = {"type": "temporary", "remaining_minutes": remaining_minutes}
                    await self._send_mgmt_json(websocket, {"type": "blacklist_list", "data": data})
                else:
                    if blacklist:
                        response = "=== 黑名单列表 ===\n"
                        for ip in sorted(blacklist.keys()):
                            expiry = blacklist[ip]
                            if expiry == 0:
                                response += f"• {ip} (永久封禁)\n"
                            else:
                                remaining_minutes = max(0, int((expiry - time.time()) / 60))
                                response += f"• {ip} (剩余 {remaining_minutes} 分钟)\n"
                        response = response.rstrip()  # 移除最后的换行符
                    else:
                        response = "黑名单为空"
                    await websocket.send(response)

            elif command in ('source_status', 'SOURCE_STATUS', '数据源状态'):
                from services.common.source_status import get_source_status_registry
                snap = get_source_status_registry().snapshot()
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_status", "data": snap})
                else:
                    lines = ["=== 数据源采集状态 ==="]
                    for sid, info in snap.get("sources", {}).items():
                        conn = "已连接" if info.get("connected") else "断开"
                        lines.append(f"{info.get('label', sid)} [{sid}]: {conn}, 消息数={info.get('message_count', 0)}")
                    await websocket.send("\n".join(lines))

            elif command in ('服务器状态', 'FANSTUDIO_STATUS', 'fanstudio_status'):
                if self.ws_client_mgr:
                    status = {
                        'current_server': self.ws_client_mgr.current_server_url,
                        'manual_target': self.ws_client_mgr.fanstudio_manual_target,
                        'cea_jma_upstream': self.ws_client_mgr.cea_jma_upstream,
                        'wolfx_url': self.ws_client_mgr.config.WOLFX_ALL_EEW_URL,
                        'primary_health': self.ws_client_mgr.connection_health['primary_server_health'],
                        'backup_health': self.ws_client_mgr.connection_health['backup_server_health'],
                        'connection_quality': self.ws_client_mgr.connection_health['connection_quality'],
                        'fail_streak': self.ws_client_mgr.connection_stats['current_fail_streak'],
                        'server_switches': self.ws_client_mgr.connection_stats['server_switch_count']
                    }
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "fanstudio_status", "data": status})
                    else:
                        response = "=== Fan Studio连接状态 ===\n"
                        response += f"当前服务器: {status['current_server']}\n"
                        response += f"CEA/JMA 上游: {status.get('cea_jma_upstream', 'fanstudio')}（wolfx 时见 Wolfx URL）\n"
                        if status.get('cea_jma_upstream') == 'wolfx':
                            response += f"Wolfx: {status.get('wolfx_url', '')}\n"
                        mt = status.get('manual_target')
                        if mt:
                            response += f"手动锁定: {mt}（自动切换已暂停，请发恢复FanStudio自动切换）\n"
                        else:
                            response += "手动锁定: 无（自动切换已启用）\n"
                        response += f"主服务器健康度: {status['primary_health']:.2f}\n"
                        response += f"备用服务器健康度: {status['backup_health']:.2f}\n"
                        response += f"连接质量: {status['connection_quality']}\n"
                        response += f"连续失败次数: {status['fail_streak']}\n"
                        response += f"服务器切换次数: {status['server_switches']}"
                        await websocket.send(response)
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换wolfx服务器', 'WOLFX_UPSTREAM', 'wolfx_upstream', 'cea_jma_wolfx'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_cea_jma_to_wolfx)
                    msg = (
                        "已切换 CEA/JMA 至 Wolfx all_eew，Fan Studio /all 已断开；"
                        "CEA_PR/CWA_FS/SA/KMA 等无推送直至发送「切换fan studio服务器」"
                    )
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换fan studio服务器', 'CEA_JMA_FANSTUDIO_UPSTREAM', 'cea_jma_fanstudio'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_cea_jma_to_fanstudio)
                    msg = "已切换 CEA/JMA 回 Fan Studio /all，Wolfx 已断开，Fan Studio 将重连"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换备用服务器', '切换副服务器', 'FANSTUDIO_USE_BACKUP', 'fanstudio_use_backup'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_fanstudio_to_backup)
                    msg = "已切换至 Fan Studio 备用服务器并锁定，自动切换已暂停；需恢复请发送 fanstudio_resume_auto / 恢复FanStudio自动切换"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换主服务器', 'FANSTUDIO_USE_PRIMARY', 'fanstudio_use_primary'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_fanstudio_to_primary)
                    msg = "已切换至 Fan Studio 主服务器并锁定，自动切换已暂停；需恢复请发送 fanstudio_resume_auto / 恢复FanStudio自动切换"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('恢复FanStudio自动切换', 'FANSTUDIO_RESUME_AUTO', 'fanstudio_resume_auto'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.fanstudio_resume_auto_switch)
                    msg = "已清除 Fan Studio 手动锁定，恢复按健康度自动切换"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('设置连接限制', 'SET_CONNECTION_LIMITS', 'set_connection_limits'):
                if is_json:
                    max_connections = params.get('max_connections', 20)
                    timeout = params.get('timeout', 1800)
                else:
                    # 从文本命令解析参数
                    args = params.get('args', [])
                    max_connections = int(args[0]) if len(args) > 0 else 20
                    timeout = int(args[1]) if len(args) > 1 else 1800

                # 更新设置
                old_max = self.client_ip_manager.max_connections_per_ip
                old_timeout = self.client_ip_manager.connection_timeout

                self.client_ip_manager.max_connections_per_ip = max_connections
                self.client_ip_manager.connection_timeout = timeout

                msg = f"连接限制已更新: 最大连接数 {old_max} -> {max_connections}, 超时时间 {old_timeout} -> {timeout}秒"
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                else:
                    await websocket.send(f"RESULT:{msg}")

            elif command in ('自动检查', 'AUTO_CHECK', 'auto_check'):
                # 执行自动检查
                check_result = await self._perform_auto_check()
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "auto_check", "data": check_result})
                else:
                    response = self._format_check_result_text(check_result)
                    await websocket.send(response)

            elif command in ('线程池实况', 'THREAD_POOL_STATUS', 'thread_pool_status'):
                # 获取线程池运行状态
                if self.eew_service:
                    status = self.eew_service.get_thread_pool_status()
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "thread_pool_status", "data": status})
                    else:
                        response = "=== 线程池运行状态 ===\n"
                        response += f"状态: {status.get('状态', 'N/A')}\n"
                        response += f"最大工作线程数: {status.get('最大工作线程数', 'N/A')}\n"
                        response += f"活动线程数: {status.get('活动线程数', 'N/A')}\n"
                        response += f"队列大小: {status.get('队列大小', 'N/A')}\n"
                        response += f"创建时间: {status.get('创建时间', 'N/A')}\n"
                        response += f"运行时间: {status.get('运行时间', 'N/A')}\n"
                        response += f"总任务数: {status.get('总任务数', 'N/A')}"
                        await websocket.send(response)
                else:
                    msg = "EEWService不可用，无法获取线程池状态"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('线程池检查', 'THREAD_POOL_CHECK', 'thread_pool_check'):
                # 执行线程池健康检查
                if self.eew_service:
                    check_result = self.eew_service.check_thread_pool()
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "thread_pool_check", "data": check_result})
                    else:
                        response = "=== 线程池健康检查 ===\n"
                        response += f"健康状态: {check_result.get('健康状态', 'N/A')}\n"
                        response += f"检查时间: {check_result.get('时间戳', 'N/A')}\n\n"
                        
                        status = check_result.get('状态', {})
                        response += "运行状态:\n"
                        response += f"  状态: {status.get('状态', 'N/A')}\n"
                        response += f"  最大工作线程数: {status.get('最大工作线程数', 'N/A')}\n"
                        response += f"  活动线程数: {status.get('活动线程数', 'N/A')}\n"
                        response += f"  队列大小: {status.get('队列大小', 'N/A')}\n"
                        response += f"  运行时间: {status.get('运行时间', 'N/A')}\n"
                        response += f"  总任务数: {status.get('总任务数', 'N/A')}\n\n"
                        
                        issues = check_result.get('异常问题', [])
                        if issues:
                            response += "异常问题:\n"
                            for issue in issues:
                                response += f"  ⚠️ {issue}\n"
                            response += "\n"
                        
                        warnings = check_result.get('警告信息', [])
                        if warnings:
                            response += "警告信息:\n"
                            for warning in warnings:
                                response += f"  ⚡ {warning}\n"
                        
                        if not issues and not warnings:
                            response += "✓ 线程池运行正常，无异常"
                        
                        await websocket.send(response)
                else:
                    msg = "EEWService不可用，无法执行线程池检查"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('线程池重启', 'THREAD_POOL_RESTART', 'thread_pool_restart'):
                # 重启线程池（后台执行），避免阻塞管理端口
                if self.eew_service:
                    # 在独立线程中执行实际的重启逻辑，管理端口仅负责下发指令
                    def _restart_worker():
                        result = self.eew_service.restart_thread_pool()
                        logger = self.eew_service.log_mgr.get_logger('data')
                        logger.info(f"[管理命令] 线程池重启任务完成: {result.get('消息', 'N/A')}")
                    threading.Thread(target=_restart_worker, daemon=True, name="ThreadPool-Restart").start()

                    if is_json:
                        # 立即返回“已下发”状态，重启过程在后台执行
                        await self._send_mgmt_json(websocket, {
                            "type": "thread_pool_restart",
                            "data": {
                                "started": True,
                                "message": "线程池重启命令已下发，重启过程在后台执行，请稍后通过 thread_pool_status / thread_pool_check 查询最新状态"
                            },
                        })
                    else:
                        response = "=== 线程池重启命令已下发 ===\n"
                        response += "重启将在后台执行，管理端口不会被阻塞。\n"
                        response += "请稍后通过「线程池实况」或「线程池检查」查看最新状态。"
                        await websocket.send(response)
                else:
                    msg = "EEWService不可用，无法重启线程池"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('数据源开关', 'SOURCE_SWITCHES_GET', 'source_switches_get'):
                from services.common.source_switches import get_registry, EEW_SOURCE_NAMES, LIST_SOURCE_NAMES
                ch = params.get('channel', 'eew') if is_json else 'eew'
                snap = get_registry(ch).snapshot()
                names = EEW_SOURCE_NAMES if ch == 'eew' else LIST_SOURCE_NAMES
                payload = {"channel": ch, "switches": snap, "names": names}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_switches", "data": payload})
                else:
                    lines = [f"=== 数据源开关 ({ch}) ==="]
                    for k, v in snap.items():
                        lines.append(f"  {k}: {'开' if v else '关'}")
                    await websocket.send("\n".join(lines))

            elif command in ('设置数据源开关', 'SOURCE_SWITCHES_SET', 'source_switches_set'):
                from services.common.source_switches import apply_eew_patch, save_to_settings_path
                patch = params.get('patch', {}) if is_json else {}
                if not patch and is_json:
                    patch = {k: v for k, v in params.items() if k not in ('command', 'channel') and isinstance(v, bool)}
                apply_eew_patch(patch) if patch else []
                evicted = []
                if self.eew_service and self.eew_service.distributor:
                    dist = self.eew_service.distributor
                    for sid, enabled in (patch or {}).items():
                        if enabled is False:
                            dist.evict_source(sid)
                            evicted.append(sid)
                save_to_settings_path()
                result = {
                    "ok": True,
                    "patch": patch,
                    "disabled_by_mutex": [],
                    "evicted": list(set(evicted)),
                    "republished": [],
                }
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_switches_set", "data": result})
                else:
                    await websocket.send(json.dumps(result, ensure_ascii=False, indent=2))

            elif command in (
                '设置自定义数据源URL',
                'CUSTOM_DATA_SOURCE_URL_SET',
                'custom_data_source_url_set',
            ):
                from services.common.source_switches import (
                    is_eew_enabled,
                    set_custom_data_source_url,
                )
                from services.internal import custom as custom_internal
                url = params.get('url', '') if is_json else ''
                set_custom_data_source_url(url)
                started = False
                if url and is_eew_enabled('CUSTOM'):
                    thread = custom_internal.start()
                    started = thread is not None and thread.is_alive()
                else:
                    custom_internal.stop()
                result = {"ok": True, "url": url, "started": started}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "custom_data_source_url_set", "data": result})
                else:
                    await websocket.send(json.dumps(result, ensure_ascii=False, indent=2))

            elif command in ('SOURCE_FILTERS_GET', 'source_filters_get'):
                from services.common.source_filters import get_filter_registry
                payload = {"filters": get_filter_registry().snapshot()}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_filters", "data": payload})
                else:
                    await websocket.send(json.dumps(payload, ensure_ascii=False, indent=2))

            elif command in ('SOURCE_FILTERS_SET', 'source_filters_set'):
                from services.common.source_filters import get_filter_registry, save_to_settings_path
                reg = get_filter_registry()
                reg.apply_patch(
                    list_threshold=params.get('list_source_threshold'),
                    list_region_filter=params.get('list_source_region_filter'),
                    eew_threshold=params.get('eew_source_threshold'),
                    eew_region_filter=params.get('eew_source_region_filter'),
                )
                save_to_settings_path()
                result = {"ok": True, "filters": reg.snapshot()}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_filters_set", "data": result})
                else:
                    await websocket.send(json.dumps(result, ensure_ascii=False, indent=2))

            elif command in ('全部命令', 'ALL_COMMANDS', 'all_commands', '命令列表', '帮助', 'help'):
                # 发送所有可用命令列表
                await self._send_available_commands(websocket, is_json)

            else:
                msg = f"未知命令: {command}"
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                else:
                    await websocket.send(f"ERROR:{msg}")

        except Exception as e:
            error_msg = str(e)
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "error", "message": error_msg})
            else:
                await websocket.send(f"ERROR:{error_msg}")

    async def disconnect_ip(self, target_ip: str):
        """断开指定IP的所有连接"""
        disconnected_count = 0

        # 检查所有端口的客户端集合
        client_sets = [(self.clients_5000, self.lock_5000, 5000)]

        for clients, lock, port in client_sets:
            with lock:
                # 收集需要断开的websocket连接
                to_disconnect = []
                for ws in clients:
                    try:
                        client_addr = ws.remote_address
                        client_ip = client_addr[0] if client_addr else 'unknown'
                        if client_ip == target_ip:
                            to_disconnect.append(ws)
                    except Exception as e:
                        self.logger.debug(f"获取客户端IP失败: {e}")

                # 断开连接并从集合中移除
                for ws in to_disconnect:
                    try:
                        await ws.close(code=1008, reason="Access denied - IP blacklisted")
                        clients.discard(ws)
                        # 记录断开连接，减少连接计数
                        self.client_ip_manager.record_disconnection(target_ip, port)
                        disconnected_count += 1
                        self.logger.info(f"[端口{port}] 断开黑名单IP连接: {target_ip}")
                    except Exception as e:
                        self.logger.debug(f"断开连接失败: {e}")

        if disconnected_count > 0:
            self.logger.info(f"已断开IP {target_ip} 的 {disconnected_count} 个连接")
        else:
            self.logger.debug(f"IP {target_ip} 没有找到活跃连接")

        return disconnected_count

    async def _perform_auto_check(self) -> Dict[str, Any]:
        """执行自动检查所有模块"""
        check_result = {
            "timestamp": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
            "modules": {},
            "summary": {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "warnings": 0
            }
        }

        # 1. 检查数据源连接状态
        data_sources_status = {}
        if self.ws_client_mgr and self.ws_client_mgr.sources:
            from services.common.source_status import get_source_status_registry
            reg_sources = get_source_status_registry().snapshot().get("sources", {})
            if hasattr(self.ws_client_mgr, "_sync_internal_source_connected"):
                self.ws_client_mgr._sync_internal_source_connected()
            for source_key, source in self.ws_client_mgr.sources.items():
                try:
                    source_name = DataSource.SOURCE_NAME_MAP.get(source_key, source_key)
                    
                    is_connected = DataSource.resolve_connected(source_key, source, reg_sources)
                    status = "正常" if is_connected else "未连接"
                    
                    extra_info: Dict[str, Any] = {}
                    ds_entry = {
                        "status": status,
                        "连接状态": "已连接" if is_connected else "未连接",
                        "result": "通过" if is_connected else "警告",
                    }
                    ds_entry.update(extra_info)
                    
                    data_sources_status[source_name] = ds_entry
                    check_result["summary"]["total"] += 1
                    if is_connected:
                        check_result["summary"]["passed"] += 1
                    else:
                        check_result["summary"]["warnings"] += 1
                except Exception as e:
                    data_sources_status[source_key] = {
                        "status": "检查失败",
                        "error": str(e),
                        "result": "失败"
                    }
                    check_result["summary"]["total"] += 1
                    check_result["summary"]["failed"] += 1

        check_result["modules"]["数据源"] = data_sources_status

        # 2. 检查WebSocket服务器状态
        ws_servers_status = {}
        for port in (5000,):
            try:
                client_count = len(self.clients_5000)
                cache_data = self.cache_mgr.get_fused_cache(port)
                cache_count = len(cache_data) if cache_data else 0

                ws_servers_status[f"端口{port}"] = {
                    "status": "运行中",
                    "客户端数": client_count,
                    "缓存数": cache_count,
                    "result": "通过"
                }
                check_result["summary"]["total"] += 1
                check_result["summary"]["passed"] += 1
            except Exception as e:
                ws_servers_status[f"端口{port}"] = {
                    "status": "检查失败",
                    "error": str(e),
                    "result": "失败"
                }
                check_result["summary"]["total"] += 1
                check_result["summary"]["failed"] += 1

        check_result["modules"]["WebSocket服务器"] = ws_servers_status

        # 3. 检查管理服务器状态
        try:
            # 管理服务器在2050端口，这里假设它正在运行（因为能收到命令说明服务器正常）
            check_result["modules"]["管理服务器"] = {
                "端口2050": {
                    "status": "运行中",
                    "result": "通过"
                }
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["passed"] += 1
        except Exception as e:
            check_result["modules"]["管理服务器"] = {
                "端口2050": {
                    "status": "检查失败",
                    "error": str(e),
                    "result": "失败"
                }
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        # 4. 检查Fan Studio连接状态
        if self.ws_client_mgr:
            try:
                connection_quality = self.ws_client_mgr.connection_health['connection_quality']
                quality_map = {
                    'good': '良好',
                    'fair': '一般',
                    'poor': '较差',
                    'unknown': '未知'
                }
                fanstudio_status = {
                    "当前服务器": self.ws_client_mgr.current_server_url,
                    "主服务器健康度": round(self.ws_client_mgr.connection_health['primary_server_health'], 2),
                    "备用服务器健康度": round(self.ws_client_mgr.connection_health['backup_server_health'], 2),
                    "连接质量": quality_map.get(connection_quality, connection_quality),
                    "连续失败次数": self.ws_client_mgr.connection_stats['current_fail_streak'],
                    "result": "通过" if connection_quality != 'poor' else "警告"
                }
                check_result["modules"]["Fan Studio连接"] = fanstudio_status
                check_result["summary"]["total"] += 1
                if fanstudio_status["result"] == "通过":
                    check_result["summary"]["passed"] += 1
                else:
                    check_result["summary"]["warnings"] += 1
            except Exception as e:
                check_result["modules"]["Fan Studio连接"] = {
                    "status": "检查失败",
                    "error": str(e),
                    "result": "失败"
                }
                check_result["summary"]["total"] += 1
                check_result["summary"]["failed"] += 1

        # 5. 检查缓存管理器
        try:
            cache_status = {
                "内存缓存数量": len(self.cache_mgr.memory_cache),
                "融合缓存5000": len(self.cache_mgr.fused_cache_5000),
                "结果": "通过"
            }
            check_result["modules"]["缓存管理器"] = cache_status
            check_result["summary"]["total"] += 1
            check_result["summary"]["passed"] += 1
        except Exception as e:
            check_result["modules"]["缓存管理器"] = {
                "status": "检查失败",
                "error": str(e),
                "result": "失败"
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        # 6. 检查翻译服务
        try:
            translation_cache_file = os.path.join(self.config.TRANSLATION_CACHE_DIR, "translation_cache_eew.json")
            cache_file_exists = os.path.exists(translation_cache_file)
            cache_count = 0
            if cache_file_exists:
                try:
                    with open(translation_cache_file, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                        cache_count = len(cache_data) if isinstance(cache_data, dict) else 0
                except Exception:
                    pass

            translation_status = {
                "缓存文件存在": cache_file_exists,
                "缓存数量": cache_count,
                "result": "通过"
            }
            check_result["modules"]["翻译服务"] = translation_status
            check_result["summary"]["total"] += 1
            check_result["summary"]["passed"] += 1
        except Exception as e:
            check_result["modules"]["翻译服务"] = {
                "status": "检查失败",
                "error": str(e),
                "result": "失败"
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        # 7. 检查IP管理器
        try:
            ip_stats = self.client_ip_manager.get_connection_stats()
            ip_manager_status = {
                "总IP数": ip_stats.get('总IP数', 0),
                "活跃IP数": ip_stats.get('活跃IP数', 0),
                "总连接数": ip_stats.get('总连接数', 0),
                "黑名单数量": ip_stats.get('黑名单IP数', 0),
                "result": "通过"
            }
            check_result["modules"]["IP管理器"] = ip_manager_status
            check_result["summary"]["total"] += 1
            check_result["summary"]["passed"] += 1
        except Exception as e:
            check_result["modules"]["IP管理器"] = {
                "status": "检查失败",
                "error": str(e),
                "result": "失败"
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        return check_result

    def _format_check_result_text(self, check_result: Dict[str, Any]) -> str:
        """格式化检查结果为文本"""
        response = "=== 自动检查结果 ===\n\n"
        response += f"检查时间: {check_result.get('timestamp', 'N/A')}\n\n"

        summary = check_result.get('summary', {})
        response += f"检查摘要: 总计 {summary.get('total', 0)} 项, "
        response += f"通过 {summary.get('passed', 0)} 项, "
        response += f"警告 {summary.get('warnings', 0)} 项, "
        response += f"失败 {summary.get('failed', 0)} 项\n\n"

        modules = check_result.get('modules', {})
        for module_name, module_data in modules.items():
            response += f"【{module_name}】\n"
            if isinstance(module_data, dict):
                for item_name, item_data in module_data.items():
                    if isinstance(item_data, dict):
                        # 获取状态和结果
                        status = item_data.get('status', '')
                        result = item_data.get('result', 'N/A')
                        result_symbol = "✓" if result == "通过" else "⚠" if result == "警告" else "✗"
                        
                        # 如果有status字段，显示状态行
                        if status:
                            response += f"  {result_symbol} {item_name}: {status}\n"
                        else:
                            # 没有status字段，直接显示名称和结果
                            response += f"  {result_symbol} {item_name}\n"
                        
                        # 自动显示所有其他字段（排除status、result、error）
                        excluded_keys = {'status', 'result', 'error'}
                        for key, value in item_data.items():
                            if key in excluded_keys:
                                continue
                            
                            # 格式化显示值
                            if isinstance(value, bool):
                                display_value = '是' if value else '否'
                            elif isinstance(value, (int, float)):
                                display_value = value
                            else:
                                display_value = value
                            
                            response += f"    {key}: {display_value}\n"
                        
                        # 如果有错误，最后显示
                        if 'error' in item_data:
                            response += f"    错误: {item_data['error']}\n"
                    else:
                        # 对于非字典值，直接显示键值对
                        if isinstance(item_data, bool):
                            display_value = '是' if item_data else '否'
                        else:
                            display_value = item_data
                        response += f"  {item_name}: {display_value}\n"
            response += "\n"

        return response.rstrip()

    def start_management_server(self, port: Optional[int] = None):
        """启动融合管理 WebSocket（默认 2050，EEW+List 统一入口）"""
        from services.list.management_ws import FUSED_MGMT_PORT
        if port is None:
            port = int(os.environ.get("FUSED_MGMT_PORT", os.environ.get("EEW_MGMT_PORT", str(FUSED_MGMT_PORT))))
        mgmt_bind = os.environ.get("FUSED_MGMT_BIND", os.environ.get("EEW_MGMT_BIND", "127.0.0.1"))

        async def handle_management(websocket, path=None):
            client_addr = websocket.remote_address
            self.logger.debug(f"[管理端口 {port}] 客户端连接: {client_addr}")
            try:
                await websocket.send(json.dumps({
                    "type": "welcome",
                    "message": "融合管理端口已连接（EEW+List），JSON 可用 channel: eew|list|both",
                    "service": "fused_core",
                    "port": port,
                }, ensure_ascii=False))
                await self._send_available_commands(websocket, is_json=True)
                async for message in websocket:
                    is_json = True
                    text_command = ""
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        is_json = False
                        text_command = message.strip()
                    try:
                        if is_json and data.get("type") == "welcome":
                            await self._send_available_commands(websocket, is_json=True)
                            continue
                        if is_json and data.get("type") == "logout":
                            await websocket.send(json.dumps({
                                "type": "logout_result", "success": True,
                                "message": "会话已结束（可继续发送命令）",
                            }))
                            continue
                        if not is_json and text_command.strip() in ("退出", "LOGOUT", "logout", "exit"):
                            await websocket.send("会话已结束（可继续发送命令）")
                            continue
                        if is_json:
                            command = data.get("command", "")
                            params = data
                        else:
                            command, params = self._resolve_plain_management_command(text_command)
                        await self._execute_management_command(websocket, command, params, is_json)
                    except Exception as e:
                        if is_json:
                            await self._send_mgmt_json(websocket, {"type": "error", "message": str(e)})
                        else:
                            await websocket.send(f"ERROR:{str(e)}")
            except Exception as e:
                self.logger.debug(f"管理连接异常: {e}")
            finally:
                self.logger.debug(f"[管理端口] 客户端断开: {client_addr}")

        async def run():
            async with websockets.serve(handle_management, mgmt_bind, port, ping_interval=None):
                await asyncio.Future()

        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(run())

        threading.Thread(target=run_in_thread, daemon=True, name="Management-Server").start()
        self.logger.info(f"融合管理服务器启动: {mgmt_bind}:{port}（EEW+List，无密码）")
        if mgmt_bind == "0.0.0.0":
            self.logger.warning("警告: FUSED_MGMT_BIND=0.0.0.0，管理端口对全网开放且无密码保护")
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
        self.last_broadcast_time: Dict[int, float] = {5000: 0.0}
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

    def evict_source(self, source_key: str, port: int = 5000) -> None:
        """从融合推送列表移除指定源（保留磁盘缓存）。"""
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
        ports = (5000,)
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
        self.ws_server.start_server(5000)

        # 启动管理服务器
        connection_logger.debug("启动管理服务器...")
        self.ws_server.start_management_server(2050)


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
        print(f"  - 端口 5000: 地震预警 WebSocket 推送")
        print(f"  - 端口 2050: 融合管理 WebSocket（EEW+List，channel 区分）")
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

# ============================================================================
# 入口函数
# ============================================================================

if __name__ == '__main__':
    service = EEWService()
    service.run()
