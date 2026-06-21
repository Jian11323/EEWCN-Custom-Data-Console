from __future__ import annotations

import logging
import logging.handlers
import os
from datetime import datetime, timedelta

from services.fused.eew.config import Config

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

