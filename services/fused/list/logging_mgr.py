from __future__ import annotations

import logging
import os
from datetime import datetime

from services.common.logging_setup import Utf8StdoutHandler, ensure_stdio_utf8
from services.fused.list.config import Config
from services.fused.list.state import logger


class LogManager:
    """日志管理器"""

    @staticmethod
    def setup_logging():
        """配置日志记录器"""
        ensure_stdio_utf8()
        os.makedirs(Config.LOG_DIR, exist_ok=True)

        console = Utf8StdoutHandler()
        console.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"
            )
        )
        logging.basicConfig(
            level=logging.INFO,
            handlers=[
                console,
                logging.FileHandler(
                    os.path.join(
                        Config.LOG_DIR,
                        f'earthquake_api_{datetime.now().strftime("%Y-%m-%d")}.log',
                    ),
                    encoding="utf-8",
                ),
            ],
            force=True,
        )
        return logger
