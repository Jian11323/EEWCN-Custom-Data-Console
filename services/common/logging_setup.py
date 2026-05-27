"""统一日志：默认写入软件根目录 logs/。"""

from __future__ import annotations

import io
import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional

from services.common.paths import ensure_dirs, get_log_dir

_stdio_utf8_done = False


class Utf8StdoutHandler(logging.Handler):
    """向 stdout 二进制层写入 UTF-8，避免 Windows 管道下 TextIOWrapper 用 GBK 编码。"""

    terminator = "\n"

    def __init__(self, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        ensure_stdio_utf8()
        self._buffer = getattr(sys.stdout, "buffer", None)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record) + self.terminator
            if self._buffer is not None:
                self._buffer.write(msg.encode("utf-8", errors="replace"))
                self._buffer.flush()
            else:
                sys.stdout.write(msg)
                sys.stdout.flush()
        except Exception:
            self.handleError(record)


def ensure_stdio_utf8() -> None:
    """将 stdout/stderr 设为 UTF-8，避免 Windows 控制台 PIPE 解码乱码。"""
    global _stdio_utf8_done
    if _stdio_utf8_done:
        return
    _stdio_utf8_done = True
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if stream is None or getattr(stream, "closed", False):
            continue
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
                continue
        except (AttributeError, OSError, ValueError, TypeError):
            pass
        try:
            buffer = getattr(stream, "buffer", None)
            if buffer is not None:
                setattr(
                    sys,
                    name,
                    io.TextIOWrapper(
                        buffer,
                        encoding="utf-8",
                        errors="replace",
                        line_buffering=getattr(stream, "line_buffering", True),
                    ),
                )
        except Exception:
            pass


def decode_subprocess_bytes(data: bytes | None) -> str:
    """解码子进程 stdout 行。

    融合子进程由控制台以 PYTHONUTF8=1 启动，且日志经 Utf8StdoutHandler 直写 UTF-8 字节。
    在 Windows 上 UTF-8 中文行往往也能被 GBK 无错「解码」成另一串汉字；若再按 CJK 数量
    在 UTF-8/GBK 间择优，会把正确 UTF-8 误显示为乱码（如 日志目录 → 鏃ュ織鐩綍）。
    因此：能按 UTF-8 严格解码则一律用 UTF-8；仅当 UTF-8 失败时再尝试 GBK/MBCS。
    """
    if not data:
        return ""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        if os.name == "nt":
            for enc in ("gbk", "mbcs"):
                try:
                    return data.decode(enc)
                except (UnicodeDecodeError, LookupError):
                    continue
        return data.decode("utf-8", errors="replace")


def setup_module_logger(
    name: str,
    filename: str,
    *,
    level: int = logging.INFO,
    console: bool = True,
    subdir: str = "",
) -> logging.Logger:
    """创建带按日滚动的文件 handler 的 logger。"""
    ensure_stdio_utf8()
    ensure_dirs()
    log_root = get_log_dir()
    if subdir:
        log_root = log_root / subdir
        log_root.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_path = log_root / filename
    fh = logging.handlers.TimedRotatingFileHandler(
        str(file_path),
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8",
    )
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if console:
        ch = Utf8StdoutHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        # 已挂控制台 handler 时不再向 root 传播，避免与 setup_root_logging 重复打印
        logger.propagate = False
    else:
        logger.propagate = True

    return logger


def setup_root_logging(level: int = logging.INFO) -> None:
    ensure_stdio_utf8()
    ensure_dirs()
    crash_log = get_log_dir() / "crash.log"
    console = Utf8StdoutHandler()
    console.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.basicConfig(
        level=level,
        handlers=[
            console,
            logging.FileHandler(str(crash_log), encoding="utf-8"),
        ],
    )
