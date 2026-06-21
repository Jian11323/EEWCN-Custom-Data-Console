#!/usr/bin/env python3
"""
EEW + List 融合服务（单进程）
- 唯一 Fan Studio /all 连接
- 内部采集 自定义源/BMKG/GeoNet/Early-est
- EEW: WebSocket（127.0.0.1，端口可配置）
- List: HTTP（127.0.0.1，端口可配置）
- 管理: 控制台 stdin IPC（无 TCP 管理端口）
"""

from __future__ import annotations

import sys
import threading
import time
import signal
import atexit
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from services.common.logging_setup import ensure_stdio_utf8

ensure_stdio_utf8()


def run() -> None:
    from services.common.logging_setup import ensure_stdio_utf8

    ensure_stdio_utf8()
    from services.common.single_instance import acquire_instance_lock, release_instance_lock
    if not acquire_instance_lock("fused_core", 18050):
        print("错误: 融合核心已在运行（单实例锁 127.0.0.1:18050），请勿重复启动。")
        sys.exit(1)

    from services.fused.config import apply_fused_environment
    from services.common.logging_setup import setup_root_logging, setup_module_logger
    from services.common.paths import ensure_dirs, get_app_root, get_log_dir
    from services.common.source_switches import load_from_env_or_settings
    from services.internal import start_internal_fetchers

    load_from_env_or_settings()
    from services.fused.integration import wire_services
    from services.fused.eew.engine import EEWService
    import services.fused.list.engine as list_engine
    from services.fused.list.engine import MainHandler

    apply_fused_environment()
    ensure_dirs()
    setup_root_logging()
    log = setup_module_logger("fused", "fused_core.log", subdir="", console=False)

    log.info("融合服务启动，应用根目录: %s", get_app_root())
    log.info("日志目录: %s", get_log_dir())

    start_internal_fetchers()

    log.info("初始化 EEW...")
    eew = EEWService()

    log.info("初始化 List...")
    MainHandler.initialize()

    wire_services(eew, list_engine)

    log.info("启动 EEW 后台线程...")
    eew_thread = threading.Thread(target=eew.run, name="EEW-Core", daemon=True)
    eew_thread.start()

    time.sleep(2)

    log.info("启动 List 工作线程与 HTTP...")
    MainHandler.start_threads()
    MainHandler.start_servers()

    from services.common.ports import LOCAL_BIND, get_eew_port, get_list_port

    eew_port = get_eew_port()
    list_port = get_list_port()

    print("=" * 60)
    print("融合核心服务 fused_core")
    print(f"  EEW  WS: {LOCAL_BIND}:{eew_port}")
    print(f"  List HTTP: {LOCAL_BIND}:{list_port}")
    print("  管理: 控制台 IPC（stdin）")
    print("  Fan Studio: 单条共享 /all 连接")
    print("  内部源: 自定义/BMKG/GeoNet/Early-est")
    print("  日志目录:", get_log_dir())
    print("=" * 60)

    _shutdown_called = False

    def _shutdown() -> None:
        nonlocal _shutdown_called
        if _shutdown_called:
            return
        _shutdown_called = True
        print("\n正在关闭...")
        from services.common.fanstudio import get_fanstudio_connection
        from services.internal import stop_internal_fetchers
        try:
            stop_internal_fetchers()
        except Exception:
            pass
        try:
            get_fanstudio_connection().stop()
        except Exception:
            pass
        try:
            release_instance_lock()
        except Exception:
            pass

    def _signal_handler(signum, frame):
        _shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)
    atexit.register(_shutdown)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    run()
