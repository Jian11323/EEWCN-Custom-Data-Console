#!/usr/bin/env python3
"""自定义数据源控制台入口（GUI 与融合服务共用）"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# 打包为单 exe 时，子进程用同一可执行文件 + 此参数启动融合核心
FUSED_CORE_ARG = "--run-fused-core"


def _run_fused_core() -> int:
    from services.fused.main import run
    run()
    return 0


def _run_gui() -> int:
    from console.ui.main_window import run_app
    from console.process_cleanup import cleanup_on_exit
    from services.common.paths import get_app_root, get_writable_root
    import atexit

    def _cleanup() -> None:
        try:
            from console.process_supervisor import shutdown_all_children
            shutdown_all_children()
        except Exception:
            pass
        root = get_writable_root() if getattr(sys, "frozen", False) else get_app_root()
        cleanup_on_exit(root)

    atexit.register(_cleanup)
    return run_app()


if __name__ == "__main__":
    from services.common.logging_setup import ensure_stdio_utf8

    ensure_stdio_utf8()
    if FUSED_CORE_ARG in sys.argv:
        sys.exit(_run_fused_core())
    sys.exit(_run_gui())
