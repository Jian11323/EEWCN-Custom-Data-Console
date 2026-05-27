"""退出时清理可能残留的子进程（fused_core Python）。"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Set

_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _run(cmd: list, timeout: int = 8) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        capture_output=True,
        timeout=timeout,
        creationflags=_CREATE_NO_WINDOW,
    )


def decode_subprocess_output(data: bytes | None) -> str:
    from services.common.logging_setup import decode_subprocess_bytes
    return decode_subprocess_bytes(data)


def _decode_output(data: bytes | None) -> str:
    return decode_subprocess_output(data)


def kill_pids(pids: Iterable[int], *, force: bool = True) -> None:
    seen: Set[int] = set()
    for pid in pids:
        if not pid:
            continue
        pid = int(pid)
        if pid in seen:
            continue
        seen.add(pid)
        try:
            if os.name == "nt":
                args = ["taskkill", "/PID", str(pid)]
                if force:
                    args[1:1] = ["/F", "/T"]
                _run(args, timeout=6)
            else:
                import signal

                os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
        except Exception:
            pass


def find_pids_by_command_fragment(fragment: str) -> List[int]:
    if not fragment:
        return []
    pids: List[int] = []
    if os.name != "nt":
        try:
            out = _run(["pgrep", "-f", fragment], timeout=5)
            for line in _decode_output(out.stdout).splitlines():
                line = line.strip()
                if line.isdigit():
                    pids.append(int(line))
        except Exception:
            pass
        return pids

    needle = fragment.replace("'", "''")
    ps = (
        "Get-CimInstance Win32_Process | "
        f"Where-Object {{ $_.CommandLine -like '*{needle}*' }} | "
        "Select-Object -ExpandProperty ProcessId"
    )
    try:
        out = _run(["powershell", "-NoProfile", "-Command", ps], timeout=12)
        for line in _decode_output(out.stdout).splitlines():
            line = line.strip()
            if line.isdigit():
                pids.append(int(line))
    except Exception:
        pass
    return pids


def find_frozen_fused_child_pids() -> List[int]:
    """onefile 子进程：主 exe 带 --run-fused-core。"""
    return find_pids_by_command_fragment("--run-fused-core")


def find_fused_core_pids() -> List[int]:
    pids = find_pids_by_command_fragment("services.fused.main")
    pids.extend(find_pids_by_command_fragment("services\\fused\\main.py"))
    pids.extend(find_frozen_fused_child_pids())
    seen: Set[int] = set()
    out: List[int] = []
    for pid in pids:
        if pid and pid not in seen:
            seen.add(int(pid))
            out.append(int(pid))
    return out


_cleanup_already_done = False


def mark_cleanup_done() -> None:
    global _cleanup_already_done
    _cleanup_already_done = True


def cleanup_service_orphans(
    app_root: Path,
    *,
    local_ws_port: int = 0,
    extra_pids: Optional[Iterable[int]] = None,
    passes: int = 2,
    fast: bool = False,
) -> None:
    """清理融合核心可能残留的进程。"""
    del local_ws_port  # 保留参数以兼容旧调用
    for attempt in range(max(1, passes)):
        targets: List[int] = []
        if extra_pids:
            targets.extend(int(p) for p in extra_pids if p)

        if not fast:
            targets.extend(find_fused_core_pids())
        else:
            targets.extend(find_frozen_fused_child_pids())

        kill_pids(targets)
        if attempt + 1 < passes:
            time.sleep(0.35 if not fast else 0.1)


def cleanup_on_exit(app_root: Optional[Path] = None) -> None:
    """进程退出前最后一轮清理（atexit / 控制台关闭）。"""
    if _cleanup_already_done:
        return
    try:
        from console.process_supervisor import shutdown_all_children
        shutdown_all_children()
    except Exception:
        pass
    root = app_root or Path(__file__).resolve().parent.parent
    cleanup_service_orphans(root, passes=1, fast=True)
