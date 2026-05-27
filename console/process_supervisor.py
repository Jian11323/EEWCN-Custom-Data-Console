"""应用级子进程监管：单 Job 托管全部服务子进程，退出时一并终止。"""

from __future__ import annotations

import os
from typing import Optional

from console.process_job import (
    assign_process_to_job,
    close_job,
    create_kill_on_close_job,
)

_app_job: Optional[int] = None


def ensure_app_child_job() -> Optional[int]:
    """创建/返回控制台全局 Job（KILL_ON_JOB_CLOSE）。"""
    global _app_job
    if os.name != "nt":
        return None
    if _app_job is None:
        _app_job = create_kill_on_close_job()
    return _app_job


def assign_child_to_app_job(process_handle: int) -> bool:
    if not process_handle:
        return False
    job = ensure_app_child_job()
    if not job:
        return False
    return assign_process_to_job(job, int(process_handle))


def shutdown_all_children() -> None:
    """关闭 Job 句柄，终止其内所有子进程（含单文件 --run-fused-core）。"""
    global _app_job
    if _app_job:
        close_job(_app_job)
        _app_job = None


def is_app_job_active() -> bool:
    return _app_job is not None
