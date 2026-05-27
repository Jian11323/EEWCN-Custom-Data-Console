"""Windows：子进程 Job 对象，父进程退出时自动结束整棵进程树。"""

from __future__ import annotations

import ctypes
import os
from ctypes import wintypes
from typing import Optional

JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
JobObjectExtendedLimitInformation = 9
CREATE_BREAKAWAY_FROM_JOB = 0x01000000  # 勿用于控制台子进程，否则退出时无法随 Job 一并结束


class _IO_COUNTERS(ctypes.Structure):
    _fields_ = [("data", ctypes.c_uint64 * 6)]


class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
        ("IoInfo", _IO_COUNTERS),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


def win_popen_extra_flags() -> int:
    """Popen creationflags：无控制台窗口（子进程挂到应用级 Job，勿 BREAKAWAY）。"""
    if os.name != "nt":
        return 0
    return getattr(os, "CREATE_NO_WINDOW", 0)


def create_kill_on_close_job() -> Optional[int]:
    """创建 Job；关闭最后一个句柄时终止其内所有进程。返回 job handle 或 None。"""
    if os.name != "nt":
        return None
    kernel32 = ctypes.windll.kernel32
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        return None
    info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    ok = kernel32.SetInformationJobObject(
        job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if not ok:
        kernel32.CloseHandle(job)
        return None
    return job


def assign_process_to_job(job_handle: int, process_handle: int) -> bool:
    if not job_handle or not process_handle:
        return False
    return bool(ctypes.windll.kernel32.AssignProcessToJobObject(job_handle, process_handle))


def close_job(job_handle: Optional[int]) -> None:
    if job_handle and os.name == "nt":
        ctypes.windll.kernel32.CloseHandle(job_handle)
