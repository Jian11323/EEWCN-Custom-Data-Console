"""子进程生命周期管理"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from PyQt5.QtCore import QThread, pyqtSignal


class ServiceProcess(QThread):
    started_sig = pyqtSignal(str)
    stopped_sig = pyqtSignal(str, int)
    output = pyqtSignal(str, str)
    status_update = pyqtSignal(str, dict)

    def __init__(
        self,
        service_key: str,
        script_path: Path,
        cwd: Path,
        env_overrides: Optional[dict] = None,
    ):
        super().__init__()
        self._key = service_key
        self._script_path = Path(script_path)
        self._cwd = Path(cwd)
        try:
            from services.common.paths import get_app_root
            if os.environ.get("FUSED_MODE"):
                self._cwd = get_app_root()
        except ImportError:
            pass
        self._env = os.environ.copy()
        self._env.setdefault("PYTHONUTF8", "1")
        self._env.setdefault("PYTHONIOENCODING", "utf-8")
        if env_overrides:
            self._env.update(env_overrides)
        self._process: Optional[subprocess.Popen] = None
        self._stop_event = threading.Event()
        self._abort = threading.Event()
        self._start_time: Optional[datetime] = None
        self._pid: Optional[int] = None

    @property
    def pid(self) -> Optional[int]:
        if self._process is not None and self._process.poll() is None:
            return self._process.pid
        return self._pid

    @property
    def start_time(self) -> Optional[datetime]:
        return self._start_time

    @property
    def uptime(self) -> str:
        if not self._start_time:
            return "未启动"
        delta = datetime.now() - self._start_time
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _kill_process(self, proc: subprocess.Popen, timeout: int = 8) -> None:
        pid = proc.pid
        try:
            if proc.stdin:
                try:
                    proc.stdin.close()
                except Exception:
                    pass
            if os.name == "nt" and pid:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=timeout,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
            else:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass

    def run(self):
        exit_code = 0
        try:
            if self._abort.is_set():
                return

            self._stop_event.clear()
            self._start_time = datetime.now()
            from console.process_cleanup import decode_subprocess_output
            from console.services_registry import resolve_service_launch_cmd
            cmd = resolve_service_launch_cmd(self._key)
            popen_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            try:
                from console.process_job import win_popen_extra_flags
                popen_flags = win_popen_extra_flags()
            except Exception:
                pass
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                cwd=str(self._cwd),
                env=self._env,
                bufsize=1,
                creationflags=popen_flags,
            )
            self._pid = self._process.pid
            if os.name == "nt" and self._process.poll() is None:
                try:
                    from console.process_supervisor import assign_child_to_app_job
                    assign_child_to_app_job(int(self._process._handle))
                except Exception:
                    pass

            if self._abort.is_set():
                self._kill_process(self._process)
                exit_code = 0
                return

            self.started_sig.emit(self._key)
            self.status_update.emit(
                self._key,
                {"pid": self._pid, "uptime": self.uptime, "running": True},
            )

            for raw in iter(self._process.stdout.readline, b""):
                if not raw or self._stop_event.is_set() or self._abort.is_set():
                    break
                line = decode_subprocess_output(raw).rstrip("\n\r")
                if line:
                    self.output.emit(self._key, line)

            if self._process.poll() is None:
                self._kill_process(self._process)
            self._process.wait()
            exit_code = self._process.returncode or 0
        except Exception as e:
            self.output.emit(self._key, f"[ERROR] 启动失败: {e}")
            exit_code = -1
        finally:
            self._pid = None
            self._start_time = None
            proc = self._process
            self._process = None
            if proc is not None and proc.poll() is None:
                self._kill_process(proc)
            self.stopped_sig.emit(self._key, exit_code)

    def dispose(self, wait_ms: int = 1500) -> None:
        """主线程回收日志 QThread（退出/停止后调用）。"""
        self._abort.set()
        self._stop_event.set()
        if not self.isRunning():
            return
        self.requestInterruption()
        if not self.wait(min(wait_ms, 2000)):
            self.terminate()
            self.wait(500)

    def stop(self, timeout: int = 8):
        self._abort.set()
        self._stop_event.set()
        proc = self._process
        if proc is None:
            return
        threading.Thread(
            target=self._kill_process,
            args=(proc, timeout),
            daemon=True,
            name=f"Kill-{self._key}",
        ).start()

    def is_running(self) -> bool:
        if self._process is not None and self._process.poll() is None:
            return True
        return self.isRunning()

    def wait_stopped(self, timeout_ms: int = 8000) -> bool:
        """等待子进程退出（仅轮询，勿跨线程调用 QThread.wait）。"""
        import time

        deadline = time.monotonic() + timeout_ms / 1000.0
        while time.monotonic() < deadline:
            proc = self._process
            if proc is not None and proc.poll() is not None:
                return True
            if proc is None and not self.isRunning():
                return True
            time.sleep(0.05)
        proc = self._process
        return proc is None or proc.poll() is not None


class StopServicesWorker(QThread):
    """后台停止服务并清理残留，避免阻塞 UI。"""

    finished_ok = pyqtSignal()

    def __init__(
        self,
        items: list,
        *,
        app_root: Path,
        ws_port: int = 1151,
        cleanup: bool = True,
        cleanup_passes: int = 2,
        wait_timeout_ms: int = 12000,
        fast_cleanup: bool = False,
    ):
        super().__init__()
        self._items = items
        self._app_root = app_root
        self._ws_port = ws_port
        self._cleanup = cleanup
        self._cleanup_passes = max(1, cleanup_passes)
        self._wait_timeout_ms = wait_timeout_ms
        self._fast_cleanup = fast_cleanup

    def run(self):
        from console.process_cleanup import cleanup_service_orphans, decode_subprocess_output, mark_cleanup_done
        from console.process_supervisor import shutdown_all_children

        tracked: list[int] = []
        for _key, proc in self._items:
            if proc and proc.pid:
                tracked.append(proc.pid)
            if proc:
                proc.stop()
                proc.wait_stopped(self._wait_timeout_ms)
        if self._cleanup:
            cleanup_service_orphans(
                self._app_root,
                local_ws_port=self._ws_port,
                extra_pids=tracked,
                passes=self._cleanup_passes,
                fast=self._fast_cleanup,
            )
            mark_cleanup_done()
        shutdown_all_children()
        self.finished_ok.emit()
