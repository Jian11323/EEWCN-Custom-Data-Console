"""主窗口"""

from __future__ import annotations

import sys
import traceback
from typing import Dict, List, Optional, Tuple

from PyQt5.QtCore import QThread, QTimer, Qt
from PyQt5.QtGui import QColor, QIcon, QPalette
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QMessageBox, QSplitter,
    QListWidget, QListWidgetItem, QStackedWidget, QFrame,
)

from console.config import ConfigStore
from console.management_client import ManagementHub
from console.process_cleanup import cleanup_service_orphans
from console.process_manager import ServiceProcess, StopServicesWorker
from console.process_supervisor import ensure_app_child_job, shutdown_all_children
from console.services_registry import (
    SERVICES,
    SERVICE_START_ORDER,
    build_env,
    PROJECT_ROOT,
    resolve_service_launch_path,
)
from console.ui.combined_log import CombinedLogPanel
from console.ui.control_tab import ControlTab
from console.ui.service_panel import ServicePanel
from console.ui.status_tab import StatusTab
from console.ui.styles import LIGHT_THEME, global_btn_style
from console.ui.upstream_tab import UpstreamTab
from console.ui.sources_tab import SourcesTab
from console.ui.about_tab import AboutTab
from console.ui.custom_source_tab import CustomSourceTab

TAB_ITEMS: List[Tuple[str, str]] = [
    ("服务管理", "service"),
    ("上游切换", "upstream"),
    ("控制命令", "control"),
    ("数据开关", "sources"),
    ("自定义数据源", "custom"),
    ("采集状态", "status"),
    ("关于软件", "about"),
]


def write_startup_log(exc: BaseException) -> None:
    log_path = PROJECT_ROOT / "startup.log"
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(traceback.format_exc())
    except Exception:
        pass


def _section_label(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setStyleSheet("color: #57606a; font-size: 12px; font-weight: bold; padding: 4px 2px;")
    return lbl


def _make_page(control: QWidget, log: CombinedLogPanel) -> QWidget:
    page = QWidget()
    layout = QHBoxLayout(page)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(0)
    control.setMinimumWidth(440)
    log.setMinimumWidth(260)
    log.setMaximumWidth(520)
    splitter = QSplitter(Qt.Horizontal)
    splitter.setChildrenCollapsible(False)
    splitter.setHandleWidth(5)
    splitter.addWidget(control)
    splitter.addWidget(log)
    splitter.setStretchFactor(0, 3)
    splitter.setStretchFactor(1, 2)
    splitter.setSizes([580, 360])
    layout.addWidget(splitter)
    return page


def _apply_window_icon(target) -> None:
    try:
        from services.common.paths import get_logo_path
        logo = get_logo_path()
        if not logo:
            return
        icon = QIcon(str(logo))
        if icon.isNull():
            return
        if hasattr(target, "setWindowIcon"):
            target.setWindowIcon(icon)
    except Exception:
        pass


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("自定义数据源控制台")
        _apply_window_icon(self)
        self.resize(1280, 860)
        self.setMinimumSize(1020, 680)

        self._store = ConfigStore.instance()
        s = self._store.settings
        self._mgmt = ManagementHub(s.mgmt_host, s.mgmt_port)
        self._processes: Dict[str, Optional[ServiceProcess]] = {k: None for k in SERVICES}
        self._panels: Dict[str, ServicePanel] = {}
        self._logs: Dict[str, CombinedLogPanel] = {}
        self._start_all_busy = False
        self._stop_busy = False
        self._stop_worker: Optional[StopServicesWorker] = None
        self._closing = False
        self._shutdown_done = False

        ensure_app_child_job()
        self._setup_ui()
        self.setStyleSheet(LIGHT_THEME)

        app = QApplication.instance()
        if app is not None:
            app.aboutToQuit.connect(self._on_about_to_quit)

        self._uptime_timer = QTimer(self)
        self._uptime_timer.timeout.connect(self._refresh_uptimes)
        self._uptime_timer.start(1000)

        if s.auto_start_on_launch:
            QTimer.singleShot(800, self._start_all_ordered)

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        # --- 左侧导航 ---
        sidebar = QWidget()
        sidebar.setFixedWidth(200)
        side_layout = QVBoxLayout(sidebar)
        side_layout.setContentsMargins(0, 0, 0, 0)
        side_layout.setSpacing(6)

        title = QLabel("自定义数据源控制台")
        title.setWordWrap(True)
        title.setStyleSheet("font-size: 14px; font-weight: bold; padding-bottom: 4px;")
        side_layout.addWidget(title)

        self._lbl_total = QLabel("就绪")
        self._lbl_total.setStyleSheet("color: #57606a; font-size: 12px;")
        side_layout.addWidget(self._lbl_total)

        sep0 = QFrame()
        sep0.setFrameShape(QFrame.HLine)
        side_layout.addWidget(sep0)

        side_layout.addWidget(_section_label("服务"))
        self._service_nav = QListWidget()
        self._service_nav.setMaximumHeight(72)
        for key, info in SERVICES.items():
            self._service_nav.addItem(QListWidgetItem(info["name"]))
        side_layout.addWidget(self._service_nav)

        sep1 = QFrame()
        sep1.setFrameShape(QFrame.HLine)
        side_layout.addWidget(sep1)

        side_layout.addWidget(_section_label("功能"))
        self._tab_nav = QListWidget()
        for label, _ in TAB_ITEMS:
            self._tab_nav.addItem(QListWidgetItem(label))
        side_layout.addWidget(self._tab_nav, stretch=1)

        btn_start = QPushButton("全部启动")
        btn_start.setStyleSheet(global_btn_style("#238636", "#2ea043"))
        btn_start.clicked.connect(self._start_all_ordered)
        btn_stop = QPushButton("全部停止")
        btn_stop.setStyleSheet(global_btn_style("#da3633", "#f85149"))
        btn_stop.clicked.connect(self._stop_all)
        btn_exit = QPushButton("全部退出")
        btn_exit.setStyleSheet(global_btn_style("#57606a", "#6e7781"))
        btn_exit.clicked.connect(self._exit_app)
        self._btn_start_all = btn_start
        self._btn_stop_all = btn_stop
        side_layout.addWidget(btn_start)
        side_layout.addWidget(btn_stop)
        side_layout.addWidget(btn_exit)

        root.addWidget(sidebar)

        # --- 右侧内容区 ---
        self._content_stack = QStackedWidget()

        service_log = CombinedLogPanel()
        self._logs["service"] = service_log
        service_stack = QStackedWidget()
        for key, info in SERVICES.items():
            saved = self._store.get_service_config(key)
            panel = ServicePanel(key, info, saved)
            panel.start_requested.connect(self._start_service)
            panel.restart_requested.connect(
                lambda k, c: self._start_service(k, c, force_restart=True)
            )
            panel.stop_requested.connect(self._stop_service)
            self._panels[key] = panel
            service_stack.addWidget(panel)
        self._service_stack = service_stack
        self._content_stack.addWidget(_make_page(service_stack, service_log))

        upstream_log = CombinedLogPanel()
        self._logs["upstream"] = upstream_log
        self._content_stack.addWidget(
            _make_page(UpstreamTab(self._mgmt, upstream_log), upstream_log)
        )

        control_log = CombinedLogPanel()
        self._logs["control"] = control_log
        self._content_stack.addWidget(
            _make_page(ControlTab(self._mgmt, control_log), control_log)
        )

        sources_log = CombinedLogPanel()
        self._logs["sources"] = sources_log
        self._content_stack.addWidget(
            _make_page(SourcesTab(self._mgmt), sources_log)
        )

        self._custom_tab = CustomSourceTab(self._mgmt)
        self._content_stack.addWidget(self._custom_tab)

        status_log = CombinedLogPanel()
        self._logs["status"] = status_log
        self._status_tab = StatusTab(lambda: self._processes, status_log)
        self._content_stack.addWidget(
            _make_page(self._status_tab, status_log)
        )

        self._content_stack.addWidget(AboutTab())

        root.addWidget(self._content_stack, stretch=1)

        self._service_nav.currentRowChanged.connect(self._on_service_changed)
        self._tab_nav.currentRowChanged.connect(self._on_tab_changed)
        self._service_nav.setCurrentRow(0)
        self._tab_nav.setCurrentRow(0)
        self._on_tab_changed(0)

    def _on_service_changed(self, row: int):
        if row >= 0:
            self._service_stack.setCurrentIndex(row)

    def _on_tab_changed(self, row: int):
        if row >= 0:
            self._content_stack.setCurrentIndex(row)
        on_service = row == 0
        self._service_nav.setEnabled(on_service)
        self._status_tab.set_polling_enabled(row == 5)

    def _service_is_alive(self, service_key: str) -> bool:
        proc = self._processes.get(service_key)
        return bool(proc and proc.is_running())

    def _begin_stop_ui(self, keys: List[str]) -> None:
        self._stop_busy = True
        for key in keys:
            if key in self._panels:
                self._panels[key].set_stopping(True)
        self._btn_start_all.setEnabled(False)
        self._btn_stop_all.setEnabled(False)
        self._btn_stop_all.setText("停止中...")

    def _end_stop_ui(self) -> None:
        self._stop_busy = False
        for key in SERVICE_START_ORDER:
            if key in self._panels:
                self._panels[key].set_stopping(False)
        self._btn_start_all.setEnabled(True)
        self._btn_stop_all.setEnabled(True)
        self._btn_stop_all.setText("全部停止")

    def _run_stop_worker(
        self,
        items: List[Tuple[str, Optional[ServiceProcess]]],
        *,
        log_msg: str = "",
        on_done=None,
        cleanup_passes: int = 2,
        wait_timeout_ms: int = 12000,
        fast_cleanup: bool = False,
    ) -> bool:
        active = [(k, p) for k, p in items if p and (p.is_running() or p.isRunning())]
        if not active:
            if on_done:
                on_done()
            return False
        if self._stop_worker and self._stop_worker.isRunning():
            return False

        keys = [k for k, _ in active]
        self._begin_stop_ui(keys)
        worker = StopServicesWorker(
            active,
            app_root=PROJECT_ROOT,
            cleanup_passes=cleanup_passes,
            wait_timeout_ms=wait_timeout_ms,
            fast_cleanup=fast_cleanup,
        )
        self._stop_worker = worker

        def _finished():
            self._stop_worker = None
            for key in keys:
                proc = self._processes.get(key)
                if proc:
                    proc.dispose()
                self._processes[key] = None
                if key in self._panels:
                    self._panels[key].set_running(False)
            if self._closing:
                self._stop_busy = False
            else:
                self._end_stop_ui()
            self._update_total()
            if log_msg:
                self._logs["service"].append("SYSTEM", log_msg)
            if on_done:
                QTimer.singleShot(0, on_done)

        worker.finished_ok.connect(_finished)
        worker.start()
        return True

    def _start_service(self, service_key: str, config: dict, *, force_restart: bool = False):
        if self._closing or self._stop_busy:
            return
        info = SERVICES[service_key]
        script = resolve_service_launch_path(service_key)
        if not getattr(sys, "frozen", False) and not script.exists():
            QMessageBox.critical(self, "错误", f"服务入口不存在:\n{script}")
            return

        if self._service_is_alive(service_key) and not force_restart:
            self._logs["service"].append(
                SERVICES[service_key]["name"],
                "已在运行，跳过重复启动（需重启请点「重启」或先停止）",
            )
            return

        self._store.set_service_config(service_key, config)

        proc_old = self._processes.get(service_key)
        if proc_old and (proc_old.is_running() or proc_old.isRunning()):
            def _after_stop():
                QTimer.singleShot(500, lambda: self._launch_service(service_key, config))

            started = self._run_stop_worker(
                [(service_key, proc_old)],
                on_done=_after_stop,
            )
            if started:
                return
            self._logs["service"].append(
                SERVICES[service_key]["name"],
                "警告: 上一进程未能停止，仍尝试启动（若端口占用请先全部停止）",
            )

        self._launch_service(service_key, config)

    def _launch_service(self, service_key: str, config: dict) -> None:
        if self._closing:
            return
        info = SERVICES[service_key]
        script = resolve_service_launch_path(service_key)
        env = build_env(service_key, config)
        proc = ServiceProcess(service_key, script, info["cwd"], env)
        proc.started_sig.connect(self._on_started)
        proc.stopped_sig.connect(self._on_stopped)
        proc.output.connect(self._on_output)
        self._processes[service_key] = proc
        proc.start()
        self._panels[service_key].set_running(True, uptime="启动中...")

    def _stop_service(self, service_key: str):
        if self._stop_busy:
            return
        proc = self._processes.get(service_key)
        if not proc:
            return
        self._run_stop_worker(
            [(service_key, proc)],
            log_msg=f"{SERVICES[service_key]['name']} 已停止并清理残留进程",
        )

    def _cleanup_orphans(self, extra_pids: Optional[List[int]] = None) -> None:
        cleanup_service_orphans(
            PROJECT_ROOT,
            extra_pids=extra_pids or [],
            passes=2,
        )

    def _start_all_ordered(self):
        if self._closing or self._stop_busy:
            return
        if self._start_all_busy:
            self._logs["service"].append("SYSTEM", "正在启动中，请勿重复点击")
            return
        if self._service_is_alive("fused_core"):
            self._logs["service"].append("SYSTEM", "融合核心已在运行，无需重复启动")
            return
        self._start_all_busy = True
        self._logs["service"].append("SYSTEM", "启动全部服务")
        try:
            for key in SERVICE_START_ORDER:
                panel = self._panels[key]
                self._start_service(key, panel.get_config())
        finally:
            QTimer.singleShot(2000, self._clear_start_all_busy)

    def _clear_start_all_busy(self):
        self._start_all_busy = False

    def _stop_all(
        self,
        *,
        on_done=None,
        cleanup_passes: int = 2,
        wait_timeout_ms: int = 12000,
        fast_cleanup: bool = False,
    ):
        if self._stop_busy:
            return
        items = [(key, self._processes.get(key)) for key in reversed(SERVICE_START_ORDER)]
        started = self._run_stop_worker(
            items,
            log_msg="已停止全部服务并清理残留进程",
            on_done=on_done,
            cleanup_passes=cleanup_passes,
            wait_timeout_ms=wait_timeout_ms,
            fast_cleanup=fast_cleanup,
        )
        if not started and on_done:
            QTimer.singleShot(0, on_done)

    def _shutdown_services(self, on_done=None):
        if self._shutdown_done:
            if on_done:
                QTimer.singleShot(0, on_done)
            return
        self._shutdown_done = True
        self._closing = True
        self._start_all_busy = False
        self._status_tab.set_polling_enabled(False)
        self._uptime_timer.stop()
        self._stop_all(
            on_done=on_done,
            cleanup_passes=1,
            wait_timeout_ms=5000,
            fast_cleanup=True,
        )

    def _teardown_runtime_threads(self) -> None:
        """回收控制台内 QThread / 管理 WS 工作线程。"""
        self._status_tab.cancel_background_work()
        self._uptime_timer.stop()
        self._mgmt.stop_all_workers()
        if self._stop_worker and self._stop_worker.isRunning():
            if not self._stop_worker.wait(2500):
                self._stop_worker.terminate()
                self._stop_worker.wait(500)
            self._stop_worker = None
        for key, proc in list(self._processes.items()):
            if proc:
                proc.dispose()
            self._processes[key] = None

    def _request_exit(self, event=None) -> None:
        """异步退出：不阻塞 UI 事件循环。"""
        self._lbl_total.setText("正在退出...")
        self._exit_pulse = QTimer(self)
        self._exit_pulse.timeout.connect(lambda: QApplication.processEvents())
        self._exit_pulse.start(80)

        def _finish():
            if hasattr(self, "_exit_pulse") and self._exit_pulse.isActive():
                self._exit_pulse.stop()
            self._teardown_runtime_threads()
            shutdown_all_children()
            if event is not None:
                event.accept()
            self.hide()
            QApplication.instance().quit()

        self._shutdown_services(on_done=_finish)

    def _on_about_to_quit(self) -> None:
        """正常退出时清理已在 StopServicesWorker 中完成；此处不再阻塞。"""
        if self._shutdown_done:
            return

    def _on_started(self, key: str):
        proc = self._processes.get(key)
        if proc:
            self._panels[key].set_running(True, proc.pid, proc.uptime)
        self._logs["service"].append(SERVICES[key]["name"], "=== 已启动 ===")
        self._update_total()

    def _on_stopped(self, key: str, code: int):
        if self._processes.get(key) is not None:
            self._processes[key] = None
        if not self._stop_busy:
            self._panels[key].set_running(False)
        self._logs["service"].append(SERVICES[key]["name"], f"已停止 ({code})")
        self._update_total()

    def _on_output(self, key: str, line: str):
        self._logs["service"].append(SERVICES[key]["name"], line)

    def _refresh_uptimes(self):
        for key, proc in self._processes.items():
            if proc and proc.isRunning():
                self._panels[key]._lbl_uptime.setText(proc.uptime)

    def _update_total(self):
        n = sum(1 for p in self._processes.values() if p and p.isRunning())
        t = len(SERVICES)
        self._lbl_total.setText(f"运行中 {n}/{t}")

    def _exit_app(self):
        if self._closing:
            QApplication.instance().quit()
            return
        r = QMessageBox.question(
            self, "退出", "将停止所有服务并退出控制台，确定？",
            QMessageBox.Yes | QMessageBox.No,
        )
        if r != QMessageBox.Yes:
            return
        self._request_exit()

    def closeEvent(self, event):
        if self._closing:
            event.accept()
            return
        r = QMessageBox.question(
            self, "退出", "将停止所有服务并退出控制台，确定？",
            QMessageBox.Yes | QMessageBox.No,
        )
        if r != QMessageBox.Yes:
            event.ignore()
            return
        event.ignore()
        self._request_exit(event)


def run_app():
    app = QApplication(sys.argv)
    app.setApplicationName("CustomDataSourceConsole")
    _apply_window_icon(app)
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(245, 245, 245))
    palette.setColor(QPalette.WindowText, QColor(31, 35, 40))
    palette.setColor(QPalette.Text, QColor(31, 35, 40))
    palette.setColor(QPalette.Base, QColor(255, 255, 255))
    app.setPalette(palette)
    try:
        win = MainWindow()
        win.show()
        return app.exec_()
    except Exception as e:
        write_startup_log(e)
        raise
