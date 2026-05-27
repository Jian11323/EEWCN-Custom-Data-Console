"""单服务控制面板"""

from __future__ import annotations

from typing import Dict

from PyQt5.QtCore import QTimer, pyqtSignal
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QFormLayout,
    QLabel, QLineEdit, QPushButton, QFrame, QGridLayout,
)

from console.ui.styles import panel_btn_style


class ServicePanel(QWidget):
    start_requested = pyqtSignal(str, dict)
    restart_requested = pyqtSignal(str, dict)
    stop_requested = pyqtSignal(str)

    def __init__(self, service_key: str, service_info: dict, saved_config: dict = None, parent=None):
        super().__init__(parent)
        self._key = service_key
        self._info = service_info
        self._config_widgets: Dict[str, QLineEdit] = {}
        saved_config = saved_config or {}
        self._setup_ui(saved_config)

    def _setup_ui(self, saved_config: dict):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(16, 16, 16, 16)

        title_widget = QWidget()
        title_layout = QHBoxLayout(title_widget)
        title_layout.setContentsMargins(0, 0, 0, 0)
        bar = QFrame()
        bar.setFixedWidth(4)
        bar.setStyleSheet(f"background-color: {self._info['color']}; border-radius: 2px;")
        title_layout.addWidget(bar)
        title_label = QLabel(self._info["name"])
        title_label.setStyleSheet("font-size: 15px; font-weight: bold;")
        title_layout.addWidget(title_label)
        title_layout.addStretch()
        self._status_dot = QLabel("●")
        self._status_dot.setStyleSheet("color: #8c959f; font-size: 16px;")
        title_layout.addWidget(self._status_dot)
        layout.addWidget(title_widget)

        desc_text = (self._info.get("description") or "").strip()
        if desc_text:
            desc = QLabel(desc_text)
            desc.setWordWrap(True)
            desc.setStyleSheet("color: #57606a;")
            layout.addWidget(desc)

        configs = self._info.get("config", {})
        if configs:
            cfg_group = QGroupBox("连接配置")
            cfg_form = QFormLayout(cfg_group)
            cfg_form.setSpacing(8)
            for key, cfg in configs.items():
                default = saved_config.get(key, cfg["default"])
                widget = QLineEdit(str(default))
                widget.setMinimumWidth(240)
                self._config_widgets[key] = widget
                cfg_form.addRow(QLabel(cfg["label"] + ":"), widget)
            layout.addWidget(cfg_group)

        status_group = QGroupBox("运行状态")
        status_grid = QGridLayout(status_group)
        status_grid.setSpacing(6)
        self._lbl_pid = QLabel("--")
        self._lbl_uptime = QLabel("未启动")
        self._lbl_ports = QLabel(self._info.get("ports", "--"))
        self._lbl_ports.setWordWrap(True)
        status_grid.addWidget(QLabel("进程:"), 0, 0)
        status_grid.addWidget(self._lbl_pid, 0, 1)
        status_grid.addWidget(QLabel("运行时长:"), 1, 0)
        status_grid.addWidget(self._lbl_uptime, 1, 1)
        status_grid.addWidget(QLabel("端口:"), 2, 0)
        status_grid.addWidget(self._lbl_ports, 2, 1)
        layout.addWidget(status_group)

        btn_layout = QHBoxLayout()
        self._btn_start = QPushButton("启动")
        self._btn_start.setStyleSheet(panel_btn_style("start"))
        self._btn_start.clicked.connect(self._do_start)
        self._btn_stop = QPushButton("停止")
        self._btn_stop.setStyleSheet(panel_btn_style("stop"))
        self._btn_stop.setEnabled(False)
        self._btn_stop.clicked.connect(lambda: self.stop_requested.emit(self._key))
        self._btn_restart = QPushButton("重启")
        self._btn_restart.setStyleSheet(panel_btn_style("restart"))
        self._btn_restart.setEnabled(False)
        self._btn_restart.clicked.connect(self._do_restart)
        btn_layout.addWidget(self._btn_start)
        btn_layout.addWidget(self._btn_stop)
        btn_layout.addWidget(self._btn_restart)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        layout.addStretch()

    def get_config(self) -> dict:
        return {k: w.text().strip() for k, w in self._config_widgets.items()}

    def _do_start(self):
        self.start_requested.emit(self._key, self.get_config())

    def _do_restart(self):
        self.stop_requested.emit(self._key)
        cfg = self.get_config()
        QTimer.singleShot(3500, lambda: self.restart_requested.emit(self._key, cfg))

    def set_running(self, running: bool, pid=None, uptime: str = ""):
        self._btn_stop.setText("停止")
        if running:
            self._status_dot.setStyleSheet("color: #1a7f37; font-size: 16px;")
            self._lbl_pid.setText(str(pid) if pid else "--")
            self._lbl_uptime.setText(uptime)
            self._btn_start.setEnabled(False)
            self._btn_stop.setEnabled(True)
            self._btn_restart.setEnabled(True)
        else:
            self._status_dot.setStyleSheet("color: #8c959f; font-size: 16px;")
            self._lbl_pid.setText("--")
            self._lbl_uptime.setText("已停止")
            self._btn_start.setEnabled(True)
            self._btn_stop.setEnabled(False)
            self._btn_restart.setEnabled(False)

    def set_stopping(self, stopping: bool = True) -> None:
        if not stopping:
            self._btn_stop.setText("停止")
            return
        self._status_dot.setStyleSheet("color: #bf8700; font-size: 16px;")
        self._lbl_uptime.setText("停止中...")
        self._btn_start.setEnabled(False)
        self._btn_stop.setEnabled(False)
        self._btn_restart.setEnabled(False)
        self._btn_stop.setText("停止中...")

    def append_log(self, line: str):
        pass
