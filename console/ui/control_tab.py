"""控制命令 Tab"""

from __future__ import annotations

import json

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QPushButton,
    QPlainTextEdit, QComboBox, QLabel, QLineEdit, QScrollArea,
    QGridLayout, QSizePolicy,
)

from console.ui.combined_log import CombinedLogPanel
from console.ui.styles import panel_btn_style

BOTH_COMMANDS = [
    ("stats", "双端连接/缓存统计"),
    ("fanstudio_status", "Fan Studio 状态"),
    ("fanstudio_use_backup", "Fan 切备用"),
    ("fanstudio_use_primary", "Fan 切主站"),
    ("fanstudio_resume_auto", "Fan 恢复自动"),
    ("auto_check", "双端自动检查"),
    ("source_status", "数据源采集状态"),
]


class ControlTab(QWidget):
    def __init__(self, management_hub, log_panel: CombinedLogPanel, parent=None):
        super().__init__(parent)
        self._hub = management_hub
        self._log = log_panel
        self._hub.result_ready.connect(self._on_result)
        self._setup_ui()

    def _setup_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setMinimumWidth(420)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        inner = QWidget()
        inner.setMinimumWidth(400)
        layout = QVBoxLayout(inner)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        target_row = QHBoxLayout()
        target_row.addWidget(QLabel("目标:"))
        target_row.addWidget(QLabel("both (EEW+List)"))
        layout.addLayout(target_row)

        cmd_row = QHBoxLayout()
        cmd_row.addWidget(QLabel("命令:"))
        self._cmd = QComboBox()
        for cmd, label in BOTH_COMMANDS:
            self._cmd.addItem(f"{label} ({cmd})", cmd)
        cmd_row.addWidget(self._cmd)
        btn_send = QPushButton("发送")
        btn_send.setStyleSheet(panel_btn_style("start"))
        btn_send.clicked.connect(self._send_selected)
        cmd_row.addWidget(btn_send)
        layout.addLayout(cmd_row)

        ip_row = QHBoxLayout()
        ip_row.addWidget(QLabel("IP (黑名单等):"))
        self._ip_input = QLineEdit()
        self._ip_input.setPlaceholderText("可选")
        ip_row.addWidget(self._ip_input)
        layout.addLayout(ip_row)

        json_group = QGroupBox("自定义 JSON")
        json_layout = QVBoxLayout(json_group)
        self._json_input = QPlainTextEdit()
        self._json_input.setPlaceholderText('{"command": "stats"}')
        self._json_input.setMaximumHeight(80)
        json_layout.addWidget(self._json_input)
        btn_json = QPushButton("发送 JSON")
        btn_json.clicked.connect(self._send_json)
        json_layout.addWidget(btn_json)
        layout.addWidget(json_group)

        quick = QGroupBox("快捷操作")
        ql = QGridLayout(quick)
        ql.setHorizontalSpacing(8)
        ql.setVerticalSpacing(8)
        quick_buttons = [
            ("双端统计", "stats", "both"),
            ("双端自动检查", "auto_check", "both"),
            ("双端 Fan 备用", "fanstudio_use_backup", "both"),
            ("双端 Fan 主站", "fanstudio_use_primary", "both"),
            ("Wolfx 上游", "cea_jma_wolfx", "eew"),
            ("Fan 上游", "cea_jma_fanstudio", "eew"),
        ]
        for i, (text, cmd, tgt) in enumerate(quick_buttons):
            b = QPushButton(text)
            b.setStyleSheet(panel_btn_style("action"))
            b.setMinimumHeight(34)
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            b.clicked.connect(lambda checked=False, c=cmd, t=tgt: self._send(c, t))
            ql.addWidget(b, i // 3, i % 3)
        layout.addWidget(quick)
        layout.addStretch()

        scroll.setWidget(inner)
        outer.addWidget(scroll)

    def _on_result(self, target: str, result: object):
        if isinstance(result, str):
            text = result
        elif isinstance(result, dict):
            text = json.dumps(result, ensure_ascii=False, indent=2)
        else:
            text = str(result)
        self._log.append_line(f"[{target}]\n{text}")

    def _send(self, command: str, target: str, extra: dict = None):
        params = dict(extra or {})
        ip = self._ip_input.text().strip()
        if ip and "blacklist" in command:
            params["ip"] = ip
        if target == "both":
            self._hub.send_both(command)
        else:
            self._hub.send_command(target, command, params)

    def _send_selected(self):
        cmd = self._cmd.currentData()
        self._send(cmd, "both")

    def _send_json(self):
        raw = self._json_input.toPlainText().strip()
        if not raw:
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            self._log.append("错误", f"JSON 错误: {e}")
            return
        command = data.get("command", "")
        target = (data.get("channel") or "both").lower()
        if target == "both":
            self._hub.send_both(command, data)
        else:
            self._hub.send_command(target, command, data)
