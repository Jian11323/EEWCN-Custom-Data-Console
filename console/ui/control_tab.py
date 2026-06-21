"""管理控制 Tab（原控制命令 + 上游切换）"""

from __future__ import annotations

import json
from datetime import datetime
from typing import NamedTuple, Optional

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QPushButton,
    QPlainTextEdit, QLabel, QLineEdit, QScrollArea,
    QGridLayout, QSizePolicy, QMessageBox,
)

from console.ui.combined_log import CombinedLogPanel
from console.ui.styles import LOG_CONSOLE_STYLE, panel_btn_style

_CMD_BTN_MIN_WIDTH = 160
_GRID_COLS = 3

_IP_COMMANDS = frozenset({
    "history", "full_history", "ip_details",
    "blacklist_add", "blacklist_remove",
})


class _CmdSpec(NamedTuple):
    command: str
    label: str
    target: str
    style: str = "action"
    extra: Optional[dict] = None
    confirm: bool = False


COMMAND_GROUPS: list[tuple[str, list[_CmdSpec]]] = [
    ("双端 (EEW + List)", [
        _CmdSpec("stats", "双端连接/缓存统计", "both"),
        _CmdSpec("fanstudio_status", "Fan Studio 状态", "both"),
        _CmdSpec("fanstudio_use_backup", "Fan 切备用", "both", confirm=True),
        _CmdSpec("fanstudio_use_primary", "Fan 切主站", "both", confirm=True),
        _CmdSpec("fanstudio_resume_auto", "Fan 恢复自动", "both", confirm=True),
        _CmdSpec("auto_check", "双端自动检查", "both"),
        _CmdSpec("source_status", "数据源采集状态", "both"),
    ]),
    ("EEW 连接与黑名单", [
        _CmdSpec("stats", "EEW 连接统计", "eew"),
        _CmdSpec("history", "历史连接记录", "eew"),
        _CmdSpec("full_history", "完整历史记录", "eew"),
        _CmdSpec("ip_details", "IP 连接详情", "eew"),
        _CmdSpec("blacklist_list", "黑名单列表", "eew"),
        _CmdSpec("blacklist_add", "加入黑名单", "eew"),
        _CmdSpec("blacklist_remove", "移除黑名单", "eew"),
    ]),
    ("EEW 上游切换", [
        _CmdSpec("cea_jma_wolfx", "CEA/JMA → Wolfx", "eew", "restart"),
        _CmdSpec("cea_jma_fanstudio", "CEA/JMA → Fan Studio", "eew", "restart"),
    ]),
    ("EEW 系统", [
        _CmdSpec(
            "set_connection_limits", "设置连接限制", "eew",
            extra={"max_connections": 20, "timeout": 1800},
        ),
        _CmdSpec("thread_pool_status", "线程池状态", "eew"),
        _CmdSpec("thread_pool_check", "线程池检查", "eew"),
        _CmdSpec("thread_pool_restart", "线程池重启", "eew", "restart"),
    ]),
    ("数据配置查询", [
        _CmdSpec("source_switches_get", "预警数据源开关", "eew"),
        _CmdSpec("source_switches_get", "速报数据源开关", "list"),
        _CmdSpec("source_filters_get", "预警过滤配置", "eew"),
        _CmdSpec("source_filters_get", "速报过滤配置", "list"),
    ]),
    ("List 专用", [
        _CmdSpec("error_stats", "List 错误统计", "list"),
        _CmdSpec("stats", "List 缓存统计", "list"),
    ]),
    ("帮助", [
        _CmdSpec("all_commands", "命令列表", "eew"),
    ]),
]


class ManagementControlTab(QWidget):
    def __init__(self, management_hub, log_panel: CombinedLogPanel, parent=None):
        super().__init__(parent)
        self._hub = management_hub
        self._hub.result_ready.connect(self._on_result)
        self._setup_ui()

    def _setup_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setMinimumWidth(420)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        inner = QWidget()
        inner.setMinimumWidth(400)
        layout = QVBoxLayout(inner)
        layout.setContentsMargins(16, 16, 16, 8)
        layout.setSpacing(10)

        hint = QLabel(
            "Fan Studio 主备与 CEA/JMA 上游切换；双端命令经 channel=both 一次下发。"
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #57606a; font-size: 12px;")
        layout.addWidget(hint)

        ip_row = QHBoxLayout()
        ip_row.addWidget(QLabel("IP (黑名单/历史等):"))
        self._ip_input = QLineEdit()
        self._ip_input.setPlaceholderText("可选，黑名单/历史查询时使用")
        ip_row.addWidget(self._ip_input)
        layout.addLayout(ip_row)

        for group_title, commands in COMMAND_GROUPS:
            group = QGroupBox(group_title)
            grid = QGridLayout(group)
            grid.setHorizontalSpacing(8)
            grid.setVerticalSpacing(8)
            for i, spec in enumerate(commands):
                btn = QPushButton(spec.label)
                btn.setStyleSheet(panel_btn_style(spec.style))
                btn.setMinimumHeight(34)
                btn.setMinimumWidth(_CMD_BTN_MIN_WIDTH)
                btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
                btn.clicked.connect(
                    lambda checked=False, s=spec: self._send_spec(s)
                )
                grid.addWidget(btn, i // _GRID_COLS, i % _GRID_COLS)
            layout.addWidget(group)

        layout.addStretch()
        scroll.setWidget(inner)
        outer.addWidget(scroll, stretch=1)

        response_group = QGroupBox("命令返回")
        response_layout = QVBoxLayout(response_group)
        response_layout.setContentsMargins(12, 12, 12, 12)
        response_layout.setSpacing(8)
        self._result_view = QPlainTextEdit()
        self._result_view.setReadOnly(True)
        self._result_view.setMinimumHeight(100)
        self._result_view.setMaximumHeight(140)
        self._result_view.setStyleSheet(LOG_CONSOLE_STYLE)
        response_layout.addWidget(self._result_view)
        clear_row = QHBoxLayout()
        clear_row.addStretch()
        btn_clear = QPushButton("清空返回")
        btn_clear.setStyleSheet(panel_btn_style("action"))
        btn_clear.clicked.connect(self._result_view.clear)
        clear_row.addWidget(btn_clear)
        response_layout.addLayout(clear_row)
        outer.addWidget(response_group)

    def _append_result(self, target: str, text: str) -> None:
        header = f"[{target}] {datetime.now():%H:%M:%S}\n"
        self._result_view.appendPlainText(header + text + "\n")
        sb = self._result_view.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _on_result(self, target: str, result: object):
        if isinstance(result, str):
            text = result
        elif isinstance(result, dict):
            text = json.dumps(result, ensure_ascii=False, indent=2)
        else:
            text = str(result)
        self._append_result(target, text)

    def _send_spec(self, spec: _CmdSpec) -> None:
        if spec.confirm:
            channel = spec.target if spec.target != "both" else "both"
            r = QMessageBox.question(
                self, "确认",
                f"将向融合服务发送（channel={channel}）: {spec.command}",
                QMessageBox.Yes | QMessageBox.No,
            )
            if r != QMessageBox.Yes:
                return
        params = dict(spec.extra or {})
        ip = self._ip_input.text().strip()
        if ip and (spec.command in _IP_COMMANDS or "blacklist" in spec.command):
            params["ip"] = ip
        if spec.target == "both":
            self._hub.send_both(spec.command, params or None)
        else:
            self._hub.send_command(spec.target, spec.command, params or None)


# 兼容旧引用
ControlTab = ManagementControlTab
