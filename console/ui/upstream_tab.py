"""上游与切换 Tab"""

import json

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QPushButton, QLabel, QMessageBox,
)

from console.management_client import format_management_message
from console.ui.combined_log import CombinedLogPanel
from console.ui.styles import panel_btn_style


class UpstreamTab(QWidget):
    def __init__(self, management_hub, log_panel: CombinedLogPanel, parent=None):
        super().__init__(parent)
        self._hub = management_hub
        self._log = log_panel
        self._hub.result_ready.connect(self._on_result)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        layout.addWidget(QLabel(
            "Fan Studio 主备由融合服务统一控制；全局按钮经管理端口 2050（channel=both）一次下发。"
        ))

        fan_group = QGroupBox("Fan Studio 主备（EEW + List）")
        fl = QVBoxLayout(fan_group)
        for text, cmd in [
            ("切换备用 (.hk)", "fanstudio_use_backup"),
            ("切换主站 (.tech)", "fanstudio_use_primary"),
            ("恢复自动切换", "fanstudio_resume_auto"),
            ("查询状态", "fanstudio_status"),
        ]:
            b = QPushButton(text)
            b.setStyleSheet(panel_btn_style("action"))
            b.clicked.connect(lambda checked=False, c=cmd: self._confirm_both(c))
            fl.addWidget(b)
        layout.addWidget(fan_group)

        wolfx_group = QGroupBox("Wolfx 上游（仅 EEW）")
        wl = QVBoxLayout(wolfx_group)
        for text, cmd in [
            ("CEA/JMA → Wolfx", "cea_jma_wolfx"),
            ("CEA/JMA → Fan Studio", "cea_jma_fanstudio"),
        ]:
            b = QPushButton(text)
            b.setStyleSheet(panel_btn_style("restart"))
            b.clicked.connect(lambda checked=False, c=cmd: self._hub.send_command("eew", c))
            wl.addWidget(b)
        layout.addWidget(wolfx_group)

        eew_only = QGroupBox("EEW 专用")
        el = QVBoxLayout(eew_only)
        b = QPushButton("Fan Studio 状态 (EEW)")
        b.clicked.connect(lambda: self._hub.send_command("eew", "fanstudio_status"))
        el.addWidget(b)
        layout.addWidget(eew_only)
        layout.addStretch()

    def _confirm_both(self, command: str):
        r = QMessageBox.question(
            self, "确认",
            f"将向融合管理端口 2050 发送（channel=both）: {command}",
            QMessageBox.Yes | QMessageBox.No,
        )
        if r == QMessageBox.Yes:
            self._hub.send_both(command)

    def _on_result(self, target: str, result: object):
        if isinstance(result, str):
            text = format_management_message(result) or result
        elif isinstance(result, dict):
            text = json.dumps(result, ensure_ascii=False, indent=2)
        else:
            text = str(result)
        self._log.append(target, text[:4000])
