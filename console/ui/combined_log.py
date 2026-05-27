"""合并日志面板"""

from datetime import datetime

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPlainTextEdit, QCheckBox, QPushButton

from console.ui.styles import LOG_CONSOLE_STYLE, panel_btn_style


class CombinedLogPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("进程输出")
        title.setStyleSheet("font-size: 14px; font-weight: bold;")
        header.addWidget(title)
        header.addStretch()
        self._auto_scroll = QCheckBox("自动滚动")
        self._auto_scroll.setChecked(True)
        header.addWidget(self._auto_scroll)
        layout.addLayout(header)

        self._log_view = QPlainTextEdit()
        self._log_view.setReadOnly(True)
        self._log_view.setMaximumBlockCount(5000)
        self._log_view.setStyleSheet(LOG_CONSOLE_STYLE)
        layout.addWidget(self._log_view, stretch=1)

        clear_btn = QPushButton("清空")
        clear_btn.setStyleSheet(panel_btn_style("action"))
        clear_btn.clicked.connect(self._log_view.clear)
        layout.addWidget(clear_btn, alignment=Qt.AlignRight)

    def append(self, label: str, line: str):
        ts = datetime.now().strftime("%H:%M:%S")
        tag = f"[{label}] " if label else ""
        self._log_view.appendPlainText(f"[{ts}] {tag}{line}")
        if self._auto_scroll.isChecked():
            sb = self._log_view.verticalScrollBar()
            sb.setValue(sb.maximum())

    def append_line(self, line: str):
        self.append("", line)
