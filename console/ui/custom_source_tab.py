"""自定义数据源 Tab（单 URL，格式与滚动字幕-公开版一致）"""

from __future__ import annotations

from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QPlainTextEdit, QMessageBox, QGroupBox,
)

from console.ui.styles import panel_btn_style
from services.common.source_switches import (
    CUSTOM_DATA_SOURCE_URL_KEY,
    get_custom_data_source_url,
    set_custom_data_source_url,
)
from services.internal import custom as custom_internal


class CustomSourceTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._url_entry = QLineEdit()
        self._url_entry.setPlaceholderText("输入 http/https/ws/wss URL，留空则关闭")
        self._status_label = QLabel("状态：—")
        self._status_label.setStyleSheet("color: #57606a; font-size: 12px;")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        title = QLabel("自定义数据源")
        title.setStyleSheet("font-size: 14px; font-weight: bold;")
        layout.addWidget(title)

        group = QGroupBox("连接")
        g_layout = QVBoxLayout(group)
        g_layout.addWidget(QLabel("自定义数据源 URL："))
        self._url_entry.setText(get_custom_data_source_url())
        g_layout.addWidget(self._url_entry)
        g_layout.addWidget(self._status_label)

        hint = QLabel(
            "• HTTP/HTTPS：每秒 GET 一次获取预警 JSON；留空即关闭。\n"
            "• WS/WSS：长连接接收 JSON 报文。\n"
            "• 融合列表中的机构名取自 JSON 的 source（优先）或 sourceName。\n"
            "• 修改 URL 后请重启「融合数据」服务后生效。"
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #57606a; font-size: 12px;")
        g_layout.addWidget(hint)
        layout.addWidget(group)

        fmt = QLabel("预警源数据格式示例（二选一）：")
        fmt.setStyleSheet("font-weight: bold; margin-top: 8px;")
        layout.addWidget(fmt)

        example_style = (
            "QPlainTextEdit { font-family: Consolas,Monaco,monospace; font-size: 11px; "
            "background: #f5f5f5; border: 1px solid #ddd; border-radius: 4px; padding: 6px; }"
        )
        flat = QPlainTextEdit()
        flat.setReadOnly(True)
        flat.setMaximumHeight(145)
        flat.setStyleSheet(example_style)
        flat.setPlainText(
            '格式一（平铺）：\n'
            '{\n'
            '  "eventID": "JMA_202512262525",\n'
            '  "placeName": "青森县东方冲",\n'
            '  "latitude": 41.1,\n'
            '  "longitude": 142.6,\n'
            '  "depth": 10,\n'
            '  "shockTime": "2025/12/25 25:24:00",\n'
            '  "reportNum": 5,\n'
            '  "magnitude": "3.5",\n'
            '  "sourceName": "JMA",\n'
            '  "source": "JMA"\n'
            '}'
        )
        layout.addWidget(flat)

        nested = QPlainTextEdit()
        nested.setReadOnly(True)
        nested.setMaximumHeight(145)
        nested.setStyleSheet(example_style)
        nested.setPlainText(
            '格式二（嵌套 Data）：\n'
            '{\n'
            '  "Data": {\n'
            '    "id": "CWA_202601190730",\n'
            '    "updates": 4,\n'
            '    "shockTime": "2026-01-19 07:30:00",\n'
            '    "latitude": 23.33,\n'
            '    "longitude": 120.82,\n'
            '    "depth": 10.0,\n'
            '    "magnitude": 4.5,\n'
            '    "placeName": "高雄市桃源區",\n'
            '    "source": "CWA"\n'
            '  }\n'
            '}'
        )
        layout.addWidget(nested)

        layout.addStretch()

        btn_row = QHBoxLayout()
        btn_save = QPushButton("保存")
        btn_save.setStyleSheet(panel_btn_style("action"))
        btn_save.clicked.connect(self._save_url)
        btn_row.addWidget(btn_save)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        self._timer = QTimer(self)
        self._timer.setInterval(2000)
        self._timer.timeout.connect(self._refresh_status)

    def showEvent(self, event):
        super().showEvent(event)
        self._timer.start()
        self._refresh_status()

    def hideEvent(self, event):
        self._timer.stop()
        super().hideEvent(event)

    def _save_url(self) -> None:
        url = self._url_entry.text().strip()
        if url:
            low = url.lower()
            if not (
                low.startswith("http://")
                or low.startswith("https://")
                or low.startswith("ws://")
                or low.startswith("wss://")
            ):
                QMessageBox.warning(
                    self,
                    "URL 无效",
                    "请以 http://、https://、ws:// 或 wss:// 开头。",
                )
                return
        set_custom_data_source_url(url)
        QMessageBox.information(
            self,
            "已保存",
            f"已写入配置（{CUSTOM_DATA_SOURCE_URL_KEY}）。\n"
            "请重启「融合数据」服务后采集才会使用新 URL。",
        )
        self._refresh_status()

    def _refresh_status(self) -> None:
        url = get_custom_data_source_url()
        if not url:
            self._status_label.setText("状态：未配置（已关闭）")
            self._status_label.setStyleSheet("color: #57606a; font-size: 12px;")
            return
        low = url.lower()
        if low.startswith("http://") or low.startswith("https://"):
            ok = custom_internal.get_http_last_ok()
            if ok:
                self._status_label.setText("状态：HTTP 最近请求成功")
                self._status_label.setStyleSheet("color: #1a7f37; font-size: 12px;")
            else:
                self._status_label.setText("状态：HTTP 未连接或最近请求失败")
                self._status_label.setStyleSheet("color: #cf222e; font-size: 12px;")
        elif low.startswith("ws://") or low.startswith("wss://"):
            st = custom_internal.get_ws_status()
            labels = {
                "connected": ("WS 已连接", "#1a7f37"),
                "connecting": ("WS 连接中…", "#9a6700"),
                "disconnected": ("WS 未连接", "#cf222e"),
            }
            text, color = labels.get(st, ("WS 未知", "#57606a"))
            self._status_label.setText(f"状态：{text}")
            self._status_label.setStyleSheet(f"color: {color}; font-size: 12px;")
        else:
            self._status_label.setText("状态：URL 协议无效")
            self._status_label.setStyleSheet("color: #cf222e; font-size: 12px;")
