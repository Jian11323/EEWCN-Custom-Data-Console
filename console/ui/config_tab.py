"""端口配置 Tab（端口、custom.js 同步、HTTP 轮询间隔）"""

from __future__ import annotations

from typing import Callable, Dict, Optional

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QGroupBox, QFormLayout, QFileDialog, QMessageBox, QDoubleSpinBox, QScrollArea,
)

from console.config import ConfigStore
from console.custom_js_sync import resolve_custom_js_path, sync_custom_js_ports
from console.ui.styles import panel_btn_style
from services.common.http_poll_intervals import (
    HTTP_POLL_SOURCES,
    MAX_INTERVAL,
    MIN_INTERVAL,
    get_all_intervals,
    set_poll_intervals,
)
from services.common.ports import LOCAL_BIND, eew_ws_url, list_http_url


def _validate_port(text: str) -> Optional[int]:
    try:
        port = int(text.strip())
    except ValueError:
        return None
    if 1024 <= port <= 65535:
        return port
    return None


class ConfigTab(QWidget):
    def __init__(
        self,
        is_service_running: Callable[[], bool],
        mgmt=None,
        parent=None,
    ):
        super().__init__(parent)
        self._is_running = is_service_running
        self._mgmt = mgmt
        self._store = ConfigStore.instance()
        self._eew_port = QLineEdit()
        self._list_port = QLineEdit()
        self._custom_js = QLineEdit()
        self._preview_eew = QLabel()
        self._preview_list = QLabel()
        self._interval_spins: Dict[str, QDoubleSpinBox] = {}
        self._setup_ui()
        self._load_settings()

    def _setup_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        title = QLabel("端口配置")
        title.setStyleSheet("font-size: 14px; font-weight: bold;")
        layout.addWidget(title)

        intro = QLabel(
            "本控制台为 EEWCN 客户端配套工具。在此配置本地融合服务端口，"
            "保存后将尝试同步 EEWCN 的 custom.js 订阅地址。"
        )
        intro.setWordWrap(True)
        intro.setStyleSheet("color: #57606a; font-size: 12px;")
        layout.addWidget(intro)

        port_group = QGroupBox("服务端口（绑定 127.0.0.1）")
        form = QFormLayout(port_group)
        self._eew_port.setPlaceholderText("5000")
        self._list_port.setPlaceholderText("8150")
        form.addRow("预警 WebSocket:", self._eew_port)
        form.addRow("速报 HTTP:", self._list_port)
        layout.addWidget(port_group)

        interval_group = QGroupBox("HTTP 数据获取间隔（秒）")
        interval_form = QFormLayout(interval_group)
        interval_hint = QLabel(
            f"各 HTTP 轮询源的 GET 间隔，范围 {MIN_INTERVAL:g}–{MAX_INTERVAL:g} 秒。"
            "融合服务运行中保存后即时生效，无需重启。"
        )
        interval_hint.setWordWrap(True)
        interval_hint.setStyleSheet("color: #57606a; font-size: 12px;")
        interval_form.addRow(interval_hint)
        for key, (default, label) in HTTP_POLL_SOURCES.items():
            spin = QDoubleSpinBox()
            spin.setRange(MIN_INTERVAL, MAX_INTERVAL)
            spin.setSingleStep(0.5)
            spin.setDecimals(1)
            spin.setSuffix(" 秒")
            spin.setValue(default)
            self._interval_spins[key] = spin
            interval_form.addRow(f"{label}:", spin)
        layout.addWidget(interval_group)

        js_group = QGroupBox("EEWCN custom.js")
        js_layout = QVBoxLayout(js_group)
        row = QHBoxLayout()
        self._custom_js.setPlaceholderText("留空则使用程序目录下的 custom.js")
        row.addWidget(self._custom_js)
        btn_browse = QPushButton("浏览…")
        btn_browse.clicked.connect(self._browse_custom_js)
        row.addWidget(btn_browse)
        js_layout.addLayout(row)
        hint = QLabel("保存时将替换 custom.js 中的 ws://127.0.0.1:端口 与 http://127.0.0.1:端口/earthquakes")
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #57606a; font-size: 12px;")
        js_layout.addWidget(hint)
        layout.addWidget(js_group)

        preview_group = QGroupBox("订阅 URL 预览")
        preview_layout = QFormLayout(preview_group)
        preview_layout.addRow("预警:", self._preview_eew)
        preview_layout.addRow("速报:", self._preview_list)
        layout.addWidget(preview_group)

        self._eew_port.textChanged.connect(self._update_preview)
        self._list_port.textChanged.connect(self._update_preview)

        btn_row = QHBoxLayout()
        btn_save = QPushButton("保存配置")
        btn_save.setStyleSheet(panel_btn_style("action"))
        btn_save.clicked.connect(self._save)
        btn_row.addWidget(btn_save)
        btn_row.addStretch()
        layout.addLayout(btn_row)
        layout.addStretch()

        scroll.setWidget(content)
        outer.addWidget(scroll)

    def _load_settings(self) -> None:
        s = self._store.settings
        self._eew_port.setText(str(s.eew_port))
        self._list_port.setText(str(s.list_port))
        self._custom_js.setText(s.custom_js_path)
        intervals = s.http_poll_intervals or get_all_intervals()
        for key, spin in self._interval_spins.items():
            default = HTTP_POLL_SOURCES[key][0]
            spin.setValue(float(intervals.get(key, default)))
        self._update_preview()

    def _browse_custom_js(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "选择 custom.js",
            str(resolve_custom_js_path(self._custom_js.text()).parent),
            "JavaScript (*.js);;所有文件 (*.*)",
        )
        if path:
            self._custom_js.setText(path)

    def _update_preview(self) -> None:
        eew = _validate_port(self._eew_port.text()) or 5000
        lst = _validate_port(self._list_port.text()) or 8150
        self._preview_eew.setText(eew_ws_url(LOCAL_BIND, eew))
        self._preview_list.setText(list_http_url(LOCAL_BIND, lst))

    def _save(self) -> None:
        eew = _validate_port(self._eew_port.text())
        lst = _validate_port(self._list_port.text())
        if eew is None or lst is None:
            QMessageBox.warning(self, "端口无效", "请输入 1024–65535 范围内的有效端口号。")
            return
        if eew == lst:
            QMessageBox.warning(self, "端口冲突", "预警与速报端口不能相同。")
            return

        patch = {key: spin.value() for key, spin in self._interval_spins.items()}
        saved_intervals = set_poll_intervals(patch)

        s = self._store.settings
        s.eew_port = eew
        s.list_port = lst
        s.custom_js_path = self._custom_js.text().strip()
        s.http_poll_intervals = saved_intervals
        self._store.save()

        if self._mgmt is not None:
            self._mgmt.send_command(
                "eew",
                "HTTP_POLL_INTERVALS_SET",
                {"intervals": saved_intervals},
            )

        sync_result = sync_custom_js_ports(eew, lst, s.custom_js_path)
        from console.services_registry import refresh_service_ports_label

        refresh_service_ports_label()

        msg = f"配置已保存。\n{sync_result.message}"
        if self._is_running():
            msg += "\n\n端口变更需重启融合服务后生效；HTTP 轮询间隔已即时应用。"
        QMessageBox.information(self, "已保存", msg)

    def refresh_from_store(self) -> None:
        self._load_settings()
