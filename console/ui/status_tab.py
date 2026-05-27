"""数据源状态 / 健康探活 Tab"""

import json
import time
from datetime import datetime

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFontMetrics
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLabel,
    QPushButton, QGridLayout,
)

from console.config import ConfigStore
from console.health_monitor import HealthCheckWorker, DEFAULT_CHECKS, PORT_PROBE_ORDER
from console.management_client import ManagementWorker
from console.ui.combined_log import CombinedLogPanel
from console.ui.styles import panel_btn_style

SOURCE_DISPLAY_ORDER = (
    "fanstudio", "wolfx", "custom", "bmkg", "geonet", "early-est",
    "ingv", "p2p_jma",
)

SOURCE_DISPLAY_NAMES = {
    "fanstudio": "Fan Studio",
    "wolfx": "Wolfx",
    "custom": "自定义",
    "bmkg": "BMKG",
    "geonet": "Geonet",
    "early-est": "Early-est",
    "ingv": "INGV",
    "p2p_jma": "P2PQuake",
}

_CHIP_STYLE_IDLE = (
    "padding: 4px 8px; background: #eaeef2; border-radius: 4px; color: #57606a;"
)


def _name_label_width(label_fn, keys: tuple, font) -> int:
    """两列共用同一标签宽度，避免右对齐较长名称时左侧被裁切。"""
    if not keys:
        return 56
    fm = QFontMetrics(font)
    measure = getattr(fm, "horizontalAdvance", fm.width)
    return max(measure(label_fn(k)) for k in keys) + 16


def _fill_name_chip_grid(
    parent: QVBoxLayout,
    keys: tuple,
    chips: dict,
    label_fn,
    *,
    columns: int = 2,
    pair_gap: int = 28,
) -> None:
    """名称与状态分列对齐；两列紧凑排列，避免中间被拉得过开。"""
    container = QWidget()
    outer = QHBoxLayout(container)
    outer.setContentsMargins(0, 0, 0, 0)
    outer.setSpacing(pair_gap)

    font = QLabel().font()
    side_keys = [keys[i::columns] for i in range(columns)]
    label_width = _name_label_width(label_fn, keys, font)

    for side in side_keys:
        if not side:
            continue
        col_widget = QWidget()
        col_grid = QGridLayout(col_widget)
        col_grid.setContentsMargins(0, 0, 0, 0)
        col_grid.setHorizontalSpacing(8)
        col_grid.setVerticalSpacing(6)
        for row_idx, key in enumerate(side):
            lbl = QLabel(label_fn(key))
            lbl.setMinimumWidth(label_width)
            lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            chip = QLabel("—")
            chip.setStyleSheet(_CHIP_STYLE_IDLE)
            chips[key] = chip
            col_grid.addWidget(lbl, row_idx, 0)
            col_grid.addWidget(chip, row_idx, 1)
        outer.addWidget(col_widget, 0, Qt.AlignLeft)

    outer.addStretch()
    parent.addWidget(container)


class StatusTab(QWidget):
    def __init__(self, process_getter, log_panel: CombinedLogPanel, parent=None):
        super().__init__(parent)
        self._get_processes = process_getter
        self._log = log_panel
        self._health_worker = None
        self._source_worker = None
        self._chips: dict = {}
        self._source_chips: dict = {}
        self._polling_enabled = False
        self._svc_was_running = False
        self._setup_ui()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._refresh_all)
        self._timer.setInterval(10000)

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        svc_group = QGroupBox("服务进程")
        self._svc_grid = QGridLayout(svc_group)
        layout.addWidget(svc_group)

        src_group = QGroupBox("数据源采集")
        src_layout = QVBoxLayout(src_group)
        layout.addWidget(src_group)

        port_group = QGroupBox("出口端口探活")
        port_layout = QVBoxLayout(port_group)
        btn_row = QHBoxLayout()
        btn_check = QPushButton("立即检查")
        btn_check.setStyleSheet(panel_btn_style("start"))
        btn_check.clicked.connect(self._run_health_check)
        btn_row.addWidget(btn_check)
        btn_row.addStretch()
        port_layout.addLayout(btn_row)

        _fill_name_chip_grid(
            port_layout,
            PORT_PROBE_ORDER,
            self._chips,
            lambda key: key,
        )
        layout.addWidget(port_group)
        layout.addStretch()

        self._svc_labels = {}
        from console.services_registry import SERVICES
        for row, key in enumerate(SERVICES):
            name = SERVICES[key]["name"]
            lbl = QLabel(f"{name}: 已停止")
            lbl.setStyleSheet("color: #8c959f;")
            self._svc_labels[key] = lbl
            self._svc_grid.addWidget(lbl, row, 0)

        _fill_name_chip_grid(
            src_layout,
            SOURCE_DISPLAY_ORDER,
            self._source_chips,
            lambda sid: SOURCE_DISPLAY_NAMES.get(sid, sid),
        )

    def set_polling_enabled(self, enabled: bool) -> None:
        """仅在本 Tab 可见时轮询管理端口，避免刷屏日志。"""
        self._polling_enabled = enabled
        if enabled:
            if not self._timer.isActive():
                self._timer.start()
            self._refresh_all()
        else:
            self._timer.stop()

    def cancel_background_work(self, wait_ms: int = 1200) -> None:
        """退出前停止轮询与探活/状态查询线程。"""
        self._polling_enabled = False
        self._timer.stop()
        for worker in (self._source_worker, self._health_worker):
            if worker and worker.isRunning():
                worker.requestInterruption()
                if not worker.wait(min(wait_ms, 1000)):
                    worker.terminate()
                    worker.wait(400)
        self._source_worker = None
        self._health_worker = None

    def _refresh_all(self):
        self._refresh_services()
        if self._polling_enabled:
            self._poll_source_status()

    def _is_fused_running(self) -> bool:
        proc = self._get_processes().get("fused_core")
        return bool(proc and proc.is_running())

    def _refresh_services(self):
        procs = self._get_processes()
        from console.services_registry import SERVICES
        running = self._is_fused_running()
        for key, lbl in self._svc_labels.items():
            proc = procs.get(key)
            if proc and proc.is_running():
                lbl.setText(f"{SERVICES[key]['name']}: 运行中 PID {proc.pid} | {proc.uptime}")
                lbl.setStyleSheet("color: #1a7f37;")
            else:
                lbl.setText(f"{SERVICES[key]['name']}: 已停止")
                lbl.setStyleSheet("color: #8c959f;")
        if running and not self._svc_was_running and self._polling_enabled:
            QTimer.singleShot(2500, self._run_health_check)
        self._svc_was_running = running

    def _poll_source_status(self):
        procs = self._get_processes()
        if not (procs.get("fused_core") and procs["fused_core"].is_running()):
            for chip in self._source_chips.values():
                chip.setText("服务未运行")
                chip.setStyleSheet("padding: 4px 8px; background: #eaeef2; border-radius: 4px; color: #57606a;")
            return
        if self._source_worker and self._source_worker.isRunning():
            return
        s = ConfigStore.instance().settings
        self._source_worker = ManagementWorker(
            "eew", s.mgmt_host, s.mgmt_port, "source_status", {"channel": "eew"},
        )
        self._source_worker.finished.connect(self._on_source_status)
        self._source_worker.start()

    def _on_source_status(self, target: str, result: object):
        if not result or isinstance(result, str) and result.startswith("未安装"):
            return
        try:
            raw = result
            if isinstance(raw, str):
                data = json.loads(raw)
            else:
                data = raw
            if isinstance(data, dict) and data.get("type") == "source_status":
                payload = data.get("data", {})
            elif isinstance(data, dict) and "sources" in data:
                payload = data
            else:
                return
            sources = payload.get("sources", {})
        except Exception:
            return

        for sid, chip in self._source_chips.items():
            info = sources.get(sid, {})
            if not info:
                for k, v in sources.items():
                    if k.lower() == sid.lower():
                        info = v
                        break
            if not info:
                chip.setText("无数据")
                chip.setStyleSheet("padding: 4px 8px; background: #eaeef2; border-radius: 4px; color: #57606a;")
                continue
            ok = info.get("connected", False)
            cnt = info.get("message_count", 0)
            err = (info.get("last_error") or "").strip()
            last_ok = info.get("last_ok_at")
            recent_ok = last_ok and (time.time() - float(last_ok)) < 180
            if ok or recent_ok or cnt > 0:
                chip.setText(f"正常 · {cnt}条")
                chip.setToolTip(err[:200] if err else "")
                chip.setStyleSheet("padding: 4px 8px; background: #238636; border-radius: 4px; color: white;")
            elif err:
                chip.setText("异常")
                chip.setToolTip(err[:200])
                chip.setStyleSheet("padding: 4px 8px; background: #da3633; border-radius: 4px; color: white;")
            else:
                chip.setText("等待")
                chip.setToolTip("")
                chip.setStyleSheet("padding: 4px 8px; background: #bf8700; border-radius: 4px; color: white;")

    def _run_health_check(self):
        if self._health_worker and self._health_worker.isRunning():
            return
        if not self._is_fused_running():
            for chip in self._chips.values():
                chip.setText("—")
                chip.setToolTip("")
                chip.setStyleSheet(
                    "padding: 4px 8px; background: #eaeef2; border-radius: 4px; color: #57606a;"
                )
            self._log.append_line("【端口探活】 融合核心未运行，跳过（启动服务后将自动探活）")
            return
        self._health_worker = HealthCheckWorker(DEFAULT_CHECKS)
        self._health_worker.finished.connect(self._on_health)
        self._health_worker.start()

    def _on_health(self, results: dict):
        ts = datetime.now().strftime("%H:%M:%S")
        for key, chip in self._chips.items():
            info = results.get(key, {})
            if info.get("ok"):
                ms = info.get("latency_ms", "")
                chip.setText(f"正常 {ms}ms" if ms else "正常")
                chip.setStyleSheet("padding: 4px 8px; background: #238636; border-radius: 4px; color: white;")
            else:
                chip.setText("异常")
                chip.setStyleSheet("padding: 4px 8px; background: #da3633; border-radius: 4px; color: white;")
        lines = [f"【端口探活】 {ts}"]
        for key in PORT_PROBE_ORDER:
            info = results.get(key, {})
            if not info:
                continue
            if info.get("ok"):
                ms = info.get("latency_ms", "")
                lines.append(f"  {key}: 正常" + (f" ({ms}ms)" if ms else ""))
            else:
                err = info.get("error", "失败")
                lines.append(f"  {key}: 异常 — {err}")
        self._log.append_line("\n".join(lines))

    def showEvent(self, event):
        super().showEvent(event)
        self.set_polling_enabled(True)
        if self._is_fused_running():
            QTimer.singleShot(500, self._run_health_check)
        else:
            self._run_health_check()

    def hideEvent(self, event):
        super().hideEvent(event)
        self.set_polling_enabled(False)
