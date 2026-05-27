"""数据源开关 Tab"""

from __future__ import annotations

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QCheckBox, QPushButton,
    QLabel, QScrollArea, QMessageBox, QSizePolicy, QDoubleSpinBox, QGridLayout,
)

from console.ui.styles import panel_btn_style
from services.common.source_filters import (
    LIST_FOREIGN_IDS,
    EEW_FOREIGN_IDS,
    DEFAULT_LIST_THRESHOLD,
    DEFAULT_EEW_THRESHOLD,
    get_filter_registry,
    load_from_env_or_settings,
)
from services.common.source_switches import (
    EEW_SOURCES,
    EEW_SOURCE_NAMES,
    LIST_SOURCES,
    LIST_SOURCE_NAMES,
    get_registry,
    load_from_settings_path,
    save_to_settings_path,
    apply_eew_patch,
)


class SourcesTab(QWidget):
    def __init__(self, management_hub, parent=None):
        super().__init__(parent)
        self._hub = management_hub
        self._eew_boxes: dict[str, QCheckBox] = {}
        self._list_boxes: dict[str, QCheckBox] = {}
        self._list_threshold_spin: dict[str, QDoubleSpinBox] = {}
        self._list_region_cb: dict[str, QCheckBox] = {}
        self._eew_region_cb: dict[str, QCheckBox] = {}
        self._eew_threshold_spin: dict[str, QDoubleSpinBox] = {}
        self._building = False

        outer = QVBoxLayout(self)
        outer.setContentsMargins(16, 16, 16, 16)
        outer.setSpacing(10)

        hint_group = QGroupBox("说明")
        hint_layout = QVBoxLayout(hint_group)
        hint_style = "color: #57606a; font-size: 12px;"
        for line in (
            "关闭数据源后：停止解析新报文，不再写入融合列表。",
            "速报 HTTP 8150：关闭后保留列表中已有条目，不删除磁盘缓存。",
            "预警 WS 5000：关闭后从推送列表移除该源，磁盘缓存仍保留。",
            "国外速报可设震级阈值（低于阈值不写入 8150）。",
            "勾选「中台日地区不过滤」后，中国/台湾/日本区域事件保留，境外事件丢弃。",
            "国外预警可设震级阈值；勾选「中台日地区不过滤」后，境外低于阈值丢弃，中台日区域始终保留。",
        ):
            lbl = QLabel(line)
            lbl.setWordWrap(True)
            lbl.setStyleSheet(hint_style)
            lbl.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
            lbl.setAlignment(Qt.AlignTop | Qt.AlignLeft)
            hint_layout.addWidget(lbl)
        hint_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        outer.addWidget(hint_group)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        inner = QWidget()
        layout = QVBoxLayout(inner)

        eew_group = QGroupBox("预警 (WebSocket 5000)")
        eew_layout = QVBoxLayout(eew_group)
        for sid in EEW_SOURCES:
            cb = QCheckBox(EEW_SOURCE_NAMES.get(sid, sid))
            self._eew_boxes[sid] = cb
            eew_layout.addWidget(cb)
        layout.addWidget(eew_group)

        list_group = QGroupBox("速报 (HTTP 8150)")
        list_layout = QVBoxLayout(list_group)
        for sid in LIST_SOURCES:
            cb = QCheckBox(LIST_SOURCE_NAMES.get(sid, sid))
            self._list_boxes[sid] = cb
            list_layout.addWidget(cb)
        layout.addWidget(list_group)

        foreign_list_group = QGroupBox("国外速报阈值与地区过滤")
        fl_grid = QGridLayout(foreign_list_group)
        fl_grid.addWidget(QLabel("数据来源"), 0, 0)
        fl_grid.addWidget(QLabel("震级阈值"), 0, 1)
        fl_grid.addWidget(QLabel("中台日地区不过滤"), 0, 2)
        for row, sid in enumerate(LIST_FOREIGN_IDS, start=1):
            fl_grid.addWidget(QLabel(LIST_SOURCE_NAMES.get(sid, sid)), row, 0)
            spin = QDoubleSpinBox()
            spin.setRange(0.0, 9.9)
            spin.setSingleStep(0.1)
            spin.setDecimals(1)
            spin.setValue(DEFAULT_LIST_THRESHOLD)
            self._list_threshold_spin[sid] = spin
            fl_grid.addWidget(spin, row, 1)
            region_cb = QCheckBox()
            region_cb.setToolTip("勾选后仅保留中国、台湾、日本区域事件，境外事件丢弃")
            self._list_region_cb[sid] = region_cb
            fl_grid.addWidget(region_cb, row, 2)
        layout.addWidget(foreign_list_group)

        foreign_eew_group = QGroupBox("国外预警阈值与地区过滤")
        fe_grid = QGridLayout(foreign_eew_group)
        fe_grid.addWidget(QLabel("数据来源"), 0, 0)
        fe_grid.addWidget(QLabel("震级阈值"), 0, 1)
        fe_grid.addWidget(QLabel("中台日地区不过滤"), 0, 2)
        for row, sid in enumerate(EEW_FOREIGN_IDS, start=1):
            fe_grid.addWidget(QLabel(EEW_SOURCE_NAMES.get(sid, sid)), row, 0)
            spin = QDoubleSpinBox()
            spin.setRange(0.0, 9.9)
            spin.setSingleStep(0.1)
            spin.setDecimals(1)
            spin.setValue(DEFAULT_EEW_THRESHOLD)
            spin.setToolTip("境外预警低于该震级不写入融合列表；中台日区域不受此限制")
            self._eew_threshold_spin[sid] = spin
            fe_grid.addWidget(spin, row, 1)
            region_cb = QCheckBox()
            region_cb.setToolTip("勾选后仅保留中国、台湾、日本区域预警，境外预警一律丢弃")
            self._eew_region_cb[sid] = region_cb
            fe_grid.addWidget(region_cb, row, 2)
        layout.addWidget(foreign_eew_group)

        scroll.setWidget(inner)
        outer.addWidget(scroll, stretch=1)

        btn_row = QHBoxLayout()
        btn_save = QPushButton("保存并应用")
        btn_save.setStyleSheet(panel_btn_style("action"))
        btn_save.clicked.connect(self._save_and_apply)
        btn_reload = QPushButton("重新加载配置")
        btn_reload.clicked.connect(self._reload_from_disk)
        btn_row.addWidget(btn_save)
        btn_row.addWidget(btn_reload)
        btn_row.addStretch()
        outer.addLayout(btn_row)

        self._reload_from_disk()

    def _reload_from_disk(self):
        self._building = True
        load_from_settings_path()
        eew = get_registry("eew").snapshot()
        lst = get_registry("list").snapshot()
        for sid, cb in self._eew_boxes.items():
            cb.setChecked(eew.get(sid, True))
        for sid, cb in self._list_boxes.items():
            cb.setChecked(lst.get(sid, True))

        filters = get_filter_registry().snapshot()
        list_thr = filters.get("list_source_threshold", {})
        list_reg = filters.get("list_source_region_filter", {})
        eew_thr = filters.get("eew_source_threshold", {})
        eew_reg = filters.get("eew_source_region_filter", {})
        for sid, spin in self._list_threshold_spin.items():
            spin.setValue(float(list_thr.get(sid, DEFAULT_LIST_THRESHOLD)))
        for sid, cb in self._list_region_cb.items():
            cb.setChecked(bool(list_reg.get(sid, False)))
        for sid, spin in self._eew_threshold_spin.items():
            spin.setValue(float(eew_thr.get(sid, DEFAULT_EEW_THRESHOLD)))
        for sid, cb in self._eew_region_cb.items():
            cb.setChecked(bool(eew_reg.get(sid, False)))
        self._building = False

    def _collect_patches(self) -> tuple[dict, dict, dict]:
        eew_patch = {sid: cb.isChecked() for sid, cb in self._eew_boxes.items()}
        list_patch = {sid: cb.isChecked() for sid, cb in self._list_boxes.items()}
        filter_patch = {
            "list_source_threshold": {
                sid: spin.value() for sid, spin in self._list_threshold_spin.items()
            },
            "list_source_region_filter": {
                sid: cb.isChecked() for sid, cb in self._list_region_cb.items()
            },
            "eew_source_threshold": {
                sid: spin.value() for sid, spin in self._eew_threshold_spin.items()
            },
            "eew_source_region_filter": {
                sid: cb.isChecked() for sid, cb in self._eew_region_cb.items()
            },
        }
        return eew_patch, list_patch, filter_patch

    def _apply_filter_patch(self, filter_patch: dict) -> None:
        reg = get_filter_registry()
        reg.apply_patch(
            list_threshold=filter_patch.get("list_source_threshold"),
            list_region_filter=filter_patch.get("list_source_region_filter"),
            eew_threshold=filter_patch.get("eew_source_threshold"),
            eew_region_filter=filter_patch.get("eew_source_region_filter"),
        )

    def _save_and_apply(self):
        eew_patch, list_patch, filter_patch = self._collect_patches()
        apply_eew_patch(eew_patch)
        get_registry("list").apply_patch(list_patch)
        self._apply_filter_patch(filter_patch)
        save_to_settings_path()

        fused_running = False
        try:
            parent = self.window()
            if hasattr(parent, "_processes"):
                proc = parent._processes.get("fused_core")
                fused_running = bool(proc and proc.is_running())
        except Exception:
            pass

        if fused_running:
            self._hub.send_command(
                "eew", "source_switches_set",
                {"channel": "eew", "patch": eew_patch},
            )
            self._hub.send_command(
                "list", "source_switches_set",
                {"channel": "list", "patch": list_patch},
            )
            self._hub.send_command("eew", "source_filters_set", filter_patch)
            self._hub.send_command("list", "source_filters_set", filter_patch)
            QMessageBox.information(
                self, "已保存",
                "配置已写入并已向融合服务发送热更新命令。",
            )
        else:
            QMessageBox.information(
                self, "已保存",
                "配置已写入。启动「融合数据」服务后将自动生效。",
            )
