"""关于软件 Tab"""

from __future__ import annotations

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QLabel, QScrollArea, QSizePolicy,
)

from console.version import APP_VERSION, VERSION_LABEL

_MUTED = "color: #57606a; font-size: 12px;"
_BODY = "color: #1f2328; font-size: 13px;"
_TITLE = "font-size: 18px; font-weight: bold; color: #1f2328;"
_SUB = "font-size: 13px; color: #57606a;"

_DATA_SOURCES = (
    "Fan Studio",
    "Wolfx",
    "P2PQuake",
    "BMKG",
    "GeoNet",
    "INGV",
    "Early-est",
)

_DISCLAIMERS = (
    "本软件依托第三方接口获取数据，内容时效性、准确性不作保证。",
    "所有参考内容仅作娱乐查阅使用，官方公告为最终标准！",
    "严禁盗用、转载及各类商业化牟利使用！",
    "本软件为免费开源软件，严禁任何形式的收费行为！",
)


def _muted_label(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setWordWrap(True)
    lbl.setStyleSheet(_MUTED)
    lbl.setAlignment(Qt.AlignTop | Qt.AlignLeft)
    lbl.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
    return lbl


class AboutTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        inner = QWidget()
        layout = QVBoxLayout(inner)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(14)

        title = QLabel(f"自定义数据源控制台 v{APP_VERSION}")
        title.setStyleSheet(_TITLE)
        layout.addWidget(title)

        ver = QLabel(f"当前版本：{VERSION_LABEL}")
        ver.setStyleSheet(_SUB)
        layout.addWidget(ver)

        dev = QLabel("开发者：纪安")
        dev.setStyleSheet(_BODY)
        layout.addWidget(dev)

        src_group = QGroupBox("数据源支持")
        src_layout = QVBoxLayout(src_group)
        for name in _DATA_SOURCES:
            src_layout.addWidget(_muted_label(f"· {name}"))
        layout.addWidget(src_group)

        decl_group = QGroupBox("声明")
        decl_layout = QVBoxLayout(decl_group)
        for line in _DISCLAIMERS:
            lbl = QLabel(line)
            lbl.setWordWrap(True)
            lbl.setStyleSheet(_BODY)
            lbl.setAlignment(Qt.AlignTop | Qt.AlignLeft)
            decl_layout.addWidget(lbl)
        layout.addWidget(decl_group)

        layout.addStretch()
        scroll.setWidget(inner)
        outer.addWidget(scroll)
