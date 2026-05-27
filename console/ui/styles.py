"""共享 Qt 样式"""

LIGHT_THEME = """
    QMainWindow, QWidget {
        background-color: #f5f5f5;
        color: #1f2328;
        font-family: 'Microsoft YaHei', 'Segoe UI', sans-serif;
    }
    QLabel { color: #1f2328; background: transparent; }
    QGroupBox {
        color: #1f2328;
        font-weight: bold;
        border: 1px solid #d0d7de;
        border-radius: 8px;
        margin-top: 10px;
        padding-top: 12px;
        background-color: #ffffff;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        left: 12px;
        padding: 0 6px;
    }
    QTabWidget::pane {
        border: 1px solid #d0d7de;
        border-radius: 8px;
        background-color: #ffffff;
    }
    QTabBar::tab {
        background: #eaeef2;
        color: #1f2328;
        padding: 8px 16px;
        margin-right: 2px;
        border-top-left-radius: 6px;
        border-top-right-radius: 6px;
    }
    QTabBar::tab:selected { background: #0969da; color: #ffffff; }
    QSplitter::handle { background-color: #d0d7de; width: 2px; }
    QLineEdit, QComboBox, QPlainTextEdit {
        background-color: #ffffff;
        color: #1f2328;
        border: 1px solid #d0d7de;
        border-radius: 4px;
        padding: 4px 8px;
    }
    QListWidget {
        background-color: #ffffff;
        color: #1f2328;
        border: 1px solid #d0d7de;
        border-radius: 6px;
        outline: none;
    }
    QListWidget::item {
        padding: 8px 6px;
        border-radius: 4px;
    }
    QListWidget::item:selected {
        background-color: #ddf4ff;
        color: #0969da;
    }
    QScrollArea { border: none; background: transparent; }
    QFrame[frameShape="4"] { color: #d0d7de; }
    QCheckBox { color: #1f2328; }
"""

LOG_CONSOLE_STYLE = """
    QPlainTextEdit {
        background-color: #1e1e1e;
        color: #d4d4d4;
        font-family: Consolas, 'Courier New', monospace;
        font-size: 12px;
        border: 1px solid #d0d7de;
        border-radius: 6px;
        padding: 6px;
    }
"""


def global_btn_style(bg: str, hover: str) -> str:
    return f"""
        QPushButton {{
            background-color: {bg};
            color: white;
            border: 1px solid {hover};
            border-radius: 5px;
            padding: 5px 14px;
            font-size: 12px;
            font-weight: bold;
        }}
        QPushButton:hover {{ background-color: {hover}; }}
    """


def panel_btn_style(kind: str) -> str:
    colors = {
        "start": ("#0969da", "#0550ae"),
        "stop": ("#6e7781", "#57606a"),
        "restart": ("#0969da", "#0550ae"),
        "action": ("#f6f8fa", "#d0d7de"),
    }
    bg, hover = colors.get(kind, colors["action"])
    text = "#ffffff" if kind in ("start", "stop", "restart") else "#1f2328"
    border = hover
    return f"""
        QPushButton {{
            font-size: 13px;
            font-weight: bold;
            padding: 6px 10px;
            min-height: 28px;
            border-radius: 5px;
            background-color: {bg};
            color: {text};
            border: 1px solid {border};
        }}
        QPushButton:hover:!disabled {{ background-color: {hover}; }}
        QPushButton:disabled {{
            background-color: #eaeef2;
            color: #8c959f;
            border: 1px solid #d0d7de;
        }}
    """
