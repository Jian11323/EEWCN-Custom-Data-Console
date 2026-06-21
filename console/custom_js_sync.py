"""EEWCN custom.js 端口 URL 同步。"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from services.common.ports import LOCAL_BIND, get_eew_port, get_list_port

_EEW_WS_RE = re.compile(
    rf"ws://{re.escape(LOCAL_BIND)}:(\d+)",
    re.IGNORECASE,
)
_LIST_HTTP_RE = re.compile(
    rf"http://{re.escape(LOCAL_BIND)}:(\d+)/earthquakes",
    re.IGNORECASE,
)


@dataclass
class SyncResult:
    ok: bool
    path: Optional[Path]
    message: str
    changed: bool = False


def default_custom_js_path() -> Path:
    try:
        from services.common.paths import get_app_root

        return get_app_root() / "custom.js"
    except Exception:
        return Path("custom.js")


def resolve_custom_js_path(custom_path: str = "") -> Path:
    raw = (custom_path or "").strip()
    if raw:
        return Path(raw).expanduser()
    return default_custom_js_path()


def sync_custom_js_ports(
    eew_port: Optional[int] = None,
    list_port: Optional[int] = None,
    custom_path: str = "",
) -> SyncResult:
    path = resolve_custom_js_path(custom_path)
    eew = eew_port if eew_port is not None else get_eew_port()
    lst = list_port if list_port is not None else get_list_port()

    if not path.is_file():
        return SyncResult(
            ok=True,
            path=path,
            message=f"未找到 custom.js（{path}），已保存端口配置；放置文件后可再次同步。",
            changed=False,
        )

    text = path.read_text(encoding="utf-8")
    new_text = _EEW_WS_RE.sub(f"ws://{LOCAL_BIND}:{eew}", text)
    new_text = _LIST_HTTP_RE.sub(f"http://{LOCAL_BIND}:{lst}/earthquakes", new_text)

    if new_text == text:
        return SyncResult(
            ok=True,
            path=path,
            message=f"custom.js 无需更新（{path}）",
            changed=False,
        )

    backup = path.with_suffix(path.suffix + ".bak")
    shutil.copy2(path, backup)
    path.write_text(new_text, encoding="utf-8")
    return SyncResult(
        ok=True,
        path=path,
        message=f"已同步 custom.js 端口并备份至 {backup.name}",
        changed=True,
    )
