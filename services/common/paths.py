"""应用根目录与数据路径（onefile 打包后资源在 _MEIPASS，可写目录在 LocalAppData）。"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Optional

_WRITABLE_APP_NAME = "自定义数据源控制台"
_LOGO_REL_PATHS = (Path("logo") / "logo.ico", Path("logo.ico"))


def get_app_root() -> Path:
    """打包后返回 exe 所在目录；开发时返回项目根。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def get_bundled_root() -> Optional[Path]:
    """PyInstaller onefile 解压的只读资源目录。"""
    if not getattr(sys, "frozen", False):
        return None
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass)
    return None


def get_writable_root() -> Path:
    """可写数据根（cache/logs）；onefile 使用 LocalAppData，避免临时目录只读。"""
    if getattr(sys, "frozen", False):
        root = Path(os.environ.get("LOCALAPPDATA", Path.home())) / _WRITABLE_APP_NAME
        root.mkdir(parents=True, exist_ok=True)
        return root
    return get_app_root()


def get_data_dir() -> Path:
    """区域数据 JSON（只读，优先从包内读取）。"""
    env = os.environ.get("DATA_DIR")
    if env:
        return Path(env)
    bundled = get_bundled_root()
    if bundled is not None:
        path = bundled / "data"
        if path.is_dir():
            return path
    return get_app_root() / "data"


def get_cache_dir() -> Path:
    env = os.environ.get("CACHE_DIR")
    if env:
        return Path(env)
    return get_writable_root() / "cache"


def get_log_dir() -> Path:
    env = os.environ.get("LOG_DIR")
    if env:
        return Path(env)
    return get_writable_root() / "logs"


def _logo_search_bases() -> list[Path]:
    bases: list[Path] = []
    bundled = get_bundled_root()
    if bundled is not None:
        bases.append(bundled)
    bases.append(get_app_root())
    if not getattr(sys, "frozen", False):
        bases.append(Path(__file__).resolve().parents[2])
    return bases


def _find_logo_source() -> Optional[Path]:
    for base in _logo_search_bases():
        for rel in _LOGO_REL_PATHS:
            path = (base / rel).resolve()
            if path.is_file():
                return path
    return None


def get_logo_path() -> Optional[Path]:
    """返回可用于 QIcon / 文件引用的 logo.ico 绝对路径。"""
    src = _find_logo_source()
    if src is None:
        return None
    # onefile 解压目录可能被 Qt 误读；复制到 LocalAppData 后更稳定
    if getattr(sys, "frozen", False):
        dest = get_writable_root() / "logo" / "logo.ico"
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists() or src.stat().st_mtime_ns != dest.stat().st_mtime_ns:
                shutil.copy2(src, dest)
            return dest.resolve()
        except OSError:
            return src.resolve()
    return src.resolve()


def _seed_tree_if_empty(name: str, dest: Path) -> None:
    """首次运行：将包内 cache/logs 模板复制到可写目录（目录为空时）。"""
    bundled = get_bundled_root()
    if bundled is None:
        return
    src = bundled / name
    if not src.is_dir():
        return
    dest.mkdir(parents=True, exist_ok=True)
    try:
        if any(dest.iterdir()):
            return
    except OSError:
        return
    shutil.copytree(src, dest, dirs_exist_ok=True)


def ensure_dirs() -> None:
    get_log_dir().mkdir(parents=True, exist_ok=True)
    get_cache_dir().mkdir(parents=True, exist_ok=True)
    (get_cache_dir() / "eew").mkdir(parents=True, exist_ok=True)
    (get_cache_dir() / "eew" / "translation").mkdir(parents=True, exist_ok=True)
    (get_cache_dir() / "list").mkdir(parents=True, exist_ok=True)
    (get_cache_dir() / "list" / "cache").mkdir(parents=True, exist_ok=True)
    (get_log_dir() / "eew").mkdir(parents=True, exist_ok=True)
    (get_log_dir() / "list").mkdir(parents=True, exist_ok=True)
    _seed_tree_if_empty("cache", get_cache_dir())
    _seed_tree_if_empty("logs", get_log_dir())
