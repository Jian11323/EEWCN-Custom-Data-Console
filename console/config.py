"""控制台配置持久化"""

from __future__ import annotations

import json
import os
import shutil
import threading
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Optional

from services.common.ports import DEFAULT_EEW_PORT, DEFAULT_LIST_PORT
from services.common.http_poll_intervals import HTTP_POLL_SOURCES, get_all_intervals

APP_NAME = "custom-datasource-console"


def _config_dir() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home())) / APP_NAME
    else:
        base = Path.home() / ".config" / APP_NAME
    base.mkdir(parents=True, exist_ok=True)
    return base


CONFIG_PATH = _config_dir() / "settings.json"


@dataclass
class ServiceEnvConfig:
    fused_core: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConsoleSettings:
    service_env: ServiceEnvConfig = field(default_factory=ServiceEnvConfig)
    eew_port: int = DEFAULT_EEW_PORT
    list_port: int = DEFAULT_LIST_PORT
    custom_js_path: str = ""
    auto_start_on_launch: bool = False
    start_delay_aggregate_sec: float = 0.0
    start_delay_after_aggregate_sec: float = 2.5
    custom_data_source_url: str = ""
    http_poll_intervals: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        intervals = self.http_poll_intervals or get_all_intervals()
        return {
            "service_env": asdict(self.service_env),
            "eew_port": self.eew_port,
            "list_port": self.list_port,
            "custom_js_path": self.custom_js_path,
            "auto_start_on_launch": self.auto_start_on_launch,
            "start_delay_aggregate_sec": self.start_delay_aggregate_sec,
            "start_delay_after_aggregate_sec": self.start_delay_after_aggregate_sec,
            "CUSTOM_DATA_SOURCE_URL": self.custom_data_source_url,
            "http_poll_intervals": intervals,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ConsoleSettings":
        se = data.get("service_env", {})
        raw_intervals = data.get("http_poll_intervals", {})
        intervals: Dict[str, float] = {}
        if isinstance(raw_intervals, dict):
            for key in HTTP_POLL_SOURCES:
                if key in raw_intervals:
                    try:
                        intervals[key] = float(raw_intervals[key])
                    except (TypeError, ValueError):
                        pass
        return cls(
            service_env=ServiceEnvConfig(
                fused_core=se.get("fused_core", se.get("fused_eew_api", {})) or {},
            ),
            eew_port=int(data.get("eew_port", DEFAULT_EEW_PORT)),
            list_port=int(data.get("list_port", DEFAULT_LIST_PORT)),
            custom_js_path=(data.get("custom_js_path") or "").strip(),
            auto_start_on_launch=bool(data.get("auto_start_on_launch", False)),
            start_delay_aggregate_sec=float(data.get("start_delay_aggregate_sec", 0)),
            start_delay_after_aggregate_sec=float(data.get("start_delay_after_aggregate_sec", 2.5)),
            custom_data_source_url=(data.get("CUSTOM_DATA_SOURCE_URL") or "").strip(),
            http_poll_intervals=intervals,
        )


class ConfigStore:
    _instance: Optional["ConfigStore"] = None
    _lock = threading.Lock()

    def __init__(self):
        self.settings = ConsoleSettings()
        self.load()

    @classmethod
    def instance(cls) -> "ConfigStore":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def load(self) -> None:
        if not CONFIG_PATH.exists():
            return
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.settings = ConsoleSettings.from_dict(data)
            from services.common.source_switches import set_custom_data_source_url
            url = self.settings.custom_data_source_url
            if url:
                set_custom_data_source_url(url)
        except Exception:
            pass

    def save(self) -> None:
        data: Dict[str, Any] = {}
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        data.update(self.settings.to_dict())
        tmp = CONFIG_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        shutil.move(str(tmp), str(CONFIG_PATH))
        from services.common.source_switches import set_custom_data_source_url
        set_custom_data_source_url(self.settings.custom_data_source_url)

    def get_service_config(self, key: str) -> Dict[str, str]:
        return getattr(self.settings.service_env, key, {})

    def set_service_config(self, key: str, values: Dict[str, str]) -> None:
        setattr(self.settings.service_env, key, values)
        self.save()
