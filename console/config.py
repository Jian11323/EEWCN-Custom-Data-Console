"""控制台配置持久化"""

from __future__ import annotations

import json
import os
import shutil
import threading
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Optional

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
    mgmt_host: str = "127.0.0.1"
    mgmt_port: int = 2050
    auto_start_on_launch: bool = False
    start_delay_aggregate_sec: float = 0.0
    start_delay_after_aggregate_sec: float = 2.5
    custom_data_source_url: str = ""

    def to_dict(self) -> dict:
        return {
            "service_env": asdict(self.service_env),
            "mgmt_host": self.mgmt_host,
            "mgmt_port": self.mgmt_port,
            "auto_start_on_launch": self.auto_start_on_launch,
            "start_delay_aggregate_sec": self.start_delay_aggregate_sec,
            "start_delay_after_aggregate_sec": self.start_delay_after_aggregate_sec,
            "CUSTOM_DATA_SOURCE_URL": self.custom_data_source_url,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ConsoleSettings":
        se = data.get("service_env", {})
        mgmt_host = data.get("mgmt_host") or data.get("eew_mgmt_host", "127.0.0.1")
        mgmt_port = int(
            data.get("mgmt_port", data.get("eew_mgmt_port", data.get("list_mgmt_port", 2050)))
        )
        return cls(
            service_env=ServiceEnvConfig(
                fused_core=se.get("fused_core", se.get("fused_eew_api", {})) or {},
            ),
            mgmt_host=mgmt_host,
            mgmt_port=mgmt_port,
            auto_start_on_launch=bool(data.get("auto_start_on_launch", False)),
            start_delay_aggregate_sec=float(data.get("start_delay_aggregate_sec", 0)),
            start_delay_after_aggregate_sec=float(data.get("start_delay_after_aggregate_sec", 2.5)),
            custom_data_source_url=(data.get("CUSTOM_DATA_SOURCE_URL") or "").strip(),
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
