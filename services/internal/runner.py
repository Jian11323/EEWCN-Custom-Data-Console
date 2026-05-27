"""启动/停止全部内部数据源采集线程。"""

from __future__ import annotations

import logging
from typing import List

from services.common.source_status import get_source_status_registry
from services.common.source_switches import (
    get_custom_data_source_url,
    is_eew_enabled,
    is_list_enabled,
)
from services.internal import bmkg, geonet, earlyest, custom

logger = logging.getLogger("internal.runner")

_threads: List = []


def _register_fan_wolfx_placeholders() -> None:
    reg = get_source_status_registry()
    reg.register("fanstudio", "Fan Studio /all", "fan")
    reg.register("wolfx", "Wolfx all_eew", "upstream")
    reg.register("ingv", "INGV 速报", "list")
    reg.register("p2p_jma", "P2P JMA", "list")


def start_internal_fetchers() -> None:
    global _threads
    _register_fan_wolfx_placeholders()
    _threads = []
    starters = []
    if is_eew_enabled("CUSTOM") and get_custom_data_source_url():
        starters.append(("CUSTOM", custom.start))
    if is_list_enabled("bmkg"):
        starters.append(("BMKG", bmkg.start))
    if is_list_enabled("geonet"):
        starters.append(("GeoNet", geonet.start))
    if is_eew_enabled("EARLY_EST"):
        starters.append(("Early-est", earlyest.start))
    for name, fn in starters:
        try:
            _threads.append(fn())
            logger.info("内部采集已启动: %s", name)
        except Exception as e:
            logger.error("内部采集启动失败 %s: %s", name, e)
    logger.info("内部数据源采集已启动 (%d 线程)", len(_threads))


def stop_internal_fetchers() -> None:
    custom.stop()
    bmkg.stop()
    geonet.stop()
    earlyest.stop()
