from __future__ import annotations

from datetime import datetime, timedelta

from services.fused.list import state
from services.fused.list.config import Config
from services.fused.list.state import logger


class Utils:
    """工具函数类：包含所有通用工具函数"""

    @staticmethod
    def load_location_fix_data():
        try:
            from services.common.regions import get_fe_fix_regions

            regions = get_fe_fix_regions()
            if regions:
                state.location_regions = regions
                logger.info(
                    f"已加载 {len(state.location_regions)} 个地名修正区域规则 (data/fe_fix_region_data.json)"
                )
                return
            logger.warning("未找到地名修正文件 data/fe_fix_region_data.json，将回退到API翻译。")
        except Exception as e:
            state.location_regions = None
            logger.error(f"加载地名修正文件失败: {e}")

    @staticmethod
    def get_fixed_location(lat, lon):
        if state.location_regions is None:
            return None

        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            return None

        best_name = None
        best_area = None

        for region in state.location_regions:
            try:
                lat_min = region.get("lat_min", -90)
                lat_max = region.get("lat_max", 90)
                lon_min = region.get("lon_min", -180)
                lon_max = region.get("lon_max", 180)

                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    area = (lat_max - lat_min) * (lon_max - lon_min)
                    if best_area is None or area < best_area:
                        best_area = area
                        best_name = region.get("name", "")
            except Exception:
                continue

        return best_name or None

    @staticmethod
    def get_intensity_string(scale_value):
        return {10: "1", 20: "2", 30: "3", 40: "4", 45: "5-", 50: "5+", 55: "6-", 60: "6+", 70: "7"}.get(
            scale_value, str(scale_value)
        )

    @staticmethod
    def parse_time(time_str):
        if not time_str:
            return None

        formats = ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]

        for fmt in formats:
            try:
                return datetime.strptime(str(time_str), fmt)
            except (ValueError, TypeError):
                continue

        try:
            if str(time_str).endswith("Z"):
                time_str = str(time_str)[:-1] + "+00:00"
            return datetime.fromisoformat(time_str)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def get_event_key(event):
        try:
            if "EVENT_ID" in event and event["EVENT_ID"]:
                return (event["SOURCE"], event["EVENT_ID"])

            event_time = Utils.parse_time(event["O_TIME"])
            if not event_time:
                return None
            return (
                event_time.replace(second=0, microsecond=0),
                round(float(event["EPI_LAT"]), 1),
                round(float(event["EPI_LON"]), 1),
            )
        except (ValueError, KeyError, TypeError):
            return None

    @staticmethod
    def check_circuit_breaker(source):
        if source not in state.error_stats:
            return False
        stats = state.error_stats[source]
        if stats["backoff_until"] and datetime.now() < stats["backoff_until"]:
            return True
        return False

    @staticmethod
    def handle_fetch_error(source, error):
        if source not in state.error_stats:
            return
        stats = state.error_stats[source]
        stats["fetch_errors"] += 1
        stats["last_error"] = str(error)
        stats["consecutive_failures"] += 1

        backoff_delay = min(
            Config.BACKOFF_BASE_DELAY * (2 ** (stats["consecutive_failures"] - 1)),
            Config.BACKOFF_MAX_DELAY,
        )
        stats["backoff_until"] = datetime.now() + timedelta(seconds=backoff_delay)
        logger.warning(
            f"{source}: 获取失败 (连续 {stats['consecutive_failures']} 次)，将退避 {backoff_delay} 秒"
        )

    @staticmethod
    def reset_circuit_breaker(source):
        if source not in state.error_stats:
            return
        stats = state.error_stats[source]
        if stats["consecutive_failures"] > 0:
            stats["consecutive_failures"] = 0
            stats["backoff_until"] = None
