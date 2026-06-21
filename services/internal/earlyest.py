"""Early-est 预警轮询 → eew channel。"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
import urllib3
from bs4 import BeautifulSoup

from services.common.bus import get_event_bus
from services.common.http_poll_intervals import get_poll_interval
from services.common.source_status import get_source_status_registry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("internal.earlyest")

SOURCE_ID = "early-est"
EARLYEST_URL = "http://early-est.rm.ingv.it/hypomessage.html"
EARLYEST_MAGNITUDE_BORDER = 1.0
EARLYEST_WINDOW = 60
MAX_LEN = 10
HTTP_TIMEOUT = 15

EARLYEST_PROCESS_POLY = [
    [54.145, 121.31], [54.145, 151.25], [15.80, 151.25],
    [15.8, 96.33], [26.27, 96.33], [26.27, 72.5], [51.145, 72.5],
]

_stop = threading.Event()
_earlyest_processed_ids: List[str] = []
_earlyest_events_history: Dict[str, dict] = {}
_latest_event: Optional[dict] = None


def is_in_process_area(lat: float, lon: float) -> bool:
    n = len(EARLYEST_PROCESS_POLY)
    is_inside = False
    j = n - 1
    for i in range(n):
        lat_i, lon_i = EARLYEST_PROCESS_POLY[i]
        lat_j, lon_j = EARLYEST_PROCESS_POLY[j]
        if ((lon_i > lon) != (lon_j > lon)) and \
           (lat < (lat_j - lat_i) * (lon - lon_i) / (lon_j - lon_i + 1e-10) + lat_i):
            is_inside = not is_inside
        j = i
    return is_inside


def round_mag(v) -> float:
    try:
        return round(float(v) + 1e-9, 1)
    except Exception:
        return 0.0


def _choose_mag(tds) -> Optional[float]:
    def valid(x):
        try:
            return float(x) != -9
        except Exception:
            return False

    try:
        vals = [tds[27].text.strip(), tds[23].text.strip(), tds[21].text.strip()]
        for v in vals:
            if valid(v):
                return float(v)
    except Exception:
        pass
    return None


def _poll_once() -> None:
    global _earlyest_processed_ids, _earlyest_events_history, _latest_event
    reg = get_source_status_registry()

    try:
        r = requests.get(EARLYEST_URL, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        region_col_index = 30
        try:
            header_index = None
            for tr in soup.select("tr"):
                cells = tr.find_all(["th", "td"])
                if not cells:
                    continue
                texts = [c.get_text(strip=True).lower() for c in cells]
                if any("region" in t for t in texts):
                    for idx, t in enumerate(texts):
                        if "region" in t:
                            header_index = idx
                            break
                    if header_index is not None:
                        break
            if header_index is not None:
                region_col_index = header_index
        except Exception:
            region_col_index = 30

        rows = soup.select("tr[align=right]")
        this_turn_events: List[dict] = []
        current_web_ids = set()
        now_utc = datetime.now(timezone.utc)

        for row in rows:
            link = row.find("a", target=True)
            if not link:
                continue

            eid = f"EARLY_{link.get('target', 'UNK')}"
            tds = row.find_all("td")
            min_cols = max(14, region_col_index + 1)
            if len(tds) < min_cols:
                continue

            try:
                dt_shock_utc = datetime.strptime(
                    tds[9].text.strip(), "%Y.%m.%d-%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if abs((now_utc - dt_shock_utc).total_seconds()) > (EARLYEST_WINDOW * 60):
                    continue

                lat = float(tds[10].text.strip())
                lon = float(tds[11].text.strip())
                mag = _choose_mag(tds)
                if mag is None:
                    continue
                depth = float(tds[13].text.strip())
                shock_time_str = dt_shock_utc.strftime("%Y/%m/%d %H:%M:%S") + "Z"
                loc_seq = tds[1].text.strip()
                region = ""
                if len(tds) > region_col_index:
                    region = tds[region_col_index].get_text(strip=True)
                if (not region) and region_col_index == 30 and len(tds) >= 1:
                    region = tds[-1].get_text(strip=True)
            except Exception:
                continue

            if is_in_process_area(lat, lon) or mag >= EARLYEST_MAGNITUDE_BORDER:
                if eid not in _earlyest_processed_ids:
                    if len(_earlyest_processed_ids) >= MAX_LEN:
                        _earlyest_processed_ids.pop(0)
                    _earlyest_processed_ids.append(eid)

            if eid in _earlyest_processed_ids:
                current_web_ids.add(eid)
                current_sig = {"lat": lat, "lon": lon, "dep": depth, "mag": mag, "time": shock_time_str}

                if eid not in _earlyest_events_history:
                    report_num = 1
                    report_time = now_utc.strftime("%Y/%m/%d %H:%M:%S") + "Z"
                else:
                    old = _earlyest_events_history[eid]
                    if old.get("signature") != current_sig:
                        report_num = old.get("reportNum", 0) + 1
                        report_time = now_utc.strftime("%Y/%m/%d %H:%M:%S") + "Z"
                    else:
                        report_num = old.get("reportNum", 1)
                        report_time = old.get("reportTime", "")

                _earlyest_events_history[eid] = {
                    "signature": current_sig,
                    "reportNum": report_num,
                    "reportTime": report_time,
                }
                report_num_display = int(loc_seq) if str(loc_seq).isdigit() else 0
                event_obj = {
                    "identifier": eid,
                    "otime": shock_time_str,
                    "reportTime": report_time,
                    "lat": lat,
                    "lon": lon,
                    "region": region,
                    "mag": round_mag(mag),
                    "depth": depth,
                    "locSeq": report_num_display,
                    "source": "Early-est",
                }
                this_turn_events.append(event_obj)

        if this_turn_events:
            this_turn_events.sort(key=lambda e: e.get("otime", ""), reverse=True)
            new_events = this_turn_events[:MAX_LEN]
        else:
            new_events = []

        reg.record_ok(SOURCE_ID)

        first_event = new_events[0] if new_events else None
        if first_event is not None:
            has_update = False
            if _latest_event is None:
                has_update = True
            else:
                has_update = (
                    _latest_event.get("identifier") != first_event.get("identifier")
                    or _latest_event.get("locSeq") != first_event.get("locSeq")
                    or _latest_event.get("otime") != first_event.get("otime")
                )
            if has_update:
                _latest_event = first_event
                get_event_bus().publish(
                    "eew",
                    SOURCE_ID,
                    {"Data": first_event, "type": "update"},
                )

        for old_id in list(_earlyest_events_history.keys()):
            if old_id not in current_web_ids:
                del _earlyest_events_history[old_id]
                if old_id in _earlyest_processed_ids:
                    _earlyest_processed_ids.remove(old_id)
    except Exception as e:
        reg.record_error(SOURCE_ID, str(e))
        logger.warning("Early-est 轮询失败: %s", e)


def _loop() -> None:
    reg = get_source_status_registry()
    reg.register(SOURCE_ID, "Early-est 预警", "eew")
    reg.set_connected(SOURCE_ID, True)
    while not _stop.is_set():
        try:
            _poll_once()
        except Exception as e:
            logger.exception("Early-est 轮询异常")
            reg.record_error(SOURCE_ID, str(e))
        _stop.wait(get_poll_interval("early_est"))


def start() -> threading.Thread:
    _stop.clear()
    t = threading.Thread(target=_loop, name="Internal-EarlyEst", daemon=True)
    t.start()
    return t


def stop() -> None:
    _stop.set()
