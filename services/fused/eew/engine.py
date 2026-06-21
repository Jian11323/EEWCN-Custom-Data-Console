"""EEW 兼容门面：自子模块 re-export。"""

from __future__ import annotations

import warnings

warnings.filterwarnings("ignore", message=".*InsecureRequestWarning.*")

from services.fused.common.ws_client import FanStudioWebSocketApp
from services.fused.eew.cache import CacheManager
from services.fused.eew.config import Config
from services.fused.eew.distributor import EventDistributor
from services.fused.eew.logging_mgr import LogManager
from services.fused.eew.service import EEWService
from services.fused.eew.server.client_ip import ClientIPManager
from services.fused.eew.server.ws_server import WebSocketServerManager
from services.fused.eew.translation import TranslationService
from services.fused.eew.upstream.ws_client_mgr import WebSocketClientManager
from services.fused.eew.utils import Utils
from services.fused.eew.sources.base import DataSource
from services.fused.eew.sources.custom import CustomSource, EarlyEstSource
from services.fused.eew.sources.fanstudio_base import FanStudioSource
from services.fused.eew.sources.cea import CEAPRSource, CEASource
from services.fused.eew.sources.cwa import CWAFanStudioSourceV2
from services.fused.eew.sources.sa import SASource
from services.fused.eew.sources.kma import KMASource
from services.fused.eew.sources.jma import JMAFanStudioSource

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False

__all__ = [
    "APSCHEDULER_AVAILABLE",
    "CacheManager",
    "CEAPRSource",
    "CEASource",
    "ClientIPManager",
    "Config",
    "CustomSource",
    "CWAFanStudioSourceV2",
    "DataSource",
    "EarlyEstSource",
    "EEWService",
    "EventDistributor",
    "FanStudioSource",
    "FanStudioWebSocketApp",
    "JMAFanStudioSource",
    "KMASource",
    "LogManager",
    "SASource",
    "TranslationService",
    "Utils",
    "WebSocketClientManager",
    "WebSocketServerManager",
]
