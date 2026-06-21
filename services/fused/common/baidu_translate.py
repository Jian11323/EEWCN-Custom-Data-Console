"""百度翻译 API 客户端。"""

from __future__ import annotations

import hashlib
import logging
import random
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

API_URL = "http://api.fanyi.baidu.com/api/trans/vip/translate"
_warned_missing_credentials = False


def is_translation_configured(app_id: str, secret_key: str) -> bool:
    """Return True when Baidu translate credentials are present."""
    return bool(app_id and secret_key)


def translate_text(
    text: str,
    *,
    app_id: str,
    secret_key: str,
    from_lang: str = "auto",
    to_lang: str = "zh",
    timeout: float = 2.0,
) -> Optional[str]:
    """调用百度翻译 API，成功返回译文，失败返回 None。"""
    global _warned_missing_credentials
    if not text:
        return None
    if not is_translation_configured(app_id, secret_key):
        if not _warned_missing_credentials:
            logger.info("百度翻译未配置（BAIDU_APP_ID / BAIDU_SECRET_KEY），将返回原文")
            _warned_missing_credentials = True
        return None
    try:
        salt = str(random.randint(32768, 65536))
        sign_str = app_id + text + salt + secret_key
        sign = hashlib.md5(sign_str.encode("utf-8")).hexdigest()
        params: Dict[str, Any] = {
            "q": text,
            "from": from_lang,
            "to": to_lang,
            "appid": app_id,
            "salt": salt,
            "sign": sign,
        }
        response = requests.get(API_URL, params=params, timeout=timeout)
        response.raise_for_status()
        result = response.json()
        if "trans_result" in result and result["trans_result"]:
            return result["trans_result"][0]["dst"]
        logger.error("翻译API错误: %s", result.get("error_msg", "未知错误"))
    except requests.Timeout:
        logger.warning("翻译超时: '%s'", text)
    except Exception as e:
        logger.error("翻译异常: %s", e)
    return None
