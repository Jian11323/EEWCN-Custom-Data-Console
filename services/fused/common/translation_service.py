"""统一翻译 facade（EEW / List 共用底层 baidu_translate）。"""

from __future__ import annotations

import os
from typing import Optional

from services.fused.common.baidu_translate import is_translation_configured, translate_text


class TranslationService:
    """轻量翻译服务：从环境变量读取凭证，未配置时返回原文。"""

    def __init__(self, app_id: str = "", secret_key: str = ""):
        self.app_id = app_id or os.environ.get("BAIDU_APP_ID", "")
        self.secret_key = secret_key or os.environ.get("BAIDU_SECRET_KEY", "")

    @property
    def enabled(self) -> bool:
        return is_translation_configured(self.app_id, self.secret_key)

    def translate(
        self,
        text: str,
        *,
        from_lang: str = "auto",
        to_lang: str = "zh",
        timeout: float = 2.0,
    ) -> str:
        if not text:
            return text
        if not self.enabled:
            return text
        result = translate_text(
            text,
            app_id=self.app_id,
            secret_key=self.secret_key,
            from_lang=from_lang,
            to_lang=to_lang,
            timeout=timeout,
        )
        return result if result else text
