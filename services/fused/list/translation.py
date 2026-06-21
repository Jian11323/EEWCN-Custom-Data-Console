from __future__ import annotations

import hashlib
import re
import time

import requests

from services.fused.common.baidu_translate import translate_text
from services.fused.list.config import BAIDU_TRANSLATE_CONFIG
from services.fused.list.state import cache_lock, translation_cache

class TranslationService:
    """翻译服务"""
    
    @staticmethod
    def translate_location(location, lat=None, lon=None, source=None):
        """翻译地名"""
        # JMA、CWA 和 HKO 直接返回原始地名
        if source in ['JMA', 'CWA', 'HKO']:
            return location
        
        # 如果有经纬度，先尝试 FE fix
        if lat is not None and lon is not None:
            try:
                fe_location = Utils.get_fixed_location(lat, lon)
                if fe_location:
                    return fe_location
            except Exception:
                pass
        
        # 检查缓存
        with cache_lock:
            if location in translation_cache:
                return translation_cache[location]

        # 如果没有翻译配置，直接返回
        if not BAIDU_TRANSLATE_CONFIG['APP_ID'] or not BAIDU_TRANSLATE_CONFIG['SECRET_KEY']:
            return location

        # 如果已经是简体中文，直接返回
        if re.search(r'[\u4e00-\u9fa5]', location) and not TranslationService.contains_traditional_chinese(location):
            return location

        translated_text = translate_text(
            location,
            app_id=BAIDU_TRANSLATE_CONFIG['APP_ID'],
            secret_key=BAIDU_TRANSLATE_CONFIG['SECRET_KEY'],
            from_lang='auto',
            timeout=5.0,
        )
        if translated_text:
            with cache_lock:
                translation_cache[location] = translated_text
            return translated_text
        return location
    
    @staticmethod
    def contains_traditional_chinese(text):
        """检查是否包含繁体中文"""
        try:
            text.encode('gb2312')
            return False
        except UnicodeEncodeError:
            return True
    
    @staticmethod
    def convert_traditional_to_simplified(location):
        """将繁体中文转换为简体中文"""
        return TranslationService.translate_location(location)

