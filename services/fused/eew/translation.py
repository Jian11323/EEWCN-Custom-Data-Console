from __future__ import annotations

import json
import logging
import os
import re
import threading
from typing import Any, Dict, Optional

from services.fused.common.baidu_translate import is_translation_configured, translate_text
from services.fused.eew.config import Config

class TranslationService:
    """翻译服务"""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.cache: Dict[str, str] = {}
        self.cache_file = os.path.join(config.TRANSLATION_CACHE_DIR, "translation_cache_eew.json")
        self.lock = threading.Lock()
        self._load_cache()
    
    def _normalize_key(self, text: str) -> str:
        """规范化缓存键（去除多余空格、统一处理）"""
        if not text:
            return text
        # 去除首尾空格，将多个连续空格替换为单个空格
        normalized = ' '.join(text.split())
        return normalized
    
    def _load_cache(self):
        """加载翻译缓存（自动去重）"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    raw_cache = json.load(f)
                
                # 规范化并去重：使用规范化后的键，如果有重复则保留最后一个
                normalized_cache = {}
                duplicates_removed = 0
                
                for key, value in raw_cache.items():
                    normalized_key = self._normalize_key(key)
                    if normalized_key in normalized_cache:
                        duplicates_removed += 1
                    normalized_cache[normalized_key] = value
                
                self.cache = normalized_cache
                
                if duplicates_removed > 0:
                    self.logger.info(f"加载翻译缓存时发现并移除了 {duplicates_removed} 个重复项")
                    # 立即保存去重后的缓存
                    self._async_save_cache()
                
                self.logger.debug(f"已加载 {len(self.cache)} 条翻译缓存")
            except Exception as e:
                self.logger.error(f"加载翻译缓存失败: {e}")
        else:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
    
    def save_cache(self):
        """保存翻译缓存（自动去重）"""
        try:
            with self.lock:
                # 确保缓存键都已规范化（去重处理）
                normalized_cache = {}
                for key, value in self.cache.items():
                    normalized_key = self._normalize_key(key)
                    normalized_cache[normalized_key] = value
                
                # 更新缓存为去重后的版本
                self.cache = normalized_cache
                
                # 保存到文件
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存翻译缓存失败: {e}")
    
    def _async_save_cache(self):
        """异步保存翻译缓存到文件（无阻塞，自动去重）"""
        def _save():
            try:
                with self.lock:
                    # 确保缓存键都已规范化（去重处理）
                    normalized_cache = {}
                    for key, value in self.cache.items():
                        normalized_key = self._normalize_key(key)
                        normalized_cache[normalized_key] = value
                    
                    # 更新缓存为去重后的版本
                    self.cache = normalized_cache
                    cache_copy = self.cache.copy()  # 复制缓存，减少锁持有时间
                
                # 在锁外进行文件写入
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_copy, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error(f"异步保存翻译缓存失败: {e}")
        
        threading.Thread(target=_save, daemon=True, name="SaveTranslationCache").start()
    
    def translate(self, text: str, force_lang: Optional[str] = None, quick_mode: bool = False, skip_cache: bool = False) -> str:
        """翻译文本（支持日语、韩语、英语到中文）"""
        if not text or text == '未知地点':
            return text
        
        # 规范化缓存键（去除多余空格，确保不重复）
        normalized_text = self._normalize_key(text)
        
        # 跳过缓存模式：直接调用API，不检查缓存（加快翻译速度，降低推送延迟）
        if not skip_cache:
            # 检查缓存（使用规范化后的键）
            if normalized_text in self.cache:
                return self.cache[normalized_text]
            
            # 快速模式：缓存未命中直接返回
            if quick_mode:
                return text
        
        # 检测语言
        has_korean = bool(re.search(r'[가-힣]', text))
        has_japanese = bool(re.search(r'[ひらがなカタカナ一-龯]', text))
        has_english = bool(re.search(r'[a-zA-Z]', text))
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', text))
        
        if has_chinese and not (has_korean or has_japanese or has_english):
            return text
        
        if not is_translation_configured(self.config.BAIDU_APP_ID, self.config.BAIDU_SECRET_KEY):
            return text

        if force_lang:
            from_lang = force_lang
        elif has_korean:
            from_lang = 'kor'
        elif has_japanese:
            from_lang = 'jp'
        elif has_english:
            from_lang = 'auto'
        else:
            return text
        
        # 调用百度翻译API（使用更短的超时时间以加快响应）
        translated = translate_text(
            text,
            app_id=self.config.BAIDU_APP_ID,
            secret_key=self.config.BAIDU_SECRET_KEY,
            from_lang=from_lang,
            timeout=2.0,
        )
        if translated:
            with self.lock:
                self.cache[normalized_text] = translated
            self._async_save_cache()
            self.logger.debug(f"翻译成功: '{text}' -> '{translated}'")
            return translated
        return text
    
    def translate_async(self, text: str, force_lang: Optional[str] = None):
        """异步翻译（后台任务）"""
        def _translate():
            try:
                normalized_text = self._normalize_key(text)
                if normalized_text not in self.cache:
                    self.translate(text, force_lang=force_lang, quick_mode=False)
            except Exception as e:
                self.logger.debug(f"异步翻译失败: {text}, {e}")
        
        threading.Thread(target=_translate, daemon=True, name="AsyncTranslate").start()

