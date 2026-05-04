import logging
import time
from typing import Dict, Any, Optional

from api.google_sheets_logger import get_sheets_logger


def _mask_user_id(user_id: str) -> str:
    if not user_id:
        return ""
    user_id = str(user_id)
    return user_id[:6] + "***" if len(user_id) > 6 else "***"


class UserConsentManager:
    """Usersシート同意確認の軽量ラッパー。

    Safety policy:
    - TTL内キャッシュは使用
    - Sheets障害時の stale true 利用はデフォルト禁止
    - stale fallback を使う場合も max_stale_seconds 以内に限定
    - revoke時は即キャッシュFalse
    """

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._ttl_seconds = 30
        self._allow_stale_cache_on_error = False
        self._max_stale_seconds = 300

    def _get_cached_item(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self._cache.get(user_id)

    def _get_cached(self, user_id: str):
        item = self._get_cached_item(user_id)
        if not item:
            return None
        if time.time() - item["fetched_at"] > self._ttl_seconds:
            return None
        return item["consented"]

    def _get_stale_cached_if_allowed(self, user_id: str):
        if not self._allow_stale_cache_on_error:
            return None
        item = self._get_cached_item(user_id)
        if not item:
            return None
        age = time.time() - item["fetched_at"]
        if age > self._max_stale_seconds:
            return None
        return item["consented"]

    def _set_cached(self, user_id: str, consented: bool):
        self._cache[user_id] = {"fetched_at": time.time(), "consented": bool(consented)}

    def invalidate_user(self, user_id: str):
        self._cache.pop(user_id, None)

    def has_user_consented(self, user_id: str) -> bool:
        masked = _mask_user_id(user_id)
        try:
            cached = self._get_cached(user_id)
            if cached is not None:
                return bool(cached)
            consented = get_sheets_logger().has_user_consented(user_id)
            self._set_cached(user_id, consented)
            return bool(consented)
        except Exception as e:
            stale = self._get_stale_cached_if_allowed(user_id)
            if stale is not None:
                logging.warning("Consent check failed. Using bounded stale cache for %s: %s", masked, e, exc_info=True)
                return bool(stale)
            logging.error("Failed to check user consent for %s. Failing closed: %s", masked, e, exc_info=True)
            return False

    def mark_user_consented(self, user_id: str) -> bool:
        masked = _mask_user_id(user_id)
        try:
            success = get_sheets_logger().mark_user_consented(user_id)
            if success:
                self._set_cached(user_id, True)
            return bool(success)
        except Exception as e:
            logging.error("Failed to mark user consented for %s: %s", masked, e, exc_info=True)
            return False

    def revoke_user_consent(self, user_id: str) -> bool:
        masked = _mask_user_id(user_id)
        self._set_cached(user_id, False)
        try:
            success = get_sheets_logger().revoke_user_consent(user_id)
            if success:
                self._set_cached(user_id, False)
            return bool(success)
        except Exception as e:
            logging.error("Failed to revoke user consent for %s: %s", masked, e, exc_info=True)
            return False


user_consent_manager = UserConsentManager()
