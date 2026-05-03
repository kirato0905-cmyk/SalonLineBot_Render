import logging
import time
from typing import Dict, Any, Optional

from api.google_sheets_logger import get_sheets_logger


class UserConsentManager:
    """Usersシート同意確認の軽量ラッパー。

    - 同意状態を短時間メモリキャッシュ
    - Sheets一時障害時は期限切れキャッシュをstale fallbackとして利用
    - 実データは GoogleSheetsLogger に委譲
    """

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._ttl_seconds = 30

    def _get_cached_item(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self._cache.get(user_id)

    def _get_cached(self, user_id: str):
        item = self._get_cached_item(user_id)
        if not item:
            return None
        if time.time() - item["fetched_at"] > self._ttl_seconds:
            return None
        return item["consented"]

    def _get_stale_cached(self, user_id: str):
        item = self._get_cached_item(user_id)
        if not item:
            return None
        return item["consented"]

    def _set_cached(self, user_id: str, consented: bool):
        self._cache[user_id] = {
            "fetched_at": time.time(),
            "consented": bool(consented),
        }

    def invalidate_user(self, user_id: str):
        self._cache.pop(user_id, None)

    def has_user_consented(self, user_id: str) -> bool:
        try:
            cached = self._get_cached(user_id)
            if cached is not None:
                return cached
            consented = get_sheets_logger().has_user_consented(user_id)
            self._set_cached(user_id, consented)
            return consented
        except Exception as e:
            stale = self._get_stale_cached(user_id)
            if stale is not None:
                logging.warning(
                    f"Consent check failed. Using stale cache for {user_id}: {e}",
                    exc_info=True,
                )
                return bool(stale)
            logging.error(f"Failed to check user consent for {user_id}: {e}", exc_info=True)
            return False

    def mark_user_consented(self, user_id: str) -> bool:
        try:
            success = get_sheets_logger().mark_user_consented(user_id)
            if success:
                self._set_cached(user_id, True)
            return success
        except Exception as e:
            logging.error(f"Failed to mark user consented for {user_id}: {e}", exc_info=True)
            return False

    def revoke_user_consent(self, user_id: str) -> bool:
        try:
            success = get_sheets_logger().revoke_user_consent(user_id)
            if success:
                self._set_cached(user_id, False)
            return success
        except Exception as e:
            logging.error(f"Failed to revoke user consent for {user_id}: {e}", exc_info=True)
            return False


user_consent_manager = UserConsentManager()
