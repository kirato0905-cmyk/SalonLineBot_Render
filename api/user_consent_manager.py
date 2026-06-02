import logging
import time
from typing import Dict, Any, Optional

from api.repositories.database_customer_repository import DatabaseCustomerRepository
from api.google_sheets_logger import get_sheets_logger


def _mask_user_id(user_id: str) -> str:
    if not user_id:
        return ""
    user_id = str(user_id)
    return user_id[:6] + "***" if len(user_id) > 6 else "***"


class UserConsentManager:
    """顧客DBを正本として利用案内の同意状態を管理する。

    方針:
    - 同意判定の参照元は Supabase/PostgreSQL の customers テーブル
    - 同意更新は最初にDBへ保存する
    - Google Sheets のユーザー一覧への書き込みは店舗確認用の補助同期
    - DB障害時は安全側に倒し、未同意として扱う
    """

    def __init__(self):
        self._customer_repo = DatabaseCustomerRepository()
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
        self._cache[user_id] = {
            "fetched_at": time.time(),
            "consented": bool(consented),
        }

    def invalidate_user(self, user_id: str):
        self._cache.pop(user_id, None)

    def _sync_sheet_consent(self, user_id: str, consented: bool) -> None:
        """店舗閲覧用シートへ補助同期する。失敗してもDBでの同意保存結果は覆さない。"""
        try:
            get_sheets_logger().set_user_consent(user_id, consented)
        except Exception as e:
            logging.warning(
                "Consent saved in DB, but sheet mirror failed for %s: %s",
                _mask_user_id(user_id),
                e,
                exc_info=True,
            )

    def has_user_consented(self, user_id: str) -> bool:
        masked = _mask_user_id(user_id)
        try:
            cached = self._get_cached(user_id)
            if cached is not None:
                return bool(cached)

            customer = self._customer_repo.get_customer_by_line_user_id(user_id)
            consented = bool(customer and customer.get("consented"))
            self._set_cached(user_id, consented)
            return consented
        except Exception as e:
            stale = self._get_stale_cached_if_allowed(user_id)
            if stale is not None:
                logging.warning(
                    "DB consent check failed. Using bounded stale cache for %s: %s",
                    masked,
                    e,
                    exc_info=True,
                )
                return bool(stale)
            logging.error(
                "Failed to check DB consent for %s. Failing closed: %s",
                masked,
                e,
                exc_info=True,
            )
            return False

    def mark_user_consented(self, user_id: str) -> bool:
        masked = _mask_user_id(user_id)
        try:
            success = self._customer_repo.set_consent(user_id, True)
            if not success:
                logging.error("Failed to save consent in DB for %s", masked)
                return False

            self._set_cached(user_id, True)
            self._sync_sheet_consent(user_id, True)
            return True
        except Exception as e:
            logging.error(
                "Failed to mark DB consent for %s: %s",
                masked,
                e,
                exc_info=True,
            )
            return False

    def revoke_user_consent(self, user_id: str) -> bool:
        masked = _mask_user_id(user_id)
        self._set_cached(user_id, False)
        try:
            success = self._customer_repo.set_consent(user_id, False)
            if not success:
                logging.error("Failed to revoke consent in DB for %s", masked)
                return False

            self._set_cached(user_id, False)
            self._sync_sheet_consent(user_id, False)
            return True
        except Exception as e:
            logging.error(
                "Failed to revoke DB consent for %s: %s",
                masked,
                e,
                exc_info=True,
            )
            return False


user_consent_manager = UserConsentManager()

