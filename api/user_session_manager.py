"""User session manager backed by Google Sheets "Users" worksheet."""
import logging
from api.google_sheets_logger import get_sheets_logger


class UserSessionManager:
    def __init__(self):
        self.sheets_logger = get_sheets_logger()

    def is_new_user(self, user_id: str) -> bool:
        try:
            return self.sheets_logger.is_new_user(user_id)
        except Exception as e:
            logging.error("is_new_user failed: %s", e)
            return False

    def mark_user_seen(self, user_id: str):
        """No-op by default after Last Seen removal.

        Future implementation may update at most once per day when enabled.
        """
        return None

    def get_user_count(self) -> int:
        try:
            records = self.sheets_logger.users_worksheet.get_all_records() if self.sheets_logger.users_worksheet else []
            return sum(1 for r in records if r.get("User ID") or r.get("ユーザーID"))
        except Exception as e:
            logging.error("get_user_count failed: %s", e)
            return 0

    def cleanup_old_sessions(self, days_old: int = 30):
        return None


user_session_manager = UserSessionManager()

