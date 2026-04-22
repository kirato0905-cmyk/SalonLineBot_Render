"""
User session manager backed by Google Sheets "Users" worksheet
"""
import logging
from api.google_sheets_logger import GoogleSheetsLogger


class UserSessionManager:
    def __init__(self):
        self.sheets_logger = GoogleSheetsLogger()

    def is_new_user(self, user_id: str) -> bool:
        """Check if this is a new user (first time interacting with bot)"""
        try:
            return self.sheets_logger.is_new_user(user_id)
        except Exception as e:
            logging.error(f"is_new_user failed for {user_id}: {e}")
            # Assume new user on failure to avoid duplicate notifications
            return True

    def mark_user_seen(self, user_id: str):
        """No-op for compatibility after removing Last Seen column"""
        try:
            # google_sheets_logger 側も no-op だが、ここでも明示的に何もしない
            return
        except Exception as e:
            logging.error(f"mark_user_seen failed for {user_id}: {e}")

    def get_user_count(self) -> int:
        """Get total number of unique users who have interacted with the bot"""
        try:
            records = self.sheets_logger.users_worksheet.get_all_records() if self.sheets_logger.users_worksheet else []
            return sum(1 for r in records if r.get("User ID"))
        except Exception as e:
            logging.error(f"get_user_count failed: {e}")
            return 0

    def cleanup_old_sessions(self, days_old: int = 30):
        """No-op for sheet-backed sessions (kept for compatibility)"""
        pass


# Global instance
user_session_manager = UserSessionManager()
