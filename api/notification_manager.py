"""
Unified notification manager for salon booking system.

Operator notifications are intentionally unified to Slack only.
LINE operator notifications are no longer initialized or sent from this manager.
Reminder delivery result notifications are disabled by specification.
"""
import logging
from typing import Dict, Any, List
from dotenv import load_dotenv


class NotificationManager:
    def __init__(self):
        load_dotenv()
        self.notification_method = "slack"
        self.slack_notifier = None

        try:
            from api.slack_notifier import slack_notifier
            self.slack_notifier = slack_notifier
            print("Slack notifications enabled")
        except Exception as e:
            logging.error(f"Failed to initialize Slack notifier: {e}")

    def is_enabled(self) -> bool:
        """Check if Slack notification is enabled."""
        return bool(self.slack_notifier and self.slack_notifier.enabled)

    def get_status(self) -> Dict[str, Any]:
        """Get notification status."""
        return {
            "method": "slack",
            "slack_enabled": bool(self.slack_notifier and self.slack_notifier.enabled),
            "line_enabled": False,
            "overall_enabled": self.is_enabled(),
        }

    def notify_user_login(self, user_id: str, display_name: str) -> bool:
        """User login operator notification is disabled by specification."""
        logging.info("User login operator notification is disabled.")
        return True

    def notify_reservation_confirmation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send reservation confirmation notification to Slack."""
        if self.slack_notifier and self.slack_notifier.enabled:
            try:
                return bool(self.slack_notifier.notify_reservation_confirmation(reservation_data, client_name))
            except Exception as e:
                logging.error(f"Slack reservation confirmation notification failed: {e}")
        return False

    def notify_reservation_modification(
        self,
        old_reservation: Dict[str, Any],
        new_reservation: Dict[str, Any],
        client_name: str,
    ) -> bool:
        """Send reservation modification notification to Slack."""
        if self.slack_notifier and self.slack_notifier.enabled:
            try:
                return bool(self.slack_notifier.notify_reservation_modification(old_reservation, new_reservation, client_name))
            except Exception as e:
                logging.error(f"Slack reservation modification notification failed: {e}")
        return False

    def notify_reservation_cancellation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send reservation cancellation notification to Slack."""
        if self.slack_notifier and self.slack_notifier.enabled:
            try:
                return bool(self.slack_notifier.notify_reservation_cancellation(reservation_data, client_name))
            except Exception as e:
                logging.error(f"Slack reservation cancellation notification failed: {e}")
        return False

    def notify_reminder_status(
        self,
        success_count: int,
        total_count: int,
        failed_reservations: List[Dict[str, Any]],
    ) -> bool:
        """Reminder delivery result operator notification is disabled by specification."""
        logging.info(
            "Reminder status operator notification is disabled. "
            f"success={success_count}, total={total_count}, failed={len(failed_reservations)}"
        )
        return True


# Global instance
notification_manager = NotificationManager()


# Convenience functions that use the unified Slack-only manager
def send_user_login_notification(user_id: str, display_name: str) -> bool:
    """Compatibility wrapper. Login operator notification is disabled."""
    return notification_manager.notify_user_login(user_id, display_name)


def send_reservation_confirmation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Send reservation confirmation notification using Slack."""
    return notification_manager.notify_reservation_confirmation(reservation_data, client_name)


def send_reservation_modification_notification(
    old_reservation: Dict[str, Any],
    new_reservation: Dict[str, Any],
    client_name: str,
) -> bool:
    """Send reservation modification notification using Slack."""
    return notification_manager.notify_reservation_modification(old_reservation, new_reservation, client_name)


def send_reservation_cancellation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Send reservation cancellation notification using Slack."""
    return notification_manager.notify_reservation_cancellation(reservation_data, client_name)


def send_reminder_status_notification(
    success_count: int,
    total_count: int,
    failed_reservations: List[Dict[str, Any]],
) -> bool:
    """Compatibility wrapper. Reminder operator notification is disabled."""
    return notification_manager.notify_reminder_status(success_count, total_count, failed_reservations)


if __name__ == "__main__":
    manager = NotificationManager()
    print("Notification Manager Status:")
    for key, value in manager.get_status().items():
        print(f"  {key}: {value}")

    if manager.is_enabled():
        print("\n✅ Slack notifications are enabled")
    else:
        print("\n❌ Slack notifications are disabled")
        print("Configure SLACK_WEBHOOK_URL environment variable.")

