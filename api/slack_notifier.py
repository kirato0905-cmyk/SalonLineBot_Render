"""
Slack notification service for salon booking system.

Operator notifications are unified to Slack.
Reservation notification wording follows the existing LINE operator notification format.
User-login and reminder operator notifications are disabled by specification.
"""
import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

import requests
from dotenv import load_dotenv


class SlackNotifier:
    def __init__(self):
        load_dotenv()
        self.webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        self.enabled = bool(self.webhook_url)
        self.config_data = self._load_config_data()
        self.services = self.config_data.get("services", {})

        if not self.enabled:
            logging.warning("Slack webhook URL not configured. Notifications disabled.")
        else:
            print("Slack notifications enabled")

    def _data_dir(self) -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, "data")

    def _load_json_file(self, filename: str) -> Dict[str, Any]:
        path = os.path.join(self._data_dir(), filename)
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            logging.error(f"Failed to load {filename}: {e}")
            return {}

    def _load_config_data(self) -> Dict[str, Any]:
        """Prefer config.json, fallback to services.json for older deployments."""
        config_data = self._load_json_file("config.json")
        if config_data:
            return config_data
        return self._load_json_file("services.json")

    def _reload_config(self) -> None:
        self.config_data = self._load_config_data()
        self.services = self.config_data.get("services", {})

    def _build_calendar_link_text(self, calendar_url: str) -> str:
        """Build Slack mrkdwn link text for calendar URL."""
        if not calendar_url:
            return ""
        return f"<{calendar_url}|カレンダーを開く>"

    def send_notification(self, message: str, title: str = None, color: str = "good") -> bool:
        """
        Send a notification to Slack.

        Args:
            message: The main message content.
            title: Optional title for the notification.
            color: Attachment color: good, warning, danger, or hex color.

        Returns:
            True if successful, False otherwise.
        """
        if not self.enabled:
            logging.debug("Slack notifications disabled, skipping notification")
            return False

        try:
            payload = {
                "attachments": [
                    {
                        "color": color,
                        "title": title,
                        "text": message,
                        "footer": "Salon Booking System",
                        "ts": int(datetime.now().timestamp()),
                        "mrkdwn_in": ["text", "pretext", "fields"],
                    }
                ]
            }

            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200:
                print("Slack notification sent successfully")
                return True

            logging.error(f"Failed to send Slack notification: {response.status_code} - {response.text}")
            return False

        except Exception as e:
            logging.error(f"Error sending Slack notification: {e}", exc_info=True)
            return False

    def notify_user_login(self, user_id: str, display_name: str) -> bool:
        """User-login operator notification is disabled by specification."""
        logging.info("User login operator notification is disabled.")
        return True

    def _extract_price(self, data: Dict[str, Any], service_name: str = "") -> int:
        """Extract price from multiple possible reservation payload keys."""
        for key in ("total_price", "price", "Price", "料金"):
            value = data.get(key)
            if value in (None, ""):
                continue
            try:
                if isinstance(value, str):
                    normalized = value.replace("¥", "").replace(",", "").strip()
                    if not normalized:
                        continue
                    return int(float(normalized))
                return int(value)
            except Exception:
                continue

        if service_name:
            return self._get_service_price(service_name)
        return 0

    def _format_price_change_line(self, old_reservation: Dict[str, Any], new_reservation: Dict[str, Any]) -> str:
        """Build price line for reservation modification notification."""
        old_service = str(old_reservation.get("service", "") or "")
        new_service = str(new_reservation.get("service", "") or "")
        old_price = self._extract_price(old_reservation, old_service)
        new_price = self._extract_price(new_reservation, new_service)

        if old_price > 0 and new_price > 0:
            return f"💰¥{old_price:,} ⇒ ¥{new_price:,}"
        if new_price > 0:
            return f"💰¥{new_price:,}"
        if old_price > 0:
            return f"💰¥{old_price:,} ⇒ 未設定"
        return "💰未設定"

    def notify_reservation_confirmation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is confirmed. Wording follows LINE version."""
        staff_name = reservation_data.get("staff")
        calendar_url = self._get_calendar_url(staff_name)

        service_name = reservation_data.get("service", "")
        price = self._extract_price(reservation_data, service_name)

        message = f"👤{client_name}\n"
        message += f"📅{reservation_data.get('date', 'N/A')} {reservation_data.get('start_time', 'N/A')}~{reservation_data.get('end_time', 'N/A')}\n"
        message += f"💇{service_name}（{reservation_data.get('staff', 'N/A')}）\n"
        message += f"💰¥{price:,}\n\n"
        message += f"🆔{reservation_data.get('reservation_id', 'N/A')}"

        calendar_link = self._build_calendar_link_text(calendar_url)
        if calendar_link:
            message += f"\n\n{calendar_link}"

        return self.send_notification(
            message=message,
            title="🔔新規予約",
            color="good",
        )

    def notify_reservation_modification(
        self,
        old_reservation: Dict[str, Any],
        new_reservation: Dict[str, Any],
        client_name: str,
    ) -> bool:
        """Send notification when reservation is modified. Wording follows LINE version."""
        staff_name = new_reservation.get("staff") or old_reservation.get("staff")
        calendar_url = self._get_calendar_url(staff_name)

        old_time = f"{old_reservation.get('start_time', 'N/A')}~{old_reservation.get('end_time', 'N/A')}"
        new_time = f"{new_reservation.get('start_time', 'N/A')}~{new_reservation.get('end_time', 'N/A')}"

        message = f"👤{client_name}\n"
        message += f"📅{old_reservation.get('date', 'N/A')} ⇒ {new_reservation.get('date', 'N/A')}\n"
        message += f"⏰{old_time} ⇒ {new_time}\n"
        message += f"💇{old_reservation.get('service', 'N/A')} ⇒ {new_reservation.get('service', 'N/A')}\n"
        message += f"🧑{old_reservation.get('staff', 'N/A')} ⇒ {new_reservation.get('staff', 'N/A')}\n"
        message += self._format_price_change_line(old_reservation, new_reservation)

        calendar_link = self._build_calendar_link_text(calendar_url)
        if calendar_link:
            message += f"\n\n{calendar_link}"

        return self.send_notification(
            message=message,
            title="✏️予約変更",
            color="warning",
        )

    def notify_reservation_cancellation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is cancelled. Wording follows LINE version."""
        staff_name = reservation_data.get("staff")
        calendar_url = self._get_calendar_url(staff_name)

        message = f"👤{client_name}\n"
        message += f"📅{reservation_data.get('date', 'N/A')} {reservation_data.get('start_time', 'N/A')}~{reservation_data.get('end_time', 'N/A')}\n"
        message += f"💇{reservation_data.get('service', 'N/A')}（{reservation_data.get('staff', 'N/A')}）\n\n"
        message += f"🆔{reservation_data.get('reservation_id', 'N/A')}"

        calendar_link = self._build_calendar_link_text(calendar_url)
        if calendar_link:
            message += f"\n\n{calendar_link}"

        return self.send_notification(
            message=message,
            title="❌予約キャンセル",
            color="danger",
        )

    def notify_reminder_status(
        self,
        success_count: int,
        total_count: int,
        failed_reservations: List[Dict[str, Any]],
    ) -> bool:
        """Reminder operator notification is disabled; keep no-op for compatibility."""
        logging.info("Reminder manager notification is disabled.")
        return True

    def _get_service_by_name_or_id(self, service_identifier: str) -> Optional[Dict[str, Any]]:
        self._reload_config()

        if not service_identifier:
            return None

        ident = str(service_identifier).strip()

        direct = self.services.get(ident)
        if isinstance(direct, dict):
            return direct

        for _key, service_info in self.services.items():
            if isinstance(service_info, dict) and str(service_info.get("id", "")).strip() == ident:
                return service_info

        for _key, service_info in self.services.items():
            if isinstance(service_info, dict) and str(service_info.get("name", "")).strip() == ident:
                return service_info

        return None

    def _get_service_duration(self, service_name: str) -> int:
        """Get service duration in minutes."""
        try:
            service = self._get_service_by_name_or_id(service_name)
            if not service:
                return 0
            return int(service.get("duration", 0) or 0)
        except Exception:
            return 0

    def _get_service_price(self, service_name: str) -> int:
        """Get service price."""
        try:
            service = self._get_service_by_name_or_id(service_name)
            if not service:
                return 0
            return int(service.get("price", 0) or 0)
        except Exception:
            return 0

    def _get_calendar_url(self, staff_name: str = None) -> str:
        """Get the Google Calendar URL, staff-specific when available."""
        try:
            from api.google_calendar import GoogleCalendarHelper

            calendar_helper = GoogleCalendarHelper()
            return calendar_helper.get_short_calendar_url(staff_name)
        except Exception as e:
            logging.error(f"Error getting calendar URL: {e}")
            return "https://calendar.google.com/calendar"


# Global instance for easy access
slack_notifier = SlackNotifier()


def send_user_login_notification(user_id: str, display_name: str) -> bool:
    """Compatibility wrapper. Login operator notification is disabled."""
    return slack_notifier.notify_user_login(user_id, display_name)


def send_reservation_confirmation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation confirmation notifications."""
    return slack_notifier.notify_reservation_confirmation(reservation_data, client_name)


def send_reservation_modification_notification(
    old_reservation: Dict[str, Any],
    new_reservation: Dict[str, Any],
    client_name: str,
) -> bool:
    """Convenience function for reservation modification notifications."""
    return slack_notifier.notify_reservation_modification(old_reservation, new_reservation, client_name)


def send_reservation_cancellation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation cancellation notifications."""
    return slack_notifier.notify_reservation_cancellation(reservation_data, client_name)


def send_reminder_status_notification(
    success_count: int,
    total_count: int,
    failed_reservations: List[Dict[str, Any]],
) -> bool:
    """Compatibility wrapper. Reminder operator notification is disabled."""
    return slack_notifier.notify_reminder_status(success_count, total_count, failed_reservations)


if __name__ == "__main__":
    notifier = SlackNotifier()

    test_reservation = {
        "reservation_id": "TEST-001",
        "date": "2026-05-01",
        "start_time": "10:00",
        "end_time": "11:00",
        "service": "カット",
        "staff": "田中",
        "total_price": 5000,
    }

    success = notifier.notify_reservation_confirmation(test_reservation, "Test User")
    if success:
        print("✅ Test notification sent successfully!")
    else:
        print("❌ Failed to send test notification")

