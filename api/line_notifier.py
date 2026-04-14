"""
LINE notification service for salon booking system
"""
import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import requests
from dotenv import load_dotenv

class LineNotifier:
    def __init__(self):
        load_dotenv()
        self.channel_access_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        self.notification_user_id = os.getenv("LINE_NOTIFICATION_USER_ID")
        self.enabled = bool(self.channel_access_token and self.notification_user_id)
        
        if not self.enabled:
            logging.warning("LINE notification not configured. Missing LINE_CHANNEL_ACCESS_TOKEN or LINE_NOTIFICATION_USER_ID.")
        else:
            print("LINE notifications enabled")
    
    def send_notification(self, message: str, title: str = None, calendar_url: str = None) -> bool:
        """
        Send a notification to LINE
        """
        if not self.enabled:
            logging.debug("LINE notifications disabled, skipping notification")
            return False
        
        try:
            if title:
                full_message = f"{title}\n\n{message}"
            else:
                full_message = f"{message}"
            
            if calendar_url:
                payload = {
                    "to": self.notification_user_id,
                    "messages": [
                        {
                            "type": "template",
                            "altText": full_message,
                            "template": {
                                "type": "buttons",
                                "text": full_message,
                                "actions": [
                                    {
                                        "type": "uri",
                                        "label": "カレンダーを開く",
                                        "uri": calendar_url
                                    }
                                ]
                            }
                        }
                    ]
                }
            else:
                payload = {
                    "to": self.notification_user_id,
                    "messages": [
                        {
                            "type": "text",
                            "text": full_message
                        }
                    ]
                }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.channel_access_token}"
            }
            
            response = requests.post(
                "https://api.line.me/v2/bot/message/push",
                data=json.dumps(payload),
                headers=headers,
                timeout=10
            )
            
            if response.status_code != 200:
                logging.error(f"Failed to send LINE notification: {response.status_code} - {response.text}")
                return False
            
            print("LINE notification sent successfully")
            return True
                
        except Exception as e:
            logging.error(f"Error sending LINE notification: {e}")
            return False
    
    def notify_user_login(self, user_id: str, display_name: str) -> bool:
        """Send notification when user logs in"""
        message = f"👤名前: {display_name}\n\n"
        message += f"🆔ユーザーID: `{user_id}`"
        
        return self.send_notification(
            message=message,
            title="💡ユーザー初回ログイン"
        )
    
    def notify_reservation_confirmation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is confirmed"""
        staff_name = reservation_data.get("staff")
        calendar_url = self._get_calendar_url(staff_name)

        message = f"👤{client_name}\n"
        message += f"📅{reservation_data.get('date', 'N/A')} {reservation_data.get('start_time', 'N/A')}~{reservation_data.get('end_time', 'N/A')}\n"
        message += f"💇{reservation_data.get('service', 'N/A')}（ {reservation_data.get('staff', 'N/A')}）\n"
        message += f"💰¥{self._get_service_price(reservation_data.get('service', '')):,}\n\n"
        message += f"🆔{reservation_data.get('reservation_id', 'N/A')}"
        
        return self.send_notification(
            message=message,
            title="🔔新規予約",
            calendar_url=calendar_url
        )
    
    def notify_reservation_modification(self, old_reservation: Dict[str, Any], new_reservation: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is modified"""
        staff_name = new_reservation.get("staff") or old_reservation.get("staff")
        calendar_url = self._get_calendar_url(staff_name)
        
        old_time = f"{old_reservation.get('start_time', 'N/A')}~{old_reservation.get('end_time', 'N/A')}"
        new_time = f"{new_reservation.get('start_time', 'N/A')}~{new_reservation.get('end_time', 'N/A')}"

        message = f"👤{client_name}\n"
        message += f"📅{old_reservation.get('date', 'N/A')}⇒ {new_reservation.get('date', 'N/A')}\n"
        message += f"⏰{old_time}⇒ {new_time}\n"
        message += f"💇{old_reservation.get('service', 'N/A')}⇒ {new_reservation.get('service', 'N/A')}\n"
        message += f"🧑{old_reservation.get('staff', 'N/A')}⇒ {new_reservation.get('staff', 'N/A')}"
        
        return self.send_notification(
            message=message,
            title="✏️予約変更",
            calendar_url=calendar_url
        )

    def notify_reservation_cancellation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is cancelled"""
        staff_name = reservation_data.get("staff")
        calendar_url = self._get_calendar_url(staff_name)

        message = f"👤{client_name}\n"
        message += f"📅{reservation_data.get('date', 'N/A')} {reservation_data.get('start_time', 'N/A')}~{reservation_data.get('end_time', 'N/A')}\n"
        message += f"💇{reservation_data.get('service', 'N/A')}（ {reservation_data.get('staff', 'N/A')}）\n\n"
        message += f"🆔{reservation_data.get('reservation_id', 'N/A')}"

        return self.send_notification(
            message=message,
            title="❌予約キャンセル",
            calendar_url=calendar_url
        )

    def notify_reminder_status(self, success_count: int, total_count: int, failed_reservations: List[Dict[str, Any]]) -> bool:
        """Reminder status notification is disabled"""
        logging.info("Reminder manager notification is disabled.")
        return True
    
    def _get_service_duration(self, service_name: str) -> int:
        """Get service duration in minutes"""
        try:
            # Load services data
            current_dir = os.path.dirname(os.path.abspath(__file__))
            services_file = os.path.join(current_dir, "data", "services.json")
            
            with open(services_file, 'r', encoding='utf-8') as f:
                services_data = json.load(f)
            
            services = services_data.get("services", {})
            if not services:
                return 0

            direct = services.get(service_name)
            if isinstance(direct, dict):
                return direct.get("duration", 0)

            for service_info in services.values():
                if isinstance(service_info, dict) and service_info.get("name") == service_name:
                    return service_info.get("duration", 0)
            
            return 0
        except Exception:
            return 0
    
    def _get_service_price(self, service_name: str) -> int:
        """Get service price"""
        try:
            # Load services data
            current_dir = os.path.dirname(os.path.abspath(__file__))
            services_file = os.path.join(current_dir, "data", "services.json")
            
            with open(services_file, 'r', encoding='utf-8') as f:
                services_data = json.load(f)
            
            services = services_data.get("services", {})
            if not services:
                return 0

            direct = services.get(service_name)
            if isinstance(direct, dict):
                return direct.get("price", 0)

            for service_info in services.values():
                if isinstance(service_info, dict) and service_info.get("name") == service_name:
                    return service_info.get("price", 0)
            
            return 0
        except Exception:
            return 0
    
    def _get_calendar_url(self, staff_name: str = None) -> str:
        """Get the Google Calendar URL (short version) - staff-specific"""
        try:
            from api.google_calendar import GoogleCalendarHelper
            calendar_helper = GoogleCalendarHelper()
            return calendar_helper.get_short_calendar_url(staff_name)
        except Exception as e:
            logging.error(f"Error getting calendar URL: {e}")
            return "https://calendar.google.com/calendar"


# Global instance for easy access
line_notifier = LineNotifier()


def send_user_login_notification(user_id: str, display_name: str) -> bool:
    """Convenience function for user login notifications"""
    return line_notifier.notify_user_login(user_id, display_name)


def send_reservation_confirmation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation confirmation notifications"""
    return line_notifier.notify_reservation_confirmation(reservation_data, client_name)


def send_reservation_modification_notification(old_reservation: Dict[str, Any], new_reservation: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation modification notifications"""
    return line_notifier.notify_reservation_modification(old_reservation, new_reservation, client_name)


def send_reservation_cancellation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation cancellation notifications"""
    return line_notifier.notify_reservation_cancellation(reservation_data, client_name)


if __name__ == "__main__":
    # Test the LINE notifier
    notifier = LineNotifier()
    test_message = "テスト通知です。LINE通知が正常に動作しているか確認してください。"
    success = notifier.send_notification(
        message=test_message,
        title="テスト通知"
    )
    if success:
        print("通知送信成功")
    else:
        print("通知送信失敗")
