"""
Slack notification service for salon booking system
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
        
        if not self.enabled:
            logging.warning("Slack webhook URL not configured. Notifications disabled.")
        else:
            print("Slack notifications enabled")
    
    def send_notification(self, message: str, title: str = None, color: str = "good") -> bool:
        """
        Send a notification to Slack
        
        Args:
            message: The main message content
            title: Optional title for the notification
            color: Color for the attachment (good, warning, danger, or hex color)
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            logging.debug("Slack notifications disabled, skipping notification")
            return False
        
        try:
            # Prepare the payload
            payload = {
                "attachments": [
                    {
                        "color": color,
                        "title": title,
                        "text": message,
                        "footer": "Salon Booking System",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            # Send the request
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                print("Slack notification sent successfully")
                return True
            else:
                logging.error(f"Failed to send Slack notification: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"Error sending Slack notification: {e}")
            return False
    
    def notify_user_login(self, user_id: str, display_name: str) -> bool:
        """Send notification when user logs in"""
        message = f"👤 **ユーザーログイン**\n"
        message += f"• ユーザーID: `{user_id}`\n"
        message += f"• 表示名: {display_name}\n"
        message += f"• 時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        return self.send_notification(
            message=message,
            title="🔐 ユーザーログイン",
            color="good"
        )
    
    def notify_reservation_confirmation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is confirmed"""
        # Get staff-specific calendar URL
        staff_name = reservation_data.get('staff')
        calendar_url = self._get_calendar_url(staff_name)
        message = f"✅ **新規予約確定**\n"
        message += f"• 予約ID: `{reservation_data.get('reservation_id', 'N/A')}`\n"
        message += f"• お客様: {client_name}\n"
        message += f"• 日付: {reservation_data.get('date', 'N/A')}\n"
        message += f"• 時間: {reservation_data.get('start_time', 'N/A')}~{reservation_data.get('end_time', 'N/A')}\n"
        message += f"• サービス: {reservation_data.get('service', 'N/A')}\n"
        message += f"• 担当者: {reservation_data.get('staff', 'N/A')}\n"
        message += f"• 所要時間: {self._get_service_duration(reservation_data.get('service', ''))}分\n"
        message += f"• 料金: ¥{self._get_service_price(reservation_data.get('service', '')):,}\n"
        message += f"• <{calendar_url}|カレンダーを開く>"
        
        return self.send_notification(
            message=message,
            title="📅 新規予約",
            color="good"
        )
    
    def notify_reservation_modification(self, old_reservation: Dict[str, Any], new_reservation: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is modified"""
        # Get staff-specific calendar URL (use new reservation's staff, fallback to old)
        staff_name = new_reservation.get('staff') or old_reservation.get('staff')
        calendar_url = self._get_calendar_url(staff_name)
        message = f"🔄 **予約変更**\n"
        message += f"• 予約ID: `{old_reservation.get('reservation_id', 'N/A')}`\n"
        message += f"• お客様: {client_name}\n\n"
        
        # Show changes
        changes = []
        
        # Date change
        if old_reservation.get('date') != new_reservation.get('date'):
            changes.append(f"📅 日付: {old_reservation.get('date', 'N/A')} → {new_reservation.get('date', 'N/A')}")
        
        # Time change
        old_time = f"{old_reservation.get('start_time', 'N/A')}~{old_reservation.get('end_time', 'N/A')}"
        new_time = f"{new_reservation.get('start_time', 'N/A')}~{new_reservation.get('end_time', 'N/A')}"
        if old_time != new_time:
            changes.append(f"⏰ 時間: {old_time} → {new_time}")
        
        # Service change
        if old_reservation.get('service') != new_reservation.get('service'):
            changes.append(f"💇 サービス: {old_reservation.get('service', 'N/A')} → {new_reservation.get('service', 'N/A')}")
        
        # Staff change
        if old_reservation.get('staff') != new_reservation.get('staff'):
            changes.append(f"👨‍💼 担当者: {old_reservation.get('staff', 'N/A')} → {new_reservation.get('staff', 'N/A')}")
        
        if changes:
            message += "**変更内容:**\n" + "\n".join(f"• {change}" for change in changes)
        else:
            message += "• 変更は検出されませんでした"
        
        message += f"\n• <{calendar_url}|カレンダーを開く>"
        
        return self.send_notification(
            message=message,
            title="✏️ 予約変更",
            color="warning"
        )
    
    def notify_reservation_cancellation(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """Send notification when reservation is cancelled"""
        # Get staff-specific calendar URL
        staff_name = reservation_data.get('staff')
        calendar_url = self._get_calendar_url(staff_name)
        message = f"❌ **予約キャンセル**\n"
        message += f"• 予約ID: `{reservation_data.get('reservation_id', 'N/A')}`\n"
        message += f"• お客様: {client_name}\n"
        message += f"• 日付: {reservation_data.get('date', 'N/A')}\n"
        message += f"• 時間: {reservation_data.get('start_time', 'N/A')}~{reservation_data.get('end_time', 'N/A')}\n"
        message += f"• サービス: {reservation_data.get('service', 'N/A')}\n"
        message += f"• 担当者: {reservation_data.get('staff', 'N/A')}\n"
        message += f"• <{calendar_url}|カレンダーを開く>"
        
        return self.send_notification(
            message=message,
            title="🚫 予約キャンセル",
            color="danger"
        )
    
    def notify_reminder_status(self, success_count: int, total_count: int, failed_reservations: List[Dict[str, Any]]) -> bool:
        """Send notification about reminder status to manager"""
        if success_count == total_count and total_count > 0:
            # All reminders sent successfully
            message = f"✅ **予約リマインダー送信完了**\n\n"
            message += f"📊 **送信結果:**\n"
            message += f"• 送信成功: {success_count}件\n"
            message += f"• 送信失敗: 0件\n"
            message += f"• 合計: {total_count}件\n\n"
            message += f"すべてのリマインダーが正常に送信されました。"
            
            color = "good"
            title = "📅 リマインダー送信完了"
            
        elif success_count > 0:
            # Some reminders sent successfully
            message = f"⚠️ **予約リマインダー送信結果**\n\n"
            message += f"📊 **送信結果:**\n"
            message += f"• 送信成功: {success_count}件\n"
            message += f"• 送信失敗: {total_count - success_count}件\n"
            message += f"• 合計: {total_count}件\n\n"
            
            if failed_reservations:
                message += f"🚫 **送信失敗した予約:**\n"
                for res in failed_reservations[:5]:  # Show first 5 failures
                    message += f"• {res.get('client_name', 'N/A')} - {res.get('date', 'N/A')} {res.get('start_time', 'N/A')}\n"
                
                if len(failed_reservations) > 5:
                    message += f"• ...他 {len(failed_reservations) - 5}件\n"
            
            color = "warning"
            title = "⚠️ リマインダー送信結果"
            
        else:
            # No reminders sent
            message = f"リマインダー送信はありません"
            
            color = "good"
            title = "📅 リマインダー送信"
        
        return self.send_notification(
            message=message,
            title=title,
            color=color
        )
    
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

            # Try direct lookup (service stored as ID)
            direct = services.get(service_name)
            if isinstance(direct, dict):
                return direct.get("duration", 0)

            # Fallback: search by service name field
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

            # Try direct lookup (service stored as ID)
            direct = services.get(service_name)
            if isinstance(direct, dict):
                return direct.get("price", 0)

            # Fallback: search by service name field
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
slack_notifier = SlackNotifier()


def send_user_login_notification(user_id: str, display_name: str) -> bool:
    """Convenience function for user login notifications"""
    return slack_notifier.notify_user_login(user_id, display_name)


def send_reservation_confirmation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation confirmation notifications"""
    return slack_notifier.notify_reservation_confirmation(reservation_data, client_name)


def send_reservation_modification_notification(old_reservation: Dict[str, Any], new_reservation: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation modification notifications"""
    return slack_notifier.notify_reservation_modification(old_reservation, new_reservation, client_name)


def send_reservation_cancellation_notification(reservation_data: Dict[str, Any], client_name: str) -> bool:
    """Convenience function for reservation cancellation notifications"""
    return slack_notifier.notify_reservation_cancellation(reservation_data, client_name)


if __name__ == "__main__":
    # Test the Slack notifier
    notifier = SlackNotifier()
    
    # Test notification
    test_message = "🧪 **Test Notification**\nThis is a test message from the salon booking system."
    success = notifier.send_notification(
        message=test_message,
        title="Test",
        color="good"
    )
    
    if success:
        print("✅ Test notification sent successfully!")
    else:
        print("❌ Failed to send test notification")
