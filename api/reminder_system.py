"""
Reminder system for salon booking system
Sends reminder messages to users about their reservations the day before
config.json integrated version
"""
import os
import json
import logging
import re
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv


class ReminderSystem:
    def __init__(self):
        load_dotenv()
        self.line_channel_access_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        self.enabled = bool(self.line_channel_access_token)

        self.config_data = self._load_config_data()
        self.services = self.config_data.get("services", {})
        self.salon_config = self.config_data.get("salon", {})

        if not self.enabled:
            logging.warning("LINE Channel Access Token not configured. Reminder system disabled.")
        else:
            print("Reminder system enabled")

    def _config_path(self) -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, "data", "config.json")

    def _load_config_data(self) -> Dict[str, Any]:
        try:
            with open(self._config_path(), "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading config.json: {e}")
            return {}

    def _reload_config(self):
        self.config_data = self._load_config_data()
        self.services = self.config_data.get("services", {})
        self.salon_config = self.config_data.get("salon", {})

    def get_tomorrow_reservations(self) -> List[Dict[str, Any]]:
        """Get all reservations for tomorrow (Tokyo timezone)"""
        try:
            from api.google_calendar import GoogleCalendarHelper
            from api.google_sheets_logger import GoogleSheetsLogger

            import pytz
            timezone_str = self.salon_config.get("timezone", "Asia/Tokyo")
            tokyo_tz = pytz.timezone(timezone_str)
            tomorrow = (datetime.now(tokyo_tz) + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"Getting reservations for tomorrow: {tomorrow} ({timezone_str})")

            calendar_helper = GoogleCalendarHelper()
            events = calendar_helper.get_events_for_date(tomorrow)

            reservations = []
            for event in events:
                reservation = self._parse_event_to_reservation(event)
                if reservation:
                    reservations.append(reservation)

            try:
                sheets_logger = GoogleSheetsLogger()
                sheets_reservations = sheets_logger.get_reservations_for_date(tomorrow)

                existing_ids = {r.get("reservation_id") for r in reservations}
                for sheet_res in sheets_reservations:
                    if sheet_res.get("reservation_id") not in existing_ids:
                        reservations.append(sheet_res)

            except Exception as e:
                logging.warning(f"Could not get reservations from sheets: {e}")

            print(f"Found {len(reservations)} reservations for {tomorrow}")
            return reservations

        except Exception as e:
            logging.error(f"Error getting tomorrow's reservations: {e}", exc_info=True)
            return []

    def _parse_event_to_reservation(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse Google Calendar event to reservation format"""
        try:
            summary = event.get("summary", "")
            description = event.get("description", "")
            start_time = event.get("start", {}).get("dateTime", "")
            end_time = event.get("end", {}).get("dateTime", "")

            reservation_id = None
            if "予約ID:" in description:
                reservation_id = description.split("予約ID:")[1].split("\n")[0].strip()

            match = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
            if not match:
                return None

            service = match.group(1)
            client_name = match.group(2)
            staff = match.group(3)

            if start_time:
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))

                import pytz
                timezone_str = self.salon_config.get("timezone", "Asia/Tokyo")
                tz = pytz.timezone(timezone_str)
                start_dt = start_dt.astimezone(tz)

                date = start_dt.strftime("%Y-%m-%d")
                start_time_str = start_dt.strftime("%H:%M")

                if end_time:
                    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                    end_dt = end_dt.astimezone(tz)
                    end_time_str = end_dt.strftime("%H:%M")
                else:
                    end_time_str = ""

                return {
                    "reservation_id": reservation_id,
                    "date": date,
                    "start_time": start_time_str,
                    "end_time": end_time_str,
                    "service": service,
                    "staff": staff,
                    "client_name": client_name,
                }

            return None

        except Exception as e:
            logging.error(f"Error parsing event to reservation: {e}", exc_info=True)
            return None

    def _load_kb_data(self) -> Dict[str, str]:
        """Load data from kb.json file"""
        try:
            possible_paths = []

            base_dirs = [
                os.path.dirname(os.path.abspath(__file__)),
                os.getcwd(),
                os.path.join(os.getcwd(), "api"),
            ]

            for base_dir in base_dirs:
                possible_paths.append(os.path.join(base_dir, "data", "kb.json"))
                possible_paths.append(os.path.join(base_dir, "api", "data", "kb.json"))
                possible_paths.append(os.path.join(base_dir, "data", "KB.json"))
                possible_paths.append(os.path.join(base_dir, "api", "data", "KB.json"))

            for kb_file in possible_paths:
                try:
                    if not os.path.exists(kb_file):
                        continue
                    if not os.path.isfile(kb_file):
                        continue

                    with open(kb_file, "r", encoding="utf-8") as f:
                        kb_data = json.load(f)

                    kb_dict = {}
                    for item in kb_data:
                        key = item.get("キー", "")
                        value = item.get("例（置換値）", "")
                        kb_dict[key] = value

                    return kb_dict
                except (FileNotFoundError, OSError, json.JSONDecodeError):
                    continue

            raise FileNotFoundError(f"Could not find kb.json file. Tried paths: {possible_paths}")

        except Exception as e:
            logging.error(f"Error loading kb.json: {e}")
            return {}

    def _get_service_by_name_or_id(self, service_identifier: str) -> Optional[Dict[str, Any]]:
        self._reload_config()

        if not service_identifier:
            return None

        ident = str(service_identifier).strip()

        direct = self.services.get(ident)
        if isinstance(direct, dict):
            return direct

        for _key, service_info in self.services.items():
            if isinstance(service_info, dict) and service_info.get("id") == ident:
                return service_info

        for _key, service_info in self.services.items():
            if isinstance(service_info, dict) and service_info.get("name") == ident:
                return service_info

        return None

    def _get_service_duration(self, service_name: str, kb_data: Optional[Dict[str, str]] = None) -> str:
        """Get service duration string for the given service"""
        if not service_name:
            return "N/A"

        try:
            service = self._get_service_by_name_or_id(service_name)
            if service:
                duration = service.get("duration")
                if duration:
                    return f"{int(duration)}分"

            if kb_data is None:
                kb_data = self._load_kb_data()

            kb_value = kb_data.get(service_name)
            if kb_value:
                match = re.search(r"([約\d~〜\-]+)\s*分", kb_value)
                if match:
                    minutes_text = match.group(1)
                    if minutes_text.endswith("分"):
                        return minutes_text
                    return f"{minutes_text}分"

            return "N/A"
        except Exception as e:
            logging.error(f"Error getting service duration for {service_name}: {e}")
            return "N/A"

    def send_reminder_to_user(self, reservation: Dict[str, Any], user_id: str) -> bool:
        """Send reminder message to a specific user"""
        try:
            kb_data = self._load_kb_data()

            user_name = reservation.get("client_name", "N/A")
            reservation_date = reservation.get("date", "N/A")
            start_time = reservation.get("start_time", "N/A")
            service = reservation.get("service", "N/A")
            staff = reservation.get("staff", "N/A")

            duration_display = None
            raw_duration = reservation.get("duration") or reservation.get("duration_minutes")
            if raw_duration:
                if isinstance(raw_duration, (int, float)):
                    if raw_duration > 0:
                        duration_display = f"{int(raw_duration)}分"
                elif isinstance(raw_duration, str):
                    match = re.search(r"(約?\d+)", raw_duration)
                    if match:
                        minutes_text = match.group(1)
                        duration_display = f"{minutes_text}分"

            if not duration_display or duration_display == "0分":
                duration_display = self._get_service_duration(service, kb_data)

            duration = duration_display if duration_display else "N/A"

            cancel_deadline = kb_data.get("キャンセル規定", "来店の2時間前まで")
            salon_phone = kb_data.get("電話番号", "03-1234-5678")
            salon_name = kb_data.get("店名", "SalonAI 表参道店")

            message = f"{user_name} 様\n"
            message += f"明日（{reservation_date}）{start_time} より {service}（担当：{staff}）のご予約を承っております😊\n\n"
            message += f"⏰所要時間：{duration}\n\n"
            message += "当日はしっかりカウンセリングを行い、ご自宅でも扱いやすく、再現しやすいスタイルに仕上げますのでご安心ください。\n\n"
            message += "また、最近は乾燥・広がり対策でトリートメントを一緒にされる方が増えています。\n"
            message += "👉当日追加も可能です\n\n"
            message += f"※変更・キャンセルは{cancel_deadline}までにメニューからお手続きください。\n\n"
            message += "明日のご来店を心よりお待ちしております✨\n"
            message += f"{salon_name}\n"
            message += f"{salon_phone}"

            payload = {
                "to": user_id,
                "messages": [
                    {
                        "type": "text",
                        "text": message,
                    }
                ],
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.line_channel_access_token}",
            }

            response = requests.post(
                "https://api.line.me/v2/bot/message/push",
                data=json.dumps(payload),
                headers=headers,
                timeout=10,
            )

            if response.status_code == 200:
                print(f"Reminder sent successfully to user {user_id} for reservation {reservation.get('reservation_id')}")
                return True
            else:
                logging.error(f"Failed to send reminder to user {user_id}: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logging.error(f"Error sending reminder to user {user_id}: {e}", exc_info=True)
            return False

    def get_user_id_for_reservation(self, reservation: Dict[str, Any]) -> Optional[str]:
        """Get LINE user ID for a reservation"""
        try:
            from api.google_sheets_logger import GoogleSheetsLogger
            sheets_logger = GoogleSheetsLogger()

            reservation_id = reservation.get("reservation_id")
            client_name = reservation.get("client_name", "N/A")

            print(f"Looking for user ID for reservation {reservation_id} (client: {client_name})")

            if reservation_id:
                user_id = sheets_logger.get_user_id_for_reservation(reservation_id)
                if user_id:
                    print(f"Found user ID {user_id} for reservation {reservation_id}")
                    return user_id
                else:
                    print(f"No user ID found for reservation {reservation_id}")

            logging.warning(f"Could not find user ID for reservation {reservation_id}")
            return None

        except Exception as e:
            logging.error(f"Error getting user ID for reservation: {e}", exc_info=True)
            return None

    def send_reminder_notification_to_manager(
        self,
        success_count: int,
        total_count: int,
        failed_reservations: List[Dict[str, Any]],
    ) -> bool:
        """Send notification to manager about reminder status"""
        try:
            from api.notification_manager import line_notifier
            return line_notifier.notify_reminder_status(success_count, total_count, failed_reservations)
        except Exception as e:
            logging.error(f"Error sending reminder notification to manager: {e}", exc_info=True)
            return False

    def run_daily_reminders(self) -> Dict[str, Any]:
        """Run daily reminder process"""
        print("Starting daily reminder process...")

        reservations = self.get_tomorrow_reservations()

        success_count = 0
        failed_reservations = []

        for reservation in reservations:
            print(f"Processing reservation: {reservation.get('reservation_id')} for {reservation.get('client_name')}")
            user_id = self.get_user_id_for_reservation(reservation)

            if user_id:
                print(f"Attempting to send reminder to user {user_id}")
                if self.send_reminder_to_user(reservation, user_id):
                    success_count += 1
                    print(f"✅ Reminder sent successfully to user {user_id}")
                else:
                    failed_reservations.append(reservation)
                    print(f"❌ Failed to send reminder to user {user_id}")
            else:
                logging.warning(f"Could not find user ID for reservation {reservation.get('reservation_id')}")
                failed_reservations.append(reservation)
                print(f"❌ No user ID found for reservation {reservation.get('reservation_id')}")

        self.send_reminder_notification_to_manager(success_count, len(reservations), failed_reservations)

        result = {
            "success_count": success_count,
            "total_count": len(reservations),
            "failed_reservations": failed_reservations,
        }

        print(f"Daily reminder process completed: {success_count}/{len(reservations)} sent successfully")
        return result


# Global instance
reminder_system = ReminderSystem()


def run_daily_reminders():
    """Convenience function to run daily reminders"""
    return reminder_system.run_daily_reminders()

