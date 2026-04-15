"""
Scheduler for running daily reminder tasks
Runs at configured time daily to send reservation reminders
Compatible with new KB format:
[
  {
    "id": "remind_time",
    "カテゴリ": "設定",
    "キー": ["REMIND_TIME", "リマインド時刻"],
    "値": "来店前日 09:00 自動配信"
  }
]
"""
import os
import time
import json
import logging
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv


class ReminderScheduler:
    def __init__(self):
        load_dotenv()
        self.enabled = os.getenv("REMINDER_SCHEDULER_ENABLED", "true").lower() == "true"
        self.timezone = os.getenv("TIMEZONE", "Asia/Tokyo")
        self.default_remind_time = "来店前日 09:00 自動配信"

        if self.enabled:
            print("Reminder scheduler enabled")
            self._setup_schedule()
        else:
            print("Reminder scheduler disabled")

    def _load_kb_data(self) -> dict:
        """Load kb.json in new KB format and flatten keys into a simple dict."""
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
                    if not os.path.exists(kb_file) or not os.path.isfile(kb_file):
                        continue

                    with open(kb_file, "r", encoding="utf-8") as f:
                        kb_data = json.load(f)

                    if not isinstance(kb_data, list):
                        logging.warning(f"KB file is not a list: {kb_file}")
                        continue

                    kb_dict = {}

                    for item in kb_data:
                        if not isinstance(item, dict):
                            continue

                        keys = item.get("キー", [])
                        value = item.get("値", "")

                        if isinstance(keys, str):
                            keys = [keys]
                        elif not isinstance(keys, list):
                            keys = []

                        value = str(value).strip()
                        if not value:
                            continue

                        for key in keys:
                            key = str(key).strip()
                            if key:
                                kb_dict[key] = value

                    logging.info(f"Loaded KB data from {kb_file} ({len(kb_dict)} flattened keys)")
                    return kb_dict

                except (FileNotFoundError, OSError, json.JSONDecodeError) as e:
                    logging.warning(f"Failed to read KB file {kb_file}: {e}")
                    continue

            logging.warning(f"Could not find kb.json. Tried paths: {possible_paths}")
            return {}

        except Exception as e:
            logging.error(f"Error loading kb.json: {e}", exc_info=True)
            return {}

    def _get_remind_time_text(self) -> str:
        """Get reminder time text from KB, with safe fallback."""
        kb_data = self._load_kb_data()

        # 優先順位: REMIND_TIME -> リマインド時刻 -> デフォルト
        remind_time = (
            kb_data.get("REMIND_TIME")
            or kb_data.get("リマインド時刻")
            or self.default_remind_time
        )

        remind_time = str(remind_time).strip()
        if not remind_time:
            remind_time = self.default_remind_time

        return remind_time

    def _parse_scheduled_time(self, remind_time_text: str) -> tuple[int, int]:
        """
        Extract HH:MM from text.
        Example:
        '来店前日 09:00 自動配信' -> (9, 0)
        """
        time_match = re.search(r"(\d{1,2}):(\d{2})", remind_time_text)

        if not time_match:
            logging.warning(
                f"Could not parse time from remind_time='{remind_time_text}'. "
                f"Using default 09:00."
            )
            return 9, 0

        hour = int(time_match.group(1))
        minute = int(time_match.group(2))

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            logging.warning(
                f"Parsed invalid time from remind_time='{remind_time_text}'. "
                f"Using default 09:00."
            )
            return 9, 0

        return hour, minute

    def _setup_schedule(self):
        """Setup the daily reminder schedule using Tokyo timezone."""
        import pytz

        tokyo_tz = pytz.timezone("Asia/Tokyo")
        current_tokyo_time = datetime.now(tokyo_tz)

        remind_time_text = self._get_remind_time_text()
        scheduled_hour, scheduled_minute = self._parse_scheduled_time(remind_time_text)

        print(f"Current Tokyo time: {current_tokyo_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Schedule time: {scheduled_hour:02d}:{scheduled_minute:02d} (Tokyo timezone)")
        print(
            f"Reminder schedule configured: Daily at "
            f"{scheduled_hour:02d}:{scheduled_minute:02d} Tokyo time "
            f"(from kb.json: {remind_time_text})"
        )

    def _run_reminders(self):
        """Run the daily reminder process."""
        try:
            print("Starting scheduled reminder process...")

            from api.reminder_system import reminder_system

            result = reminder_system.run_daily_reminders()

            success_count = result.get("success_count", 0)
            total_count = result.get("total_count", 0)
            failed_reservations = result.get("failed_reservations", [])

            print(f"Reminder process completed: {success_count}/{total_count} sent successfully")

            if failed_reservations:
                logging.warning(f"Failed to send {len(failed_reservations)} reminders")
                for res in failed_reservations:
                    logging.warning(
                        "Failed reminder: %s - %s %s",
                        res.get("client_name", "N/A"),
                        res.get("date", "N/A"),
                        res.get("start_time", "N/A"),
                    )

        except Exception as e:
            logging.error(f"Error in scheduled reminder process: {e}", exc_info=True)

    def run_scheduler(self):
        """Run the scheduler loop with Tokyo timezone awareness."""
        if not self.enabled:
            print("Scheduler is disabled, not running")
            return

        print("Starting reminder scheduler with Tokyo timezone...")

        import pytz

        tokyo_tz = pytz.timezone("Asia/Tokyo")
        remind_time_text = self._get_remind_time_text()
        scheduled_hour, scheduled_minute = self._parse_scheduled_time(remind_time_text)

        print(f"Will run reminders at {scheduled_hour:02d}:{scheduled_minute:02d} Tokyo time")

        while True:
            try:
                current_tokyo_time = datetime.now(tokyo_tz)
                current_hour = current_tokyo_time.hour
                current_minute = current_tokyo_time.minute

                if current_hour == scheduled_hour and current_minute == scheduled_minute:
                    print(f"Tokyo time {current_tokyo_time.strftime('%H:%M')} - Running reminders...")
                    self._run_reminders()
                    time.sleep(60)
                else:
                    print(f"Tokyo time {current_tokyo_time.strftime('%H:%M')} - Not running reminders")
                    time.sleep(60)

            except KeyboardInterrupt:
                print("Scheduler stopped by user")
                break
            except Exception as e:
                logging.error(f"Error in scheduler loop: {e}", exc_info=True)
                time.sleep(60)

    def run_reminders_now(self):
        """Manually run reminders (for testing)."""
        print("Manually running reminders...")
        self._run_reminders()

    def get_next_run_time(self):
        """Get the next scheduled run time in Tokyo timezone."""
        if not self.enabled:
            return None

        import pytz

        tokyo_tz = pytz.timezone("Asia/Tokyo")
        current_tokyo_time = datetime.now(tokyo_tz)

        remind_time_text = self._get_remind_time_text()
        scheduled_hour, scheduled_minute = self._parse_scheduled_time(remind_time_text)

        next_run = current_tokyo_time.replace(
            hour=scheduled_hour,
            minute=scheduled_minute,
            second=0,
            microsecond=0
        )

        if next_run <= current_tokyo_time:
            next_run += timedelta(days=1)

        return next_run

    def get_status(self):
        """Get scheduler status with Tokyo timezone information."""
        import pytz

        tokyo_tz = pytz.timezone("Asia/Tokyo")
        current_tokyo_time = datetime.now(tokyo_tz)
        remind_time_text = self._get_remind_time_text()
        next_run = self.get_next_run_time()

        return {
            "enabled": self.enabled,
            "timezone": "Asia/Tokyo",
            "current_tokyo_time": current_tokyo_time.strftime("%Y-%m-%d %H:%M:%S"),
            "remind_time": remind_time_text,
            "next_run": next_run,
            "next_run_formatted": next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else None,
        }


# Global instance
reminder_scheduler = ReminderScheduler()


def start_reminder_scheduler():
    """Start the reminder scheduler."""
    reminder_scheduler.run_scheduler()


def run_reminders_manually():
    """Manually run reminders (for testing)."""
    reminder_scheduler.run_reminders_now()


if __name__ == "__main__":
    print("🧪 Testing Reminder Scheduler")
    print("=" * 50)

    scheduler = ReminderScheduler()

    print("📊 Scheduler Status:")
    status = scheduler.get_status()
    for key, value in status.items():
        print(f"  {key}: {value}")

    print()

    if scheduler.enabled:
        print("✅ Scheduler is enabled")
        print("⏰ Next run time:", scheduler.get_next_run_time())
        print()

        print("🧪 Testing manual reminder run...")
        scheduler.run_reminders_now()
    else:
        print("❌ Scheduler is disabled")
        print("Set REMINDER_SCHEDULER_ENABLED=true to enable")

    print("\n✅ Scheduler test completed!")
