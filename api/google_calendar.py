"""
Google Calendar integration for salon reservations
- 店舗営業時間とスタッフ勤怠の積集合で予約可能範囲を判定
- 指名なし時は、対応可能スタッフの空き時間帯を統合して候補表示
"""
import os
import json
import logging
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List, Tuple

import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

from api.business_hours import get_hours_for_date, is_closed_date, get_timezone


class GoogleCalendarHelper:
    NO_PREFERENCE_LABELS = {"指名なし", "おまかせ", "未指定", ""}

    def __init__(self):
        load_dotenv()

        self.service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        self.config_data = self._load_config_data()

        self.timezone = self.config_data.get("salon", {}).get("timezone", get_timezone())
        self.staff_data = self.config_data.get("staff", {})
        self.services = self.config_data.get("services", {})

        self.calendar_id = None
        self.service = None
        self.service_account_email = None

        try:
            self._authenticate()
        except Exception as e:
            print(f"Failed to initialize Google Calendar: {e}")
            self.service = None

    def _config_path(self) -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, "data", "config.json")

    def _load_config_data(self) -> Dict[str, Any]:
        try:
            with open(self._config_path(), "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load config data: {e}")
            return {}

    def _reload_config(self):
        self.config_data = self._load_config_data()
        self.timezone = self.config_data.get("salon", {}).get("timezone", get_timezone())
        self.staff_data = self.config_data.get("staff", {})
        self.services = self.config_data.get("services", {})

    def _normalize_time_format(self, time_str: str) -> Optional[str]:
        try:
            parts = time_str.split(":")
            if len(parts) != 2:
                return None

            hour_part, minute_part = parts
            if len(minute_part) != 2 or not minute_part.isdigit():
                return None

            if len(hour_part) == 1 and hour_part.isdigit():
                hour_part = f"0{hour_part}"
            elif not (len(hour_part) == 2 and hour_part.isdigit()):
                return None

            normalized = f"{hour_part}:{minute_part}"
            datetime.strptime(normalized, "%H:%M")
            return normalized
        except Exception:
            return None

    def _authenticate(self):
        try:
            if not self.service_account_json:
                print("GOOGLE_SERVICE_ACCOUNT_JSON not set, calendar integration disabled")
                return

            try:
                service_account_info = json.loads(self.service_account_json)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
                return

            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/calendar"],
            )

            self.service_account_email = service_account_info.get("client_email", "")
            if self.service_account_email:
                print("Google Calendar API authenticated successfully")
                print(f"Service Account Email: {self.service_account_email}")
                print("IMPORTANT: Share each staff calendar with this email address!")
            else:
                print("Google Calendar API authenticated successfully (service account email not found)")

            self.service = build("calendar", "v3", credentials=credentials)

        except Exception as e:
            print(f"Failed to authenticate with Google Calendar: {e}")
            self.service = None

    def generate_reservation_id(self, date_str: str) -> str:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_part = date_obj.strftime("%Y%m%d")

        import time
        counter = int(time.time() * 1000) % 10000
        return f"RES-{date_part}-{counter:04d}"

    def _is_no_preference(self, staff_name: Optional[str]) -> bool:
        if staff_name is None:
            return True
        return str(staff_name).strip() in self.NO_PREFERENCE_LABELS

    def _weekday_key(self, target_date: date) -> str:
        return ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][target_date.weekday()]

    def _time_to_minutes(self, time_str: str) -> Optional[int]:
        try:
            h, m = map(int, str(time_str).split(":"))
            return h * 60 + m
        except Exception:
            return None

    def _minutes_to_time(self, total_minutes: int) -> str:
        h, m = divmod(total_minutes, 60)
        return f"{h:02d}:{m:02d}"

    def _intersect_periods(
        self,
        left: List[Dict[str, str]],
        right: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        for l in left:
            l_start = self._time_to_minutes(l.get("start"))
            l_end = self._time_to_minutes(l.get("end"))
            if l_start is None or l_end is None or l_start >= l_end:
                continue

            for r in right:
                r_start = self._time_to_minutes(r.get("start"))
                r_end = self._time_to_minutes(r.get("end"))
                if r_start is None or r_end is None or r_start >= r_end:
                    continue

                start = max(l_start, r_start)
                end = min(l_end, r_end)
                if start < end:
                    out.append({"start": self._minutes_to_time(start), "end": self._minutes_to_time(end)})

        out.sort(key=lambda x: x["start"])
        return out

    def _merge_periods(self, periods: List[Dict[str, str]]) -> List[Dict[str, str]]:
        normalized = []
        for p in periods:
            s = self._time_to_minutes(p.get("start"))
            e = self._time_to_minutes(p.get("end"))
            if s is None or e is None or s >= e:
                continue
            normalized.append((s, e))

        if not normalized:
            return []

        normalized.sort()
        merged: List[Tuple[int, int]] = [normalized[0]]
        for start, end in normalized[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))

        return [{"start": self._minutes_to_time(s), "end": self._minutes_to_time(e)} for s, e in merged]

    def _subtract_periods(
        self,
        base_periods: List[Dict[str, str]],
        blocked_periods: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        current = []
        for p in base_periods:
            s = self._time_to_minutes(p.get("start"))
            e = self._time_to_minutes(p.get("end"))
            if s is not None and e is not None and s < e:
                current.append((s, e))

        for blocked in blocked_periods:
            b_start = self._time_to_minutes(blocked.get("start"))
            b_end = self._time_to_minutes(blocked.get("end"))
            if b_start is None or b_end is None or b_start >= b_end:
                continue

            next_segments = []
            for seg_start, seg_end in current:
                if b_end <= seg_start or seg_end <= b_start:
                    next_segments.append((seg_start, seg_end))
                    continue
                if seg_start < b_start:
                    next_segments.append((seg_start, b_start))
                if b_end < seg_end:
                    next_segments.append((b_end, seg_end))
            current = next_segments

        return [{"start": self._minutes_to_time(s), "end": self._minutes_to_time(e)} for s, e in current if s < e]

    def _get_staff_record(self, staff_name: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        for staff_id, staff_data in self.staff_data.items():
            if staff_data.get("name") == staff_name:
                return staff_id, staff_data
        return None

    def _staff_can_handle_service(self, staff_data: Dict[str, Any], service_id: Optional[str]) -> bool:
        if not service_id:
            return True
        service_ids = staff_data.get("service_ids")
        if isinstance(service_ids, list) and service_ids:
            return service_id in service_ids
        return True

    def _get_active_staff_records(self, service_id: Optional[str] = None) -> List[Tuple[str, Dict[str, Any]]]:
        out: List[Tuple[str, Dict[str, Any]]] = []
        for staff_id, staff_data in self.staff_data.items():
            if not isinstance(staff_data, dict):
                continue
            if not staff_data.get("is_active", True):
                continue
            if not self._staff_can_handle_service(staff_data, service_id):
                continue
            out.append((staff_id, staff_data))

        out.sort(key=lambda item: (item[1].get("order", 999), item[1].get("name", item[0])))
        return out

    def _get_service_duration_minutes(self, service_identifier: str) -> int:
        if not service_identifier:
            return 60

        ident = str(service_identifier).strip()

        for _key, data in self.services.items():
            if isinstance(data, dict) and data.get("id") and str(data.get("id")).lower() == ident.lower():
                return data.get("duration", 60)

        service_data = self.services.get(ident)
        if service_data and isinstance(service_data, dict):
            return service_data.get("duration", 60)

        for _key, data in self.services.items():
            if isinstance(data, dict) and data.get("name") == ident:
                return data.get("duration", 60)

        return 60

    def _get_store_periods_for_date(self, target_date: date) -> List[Dict[str, str]]:
        try:
            return get_hours_for_date(target_date) or []
        except Exception:
            return []

    def get_staff_attendance_for_date(self, staff_name: str, target_date: date) -> Dict[str, Any]:
        self._reload_config()

        record = self._get_staff_record(staff_name)
        if not record:
            return {"is_working": False, "reason": "staff_not_found"}

        staff_id, staff_data = record
        attendance = staff_data.get("attendance")

        if not isinstance(attendance, dict):
            return {"is_working": True, "fallback_to_store_hours": True, "source": "staff_attendance_missing"}

        weekday_key = self._weekday_key(target_date)
        day_rule = attendance.get(weekday_key)

        if day_rule is None:
            return {"is_working": True, "fallback_to_store_hours": True, "source": "weekday_attendance_missing"}

        if not isinstance(day_rule, dict):
            logging.error("Invalid attendance data for staff=%s weekday=%s: %s", staff_id, weekday_key, day_rule)
            return {"is_working": False, "invalid": True, "reason": "invalid_attendance_format"}

        is_working = day_rule.get("is_working")
        if is_working is False:
            return {"is_working": False, "source": "attendance_day_off"}

        start = day_rule.get("start")
        end = day_rule.get("end")

        if is_working is True:
            start = self._normalize_time_format(start) if start else None
            end = self._normalize_time_format(end) if end else None
            if not start or not end:
                logging.error(
                    "Attendance invalid for staff=%s date=%s is_working=true but start/end missing. day_rule=%s",
                    staff_id,
                    target_date.isoformat(),
                    day_rule,
                )
                return {"is_working": False, "invalid": True, "reason": "missing_start_or_end"}
            if self._time_to_minutes(start) >= self._time_to_minutes(end):
                logging.error(
                    "Attendance invalid for staff=%s date=%s start >= end. day_rule=%s",
                    staff_id,
                    target_date.isoformat(),
                    day_rule,
                )
                return {"is_working": False, "invalid": True, "reason": "invalid_range"}

            return {"is_working": True, "start": start, "end": end, "source": "staff_attendance"}

        # 明示的な is_working が無い / null の場合は既存挙動維持
        return {"is_working": True, "fallback_to_store_hours": True, "source": "attendance_undefined"}

    def get_effective_working_periods(self, staff_name: str, target_date: date) -> List[Dict[str, str]]:
        if is_closed_date(target_date):
            return []

        store_periods = self._get_store_periods_for_date(target_date)
        if not store_periods:
            return []

        attendance = self.get_staff_attendance_for_date(staff_name, target_date)
        if not attendance.get("is_working"):
            return []

        if attendance.get("fallback_to_store_hours"):
            return store_periods

        staff_period = [{"start": attendance["start"], "end": attendance["end"]}]
        return self._intersect_periods(store_periods, staff_period)

    def _event_to_local_period(self, event: Dict[str, Any], target_date: date) -> Optional[Dict[str, str]]:
        try:
            start_raw = event.get("start", {}).get("dateTime", event.get("start", {}).get("date", ""))
            end_raw = event.get("end", {}).get("dateTime", event.get("end", {}).get("date", ""))
            if not start_raw or not end_raw:
                return None

            tz = pytz.timezone(self.timezone)

            if "T" in start_raw:
                event_start = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone(tz)
            else:
                event_start = tz.localize(datetime.strptime(start_raw, "%Y-%m-%d"))

            if "T" in end_raw:
                event_end = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).astimezone(tz)
            else:
                event_end = tz.localize(datetime.strptime(end_raw, "%Y-%m-%d"))

            day_start = tz.localize(datetime.combine(target_date, datetime.min.time()))
            day_end = day_start + timedelta(days=1)

            clipped_start = max(event_start, day_start)
            clipped_end = min(event_end, day_end)

            if clipped_start >= clipped_end:
                return None

            return {
                "start": clipped_start.strftime("%H:%M"),
                "end": clipped_end.strftime("%H:%M"),
            }
        except Exception:
            return None

    def _find_available_periods(self, target_date: date, base_periods: List[Dict[str, str]], events: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        blocked_periods: List[Dict[str, str]] = []
        for event in events:
            period = self._event_to_local_period(event, target_date)
            if period:
                blocked_periods.append(period)

        blocked_periods = self._merge_periods(blocked_periods)
        return self._subtract_periods(base_periods, blocked_periods)

    def get_events_for_date(self, date_str: str, staff_name: str = None) -> List[Dict]:
        if self._is_no_preference(staff_name):
            return []

        calendar_id = self._get_staff_calendar_id(staff_name)

        if not self.service or not calendar_id:
            return []

        try:
            tz = pytz.timezone(self.timezone)
            start_date = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            start_date_aware = tz.localize(start_date)
            end_date_aware = start_date_aware + timedelta(days=1)

            events_result = self.service.events().list(
                calendarId=calendar_id,
                timeMin=start_date_aware.isoformat(),
                timeMax=end_date_aware.isoformat(),
                singleEvents=True,
                orderBy="startTime",
            ).execute()

            return events_result.get("items", [])
        except Exception as e:
            print(f"Failed to get events for date {date_str}: {e}")
            return []

    def _build_slots_from_periods(self, target_date: date, periods: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        return [
            {
                "date": target_date.strftime("%Y-%m-%d"),
                "time": period["start"],
                "end_time": period["end"],
                "available": True,
            }
            for period in periods
        ]

    def get_available_slots(
        self,
        start_date: datetime,
        end_date: datetime,
        staff_name: str = None,
        service_id: str = None,
        exclude_reservation_id: str = None,
    ) -> List[Dict[str, Any]]:
        self._reload_config()

        slots: List[Dict[str, Any]] = []
        current_date = start_date.date()
        end_date_only = end_date.date()

        while current_date <= end_date_only:
            if is_closed_date(current_date):
                current_date += timedelta(days=1)
                continue

            if self._is_no_preference(staff_name):
                all_periods: List[Dict[str, str]] = []
                for _, staff_data in self._get_active_staff_records(service_id):
                    staff_name_each = staff_data.get("name")
                    base_periods = self.get_effective_working_periods(staff_name_each, current_date)
                    if not base_periods:
                        continue
                    events = self.get_events_for_date(current_date.strftime("%Y-%m-%d"), staff_name_each)
                    if exclude_reservation_id:
                        events = [
                            e for e in events
                            if f"予約ID: {exclude_reservation_id}" not in e.get("description", "")
                        ]
                    available = self._find_available_periods(current_date, base_periods, events)
                    all_periods.extend(available)

                merged = self._merge_periods(all_periods)
                slots.extend(self._build_slots_from_periods(current_date, merged))
            else:
                base_periods = self.get_effective_working_periods(staff_name, current_date)
                if base_periods:
                    events = self.get_events_for_date(current_date.strftime("%Y-%m-%d"), staff_name)
                    if exclude_reservation_id:
                        events = [
                            e for e in events
                            if f"予約ID: {exclude_reservation_id}" not in e.get("description", "")
                        ]
                    available_periods = self._find_available_periods(current_date, base_periods, events)
                    slots.extend(self._build_slots_from_periods(current_date, available_periods))

            current_date += timedelta(days=1)

        return slots

    def get_available_slots_for_modification(
        self,
        date_str: str,
        exclude_reservation_id: str = None,
        staff_name: str = None,
        service_id: str = None,
    ) -> List[Dict]:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        return self.get_available_slots(
            target_date,
            target_date,
            staff_name=staff_name,
            service_id=service_id,
            exclude_reservation_id=exclude_reservation_id,
        )

    def get_reservation_by_id(self, reservation_id: str, staff_name: str = None) -> Optional[Dict]:
        if not self.service:
            return None

        try:
            target_staff_names: List[str] = []

            if not self._is_no_preference(staff_name):
                target_staff_names = [staff_name]
            else:
                target_staff_names = [
                    data.get("name")
                    for _, data in self._get_active_staff_records()
                    if data.get("name")
                ]

            for staff_name_item in target_staff_names:
                staff_calendar_id = self._get_staff_calendar_id(staff_name_item)
                if not staff_calendar_id:
                    continue

                try:
                    events_result = self.service.events().list(
                        calendarId=staff_calendar_id,
                        timeMin=datetime.now().isoformat() + "Z",
                        maxResults=100,
                        singleEvents=True,
                        orderBy="startTime",
                    ).execute()

                    for event in events_result.get("items", []):
                        if reservation_id in event.get("description", ""):
                            return event
                except Exception:
                    continue

            return None

        except Exception as e:
            print(f"Failed to get reservation by ID {reservation_id}: {e}")
            return None

    def cancel_reservation_by_id(self, reservation_id: str, staff_name: str = None) -> bool:
        try:
            event = self.get_reservation_by_id(reservation_id, staff_name)
            if not event:
                print(f"Reservation {reservation_id} not found in calendar")
                return False

            if self._is_no_preference(staff_name):
                summary = event.get("summary", "")
                try:
                    import re
                    m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                    if m:
                        staff_name = m.group(3)
                except Exception:
                    pass

            staff_calendar_id = self._get_staff_calendar_id(staff_name)
            if not staff_calendar_id:
                return False

            self.service.events().delete(
                calendarId=staff_calendar_id,
                eventId=event["id"],
            ).execute()

            return True

        except Exception as e:
            print(f"Failed to cancel reservation {reservation_id}: {e}")
            return False

    def _get_staff_calendar_id(self, staff_name: str) -> Optional[str]:
        if self._is_no_preference(staff_name):
            return None

        record = self._get_staff_record(staff_name)
        if not record:
            return None

        _, staff_data = record
        return staff_data.get("calendar_id") or self.calendar_id

    def check_staff_availability_for_time(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
        exclude_reservation_id: str = None
    ) -> bool:
        if self._is_no_preference(staff_name):
            return False

        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            effective_periods = self.get_effective_working_periods(staff_name, target_date)
            if not effective_periods:
                return False

            start_min = self._time_to_minutes(start_time)
            end_min = self._time_to_minutes(end_time)
            if start_min is None or end_min is None or start_min >= end_min:
                return False

            within_attendance = False
            for period in effective_periods:
                period_start = self._time_to_minutes(period["start"])
                period_end = self._time_to_minutes(period["end"])
                if period_start <= start_min and end_min <= period_end:
                    within_attendance = True
                    break

            if not within_attendance:
                return False

            staff_events = self.get_events_for_date(date_str, staff_name)

            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")

            for event in staff_events:
                if exclude_reservation_id:
                    description = event.get("description", "")
                    if f"予約ID: {exclude_reservation_id}" in description:
                        continue

                event_start_str = event.get("start", {}).get("dateTime", "")
                event_end_str = event.get("end", {}).get("dateTime", "")

                if event_start_str and event_end_str:
                    event_start = datetime.fromisoformat(event_start_str.replace("Z", "+00:00"))
                    event_end = datetime.fromisoformat(event_end_str.replace("Z", "+00:00"))

                    tz = pytz.timezone(self.timezone)
                    event_start = event_start.astimezone(tz).replace(tzinfo=None)
                    event_end = event_end.astimezone(tz).replace(tzinfo=None)

                    if start_datetime < event_end and end_datetime > event_start:
                        return False

            return True

        except Exception as e:
            print(f"Error checking staff availability: {e}")
            return False

    def find_assignable_staff(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        service_id: Optional[str] = None,
        exclude_reservation_id: str = None,
    ) -> Optional[str]:
        for _, staff_data in self._get_active_staff_records(service_id):
            staff_name = staff_data.get("name")
            if not staff_name:
                continue
            if self.check_staff_availability_for_time(
                date_str=date_str,
                start_time=start_time,
                end_time=end_time,
                staff_name=staff_name,
                exclude_reservation_id=exclude_reservation_id,
            ):
                return staff_name
        return None

    def check_user_time_conflict(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        user_id: str,
        exclude_reservation_id: str = None,
        staff_name: str = None
    ) -> bool:
        try:
            all_events = []

            for _, staff_data in self._get_active_staff_records():
                staff_name_check = staff_data.get("name")
                if staff_name_check:
                    all_events.extend(self.get_events_for_date(date_str, staff_name_check))

            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")

            for event in all_events:
                if exclude_reservation_id:
                    description = event.get("description", "")
                    if f"予約ID: {exclude_reservation_id}" in description:
                        continue

                if self._is_user_reservation(event, user_id):
                    event_start_str = event.get("start", {}).get("dateTime", "")
                    event_end_str = event.get("end", {}).get("dateTime", "")

                    if event_start_str and event_end_str:
                        event_start = datetime.fromisoformat(event_start_str.replace("Z", "+00:00"))
                        event_end = datetime.fromisoformat(event_end_str.replace("Z", "+00:00"))

                        tz = pytz.timezone(self.timezone)
                        event_start = event_start.astimezone(tz).replace(tzinfo=None)
                        event_end = event_end.astimezone(tz).replace(tzinfo=None)

                        if start_datetime < event_end and end_datetime > event_start:
                            return True

            return False

        except Exception as e:
            print(f"Error checking user time conflict: {e}")
            return True

    def _is_user_reservation(self, event: Dict, user_id: str) -> bool:
        try:
            description = event.get("description", "")
            if "User ID:" in description:
                event_user_id = description.split("User ID:")[1].split("\n")[0].strip()
                return event_user_id == user_id
            return False
        except Exception:
            return False

    def create_reservation_event(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        if not self.service:
            print("Google Calendar not configured, skipping event creation")
            return False

        try:
            self._reload_config()

            date_str = reservation_data["date"]
            service = reservation_data["service"]
            staff = reservation_data.get("staff")

            if not staff or self._is_no_preference(staff):
                logging.error(f"Staff name missing or unresolved in reservation_data: {reservation_data}")
                return False

            staff_calendar_id = self._get_staff_calendar_id(staff)
            if not staff_calendar_id:
                logging.error(f"Staff calendar ID not found for staff '{staff}'")
                return False

            if "start_time" in reservation_data and "end_time" in reservation_data:
                start_time_str = reservation_data["start_time"]
                end_time_str = reservation_data["end_time"]
                start_datetime = datetime.strptime(f"{date_str} {start_time_str}", "%Y-%m-%d %H:%M")
                end_datetime = datetime.strptime(f"{date_str} {end_time_str}", "%Y-%m-%d %H:%M")
            else:
                time_str = reservation_data["time"]
                start_datetime = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
                duration_minutes = self._get_service_duration_minutes(
                    reservation_data.get("service_id") or reservation_data.get("service")
                )
                end_datetime = start_datetime + timedelta(minutes=duration_minutes)

            if not self.check_staff_availability_for_time(
                date_str=date_str,
                start_time=start_datetime.strftime("%H:%M"),
                end_time=end_datetime.strftime("%H:%M"),
                staff_name=staff,
            ):
                logging.error("Attendance/calendar validation failed before event insert. reservation=%s", reservation_data)
                return False

            duration_minutes = int((end_datetime - start_datetime).total_seconds() / 60)

            tokyo_tz = pytz.timezone(self.timezone)
            if start_datetime.tzinfo is None:
                start_datetime = tokyo_tz.localize(start_datetime)
            else:
                start_datetime = start_datetime.astimezone(tokyo_tz)

            if end_datetime.tzinfo is None:
                end_datetime = tokyo_tz.localize(end_datetime)
            else:
                end_datetime = end_datetime.astimezone(tokyo_tz)

            start_iso = start_datetime.isoformat()
            end_iso = end_datetime.isoformat()

            reservation_id = reservation_data.get("reservation_id", self.generate_reservation_id(date_str))
            user_id = reservation_data.get("user_id", "")

            description_lines = [
                f"予約ID: {reservation_id}",
                f"サービス: {service}",
                f"担当者: {staff}",
                f"お客様: {client_name}",
                f"所要時間: {duration_minutes}分",
            ]
            if user_id:
                description_lines.append(f"User ID: {user_id}")
            description_lines.append("予約元: LINE Bot")

            event = {
                "summary": f"[予約] {service} - {client_name} ({staff})",
                "description": "\n".join(description_lines),
                "start": {
                    "dateTime": start_iso,
                    "timeZone": self.timezone,
                },
                "end": {
                    "dateTime": end_iso,
                    "timeZone": self.timezone,
                },
            }

            self.service.events().insert(
                calendarId=staff_calendar_id,
                body=event,
            ).execute()

            return True

        except HttpError as e:
            logging.error(f"Google Calendar API error: {e}", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"Failed to create calendar event: {e}", exc_info=True)
            return False
