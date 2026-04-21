"""
Google Calendar integration for salon reservations
- Staff attendance aware version
"""
import os
import json
import logging
from datetime import datetime, timedelta, date, time
from typing import Dict, Any, Optional, List, Tuple

import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

from api.business_hours import get_hours_for_date, is_closed_date, get_timezone


class GoogleCalendarHelper:
    WEEKDAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

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

    def _reload_config_data(self) -> None:
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

    def _time_str_to_time(self, time_str: str) -> Optional[time]:
        normalized = self._normalize_time_format(time_str)
        if not normalized:
            return None
        return datetime.strptime(normalized, "%H:%M").time()

    def _time_str_to_minutes(self, time_str: str) -> Optional[int]:
        normalized = self._normalize_time_format(time_str)
        if not normalized:
            return None
        hour, minute = map(int, normalized.split(":"))
        return hour * 60 + minute

    def _minutes_to_time_str(self, total_minutes: int) -> str:
        hour, minute = divmod(total_minutes, 60)
        return f"{hour:02d}:{minute:02d}"

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

        import time as time_module
        counter = int(time_module.time() * 1000) % 10000
        return f"RES-{date_part}-{counter:04d}"

    def _get_service_duration_minutes(self, service_identifier: str) -> int:
        if not service_identifier:
            return 60

        ident = str(service_identifier).strip()

        for _key, data in self.services.items():
            if isinstance(data, dict) and data.get("id") and str(data.get("id")).lower() == ident.lower():
                return int(data.get("duration", 60))

        service_data = self.services.get(ident)
        if service_data and isinstance(service_data, dict):
            return int(service_data.get("duration", 60))

        for _key, data in self.services.items():
            if isinstance(data, dict) and data.get("name") == ident:
                return int(data.get("duration", 60))

        return 60

    def _get_staff_record_by_name(self, staff_name: str) -> Optional[Dict[str, Any]]:
        if not staff_name:
            return None

        for _staff_id, staff_data in self.staff_data.items():
            if not isinstance(staff_data, dict):
                continue
            if staff_data.get("name") == staff_name:
                return staff_data

        return None

    def _get_weekday_key(self, target_date: date) -> str:
        return self.WEEKDAY_KEYS[target_date.weekday()]

    def _intersect_periods(
        self,
        store_periods: List[Dict[str, str]],
        staff_start: str,
        staff_end: str,
    ) -> List[Dict[str, str]]:
        results: List[Dict[str, str]] = []

        staff_start_min = self._time_str_to_minutes(staff_start)
        staff_end_min = self._time_str_to_minutes(staff_end)

        if staff_start_min is None or staff_end_min is None:
            return results

        for period in store_periods:
            p_start = period.get("start")
            p_end = period.get("end")

            p_start_min = self._time_str_to_minutes(p_start) if p_start else None
            p_end_min = self._time_str_to_minutes(p_end) if p_end else None

            if p_start_min is None or p_end_min is None:
                continue

            start_min = max(p_start_min, staff_start_min)
            end_min = min(p_end_min, staff_end_min)

            if start_min < end_min:
                results.append(
                    {
                        "start": self._minutes_to_time_str(start_min),
                        "end": self._minutes_to_time_str(end_min),
                    }
                )

        return results

    def _get_effective_business_periods_for_staff(
        self,
        target_date: date,
        staff_name: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        Returns effective bookable periods for the given date and staff:
        - store business hours
        - intersected with staff attendance when available

        Rules:
        - no staff_name / 未指定 => store business hours only
        - attendance missing => fallback to store business hours
        - weekday attendance missing => fallback to store business hours
        - is_working=false => no periods
        - is_working=true but start/end missing => invalid => no periods
        """
        store_periods = get_hours_for_date(target_date) or []
        if not store_periods:
            return []

        if not staff_name or staff_name == "未指定":
            return store_periods

        staff_record = self._get_staff_record_by_name(staff_name)
        if not staff_record:
            # スタッフ不明時は既存互換のため店舗営業時間にフォールバック
            return store_periods

        attendance = staff_record.get("attendance")
        if not isinstance(attendance, dict) or not attendance:
            return store_periods

        weekday_key = self._get_weekday_key(target_date)
        day_attendance = attendance.get(weekday_key)

        if day_attendance is None:
            return store_periods

        if not isinstance(day_attendance, dict):
            logging.error(
                f"[attendance] Invalid attendance data for staff={staff_name}, "
                f"date={target_date}, weekday={weekday_key}: {day_attendance}"
            )
            return []

        is_working = day_attendance.get("is_working")

        if is_working is False:
            return []

        if is_working is True:
            staff_start = day_attendance.get("start")
            staff_end = day_attendance.get("end")

            if not staff_start or not staff_end:
                logging.error(
                    f"[attendance] Missing start/end for working day. "
                    f"staff={staff_name}, date={target_date}, weekday={weekday_key}, data={day_attendance}"
                )
                return []

            intersected = self._intersect_periods(store_periods, staff_start, staff_end)
            return intersected

        # is_working が未指定なら後方互換として店舗営業時間を適用
        return store_periods

    def _is_within_effective_business_periods(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: Optional[str] = None,
    ) -> bool:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            start_min = self._time_str_to_minutes(start_time)
            end_min = self._time_str_to_minutes(end_time)

            if start_min is None or end_min is None:
                return False

            effective_periods = self._get_effective_business_periods_for_staff(target_date, staff_name)
            if not effective_periods:
                return False

            for period in effective_periods:
                p_start_min = self._time_str_to_minutes(period["start"])
                p_end_min = self._time_str_to_minutes(period["end"])
                if p_start_min is None or p_end_min is None:
                    continue

                if start_min >= p_start_min and end_min <= p_end_min:
                    return True

            return False
        except Exception as e:
            logging.error(f"Failed to validate effective business periods: {e}", exc_info=True)
            return False

    def check_staff_attendance_for_time(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
    ) -> bool:
        return self._is_within_effective_business_periods(date_str, start_time, end_time, staff_name)

    def create_reservation_event(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        if not self.service:
            print("Google Calendar not configured, skipping event creation")
            return False

        try:
            self._reload_config_data()

            date_str = reservation_data["date"]
            service_name = reservation_data["service"]
            staff = reservation_data.get("staff")

            if not staff:
                logging.error(f"Staff name missing in reservation_data: {reservation_data}")
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

            start_time_str = start_datetime.strftime("%H:%M")
            end_time_str = end_datetime.strftime("%H:%M")
            duration_minutes = int((end_datetime - start_datetime).total_seconds() / 60)

            # 勤怠・営業時間の最終防御
            if not self._is_within_effective_business_periods(date_str, start_time_str, end_time_str, staff):
                logging.error(
                    f"Reservation rejected by attendance/business-hours check: "
                    f"staff={staff}, date={date_str}, start={start_time_str}, end={end_time_str}"
                )
                return False

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

            event = {
                "summary": f"[予約] {service_name} - {client_name} ({staff})",
                "description": f"""
予約ID: {reservation_id}
サービス: {service_name}
担当者: {staff}
お客様: {client_name}
所要時間: {duration_minutes}分
予約元: LINE Bot
                """.strip(),
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

    def get_available_slots(self, start_date: datetime, end_date: datetime, staff_name: str = None) -> list:
        self._reload_config_data()
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id

        if not self.service or not calendar_id:
            print("Google Calendar not configured, using fallback slots")
            return self._generate_fallback_slots(start_date, end_date, staff_name)

        try:
            tz = pytz.timezone(self.timezone)

            start_date_aware = start_date
            end_date_aware = end_date

            if start_date_aware.tzinfo is None:
                start_date_aware = tz.localize(start_date_aware)
            else:
                start_date_aware = start_date_aware.astimezone(tz)

            if end_date_aware.tzinfo is None:
                end_date_aware = tz.localize(end_date_aware)
            else:
                end_date_aware = end_date_aware.astimezone(tz)

            events_result = self.service.events().list(
                calendarId=calendar_id,
                timeMin=start_date_aware.isoformat(),
                timeMax=end_date_aware.isoformat(),
                singleEvents=True,
                orderBy="startTime",
            ).execute()

            events = events_result.get("items", [])
            return self._generate_all_slots(start_date, end_date, events, staff_name)

        except Exception as e:
            print(f"Failed to get available slots from Google Calendar: {e}")
            return self._generate_fallback_slots(start_date, end_date, staff_name)

    def _parse_event_datetime(self, event_time_obj: Dict[str, Any], default_is_end: bool = False) -> Optional[datetime]:
        """
        Supports both:
        - {'dateTime': '...'}
        - {'date': 'YYYY-MM-DD'}  # all-day event
        """
        try:
            tz = pytz.timezone(self.timezone)

            if "dateTime" in event_time_obj and event_time_obj["dateTime"]:
                dt = datetime.fromisoformat(event_time_obj["dateTime"].replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    return tz.localize(dt)
                return dt.astimezone(tz)

            if "date" in event_time_obj and event_time_obj["date"]:
                d = datetime.strptime(event_time_obj["date"], "%Y-%m-%d")
                if default_is_end:
                    d = d.replace(hour=0, minute=0)  # Google all-day end is exclusive next day
                else:
                    d = d.replace(hour=0, minute=0)
                return tz.localize(d)

            return None
        except Exception:
            return None

    def _generate_all_slots(
        self,
        start_date: datetime,
        end_date: datetime,
        events: list = None,
        staff_name: str = None,
    ) -> list:
        slots = []
        current_date = start_date.date()
        end_date_only = end_date.date()

        while current_date <= end_date_only:
            if is_closed_date(current_date):
                current_date += timedelta(days=1)
                continue

            effective_periods = self._get_effective_business_periods_for_staff(current_date, staff_name)
            if not effective_periods:
                current_date += timedelta(days=1)
                continue

            date_events = []
            if events:
                for event in events:
                    event_start = self._parse_event_datetime(event.get("start", {}), default_is_end=False)
                    if not event_start:
                        continue
                    if event_start.date() == current_date:
                        date_events.append(event)

            date_events.sort(
                key=lambda e: self._parse_event_datetime(e.get("start", {}), default_is_end=False) or datetime.min
            )

            for business_period in effective_periods:
                available_periods = self._find_available_periods(current_date, business_period, date_events)
                for period in available_periods:
                    slots.append({
                        "date": current_date.strftime("%Y-%m-%d"),
                        "time": period["start"],
                        "end_time": period["end"],
                        "available": True,
                    })

            current_date += timedelta(days=1)

        return slots

    def _find_available_periods(self, target_date, business_period, events):
        tz = pytz.timezone(self.timezone)
        start_str = business_period.get("start", "00:00")
        end_str = business_period.get("end", "23:59")

        business_start = tz.localize(datetime.combine(target_date, datetime.strptime(start_str, "%H:%M").time()))
        business_end = tz.localize(datetime.combine(target_date, datetime.strptime(end_str, "%H:%M").time()))

        available_periods = []
        cursor = business_start

        sorted_events: List[Tuple[datetime, datetime]] = []

        for event in events:
            event_start = self._parse_event_datetime(event.get("start", {}), default_is_end=False)
            event_end = self._parse_event_datetime(event.get("end", {}), default_is_end=True)

            if not event_start or not event_end:
                continue

            sorted_events.append((event_start, event_end))

        sorted_events.sort(key=lambda x: x[0])

        for event_start, event_end in sorted_events:
            if event_end <= cursor:
                continue

            if event_start >= business_end:
                break

            if event_start > cursor:
                available_periods.append({
                    "start": cursor.strftime("%H:%M"),
                    "end": min(event_start, business_end).strftime("%H:%M"),
                })

            cursor = max(cursor, event_end)
            if cursor >= business_end:
                break

        if cursor < business_end:
            available_periods.append({
                "start": cursor.strftime("%H:%M"),
                "end": business_end.strftime("%H:%M"),
            })

        return available_periods

    def _generate_fallback_slots(self, start_date: datetime, end_date: datetime, staff_name: str = None) -> list:
        try:
            return self._generate_all_slots(start_date, end_date, None, staff_name)
        except Exception:
            fallback = []
            d = start_date.date()
            if not is_closed_date(d):
                effective_periods = self._get_effective_business_periods_for_staff(d, staff_name)
                for slot in effective_periods:
                    fallback.append({
                        "date": d.strftime("%Y-%m-%d"),
                        "time": slot["start"],
                        "end_time": slot["end"],
                        "available": True,
                    })
            return fallback if fallback else []

    def get_events_for_date(self, date_str: str, staff_name: str = None) -> List[Dict]:
        self._reload_config_data()
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id

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

    def get_available_slots_for_modification(
        self,
        date_str: str,
        exclude_reservation_id: str = None,
        staff_name: str = None
    ) -> List[Dict]:
        self._reload_config_data()
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id

        if not self.service or not calendar_id:
            return self._generate_fallback_slots(
                datetime.strptime(date_str, "%Y-%m-%d"),
                datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1),
                staff_name,
            )

        try:
            all_events = self.get_events_for_date(date_str, staff_name)

            other_events = []

            if exclude_reservation_id:
                for e in all_events:
                    description = e.get("description", "")
                    if f"予約ID: {exclude_reservation_id}" in description:
                        continue
                    other_events.append(e)
            else:
                other_events = all_events

            start_date = datetime.strptime(date_str, "%Y-%m-%d")
            end_date = start_date

            return self._generate_all_slots(start_date, end_date, other_events, staff_name)

        except Exception as e:
            print(f"Failed to get available slots for modification: {e}")
            return []

    def get_reservation_by_id(self, reservation_id: str, staff_name: str = None) -> Optional[Dict]:
        if not self.service:
            return None

        try:
            if staff_name:
                calendar_id = self._get_staff_calendar_id(staff_name)
                if not calendar_id:
                    return None

                events_result = self.service.events().list(
                    calendarId=calendar_id,
                    timeMin=datetime.now().isoformat() + "Z",
                    maxResults=100,
                    singleEvents=True,
                    orderBy="startTime",
                ).execute()

                for event in events_result.get("items", []):
                    if reservation_id in event.get("description", ""):
                        return event
                return None

            if self.calendar_id:
                events_result = self.service.events().list(
                    calendarId=self.calendar_id,
                    timeMin=datetime.now().isoformat() + "Z",
                    maxResults=100,
                    singleEvents=True,
                    orderBy="startTime",
                ).execute()

                for event in events_result.get("items", []):
                    if reservation_id in event.get("description", ""):
                        return event

            for _staff_id, staff_data in self.staff_data.items():
                staff_calendar_id = self._get_staff_calendar_id(staff_data.get("name"))
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

            if not staff_name:
                summary = event.get("summary", "")
                try:
                    import re
                    m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                    if m:
                        staff_name = m.group(3)
                except Exception:
                    pass

            staff_calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
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
        if not staff_name or staff_name == "未指定":
            return self.calendar_id

        for _staff_id, staff_data in self.staff_data.items():
            if staff_data.get("name") == staff_name:
                return staff_data.get("calendar_id") or self.calendar_id

        return self.calendar_id

    def get_short_calendar_url(self, staff_name: str = None) -> str:
        """
        互換用メソッド。
        既存コードで get_short_calendar_url() が呼ばれている場合でも
        落ちないように、まずは通常のGoogleカレンダーURLを返す。
        """
        try:
            staff_calendar_id = self._get_staff_calendar_id(staff_name)

            if staff_calendar_id:
                return (
                    f"https://calendar.google.com/calendar/embed"
                    f"?src={staff_calendar_id}&ctz=Asia%2FTokyo"
                )

            return "https://calendar.google.com/calendar"

        except Exception as e:
            logging.error(f"Failed to generate calendar url: {e}", exc_info=True)
            return "https://calendar.google.com/calendar"

    def check_staff_availability_for_time(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
        exclude_reservation_id: str = None
    ) -> bool:
        try:
            # 勤怠・営業時間チェック
            if not self._is_within_effective_business_periods(date_str, start_time, end_time, staff_name):
                return False

            staff_events = self.get_events_for_date(date_str, staff_name)

            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")

            for event in staff_events:
                if exclude_reservation_id:
                    description = event.get("description", "")
                    if f"予約ID: {exclude_reservation_id}" in description:
                        continue

                event_start = self._parse_event_datetime(event.get("start", {}), default_is_end=False)
                event_end = self._parse_event_datetime(event.get("end", {}), default_is_end=True)

                if event_start and event_end:
                    tz = pytz.timezone(self.timezone)
                    event_start = event_start.astimezone(tz).replace(tzinfo=None)
                    event_end = event_end.astimezone(tz).replace(tzinfo=None)

                    if start_datetime < event_end and end_datetime > event_start:
                        return False

            return True

        except Exception as e:
            print(f"Error checking staff availability: {e}")
            return False

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

            if self.calendar_id:
                all_events.extend(self.get_events_for_date(date_str, None))

            for _staff_id, staff_data in self.staff_data.items():
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
                    event_start = self._parse_event_datetime(event.get("start", {}), default_is_end=False)
                    event_end = self._parse_event_datetime(event.get("end", {}), default_is_end=True)

                    if event_start and event_end:
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
