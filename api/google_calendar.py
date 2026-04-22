"""
Google Calendar integration for salon reservations
- Staff attendance aware version
- No-preference staff assignment supported
- Load-balanced free-staff assignment version
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
from api.staff_attendance import get_staff_attendance_for_date, get_staff_effective_periods_for_date, is_staff_working_for_time


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
        self._config_mtime = self._get_config_mtime()

        self._events_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._all_events_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._slots_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._assignable_staff_cache: Dict[str, Optional[str]] = {}
        self._free_staff_assignment_cache: Dict[str, Optional[Dict[str, Any]]] = {}

        try:
            self._authenticate()
        except Exception as e:
            print(f"Failed to initialize Google Calendar: {e}")
            self.service = None

    def _config_path(self) -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, "data", "config.json")

    def _get_config_mtime(self) -> Optional[float]:
        try:
            return os.path.getmtime(self._config_path())
        except Exception:
            return None

    def _load_config_data(self) -> Dict[str, Any]:
        try:
            with open(self._config_path(), "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load config data: {e}")
            return {}

    def _clear_runtime_caches(self) -> None:
        self._events_cache.clear()
        self._all_events_cache.clear()
        self._slots_cache.clear()
        self._assignable_staff_cache.clear()
        self._free_staff_assignment_cache.clear()

    def _reload_config_data(self, force: bool = False) -> None:
        current_mtime = self._get_config_mtime()
        if not force and current_mtime is not None and self._config_mtime == current_mtime:
            return

        self.config_data = self._load_config_data()
        self.timezone = self.config_data.get("salon", {}).get("timezone", get_timezone())
        self.staff_data = self.config_data.get("staff", {})
        self.services = self.config_data.get("services", {})
        self._config_mtime = current_mtime
        self._clear_runtime_caches()

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

    def _supports_all_services(self, staff_data: Dict[str, Any], service_ids: Optional[List[str]] = None) -> bool:
        normalized_ids = [sid for sid in (service_ids or []) if sid]
        if not normalized_ids:
            return True
        configured = staff_data.get("service_ids")
        if isinstance(configured, list) and configured:
            return all(service_id in configured for service_id in normalized_ids)
        return True

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
        store_periods = get_hours_for_date(target_date) or []
        if not store_periods:
            return []

        if not staff_name or staff_name in {"未指定", "指名なし", "おまかせ", "free"}:
            return store_periods

        staff_record = self._get_staff_record_by_name(staff_name)
        return get_staff_effective_periods_for_date(
            staff_record=staff_record,
            target_date=target_date,
            fallback_to_store_hours=True,
        )

    def get_staff_attendance_for_date(self, staff_name: str, target_date: date) -> Dict[str, Any]:
        staff_record = self._get_staff_record_by_name(staff_name)
        return get_staff_attendance_for_date(
            staff_record=staff_record,
            target_date=target_date,
            fallback_to_store_hours=True,
        )

    def _is_within_effective_business_periods(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: Optional[str] = None,
    ) -> bool:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if not staff_name or staff_name in {"未指定", "指名なし", "おまかせ", "free"}:
                effective_periods = self._get_effective_business_periods_for_staff(target_date, None)
                start_min = self._time_str_to_minutes(start_time)
                end_min = self._time_str_to_minutes(end_time)
                if start_min is None or end_min is None:
                    return False
                for period in effective_periods:
                    p_start_min = self._time_str_to_minutes(period["start"])
                    p_end_min = self._time_str_to_minutes(period["end"])
                    if p_start_min is None or p_end_min is None:
                        continue
                    if start_min >= p_start_min and end_min <= p_end_min:
                        return True
                return False

            staff_record = self._get_staff_record_by_name(staff_name)
            return is_staff_working_for_time(
                staff_record=staff_record,
                target_date=target_date,
                start_time=start_time,
                end_time=end_time,
                fallback_to_store_hours=True,
            )
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

    def check_staff_attendance_detail_for_time(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
    ) -> Dict[str, Any]:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            detail = self.get_staff_attendance_for_date(staff_name, target_date)
            detail = dict(detail)
            detail["fits_time"] = self._is_within_effective_business_periods(date_str, start_time, end_time, staff_name)
            return detail
        except Exception as e:
            logging.error(f"Failed to get staff attendance detail: {e}", exc_info=True)
            return {"is_working": False, "source": "weekly", "periods": [], "fits_time": False}

    def _attendance_unavailable_reason(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
    ) -> str:
        detail = self.check_staff_attendance_detail_for_time(date_str, start_time, end_time, staff_name)
        if not detail.get("is_working"):
            return "off"
        if not detail.get("fits_time"):
            return "outside"
        return "ok"

    def check_staff_availability_reason(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
        exclude_reservation_id: str = None,
    ) -> str:
        try:
            attendance_reason = self._attendance_unavailable_reason(date_str, start_time, end_time, staff_name)
            if attendance_reason != "ok":
                return attendance_reason

            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
            for event_start, event_end in self._build_event_ranges_for_staff(date_str, staff_name, exclude_reservation_id):
                if start_datetime < event_end and end_datetime > event_start:
                    return "busy"
            return "ok"
        except Exception as e:
            logging.error(f"Error checking staff availability reason: {e}", exc_info=True)
            return "busy"

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
            services = reservation_data.get("services") or reservation_data.get("cart") or []
            service_name = reservation_data.get("service") or " / ".join(
                [str(item.get("service_name", "")).strip() for item in services if isinstance(item, dict)]
            )
            staff = reservation_data.get("assigned_staff") or reservation_data.get("staff")
            user_id = reservation_data.get("user_id", "")
            selected_staff = reservation_data.get("selected_staff")
            assigned_staff = reservation_data.get("assigned_staff") or staff

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
                duration_minutes = int(reservation_data.get("total_duration", 0) or 0)
                if duration_minutes <= 0:
                    duration_minutes = self._get_service_duration_minutes(
                        reservation_data.get("service_id") or reservation_data.get("service")
                    )
                end_datetime = start_datetime + timedelta(minutes=duration_minutes)

            start_time_str = start_datetime.strftime("%H:%M")
            end_time_str = end_datetime.strftime("%H:%M")
            duration_minutes = int((end_datetime - start_datetime).total_seconds() / 60)

            if not self._is_within_effective_business_periods(date_str, start_time_str, end_time_str, staff):
                logging.error(
                    f"Reservation rejected by attendance/business-hours check: staff={staff}, date={date_str}, start={start_time_str}, end={end_time_str}"
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

            description_lines = [
                f"予約ID: {reservation_id}",
                f"サービス: {service_name}",
                f"サービス一覧: {service_name}",
                f"担当者: {assigned_staff}",
                f"お客様: {client_name}",
                f"所要時間: {duration_minutes}分",
                "予約元: LINE Bot",
            ]
            if selected_staff:
                description_lines.append(f"Selected Staff: {selected_staff}")
            if assigned_staff:
                description_lines.append(f"Assigned Staff: {assigned_staff}")
            if user_id:
                description_lines.append(f"User ID: {user_id}")

            event = {
                "summary": f"[予約] {service_name} - {client_name} ({assigned_staff})",
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

            self.service.events().insert(calendarId=staff_calendar_id, body=event).execute()
            self._clear_runtime_caches()
            return True

        except HttpError as e:
            logging.error(f"Google Calendar API error: {e}", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"Failed to create calendar event: {e}", exc_info=True)
            return False

    def _parse_event_datetime(self, event_time_obj: Dict[str, Any], default_is_end: bool = False) -> Optional[datetime]:
        try:
            tz = pytz.timezone(self.timezone)

            if "dateTime" in event_time_obj and event_time_obj["dateTime"]:
                dt = datetime.fromisoformat(event_time_obj["dateTime"].replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    return tz.localize(dt)
                return dt.astimezone(tz)

            if "date" in event_time_obj and event_time_obj["date"]:
                d = datetime.strptime(event_time_obj["date"], "%Y-%m-%d")
                d = d.replace(hour=0, minute=0)
                return tz.localize(d)

            return None
        except Exception:
            return None

    def _find_available_periods(self, target_date: date, business_period: Dict[str, str], events: List[Dict[str, Any]]):
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

            date_events.sort(key=lambda e: self._parse_event_datetime(e.get("start", {}), default_is_end=False) or datetime.min)

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

        cache_key = f"{date_str}|{staff_name or '__BASE__'}"
        if cache_key in self._events_cache:
            return list(self._events_cache[cache_key])

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

            events = events_result.get("items", [])
            self._events_cache[cache_key] = list(events)
            return list(events)
        except Exception as e:
            print(f"Failed to get events for date {date_str}: {e}")
            return []

    def _filter_events_by_reservation_id(self, events: List[Dict[str, Any]], exclude_reservation_id: Optional[str]) -> List[Dict[str, Any]]:
        if not exclude_reservation_id:
            return list(events)

        filtered = []
        for event in events:
            description = event.get("description", "")
            if f"予約ID: {exclude_reservation_id}" in description:
                continue
            filtered.append(event)
        return filtered

    def _generate_slots_for_no_preference(
        self,
        start_date: datetime,
        end_date: datetime,
        service_ids: Optional[List[str]] = None,
        exclude_reservation_id: str = None,
    ) -> List[Dict[str, Any]]:
        slots_map: Dict[str, Dict[str, Any]] = {}
        current_date = start_date.date()
        end_date_only = end_date.date()

        normalized_service_ids = sorted(set(
            str(sid).strip() for sid in (service_ids or []) if str(sid).strip()
        ))

        while current_date <= end_date_only:
            date_str = current_date.strftime("%Y-%m-%d")
            for _staff_id, staff_data in self.staff_data.items():
                if not isinstance(staff_data, dict):
                    continue
                if not staff_data.get("is_active", True):
                    continue

                staff_name = staff_data.get("name")
                if not staff_name:
                    continue

                if not self._supports_all_services(staff_data, normalized_service_ids):
                    continue

                try:
                    staff_slots = self.get_available_slots_for_modification(
                        date_str=date_str,
                        exclude_reservation_id=exclude_reservation_id,
                        staff_name=staff_name,
                        service_ids=normalized_service_ids,
                    )
                except Exception:
                    continue

                for slot in staff_slots:
                    key = f"{slot['date']}|{slot['time']}|{slot['end_time']}"
                    if key not in slots_map:
                        slots_map[key] = {
                            "date": slot["date"],
                            "time": slot["time"],
                            "end_time": slot["end_time"],
                            "available": True,
                        }

            current_date += timedelta(days=1)

        results = list(slots_map.values())
        results.sort(key=lambda x: (x["date"], x["time"]))
        return results

    def get_available_slots(
        self,
        start_date: datetime,
        end_date: datetime,
        staff_name: str = None,
        service_id: str = None,
        service_ids: Optional[List[str]] = None,
        exclude_reservation_id: str = None,
    ) -> list:
        self._reload_config_data()

        normalized_service_ids = sorted(set(
            str(sid).strip() for sid in (service_ids or []) if str(sid).strip()
        ))
        if not normalized_service_ids and service_id:
            normalized_service_ids = [str(service_id).strip()]

        service_cache_key = ",".join(normalized_service_ids) if normalized_service_ids else "__NO_SERVICE__"

        cache_key = "|".join([
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
            staff_name or "__NO_STAFF__",
            service_cache_key,
            exclude_reservation_id or "__NO_EXCLUDE__",
        ])
        if cache_key in self._slots_cache:
            return list(self._slots_cache[cache_key])

        if staff_name and staff_name not in {"未指定", "指名なし", "おまかせ", "free"}:
            calendar_id = self._get_staff_calendar_id(staff_name)
            if not self.service or not calendar_id:
                result = self._generate_fallback_slots(start_date, end_date, staff_name)
                self._slots_cache[cache_key] = list(result)
                return result

            try:
                tz = pytz.timezone(self.timezone)
                start_date_aware = start_date if start_date.tzinfo else tz.localize(start_date)
                end_date_aware = (end_date + timedelta(days=1)) if end_date.tzinfo is None else end_date + timedelta(days=1)
                if end_date_aware.tzinfo is None:
                    end_date_aware = tz.localize(end_date_aware)

                events_result = self.service.events().list(
                    calendarId=calendar_id,
                    timeMin=start_date_aware.isoformat(),
                    timeMax=end_date_aware.isoformat(),
                    singleEvents=True,
                    orderBy="startTime",
                ).execute()
                events = self._filter_events_by_reservation_id(events_result.get("items", []), exclude_reservation_id)
                result = self._generate_all_slots(start_date, end_date, events, staff_name)
                self._slots_cache[cache_key] = list(result)
                return result
            except Exception as e:
                print(f"Failed to get available slots from Google Calendar: {e}")
                result = self._generate_fallback_slots(start_date, end_date, staff_name)
                self._slots_cache[cache_key] = list(result)
                return result

        result = self._generate_slots_for_no_preference(
            start_date=start_date,
            end_date=end_date,
            service_ids=normalized_service_ids,
            exclude_reservation_id=exclude_reservation_id,
        )
        self._slots_cache[cache_key] = list(result)
        return result

    def get_available_slots_for_modification(
        self,
        date_str: str,
        exclude_reservation_id: str = None,
        staff_name: str = None,
        service_id: str = None,
        service_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        self._reload_config_data()

        normalized_service_ids = sorted(set(
            str(sid).strip() for sid in (service_ids or []) if str(sid).strip()
        ))
        if not normalized_service_ids and service_id:
            normalized_service_ids = [str(service_id).strip()]

        service_cache_key = ",".join(normalized_service_ids) if normalized_service_ids else "__NO_SERVICE__"

        cache_key = f"MOD|{date_str}|{staff_name or '__NO_STAFF__'}|{service_cache_key}|{exclude_reservation_id or '__NO_EXCLUDE__'}"
        if cache_key in self._slots_cache:
            return list(self._slots_cache[cache_key])

        if not staff_name or staff_name in {"指名なし", "未指定", "おまかせ", "free"}:
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            result = self._generate_slots_for_no_preference(
                start_date=target_date,
                end_date=target_date,
                service_ids=normalized_service_ids,
                exclude_reservation_id=exclude_reservation_id,
            )
            self._slots_cache[cache_key] = list(result)
            return result

        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        if not self.service or not calendar_id:
            result = self._generate_fallback_slots(
                datetime.strptime(date_str, "%Y-%m-%d"),
                datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1),
                staff_name,
            )
            self._slots_cache[cache_key] = list(result)
            return result

        try:
            all_events = self.get_events_for_date(date_str, staff_name)
            other_events = self._filter_events_by_reservation_id(all_events, exclude_reservation_id)
            start_date = datetime.strptime(date_str, "%Y-%m-%d")
            end_date = start_date
            result = self._generate_all_slots(start_date, end_date, other_events, staff_name)
            self._slots_cache[cache_key] = list(result)
            return result
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

            self.service.events().delete(calendarId=staff_calendar_id, eventId=event["id"]).execute()
            self._clear_runtime_caches()
            return True
        except Exception as e:
            print(f"Failed to cancel reservation {reservation_id}: {e}")
            return False

    def _get_staff_calendar_id(self, staff_name: str) -> Optional[str]:
        if not staff_name or staff_name in {"未指定", "指名なし", "おまかせ", "free"}:
            return self.calendar_id

        for _staff_id, staff_data in self.staff_data.items():
            if staff_data.get("name") == staff_name:
                return staff_data.get("calendar_id") or self.calendar_id
        return self.calendar_id

    def get_short_calendar_url(self, staff_name: str = None) -> str:
        try:
            staff_calendar_id = self._get_staff_calendar_id(staff_name)
            if staff_calendar_id:
                return f"https://calendar.google.com/calendar/embed?src={staff_calendar_id}&ctz=Asia%2FTokyo"
            return "https://calendar.google.com/calendar"
        except Exception as e:
            logging.error(f"Failed to generate calendar url: {e}", exc_info=True)
            return "https://calendar.google.com/calendar"

    def _build_event_ranges_for_staff(
        self,
        date_str: str,
        staff_name: str,
        exclude_reservation_id: str = None,
    ) -> List[Tuple[datetime, datetime]]:
        staff_events = self._filter_events_by_reservation_id(self.get_events_for_date(date_str, staff_name), exclude_reservation_id)
        tz = pytz.timezone(self.timezone)
        ranges: List[Tuple[datetime, datetime]] = []
        for event in staff_events:
            event_start = self._parse_event_datetime(event.get("start", {}), default_is_end=False)
            event_end = self._parse_event_datetime(event.get("end", {}), default_is_end=True)
            if event_start and event_end:
                ranges.append((event_start.astimezone(tz).replace(tzinfo=None), event_end.astimezone(tz).replace(tzinfo=None)))
        ranges.sort(key=lambda x: x[0])
        return ranges

    def check_staff_availability_for_time(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
        exclude_reservation_id: str = None
    ) -> bool:
        return self.check_staff_availability_reason(
            date_str=date_str,
            start_time=start_time,
            end_time=end_time,
            staff_name=staff_name,
            exclude_reservation_id=exclude_reservation_id,
        ) == "ok"

    def _get_all_events_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        if date_str in self._all_events_cache:
            return list(self._all_events_cache[date_str])

        all_events: List[Dict[str, Any]] = []
        if self.calendar_id:
            all_events.extend(self.get_events_for_date(date_str, None))

        for _staff_id, staff_data in self.staff_data.items():
            staff_name_check = staff_data.get("name") if isinstance(staff_data, dict) else None
            if not staff_name_check:
                continue
            all_events.extend(self.get_events_for_date(date_str, staff_name_check))

        self._all_events_cache[date_str] = list(all_events)
        return list(all_events)

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
            all_events = self._filter_events_by_reservation_id(self._get_all_events_for_date(date_str), exclude_reservation_id)
            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
            tz = pytz.timezone(self.timezone)

            for event in all_events:
                if self._is_user_reservation(event, user_id):
                    event_start = self._parse_event_datetime(event.get("start", {}), default_is_end=False)
                    event_end = self._parse_event_datetime(event.get("end", {}), default_is_end=True)
                    if event_start and event_end:
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

    def _is_reservation_event(self, event: Dict[str, Any]) -> bool:
        summary = str(event.get("summary", "")).strip()
        return summary.startswith("[予約]")

    def _extract_event_duration_minutes(self, event: Dict[str, Any]) -> int:
        try:
            event_start = self._parse_event_datetime(event.get("start", {}), default_is_end=False)
            event_end = self._parse_event_datetime(event.get("end", {}), default_is_end=True)
            if event_start and event_end:
                return max(0, int((event_end - event_start).total_seconds() // 60))

            description = str(event.get("description", ""))
            marker = "所要時間:"
            if marker in description:
                raw = description.split(marker, 1)[1].split("\n", 1)[0].strip()
                if raw.endswith("分"):
                    raw = raw[:-1]
                if raw.isdigit():
                    return int(raw)
        except Exception:
            pass
        return 0

    def _get_staff_day_workload(
        self,
        date_str: str,
        staff_name: str,
        exclude_reservation_id: Optional[str] = None,
    ) -> Dict[str, int]:
        total_duration = 0
        reservation_count = 0

        try:
            events = self._filter_events_by_reservation_id(
                self.get_events_for_date(date_str, staff_name),
                exclude_reservation_id,
            )
            for event in events:
                if not self._is_reservation_event(event):
                    continue
                total_duration += self._extract_event_duration_minutes(event)
                reservation_count += 1
        except Exception as e:
            logging.warning(f"Failed to calculate workload for {staff_name} on {date_str}: {e}")

        return {
            "total_duration_minutes": total_duration,
            "reservation_count": reservation_count,
        }

    def _staff_order(self, staff_name: str) -> int:
        for _, s in self.staff_data.items():
            if isinstance(s, dict) and s.get("name") == staff_name:
                return int(s.get("order", 999))
        return 999

    def assign_staff_for_free_reservation(
        self,
        date_str: str,
        start_time: str,
        duration_minutes: int,
        service_id: str = None,
        service_ids: Optional[List[str]] = None,
        exclude_reservation_id: str = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Free-staff assignment rule:
        1. candidate staff filtered by service support / attendance / business hours / calendar vacancy
        2. smaller total scheduled minutes on the day first
        3. fewer reservation count on the day second
        4. lower display order third
        """
        self._reload_config_data()

        requested_service_ids = [str(sid).strip() for sid in (service_ids or []) if str(sid).strip()]
        if not requested_service_ids and service_id:
            requested_service_ids = [str(service_id).strip()]
        requested_service_ids = sorted(set(requested_service_ids))

        service_cache_key = ",".join(requested_service_ids) if requested_service_ids else "__NO_SERVICE__"
        cache_key = "|".join([
            date_str,
            start_time,
            str(duration_minutes),
            service_cache_key,
            exclude_reservation_id or "__NO_EXCLUDE__",
        ])
        if cache_key in self._free_staff_assignment_cache:
            cached = self._free_staff_assignment_cache[cache_key]
            return dict(cached) if cached else None

        end_time = self._calculate_end_time(start_time, duration_minutes)
        if not end_time:
            self._free_staff_assignment_cache[cache_key] = None
            return None

        candidates: List[Dict[str, Any]] = []

        for _staff_id, staff_data in self.staff_data.items():
            if not isinstance(staff_data, dict):
                continue
            if not staff_data.get("is_active", True):
                continue

            staff_name = staff_data.get("name")
            if not staff_name:
                continue

            if not self._supports_all_services(staff_data, requested_service_ids):
                continue

            if not self.check_staff_availability_for_time(
                date_str=date_str,
                start_time=start_time,
                end_time=end_time,
                staff_name=staff_name,
                exclude_reservation_id=exclude_reservation_id,
            ):
                continue

            workload = self._get_staff_day_workload(
                date_str=date_str,
                staff_name=staff_name,
                exclude_reservation_id=exclude_reservation_id,
            )

            candidates.append({
                "staff_name": staff_name,
                "total_duration_minutes": workload["total_duration_minutes"],
                "reservation_count": workload["reservation_count"],
                "order": self._staff_order(staff_name),
            })

        if not candidates:
            self._free_staff_assignment_cache[cache_key] = None
            return None

        candidates.sort(
            key=lambda item: (
                item["total_duration_minutes"],
                item["reservation_count"],
                item["order"],
            )
        )
        selected = candidates[0]
        self._free_staff_assignment_cache[cache_key] = dict(selected)
        return dict(selected)

    def _calculate_end_time(self, start_time: str, duration_minutes: int) -> Optional[str]:
        try:
            start_hour, start_minute = map(int, start_time.split(":"))
            start_total = start_hour * 60 + start_minute
            end_total = start_total + int(duration_minutes)
            end_hour, end_minute = divmod(end_total, 60)
            return f"{end_hour:02d}:{end_minute:02d}"
        except Exception:
            return None

    def find_assignable_staff(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        service_id: str = None,
        service_ids: Optional[List[str]] = None,
        exclude_reservation_id: str = None,
    ) -> Optional[str]:
        duration_minutes = 0
        try:
            start_dt = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
            duration_minutes = int((end_dt - start_dt).total_seconds() // 60)
        except Exception:
            pass

        if duration_minutes <= 0 and service_id:
            duration_minutes = self._get_service_duration_minutes(service_id)

        assigned = self.assign_staff_for_free_reservation(
            date_str=date_str,
            start_time=start_time,
            duration_minutes=duration_minutes or 60,
            service_id=service_id,
            service_ids=service_ids,
            exclude_reservation_id=exclude_reservation_id,
        )
        return assigned["staff_name"] if assigned else None



