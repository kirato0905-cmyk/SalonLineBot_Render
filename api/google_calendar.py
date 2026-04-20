"""
Google Calendar integration for salon reservations
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

from api.business_hours import get_hours_for_date, is_closed_date, get_timezone


class GoogleCalendarHelper:
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

    def create_reservation_event(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        if not self.service:
            print("Google Calendar not configured, skipping event creation")
            return False

        try:
            date_str = reservation_data["date"]
            service = reservation_data["service"]
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

            event = {
                "summary": f"[予約] {service} - {client_name} ({staff})",
                "description": f"""
予約ID: {reservation_id}
サービス: {service}
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

    def get_available_slots(self, start_date: datetime, end_date: datetime, staff_name: str = None) -> list:
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id

        if not self.service or not calendar_id:
            print("Google Calendar not configured, using fallback slots")
            return self._generate_fallback_slots(start_date, end_date)

        try:
            events_result = self.service.events().list(
                calendarId=calendar_id,
                timeMin=start_date.isoformat() + "Z",
                timeMax=end_date.isoformat() + "Z",
                singleEvents=True,
                orderBy="startTime",
            ).execute()

            events = events_result.get("items", [])
            return self._generate_all_slots(start_date, end_date, events)

        except Exception as e:
            print(f"Failed to get available slots from Google Calendar: {e}")
            return self._generate_fallback_slots(start_date, end_date)

    def _generate_all_slots(self, start_date: datetime, end_date: datetime, events: list = None) -> list:
        slots = []
        current_date = start_date.date()
        end_date_only = end_date.date()

        while current_date <= end_date_only:
            if is_closed_date(current_date):
                current_date += timedelta(days=1)
                continue

            business_periods = get_hours_for_date(current_date)
            if not business_periods:
                current_date += timedelta(days=1)
                continue

            date_events = []
            if events:
                for event in events:
                    event_start = datetime.fromisoformat(
                        event["start"].get("dateTime", event["start"].get("date", ""))
                    )
                    if event_start.date() == current_date:
                        date_events.append(event)

            date_events.sort(
                key=lambda e: datetime.fromisoformat(
                    e["start"].get("dateTime", e["start"].get("date", ""))
                )
            )

            for business_period in business_periods:
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

        for event in events:
            event_start = datetime.fromisoformat(event["start"].get("dateTime", event["start"].get("date", "")))
            event_end = datetime.fromisoformat(event["end"].get("dateTime", event["end"].get("date", "")))

            if event_start <= business_end and event_end >= business_start:
                if event_start > business_start:
                    available_periods.append({
                        "start": business_start.strftime("%H:%M"),
                        "end": event_start.strftime("%H:%M"),
                    })
                    business_start = event_end
                elif event_start == business_start:
                    business_start = event_end

        if business_start < business_end:
            available_periods.append({
                "start": business_start.strftime("%H:%M"),
                "end": business_end.strftime("%H:%M"),
            })

        return available_periods

    def _generate_fallback_slots(self, start_date: datetime, end_date: datetime) -> list:
        try:
            return self._generate_all_slots(start_date, end_date, None)
        except Exception:
            fallback = []
            d = start_date.date()
            if not is_closed_date(d):
                for slot in get_hours_for_date(d):
                    fallback.append({
                        "date": d.strftime("%Y-%m-%d"),
                        "time": slot["start"],
                        "end_time": slot["end"],
                        "available": True,
                    })
            return fallback if fallback else []

    def get_events_for_date(self, date_str: str, staff_name: str = None) -> List[Dict]:
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
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id

        if not self.service or not calendar_id:
            return self._generate_fallback_slots(
                datetime.strptime(date_str, "%Y-%m-%d"),
                datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1),
            )

        try:
            all_events = self.get_events_for_date(date_str, staff_name)

            current_reservation = None
            other_events = []

            if exclude_reservation_id:
                for e in all_events:
                    description = e.get("description", "")
                    if f"予約ID: {exclude_reservation_id}" in description:
                        current_reservation = e
                    else:
                        other_events.append(e)
            else:
                other_events = all_events

            start_date = datetime.strptime(date_str, "%Y-%m-%d")
            end_date = start_date

            return self._generate_all_slots(start_date, end_date, other_events)

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

            for staff_id, staff_data in self.staff_data.items():
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

        for staff_id, staff_data in self.staff_data.items():
            if staff_data.get("name") == staff_name:
                return staff_data.get("calendar_id") or self.calendar_id

        return self.calendar_id

    def check_staff_availability_for_time(
        self,
        date_str: str,
        start_time: str,
        end_time: str,
        staff_name: str,
        exclude_reservation_id: str = None
    ) -> bool:
        try:
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

            for staff_id, staff_data in self.staff_data.items():
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
