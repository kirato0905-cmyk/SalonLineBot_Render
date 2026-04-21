import os
import json
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import pytz


class GoogleSheetsLogger:
    """Google Sheets logger for:
    1. Reservations sheet (reservation ledger)
    2. Users sheet (initial registration + consent management)

    Free-staff assignment対応版:
    - selected_staff / assigned_staff を分離保持
    - 既存の Staff 列には実担当（assigned_staff）を保持
    """

    RESERVATION_HEADERS = [
        "Timestamp",
        "Reservation ID",
        "User ID",
        "Client Name",
        "Date",
        "Start Time",
        "End Time",
        "Service",
        "Selected Staff",
        "Assigned Staff",
        "Staff",
        "Duration (min)",
        "Price",
        "Status",
    ]

    USER_HEADERS = [
        "Timestamp",
        "User ID",
        "Display Name",
        "Phone Number",
        "Status",
        "Notes",
        "Consented",
        "Consent Date",
        "First Seen",
        "Last Seen"
    ]

    _instance = None
    _instance_lock = threading.Lock()
    _gspread_client = None
    _spreadsheet = None

    def __new__(cls, *args, **kwargs):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return

        self.reservations_worksheet = None
        self.users_worksheet = None
        self.spreadsheet = None
        self.tokyo_tz = pytz.timezone("Asia/Tokyo")
        self._records_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl_seconds = 8
        self._setup_connection()
        self._initialized = True

    def _get_tokyo_timestamp(self) -> str:
        tokyo_time = datetime.now(self.tokyo_tz)
        return tokyo_time.strftime("%Y-%m-%d %H:%M:%S")

    def _create_gspread_client(self):
        if GoogleSheetsLogger._gspread_client is not None:
            return GoogleSheetsLogger._gspread_client

        load_dotenv()
        credentials_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not credentials_json:
            logging.warning("GOOGLE_SERVICE_ACCOUNT_JSON not found. Google Sheets disabled.")
            return None

        try:
            credentials_info = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            logging.error(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
            return None

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]

        try:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_info, scope)
            GoogleSheetsLogger._gspread_client = gspread.authorize(creds)
            return GoogleSheetsLogger._gspread_client
        except Exception as e:
            logging.error(f"Failed to authorize Google Sheets client: {e}")
            return None

    def _get_spreadsheet(self):
        if self.spreadsheet:
            return self.spreadsheet
        if GoogleSheetsLogger._spreadsheet is not None:
            self.spreadsheet = GoogleSheetsLogger._spreadsheet
            return self.spreadsheet

        gc = self._create_gspread_client()
        if not gc:
            return None

        spreadsheet_id = os.getenv("GOOGLE_SHEET_ID")
        if not spreadsheet_id:
            logging.warning("GOOGLE_SHEET_ID not found. Google Sheets disabled.")
            return None

        try:
            GoogleSheetsLogger._spreadsheet = gc.open_by_key(spreadsheet_id)
            self.spreadsheet = GoogleSheetsLogger._spreadsheet
            return self.spreadsheet
        except Exception as e:
            logging.error(f"Failed to open spreadsheet: {e}")
            return None

    def _setup_connection(self):
        try:
            spreadsheet = self._get_spreadsheet()
            if not spreadsheet:
                return

            self.reservations_worksheet = self._get_or_create_worksheet(
                title="Reservations",
                rows=1000,
                cols=len(self.RESERVATION_HEADERS),
                headers=self.RESERVATION_HEADERS
            )
            self.users_worksheet = self._get_or_create_worksheet(
                title="Users",
                rows=1000,
                cols=10,
                headers=self.USER_HEADERS
            )
            print("Google Sheets logger initialized successfully")
            print("Sheet1: unused / Reservations: active / Users: initial registration only")
        except Exception as e:
            logging.error(f"Failed to setup Google Sheets connection: {e}")
            self.reservations_worksheet = None
            self.users_worksheet = None
            self.spreadsheet = None

    def _get_or_create_worksheet(self, title: str, rows: int, cols: int, headers: List[str]):
        spreadsheet = self._get_spreadsheet()
        if not spreadsheet:
            return None

        try:
            worksheet = spreadsheet.worksheet(title)
            self._ensure_headers(worksheet, headers)
            return worksheet
        except gspread.WorksheetNotFound:
            try:
                worksheet = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
                worksheet.append_row(headers)
                print(f"Created new worksheet: {title}")
                return worksheet
            except Exception as e:
                logging.error(f"Failed to create worksheet '{title}': {e}")
                return None
        except Exception as e:
            logging.error(f"Failed to get worksheet '{title}': {e}")
            return None

    def _ensure_headers(self, worksheet, expected_headers: List[str]) -> bool:
        if not worksheet:
            return False
        try:
            actual_headers = worksheet.row_values(1)
            if not actual_headers:
                worksheet.append_row(expected_headers)
                print(f"Headers initialized for worksheet: {worksheet.title}")
                return True
            if actual_headers == expected_headers:
                return True
            logging.warning(
                f"Header mismatch detected in worksheet '{worksheet.title}'. Resetting headers."
            )
            worksheet.clear()
            worksheet.append_row(expected_headers)
            self._invalidate_all_cache()
            return True
        except Exception as e:
            logging.error(f"Failed to ensure headers for worksheet '{worksheet.title}': {e}")
            return False

    def _get_reservations_worksheet(self):
        if self.reservations_worksheet:
            return self.reservations_worksheet
        self.reservations_worksheet = self._get_or_create_worksheet(
            title="Reservations",
            rows=1000,
            cols=len(self.RESERVATION_HEADERS),
            headers=self.RESERVATION_HEADERS
        )
        return self.reservations_worksheet

    def _get_users_worksheet(self):
        if self.users_worksheet:
            return self.users_worksheet
        self.users_worksheet = self._get_or_create_worksheet(
            title="Users",
            rows=1000,
            cols=10,
            headers=self.USER_HEADERS
        )
        return self.users_worksheet

    def _invalidate_cache(self, key: str):
        self._records_cache.pop(key, None)

    def _invalidate_all_cache(self):
        self._records_cache.clear()

    def _get_cached_records(self, cache_key: str):
        cached = self._records_cache.get(cache_key)
        if not cached:
            return None
        if time.time() - cached["fetched_at"] > self._cache_ttl_seconds:
            self._records_cache.pop(cache_key, None)
            return None
        return cached["records"]

    def _set_cached_records(self, cache_key: str, records: List[Dict[str, Any]]):
        self._records_cache[cache_key] = {
            "fetched_at": time.time(),
            "records": records,
        }

    def _get_users_records(self) -> List[Dict[str, Any]]:
        cache_key = "users_records"
        cached = self._get_cached_records(cache_key)
        if cached is not None:
            return cached

        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return []
        try:
            records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            self._set_cached_records(cache_key, records)
            return records
        except Exception as e:
            logging.error(f"Failed to get users from Google Sheets: {e}")
            return []

    def _get_reservation_records(self) -> List[Dict[str, Any]]:
        cache_key = "reservation_records"
        cached = self._get_cached_records(cache_key)
        if cached is not None:
            return cached

        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return []
        try:
            records = reservations_worksheet.get_all_records(expected_headers=self.RESERVATION_HEADERS)
            self._set_cached_records(cache_key, records)
            return records
        except Exception as e:
            logging.error(f"Failed to get reservations from Google Sheets: {e}")
            return []

    def save_reservation(self, reservation_data: Dict[str, Any]) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            selected_staff = reservation_data.get("selected_staff", "")
            assigned_staff = reservation_data.get("assigned_staff") or reservation_data.get("staff", "")
            row_data = [
                self._get_tokyo_timestamp(),
                reservation_data.get("reservation_id", ""),
                reservation_data.get("user_id", ""),
                reservation_data.get("client_name", ""),
                reservation_data.get("date", ""),
                reservation_data.get("start_time", ""),
                reservation_data.get("end_time", ""),
                reservation_data.get("service", ""),
                selected_staff,
                assigned_staff,
                assigned_staff,
                reservation_data.get("duration", ""),
                reservation_data.get("price", ""),
                "Confirmed",
            ]
            reservations_worksheet.append_row(row_data)
            self._invalidate_cache("reservation_records")
            print(f"Saved reservation {reservation_data.get('reservation_id')} to Google Sheets")
            return True
        except Exception as e:
            logging.error(f"Failed to save reservation to Google Sheets: {e}")
            return False

    def _record_to_reservation(self, record: Dict[str, Any]) -> Dict[str, Any]:
        selected_staff = record.get("Selected Staff", "")
        assigned_staff = record.get("Assigned Staff", "") or record.get("Staff", "")
        return {
            "reservation_id": record.get("Reservation ID"),
            "user_id": record.get("User ID"),
            "client_name": record.get("Client Name"),
            "date": record.get("Date"),
            "start_time": record.get("Start Time"),
            "end_time": record.get("End Time"),
            "service": record.get("Service"),
            "selected_staff": selected_staff,
            "assigned_staff": assigned_staff,
            "staff": assigned_staff,
            "duration": record.get("Duration (min)"),
            "price": record.get("Price"),
            "status": record.get("Status"),
        }

    def get_all_reservations(self) -> list:
        records = self._get_reservation_records()
        reservations = []
        for record in records:
            if record.get("Reservation ID"):
                reservations.append(self._record_to_reservation(record))
        return reservations

    def get_confirmed_reservations(self) -> list:
        return [res for res in self.get_all_reservations() if res.get("status") == "Confirmed"]

    def get_user_reservations(self, client_name: str) -> list:
        all_reservations = self.get_all_reservations()
        return [
            res for res in all_reservations
            if res["client_name"] == client_name and res.get("status") == "Confirmed"
        ]

    def get_user_reservations_by_user_id(self, user_id: str) -> list:
        all_reservations = self.get_all_reservations()
        return [
            res for res in all_reservations
            if str(res.get("user_id", "")).strip() == str(user_id).strip()
            and res.get("status") == "Confirmed"
        ]

    def update_reservation_status(self, reservation_id: str, status: str) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            records = self._get_reservation_records()
            status_col = self.RESERVATION_HEADERS.index("Status") + 1
            for i, record in enumerate(records, start=2):
                if record.get("Reservation ID") == reservation_id:
                    reservations_worksheet.update_cell(i, status_col, status)
                    self._invalidate_cache("reservation_records")
                    print(f"Updated reservation {reservation_id} status to {status}")
                    return True
            logging.warning(f"Reservation {reservation_id} not found for status update")
            return False
        except Exception as e:
            logging.error(f"Failed to update reservation status: {e}")
            return False

    def get_reservation_by_id(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        try:
            for record in self._get_reservation_records():
                if record.get("Reservation ID") == reservation_id:
                    return self._record_to_reservation(record)
            return None
        except Exception as e:
            logging.error(f"Failed to get reservation by ID: {e}")
            return None

    def update_reservation_data(self, reservation_id: str, field_updates: Dict[str, Any]) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            records = self._get_reservation_records()
            for i, record in enumerate(records, start=2):
                if record.get("Reservation ID") == reservation_id:
                    for field, value in field_updates.items():
                        if field in self.RESERVATION_HEADERS:
                            column_index = self.RESERVATION_HEADERS.index(field) + 1
                            reservations_worksheet.update_cell(i, column_index, value)
                    self._invalidate_cache("reservation_records")
                    print(f"Updated reservation {reservation_id} with fields: {list(field_updates.keys())}")
                    return True
            logging.warning(f"Reservation {reservation_id} not found for data update")
            return False
        except Exception as e:
            logging.error(f"Failed to update reservation data: {e}")
            return False

    def get_reservations_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        try:
            date_reservations = []
            for record in self._get_reservation_records():
                if record.get("Date") == date_str:
                    date_reservations.append(self._record_to_reservation(record))
            return date_reservations
        except Exception as e:
            logging.error(f"Error getting reservations for date {date_str}: {e}")
            return []

    def get_user_id_for_reservation(self, reservation_id: str) -> Optional[str]:
        try:
            for record in self._get_reservation_records():
                if record.get("Reservation ID") == reservation_id:
                    user_id = record.get("User ID", "")
                    return user_id if user_id else None
            logging.warning(f"Reservation {reservation_id} not found in sheets")
            return None
        except Exception as e:
            logging.error(f"Error getting user ID for reservation {reservation_id}: {e}")
            return None

    def log_new_user(self, user_id: str, display_name: str, phone_number: str = "") -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            logging.error("Users worksheet not available. Cannot log user data.")
            return False
        try:
            existing_records = self._get_users_records()
            for record in existing_records:
                if record.get("User ID") == user_id:
                    print(f"User {user_id} already exists in Users sheet")
                    return True

            timestamp = self._get_tokyo_timestamp()
            user_data = [
                timestamp,
                user_id,
                display_name,
                phone_number,
                "Active",
                "Added via LINE Bot",
                "No",
                "",
                timestamp,
                timestamp,
            ]
            users_worksheet.append_row(user_data)
            self._invalidate_cache("users_records")
            print(f"Successfully logged new user: {display_name} ({user_id})")
            return True
        except Exception as e:
            logging.error(f"Failed to log user data: {e}")
            return False

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        try:
            for record in self._get_users_records():
                if record.get("User ID") == user_id:
                    return record
            return None
        except Exception as e:
            logging.error(f"Error getting user by ID {user_id}: {e}")
            return None

    def update_user_status(self, user_id: str, status: str, notes: str = "") -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return False
        try:
            records = self._get_users_records()
            for i, record in enumerate(records, start=2):
                if record.get("User ID") == user_id:
                    users_worksheet.update_cell(i, 5, status)
                    if notes:
                        users_worksheet.update_cell(i, 6, notes)
                    self._invalidate_cache("users_records")
                    print(f"Updated user {user_id} status to: {status}")
                    return True
            logging.warning(f"User {user_id} not found for status update")
            return False
        except Exception as e:
            logging.error(f"Error updating user status: {e}")
            return False

    def has_user_consented(self, user_id: str) -> bool:
        try:
            for record in self._get_users_records():
                if record.get("User ID") == user_id:
                    return str(record.get("Consented", "")).strip().lower() == "yes"
            return False
        except Exception as e:
            logging.error(f"Error checking consent for user {user_id}: {e}")
            return False

    def set_user_consent(self, user_id: str, consented: bool) -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return False
        try:
            records = self._get_users_records()
            for i, record in enumerate(records, start=2):
                if record.get("User ID") == user_id:
                    users_worksheet.update_cell(i, 7, "Yes" if consented else "No")
                    users_worksheet.update_cell(i, 8, self._get_tokyo_timestamp() if consented else "")
                    self._invalidate_cache("users_records")
                    return True
            return False
        except Exception as e:
            logging.error(f"Error setting consent for user {user_id}: {e}")
            return False


_sheets_logger_instance = None


def get_sheets_logger() -> GoogleSheetsLogger:
    global _sheets_logger_instance
    if _sheets_logger_instance is None:
        _sheets_logger_instance = GoogleSheetsLogger()
    return _sheets_logger_instance

