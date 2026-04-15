import os
import json
import logging
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

    Sheet1 is intentionally unused.
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
        "Staff",
        "Duration (min)",
        "Price",
        "Status"
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

    def __init__(self):
        self.reservations_worksheet = None
        self.users_worksheet = None
        self.spreadsheet = None
        self.tokyo_tz = pytz.timezone("Asia/Tokyo")
        self._setup_connection()

    def _get_tokyo_timestamp(self) -> str:
        """Get current timestamp in Tokyo timezone"""
        tokyo_time = datetime.now(self.tokyo_tz)
        return tokyo_time.strftime("%Y-%m-%d %H:%M:%S")

    def _create_gspread_client(self):
        """Create and return authorized gspread client"""
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
            return gspread.authorize(creds)
        except Exception as e:
            logging.error(f"Failed to authorize Google Sheets client: {e}")
            return None

    def _get_spreadsheet(self):
        """Get spreadsheet object"""
        if self.spreadsheet:
            return self.spreadsheet

        gc = self._create_gspread_client()
        if not gc:
            return None

        spreadsheet_id = os.getenv("GOOGLE_SHEET_ID")
        if not spreadsheet_id:
            logging.warning("GOOGLE_SHEET_ID not found. Google Sheets disabled.")
            return None

        try:
            self.spreadsheet = gc.open_by_key(spreadsheet_id)
            return self.spreadsheet
        except Exception as e:
            logging.error(f"Failed to open spreadsheet: {e}")
            return None

    def _setup_connection(self):
        """Setup Google Sheets connection for Reservations and Users only"""
        try:
            spreadsheet = self._get_spreadsheet()
            if not spreadsheet:
                return

            self.reservations_worksheet = self._get_or_create_worksheet(
                title="Reservations",
                rows=1000,
                cols=12,
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
        """Get existing worksheet or create a new one, then ensure headers"""
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
        """Ensure worksheet has correct headers"""
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
            return True

        except Exception as e:
            logging.error(f"Failed to ensure headers for worksheet '{worksheet.title}': {e}")
            return False

    def _get_reservations_worksheet(self):
        """Get or create the reservations worksheet"""
        if self.reservations_worksheet:
            return self.reservations_worksheet

        self.reservations_worksheet = self._get_or_create_worksheet(
            title="Reservations",
            rows=1000,
            cols=12,
            headers=self.RESERVATION_HEADERS
        )
        return self.reservations_worksheet

    def _get_users_worksheet(self):
        """Get or create the users worksheet"""
        if self.users_worksheet:
            return self.users_worksheet

        self.users_worksheet = self._get_or_create_worksheet(
            title="Users",
            rows=1000,
            cols=10,
            headers=self.USER_HEADERS
        )
        return self.users_worksheet

    # -----------------------------
    # Reservations
    # -----------------------------
    def save_reservation(self, reservation_data: Dict[str, Any]) -> bool:
        """Save a new reservation to the Reservations worksheet"""
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False

        try:
            timestamp = self._get_tokyo_timestamp()

            row_data = [
                timestamp,
                reservation_data.get("reservation_id", ""),
                reservation_data.get("user_id", ""),
                reservation_data.get("client_name", ""),
                reservation_data.get("date", ""),
                reservation_data.get("start_time", ""),
                reservation_data.get("end_time", ""),
                reservation_data.get("service", ""),
                reservation_data.get("staff", ""),
                reservation_data.get("duration", ""),
                reservation_data.get("price", ""),
                "Confirmed"
            ]

            reservations_worksheet.append_row(row_data)
            print(f"Saved reservation {reservation_data.get('reservation_id')} to Google Sheets")
            return True

        except Exception as e:
            logging.error(f"Failed to save reservation to Google Sheets: {e}")
            return False

    def get_all_reservations(self) -> list:
        """Get all reservations from the Reservations worksheet"""
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return []

        try:
            records = reservations_worksheet.get_all_records(expected_headers=self.RESERVATION_HEADERS)

            reservations = []
            for record in records:
                if record.get("Reservation ID"):
                    reservations.append({
                        "reservation_id": record.get("Reservation ID"),
                        "user_id": record.get("User ID"),
                        "client_name": record.get("Client Name"),
                        "date": record.get("Date"),
                        "start_time": record.get("Start Time"),
                        "end_time": record.get("End Time"),
                        "service": record.get("Service"),
                        "staff": record.get("Staff"),
                        "duration": record.get("Duration (min)"),
                        "price": record.get("Price"),
                        "status": record.get("Status")
                    })
            return reservations

        except Exception as e:
            logging.error(f"Failed to get reservations from Google Sheets: {e}")
            return []

    def get_confirmed_reservations(self) -> list:
        """Get only confirmed reservations"""
        all_reservations = self.get_all_reservations()
        return [res for res in all_reservations if res.get("status") == "Confirmed"]

    def get_user_reservations(self, client_name: str) -> list:
        """Get reservations for a specific client (confirmed only)"""
        all_reservations = self.get_all_reservations()
        return [
            res for res in all_reservations
            if res["client_name"] == client_name and res.get("status") == "Confirmed"
        ]

    def update_reservation_status(self, reservation_id: str, status: str) -> bool:
        """Update the status of a reservation"""
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False

        try:
            records = reservations_worksheet.get_all_records(expected_headers=self.RESERVATION_HEADERS)

            for i, record in enumerate(records, start=2):
                if record.get("Reservation ID") == reservation_id:
                    reservations_worksheet.update_cell(i, 12, status)
                    print(f"Updated reservation {reservation_id} status to {status}")
                    return True

            logging.warning(f"Reservation {reservation_id} not found for status update")
            return False

        except Exception as e:
            logging.error(f"Failed to update reservation status: {e}")
            return False

    def get_reservation_by_id(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        """Get reservation details by reservation ID"""
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return None

        try:
            records = reservations_worksheet.get_all_records(expected_headers=self.RESERVATION_HEADERS)

            for record in records:
                if record.get("Reservation ID") == reservation_id:
                    return {
                        "reservation_id": record.get("Reservation ID"),
                        "user_id": record.get("User ID"),
                        "client_name": record.get("Client Name"),
                        "date": record.get("Date"),
                        "start_time": record.get("Start Time"),
                        "end_time": record.get("End Time"),
                        "service": record.get("Service"),
                        "staff": record.get("Staff"),
                        "duration": record.get("Duration (min)"),
                        "price": record.get("Price"),
                        "status": record.get("Status")
                    }

            return None

        except Exception as e:
            logging.error(f"Failed to get reservation by ID: {e}")
            return None

    def update_reservation_data(self, reservation_id: str, field_updates: Dict[str, Any]) -> bool:
        """Update specific fields of a reservation

        field_updates keys must match sheet headers:
        - Client Name
        - Date
        - Start Time
        - End Time
        - Service
        - Staff
        - Duration (min)
        - Price
        - Status
        """
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False

        try:
            records = reservations_worksheet.get_all_records(expected_headers=self.RESERVATION_HEADERS)

            for i, record in enumerate(records, start=2):
                if record.get("Reservation ID") == reservation_id:
                    for field, value in field_updates.items():
                        if field in self.RESERVATION_HEADERS:
                            column_index = self.RESERVATION_HEADERS.index(field) + 1
                            reservations_worksheet.update_cell(i, column_index, value)

                    print(f"Updated reservation {reservation_id} with fields: {list(field_updates.keys())}")
                    return True

            logging.warning(f"Reservation {reservation_id} not found for data update")
            return False

        except Exception as e:
            logging.error(f"Failed to update reservation data: {e}")
            return False

    def get_reservations_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        """Get all reservations for a specific date"""
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            logging.warning("Reservations worksheet not available")
            return []

        try:
            records = reservations_worksheet.get_all_records(expected_headers=self.RESERVATION_HEADERS)

            date_reservations = []
            for record in records:
                if record.get("Date") == date_str:
                    reservation = {
                        "reservation_id": record.get("Reservation ID", ""),
                        "user_id": record.get("User ID", ""),
                        "client_name": record.get("Client Name", ""),
                        "date": record.get("Date", ""),
                        "start_time": record.get("Start Time", ""),
                        "end_time": record.get("End Time", ""),
                        "service": record.get("Service", ""),
                        "staff": record.get("Staff", ""),
                        "duration": record.get("Duration (min)", ""),
                        "price": record.get("Price", ""),
                        "status": record.get("Status", "")
                    }
                    date_reservations.append(reservation)

            return date_reservations

        except Exception as e:
            logging.error(f"Error getting reservations for date {date_str}: {e}")
            return []

    def get_user_id_for_reservation(self, reservation_id: str) -> Optional[str]:
        """Get user ID for a specific reservation"""
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            logging.warning("Reservations worksheet not available")
            return None

        try:
            records = reservations_worksheet.get_all_records(expected_headers=self.RESERVATION_HEADERS)

            for record in records:
                if record.get("Reservation ID") == reservation_id:
                    user_id = record.get("User ID", "")
                    return user_id if user_id else None

            logging.warning(f"Reservation {reservation_id} not found in sheets")
            return None

        except Exception as e:
            logging.error(f"Error getting user ID for reservation {reservation_id}: {e}")
            return None

    # -----------------------------
    # Users
    # -----------------------------
    def log_new_user(self, user_id: str, display_name: str, phone_number: str = "") -> bool:
        """Register a new user only once in the Users sheet"""
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            logging.error("Users worksheet not available. Cannot log user data.")
            return False

        try:
            existing_records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)

            for record in existing_records:
                if record.get("User ID") == user_id:
                    print(f"User {user_id} already exists in Users sheet")
                    return True

            timestamp = self._get_tokyo_timestamp()
            user_data = [
                timestamp,                # Timestamp
                user_id,                  # User ID
                display_name,             # Display Name
                phone_number,             # Phone Number
                "Active",                 # Status
                "Added via LINE Bot",     # Notes
                "No",                     # Consented
                "",                       # Consent Date
                timestamp,                # First Seen
                timestamp                 # Last Seen（初回登録時だけ入れる）
            ]

            users_worksheet.append_row(user_data)
            print(f"Successfully logged new user: {display_name} ({user_id})")
            return True

        except Exception as e:
            logging.error(f"Failed to log user data: {e}")
            return False

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user data by user ID from Users sheet"""
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return None

        try:
            records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            for record in records:
                if record.get("User ID") == user_id:
                    return record
            return None

        except Exception as e:
            logging.error(f"Error getting user by ID {user_id}: {e}")
            return None

    def update_user_status(self, user_id: str, status: str, notes: str = "") -> bool:
        """Update user status in Users sheet"""
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return False

        try:
            records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            for i, record in enumerate(records, start=2):
                if record.get("User ID") == user_id:
                    users_worksheet.update_cell(i, 5, status)
                    if notes:
                        users_worksheet.update_cell(i, 6, notes)

                    print(f"Updated user {user_id} status to: {status}")
                    return True

            logging.warning(f"User {user_id} not found for status update")
            return False

        except Exception as e:
            logging.error(f"Error updating user status: {e}")
            return False

    # -----------------------------
    # Users sheet consent helpers
    # -----------------------------
    def has_user_consented(self, user_id: str) -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return False

        try:
            records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            for record in records:
                if record.get("User ID") == user_id:
                    return str(record.get("Consented", "No")).strip().lower() in ("yes", "true", "1", "y")
            return False

        except Exception as e:
            logging.error(f"Error checking consent for {user_id}: {e}")
            return False

    def mark_user_consented(self, user_id: str) -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return False

        try:
            records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            for i, record in enumerate(records, start=2):
                if record.get("User ID") == user_id:
                    timestamp = self._get_tokyo_timestamp()
                    users_worksheet.update_cell(i, 7, "Yes")
                    users_worksheet.update_cell(i, 8, timestamp)
                    print(f"Marked consented in Users sheet: {user_id}")
                    return True

            self.log_new_user(user_id, display_name="", phone_number="")
            return self.mark_user_consented(user_id)

        except Exception as e:
            logging.error(f"Error marking consent for {user_id}: {e}")
            return False

    def revoke_user_consent(self, user_id: str) -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return False

        try:
            records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            for i, record in enumerate(records, start=2):
                if record.get("User ID") == user_id:
                    users_worksheet.update_cell(i, 7, "No")
                    users_worksheet.update_cell(i, 8, "")
                    print(f"Revoked consent in Users sheet: {user_id}")
                    return True
            return False

        except Exception as e:
            logging.error(f"Error revoking consent for {user_id}: {e}")
            return False

    def is_new_user(self, user_id: str) -> bool:
        """Return True if user does not exist in Users sheet"""
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return True

        try:
            records = users_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            for record in records:
                if record.get("User ID") == user_id:
                    return False
            return True

        except Exception as e:
            logging.error(f"Error checking if user is new ({user_id}): {e}")
            return True

    def mark_user_seen(self, user_id: str) -> bool:
        """Last Seen update is intentionally disabled"""
        return True
