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
    """Google Sheets logger for Beauty Links.

    運営者向けスプレッドシート改善版:
    - Reservations シートの列名を日本語化
    - 電話番号、備考列を追加
    - メニュー表示用とメニューJSONを分離
    - 指名スタッフ / 実担当スタッフに整理
    - シート上のステータスを日本語化
    - TodayReservations シートを自動更新
    - Users シートは初回登録・同意管理のみ維持

    互換性:
    - 既存コード側が英語ヘッダー名で update_reservation_data() を呼んでも動作するよう、
      英語キー -> 日本語キーの変換を内部で行う。
    - get_all_reservations() などの戻り値 status は既存ロジック互換のため
      Confirmed / Cancelled / Modified に正規化して返す。
    """

    RESERVATION_HEADERS = [
        "登録日時",
        "予約ID",
        "予約日",
        "開始時間",
        "終了時間",
        "顧客名",
        "電話番号",
        "メニュー表示用",
        "指名スタッフ",
        "実担当スタッフ",
        "所要時間（分）",
        "料金",
        "ステータス",
        "備考",
        "ユーザーID",
        "メニューJSON",
    ]

    TODAY_RESERVATION_HEADERS = [
        "開始時間",
        "終了時間",
        "顧客名",
        "電話番号",
        "メニュー表示用",
        "実担当スタッフ",
        "ステータス",
        "備考",
    ]

    USER_HEADERS = [
        "Timestamp",
        "User ID",
        "Display Name",
        "Phone Number",
        "Status",
        "Consented",
        "Consent Date",
    ]

    # 旧英語ヘッダー / 内部フィールド名との互換マップ
    FIELD_ALIASES = {
        "Timestamp": "登録日時",
        "Reservation ID": "予約ID",
        "reservation_id": "予約ID",
        "User ID": "ユーザーID",
        "user_id": "ユーザーID",
        "Client Name": "顧客名",
        "client_name": "顧客名",
        "Phone Number": "電話番号",
        "phone_number": "電話番号",
        "Date": "予約日",
        "date": "予約日",
        "Start Time": "開始時間",
        "start_time": "開始時間",
        "End Time": "終了時間",
        "end_time": "終了時間",
        "Service": "メニュー表示用",
        "service": "メニュー表示用",
        "Services JSON": "メニューJSON",
        "services_json": "メニューJSON",
        "Selected Staff": "指名スタッフ",
        "selected_staff": "指名スタッフ",
        "Assigned Staff": "実担当スタッフ",
        "Staff": "実担当スタッフ",
        "assigned_staff": "実担当スタッフ",
        "staff": "実担当スタッフ",
        "Duration (min)": "所要時間（分）",
        "duration": "所要時間（分）",
        "total_duration": "所要時間（分）",
        "Price": "料金",
        "price": "料金",
        "total_price": "料金",
        "Status": "ステータス",
        "status": "ステータス",
        "Notes": "備考",
        "note": "備考",
        "remarks": "備考",
        "備考": "備考",
    }

    SHEET_STATUS_MAP = {
        "Confirmed": "予約済み",
        "confirmed": "予約済み",
        "予約済み": "予約済み",
        "Modified": "変更済み",
        "modified": "変更済み",
        "変更済み": "変更済み",
        "Cancelled": "キャンセル済み",
        "Canceled": "キャンセル済み",
        "cancelled": "キャンセル済み",
        "canceled": "キャンセル済み",
        "キャンセル済み": "キャンセル済み",
    }

    INTERNAL_STATUS_MAP = {
        "予約済み": "Confirmed",
        "変更済み": "Modified",
        "キャンセル済み": "Cancelled",
        "Confirmed": "Confirmed",
        "Modified": "Modified",
        "Cancelled": "Cancelled",
        "Canceled": "Cancelled",
    }

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
        self.today_reservations_worksheet = None
        self.spreadsheet = None
        self.tokyo_tz = pytz.timezone("Asia/Tokyo")
        self._records_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl_seconds = 8
        self._setup_connection()
        self._initialized = True

    def _get_tokyo_timestamp(self) -> str:
        tokyo_time = datetime.now(self.tokyo_tz)
        return tokyo_time.strftime("%Y-%m-%d %H:%M:%S")

    def _get_tokyo_date(self) -> str:
        tokyo_time = datetime.now(self.tokyo_tz)
        return tokyo_time.strftime("%Y-%m-%d")

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
            "https://www.googleapis.com/auth/drive",
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
                headers=self.RESERVATION_HEADERS,
                migrate=True,
            )
            self.users_worksheet = self._get_or_create_worksheet(
                title="Users",
                rows=1000,
                cols=len(self.USER_HEADERS),
                headers=self.USER_HEADERS,
                migrate=False,
            )
            self.today_reservations_worksheet = self._get_or_create_worksheet(
                title="TodayReservations",
                rows=200,
                cols=len(self.TODAY_RESERVATION_HEADERS),
                headers=self.TODAY_RESERVATION_HEADERS,
                migrate=False,
            )
            self.refresh_today_reservations()
            print("Google Sheets logger initialized successfully")
            print("Reservations / Users / TodayReservations: active")
        except Exception as e:
            logging.error(f"Failed to setup Google Sheets connection: {e}")
            self.reservations_worksheet = None
            self.users_worksheet = None
            self.today_reservations_worksheet = None
            self.spreadsheet = None

    def _get_or_create_worksheet(self, title: str, rows: int, cols: int, headers: List[str], migrate: bool = False):
        spreadsheet = self._get_spreadsheet()
        if not spreadsheet:
            return None

        try:
            worksheet = spreadsheet.worksheet(title)
            self._ensure_headers(worksheet, headers, migrate=migrate)
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

    def _ensure_headers(self, worksheet, expected_headers: List[str], migrate: bool = False) -> bool:
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

            logging.warning(f"Header mismatch detected in worksheet '{worksheet.title}'. Updating headers.")

            if migrate and worksheet.title == "Reservations":
                self._migrate_reservations_sheet_headers(worksheet, actual_headers, expected_headers)
            else:
                worksheet.clear()
                worksheet.append_row(expected_headers)

            self._invalidate_all_cache()
            return True
        except Exception as e:
            logging.error(f"Failed to ensure headers for worksheet '{worksheet.title}': {e}")
            return False

    def _migrate_reservations_sheet_headers(self, worksheet, actual_headers: List[str], expected_headers: List[str]) -> None:
        """旧Reservationsヘッダーから新ヘッダーへ可能な範囲でデータ移行する。"""
        try:
            all_values = worksheet.get_all_values()
            data_rows = all_values[1:] if len(all_values) > 1 else []
            migrated_rows = []
            for row in data_rows:
                old_record = {}
                for i, header in enumerate(actual_headers):
                    old_record[header] = row[i] if i < len(row) else ""
                normalized = self._normalize_legacy_reservation_record(old_record)
                migrated_rows.append(self._record_to_row(normalized))

            worksheet.clear()
            worksheet.append_row(expected_headers)
            if migrated_rows:
                worksheet.append_rows(migrated_rows, value_input_option="USER_ENTERED")
            print(f"Migrated Reservations sheet to Japanese headers. rows={len(migrated_rows)}")
        except Exception as e:
            logging.error(f"Failed to migrate Reservations headers. Resetting only headers: {e}")
            worksheet.clear()
            worksheet.append_row(expected_headers)

    def _normalize_legacy_reservation_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """旧/新どちらの列名でも新ヘッダーのdictへ正規化する。"""
        def pick(*keys, default=""):
            for key in keys:
                value = record.get(key)
                if value not in (None, ""):
                    return value
            return default

        raw_services = pick("メニューJSON", "Services JSON", default="")
        service_display = pick("メニュー表示用", "Service", default="")
        if not service_display:
            service_display = self._build_service_display_from_raw(raw_services)

        selected_staff = pick("指名スタッフ", "Selected Staff", default="")
        assigned_staff = pick("実担当スタッフ", "Assigned Staff", "Staff", default="")
        if not assigned_staff:
            assigned_staff = selected_staff

        normalized = {
            "登録日時": pick("登録日時", "Timestamp"),
            "予約ID": pick("予約ID", "Reservation ID"),
            "予約日": pick("予約日", "Date"),
            "開始時間": pick("開始時間", "Start Time"),
            "終了時間": pick("終了時間", "End Time"),
            "顧客名": pick("顧客名", "Client Name"),
            "電話番号": pick("電話番号", "Phone Number"),
            "メニュー表示用": service_display,
            "指名スタッフ": self._display_selected_staff(selected_staff),
            "実担当スタッフ": assigned_staff,
            "所要時間（分）": pick("所要時間（分）", "Duration (min)"),
            "料金": pick("料金", "Price"),
            "ステータス": self._to_sheet_status(pick("ステータス", "Status", default="Confirmed")),
            "備考": pick("備考", "Notes"),
            "ユーザーID": pick("ユーザーID", "User ID"),
            "メニューJSON": raw_services,
        }
        return normalized

    def _record_to_row(self, record: Dict[str, Any]) -> List[Any]:
        return [record.get(header, "") for header in self.RESERVATION_HEADERS]

    def _get_reservations_worksheet(self):
        if self.reservations_worksheet:
            return self.reservations_worksheet
        self.reservations_worksheet = self._get_or_create_worksheet(
            title="Reservations",
            rows=1000,
            cols=len(self.RESERVATION_HEADERS),
            headers=self.RESERVATION_HEADERS,
            migrate=True,
        )
        return self.reservations_worksheet

    def _get_users_worksheet(self):
        if self.users_worksheet:
            return self.users_worksheet
        self.users_worksheet = self._get_or_create_worksheet(
            title="Users",
            rows=1000,
            cols=len(self.USER_HEADERS),
            headers=self.USER_HEADERS,
            migrate=False,
        )
        return self.users_worksheet

    def _get_today_reservations_worksheet(self):
        if self.today_reservations_worksheet:
            return self.today_reservations_worksheet
        self.today_reservations_worksheet = self._get_or_create_worksheet(
            title="TodayReservations",
            rows=200,
            cols=len(self.TODAY_RESERVATION_HEADERS),
            headers=self.TODAY_RESERVATION_HEADERS,
            migrate=False,
        )
        return self.today_reservations_worksheet

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

    def _to_sheet_status(self, status: Any) -> str:
        if status is None or status == "":
            return "予約済み"
        return self.SHEET_STATUS_MAP.get(str(status).strip(), str(status).strip())

    def _to_internal_status(self, status: Any) -> str:
        if status is None or status == "":
            return "Confirmed"
        return self.INTERNAL_STATUS_MAP.get(str(status).strip(), str(status).strip())

    def _display_selected_staff(self, selected_staff: Any) -> str:
        value = str(selected_staff or "").strip()
        if value in {"", "free", "指名なし", "未指定", "おまかせ"}:
            return "指名なし"
        return value

    def _build_service_display_from_services(self, services: Any, fallback: str = "") -> str:
        if isinstance(services, list):
            names = []
            for item in services:
                if isinstance(item, dict):
                    name = item.get("service_name") or item.get("name") or item.get("service")
                    if name:
                        names.append(str(name))
                elif item:
                    names.append(str(item))
            if names:
                return " / ".join(names)
        return fallback or ""

    def _build_service_display_from_raw(self, raw_services: Any, fallback: str = "") -> str:
        if not raw_services:
            return fallback or ""
        try:
            parsed = json.loads(raw_services) if isinstance(raw_services, str) else raw_services
            return self._build_service_display_from_services(parsed, fallback=fallback)
        except Exception:
            return fallback or ""

    def _get_phone_number_by_user_id(self, user_id: str) -> str:
        if not user_id:
            return ""
        try:
            user = self.get_user_by_id(user_id)
            if not user:
                return ""
            return str(user.get("Phone Number", "") or "")
        except Exception as e:
            logging.warning(f"Failed to get phone number for user_id={user_id}: {e}")
            return ""

    def save_reservation(self, reservation_data: Dict[str, Any]) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            user_id = reservation_data.get("user_id", "")
            services = reservation_data.get("services", [])
            service_display = self._build_service_display_from_services(
                services,
                fallback=str(reservation_data.get("service", "") or ""),
            )
            selected_staff = reservation_data.get("selected_staff", "")
            assigned_staff = reservation_data.get("assigned_staff") or reservation_data.get("staff", "")

            normalized_record = {
                "登録日時": self._get_tokyo_timestamp(),
                "予約ID": reservation_data.get("reservation_id", ""),
                "予約日": reservation_data.get("date", ""),
                "開始時間": reservation_data.get("start_time", ""),
                "終了時間": reservation_data.get("end_time", ""),
                "顧客名": reservation_data.get("client_name", ""),
                "電話番号": reservation_data.get("phone_number") or self._get_phone_number_by_user_id(user_id),
                "メニュー表示用": service_display,
                "指名スタッフ": self._display_selected_staff(selected_staff),
                "実担当スタッフ": assigned_staff,
                "所要時間（分）": reservation_data.get("duration", reservation_data.get("total_duration", "")),
                "料金": reservation_data.get("price", reservation_data.get("total_price", "")),
                "ステータス": self._to_sheet_status(reservation_data.get("status", "Confirmed")),
                "備考": reservation_data.get("remarks", reservation_data.get("note", "")),
                "ユーザーID": user_id,
                "メニューJSON": json.dumps(services, ensure_ascii=False),
            }

            reservations_worksheet.append_row(self._record_to_row(normalized_record), value_input_option="USER_ENTERED")
            self._invalidate_cache("reservation_records")
            self.refresh_today_reservations()
            print(f"Saved reservation {reservation_data.get('reservation_id')} to Google Sheets")
            return True
        except Exception as e:
            logging.error(f"Failed to save reservation to Google Sheets: {e}")
            return False

    def _record_to_reservation(self, record: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self._normalize_legacy_reservation_record(record)
        selected_staff = normalized.get("指名スタッフ", "")
        assigned_staff = normalized.get("実担当スタッフ", "")

        services = []
        raw_services = normalized.get("メニューJSON", "")
        if raw_services:
            try:
                parsed = json.loads(raw_services) if isinstance(raw_services, str) else raw_services
                if isinstance(parsed, list):
                    services = parsed
            except Exception:
                services = []

        service_display = normalized.get("メニュー表示用") or self._build_service_display_from_services(services)

        return {
            "reservation_id": normalized.get("予約ID"),
            "user_id": normalized.get("ユーザーID"),
            "client_name": normalized.get("顧客名"),
            "phone_number": normalized.get("電話番号"),
            "date": normalized.get("予約日"),
            "start_time": normalized.get("開始時間"),
            "end_time": normalized.get("終了時間"),
            "service": service_display,
            "services": services,
            "selected_staff": selected_staff,
            "assigned_staff": assigned_staff,
            "staff": assigned_staff,
            "duration": normalized.get("所要時間（分）"),
            "price": normalized.get("料金"),
            "status": self._to_internal_status(normalized.get("ステータス")),
            "status_display": self._to_sheet_status(normalized.get("ステータス")),
            "remarks": normalized.get("備考", ""),
        }

    def get_all_reservations(self) -> list:
        records = self._get_reservation_records()
        reservations = []
        for record in records:
            normalized = self._normalize_legacy_reservation_record(record)
            if normalized.get("予約ID"):
                reservations.append(self._record_to_reservation(normalized))
        return reservations

    def get_confirmed_reservations(self) -> list:
        return [
            res for res in self.get_all_reservations()
            if res.get("status") in {"Confirmed", "Modified"}
        ]

    def get_user_reservations(self, client_name: str) -> list:
        all_reservations = self.get_all_reservations()
        return [
            res for res in all_reservations
            if res["client_name"] == client_name and res.get("status") in {"Confirmed", "Modified"}
        ]

    def get_user_reservations_by_user_id(self, user_id: str) -> list:
        all_reservations = self.get_all_reservations()
        return [
            res for res in all_reservations
            if str(res.get("user_id", "")).strip() == str(user_id).strip()
            and res.get("status") in {"Confirmed", "Modified"}
        ]

    def update_reservation_status(self, reservation_id: str, status: str) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            records = self._get_reservation_records()
            status_col = self.RESERVATION_HEADERS.index("ステータス") + 1
            sheet_status = self._to_sheet_status(status)
            for i, record in enumerate(records, start=2):
                normalized = self._normalize_legacy_reservation_record(record)
                if normalized.get("予約ID") == reservation_id:
                    reservations_worksheet.update_cell(i, status_col, sheet_status)
                    self._invalidate_cache("reservation_records")
                    self.refresh_today_reservations()
                    print(f"Updated reservation {reservation_id} status to {sheet_status}")
                    return True
            logging.warning(f"Reservation {reservation_id} not found for status update")
            return False
        except Exception as e:
            logging.error(f"Failed to update reservation status: {e}")
            return False

    def get_reservation_by_id(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        try:
            for record in self._get_reservation_records():
                normalized = self._normalize_legacy_reservation_record(record)
                if normalized.get("予約ID") == reservation_id:
                    return self._record_to_reservation(normalized)
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
                normalized = self._normalize_legacy_reservation_record(record)
                if normalized.get("予約ID") == reservation_id:
                    for field, value in field_updates.items():
                        header = self.FIELD_ALIASES.get(field, field)
                        if header not in self.RESERVATION_HEADERS:
                            continue

                        if header == "ステータス":
                            value = self._to_sheet_status(value)
                        elif header == "指名スタッフ":
                            value = self._display_selected_staff(value)
                        elif header == "メニューJSON" and not isinstance(value, str):
                            value = json.dumps(value, ensure_ascii=False)

                        column_index = self.RESERVATION_HEADERS.index(header) + 1
                        reservations_worksheet.update_cell(i, column_index, value)

                    # メニューJSONだけ更新された場合も表示用を可能な範囲で同期
                    if "Services JSON" in field_updates or "メニューJSON" in field_updates:
                        raw = field_updates.get("Services JSON", field_updates.get("メニューJSON"))
                        display = self._build_service_display_from_raw(raw)
                        if display:
                            col = self.RESERVATION_HEADERS.index("メニュー表示用") + 1
                            reservations_worksheet.update_cell(i, col, display)

                    self._invalidate_cache("reservation_records")
                    self.refresh_today_reservations()
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
                normalized = self._normalize_legacy_reservation_record(record)
                if normalized.get("予約日") == date_str:
                    date_reservations.append(self._record_to_reservation(normalized))
            return date_reservations
        except Exception as e:
            logging.error(f"Error getting reservations for date {date_str}: {e}")
            return []

    def refresh_today_reservations(self) -> bool:
        """TodayReservations シートを当日分の予約済み/変更済みに再生成する。"""
        today_worksheet = self._get_today_reservations_worksheet()
        if not today_worksheet:
            return False
        try:
            today = self._get_tokyo_date()
            rows = []
            for record in self._get_reservation_records():
                normalized = self._normalize_legacy_reservation_record(record)
                if normalized.get("予約日") != today:
                    continue
                status_display = self._to_sheet_status(normalized.get("ステータス"))
                if status_display == "キャンセル済み":
                    continue
                rows.append([
                    normalized.get("開始時間", ""),
                    normalized.get("終了時間", ""),
                    normalized.get("顧客名", ""),
                    normalized.get("電話番号", ""),
                    normalized.get("メニュー表示用", ""),
                    normalized.get("実担当スタッフ", ""),
                    status_display,
                    normalized.get("備考", ""),
                ])

            rows.sort(key=lambda r: r[0])
            today_worksheet.clear()
            today_worksheet.append_row(self.TODAY_RESERVATION_HEADERS)
            if rows:
                today_worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            return True
        except Exception as e:
            logging.error(f"Failed to refresh TodayReservations: {e}")
            return False

    def get_user_id_for_reservation(self, reservation_id: str) -> Optional[str]:
        try:
            for record in self._get_reservation_records():
                normalized = self._normalize_legacy_reservation_record(record)
                if normalized.get("予約ID") == reservation_id:
                    user_id = normalized.get("ユーザーID", "")
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
                "No",
                "",
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

    def update_user_status(self, user_id: str, status: str) -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return False
        try:
            records = self._get_users_records()
            for i, record in enumerate(records, start=2):
                if record.get("User ID") == user_id:
                    users_worksheet.update_cell(i, 5, status)
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
                    users_worksheet.update_cell(i, 6, "Yes" if consented else "No")
                    users_worksheet.update_cell(i, 7, self._get_tokyo_timestamp() if consented else "")
                    self._invalidate_cache("users_records")
                    return True
            return False
        except Exception as e:
            logging.error(f"Error setting consent for user {user_id}: {e}")
            return False

    def mark_user_seen(self, user_id: str) -> bool:
        """
        互換性維持用。
        Users シートから Last Seen を削除したため no-op。
        """
        return True

    def mark_user_consented(self, user_id: str) -> bool:
        return self.set_user_consent(user_id, True)

    def revoke_user_consent(self, user_id: str) -> bool:
        return self.set_user_consent(user_id, False)

    def is_new_user(self, user_id: str) -> bool:
        try:
            return self.get_user_by_id(user_id) is None
        except Exception as e:
            logging.error(f"Error checking new user for {user_id}: {e}")
            return True


_sheets_logger_instance = None


def get_sheets_logger() -> GoogleSheetsLogger:
    global _sheets_logger_instance
    if _sheets_logger_instance is None:
        _sheets_logger_instance = GoogleSheetsLogger()
    return _sheets_logger_instance
