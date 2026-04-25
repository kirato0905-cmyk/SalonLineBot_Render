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
    """Google Sheets logger for salon reservations and users.

    改修内容:
    - 予約シートの列名を日本語化
    - 予約シートに電話番号を追加
    - メニュー詳細データ（旧 Services JSON）を右端へ移動
    - シート表示用ステータスを日本語化
    - 備考列を追加
    - キャンセルは削除せず「キャンセル履歴」へ転記
    - 「今日の予約」シートを自動更新

    既存コード互換:
    - 既存の英語ヘッダーのシートも読み取り可能
    - status は内部互換のため Confirmed / Cancelled で返す
    - status_label に日本語ステータスを返す
    """

    RESERVATIONS_SHEET_TITLE = "Reservations"
    USERS_SHEET_TITLE = "Users"
    TODAY_RESERVATIONS_SHEET_TITLE = "今日の予約"
    CANCELLATION_HISTORY_SHEET_TITLE = "キャンセル履歴"

    RESERVATION_HEADERS = [
        "予約作成日時",
        "最終更新日時",
        "予約ID",
        "お客様名",
        "電話番号",
        "来店日",
        "開始時間",
        "終了時間",
        "メニュー",
        "お客様の選択",
        "実際の担当者",
        "担当者",
        "所要時間（分）",
        "金額",
        "ステータス",
        "備考",
        "LINEユーザーID",
        "メニュー詳細データ",
    ]

    # 旧 Reservations ヘッダー。既存データ移行・互換読取に使う。
    LEGACY_RESERVATION_HEADERS = [
        "Timestamp",
        "Reservation ID",
        "User ID",
        "Client Name",
        "Date",
        "Start Time",
        "End Time",
        "Service",
        "Services JSON",
        "Selected Staff",
        "Assigned Staff",
        "Staff",
        "Duration (min)",
        "Price",
        "Status",
    ]

    USER_HEADERS = [
        "初回登録日時",
        "LINEユーザーID",
        "LINE表示名",
        "電話番号",
        "状態",
        "同意済み",
        "同意日時",
    ]

    LEGACY_USER_HEADERS = [
        "Timestamp",
        "User ID",
        "Display Name",
        "Phone Number",
        "Status",
        "Consented",
        "Consent Date",
    ]

    TODAY_RESERVATION_HEADERS = [
        "開始時間",
        "お客様名",
        "電話番号",
        "メニュー",
        "担当者",
        "来店状況",
        "支払い状況",
        "備考",
        "予約ID",
    ]

    CANCELLATION_HISTORY_HEADERS = [
        "キャンセル日時",
        "予約ID",
        "お客様名",
        "電話番号",
        "来店予定日",
        "開始時間",
        "終了時間",
        "メニュー",
        "担当者",
        "キャンセル理由",
        "LINEユーザーID",
    ]

    FIELD_ALIASES = {
        # reservation sheet aliases
        "予約作成日時": ["予約作成日時", "Timestamp"],
        "最終更新日時": ["最終更新日時"],
        "予約ID": ["予約ID", "Reservation ID"],
        "お客様名": ["お客様名", "Client Name"],
        "電話番号": ["電話番号", "Phone Number"],
        "来店日": ["来店日", "Date"],
        "開始時間": ["開始時間", "Start Time"],
        "終了時間": ["終了時間", "End Time"],
        "メニュー": ["メニュー", "Service"],
        "お客様の選択": ["お客様の選択", "Selected Staff"],
        "実際の担当者": ["実際の担当者", "Assigned Staff"],
        "担当者": ["担当者", "Staff"],
        "所要時間（分）": ["所要時間（分）", "Duration (min)"],
        "金額": ["金額", "Price"],
        "ステータス": ["ステータス", "Status"],
        "備考": ["備考", "Note", "Notes", "Memo"],
        "LINEユーザーID": ["LINEユーザーID", "User ID"],
        "メニュー詳細データ": ["メニュー詳細データ", "Services JSON"],
        # user sheet aliases
        "初回登録日時": ["初回登録日時", "Timestamp"],
        "LINE表示名": ["LINE表示名", "Display Name"],
        "状態": ["状態", "Status"],
        "同意済み": ["同意済み", "Consented"],
        "同意日時": ["同意日時", "Consent Date"],
    }

    FIELD_UPDATE_ALIASES = {
        "Timestamp": "予約作成日時",
        "Reservation ID": "予約ID",
        "User ID": "LINEユーザーID",
        "Client Name": "お客様名",
        "Phone Number": "電話番号",
        "Date": "来店日",
        "Start Time": "開始時間",
        "End Time": "終了時間",
        "Service": "メニュー",
        "Services JSON": "メニュー詳細データ",
        "Selected Staff": "お客様の選択",
        "Assigned Staff": "実際の担当者",
        "Staff": "担当者",
        "Duration (min)": "所要時間（分）",
        "Price": "金額",
        "Status": "ステータス",
        "Note": "備考",
        "Notes": "備考",
        "Memo": "備考",
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
        self.cancellation_history_worksheet = None
        self.spreadsheet = None
        self.tokyo_tz = pytz.timezone("Asia/Tokyo")
        self._records_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl_seconds = 8
        self._setup_connection()
        self._initialized = True

    def _get_tokyo_timestamp(self) -> str:
        tokyo_time = datetime.now(self.tokyo_tz)
        return tokyo_time.strftime("%Y-%m-%d %H:%M:%S")

    def _get_tokyo_today_str(self) -> str:
        return datetime.now(self.tokyo_tz).strftime("%Y-%m-%d")

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
                title=self.RESERVATIONS_SHEET_TITLE,
                rows=1000,
                cols=len(self.RESERVATION_HEADERS),
                headers=self.RESERVATION_HEADERS,
            )
            self.users_worksheet = self._get_or_create_worksheet(
                title=self.USERS_SHEET_TITLE,
                rows=1000,
                cols=len(self.USER_HEADERS),
                headers=self.USER_HEADERS,
            )
            self.today_reservations_worksheet = self._get_or_create_worksheet(
                title=self.TODAY_RESERVATIONS_SHEET_TITLE,
                rows=200,
                cols=len(self.TODAY_RESERVATION_HEADERS),
                headers=self.TODAY_RESERVATION_HEADERS,
            )
            self.cancellation_history_worksheet = self._get_or_create_worksheet(
                title=self.CANCELLATION_HISTORY_SHEET_TITLE,
                rows=1000,
                cols=len(self.CANCELLATION_HISTORY_HEADERS),
                headers=self.CANCELLATION_HISTORY_HEADERS,
            )
            self.refresh_today_reservations_sheet()
            print("Google Sheets logger initialized successfully")
            print("Reservations / Users / 今日の予約 / キャンセル履歴: active")
        except Exception as e:
            logging.error(f"Failed to setup Google Sheets connection: {e}", exc_info=True)
            self.reservations_worksheet = None
            self.users_worksheet = None
            self.today_reservations_worksheet = None
            self.cancellation_history_worksheet = None
            self.spreadsheet = None

    def _get_or_create_worksheet(self, title: str, rows: int, cols: int, headers: List[str]):
        spreadsheet = self._get_spreadsheet()
        if not spreadsheet:
            return None

        try:
            worksheet = spreadsheet.worksheet(title)
            self._ensure_headers_preserve_data(worksheet, headers)
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

    def _ensure_headers_preserve_data(self, worksheet, expected_headers: List[str]) -> bool:
        """ヘッダー不一致時も既存行をなるべく保持して日本語ヘッダーへ移行する。"""
        if not worksheet:
            return False
        try:
            actual_headers = worksheet.row_values(1)
            if not actual_headers:
                worksheet.append_row(expected_headers)
                return True
            if actual_headers == expected_headers:
                return True

            records = worksheet.get_all_records()
            migrated_rows = []
            for record in records:
                migrated_rows.append([self._get_value(record, header) for header in expected_headers])

            worksheet.clear()
            worksheet.append_row(expected_headers)
            if migrated_rows:
                worksheet.append_rows(migrated_rows, value_input_option="USER_ENTERED")
            self._invalidate_all_cache()
            logging.info(f"Migrated headers for worksheet '{worksheet.title}' without dropping data")
            return True
        except Exception as e:
            logging.error(f"Failed to ensure headers for worksheet '{worksheet.title}': {e}", exc_info=True)
            return False

    def _get_reservations_worksheet(self):
        if self.reservations_worksheet:
            return self.reservations_worksheet
        self.reservations_worksheet = self._get_or_create_worksheet(
            title=self.RESERVATIONS_SHEET_TITLE,
            rows=1000,
            cols=len(self.RESERVATION_HEADERS),
            headers=self.RESERVATION_HEADERS,
        )
        return self.reservations_worksheet

    def _get_users_worksheet(self):
        if self.users_worksheet:
            return self.users_worksheet
        self.users_worksheet = self._get_or_create_worksheet(
            title=self.USERS_SHEET_TITLE,
            rows=1000,
            cols=len(self.USER_HEADERS),
            headers=self.USER_HEADERS,
        )
        return self.users_worksheet

    def _get_today_reservations_worksheet(self):
        if self.today_reservations_worksheet:
            return self.today_reservations_worksheet
        self.today_reservations_worksheet = self._get_or_create_worksheet(
            title=self.TODAY_RESERVATIONS_SHEET_TITLE,
            rows=200,
            cols=len(self.TODAY_RESERVATION_HEADERS),
            headers=self.TODAY_RESERVATION_HEADERS,
        )
        return self.today_reservations_worksheet

    def _get_cancellation_history_worksheet(self):
        if self.cancellation_history_worksheet:
            return self.cancellation_history_worksheet
        self.cancellation_history_worksheet = self._get_or_create_worksheet(
            title=self.CANCELLATION_HISTORY_SHEET_TITLE,
            rows=1000,
            cols=len(self.CANCELLATION_HISTORY_HEADERS),
            headers=self.CANCELLATION_HISTORY_HEADERS,
        )
        return self.cancellation_history_worksheet

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

    def _get_value(self, record: Dict[str, Any], canonical_key: str, default: Any = "") -> Any:
        for key in self.FIELD_ALIASES.get(canonical_key, [canonical_key]):
            if key in record and record.get(key) not in (None, ""):
                return record.get(key)
        return default

    def _normalize_status_label(self, status: Any) -> str:
        raw = str(status or "").strip()
        lower = raw.lower()
        mapping = {
            "confirmed": "予約確定",
            "予約確定": "予約確定",
            "active": "予約確定",
            "modified": "変更済み",
            "変更済み": "変更済み",
            "changed": "変更済み",
            "cancelled": "キャンセル",
            "canceled": "キャンセル",
            "cancel": "キャンセル",
            "cansel": "キャンセル",
            "キャンセル": "キャンセル",
            "キャンセル済み": "キャンセル",
            "completed": "来店済み",
            "done": "来店済み",
            "来店済み": "来店済み",
            "no_show": "無断キャンセル",
            "noshow": "無断キャンセル",
            "無断キャンセル": "無断キャンセル",
        }
        return mapping.get(lower, mapping.get(raw, raw or "予約確定"))

    def _status_label_to_internal(self, status_label: Any) -> str:
        label = self._normalize_status_label(status_label)
        mapping = {
            "予約確定": "Confirmed",
            "変更済み": "Confirmed",  # 変更後も有効予約として扱う
            "キャンセル": "Cancelled",
            "来店済み": "Completed",
            "無断キャンセル": "NoShow",
        }
        return mapping.get(label, str(status_label or ""))

    def _is_active_reservation_status(self, status: Any) -> bool:
        return self._status_label_to_internal(status) == "Confirmed"

    def _get_users_records(self) -> List[Dict[str, Any]]:
        cache_key = "users_records"
        cached = self._get_cached_records(cache_key)
        if cached is not None:
            return cached

        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            return []
        try:
            records = users_worksheet.get_all_records()
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
            records = reservations_worksheet.get_all_records()
            self._set_cached_records(cache_key, records)
            return records
        except Exception as e:
            logging.error(f"Failed to get reservations from Google Sheets: {e}")
            return []

    def _get_phone_number_for_user(self, user_id: str) -> str:
        if not user_id:
            return ""
        user = self.get_user_by_id(user_id)
        if not user:
            return ""
        return str(self._get_value(user, "電話番号", "") or "")

    def save_reservation(self, reservation_data: Dict[str, Any]) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            user_id = reservation_data.get("user_id", "")
            phone_number = reservation_data.get("phone_number") or self._get_phone_number_for_user(user_id)
            selected_staff = reservation_data.get("selected_staff", "")
            assigned_staff = reservation_data.get("assigned_staff") or reservation_data.get("staff", "")
            timestamp = self._get_tokyo_timestamp()
            row_data = [
                timestamp,
                timestamp,
                reservation_data.get("reservation_id", ""),
                reservation_data.get("client_name", ""),
                phone_number or "未入力",
                reservation_data.get("date", ""),
                reservation_data.get("start_time", ""),
                reservation_data.get("end_time", ""),
                reservation_data.get("service", ""),
                selected_staff,
                assigned_staff,
                assigned_staff,
                reservation_data.get("duration", ""),
                reservation_data.get("price", ""),
                self._normalize_status_label(reservation_data.get("status", "Confirmed")),
                reservation_data.get("note", reservation_data.get("備考", "")),
                user_id,
                json.dumps(reservation_data.get("services", []), ensure_ascii=False),
            ]
            reservations_worksheet.append_row(row_data, value_input_option="USER_ENTERED")
            self._invalidate_cache("reservation_records")
            self.refresh_today_reservations_sheet()
            print(f"Saved reservation {reservation_data.get('reservation_id')} to Google Sheets")
            return True
        except Exception as e:
            logging.error(f"Failed to save reservation to Google Sheets: {e}", exc_info=True)
            return False

    def _record_to_reservation(self, record: Dict[str, Any]) -> Dict[str, Any]:
        selected_staff = self._get_value(record, "お客様の選択", "")
        assigned_staff = self._get_value(record, "実際の担当者", "") or self._get_value(record, "担当者", "")
        raw_services = self._get_value(record, "メニュー詳細データ", "")
        services = []
        if raw_services:
            try:
                parsed = json.loads(raw_services)
                if isinstance(parsed, list):
                    services = parsed
            except Exception:
                services = []

# Global logger instance
_sheets_logger_instance = None


def get_sheets_logger():
    """
    Get singleton GoogleSheetsLogger instance.
    Existing modules import this function, so keep it for compatibility.
    """
    global _sheets_logger_instance

    if _sheets_logger_instance is None:
        _sheets_logger_instance = GoogleSheetsLogger()

    return _sheets_logger_instance
