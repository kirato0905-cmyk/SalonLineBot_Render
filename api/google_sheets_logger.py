import os
import json
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import pytz


class GoogleSheetsLogger:
    """Google Sheets logger for Beauty Links.

    運営者向けスプレッドシート改善 追記仕様対応版:
    - Reservations / Users / TodayReservations を日本語ヘッダーで統一
    - TodayReservations に予約ID・予約日を追加
    - 電話番号未登録時は「未登録」で統一表示
    - 所要時間・料金を数値として保存
    - selected_menu_label がある場合はメニュー表示用へ優先反映
    - update_reservation_data() は1行単位更新でAPI呼び出しを削減
    - TodayReservations は全再構築方式を維持しつつ、行生成関数を分離

    互換性:
    - 既存コードが英語ヘッダー名で更新しても動くように内部で日本語キーへ変換する。
    - get_all_reservations() などの戻り値 status は既存ロジック互換のため
      Confirmed / Modified / Cancelled に正規化して返す。
    - Users シートは日本語ヘッダー化しつつ、既存の英語キー前提の呼び出しにも対応する。
    """

    PHONE_UNREGISTERED_LABEL = "未登録"

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
        "予約ID",
        "予約日",
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
        "登録日時",
        "ユーザーID",
        "表示名",
        "電話番号",
        "ステータス",
        "同意有無",
        "同意日時",
    ]

    FIELD_ALIASES = {
        "Timestamp": "登録日時",
        "timestamp": "登録日時",
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
        "selected_menu_label": "メニュー表示用",
        "Services JSON": "メニューJSON",
        "services_json": "メニューJSON",
        "services": "メニューJSON",
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

    USER_FIELD_ALIASES = {
        "Timestamp": "登録日時",
        "timestamp": "登録日時",
        "User ID": "ユーザーID",
        "user_id": "ユーザーID",
        "Display Name": "表示名",
        "display_name": "表示名",
        "Phone Number": "電話番号",
        "phone_number": "電話番号",
        "Status": "ステータス",
        "status": "ステータス",
        "Consented": "同意有無",
        "consented": "同意有無",
        "Consent Date": "同意日時",
        "consent_date": "同意日時",
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

    USER_STATUS_TO_SHEET = {
        "Active": "有効",
        "active": "有効",
        "有効": "有効",
        "Inactive": "停止",
        "inactive": "停止",
        "停止": "停止",
    }

    USER_CONSENT_TO_SHEET = {
        "Yes": "はい",
        "yes": "はい",
        "true": "はい",
        "True": "はい",
        True: "はい",
        "はい": "はい",
        "No": "いいえ",
        "no": "いいえ",
        "false": "いいえ",
        "False": "いいえ",
        False: "いいえ",
        "いいえ": "いいえ",
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

    # =========================================================
    # basic helpers
    # =========================================================
    def _get_tokyo_timestamp(self) -> str:
        return datetime.now(self.tokyo_tz).strftime("%Y-%m-%d %H:%M:%S")

    def _get_tokyo_date(self) -> str:
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
                migrate=True,
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
            elif migrate and worksheet.title == "Users":
                self._migrate_users_sheet_headers(worksheet, actual_headers, expected_headers)
            else:
                worksheet.clear()
                worksheet.append_row(expected_headers)

            self._invalidate_all_cache()
            return True
        except Exception as e:
            logging.error(f"Failed to ensure headers for worksheet '{worksheet.title}': {e}")
            return False

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

    # =========================================================
    # normalization helpers
    # =========================================================
    def _to_sheet_status(self, status: Any) -> str:
        if status is None or status == "":
            return "予約済み"
        return self.SHEET_STATUS_MAP.get(str(status).strip(), str(status).strip())

    def _to_internal_status(self, status: Any) -> str:
        if status is None or status == "":
            return "Confirmed"
        return self.INTERNAL_STATUS_MAP.get(str(status).strip(), str(status).strip())

    def _to_sheet_user_status(self, status: Any) -> str:
        if status is None or status == "":
            return "有効"
        return self.USER_STATUS_TO_SHEET.get(str(status).strip(), str(status).strip())

    def _to_sheet_consent(self, value: Any) -> str:
        if value is None or value == "":
            return "いいえ"
        return self.USER_CONSENT_TO_SHEET.get(value, self.USER_CONSENT_TO_SHEET.get(str(value).strip(), str(value).strip()))

    def _has_consented_value(self, value: Any) -> bool:
        return str(value or "").strip().lower() in {"yes", "true", "はい", "同意済み"}

    def _display_selected_staff(self, selected_staff: Any) -> str:
        value = str(selected_staff or "").strip()
        if value in {"", "free", "指名なし", "未指定", "おまかせ"}:
            return "指名なし"
        return value

    def _normalize_phone_number(self, phone_number: Any, for_sheet: bool = True) -> str:
        value = str(phone_number or "").strip()
        if value in {"", "None", "null", "未登録"}:
            return self.PHONE_UNREGISTERED_LABEL if for_sheet else ""
        return value

    def _to_int_or_blank(self, value: Any) -> Any:
        if value is None or value == "":
            return ""
        if isinstance(value, bool):
            return ""
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else value
        s = str(value).strip()
        if not s:
            return ""
        s = s.replace(",", "").replace("円", "").replace("分", "")
        try:
            numeric = float(s)
            return int(numeric) if numeric.is_integer() else numeric
        except Exception:
            return value

    def _normalize_time_value(self, value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        raw = raw.replace("時", ":").replace("分", "")
        if ":" not in raw and raw.isdigit():
            raw = f"{raw}:00"
        try:
            hour, minute = raw.split(":", 1)
            hour_i = int(hour)
            minute_i = int(minute)
            if not (0 <= hour_i <= 47 and 0 <= minute_i <= 59):
                return str(value)
            return f"{hour_i:02d}:{minute_i:02d}"
        except Exception:
            return str(value)

    def _time_sort_key(self, time_str: Any) -> int:
        normalized = self._normalize_time_value(time_str)
        try:
            hour, minute = normalized.split(":", 1)
            return int(hour) * 60 + int(minute)
        except Exception:
            return 99999

    def _build_service_display_from_services(self, services: Any, fallback: str = "", selected_menu_label: str = "") -> str:
        if selected_menu_label:
            return str(selected_menu_label).strip()
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

    def _build_service_display_from_raw(self, raw_services: Any, fallback: str = "", selected_menu_label: str = "") -> str:
        if selected_menu_label:
            return str(selected_menu_label).strip()
        if not raw_services:
            return fallback or ""
        try:
            parsed = json.loads(raw_services) if isinstance(raw_services, str) else raw_services
            return self._build_service_display_from_services(parsed, fallback=fallback)
        except Exception:
            return fallback or ""

    def _json_dumps_services(self, services: Any) -> str:
        if isinstance(services, str):
            return services
        return json.dumps(services or [], ensure_ascii=False)

    # =========================================================
    # migration
    # =========================================================
    def _migrate_reservations_sheet_headers(self, worksheet, actual_headers: List[str], expected_headers: List[str]) -> None:
        try:
            all_values = worksheet.get_all_values()
            data_rows = all_values[1:] if len(all_values) > 1 else []
            migrated_rows = []
            for row in data_rows:
                old_record = {header: row[i] if i < len(row) else "" for i, header in enumerate(actual_headers)}
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

    def _migrate_users_sheet_headers(self, worksheet, actual_headers: List[str], expected_headers: List[str]) -> None:
        try:
            all_values = worksheet.get_all_values()
            data_rows = all_values[1:] if len(all_values) > 1 else []
            migrated_rows = []
            for row in data_rows:
                old_record = {header: row[i] if i < len(row) else "" for i, header in enumerate(actual_headers)}
                normalized = self._normalize_legacy_user_record(old_record)
                migrated_rows.append(self._user_record_to_row(normalized))

            worksheet.clear()
            worksheet.append_row(expected_headers)
            if migrated_rows:
                worksheet.append_rows(migrated_rows, value_input_option="USER_ENTERED")
            print(f"Migrated Users sheet to Japanese headers. rows={len(migrated_rows)}")
        except Exception as e:
            logging.error(f"Failed to migrate Users headers. Resetting only headers: {e}")
            worksheet.clear()
            worksheet.append_row(expected_headers)

    def _normalize_legacy_reservation_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        def pick(*keys, default=""):
            for key in keys:
                value = record.get(key)
                if value not in (None, ""):
                    return value
            return default

        raw_services = pick("メニューJSON", "Services JSON", default="")
        selected_menu_label = pick("selected_menu_label", "セットメニュー名", default="")
        service_display = pick("メニュー表示用", "Service", default="")
        if selected_menu_label:
            service_display = selected_menu_label
        elif not service_display:
            service_display = self._build_service_display_from_raw(raw_services)

        selected_staff = pick("指名スタッフ", "Selected Staff", default="")
        assigned_staff = pick("実担当スタッフ", "Assigned Staff", "Staff", default="")
        if not assigned_staff:
            assigned_staff = selected_staff

        normalized = {
            "登録日時": pick("登録日時", "Timestamp"),
            "予約ID": pick("予約ID", "Reservation ID"),
            "予約日": pick("予約日", "Date"),
            "開始時間": self._normalize_time_value(pick("開始時間", "Start Time")),
            "終了時間": self._normalize_time_value(pick("終了時間", "End Time")),
            "顧客名": pick("顧客名", "Client Name"),
            "電話番号": self._normalize_phone_number(pick("電話番号", "Phone Number"), for_sheet=True),
            "メニュー表示用": service_display,
            "指名スタッフ": self._display_selected_staff(selected_staff),
            "実担当スタッフ": assigned_staff,
            "所要時間（分）": self._to_int_or_blank(pick("所要時間（分）", "Duration (min)")),
            "料金": self._to_int_or_blank(pick("料金", "Price")),
            "ステータス": self._to_sheet_status(pick("ステータス", "Status", default="Confirmed")),
            "備考": pick("備考", "Notes"),
            "ユーザーID": pick("ユーザーID", "User ID"),
            "メニューJSON": raw_services,
        }
        return normalized

    def _normalize_legacy_user_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        def pick(*keys, default=""):
            for key in keys:
                value = record.get(key)
                if value not in (None, ""):
                    return value
            return default

        return {
            "登録日時": pick("登録日時", "Timestamp"),
            "ユーザーID": pick("ユーザーID", "User ID"),
            "表示名": pick("表示名", "Display Name"),
            "電話番号": self._normalize_phone_number(pick("電話番号", "Phone Number"), for_sheet=False),
            "ステータス": self._to_sheet_user_status(pick("ステータス", "Status", default="Active")),
            "同意有無": self._to_sheet_consent(pick("同意有無", "Consented", default="No")),
            "同意日時": pick("同意日時", "Consent Date"),
        }

    def _record_to_row(self, record: Dict[str, Any]) -> List[Any]:
        return [record.get(header, "") for header in self.RESERVATION_HEADERS]

    def _user_record_to_row(self, record: Dict[str, Any]) -> List[Any]:
        return [record.get(header, "") for header in self.USER_HEADERS]

    def _add_legacy_user_keys(self, user: Dict[str, Any]) -> Dict[str, Any]:
        if not user:
            return user
        enriched = dict(user)
        enriched.update({
            "Timestamp": user.get("登録日時", ""),
            "User ID": user.get("ユーザーID", ""),
            "Display Name": user.get("表示名", ""),
            "Phone Number": user.get("電話番号", ""),
            "Status": user.get("ステータス", ""),
            "Consented": "Yes" if self._has_consented_value(user.get("同意有無")) else "No",
            "Consent Date": user.get("同意日時", ""),
        })
        return enriched

    # =========================================================
    # worksheet access
    # =========================================================
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
            migrate=True,
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
            normalized = [self._normalize_legacy_user_record(record) for record in records]
            self._set_cached_records(cache_key, normalized)
            return normalized
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
            normalized = [self._normalize_legacy_reservation_record(record) for record in records]
            self._set_cached_records(cache_key, normalized)
            return normalized
        except Exception as e:
            logging.error(f"Failed to get reservations from Google Sheets: {e}")
            return []

    # =========================================================
    # reservation functions
    # =========================================================
    def _get_phone_number_by_user_id(self, user_id: str) -> str:
        if not user_id:
            return self.PHONE_UNREGISTERED_LABEL
        try:
            user = self.get_user_by_id(user_id)
            if not user:
                return self.PHONE_UNREGISTERED_LABEL
            return self._normalize_phone_number(user.get("電話番号") or user.get("Phone Number"), for_sheet=True)
        except Exception as e:
            logging.warning(f"Failed to get phone number for user_id={user_id}: {e}")
            return self.PHONE_UNREGISTERED_LABEL

    def save_reservation(self, reservation_data: Dict[str, Any]) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            user_id = reservation_data.get("user_id", "")
            services = reservation_data.get("services", [])
            selected_menu_label = str(reservation_data.get("selected_menu_label", "") or "").strip()
            service_display = self._build_service_display_from_services(
                services,
                fallback=str(reservation_data.get("service", "") or ""),
                selected_menu_label=selected_menu_label,
            )
            selected_staff = reservation_data.get("selected_staff", "")
            assigned_staff = reservation_data.get("assigned_staff") or reservation_data.get("staff", "")

            normalized_record = {
                "登録日時": self._get_tokyo_timestamp(),
                "予約ID": reservation_data.get("reservation_id", ""),
                "予約日": reservation_data.get("date", ""),
                "開始時間": self._normalize_time_value(reservation_data.get("start_time", "")),
                "終了時間": self._normalize_time_value(reservation_data.get("end_time", "")),
                "顧客名": reservation_data.get("client_name", ""),
                "電話番号": self._normalize_phone_number(
                    reservation_data.get("phone_number") or self._get_phone_number_by_user_id(user_id),
                    for_sheet=True,
                ),
                "メニュー表示用": service_display,
                "指名スタッフ": self._display_selected_staff(selected_staff),
                "実担当スタッフ": assigned_staff,
                "所要時間（分）": self._to_int_or_blank(reservation_data.get("duration", reservation_data.get("total_duration", ""))),
                "料金": self._to_int_or_blank(reservation_data.get("price", reservation_data.get("total_price", ""))),
                "ステータス": self._to_sheet_status(reservation_data.get("status", "Confirmed")),
                "備考": reservation_data.get("remarks", reservation_data.get("note", "")),
                "ユーザーID": user_id,
                "メニューJSON": self._json_dumps_services(services),
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
            "phone_number": self._normalize_phone_number(normalized.get("電話番号"), for_sheet=True),
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
        reservations = []
        for record in self._get_reservation_records():
            if record.get("予約ID"):
                reservations.append(self._record_to_reservation(record))
        return reservations

    def get_confirmed_reservations(self) -> list:
        return [res for res in self.get_all_reservations() if res.get("status") in {"Confirmed", "Modified"}]

    def get_user_reservations(self, client_name: str) -> list:
        return [
            res for res in self.get_all_reservations()
            if res["client_name"] == client_name and res.get("status") in {"Confirmed", "Modified"}
        ]

    def get_user_reservations_by_user_id(self, user_id: str) -> list:
        return [
            res for res in self.get_all_reservations()
            if str(res.get("user_id", "")).strip() == str(user_id).strip()
            and res.get("status") in {"Confirmed", "Modified"}
        ]

    def _find_reservation_row(self, reservation_id: str) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
        records = self._get_reservation_records()
        for i, record in enumerate(records, start=2):
            if record.get("予約ID") == reservation_id:
                return i, record
        return None, None

    def update_reservation_status(self, reservation_id: str, status: str) -> bool:
        return self.update_reservation_data(reservation_id, {"Status": status})

    def get_reservation_by_id(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        try:
            _, record = self._find_reservation_row(reservation_id)
            if record:
                return self._record_to_reservation(record)
            return None
        except Exception as e:
            logging.error(f"Failed to get reservation by ID: {e}")
            return None

    def _apply_reservation_field_updates(self, current: Dict[str, Any], field_updates: Dict[str, Any]) -> Dict[str, Any]:
        updated = dict(current)
        raw_services_for_display = None
        selected_menu_label = ""

        for field, value in field_updates.items():
            if field == "selected_menu_label":
                selected_menu_label = str(value or "").strip()
                if selected_menu_label:
                    updated["メニュー表示用"] = selected_menu_label
                continue

            header = self.FIELD_ALIASES.get(field, field)
            if header not in self.RESERVATION_HEADERS:
                continue

            if header == "ステータス":
                value = self._to_sheet_status(value)
            elif header == "指名スタッフ":
                value = self._display_selected_staff(value)
            elif header == "電話番号":
                value = self._normalize_phone_number(value, for_sheet=True)
            elif header in {"開始時間", "終了時間"}:
                value = self._normalize_time_value(value)
            elif header in {"所要時間（分）", "料金"}:
                value = self._to_int_or_blank(value)
            elif header == "メニューJSON":
                value = self._json_dumps_services(value)
                raw_services_for_display = value

            updated[header] = value

        # services / メニューJSON更新時はメニュー表示用も同期。ただし selected_menu_label がある場合はそちら優先。
        if raw_services_for_display is not None:
            display = self._build_service_display_from_raw(
                raw_services_for_display,
                fallback=updated.get("メニュー表示用", ""),
                selected_menu_label=selected_menu_label,
            )
            if display:
                updated["メニュー表示用"] = display

        # Service と selected_menu_label が両方ある場合は selected_menu_label 優先。
        if selected_menu_label:
            updated["メニュー表示用"] = selected_menu_label

        return updated

    def update_reservation_data(self, reservation_id: str, field_updates: Dict[str, Any]) -> bool:
        reservations_worksheet = self._get_reservations_worksheet()
        if not reservations_worksheet:
            return False
        try:
            row_index, current_record = self._find_reservation_row(reservation_id)
            if not row_index or not current_record:
                logging.warning(f"Reservation {reservation_id} not found for data update")
                return False

            updated_record = self._apply_reservation_field_updates(current_record, field_updates)
            row_values = self._record_to_row(updated_record)
            end_col = self._column_number_to_letter(len(self.RESERVATION_HEADERS))
            reservations_worksheet.update(
                f"A{row_index}:{end_col}{row_index}",
                [row_values],
                value_input_option="USER_ENTERED",
            )
            self._invalidate_cache("reservation_records")
            self.refresh_today_reservations()
            print(f"Updated reservation {reservation_id} with fields: {list(field_updates.keys())}")
            return True
        except Exception as e:
            logging.error(f"Failed to update reservation data: {e}")
            return False

    @staticmethod
    def _column_number_to_letter(n: int) -> str:
        result = ""
        while n:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    def get_reservations_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        try:
            return [self._record_to_reservation(record) for record in self._get_reservation_records() if record.get("予約日") == date_str]
        except Exception as e:
            logging.error(f"Error getting reservations for date {date_str}: {e}")
            return []

    # =========================================================
    # TodayReservations
    # =========================================================
    def _build_today_reservation_rows(self, today: str) -> List[List[Any]]:
        rows = []
        for record in self._get_reservation_records():
            if record.get("予約日") != today:
                continue
            status_display = self._to_sheet_status(record.get("ステータス"))
            if status_display == "キャンセル済み":
                continue
            rows.append([
                record.get("予約ID", ""),
                record.get("予約日", ""),
                self._normalize_time_value(record.get("開始時間", "")),
                self._normalize_time_value(record.get("終了時間", "")),
                record.get("顧客名", ""),
                self._normalize_phone_number(record.get("電話番号", ""), for_sheet=True),
                record.get("メニュー表示用", ""),
                record.get("実担当スタッフ", ""),
                status_display,
                record.get("備考", ""),
            ])
        rows.sort(key=lambda r: self._time_sort_key(r[2]))
        return rows

    def refresh_today_reservations(self) -> bool:
        """TodayReservations シートを当日分の予約済み/変更済みに再生成する。

        現段階では全再構築。将来の差分更新に移行しやすいよう、
        行生成処理は _build_today_reservation_rows() に分離している。
        """
        today_worksheet = self._get_today_reservations_worksheet()
        if not today_worksheet:
            return False
        try:
            today = self._get_tokyo_date()
            rows = self._build_today_reservation_rows(today)
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
            _, record = self._find_reservation_row(reservation_id)
            if record:
                user_id = record.get("ユーザーID", "")
                return user_id if user_id else None
            logging.warning(f"Reservation {reservation_id} not found in sheets")
            return None
        except Exception as e:
            logging.error(f"Error getting user ID for reservation {reservation_id}: {e}")
            return None

    # =========================================================
    # user functions
    # =========================================================
    def log_new_user(self, user_id: str, display_name: str, phone_number: str = "") -> bool:
        users_worksheet = self._get_users_worksheet()
        if not users_worksheet:
            logging.error("Users worksheet not available. Cannot log user data.")
            return False
        try:
            existing_records = self._get_users_records()
            for record in existing_records:
                if record.get("ユーザーID") == user_id:
                    print(f"User {user_id} already exists in Users sheet")
                    return True

            user_record = {
                "登録日時": self._get_tokyo_timestamp(),
                "ユーザーID": user_id,
                "表示名": display_name,
                "電話番号": self._normalize_phone_number(phone_number, for_sheet=False),
                "ステータス": "有効",
                "同意有無": "いいえ",
                "同意日時": "",
            }
            users_worksheet.append_row(self._user_record_to_row(user_record), value_input_option="USER_ENTERED")
            self._invalidate_cache("users_records")
            print(f"Successfully logged new user: {display_name} ({user_id})")
            return True
        except Exception as e:
            logging.error(f"Failed to log user data: {e}")
            return False

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        try:
            for record in self._get_users_records():
                if record.get("ユーザーID") == user_id:
                    return self._add_legacy_user_keys(record)
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
            status_col = self.USER_HEADERS.index("ステータス") + 1
            for i, record in enumerate(records, start=2):
                if record.get("ユーザーID") == user_id:
                    users_worksheet.update_cell(i, status_col, self._to_sheet_user_status(status))
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
                if record.get("ユーザーID") == user_id:
                    return self._has_consented_value(record.get("同意有無", ""))
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
            consent_col = self.USER_HEADERS.index("同意有無") + 1
            consent_date_col = self.USER_HEADERS.index("同意日時") + 1
            for i, record in enumerate(records, start=2):
                if record.get("ユーザーID") == user_id:
                    users_worksheet.update_cell(i, consent_col, "はい" if consented else "いいえ")
                    users_worksheet.update_cell(i, consent_date_col, self._get_tokyo_timestamp() if consented else "")
                    self._invalidate_cache("users_records")
                    return True
            return False
        except Exception as e:
            logging.error(f"Error setting consent for user {user_id}: {e}")
            return False

    def mark_user_seen(self, user_id: str) -> bool:
        """互換性維持用。Last Seen は追加・更新しない。"""
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

