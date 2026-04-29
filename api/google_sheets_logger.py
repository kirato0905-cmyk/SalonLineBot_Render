import os
import json
import re
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

    日本語シート名・日本語ヘッダー前提の運営者向け版。

    使用シート:
    - 予約一覧
    - ユーザー一覧
    - 今日の予約

    互換性:
    - 旧英語シート名 Reservations / Users / TodayReservations が存在する場合は、可能な範囲で日本語名へ移行する。
    - 旧英語ヘッダーのデータも日本語ヘッダーへ正規化して扱う。
    - 既存コードが英語キーで更新しても動くようにエイリアス変換する。
    """

    RESERVATIONS_SHEET_TITLE = "予約一覧"
    USERS_SHEET_TITLE = "ユーザー一覧"
    TODAY_RESERVATIONS_SHEET_TITLE = "今日の予約"

    LEGACY_RESERVATIONS_SHEET_TITLE = "Reservations"
    LEGACY_USERS_SHEET_TITLE = "Users"
    LEGACY_TODAY_RESERVATIONS_SHEET_TITLE = "TodayReservations"

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

    USER_HEADERS = [
        "登録日時",
        "ユーザーID",
        "表示名",
        "電話番号",
        "ステータス",
        "同意有無",
        "同意日時",
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
        "同意済み": "はい",
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

    @staticmethod
    def _column_number_to_letter(n: int) -> str:
        result = ""
        while n:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

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
                legacy_title=self.LEGACY_RESERVATIONS_SHEET_TITLE,
                rows=1000,
                cols=len(self.RESERVATION_HEADERS),
                headers=self.RESERVATION_HEADERS,
                migrate=True,
            )
            self.users_worksheet = self._get_or_create_worksheet(
                title=self.USERS_SHEET_TITLE,
                legacy_title=self.LEGACY_USERS_SHEET_TITLE,
                rows=1000,
                cols=len(self.USER_HEADERS),
                headers=self.USER_HEADERS,
                migrate=True,
            )
            self.today_reservations_worksheet = self._get_or_create_worksheet(
                title=self.TODAY_RESERVATIONS_SHEET_TITLE,
                legacy_title=self.LEGACY_TODAY_RESERVATIONS_SHEET_TITLE,
                rows=200,
                cols=len(self.TODAY_RESERVATION_HEADERS),
                headers=self.TODAY_RESERVATION_HEADERS,
                migrate=True,
            )
            self.refresh_today_reservations()
            print("Google Sheets logger initialized successfully")
            print("予約一覧 / ユーザー一覧 / 今日の予約: active")
        except Exception as e:
            logging.error(f"Failed to setup Google Sheets connection: {e}")
            self.reservations_worksheet = None
            self.users_worksheet = None
            self.today_reservations_worksheet = None
            self.spreadsheet = None

    def _get_or_create_worksheet(
        self,
        title: str,
        rows: int,
        cols: int,
        headers: List[str],
        migrate: bool = False,
        legacy_title: str = "",
    ):
        spreadsheet = self._get_spreadsheet()
        if not spreadsheet:
            return None

        # まず日本語シートを使う
        try:
            worksheet = spreadsheet.worksheet(title)
            self._ensure_headers(worksheet, headers, migrate=migrate)
            # 旧英語シートが別で残っている場合、未移行データを日本語シートへ取り込む
            if legacy_title:
                self._merge_legacy_sheet_if_exists(legacy_title, worksheet)
            return worksheet
        except gspread.WorksheetNotFound:
            pass
        except Exception as e:
            logging.error(f"Failed to get worksheet '{title}': {e}")
            return None

        # 日本語シートがない場合は旧英語シートをリネームして使う
        if legacy_title:
            try:
                legacy_worksheet = spreadsheet.worksheet(legacy_title)
                try:
                    legacy_worksheet.update_title(title)
                    print(f"Renamed worksheet: {legacy_title} -> {title}")
                    self._ensure_headers(legacy_worksheet, headers, migrate=migrate)
                    return legacy_worksheet
                except Exception as e:
                    logging.warning(f"Could not rename worksheet {legacy_title} -> {title}: {e}")
                    worksheet = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
                    worksheet.update(
                        f"A1:{self._column_number_to_letter(len(headers))}1",
                        [headers],
                        value_input_option="USER_ENTERED",
                    )
                    self._copy_legacy_sheet_to_new(legacy_worksheet, worksheet)
                    return worksheet
            except gspread.WorksheetNotFound:
                pass
            except Exception as e:
                logging.error(f"Failed to get legacy worksheet '{legacy_title}': {e}")

        try:
            worksheet = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
            worksheet.update(
                f"A1:{self._column_number_to_letter(len(headers))}1",
                [headers],
                value_input_option="USER_ENTERED",
            )
            print(f"Created new worksheet: {title}")
            return worksheet
        except Exception as e:
            logging.error(f"Failed to create worksheet '{title}': {e}")
            return None

    def _ensure_headers(self, worksheet, expected_headers: List[str], migrate: bool = False) -> bool:
        if not worksheet:
            return False
        try:
            actual_headers = worksheet.row_values(1)
            if not actual_headers:
                worksheet.update(
                    f"A1:{self._column_number_to_letter(len(expected_headers))}1",
                    [expected_headers],
                    value_input_option="USER_ENTERED",
                )
                return True
            if actual_headers == expected_headers:
                return True

            logging.warning(f"Header mismatch detected in worksheet '{worksheet.title}'. Updating headers.")
            if migrate and worksheet.title in {self.RESERVATIONS_SHEET_TITLE, self.LEGACY_RESERVATIONS_SHEET_TITLE}:
                self._migrate_sheet_headers(worksheet, actual_headers, expected_headers, self._normalize_legacy_reservation_record, self._record_to_row)
            elif migrate and worksheet.title in {self.USERS_SHEET_TITLE, self.LEGACY_USERS_SHEET_TITLE}:
                self._migrate_sheet_headers(worksheet, actual_headers, expected_headers, self._normalize_legacy_user_record, self._user_record_to_row)
            elif migrate and worksheet.title in {self.TODAY_RESERVATIONS_SHEET_TITLE, self.LEGACY_TODAY_RESERVATIONS_SHEET_TITLE}:
                worksheet.update(
                    f"A1:{self._column_number_to_letter(len(expected_headers))}1",
                    [expected_headers],
                    value_input_option="USER_ENTERED",
                )
            else:
                worksheet.update(
                    f"A1:{self._column_number_to_letter(len(expected_headers))}1",
                    [expected_headers],
                    value_input_option="USER_ENTERED",
                )
            self._invalidate_all_cache()
            return True
        except Exception as e:
            logging.error(f"Failed to ensure headers for worksheet '{getattr(worksheet, 'title', '')}': {e}")
            return False

    def _migrate_sheet_headers(self, worksheet, actual_headers, expected_headers, normalizer, row_builder) -> None:
        try:
            values = worksheet.get_all_values()
            data_rows = values[1:] if len(values) > 1 else []
            migrated_rows = []
            for row in data_rows:
                old_record = {header: row[i] if i < len(row) else "" for i, header in enumerate(actual_headers)}
                migrated_rows.append(row_builder(normalizer(old_record)))

            worksheet.clear()
            worksheet.update(
                f"A1:{self._column_number_to_letter(len(expected_headers))}1",
                [expected_headers],
                value_input_option="USER_ENTERED",
            )
            if migrated_rows:
                worksheet.update(
                    f"A2:{self._column_number_to_letter(len(expected_headers))}{len(migrated_rows) + 1}",
                    migrated_rows,
                    value_input_option="USER_ENTERED",
                )
            print(f"Migrated worksheet '{worksheet.title}'. rows={len(migrated_rows)}")
        except Exception as e:
            logging.error(f"Failed to migrate worksheet '{worksheet.title}': {e}")
            worksheet.update(
                f"A1:{self._column_number_to_letter(len(expected_headers))}1",
                [expected_headers],
                value_input_option="USER_ENTERED",
            )

    def _copy_legacy_sheet_to_new(self, legacy_worksheet, new_worksheet) -> None:
        try:
            headers = legacy_worksheet.row_values(1)
            values = legacy_worksheet.get_all_values()
            rows = values[1:] if len(values) > 1 else []
            if not rows:
                return
            target_title = new_worksheet.title
            if target_title == self.USERS_SHEET_TITLE:
                normalized = [self._user_record_to_row(self._normalize_legacy_user_record({h: r[i] if i < len(r) else "" for i, h in enumerate(headers)})) for r in rows]
                end = self._column_number_to_letter(len(self.USER_HEADERS))
            elif target_title == self.RESERVATIONS_SHEET_TITLE:
                normalized = [self._record_to_row(self._normalize_legacy_reservation_record({h: r[i] if i < len(r) else "" for i, h in enumerate(headers)})) for r in rows]
                end = self._column_number_to_letter(len(self.RESERVATION_HEADERS))
            else:
                return
            if normalized:
                new_worksheet.update(f"A2:{end}{len(normalized)+1}", normalized, value_input_option="USER_ENTERED")
        except Exception as e:
            logging.error(f"Failed to copy legacy worksheet: {e}")

    def _merge_legacy_sheet_if_exists(self, legacy_title: str, dest_worksheet) -> None:
        """日本語シートが既にある状態で旧英語シートも残っている場合、未登録ユーザーなどを救済する。"""
        try:
            spreadsheet = self._get_spreadsheet()
            if not spreadsheet:
                return
            legacy = spreadsheet.worksheet(legacy_title)
        except Exception:
            return

        try:
            # ユーザー一覧だけは電話番号保存失敗の原因になりやすいため、ID単位でマージする。
            if dest_worksheet.title != self.USERS_SHEET_TITLE:
                return
            dest_records = dest_worksheet.get_all_records(expected_headers=self.USER_HEADERS)
            existing_ids = {str(r.get("ユーザーID", "")).strip() for r in dest_records}
            legacy_headers = legacy.row_values(1)
            legacy_values = legacy.get_all_values()[1:]
            rows_to_append = []
            for row in legacy_values:
                old_record = {h: row[i] if i < len(row) else "" for i, h in enumerate(legacy_headers)}
                normalized = self._normalize_legacy_user_record(old_record)
                uid = str(normalized.get("ユーザーID", "")).strip()
                if uid and uid not in existing_ids:
                    rows_to_append.append(self._user_record_to_row(normalized))
                    existing_ids.add(uid)
            if rows_to_append:
                dest_worksheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")
                self._invalidate_cache("users_records")
                print(f"Merged legacy Users rows into ユーザー一覧. rows={len(rows_to_append)}")
        except Exception as e:
            logging.warning(f"Failed to merge legacy sheet '{legacy_title}': {e}")

    # =========================================================
    # cache / worksheet access
    # =========================================================
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
        self._records_cache[cache_key] = {"fetched_at": time.time(), "records": records}

    def _get_reservations_worksheet(self):
        if self.reservations_worksheet:
            return self.reservations_worksheet
        self.reservations_worksheet = self._get_or_create_worksheet(
            title=self.RESERVATIONS_SHEET_TITLE,
            legacy_title=self.LEGACY_RESERVATIONS_SHEET_TITLE,
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
            title=self.USERS_SHEET_TITLE,
            legacy_title=self.LEGACY_USERS_SHEET_TITLE,
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
            title=self.TODAY_RESERVATIONS_SHEET_TITLE,
            legacy_title=self.LEGACY_TODAY_RESERVATIONS_SHEET_TITLE,
            rows=200,
            cols=len(self.TODAY_RESERVATION_HEADERS),
            headers=self.TODAY_RESERVATION_HEADERS,
            migrate=True,
        )
        return self.today_reservations_worksheet

    def _get_users_records(self) -> List[Dict[str, Any]]:
        cache_key = "users_records"
        cached = self._get_cached_records(cache_key)
        if cached is not None:
            return cached
        ws = self._get_users_worksheet()
        if not ws:
            return []
        try:
            records = ws.get_all_records(expected_headers=self.USER_HEADERS)
            normalized = [self._normalize_legacy_user_record(r) for r in records]
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
        ws = self._get_reservations_worksheet()
        if not ws:
            return []
        try:
            records = ws.get_all_records(expected_headers=self.RESERVATION_HEADERS)
            normalized = [self._normalize_legacy_reservation_record(r) for r in records]
            self._set_cached_records(cache_key, normalized)
            return normalized
        except Exception as e:
            logging.error(f"Failed to get reservations from Google Sheets: {e}")
            return []

    # =========================================================
    # normalization helpers
    # =========================================================
    def _to_sheet_status(self, status: Any) -> str:
        if status is None or status == "":
            return "予約済み"
        normalized = self.SHEET_STATUS_MAP.get(str(status).strip())
        if normalized:
            return normalized
        logging.warning(f"Unknown reservation status '{status}'. Falling back to 予約済み.")
        return "予約済み"

    def _to_internal_status(self, status: Any) -> str:
        if status is None or status == "":
            return "Confirmed"
        internal = self.INTERNAL_STATUS_MAP.get(str(status).strip())
        if internal:
            return internal
        logging.warning(f"Unknown internal reservation status '{status}'. Falling back to Confirmed.")
        return "Confirmed"

    def _to_sheet_user_status(self, status: Any) -> str:
        if status is None or status == "":
            return "有効"
        return self.USER_STATUS_TO_SHEET.get(str(status).strip(), str(status).strip())

    def _to_sheet_consent(self, value: Any) -> str:
        if value is None or value == "":
            return "いいえ"
        return self.USER_CONSENT_TO_SHEET.get(value, self.USER_CONSENT_TO_SHEET.get(str(value).strip(), "いいえ"))

    def _has_consented_value(self, value: Any) -> bool:
        return str(value or "").strip().lower() in {"yes", "true", "はい", "同意済み"}

    def _display_selected_staff(self, selected_staff: Any) -> str:
        value = str(selected_staff or "").strip()
        if value in {"", "free", "指名なし", "未指定", "おまかせ"}:
            return "指名なし"
        return value

    def _restore_lost_leading_zero(self, digits: str) -> str:
        """Google Sheets が電話番号を数値扱いして先頭0を落とした場合に復元する。

        例:
        - 7048065920  -> 07048065920
        - 8048065920  -> 08048065920
        - 9048065920  -> 09048065920
        - 5048065920  -> 05048065920

        すでに 0 始まり、または復元対象外の値はそのまま返す。
        """
        digits = str(digits or "").strip()
        if not digits:
            return ""
        if digits.startswith("0"):
            return digits
        # 携帯・IP電話系で先頭0が落ちた典型パターンを復元
        if len(digits) == 10 and digits[0] in {"5", "7", "8", "9"}:
            return "0" + digits
        return digits

    def _clean_phone_digits(self, phone_number: Any) -> str:
        """電話番号を比較・保存しやすい数字のみの形へ正規化する。

        - Google Sheets の文字列化用シングルクォートは除去
        - ハイフン・空白は除去
        - Sheets が数値化して 070... -> 704... になった値は可能な範囲で復元
        """
        value = str(phone_number or "").strip()
        if value.startswith("'"):
            value = value[1:]
        if value in {"", "None", "none", "null", "未登録"}:
            return ""
        # Sheets から 7048065920.0 のように返るケースを救済
        if re.fullmatch(r"\d+\.0", value):
            value = value[:-2]
        cleaned = (
            value.replace("-", "")
            .replace("−", "")
            .replace("ー", "")
            .replace("―", "")
            .replace(" ", "")
            .replace("　", "")
        )
        cleaned = re.sub(r"\D", "", cleaned)
        return self._restore_lost_leading_zero(cleaned)

    def _phone_to_sheet_text(self, phone_number: Any) -> str:
        """Google Sheetsへ電話番号を文字列として保存するための値にする。"""
        digits = self._clean_phone_digits(phone_number)
        if not digits:
            return ""
        return str(digits)

    def _normalize_phone_number(self, phone_number: Any, for_sheet: bool = True) -> str:
        digits = self._clean_phone_digits(phone_number)
        if not digits:
            return self.PHONE_UNREGISTERED_LABEL if for_sheet else ""
        return self._phone_to_sheet_text(digits) if for_sheet else digits

    def _normalize_user_phone_number_for_storage(self, phone_number: Any) -> str:
        return self._phone_to_sheet_text(phone_number)

    def _phone_digits_for_compare(self, phone_number: Any) -> str:
        """比較用に電話番号を数字のみにする。"""
        return self._clean_phone_digits(phone_number)

    def _get_user_phone_for_sheet_by_user_id(self, user_id: str) -> str:
        """ユーザー一覧の電話番号を、予約一覧・今日の予約へ転記できる文字列形式で返す。"""
        if not user_id:
            return ""
        try:
            user = self.get_user_by_id(user_id)
            if not user:
                return ""
            return self._normalize_phone_number(user.get("電話番号") or user.get("Phone Number"), for_sheet=True)
        except Exception as e:
            logging.warning(f"Failed to get user phone for sheet. user_id={user_id}, error={e}")
            return ""

    def _resolve_phone_for_reservation_sheet(self, phone_number: Any, user_id: str = "") -> str:
        """予約一覧・今日の予約に保存する電話番号を決定する。

        優先順位:
        1. ユーザー一覧に正しい電話番号があればそれを優先
        2. 予約データ側の電話番号
        3. 未登録
        """
        sheet_phone = self._normalize_phone_number(phone_number, for_sheet=True)
        user_phone = self._get_user_phone_for_sheet_by_user_id(user_id) if user_id else ""
        user_digits = self._phone_digits_for_compare(user_phone)
        sheet_digits = self._phone_digits_for_compare(sheet_phone)
        if user_digits and len(user_digits) in {10, 11}:
            return self._phone_to_sheet_text(user_digits)
        if sheet_digits and len(sheet_digits) in {10, 11}:
            return self._phone_to_sheet_text(sheet_digits)
        return self.PHONE_UNREGISTERED_LABEL

    def _format_phone_cell_as_text(self, worksheet, row_index: int, col_index: int) -> None:
        try:
            col_letter = self._column_number_to_letter(col_index)
            worksheet.format(f"{col_letter}{row_index}", {"numberFormat": {"type": "TEXT"}})
        except Exception as e:
            logging.warning(f"Failed to format phone cell as text: row={row_index}, col={col_index}, error={e}")

    def _write_phone_cell_as_text(self, worksheet, row_index: int, col_index: int, phone_value: Any) -> bool:
        """電話番号セルをテキスト形式にしてからRAW文字列で更新する。"""
        try:
            value = self._phone_to_sheet_text(phone_value)
            if not value:
                value = self.PHONE_UNREGISTERED_LABEL
            col_letter = self._column_number_to_letter(col_index)
            self._format_phone_cell_as_text(worksheet, row_index, col_index)
            worksheet.update(f"{col_letter}{row_index}", [[str(value)]], value_input_option="RAW")
            return True
        except Exception as e:
            logging.warning(f"Failed to write phone cell as text: row={row_index}, col={col_index}, error={e}")
            return False

    def _format_phone_column_as_text(self, worksheet, col_index: int, start_row: int = 2, end_row: Optional[int] = None) -> None:
        try:
            col_letter = self._column_number_to_letter(col_index)
            end = end_row or max(getattr(worksheet, "row_count", 0) or 1000, 1000)
            worksheet.format(f"{col_letter}{start_row}:{col_letter}{end}", {"numberFormat": {"type": "TEXT"}})
        except Exception as e:
            logging.warning(f"Failed to format phone column as text: col={col_index}, error={e}")

    def _sync_user_phone_numbers_as_text(self) -> bool:
        """ユーザー一覧の電話番号列を文字列保存へ補正する。"""
        ws = self._get_users_worksheet()
        if not ws:
            return False
        try:
            records = self._get_users_records()
            phone_col = self.USER_HEADERS.index("電話番号") + 1
            changed = False
            for row_index, record in enumerate(records, start=2):
                current_phone = record.get("電話番号", "")
                fixed_phone = self._normalize_user_phone_number_for_storage(current_phone)
                if not fixed_phone:
                    continue
                if self._phone_digits_for_compare(current_phone) != self._phone_digits_for_compare(fixed_phone) or str(current_phone) != str(fixed_phone):
                    if self._write_phone_cell_as_text(ws, row_index, phone_col, fixed_phone):
                        changed = True
            if changed:
                self._invalidate_cache("users_records")
            return changed
        except Exception as e:
            logging.warning(f"Failed to sync user phone numbers as text: {e}")
            return False

    def _sync_reservation_phone_numbers_from_users(self) -> bool:
        """予約一覧の電話番号をユーザー一覧の正しい電話番号で補正する。"""
        ws = self._get_reservations_worksheet()
        if not ws:
            return False
        try:
            records = self._get_reservation_records()
            phone_col = self.RESERVATION_HEADERS.index("電話番号") + 1
            changed = False
            for row_index, record in enumerate(records, start=2):
                user_id = str(record.get("ユーザーID") or "").strip()
                current_phone = record.get("電話番号", "")
                fixed_phone = self._resolve_phone_for_reservation_sheet(current_phone, user_id=user_id)
                current_digits = self._phone_digits_for_compare(current_phone)
                fixed_digits = self._phone_digits_for_compare(fixed_phone)
                if fixed_digits and (fixed_digits != current_digits or str(current_phone) != str(fixed_phone)):
                    if self._write_phone_cell_as_text(ws, row_index, phone_col, fixed_phone):
                        changed = True
            if changed:
                self._invalidate_cache("reservation_records")
            return changed
        except Exception as e:
            logging.warning(f"Failed to sync reservation phone numbers from users: {e}")
            return False


    def _to_int_or_blank(self, value: Any) -> Any:
        if value is None or value == "":
            return ""
        if isinstance(value, bool):
            return ""
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else value
        s = str(value).strip().replace(",", "").replace("円", "").replace("分", "")
        if not s:
            return ""
        try:
            n = float(s)
            return int(n) if n.is_integer() else n
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
            h, m = raw.split(":", 1)
            hi, mi = int(h), int(m)
            if 0 <= hi <= 47 and 0 <= mi <= 59:
                return f"{hi:02d}:{mi:02d}"
        except Exception:
            pass
        return str(value)

    def _time_sort_key(self, time_str: Any) -> int:
        try:
            h, m = self._normalize_time_value(time_str).split(":", 1)
            return int(h) * 60 + int(m)
        except Exception:
            return 99999

    def _date_sort_key(self, date_str: Any) -> str:
        value = str(date_str or "").strip()
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return value
        except Exception:
            return "9999-99-99"

    def _build_service_display_from_services(self, services: Any, fallback: str = "", selected_menu_label: str = "") -> str:
        names = []
        if isinstance(services, list):
            for item in services:
                if isinstance(item, dict):
                    name = item.get("service_name") or item.get("name") or item.get("service")
                    if name:
                        names.append(str(name))
                elif item:
                    names.append(str(item))
        generated = " / ".join(names) if names else (fallback or "")
        selected_menu_label = str(selected_menu_label or "").strip()
        if selected_menu_label:
            if names and not any(name in selected_menu_label for name in names):
                logging.warning(f"selected_menu_label may not match services JSON. label='{selected_menu_label}', services={names}")
            return selected_menu_label
        return generated

    def _build_service_display_from_raw(self, raw_services: Any, fallback: str = "", selected_menu_label: str = "") -> str:
        try:
            parsed = json.loads(raw_services) if isinstance(raw_services, str) and raw_services else raw_services
        except Exception:
            parsed = []
        return self._build_service_display_from_services(parsed, fallback=fallback, selected_menu_label=selected_menu_label)

    def _json_dumps_services(self, services: Any) -> str:
        if isinstance(services, str):
            return services
        return json.dumps(services or [], ensure_ascii=False)

    # =========================================================
    # record normalization / row builders
    # =========================================================
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

    def _normalize_legacy_reservation_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
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
        assigned_staff = pick("実担当スタッフ", "Assigned Staff", "Staff", default="") or selected_staff

        return {
            "登録日時": pick("登録日時", "Timestamp"),
            "予約ID": pick("予約ID", "Reservation ID"),
            "予約日": pick("予約日", "Date"),
            "開始時間": self._normalize_time_value(pick("開始時間", "Start Time")),
            "終了時間": self._normalize_time_value(pick("終了時間", "End Time")),
            "顧客名": pick("顧客名", "Client Name"),
            "電話番号": self._resolve_phone_for_reservation_sheet(
                pick("電話番号", "Phone Number"),
                user_id=pick("ユーザーID", "User ID"),
            ),
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
        ws = self._get_reservations_worksheet()
        if not ws:
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
            assigned_staff = reservation_data.get("assigned_staff") or reservation_data.get("staff", "")

            record = {
                "登録日時": self._get_tokyo_timestamp(),
                "予約ID": reservation_data.get("reservation_id", ""),
                "予約日": reservation_data.get("date", ""),
                "開始時間": self._normalize_time_value(reservation_data.get("start_time", "")),
                "終了時間": self._normalize_time_value(reservation_data.get("end_time", "")),
                "顧客名": reservation_data.get("client_name", ""),
                "電話番号": self._resolve_phone_for_reservation_sheet(
                    reservation_data.get("phone_number") or self._get_phone_number_by_user_id(user_id),
                    user_id=user_id,
                ),
                "メニュー表示用": service_display,
                "指名スタッフ": self._display_selected_staff(reservation_data.get("selected_staff", "")),
                "実担当スタッフ": assigned_staff,
                "所要時間（分）": self._to_int_or_blank(reservation_data.get("duration", reservation_data.get("total_duration", ""))),
                "料金": self._to_int_or_blank(reservation_data.get("price", reservation_data.get("total_price", ""))),
                "ステータス": self._to_sheet_status(reservation_data.get("status", "Confirmed")),
                "備考": reservation_data.get("remarks", reservation_data.get("note", "")),
                "ユーザーID": user_id,
                "メニューJSON": self._json_dumps_services(services),
            }
            ws.append_row(self._record_to_row(record), value_input_option="RAW")
            self._invalidate_cache("reservation_records")

            # append_row 後に電話番号セルだけを明示的に RAW 文字列で上書きする。
            # これで 070... が 704... に変換されるのを防ぐ。
            try:
                phone_col = self.RESERVATION_HEADERS.index("電話番号") + 1
                row_index, _ = self._find_reservation_row(record.get("予約ID", ""))
                if row_index:
                    self._write_phone_cell_as_text(ws, row_index, phone_col, record.get("電話番号", ""))
                    self._invalidate_cache("reservation_records")
            except Exception as format_error:
                logging.warning(f"Failed to rewrite reservation phone cell as text: {format_error}")

            self.refresh_today_reservations()
            return True
        except Exception as e:
            logging.error(f"Failed to save reservation to Google Sheets: {e}")
            return False

    def _record_to_reservation(self, record: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self._normalize_legacy_reservation_record(record)
        raw_services = normalized.get("メニューJSON", "")
        services = []
        try:
            parsed = json.loads(raw_services) if isinstance(raw_services, str) and raw_services else raw_services
            if isinstance(parsed, list):
                services = parsed
        except Exception:
            services = []
        service_display = normalized.get("メニュー表示用") or self._build_service_display_from_services(services)
        assigned_staff = normalized.get("実担当スタッフ", "")
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
            "selected_staff": normalized.get("指名スタッフ", ""),
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

    def get_all_reservations_sorted(self) -> list:
        reservations = self.get_all_reservations()
        reservations.sort(key=lambda r: (self._date_sort_key(r.get("date")), self._time_sort_key(r.get("start_time"))))
        return reservations

    def get_confirmed_reservations(self) -> list:
        return [r for r in self.get_all_reservations_sorted() if r.get("status") in {"Confirmed", "Modified"}]

    def get_user_reservations(self, client_name: str) -> list:
        return [r for r in self.get_all_reservations_sorted() if r.get("client_name") == client_name and r.get("status") in {"Confirmed", "Modified"}]

    def get_user_reservations_by_user_id(self, user_id: str) -> list:
        return [
            r for r in self.get_all_reservations_sorted()
            if str(r.get("user_id", "")).strip() == str(user_id).strip()
            and r.get("status") in {"Confirmed", "Modified"}
        ]

    def _find_reservation_row(self, reservation_id: str) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
        for i, record in enumerate(self._get_reservation_records(), start=2):
            if record.get("予約ID") == reservation_id:
                return i, record
        return None, None

    def get_reservation_by_id(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        _, record = self._find_reservation_row(reservation_id)
        return self._record_to_reservation(record) if record else None

    def update_reservation_status(self, reservation_id: str, status: str) -> bool:
        return self.update_reservation_data(reservation_id, {"Status": status})

    def update_reservation_data(self, reservation_id: str, field_updates: Dict[str, Any]) -> bool:
        ws = self._get_reservations_worksheet()
        if not ws:
            return False
        try:
            row_index, current = self._find_reservation_row(reservation_id)
            if not row_index or not current:
                logging.warning(f"Reservation {reservation_id} not found for data update")
                return False
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
            if raw_services_for_display is not None:
                display = self._build_service_display_from_raw(raw_services_for_display, fallback=updated.get("メニュー表示用", ""), selected_menu_label=selected_menu_label)
                if display:
                    updated["メニュー表示用"] = display
            if selected_menu_label:
                updated["メニュー表示用"] = selected_menu_label
            row_values = self._record_to_row(self._normalize_legacy_reservation_record(updated))
            end_col = self._column_number_to_letter(len(self.RESERVATION_HEADERS))
            ws.update(f"A{row_index}:{end_col}{row_index}", [row_values], value_input_option="RAW")
            try:
                phone_col = self.RESERVATION_HEADERS.index("電話番号") + 1
                self._write_phone_cell_as_text(ws, row_index, phone_col, row_values[phone_col - 1])
            except Exception as format_error:
                logging.warning(f"Failed to rewrite updated reservation phone cell as text: {format_error}")
            self._invalidate_cache("reservation_records")
            self.refresh_today_reservations()
            return True
        except Exception as e:
            logging.error(f"Failed to update reservation data: {e}")
            return False

    def get_reservations_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        rows = [self._record_to_reservation(r) for r in self._get_reservation_records() if r.get("予約日") == date_str]
        rows.sort(key=lambda r: self._time_sort_key(r.get("start_time")))
        return rows

    # =========================================================
    # 今日の予約
    # =========================================================
    def _build_today_reservation_rows(self, today: str) -> List[List[Any]]:
        rows = []
        for record in self._get_reservation_records():
            if record.get("予約日") != today:
                continue
            status = self._to_sheet_status(record.get("ステータス"))
            if status == "キャンセル済み":
                continue
            rows.append([
                record.get("予約ID", ""),
                record.get("予約日", ""),
                self._normalize_time_value(record.get("開始時間", "")),
                self._normalize_time_value(record.get("終了時間", "")),
                record.get("顧客名", ""),
                self._resolve_phone_for_reservation_sheet(record.get("電話番号", ""), user_id=record.get("ユーザーID", "")),
                record.get("メニュー表示用", ""),
                record.get("実担当スタッフ", ""),
                status,
                record.get("備考", ""),
            ])
        rows.sort(key=lambda r: self._time_sort_key(r[2]))
        return rows

    def _clear_today_reservation_data_rows(self, worksheet, row_count_hint: int = 200) -> None:
        try:
            end_col = self._column_number_to_letter(len(self.TODAY_RESERVATION_HEADERS))
            max_rows = max(getattr(worksheet, "row_count", 0) or 0, row_count_hint, 2)
            worksheet.batch_clear([f"A2:{end_col}{max_rows}"])
        except Exception as e:
            logging.warning(f"Failed to clear 今日の予約 data rows: {e}")

    def refresh_today_reservations(self) -> bool:
        ws = self._get_today_reservations_worksheet()
        if not ws:
            return False
        try:
            self._sync_user_phone_numbers_as_text()
            self._sync_reservation_phone_numbers_from_users()
            rows = self._build_today_reservation_rows(self._get_tokyo_date())
            end_col = self._column_number_to_letter(len(self.TODAY_RESERVATION_HEADERS))
            ws.update(f"A1:{end_col}1", [self.TODAY_RESERVATION_HEADERS], value_input_option="USER_ENTERED")
            self._clear_today_reservation_data_rows(ws, row_count_hint=max(200, len(rows) + 10))
            try:
                today_phone_col = self.TODAY_RESERVATION_HEADERS.index("電話番号") + 1
                self._format_phone_column_as_text(ws, today_phone_col, start_row=2, end_row=max(200, len(rows) + 10))
            except Exception as format_error:
                logging.warning(f"Failed to format 今日の予約 phone column as text: {format_error}")
            if rows:
                ws.update(f"A2:{end_col}{len(rows) + 1}", rows, value_input_option="RAW")
            return True
        except Exception as e:
            logging.error(f"Failed to refresh 今日の予約: {e}")
            return False

    def get_user_id_for_reservation(self, reservation_id: str) -> Optional[str]:
        _, record = self._find_reservation_row(reservation_id)
        if record:
            return record.get("ユーザーID") or None
        return None

    # =========================================================
    # user functions
    # =========================================================
    def _find_user_row(self, user_id: str) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
        for i, record in enumerate(self._get_users_records(), start=2):
            if str(record.get("ユーザーID", "")).strip() == str(user_id).strip():
                return i, record
        return None, None

    def log_new_user(self, user_id: str, display_name: str, phone_number: str = "") -> bool:
        ws = self._get_users_worksheet()
        if not ws:
            logging.error("ユーザー一覧 worksheet not available. Cannot log user data.")
            return False
        try:
            row_index, existing = self._find_user_row(user_id)
            if existing:
                # 既存ユーザーで電話番号だけ未登録なら補完する
                normalized_phone = self._normalize_user_phone_number_for_storage(phone_number)
                if normalized_phone and not existing.get("電話番号"):
                    return self.update_user_phone_number(user_id, normalized_phone)
                return True
            record = {
                "登録日時": self._get_tokyo_timestamp(),
                "ユーザーID": user_id,
                "表示名": display_name,
                "電話番号": self._normalize_user_phone_number_for_storage(phone_number),
                "ステータス": "有効",
                "同意有無": "いいえ",
                "同意日時": "",
            }
            ws.append_row(self._user_record_to_row(record), value_input_option="RAW")
            self._invalidate_cache("users_records")

            # append_row 後に電話番号セルだけを明示的にRAW文字列で上書きする。
            try:
                phone_col = self.USER_HEADERS.index("電話番号") + 1
                row_index, _ = self._find_user_row(user_id)
                if row_index:
                    self._write_phone_cell_as_text(ws, row_index, phone_col, record.get("電話番号", ""))
                    self._invalidate_cache("users_records")
            except Exception as format_error:
                logging.warning(f"Failed to rewrite new user phone cell as text: {format_error}")
            return True
        except Exception as e:
            logging.error(f"Failed to log user data: {e}")
            return False

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        try:
            _, record = self._find_user_row(user_id)
            if record:
                return self._add_legacy_user_keys(record)
            return None
        except Exception as e:
            logging.error(f"Error getting user by ID {user_id}: {e}")
            return None

    def update_user_phone_number(self, user_id: str, phone_number: str) -> bool:
        ws = self._get_users_worksheet()
        if not ws:
            return False
        try:
            normalized_phone = self._normalize_user_phone_number_for_storage(phone_number)
            if not normalized_phone:
                logging.warning(f"Empty phone number for user {user_id}")
                return False
            row_index, record = self._find_user_row(user_id)
            if not row_index or not record:
                logging.warning(f"User {user_id} not found for phone number update")
                return False
            updated = dict(record)
            updated["電話番号"] = normalized_phone
            end_col = self._column_number_to_letter(len(self.USER_HEADERS))
            ws.update(f"A{row_index}:{end_col}{row_index}", [self._user_record_to_row(updated)], value_input_option="RAW")
            try:
                phone_col = self.USER_HEADERS.index("電話番号") + 1
                self._write_phone_cell_as_text(ws, row_index, phone_col, normalized_phone)
            except Exception as format_error:
                logging.warning(f"Failed to rewrite updated user phone cell as text: {format_error}")
            self._invalidate_cache("users_records")
            return True
        except Exception as e:
            logging.error(f"Error updating user phone number: {e}")
            return False

    def upsert_user_phone_number(self, user_id: str, display_name: str, phone_number: str) -> bool:
        """電話番号保存の保険付きメソッド。

        ユーザー一覧に既存行がなければ仮登録してから電話番号を保存する。
        同意後電話番号入力で、FollowEvent未発火・旧シート移行漏れがあっても落ちにくくする。
        """
        if not self.get_user_by_id(user_id):
            self.log_new_user(user_id=user_id, display_name=display_name or "Unknown", phone_number="")
        return self.update_user_phone_number(user_id, phone_number)

    def update_user_status(self, user_id: str, status: str) -> bool:
        ws = self._get_users_worksheet()
        if not ws:
            return False
        try:
            row_index, record = self._find_user_row(user_id)
            if not row_index or not record:
                logging.warning(f"User {user_id} not found for status update")
                return False
            updated = dict(record)
            updated["ステータス"] = self._to_sheet_user_status(status)
            end_col = self._column_number_to_letter(len(self.USER_HEADERS))
            ws.update(f"A{row_index}:{end_col}{row_index}", [self._user_record_to_row(updated)], value_input_option="RAW")
            self._invalidate_cache("users_records")
            return True
        except Exception as e:
            logging.error(f"Error updating user status: {e}")
            return False

    def has_user_consented(self, user_id: str) -> bool:
        try:
            _, record = self._find_user_row(user_id)
            return self._has_consented_value(record.get("同意有無", "")) if record else False
        except Exception as e:
            logging.error(f"Error checking consent for user {user_id}: {e}")
            return False

    def set_user_consent(self, user_id: str, consented: bool) -> bool:
        ws = self._get_users_worksheet()
        if not ws:
            return False
        try:
            row_index, record = self._find_user_row(user_id)
            if not row_index or not record:
                # 同意時にユーザー行がない場合も最低限作る
                if not self.log_new_user(user_id=user_id, display_name="Unknown", phone_number=""):
                    return False
                row_index, record = self._find_user_row(user_id)
                if not row_index or not record:
                    return False
            updated = dict(record)
            updated["同意有無"] = "はい" if consented else "いいえ"
            updated["同意日時"] = self._get_tokyo_timestamp() if consented else ""
            end_col = self._column_number_to_letter(len(self.USER_HEADERS))
            ws.update(f"A{row_index}:{end_col}{row_index}", [self._user_record_to_row(updated)], value_input_option="RAW")
            self._invalidate_cache("users_records")
            return True
        except Exception as e:
            logging.error(f"Error setting consent for user {user_id}: {e}")
            return False

    def mark_user_seen(self, user_id: str) -> bool:
        return True

    def mark_user_consented(self, user_id: str) -> bool:
        return self.set_user_consent(user_id, True)

    def revoke_user_consent(self, user_id: str) -> bool:
        return self.set_user_consent(user_id, False)

    def is_new_user(self, user_id: str) -> bool:
        return self.get_user_by_id(user_id) is None


_sheets_logger_instance = None


def get_sheets_logger() -> GoogleSheetsLogger:
    global _sheets_logger_instance
    if _sheets_logger_instance is None:
        _sheets_logger_instance = GoogleSheetsLogger()
    return _sheets_logger_instance


