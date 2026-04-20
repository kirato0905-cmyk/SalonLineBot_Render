"""
Reservation flow system with intent detection, candidate suggestions, and confirmation
Current supported flows:
- New reservation
- Reservation modification via re-reservation only
- Reservation cancellation

Added:
- Back navigation ("← 戻る") for:
  - New reservation flow
  - Re-reservation flow from modification
  - Reservation flow via menu introduction
  - Reservation flow via staff introduction
- Safe handling for modification/cancellation when user has no reservations:
  do not enter modify/cancel state until a valid reservation list exists

Updated:
- Reservation deadline rules are now managed by data/settings.json
- Separate limits for create / change / cancel
- Invalid or missing settings fallback to 2 hours
- settings.json is reloaded on every rule read for immediate reflection
- Modification/cancellation selection also checks deadline immediately
- If modification/cancellation is not allowed due to deadline, that flow ends immediately
"""

import re
import os
import json
import time
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta, date

from api.google_calendar import GoogleCalendarHelper
from api.business_hours import (
    get_slot_minutes,
    is_open_date,
    get_reservation_ui_limit_days,
)


class ReservationFlow:
    def __init__(self):
        self.user_states = {}
        self.google_calendar = GoogleCalendarHelper()
        self.line_configuration = None

        self.services_data = self._load_services_data()
        self.services = self.services_data.get("services", {})
        self.staff_members = self.services_data.get("staff", {})

        self.keywords_data = self._load_keywords_data()
        self.intent_keywords = self.keywords_data.get("intent_keywords", {})
        self.navigation_keywords = self.keywords_data.get("navigation_keywords", {})
        self.confirmation_keywords = self.keywords_data.get("confirmation_keywords", {})

        self.settings_data = self._load_settings_data()

        self.back_label = "← 戻る"

    def _load_services_data(self) -> Dict[str, Any]:
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            services_file = os.path.join(current_dir, "data", "services.json")

            with open(services_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load services data: {e}")
            raise RuntimeError(f"Cannot load services.json: {e}")

    def _load_keywords_data(self) -> Dict[str, Any]:
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            keywords_file = os.path.join(current_dir, "data", "keywords.json")

            with open(keywords_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load keywords data: {e}")
            raise RuntimeError(f"Cannot load keywords.json: {e}")

    def _load_settings_data(self) -> Dict[str, Any]:
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            settings_file = os.path.join(current_dir, "data", "settings.json")

            if not os.path.exists(settings_file):
                logging.warning("settings.json not found. Using default reservation rules.")
                return {}

            with open(settings_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load settings data: {e}")
            return {}

    def _get_reservation_limit_hours(self, rule_key: str, default: int = 2) -> int:
        """
        Reload settings.json every time for immediate reflection.
        Fallback to default when the setting is missing or invalid.
        """
        try:
            self.settings_data = self._load_settings_data()
            reservation_rules = self.settings_data.get("reservation_rules", {})
            value = reservation_rules.get(rule_key, default)

            if value is None:
                return default

            if isinstance(value, bool):
                return default

            if isinstance(value, str):
                value = value.strip()
                if not value.isdigit():
                    return default
                value = int(value)

            if not isinstance(value, (int, float)):
                return default

            if value < 0:
                return default

            return int(value)
        except Exception as e:
            logging.warning(
                f"Invalid reservation rule '{rule_key}'. Using default={default}. error={e}"
            )
            return default

    def _check_reservation_deadline(
        self,
        date_str: str,
        start_time: str,
        limit_hours: int,
        action_label: str = "予約",
        selection_label: Optional[str] = None,
    ) -> tuple:
        """
        Common deadline checker.
        Rule:
            deadline_datetime = reservation_start - limit_hours
            if current_datetime > deadline_datetime: reject
        """
        try:
            import pytz

            tokyo_tz = pytz.timezone("Asia/Tokyo")

            reservation_datetime_naive = datetime.strptime(
                f"{date_str} {start_time}",
                "%Y-%m-%d %H:%M",
            )
            reservation_datetime = tokyo_tz.localize(reservation_datetime_naive)
            current_datetime = datetime.now(tokyo_tz)

            deadline_datetime = reservation_datetime - timedelta(hours=limit_hours)

            if current_datetime > deadline_datetime:
                target_label = selection_label or "時間帯"
                error_message = (
                    f"申し訳ございませんが、{action_label}は来店の{limit_hours}時間前までとなっております。\n"
                    f"{limit_hours}時間以上先の{target_label}をご選択ください。"
                )
                return False, error_message

            return True, None

        except Exception as e:
            logging.error(f"Error checking reservation deadline: {e}")
            return False, "エラーが発生しました。もう一度お試しください。"

    def _check_existing_reservation_deadline(
        self,
        reservation: Dict[str, Any],
        rule_key: str,
        action_label: str,
    ) -> tuple:
        """
        Existing reservation deadline checker for modification / cancellation.
        reservation must contain:
          - date
          - start_time
        """
        try:
            reservation_date = reservation.get("date")
            reservation_start_time = reservation.get("start_time")

            if not reservation_date or not reservation_start_time:
                return False, "予約情報の取得に失敗しました。もう一度お試しください。"

            limit_hours = self._get_reservation_limit_hours(rule_key, 2)

            import pytz

            tokyo_tz = pytz.timezone("Asia/Tokyo")
            reservation_datetime_naive = datetime.strptime(
                f"{reservation_date} {reservation_start_time}",
                "%Y-%m-%d %H:%M",
            )
            reservation_datetime = tokyo_tz.localize(reservation_datetime_naive)
            current_datetime = datetime.now(tokyo_tz)

            deadline_datetime = reservation_datetime - timedelta(hours=limit_hours)

            if current_datetime > deadline_datetime:
                return (
                    False,
                    f"申し訳ございませんが、{action_label}は予約開始時刻の{limit_hours}時間前までとなっております。\n"
                    f"この予約は締切時間を過ぎているため、お手続きできません。\n"
                    f"緊急の場合は直接サロンにご連絡ください。"
                )

            return True, None

        except Exception as e:
            logging.error(f"Error checking existing reservation deadline: {e}")
            return False, "エラーが発生しました。もう一度お試しください。"

    def _calculate_time_duration_minutes(self, start_time: str, end_time: str) -> int:
        try:
            start_hour, start_minute = map(int, start_time.split(":"))
            end_hour, end_minute = map(int, end_time.split(":"))

            start_total_minutes = start_hour * 60 + start_minute
            end_total_minutes = end_hour * 60 + end_minute

            return end_total_minutes - start_total_minutes
        except (ValueError, IndexError):
            return 0

    def _calculate_optimal_end_time(self, start_time: str, service_duration_minutes: int) -> str:
        try:
            start_hour, start_minute = map(int, start_time.split(":"))
            start_total_minutes = start_hour * 60 + start_minute

            end_total_minutes = start_total_minutes + service_duration_minutes

            end_hour = end_total_minutes // 60
            end_minute = end_total_minutes % 60

            return f"{end_hour:02d}:{end_minute:02d}"
        except (ValueError, IndexError):
            return start_time

    def _get_service_by_id(self, service_id: str) -> Optional[Dict[str, Any]]:
        if not service_id:
            return None
        normalized = str(service_id).strip()
        for _key, data in self.services.items():
            if isinstance(data, dict) and data.get("id") and str(data.get("id")).lower() == normalized.lower():
                return data
        return None

    def _get_service_name_by_id(self, service_id: str) -> str:
        svc = self._get_service_by_id(service_id)
        return svc.get("name", service_id) if svc else service_id

    def _get_current_service_id(self, user_id: str) -> Optional[str]:
        data = self.user_states.get(user_id, {}).get("data", {})
        sid = data.get("service_id")
        if sid:
            return sid
        name = data.get("service")
        if name:
            return self._get_service_id_by_name(name)
        return None

    def _get_service_id_by_name(self, service_name: str) -> Optional[str]:
        for _key, service_data in self.services.items():
            if isinstance(service_data, dict) and service_data.get("name") == service_name:
                return service_data.get("id")
        return None

    def _is_back_command(self, message: str) -> bool:
        raw = str(message).strip()
        return raw in [self.back_label, "戻る"]

    def _quick_reply_return(
        self,
        text: str,
        items: List[Dict[str, Any]],
        include_cancel: bool = True,
        include_back: bool = False,
    ) -> Dict[str, Any]:
        final_items = []

        if include_back:
            final_items.append({"label": self.back_label, "text": self.back_label})

        final_items.extend(list(items))

        if include_cancel:
            final_items.append({"label": "キャンセル", "text": "キャンセル"})

        return {"text": text, "quick_reply_items": final_items}

    def _clear_reservation_selection_after_service(self, user_id: str):
        state = self.user_states.get(user_id, {})
        data = state.get("data", {})
        for key in ["date", "start_time", "end_time", "time"]:
            data.pop(key, None)

        for key in [
            "time_options",
            "time_slot_page",
            "time_selection_date",
            "time_selection_service_duration",
            "time_filtered_periods",
        ]:
            state.pop(key, None)

    def _clear_reservation_selection_after_staff(self, user_id: str):
        state = self.user_states.get(user_id, {})
        data = state.get("data", {})
        for key in ["date", "start_time", "end_time", "time"]:
            data.pop(key, None)

        for key in [
            "time_options",
            "time_slot_page",
            "time_selection_date",
            "time_selection_service_duration",
            "time_filtered_periods",
            "date_selection_week_start",
        ]:
            state.pop(key, None)

    def _clear_time_selection(self, user_id: str):
        state = self.user_states.get(user_id, {})
        data = state.get("data", {})
        for key in ["start_time", "end_time", "time"]:
            data.pop(key, None)

    def _build_time_options_30min(
        self,
        filtered_periods: List[Dict[str, Any]],
        service_duration_minutes: int,
    ) -> List[str]:
        slot_minutes = get_slot_minutes()
        start_times_set = set()

        for period in filtered_periods:
            p_start = period["time"]
            p_end = period["end_time"]
            try:
                start_h, start_m = map(int, p_start.split(":"))
                end_h, end_m = map(int, p_end.split(":"))
                start_min = start_h * 60 + start_m
                end_min = end_h * 60 + end_m

                t = start_min
                while t + service_duration_minutes <= end_min:
                    h, m = divmod(t, 60)
                    start_times_set.add(f"{h:02d}:{m:02d}")
                    t += slot_minutes
            except (ValueError, KeyError):
                continue

        return sorted(start_times_set)

    def _build_time_selection_quick_reply(
        self,
        user_id: str,
        text: str,
        page: int,
    ) -> Dict[str, Any]:
        time_options = self.user_states[user_id].get("time_options", [])
        per_page = 8
        total_pages = max(1, (len(time_options) + per_page - 1) // per_page)
        page = max(0, min(page, total_pages - 1))
        self.user_states[user_id]["time_slot_page"] = page

        start_i = page * per_page
        page_times = time_options[start_i:start_i + per_page]

        items = []
        if page > 0:
            items.append({"label": "前へ", "text": "前へ"})
        for t in page_times:
            items.append({"label": t, "text": t})
        if page < total_pages - 1:
            items.append({"label": "次へ", "text": "次へ"})

        return self._quick_reply_return(
            text,
            items,
            include_cancel=True,
            include_back=True,
        )

    def _normalize_service_input(self, text: str) -> str:
        if not text:
            return ""
        s = str(text)
        s = s.replace("＋", "+").replace("\u3000", " ")
        s = re.sub(r"\s+", " ", s)
        s = re.sub(r"\s*([+])\s*", r"\1", s)
        return s.strip()

    def _fallback_match_service_by_text(self, normalized_input: str) -> List[tuple]:
        all_services = []
        for _key, data in self.services.items():
            if not isinstance(data, dict) or not data.get("id"):
                continue
            name = data.get("name", "")
            if not name:
                continue
            all_services.append((data.get("id"), data))

        if not all_services:
            return []

        for sid, data in all_services:
            name = data.get("name", "")
            norm_name = self._normalize_service_input(name)
            if normalized_input == name or normalized_input == norm_name:
                return [(sid, data)]

        sorted_by_len = sorted(all_services, key=lambda x: len(x[1].get("name", "")), reverse=True)
        partial_matches = []
        for sid, data in sorted_by_len:
            name = data.get("name", "")
            norm_name = self._normalize_service_input(name)
            if (
                normalized_input in name
                or normalized_input in norm_name
                or name in normalized_input
                or norm_name in normalized_input
            ):
                partial_matches.append((sid, data))

        return partial_matches

    def _has_single_staff(self) -> bool:
        active_staff = [
            staff
            for staff_id, staff in self.staff_members.items()
            if staff.get("name") != "未指定"
        ]
        return len(active_staff) == 1

    def _get_single_staff_name(self) -> Optional[str]:
        active_staff = [
            staff
            for staff_id, staff in self.staff_members.items()
            if staff.get("name") != "未指定"
        ]
        if len(active_staff) == 1:
            return active_staff[0].get("name")
        return None

    def _get_staff_calendar_url(self, staff_name: str) -> str:
        staff_calendar_id = None
        for staff_id, staff_data in self.staff_members.items():
            if staff_data.get("name") == staff_name:
                staff_calendar_id = staff_data.get("calendar_id")
                break

        if staff_calendar_id:
            return f"https://calendar.google.com/calendar/embed?src={staff_calendar_id}&ctz=Asia%2FTokyo"
        return "https://calendar.google.com/calendar"

    def _get_available_slots(
        self,
        selected_date: str = None,
        staff_name: str = None,
        user_id: str = None,
    ) -> List[Dict[str, Any]]:
        if selected_date is None:
            selected_date = datetime.now().strftime("%Y-%m-%d")

        original_reservation = None
        if user_id and user_id in self.user_states:
            if self.user_states[user_id].get("is_modification", False):
                original_reservation = self.user_states[user_id].get("original_reservation")

        if staff_name:
            exclude_reservation_id = None
            if original_reservation and original_reservation.get("date") == selected_date:
                exclude_reservation_id = original_reservation.get("reservation_id")

            staff_slots = self.google_calendar.get_available_slots_for_modification(
                selected_date,
                exclude_reservation_id,
                staff_name,
            )

            if original_reservation and original_reservation.get("date") == selected_date:
                original_start_time = original_reservation.get("start_time")
                original_end_time = original_reservation.get("end_time")
                if original_start_time and original_end_time:
                    original_slot_exists = False
                    for slot in staff_slots:
                        if slot.get("time") == original_start_time and slot.get("end_time") == original_end_time:
                            original_slot_exists = True
                            break

                    if not original_slot_exists:
                        original_slot = {
                            "date": selected_date,
                            "time": original_start_time,
                            "end_time": original_end_time,
                            "available": True,
                        }
                        staff_slots.append(original_slot)
                        staff_slots.sort(key=lambda x: x.get("time", ""))

            return staff_slots

        start_date = datetime.strptime(selected_date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_date = start_date + timedelta(days=1)

        all_slots = self.google_calendar.get_available_slots(start_date, end_date, staff_name)
        date_slots = [slot for slot in all_slots if slot["date"] == selected_date]

        if original_reservation and original_reservation.get("date") == selected_date:
            original_start_time = original_reservation.get("start_time")
            original_end_time = original_reservation.get("end_time")
            if original_start_time and original_end_time:
                original_slot_exists = False
                for slot in date_slots:
                    if slot.get("time") == original_start_time and slot.get("end_time") == original_end_time:
                        original_slot_exists = True
                        break

                if not original_slot_exists:
                    original_slot = {
                        "date": selected_date,
                        "time": original_start_time,
                        "end_time": original_end_time,
                        "available": True,
                    }
                    date_slots.append(original_slot)
                    date_slots.sort(key=lambda x: x.get("time", ""))

        return date_slots

    @staticmethod
    def _calendar_week_monday(d: date) -> date:
        return d - timedelta(days=d.weekday())

    @staticmethod
    def _date_quick_reply_label(date_str: str) -> str:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        wk = ["月", "火", "水", "木", "金", "土", "日"][d.weekday()]
        return f"{d.month}/{d.day}({wk})"

    def _periods_fittable_for_service(
        self,
        available_periods: List[Dict[str, Any]],
        service_duration: int,
    ) -> List[Dict[str, Any]]:
        filtered = []
        for period in available_periods:
            slot_duration = self._calculate_time_duration_minutes(period["time"], period["end_time"])
            if slot_duration >= service_duration:
                filtered.append(period)
        return filtered

    def _date_has_fittable_slot_new_booking(
        self,
        user_id: str,
        date_str: str,
        staff_name: Optional[str],
        service_duration: int,
    ) -> bool:
        try:
            slots = self._get_available_slots(date_str, staff_name, user_id)
            available_periods = [slot for slot in slots if slot.get("available")]
            return bool(self._periods_fittable_for_service(available_periods, service_duration))
        except Exception as e:
            logging.error(f"[date UI] slot check failed for new booking {date_str}: {e}")
            return False

    def _collect_bookable_dates_in_calendar_week(
        self,
        user_id: str,
        week_start: date,
        today: date,
        last_ui: date,
        *,
        context: str,
    ) -> List[str]:
        dates_out: List[str] = []

        for i in range(7):
            d = week_start + timedelta(days=i)
            if d < today or d > last_ui:
                continue
            if not is_open_date(d):
                continue

            ds = d.strftime("%Y-%m-%d")
            if context == "new_reservation":
                staff_name = self.user_states[user_id]["data"].get("staff")
                sid = self._get_current_service_id(user_id)
                svc = self._get_service_by_id(sid) if sid else {}
                duration = int(svc.get("duration", 60))
                if self._date_has_fittable_slot_new_booking(user_id, ds, staff_name, duration):
                    dates_out.append(ds)

        return dates_out

    def _build_date_week_selection_message(
        self,
        user_id: str,
        *,
        context: str,
        error_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        today = datetime.now().date()
        limit_days = get_reservation_ui_limit_days()
        last_ui = today + timedelta(days=limit_days)
        min_ws = self._calendar_week_monday(today)

        raw_ws = self.user_states[user_id].get("date_selection_week_start")
        if not raw_ws:
            ws = min_ws
            self.user_states[user_id]["date_selection_week_start"] = ws.strftime("%Y-%m-%d")
        else:
            try:
                ws = datetime.strptime(raw_ws, "%Y-%m-%d").date()
            except ValueError:
                ws = min_ws
                self.user_states[user_id]["date_selection_week_start"] = ws.strftime("%Y-%m-%d")
            if ws < min_ws:
                ws = min_ws
                self.user_states[user_id]["date_selection_week_start"] = ws.strftime("%Y-%m-%d")

        bookable = self._collect_bookable_dates_in_calendar_week(
            user_id,
            ws,
            today,
            last_ui,
            context=context,
        )

        show_prev = ws > min_ws
        show_next = (ws + timedelta(days=7)) <= last_ui

        items: List[Dict[str, str]] = []
        if show_prev:
            items.append({"label": "前の週", "text": "前の週"})
        for ds in bookable:
            items.append({"label": self._date_quick_reply_label(ds), "text": ds})
        if show_next:
            items.append({"label": "次の週", "text": "次の週"})

        header = "📅 ご希望の日付をお選びください👇\n\n"
        header += "※土日・午前中は埋まりやすいためお早めのご予約がおすすめです！\n\n"
        header += f"※{limit_days}日以降は「2026-01-07」の形式でご入力ください。"

        text = (f"{error_prefix}\n\n" if error_prefix else "") + header
        return self._quick_reply_return(
            text,
            items,
            include_cancel=True,
            include_back=True,
        )

    def _build_staff_selection_message(self, user_id: str) -> Dict[str, Any]:
        service_id = self.user_states[user_id]["data"].get("service_id")
        service_name = self._get_service_name_by_id(service_id) if service_id else ""

        self.user_states[user_id]["step"] = "staff_selection"

        staff_items = []
        for staff_id, staff_data in self.staff_members.items():
            staff_name = staff_data.get("name", staff_id)
            staff_items.append({"label": staff_name, "text": staff_name})

        text = f"""{service_name}承ります👌

担当スタッフをお選びください👇

【🔥一番人気】
・山田
→指名率No.1／扱いやすいカットが好評

【✨迷ったらこちら】
・おまかせ
→当店おすすめのスタッフが担当します

【その他】
・佐藤
→透明感カラーが得意

・鈴木
→パーマ・似合わせが得意"""
        return self._quick_reply_return(text, staff_items, include_cancel=True, include_back=True)

    def _go_back_one_step(self, user_id: str) -> Union[str, Dict[str, Any]]:
        if user_id not in self.user_states:
            return "現在進行中の予約はありません。"

        state = self.user_states[user_id]
        step = state.get("step")
        data = state.get("data", {})

        if step == "service_selection":
            text = "この画面では戻れません。メニューをお選びください。"
            return self._quick_reply_return(
                text,
                self._build_service_quick_reply_postback_items(),
                include_cancel=True,
                include_back=False,
            )

        if step == "staff_selection":
            data.pop("staff", None)
            self._clear_reservation_selection_after_staff(user_id)
            state["step"] = "service_selection"
            return self._start_reservation(user_id)

        if step == "date_selection":
            self._clear_reservation_selection_after_staff(user_id)
            back_target = state.get("date_selection_back_target", "staff_selection")

            if back_target == "service_selection":
                state["step"] = "service_selection"
                return self._start_reservation(user_id)

            data.pop("staff", None)
            return self._build_staff_selection_message(user_id)

        if step == "time_selection":
            self._clear_time_selection(user_id)
            state["step"] = "date_selection"

            selected_date = data.get("date")
            if selected_date:
                try:
                    selected_date_obj = datetime.strptime(selected_date, "%Y-%m-%d").date()
                    state["date_selection_week_start"] = self._calendar_week_monday(
                        selected_date_obj
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    state["date_selection_week_start"] = self._calendar_week_monday(
                        datetime.now().date()
                    ).strftime("%Y-%m-%d")
            else:
                state["date_selection_week_start"] = self._calendar_week_monday(
                    datetime.now().date()
                ).strftime("%Y-%m-%d")

            return self._build_date_week_selection_message(user_id, context="new_reservation")

        if step == "confirmation":
            self._clear_time_selection(user_id)
            state["step"] = "time_selection"
            selected_date = data.get("date")
            if not selected_date:
                state["step"] = "date_selection"
                return self._build_date_week_selection_message(user_id, context="new_reservation")
            return self._apply_selected_date_go_to_time_selection(user_id, selected_date)

        return "この画面では戻れません。"

    def _apply_selected_date_go_to_time_selection(
        self,
        user_id: str,
        selected_date: str,
    ) -> Union[str, Dict[str, Any]]:
        self.user_states[user_id]["data"]["date"] = selected_date
        self.user_states[user_id]["step"] = "time_selection"

        staff_name = self.user_states[user_id]["data"].get("staff")
        available_slots = self._get_available_slots(selected_date, staff_name, user_id)
        available_periods = [slot for slot in available_slots if slot["available"]]

        sid = self._get_current_service_id(user_id)
        service_info = self._get_service_by_id(sid) if sid else {}
        service_duration = service_info.get("duration", 60)
        service_name = self._get_service_name_by_id(sid) if sid else ""

        filtered_periods = self._periods_fittable_for_service(available_periods, service_duration)

        if not filtered_periods:
            self.user_states[user_id]["step"] = "date_selection"
            err = f"""申し訳ございませんが、{selected_date}は{service_name}（{service_duration}分）の予約可能な時間がありません。

他の日付をお選びください。"""
            return self._build_date_week_selection_message(
                user_id,
                context="new_reservation",
                error_prefix=err,
            )

        can_accommodate = False
        max_slot_duration = 0

        for period in available_periods:
            slot_duration = self._calculate_time_duration_minutes(period["time"], period["end_time"])
            max_slot_duration = max(max_slot_duration, slot_duration)

            if slot_duration >= service_duration:
                can_accommodate = True
                break

        if not can_accommodate:
            self.user_states[user_id]["step"] = "date_selection"

            service_hours = service_duration // 60
            service_minutes = service_duration % 60
            if service_hours > 0 and service_minutes > 0:
                duration_str = f"{service_hours}時間{service_minutes}分"
            elif service_hours > 0:
                duration_str = f"{service_hours}時間"
            else:
                duration_str = f"{service_minutes}分"

            max_hours = max_slot_duration // 60
            max_minutes = max_slot_duration % 60
            if max_hours > 0 and max_minutes > 0:
                max_duration_str = f"{max_hours}時間{max_minutes}分"
            elif max_hours > 0:
                max_duration_str = f"{max_hours}時間"
            else:
                max_duration_str = f"{max_minutes}分"

            err = f"""申し訳ございませんが、{selected_date}の予約可能な時間帯では、{service_name}（{duration_str}）の予約ができません。

📅 選択した日付：{selected_date}
💇 選択したサービス：{service_name}（{duration_str}）
⏱️ この日の最大空き時間：{max_duration_str}

この日付では{service_name}の予約時間が確保できません。

他の日付をお選びください。"""
            return self._build_date_week_selection_message(
                user_id,
                context="new_reservation",
                error_prefix=err,
            )

        time_options = self._build_time_options_30min(filtered_periods, service_duration)
        self.user_states[user_id]["time_options"] = time_options
        self.user_states[user_id]["time_slot_page"] = 0
        self.user_states[user_id]["time_selection_date"] = selected_date
        self.user_states[user_id]["time_selection_service_duration"] = service_duration
        self.user_states[user_id]["time_filtered_periods"] = filtered_periods

        text = f"""{selected_date}ですね👌

{service_name}（{service_duration}分）の空き状況はこちら。

【🔥おすすめ】
・10:00～
・11:00～

【その他】
・13:00～
・15:00～
・17:00～

ご希望の時間をお選びください👇"""
        return self._build_time_selection_quick_reply(user_id, text, page=0)

    def detect_intent(self, message: str, user_id: str = None) -> str:
        message_normalized = message.strip()

        if message_normalized in ["予約変更", "予約を変更", "予約変更したい"]:
            return "modify"

        if user_id and user_id in self.user_states:
            state = self.user_states[user_id]
            step = state.get("step", "")

            if step in [
                "service_selection",
                "staff_selection",
                "date_selection",
                "time_selection",
                "confirmation",
            ]:
                return "reservation_flow"

            if step in [
                "cancel_select_reservation",
                "cancel_confirm",
            ]:
                return "cancel"

            if step in [
                "modify_select_reservation",
            ]:
                return "modify"

        if re.match(r"^RES-\d{8}-\d{4}$", message_normalized):
            return "general"

        if re.match(r"^\d{4}-\d{2}-\d{2}$", message_normalized):
            try:
                datetime.strptime(message_normalized, "%Y-%m-%d")
                return "reservation_flow"
            except ValueError:
                pass

        reservation_keywords = self.intent_keywords.get("reservation", [])
        cancel_keywords = self.intent_keywords.get("cancel", [])
        modify_keywords = self.intent_keywords.get("modify", [])

        if any(keyword in message_normalized for keyword in modify_keywords):
            return "modify"
        elif any(keyword in message_normalized for keyword in cancel_keywords):
            return "cancel"
        elif any(keyword in message_normalized for keyword in reservation_keywords):
            return "reservation"
        else:
            return "general"

    def handle_reservation_flow(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        if user_id not in self.user_states:
            self.user_states[user_id] = {"step": "start", "data": {"user_id": user_id}}

        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()

        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            is_modification = self.user_states[user_id].get("is_modification", False)
            del self.user_states[user_id]
            if is_modification:
                return "予約変更をキャンセルいたします。元の予約はそのまま有効です。またのご利用をお待ちしております。"
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        if self._is_back_command(message_normalized):
            return self._go_back_one_step(user_id)

        state = self.user_states[user_id]
        step = state["step"]

        if step == "start":
            if re.match(r"^\d{4}-\d{2}-\d{2}$", message_normalized):
                try:
                    datetime.strptime(message_normalized, "%Y-%m-%d")
                    self._start_reservation(user_id)
                    return self._handle_date_selection(user_id, message)
                except ValueError:
                    pass
            return self._start_reservation(user_id)
        elif step == "service_selection":
            return self._handle_service_selection(user_id, message)
        elif step == "staff_selection":
            return self._handle_staff_selection(user_id, message)
        elif step == "date_selection":
            return self._handle_date_selection(user_id, message)
        elif step == "time_selection":
            return self._handle_time_selection(user_id, message)
        elif step == "confirmation":
            return self._handle_confirmation(user_id, message)
        else:
            return "エラーが発生しました。もう一度最初からお願いいたします。"

    def _start_reservation(self, user_id: str) -> Union[str, Dict[str, Any]]:
        self.user_states[user_id]["step"] = "service_selection"
        menu_items = self._build_service_quick_reply_postback_items()

        text = """ご予約ありがとうございます😊

人気メニューはこちら👇

【🔥一番人気】
・カット＋カラー＋トリートメント
→ツヤ・色持ち◎

【✨迷ったらこれ】
・カット＋カラー

【その他】
・カット
・カラー
・パーマ

ご希望のメニューをお選びください👇"""
        return self._quick_reply_return(
            text,
            menu_items,
            include_cancel=True,
            include_back=False,
        )

    def _build_service_quick_reply_postback_items(self) -> List[Dict[str, str]]:
        items = []
        for _key, data in self.services.items():
            if isinstance(data, dict) and data.get("id"):
                sid = data.get("id")
                name = data.get("name", sid)
                items.append({
                    "label": name,
                    "type": "postback",
                    "data": f"action=select_service&service_id={sid}",
                })
        return items

    def start_reservation_with_service(
        self,
        user_id: str,
        service_identifier: str,
    ) -> Union[str, Dict[str, Any]]:
        if not service_identifier or not str(service_identifier).strip():
            text = "もう一度メニューをお選びください。"
            return self._quick_reply_return(
                text,
                self._build_service_quick_reply_postback_items(),
                include_cancel=True,
                include_back=False,
            )

        service_id = str(service_identifier).strip()
        svc = self._get_service_by_id(service_id)
        if not svc:
            text = "もう一度メニューをお選びください。"
            return self._quick_reply_return(
                text,
                self._build_service_quick_reply_postback_items(),
                include_cancel=True,
                include_back=False,
            )

        existing_state = self.user_states.get(user_id, {})
        existing_data = existing_state.get("data", {})
        existing_data["user_id"] = user_id
        existing_data["service_id"] = service_id

        existing_state["step"] = "service_selection"
        existing_state["data"] = existing_data

        self.user_states[user_id] = existing_state

        return self._reply_after_service_selected(user_id)

    def start_reservation_with_staff(self, user_id: str, staff_identifier: str) -> Union[str, Dict[str, Any]]:
        staff_name = None

        if staff_identifier in self.staff_members:
            staff_name = self.staff_members[staff_identifier].get("name", staff_identifier)
        else:
            for sid, sdata in self.staff_members.items():
                if sdata.get("name") == staff_identifier:
                    staff_name = sdata.get("name")
                    break

        if not staff_name:
            return "申し訳ございませんが、選択されたスタッフは現在ご指定いただけません。"

        existing_state = self.user_states.get(user_id, {})
        existing_data = existing_state.get("data", {})

        existing_data["user_id"] = user_id
        existing_data["staff"] = staff_name

        existing_state["step"] = "service_selection"
        existing_state["data"] = existing_data

        self.user_states[user_id] = existing_state

        return self._start_reservation(user_id)

    def _reply_after_service_selected(self, user_id: str) -> Union[str, Dict[str, Any]]:
        service_id = self.user_states[user_id]["data"].get("service_id")
        service_name = self._get_service_name_by_id(service_id) if service_id else ""
        preselected_staff = self.user_states[user_id]["data"].get("staff")

        self._clear_reservation_selection_after_service(user_id)

        if preselected_staff:
            self.user_states[user_id]["data"]["staff"] = preselected_staff
            self.user_states[user_id]["step"] = "date_selection"
            self.user_states[user_id]["date_selection_back_target"] = "service_selection"
            staff_display = f"{preselected_staff}さん" if preselected_staff != "未指定" else preselected_staff
            intro = f"""{service_name}ですね👌
担当者は{staff_display}になります😊
"""
            self.user_states[user_id]["date_selection_week_start"] = self._calendar_week_monday(
                datetime.now().date()
            ).strftime("%Y-%m-%d")
            reply = self._build_date_week_selection_message(user_id, context="new_reservation")
            reply["text"] = intro + reply["text"]
            return reply

        if self._has_single_staff():
            single_staff_name = self._get_single_staff_name()
            self.user_states[user_id]["data"]["staff"] = single_staff_name
            self.user_states[user_id]["step"] = "date_selection"
            self.user_states[user_id]["date_selection_back_target"] = "service_selection"
            intro = f"""{service_name}ですね👌

担当者は{single_staff_name}になります😊
"""
            self.user_states[user_id]["date_selection_week_start"] = self._calendar_week_monday(
                datetime.now().date()
            ).strftime("%Y-%m-%d")
            reply = self._build_date_week_selection_message(user_id, context="new_reservation")
            reply["text"] = intro + reply["text"]
            return reply

        self.user_states[user_id]["date_selection_back_target"] = "staff_selection"
        return self._build_staff_selection_message(user_id)

    def _handle_service_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        raw = message.strip()
        if any(keyword in raw for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        normalized_input = self._normalize_service_input(raw)
        matches = self._fallback_match_service_by_text(normalized_input)

        if not matches:
            text = "メニューを選択してください。"
            return self._quick_reply_return(
                text,
                self._build_service_quick_reply_postback_items(),
                include_cancel=True,
                include_back=False,
            )

        if len(matches) > 1:
            items = [
                {
                    "label": m[1].get("name", m[0]),
                    "type": "postback",
                    "data": f"action=select_service&service_id={m[0]}",
                }
                for m in matches
            ]
            return self._quick_reply_return(
                "複数該当しました。どちらにしますか？",
                items,
                include_cancel=True,
                include_back=False,
            )

        service_id, _ = matches[0]
        self.user_states[user_id]["data"]["service_id"] = service_id
        return self._reply_after_service_selected(user_id)

    def _handle_staff_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        service_change_keywords = self.navigation_keywords.get("service_change", [])
        if any(keyword in message_normalized for keyword in service_change_keywords):
            self.user_states[user_id]["step"] = "service_selection"
            return self._start_reservation(user_id)

        selected_staff = None
        message_lower = message.strip().lower()

        for staff_id, staff_data in self.staff_members.items():
            staff_name = staff_data.get("name", staff_id)
            if staff_name.lower() in message_lower or message_lower in staff_name.lower():
                selected_staff = staff_name
                break

        if not selected_staff:
            staff_items = [
                {"label": s.get("name", sid), "text": s.get("name", sid)}
                for sid, s in self.staff_members.items()
            ]
            staff_lines = [
                f"・{s.get('name', sid)}（{s.get('specialty', '')}・{s.get('experience', '')}）"
                for sid, s in self.staff_members.items()
            ]
            text = "申し訳ございませんが、その美容師は選択できません。上記の美容師からお選びください。\n\n" + "\n".join(staff_lines)
            return self._quick_reply_return(text, staff_items, include_cancel=True, include_back=True)

        self.user_states[user_id]["data"]["staff"] = selected_staff
        self.user_states[user_id]["step"] = "date_selection"
        self.user_states[user_id]["date_selection_back_target"] = "staff_selection"

        staff_display = f"{selected_staff}さん" if selected_staff != "未指定" else selected_staff
        intro = f"""担当者：{staff_display}ですね。

"""
        self.user_states[user_id]["date_selection_week_start"] = self._calendar_week_monday(
            datetime.now().date()
        ).strftime("%Y-%m-%d")
        reply = self._build_date_week_selection_message(user_id, context="new_reservation")
        reply["text"] = intro + reply["text"]
        return reply

    def _handle_date_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        service_change_keywords = self.navigation_keywords.get("service_change", [])
        if any(keyword in message_normalized for keyword in service_change_keywords):
            self.user_states[user_id]["step"] = "service_selection"
            return self._start_reservation(user_id)

        today = datetime.now().date()
        min_ws = self._calendar_week_monday(today)

        if message_normalized == "前の週":
            st = self.user_states[user_id]
            raw = st.get("date_selection_week_start", min_ws.strftime("%Y-%m-%d"))
            try:
                ws = datetime.strptime(raw, "%Y-%m-%d").date()
            except ValueError:
                ws = min_ws
            new_ws = max(min_ws, ws - timedelta(days=7))
            st["date_selection_week_start"] = new_ws.strftime("%Y-%m-%d")
            return self._build_date_week_selection_message(user_id, context="new_reservation")

        if message_normalized == "次の週":
            st = self.user_states[user_id]
            raw = st.get("date_selection_week_start", min_ws.strftime("%Y-%m-%d"))
            try:
                ws = datetime.strptime(raw, "%Y-%m-%d").date()
            except ValueError:
                ws = min_ws
            new_ws = ws + timedelta(days=7)
            st["date_selection_week_start"] = new_ws.strftime("%Y-%m-%d")
            return self._build_date_week_selection_message(user_id, context="new_reservation")

        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", message)
        selected_date = None
        if date_match:
            selected_date = date_match.group(1)
            try:
                datetime.strptime(selected_date, "%Y-%m-%d")
            except ValueError:
                selected_date = None

        if not selected_date:
            err = (
                "申し訳ございませんが、日付の形式が正しくありません。\n"
                "「2026-01-07」の形式で入力するか、下の日付ボタンからお選びください。"
            )
            return self._build_date_week_selection_message(
                user_id,
                context="new_reservation",
                error_prefix=err,
            )

        try:
            date_obj = datetime.strptime(selected_date, "%Y-%m-%d").date()
        except ValueError:
            err = (
                "申し訳ございませんが、日付の形式が正しくありません。\n"
                "「2026-01-07」の形式で入力するか、下の日付ボタンからお選びください。"
            )
            return self._build_date_week_selection_message(
                user_id,
                context="new_reservation",
                error_prefix=err,
            )

        if date_obj < today:
            err = "過去の日付は選択いたしかねます。\n本日以降の日付を入力してください。"
            return self._build_date_week_selection_message(
                user_id,
                context="new_reservation",
                error_prefix=err,
            )

        if not is_open_date(date_obj):
            err = f"申し訳ございませんが、{selected_date}は休業日です。\n別の日付をお選びください。"
            return self._build_date_week_selection_message(
                user_id,
                context="new_reservation",
                error_prefix=err,
            )

        return self._apply_selected_date_go_to_time_selection(user_id, selected_date)

    def _check_advance_booking_time(self, date_str: str, start_time: str, user_id: str = None) -> tuple:
        is_modification = False
        if user_id and user_id in self.user_states:
            is_modification = self.user_states[user_id].get("is_modification", False)

        if is_modification:
            limit_hours = self._get_reservation_limit_hours("change_limit_hours", 2)
            action_label = "予約変更"
        else:
            limit_hours = self._get_reservation_limit_hours("create_limit_hours", 2)
            action_label = "ご予約"

        return self._check_reservation_deadline(
            date_str=date_str,
            start_time=start_time,
            limit_hours=limit_hours,
            action_label=action_label,
            selection_label="時間帯",
        )

    def _normalize_time_format(self, time_str: str) -> Optional[str]:
        try:
            parts = time_str.split(":")
            if len(parts) == 2:
                hour_part = parts[0]
                minute_part = parts[1]

                if len(minute_part) != 2 or not minute_part.isdigit():
                    return None

                if len(hour_part) == 1:
                    normalized_hour = f"0{hour_part}"
                elif len(hour_part) == 2 and hour_part.isdigit():
                    normalized_hour = hour_part
                else:
                    return None

                normalized_time = f"{normalized_hour}:{minute_part}"
                datetime.strptime(normalized_time, "%H:%M")
                return normalized_time
            else:
                return None
        except (ValueError, IndexError):
            return None

    def _parse_single_time(self, text: str) -> Optional[str]:
        text = text.strip()

        match = re.search(r"^(\d{1,2}:\d{2})$", text)
        if match:
            return self._normalize_time_format(match.group(1))

        match = re.search(r"^(\d{1,2})$", text)
        if match:
            return self._normalize_time_format(f"{match.group(1)}:00")

        match = re.search(r"^(\d{1,2})時$", text)
        if match:
            return self._normalize_time_format(f"{match.group(1)}:00")

        match = re.search(r"^(\d{1,2})時(\d{1,2})分$", text)
        if match:
            return self._normalize_time_format(f"{match.group(1)}:{match.group(2)}")

        return None

    def _handle_time_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        date_change_keywords = self.navigation_keywords.get("date_change", [])
        if any(keyword in message_normalized for keyword in date_change_keywords):
            self.user_states[user_id]["step"] = "date_selection"
            self.user_states[user_id]["date_selection_week_start"] = self._calendar_week_monday(
                datetime.now().date()
            ).strftime("%Y-%m-%d")
            return self._build_date_week_selection_message(user_id, context="new_reservation")

        if message_normalized in ("前へ", "次へ"):
            time_options = self.user_states[user_id].get("time_options", [])
            current_page = self.user_states[user_id].get("time_slot_page", 0)
            per_page = 8
            total_pages = max(1, (len(time_options) + per_page - 1) // per_page)

            if message_normalized == "前へ":
                new_page = max(0, current_page - 1)
            else:
                new_page = min(total_pages - 1, current_page + 1)

            selected_date = self.user_states[user_id].get(
                "time_selection_date",
                self.user_states[user_id]["data"]["date"],
            )
            sid = self._get_current_service_id(user_id)
            service_name = self._get_service_name_by_id(sid) if sid else ""
            service_duration = (
                (self._get_service_by_id(sid) or {}).get("duration", 60)
                if sid
                else self.user_states[user_id].get("time_selection_service_duration", 60)
            )

            text = f"""{selected_date}ですね👌
{service_name}（{service_duration}分）の空き状況はこちら。

【🔥おすすめ】
・10:00～
・11:00～

【その他】
・13:00～
・15:00～
・17:00～

ご希望の時間をお選びください👇"""
            return self._build_time_selection_quick_reply(user_id, text, new_page)

        selected_date = self.user_states[user_id]["data"]["date"]
        staff_name = self.user_states[user_id]["data"].get("staff")

        try:
            available_slots = self._get_available_slots(selected_date, staff_name, user_id)
            available_periods = [slot for slot in available_slots if slot["available"]]

            sid = self._get_current_service_id(user_id)
            service_info = self._get_service_by_id(sid) if sid else {}
            service_duration = service_info.get("duration", 60)

            filtered_periods = []
            for period in available_periods:
                slot_duration = self._calculate_time_duration_minutes(period["time"], period["end_time"])
                if slot_duration >= service_duration:
                    filtered_periods.append(period)

        except Exception as e:
            logging.error(f"Error getting available slots: {e}")
            return "申し訳ございません。エラーが発生しました。\nもう一度お試しください。"

        start_time = self._parse_single_time(message.strip())

        if not start_time:
            is_modification = self.user_states[user_id].get("is_modification", False)
            original_reservation = self.user_states[user_id].get("original_reservation") if is_modification else None

            period_strings = []
            for period in filtered_periods:
                period_start = period["time"]
                period_end = period["end_time"]
                if is_modification and original_reservation:
                    if (
                        period_start == original_reservation.get("start_time")
                        and period_end == original_reservation.get("end_time")
                    ):
                        period_strings.append(f"・{period_start}~{period_end} ⭐（現在の予約時間）")
                    else:
                        period_strings.append(f"・{period_start}~{period_end}")
                else:
                    period_strings.append(f"・{period_start}~{period_end}")

            modification_note = ""
            if is_modification and original_reservation:
                modification_note = (
                    f"\n\n💡 現在の予約時間（{original_reservation.get('start_time')}~"
                    f"{original_reservation.get('end_time')}）も選択できます。"
                )

            return f"""時間の入力形式が正しくありません。

正しい入力例：
・10:00
・10:30
・10時
・10時30分

{selected_date}の予約可能な時間帯：
{chr(10).join(period_strings)}{modification_note}

上記の空き時間から開始時間をお選びください。

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""

        is_valid_time, time_error_message = self._check_advance_booking_time(
            selected_date,
            start_time,
            user_id,
        )
        if not is_valid_time:
            return time_error_message

        sid = self._get_current_service_id(user_id)
        service_info = self._get_service_by_id(sid) if sid else {}
        required_duration = service_info.get("duration", 60)

        end_time = self._calculate_optimal_end_time(start_time, required_duration)

        is_valid_range = False
        for period in available_periods:
            period_start = period["time"]
            period_end = period["end_time"]
            if period_start <= start_time and end_time <= period_end:
                is_valid_range = True
                break

        if not is_valid_range:
            period_strings = [f"・{period['time']}~{period['end_time']}" for period in available_periods]
            return f"""申し訳ございませんが、{start_time}から{required_duration}分の予約は空いていません。

{selected_date}の予約可能な時間帯：
{chr(10).join(period_strings)}

上記の空き時間からお選びください。

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""

        user_time_conflict = self.google_calendar.check_user_time_conflict(
            selected_date,
            start_time,
            end_time,
            user_id,
        )

        if user_time_conflict:
            self.user_states[user_id]["step"] = "time_selection"

            available_slots = self._get_available_slots(selected_date, staff_name, user_id)
            available_periods = [slot for slot in available_slots if slot["available"]]
            period_strings = [f"・{period['time']}~{period['end_time']}" for period in available_periods]

            return f"""申し訳ございませんが、{selected_date} {start_time}~{end_time}の時間帯に既に他のご予約が入っています。

お客様は同じ時間帯に複数のご予約をお取りいただけません。

{selected_date}の予約可能な時間帯は以下の通りです：

{chr(10).join(period_strings)}

別の時間を選択してください。

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""

        self.user_states[user_id]["data"]["start_time"] = start_time
        self.user_states[user_id]["data"]["end_time"] = end_time
        self.user_states[user_id]["data"]["time"] = start_time
        self.user_states[user_id]["step"] = "confirmation"

        service = self._get_service_name_by_id(sid) if sid else ""
        staff = self.user_states[user_id]["data"]["staff"]
        price_val = service_info.get("price", 0)

        text = f"""ご予約内容の確認です😊

日時：{selected_date} {start_time}~{end_time}
メニュー：{service}
担当：{staff}
料金：{price_val:,}円

この内容で予約を確定しますか？"""
        return self._quick_reply_return(
            text,
            [{"label": "確定", "text": "確定"}],
            include_cancel=True,
            include_back=True,
        )

    def _check_final_availability(self, reservation_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            date_str = reservation_data["date"]
            start_time = reservation_data.get("start_time", reservation_data.get("time", ""))
            end_time = reservation_data.get("end_time", "")
            staff_name = reservation_data["staff"]
            user_id = reservation_data.get("user_id", "")

            if not end_time:
                sid = reservation_data.get("service_id")
                if not sid and reservation_data.get("service"):
                    sid = self._get_service_id_by_name(reservation_data["service"])
                service_info = self._get_service_by_id(sid) if sid else {}
                duration = service_info.get("duration", 60)
                start_dt = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
                end_dt = start_dt + timedelta(minutes=duration)
                end_time = end_dt.strftime("%H:%M")

            exclude_reservation_id = None
            try:
                if user_id and user_id in self.user_states:
                    state = self.user_states[user_id]
                    if state.get("is_modification") and state.get("original_reservation"):
                        original_reservation = state["original_reservation"]
                        if original_reservation.get("date") == date_str:
                            exclude_reservation_id = original_reservation.get("reservation_id")
            except Exception as e:
                logging.error(f"Error detecting modification context in _check_final_availability: {e}")

            staff_available = self.google_calendar.check_staff_availability_for_time(
                date_str,
                start_time,
                end_time,
                staff_name,
                exclude_reservation_id,
            )

            if not staff_available:
                return {
                    "available": False,
                    "message": f"👨‍💼 {staff_name}さんの{start_time}~{end_time}の時間帯は既に予約が入っております。",
                }

            user_conflict = self.google_calendar.check_user_time_conflict(
                date_str,
                start_time,
                end_time,
                user_id,
                exclude_reservation_id,
                staff_name,
            )

            if user_conflict:
                return {
                    "available": False,
                    "message": "⚠️ 同じ時間帯に他のご予約がございます。",
                }

            return {"available": True, "message": ""}

        except Exception as e:
            logging.error(f"Error checking final availability: {e}")
            return {"available": True, "message": ""}

    def _handle_confirmation(self, user_id: str, message: str) -> str:
        yes_keywords = self.confirmation_keywords.get("yes", [])
        if any(keyword in message for keyword in yes_keywords):
            reservation_data = self.user_states[user_id]["data"].copy()

            if "staff" not in reservation_data or not reservation_data.get("staff"):
                logging.error(
                    f"[_handle_confirmation] ERROR: Staff not found in reservation_data! Data: {reservation_data}"
                )
                return "申し訳ございませんがエラーが発生しました。「キャンセル」とお送りして、もう一度最初からやり直してください。"

            sid = reservation_data.get("service_id") or self._get_service_id_by_name(reservation_data.get("service"))
            if sid:
                reservation_data["service_id"] = sid
                reservation_data["service"] = self._get_service_name_by_id(sid)
            service_info_for_confirm = self._get_service_by_id(sid) if sid else {}

            availability_check = self._check_final_availability(reservation_data)
            if not availability_check["available"]:
                del self.user_states[user_id]
                return f"""❌ 申し訳ございませんが、選択された時間帯は既に他のお客様にご予約いただいておりました。

{availability_check["message"]}

別の時間帯でご予約いただけますでしょうか？
「予約したい」とお送りください。"""

            reservation_id = self.google_calendar.generate_reservation_id(reservation_data["date"])
            reservation_data["reservation_id"] = reservation_id

            client_name = self._get_line_display_name(user_id)

            calendar_success = self.google_calendar.create_reservation_event(
                reservation_data,
                client_name,
            )

            if not calendar_success:
                logging.error(
                    f"[_handle_confirmation] Failed to create calendar event for user {user_id}, reservation {reservation_id}"
                )

            try:
                from api.google_sheets_logger import GoogleSheetsLogger

                sheets_logger = GoogleSheetsLogger()

                sheet_reservation_data = {
                    "reservation_id": reservation_id,
                    "user_id": user_id,
                    "client_name": client_name,
                    "date": reservation_data["date"],
                    "start_time": reservation_data.get("start_time", reservation_data.get("time", "")),
                    "end_time": reservation_data.get("end_time", ""),
                    "service": reservation_data["service"],
                    "staff": reservation_data["staff"],
                    "duration": service_info_for_confirm.get("duration", 60),
                    "price": service_info_for_confirm.get("price", 0),
                }

                sheets_success = sheets_logger.save_reservation(sheet_reservation_data)
                if not sheets_success:
                    logging.error(f"Failed to save reservation {reservation_id} to Reservations sheet")

            except Exception as e:
                logging.error(f"Error saving reservation to Google Sheets: {e}", exc_info=True)

            is_modification = self.user_states[user_id].get("is_modification", False)
            original_reservation = self.user_states[user_id].get("original_reservation")

            if is_modification and original_reservation:
                try:
                    from api.google_sheets_logger import GoogleSheetsLogger

                    sheets_logger = GoogleSheetsLogger()

                    original_reservation_id = original_reservation["reservation_id"]
                    original_staff_name = original_reservation.get("staff")

                    sheets_success = sheets_logger.update_reservation_status(
                        original_reservation_id,
                        "Cancelled",
                    )
                    if not sheets_success:
                        logging.warning(
                            f"[Modification] Failed to update Google Sheets status for {original_reservation_id}"
                        )

                    calendar_success = self.google_calendar.cancel_reservation_by_id(
                        original_reservation_id,
                        original_staff_name,
                    )
                    if not calendar_success:
                        logging.warning(
                            f"[Modification] Failed to delete original reservation {original_reservation_id} from Google Calendar"
                        )

                except Exception as e:
                    logging.error(
                        f"Failed to cancel original reservation during modification: {e}",
                        exc_info=True,
                    )

            try:
                from api.notification_manager import (
                    send_reservation_confirmation_notification,
                    send_reservation_modification_notification,
                )

                if is_modification and original_reservation:
                    notification_success = send_reservation_modification_notification(
                        original_reservation,
                        reservation_data,
                        client_name,
                    )
                    if not notification_success:
                        logging.warning(f"[Notification] Modification notification failed for user {user_id}")
                else:
                    send_reservation_confirmation_notification(reservation_data, client_name)

            except Exception as e:
                logging.error(f"Failed to send notification: {e}", exc_info=True)

            self.user_states[user_id]["data"] = reservation_data

            time_display = reservation_data.get("start_time", reservation_data["time"])
            if "end_time" in reservation_data:
                time_display = f"{reservation_data['start_time']}~{reservation_data['end_time']}"

            if is_modification and original_reservation:
                return f"""予約の変更が完了しました😊

🆔：{reservation_id}
📅：{reservation_data['date']} {time_display}
💇：{reservation_data['service']}
👤：{reservation_data['staff']}

当日はお気をつけてお越しください。
ご来店をお待ちしております✨"""
            else:
                return f"""ご予約が確定しました😊

🆔：{reservation_id}
📅：{reservation_data['date']} {time_display}
💇：{reservation_data['service']}
👤：{reservation_data['staff']}

当日はお気をつけてお越しください。
ご来店お待ちしております✨"""

        return "「確定」とお送りください。"

    def _handle_modify_request(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        state = self.user_states.get(user_id)

        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            if user_id in self.user_states:
                del self.user_states[user_id]
            return "予約変更をキャンセルいたします。またのご利用をお待ちしております。"

        if state and state.get("step") == "modify_select_reservation":
            return self._handle_modify_reservation_selection(user_id, message)

        return self._show_user_reservations_for_modification(user_id)

    def _show_user_reservations_for_modification(self, user_id: str) -> Union[str, Dict[str, Any]]:
        try:
            from api.google_sheets_logger import GoogleSheetsLogger
            import pytz

            sheets_logger = GoogleSheetsLogger()
            client_name = self._get_line_display_name(user_id)

            reservations = sheets_logger.get_user_reservations(client_name)

            if not reservations:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "申し訳ございませんが、あなたの予約が見つかりませんでした。\n新しくご予約される場合は「予約したい」とお送りください。"

            tokyo_tz = pytz.timezone("Asia/Tokyo")
            current_time = datetime.now(tokyo_tz)
            future_reservations = []

            for res in reservations:
                try:
                    reservation_date = res.get("date", "")
                    reservation_start_time = res.get("start_time", "")

                    if not reservation_date or not reservation_start_time:
                        continue

                    reservation_datetime_naive = datetime.strptime(
                        f"{reservation_date} {reservation_start_time}",
                        "%Y-%m-%d %H:%M",
                    )
                    reservation_datetime = tokyo_tz.localize(reservation_datetime_naive)

                    if reservation_datetime > current_time:
                        future_reservations.append(res)

                except (ValueError, TypeError) as e:
                    logging.warning(
                        f"Skipping reservation with invalid date/time: "
                        f"{res.get('reservation_id', 'Unknown')} - {e}"
                    )
                    continue

            if not future_reservations:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "申し訳ございませんが、今後予定されている予約が見つかりませんでした。\n新しくご予約される場合は「予約したい」とお送りください。"

            self.user_states[user_id] = {
                "step": "modify_select_reservation",
                "user_reservations": future_reservations,
            }

            reservation_list = []
            quick_reply_items = []

            for i, res in enumerate(future_reservations[:5], 1):
                reservation_list.append(
                    f"{i}️⃣ {res['date']} {res['start_time']}~{res['end_time']} - "
                    f"{res['service']} ({res['reservation_id']})"
                )
                quick_reply_items.append({
                    "label": f"{i}️⃣",
                    "text": res["reservation_id"],
                })

            text = f"""ご予約の変更ですね😊

変更する予約をお選びください👇

{chr(10).join(reservation_list)}"""

            return self._quick_reply_return(
                text,
                quick_reply_items,
                include_cancel=True,
                include_back=False,
            )

        except Exception as e:
            logging.error(f"Failed to show user reservations for modification: {e}")
            if user_id in self.user_states:
                del self.user_states[user_id]
            return "申し訳ございません。エラーが発生しました。もう一度お試しください。"

    def _handle_modify_reservation_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        state = self.user_states[user_id]

        if "user_reservations" not in state:
            return self._show_user_reservations_for_modification(user_id)

        reservations = state["user_reservations"]

        try:
            selected_reservation = None

            if re.match(r"^RES-\d{8}-\d{4}$", message):
                reservation_id = message.strip()
                for res in reservations:
                    if res["reservation_id"] == reservation_id:
                        selected_reservation = res
                        break

            elif message.strip().isdigit():
                reservation_index = int(message.strip()) - 1
                if 0 <= reservation_index < len(reservations):
                    selected_reservation = reservations[reservation_index]
                else:
                    return (
                        f"申し訳ございませんが、その番号は選択できません。\n"
                        f"1から{len(reservations)}の番号を入力してください。\n\n"
                        f"変更をやめる場合は「キャンセル」とお送りください。"
                    )
            else:
                return (
                    f"申し訳ございませんが、正しい形式で入力してください。\n"
                    f"番号（1-{len(reservations)}）または予約ID（RES-YYYYMMDD-XXXX）を入力してください。\n\n"
                    f"変更をやめる場合は「キャンセル」とお送りください。"
                )

            if not selected_reservation:
                return self._quick_reply_return(
                    "申し訳ございませんが、その予約IDが見つからないか、あなたの予約ではありません。\n"
                    "正しい予約IDまたは番号を入力してください。\n\n"
                    "変更をやめる場合は「キャンセル」とお送りください。",
                    [],
                    include_cancel=True,
                    include_back=False,
                )

            is_within_deadline, deadline_message = self._check_existing_reservation_deadline(
                selected_reservation,
                "change_limit_hours",
                "予約変更",
            )
            if not is_within_deadline:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return deadline_message

            self.user_states[user_id]["original_reservation"] = selected_reservation
            self.user_states[user_id]["is_modification"] = True

            self.user_states[user_id]["step"] = "service_selection"
            self.user_states[user_id]["data"] = {
                "user_id": user_id,
            }

            menu_items = self._build_service_quick_reply_postback_items()

            text = f"""以下の予約を変更します👇

📅：{selected_reservation['date']} {selected_reservation['start_time']}~{selected_reservation['end_time']}
💇：{selected_reservation['service']}
👤：{selected_reservation['staff']}

新しい内容をお選びください👇

【🔥一番人気】
・カット＋カラー＋トリートメント
→ツヤ・色持ち◎

【✨迷ったらこれ】
・カット＋カラー

【その他】
・カット
・カラー
・パーマ

ご希望のメニューをお選びください👇"""

            return self._quick_reply_return(
                text,
                menu_items,
                include_cancel=True,
                include_back=False,
            )

        except Exception as e:
            logging.error(f"Reservation selection for modification failed: {e}")
            return (
                "申し訳ございません。予約選択中にエラーが発生しました。"
                "もう一度お試しください。\n\n"
                "変更をやめる場合は「キャンセル」とお送りください。"
            )

    def get_response(self, user_id: str, message: str) -> Optional[Union[str, Dict[str, Any]]]:
        intent = self.detect_intent(message, user_id)

        if intent == "reservation":
            return self.handle_reservation_flow(user_id, message)
        elif intent == "reservation_flow":
            return self.handle_reservation_flow(user_id, message)
        elif intent == "modify":
            return self._handle_modify_request(user_id, message)
        elif intent == "cancel":
            return self._handle_cancel_request(user_id, message)
        else:
            return None

    def set_line_configuration(self, configuration):
        self.line_configuration = configuration

    def _get_line_display_name(self, user_id: str) -> str:
        if not self.line_configuration:
            return "お客様"

        try:
            from linebot.v3.messaging import ApiClient, MessagingApi

            with ApiClient(self.line_configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                profile = line_bot_api.get_profile(user_id)
                return profile.display_name
        except Exception as e:
            logging.error(f"Failed to get LINE display name: {e}")
            return "お客様"

    def _handle_cancel_request(self, user_id: str, message: str = None) -> Union[str, Dict[str, Any]]:
        state = self.user_states.get(user_id)

        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        if message:
            message_normalized = message.strip()
            if any(keyword in message_normalized for keyword in flow_cancel_keywords):
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "予約取り消しをキャンセルいたします。またのご利用をお待ちしております。"

        if state and state.get("step") == "cancel_select_reservation":
            return self._handle_cancel_reservation_selection(user_id, message)

        if state and state.get("step") == "cancel_confirm":
            return self._handle_cancel_confirmation(user_id, message)

        return self._show_user_reservations_for_cancellation(user_id)

    def _show_user_reservations_for_cancellation(self, user_id: str) -> Union[str, Dict[str, Any]]:
        try:
            from api.google_sheets_logger import GoogleSheetsLogger
            import pytz

            sheets_logger = GoogleSheetsLogger()
            client_name = self._get_line_display_name(user_id)

            reservations = sheets_logger.get_user_reservations(client_name)

            if not reservations:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "申し訳ございませんが、あなたの予約が見つかりませんでした。\n新しくご予約される場合は「予約したい」とお送りください。"

            tokyo_tz = pytz.timezone("Asia/Tokyo")
            current_time = datetime.now(tokyo_tz)
            future_reservations = []

            for res in reservations:
                try:
                    reservation_date = res.get("date", "")
                    reservation_start_time = res.get("start_time", "")

                    if not reservation_date or not reservation_start_time:
                        continue

                    reservation_datetime_naive = datetime.strptime(
                        f"{reservation_date} {reservation_start_time}",
                        "%Y-%m-%d %H:%M",
                    )
                    reservation_datetime = tokyo_tz.localize(reservation_datetime_naive)

                    if reservation_datetime > current_time:
                        future_reservations.append(res)

                except (ValueError, TypeError) as e:
                    logging.warning(
                        f"Skipping reservation with invalid date/time: "
                        f"{res.get('reservation_id', 'Unknown')} - {e}"
                    )
                    continue

            if not future_reservations:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "申し訳ございませんが、今後予定されている予約が見つかりませんでした。\n過去の予約はキャンセルできません。\n新しくご予約される場合は「予約したい」とお送りください。"

            self.user_states[user_id] = {
                "step": "cancel_select_reservation",
                "user_reservations": future_reservations,
            }

            reservation_list = []
            quick_reply_items = []
            for i, res in enumerate(future_reservations[:5], 1):
                reservation_list.append(
                    f"{i}️⃣ {res['date']} {res['start_time']}~{res['end_time']} - {res['service']} ({res['reservation_id']})"
                )
                quick_reply_items.append({"label": f"{i}️⃣", "text": str(i)})

            text = f"""ご予約のキャンセルですね😊

キャンセルする予約をお選びください👇

{chr(10).join(reservation_list)}"""
            return self._quick_reply_return(
                text,
                quick_reply_items,
                include_cancel=True,
                include_back=False,
            )

        except Exception as e:
            logging.error(f"Failed to show user reservations for cancellation: {e}")
            if user_id in self.user_states:
                del self.user_states[user_id]
            return "申し訳ございません。エラーが発生しました。もう一度お試しください。"

    def _handle_cancel_reservation_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        state = self.user_states[user_id]
        if "user_reservations" not in state:
            return self._show_user_reservations_for_cancellation(user_id)

        reservations = state["user_reservations"]

        try:
            if re.match(r"^RES-\d{8}-\d{4}$", message):
                reservation_id = message.strip()
                selected_reservation = None
                for res in reservations:
                    if res["reservation_id"] == reservation_id:
                        selected_reservation = res
                        break

                if selected_reservation:
                    is_within_deadline, deadline_message = self._check_existing_reservation_deadline(
                        selected_reservation,
                        "cancel_limit_hours",
                        "予約キャンセル",
                    )
                    if not is_within_deadline:
                        if user_id in self.user_states:
                            del self.user_states[user_id]
                        return deadline_message

                    self.user_states[user_id]["selected_reservation"] = selected_reservation
                    self.user_states[user_id]["step"] = "cancel_confirm"

                    text = f"""こちらのご予約をキャンセルしてよろしいですか？

 📅：{selected_reservation['date']} {selected_reservation['start_time']}~{selected_reservation['end_time']}
 💇：{selected_reservation['service']}
 👤：{selected_reservation['staff']}"""
                    return self._quick_reply_return(
                        text,
                        [{"label": "確定", "text": "はい"}],
                        include_cancel=True,
                        include_back=False,
                    )
                else:
                    return self._quick_reply_return(
                        "申し訳ございませんが、その予約IDが見つからないか、あなたの予約ではありません。\n正しい予約IDまたは番号を入力してください。",
                        [],
                        include_cancel=True,
                        include_back=False,
                    )

            elif message.isdigit():
                reservation_index = int(message) - 1
                if 0 <= reservation_index < len(reservations):
                    selected_reservation = reservations[reservation_index]

                    is_within_deadline, deadline_message = self._check_existing_reservation_deadline(
                        selected_reservation,
                        "cancel_limit_hours",
                        "予約キャンセル",
                    )
                    if not is_within_deadline:
                        if user_id in self.user_states:
                            del self.user_states[user_id]
                        return deadline_message

                    self.user_states[user_id]["selected_reservation"] = selected_reservation
                    self.user_states[user_id]["step"] = "cancel_confirm"

                    text = f"""こちらのご予約をキャンセルしてよろしいですか？

 📅：{selected_reservation['date']} {selected_reservation['start_time']}~{selected_reservation['end_time']}
 💇：{selected_reservation['service']}
 👤：{selected_reservation['staff']}"""
                    return self._quick_reply_return(
                        text,
                        [{"label": "確定", "text": "はい"}],
                        include_cancel=True,
                        include_back=False,
                    )
                else:
                    return f"申し訳ございませんが、その番号は選択できません。\n1から{len(reservations)}の番号を入力してください。"
            else:
                return f"申し訳ございませんが、正しい形式で入力してください。\n番号（1-{len(reservations)}）または予約ID（RES-YYYYMMDD-XXXX）を入力してください。"

        except Exception as e:
            logging.error(f"Reservation selection for cancellation failed: {e}")
            return "申し訳ございません。エラーが発生しました。\nもう一度お試しください。"

    def _handle_cancel_confirmation(self, user_id: str, message: str) -> str:
        state = self.user_states[user_id]
        reservation = state["selected_reservation"]

        yes_keywords = self.confirmation_keywords.get("yes", [])
        no_keywords = self.confirmation_keywords.get("no", [])

        if any(keyword in message for keyword in yes_keywords):
            return self._execute_reservation_cancellation(user_id, reservation)
        elif any(keyword in message for keyword in no_keywords):
            del self.user_states[user_id]
            return "予約取り消しをキャンセルいたします。予約はそのまま残ります。\nまたのご利用をお待ちしております。"
        else:
            return "「はい」または「確定」でキャンセルを確定するか、「キャンセル」で中止してください。"

    def _execute_reservation_cancellation(self, user_id: str, reservation: Dict[str, Any]) -> str:
        try:
            import pytz

            tokyo_tz = pytz.timezone("Asia/Tokyo")
            current_time = datetime.now(tokyo_tz)

            reservation_date = reservation["date"]
            reservation_start_time = reservation["start_time"]

            reservation_datetime = datetime.strptime(
                f"{reservation_date} {reservation_start_time}",
                "%Y-%m-%d %H:%M",
            )
            reservation_datetime = tokyo_tz.localize(reservation_datetime)

            cancel_limit_hours = self._get_reservation_limit_hours("cancel_limit_hours", 2)
            deadline_datetime = reservation_datetime - timedelta(hours=cancel_limit_hours)

            if current_time > deadline_datetime:
                return (
                    f"申し訳ございませんが、予約開始時刻の{cancel_limit_hours}時間以内のキャンセルはお受けできません。\n\n"
                    f"緊急の場合は直接サロンまでお電話ください。"
                )

        except Exception as e:
            logging.error(f"Error checking cancellation time limit: {e}")

        try:
            from api.google_sheets_logger import GoogleSheetsLogger

            sheets_logger = GoogleSheetsLogger()

            reservation_id = reservation["reservation_id"]
            sheets_success = sheets_logger.update_reservation_status(reservation_id, "Cancelled")

            if not sheets_success:
                return "申し訳ございません。エラーが発生しました。\nもう一度お試しください。"

            staff_name = reservation.get("staff")
            calendar_success = self.google_calendar.cancel_reservation_by_id(reservation_id, staff_name)

            if not calendar_success:
                logging.warning(f"Failed to remove reservation {reservation_id} from Google Calendar")

            try:
                from api.notification_manager import send_reservation_cancellation_notification

                client_name = self._get_line_display_name(user_id)
                send_reservation_cancellation_notification(reservation, client_name)
            except Exception as e:
                logging.error(f"Failed to send reservation cancellation notification: {e}")

            del self.user_states[user_id]

            return """✅キャンセルが完了しました。

ご都合が合う日があれば、いつでもご予約お待ちしております😊"""

        except Exception as e:
            logging.error(f"Reservation cancellation execution failed: {e}")
            return "申し訳ございません。エラーが発生しました。\nもう一度お試しください"

    def _handle_reservation_id_cancellation(self, user_id: str, reservation_id: str) -> str:
        try:
            from api.google_sheets_logger import GoogleSheetsLogger

            sheets_logger = GoogleSheetsLogger()
            sheets_success = sheets_logger.update_reservation_status(reservation_id, "Cancelled")

            if not sheets_success:
                return "申し訳ございません。エラーが発生しました。\nもう一度お試しください。"

            calendar_success = self.google_calendar.cancel_reservation_by_id(reservation_id)

            if not calendar_success:
                logging.warning(f"Failed to remove reservation {reservation_id} from Google Calendar")

            return """✅キャンセルが完了しました。

ご都合が合う日があれば、いつでもご予約お待ちしております😊"""

        except Exception as e:
            logging.error(f"Reservation ID cancellation failed: {e}")
            return "申し訳ございません。エラーが発生しました。\nもう一度お試しください。"


def main():
    print("=== Interactive Reservation Flow Tester ===")
    print("Type your messages to test the reservation system interactively!")
    print("Type 'quit' or 'exit' to stop testing.")
    print("Type 'help' to see available commands.")
    print("=" * 60)

    try:
        rf = ReservationFlow()
        print("✅ ReservationFlow initialized successfully")

        test_user_id = "interactive_test_user"

        print(f"\n🎯 Ready to test! User ID: {test_user_id}")
        print("💡 Try starting with: 予約したい")
        print("-" * 60)

        while True:
            try:
                user_input = input("\n👤 You: ").strip()

                if user_input.lower() in ["quit", "exit", "q"]:
                    print("👋 Goodbye! Thanks for testing!")
                    break
                elif user_input.lower() == "help":
                    print_help()
                    continue
                elif user_input.lower() == "status":
                    print_user_status(rf, test_user_id)
                    continue
                elif user_input.lower() == "clear":
                    clear_user_state(rf, test_user_id)
                    continue
                elif user_input.lower() == "reset":
                    test_user_id = f"interactive_test_user_{int(time.time())}"
                    print(f"🔄 Reset with new user ID: {test_user_id}")
                    continue
                elif not user_input:
                    print("⚠️ Please enter a message or command.")
                    continue

                response = rf.get_response(test_user_id, user_input)
                print(f"\n🤖 Bot: {response}")

                if test_user_id in rf.user_states:
                    current_step = rf.user_states[test_user_id].get("step", "unknown")
                    print(f"📊 Current step: {current_step}")
                else:
                    print("📊 Current step: No active session")

            except KeyboardInterrupt:
                print("\n\n👋 Goodbye! Thanks for testing!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                import traceback
                traceback.print_exc()

    except Exception as e:
        print(f"❌ Error during initialization: {e}")
        import traceback
        traceback.print_exc()


def print_help():
    print("\n" + "=" * 60)
    print("📖 INTERACTIVE TESTER HELP")
    print("=" * 60)
    print("🎯 RESERVATION FLOW COMMANDS:")
    print("  • 予約したい, 予約お願い, 予約できますか - Start reservation")
    print("  • カット, カラー, パーマ, トリートメント - Select service")
    print("  • 田中, 佐藤, 山田, 未指定 - Select staff")
    print("  • 2025-01-15 (or any date) - Select date")
    print("  • 10:00, 10:30, 10時, 10時30分 - Select start time")
    print("  • はい, 確定, お願い - Confirm reservation")
    print("  • いいえ, キャンセル, やめる - Cancel reservation")
    print("  • ← 戻る, 戻る - Go back one step in supported reservation flows")
    print()
    print("🔄 NAVIGATION COMMANDS:")
    print("  • 日付変更, 日付を変更, 別の日 - Go back to date selection")
    print("  • サービス変更, サービスを変更 - Go back to service selection")
    print("  • キャンセル, 取り消し, やめる - Cancel current flow")
    print()
    print("📋 RESERVATION MANAGEMENT:")
    print("  • 予約キャンセル, 予約取り消し - Cancel existing reservation")
    print("  • 予約変更, 予約修正 - Modify existing reservation (re-reservation flow)")
    print()
    print("🛠️ TESTER COMMANDS:")
    print("  • help - Show this help message")
    print("  • status - Show current user state")
    print("  • clear - Clear current user state")
    print("  • reset - Reset with new user ID")
    print("  • quit, exit, q - Exit the tester")
    print("=" * 60)


def print_user_status(rf, user_id):
    print(f"\n📊 USER STATUS: {user_id}")
    print("-" * 40)

    if user_id in rf.user_states:
        state = rf.user_states[user_id]
        step = state.get("step", "unknown")
        data = state.get("data", {})

        print(f"Current Step: {step}")
        print("Reservation Data:")
        for key, value in data.items():
            print(f"  • {key}: {value}")

        if "is_modification" in state:
            print(f"  • is_modification: {state.get('is_modification')}")
        if "original_reservation" in state:
            print(f"  • original_reservation: {state.get('original_reservation')}")
        if "date_selection_back_target" in state:
            print(f"  • date_selection_back_target: {state.get('date_selection_back_target')}")
    else:
        print("No active session")

    print("-" * 40)


def clear_user_state(rf, user_id):
    if user_id in rf.user_states:
        del rf.user_states[user_id]
        print(f"✅ Cleared user state for {user_id}")
    else:
        print(f"ℹ️ No user state found for {user_id}")


if __name__ == "__main__":
    main()
