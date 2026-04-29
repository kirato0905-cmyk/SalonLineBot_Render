import re
import os
import json
import time
import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta, date

from api.google_calendar import GoogleCalendarHelper
from api.business_hours import (
    get_slot_minutes,
    is_open_date,
    get_reservation_ui_limit_days,
)
from api.google_sheets_logger import get_sheets_logger
from api.staff_attendance import get_staff_attendance_for_date


class ReservationFlow:
    def __init__(self):
        self.user_states = {}
        self.google_calendar = GoogleCalendarHelper()
        self.line_configuration = None
        self.sheets_logger = get_sheets_logger()
        self._profile_cache: Dict[str, Dict[str, Any]] = {}
        self._profile_cache_ttl_seconds = 3600

        self.config_data = self._load_config_data()
        self.services = self.config_data.get("services", {})
        self.staff_members = self.config_data.get("staff", {})

        # config.json から直接取得
        self.intent_keywords = self.config_data.get("intent_keywords", {})
        self.navigation_keywords = self.config_data.get("navigation_keywords", {})
        self.confirmation_keywords = self.config_data.get("confirmation_keywords", {})

        self.settings_data = self._extract_settings_from_config(self.config_data)
        self.service_categories = self.config_data.get("service_categories", [])
        self.featured_sets = self.config_data.get("featured_sets", [])
        self.category_input_aliases = {
            "cut": ["カット系", "カット"],
            "color": ["カラー系", "カラー"],
            "perm": ["パーマ系", "パーマ"],
            "straight": ["縮毛矯正系", "ストレート系", "ストレート"],
            "treatment": ["トリートメント系", "トリートメント"],
            "spa": ["ヘッドスパ系", "ヘッドスパ"],
            "other": ["その他メニュー", "その他", "セット"],
        }

        self.back_label = "← 戻る"
        self._config_mtime = self._get_config_mtime()


    def _ensure_runtime_cache(self, user_id: Optional[str]) -> Dict[str, Any]:
        if not user_id:
            return {}
        state = self.user_states.setdefault(user_id, {"step": "start", "data": {"user_id": user_id}})
        return state.setdefault("_runtime_cache", {
            "available_slots": {},
            "staff_slots_map": {},
            "user_day_events": {},
        })

    def _clear_runtime_cache(self, user_id: Optional[str]):
        if not user_id:
            return
        state = self.user_states.get(user_id)
        if not state:
            return
        state.pop("_runtime_cache", None)

    def _make_available_slots_cache_key(
        self,
        selected_date: str,
        staff_name: Optional[str],
        current_service_id: Optional[str],
        exclude_reservation_id: Optional[str],
    ) -> str:
        staff_key = staff_name if staff_name else "__NO_STAFF__"
        service_key = current_service_id if current_service_id else "__NO_SERVICE__"
        exclude_key = exclude_reservation_id if exclude_reservation_id else "__NO_EXCLUDE__"
        return f"{selected_date}|{staff_key}|{service_key}|{exclude_key}"

    def _get_exclude_reservation_id_for_date(
        self,
        user_id: Optional[str],
        selected_date: str,
    ) -> Optional[str]:
        if not user_id or user_id not in self.user_states:
            return None

        original_reservation = None
        if self.user_states[user_id].get("is_modification", False):
            original_reservation = self.user_states[user_id].get("original_reservation")

        if original_reservation and original_reservation.get("date") == selected_date:
            return original_reservation.get("reservation_id")

        return None

    def _time_range_fits_any_slot(
        self,
        slots: List[Dict[str, Any]],
        start_time: str,
        end_time: str,
    ) -> bool:
        for slot in slots:
            slot_start = slot.get("time")
            slot_end = slot.get("end_time")
            if not slot_start or not slot_end:
                continue
            if slot_start <= start_time and end_time <= slot_end:
                return True
        return False

    def _get_staff_available_slots_map_for_date(
        self,
        user_id: Optional[str],
        selected_date: str,
        service_id: Optional[str],
        exclude_reservation_id: Optional[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        runtime_cache = self._ensure_runtime_cache(user_id)

        service_ids = self._get_current_service_ids(user_id) if user_id else ([service_id] if service_id else [])
        service_key = ",".join(sorted([sid for sid in service_ids if sid])) or (service_id or "__NO_SERVICE__")

        cache_key = self._make_available_slots_cache_key(
            selected_date=selected_date,
            staff_name="__STAFF_MAP__",
            current_service_id=f"__CART__:{service_key}",
            exclude_reservation_id=exclude_reservation_id,
        )

        if runtime_cache and cache_key in runtime_cache["staff_slots_map"]:
            return runtime_cache["staff_slots_map"][cache_key]

        staff_slots_map: Dict[str, List[Dict[str, Any]]] = {}

        if user_id:
            selectable_staff = self._get_selectable_staff_records_for_cart(service_ids)
        else:
            selectable_staff = self._get_selectable_staff_records(service_id)

        for _staff_key, staff_data in selectable_staff:
            staff_name = staff_data.get("name")
            if not staff_name:
                continue

            try:
                slots = self.google_calendar.get_available_slots_for_modification(
                    selected_date,
                    exclude_reservation_id,
                    staff_name,
                    None,
                )
            except Exception as e:
                logging.warning(f"Failed to get slots for staff={staff_name}, date={selected_date}: {e}")
                slots = []

            staff_slots_map[staff_name] = [
                slot for slot in slots
                if slot.get("date") == selected_date and slot.get("available")
            ]

        if runtime_cache is not None:
            runtime_cache["staff_slots_map"][cache_key] = staff_slots_map

        return staff_slots_map

    def _get_user_day_event_ranges(
        self,
        user_id: Optional[str],
        selected_date: str,
        exclude_reservation_id: Optional[str] = None,
    ) -> List[Tuple[datetime, datetime]]:
        if not user_id:
            return []

        runtime_cache = self._ensure_runtime_cache(user_id)
        cache_key = f"{selected_date}|{exclude_reservation_id or '__NO_EXCLUDE__'}"
        if runtime_cache and cache_key in runtime_cache["user_day_events"]:
            return runtime_cache["user_day_events"][cache_key]

        event_ranges: List[Tuple[datetime, datetime]] = []
        all_events: List[Dict[str, Any]] = []

        try:
            if self.google_calendar.calendar_id:
                all_events.extend(self.google_calendar.get_events_for_date(selected_date, None))
        except Exception as e:
            logging.warning(f"Failed to get base calendar events for {selected_date}: {e}")

        for _staff_id, staff_data in self.staff_members.items():
            if not isinstance(staff_data, dict):
                continue
            staff_name = staff_data.get("name")
            if not staff_name:
                continue
            try:
                all_events.extend(self.google_calendar.get_events_for_date(selected_date, staff_name))
            except Exception as e:
                logging.warning(f"Failed to get staff events for {staff_name} on {selected_date}: {e}")

        for event in all_events:
            if exclude_reservation_id:
                description = event.get("description", "")
                if f"予約ID: {exclude_reservation_id}" in description:
                    continue

            if not self.google_calendar._is_user_reservation(event, user_id):
                continue

            event_start = self.google_calendar._parse_event_datetime(event.get("start", {}), default_is_end=False)
            event_end = self.google_calendar._parse_event_datetime(event.get("end", {}), default_is_end=True)

            if event_start and event_end:
                tz = self.google_calendar.timezone
                try:
                    import pytz
                    local_tz = pytz.timezone(tz)
                    event_start = event_start.astimezone(local_tz).replace(tzinfo=None)
                    event_end = event_end.astimezone(local_tz).replace(tzinfo=None)
                except Exception:
                    event_start = event_start.replace(tzinfo=None)
                    event_end = event_end.replace(tzinfo=None)

                event_ranges.append((event_start, event_end))

        if runtime_cache is not None:
            runtime_cache["user_day_events"][cache_key] = event_ranges

        return event_ranges

    def _has_local_user_conflict(
        self,
        user_id: Optional[str],
        selected_date: str,
        start_time: str,
        end_time: str,
        exclude_reservation_id: Optional[str] = None,
    ) -> bool:
        if not user_id:
            return False

        try:
            start_dt = datetime.strptime(f"{selected_date} {start_time}", "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(f"{selected_date} {end_time}", "%Y-%m-%d %H:%M")
        except Exception:
            return False

        for event_start, event_end in self._get_user_day_event_ranges(
            user_id=user_id,
            selected_date=selected_date,
            exclude_reservation_id=exclude_reservation_id,
        ):
            if start_dt < event_end and end_dt > event_start:
                return True

        return False

    def _resolve_assignable_staff_locally(
        self,
        user_id: Optional[str],
        selected_date: str,
        start_time: str,
        end_time: str,
        service_id: Optional[str],
        exclude_reservation_id: Optional[str] = None,
    ) -> Optional[str]:
        duration_minutes = self._calculate_time_duration_minutes(start_time, end_time)
        service_ids = self._get_current_service_ids(user_id) if user_id else ([service_id] if service_id else [])
        primary_service_id = service_ids[0] if service_ids else service_id

        assigned = self.google_calendar.assign_staff_for_free_reservation(
            date_str=selected_date,
            start_time=start_time,
            duration_minutes=duration_minutes,
            service_id=primary_service_id,
            service_ids=service_ids,
            exclude_reservation_id=exclude_reservation_id,
        )
        if not assigned:
            return None
        return assigned["staff_name"]


    def _get_config_mtime(self) -> Optional[float]:
        try:
            return os.path.getmtime(self._config_path())
        except Exception:
            return None

    def _config_path(self) -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, "data", "config.json")

    def _load_config_data(self) -> Dict[str, Any]:
        try:
            with open(self._config_path(), "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load config data: {e}")
            raise RuntimeError(f"Cannot load config.json: {e}")

    def _extract_settings_from_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "timezone": config.get("salon", {}).get("timezone", "Asia/Tokyo"),
            "business_hours": config.get("calendar", {}).get("business_hours", {}),
            "monthly_closed": config.get("calendar", {}).get("monthly_closed", []),
            "closed_dates": config.get("calendar", {}).get("closed_dates", []),
            "special_hours": config.get("calendar", {}).get("special_hours", []),
            "booking_rules": {
                "slot_minutes": config.get("booking", {}).get("slot_minutes", 30)
            },
            "reservation_ui_limit_days": config.get("booking", {}).get("reservation_ui_limit_days", 45),
            "reservation_rules": config.get("booking", {}).get("reservation_rules", {}),
            "recommendation_rules": config.get("booking", {}).get("recommendation_rules", {}),
        }

    def _reload_settings(self, force: bool = False) -> Dict[str, Any]:
        current_mtime = self._get_config_mtime()
        if not force and current_mtime is not None and self._config_mtime == current_mtime:
            return self.settings_data

        self.config_data = self._load_config_data()
        self.services = self.config_data.get("services", {})
        self.staff_members = self.config_data.get("staff", {})

        # config.json 再読込時にキーワードも更新
        self.intent_keywords = self.config_data.get("intent_keywords", {})
        self.navigation_keywords = self.config_data.get("navigation_keywords", {})
        self.confirmation_keywords = self.config_data.get("confirmation_keywords", {})

        self.settings_data = self._extract_settings_from_config(self.config_data)
        self.service_categories = self.config_data.get("service_categories", [])
        self.featured_sets = self.config_data.get("featured_sets", [])
        self._config_mtime = current_mtime
        return self.settings_data

    def _normalize_input_text(self, text: str) -> str:
        if text is None:
            return ""
        return str(text).strip()

    def _match_keyword_group(self, text: str, keywords: List[str]) -> bool:
        normalized = self._normalize_input_text(text)
        normalized_keywords = [self._normalize_input_text(k) for k in keywords if k]
        return normalized in normalized_keywords

    def _get_reservation_limit_hours(self, rule_key: str, default: int = 2) -> int:
        """
        Reload config.json every time for immediate reflection.
        Fallback to default when the setting is missing or invalid.
        """
        try:
            self._reload_settings()
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

    def _get_recommendation_rules(self) -> Dict[str, Any]:
        """
        Reload config.json every time for immediate reflection.
        """
        try:
            self._reload_settings()
            recommendation_rules = self.settings_data.get("recommendation_rules", {})
            if not isinstance(recommendation_rules, dict):
                return {}
            return recommendation_rules
        except Exception as e:
            logging.warning(f"Invalid recommendation_rules. error={e}")
            return {}

    def _get_recommend_count(self, default: int = 2) -> int:
        try:
            rules = self._get_recommendation_rules()
            value = rules.get("recommend_count", default)

            if isinstance(value, bool):
                return default
            if isinstance(value, str):
                value = value.strip()
                if not value.isdigit():
                    return default
                value = int(value)
            if not isinstance(value, (int, float)):
                return default

            value = int(value)
            if value < 1:
                return default
            if value > 4:
                return 4
            return value
        except Exception:
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

    def _time_to_minutes(self, time_str: str) -> Optional[int]:
        try:
            hour, minute = map(int, time_str.split(":"))
            return hour * 60 + minute
        except Exception:
            return None

    def _get_preferred_time_range_bonus(self, start_time: str) -> int:
        try:
            rules = self._get_recommendation_rules()
            ranges = rules.get("preferred_time_ranges", [])
            if not isinstance(ranges, list):
                return 0

            target_min = self._time_to_minutes(start_time)
            if target_min is None:
                return 0

            bonus = 0
            for item in ranges:
                if not isinstance(item, dict):
                    continue
                s = item.get("start")
                e = item.get("end")
                score = item.get("score", 0)

                start_min = self._time_to_minutes(s) if s else None
                end_min = self._time_to_minutes(e) if e else None

                if start_min is None or end_min is None:
                    continue

                    # bool除外
                if isinstance(score, bool):
                    continue
                if isinstance(score, str):
                    if score.strip().lstrip("-").isdigit():
                        score = int(score.strip())
                    else:
                        continue
                if not isinstance(score, (int, float)):
                    continue

                if start_min <= target_min < end_min:
                    bonus += int(score)

            return bonus
        except Exception as e:
            logging.warning(f"Preferred time range bonus error: {e}")
            return 0

    def _get_daytime_bonus(self, start_time: str) -> int:
        """
        Default philosophy:
        - morning: a little bonus
        - midday/early afternoon: stronger bonus
        - late evening: slight penalty
        """
        try:
            rules = self._get_recommendation_rules()
            bonus_cfg = rules.get("business_hours_bonus", {})
            if not isinstance(bonus_cfg, dict):
                bonus_cfg = {}

            morning_bonus = bonus_cfg.get("morning_bonus", 8)
            midday_bonus = bonus_cfg.get("midday_bonus", 12)
            evening_penalty = bonus_cfg.get("evening_penalty", -5)

            def _to_int(v, d):
                if isinstance(v, bool):
                    return d
                if isinstance(v, str):
                    if v.strip().lstrip("-").isdigit():
                        return int(v.strip())
                    return d
                if isinstance(v, (int, float)):
                    return int(v)
                return d

            morning_bonus = _to_int(morning_bonus, 8)
            midday_bonus = _to_int(midday_bonus, 12)
            evening_penalty = _to_int(evening_penalty, -5)

            m = self._time_to_minutes(start_time)
            if m is None:
                return 0

            if 10 * 60 <= m < 12 * 60:
                return morning_bonus
            if 12 * 60 <= m < 15 * 60:
                return midday_bonus
            if m >= 17 * 60:
                return evening_penalty
            return 0
        except Exception as e:
            logging.warning(f"Daytime bonus error: {e}")
            return 0

    def _get_recency_bonus(self, selected_date: str, start_time: str) -> int:
        """
        Prioritize times that are easier to commit to soon.
        Same-day near-future gets the strongest bonus.
        """
        try:
            import pytz

            tokyo_tz = pytz.timezone("Asia/Tokyo")
            now_dt = datetime.now(tokyo_tz)

            candidate_naive = datetime.strptime(
                f"{selected_date} {start_time}",
                "%Y-%m-%d %H:%M",
            )
            candidate_dt = tokyo_tz.localize(candidate_naive)

            diff_minutes = int((candidate_dt - now_dt).total_seconds() // 60)
            if diff_minutes < 0:
                return -9999

            days_diff = (candidate_dt.date() - now_dt.date()).days

            if days_diff == 0:
                if diff_minutes <= 180:
                    return 60
                if diff_minutes <= 360:
                    return 45
                if diff_minutes <= 720:
                    return 30
                return 15

            if days_diff == 1:
                if candidate_dt.hour < 12:
                    return 28
                if candidate_dt.hour < 16:
                    return 24
                return 18

            if days_diff <= 3:
                return 12

            return 5
        except Exception as e:
            logging.warning(f"Recency bonus error: {e}")
            return 0

    def _score_time_option(
        self,
        selected_date: str,
        start_time: str,
    ) -> int:
        score = 0
        score += self._get_recency_bonus(selected_date, start_time)
        score += self._get_preferred_time_range_bonus(start_time)
        score += self._get_daytime_bonus(start_time)

        minutes = self._time_to_minutes(start_time)
        if minutes is not None:
            score += max(0, 1000 - minutes) // 1000

        return score

    def _sort_time_options_for_recommendation(
        self,
        selected_date: str,
        time_options: List[str],
    ) -> List[str]:
        """
        Higher score first. If score ties, earlier time first.
        """
        try:
            scored: List[Tuple[str, int, int]] = []
            for t in time_options:
                score = self._score_time_option(selected_date, t)
                minutes = self._time_to_minutes(t)
                if minutes is None:
                    minutes = 9999
                scored.append((t, score, minutes))

            scored.sort(key=lambda x: (-x[1], x[2]))
            return [t for t, _, _ in scored]
        except Exception as e:
            logging.warning(f"Sort time options for recommendation failed: {e}")
            return sorted(time_options)

    def _compress_time_options_for_text(
        self,
        time_options: List[str],
        recommend_count: int = 2,
        other_count: int = 3,
    ) -> List[str]:
        """
        Compress only message text display for readability.
        Quick reply options remain unchanged.
        Prefer about 1-hour spacing while preserving recommendation order.
        """
        if not time_options:
            return []

        max_count = recommend_count + other_count
        if len(time_options) <= max_count:
            return time_options

        selected: List[str] = []
        selected_minutes: List[int] = []

        for t in time_options:
            t_min = self._time_to_minutes(t)
            if t_min is None:
                continue

            if not selected:
                selected.append(t)
                selected_minutes.append(t_min)
                if len(selected) >= max_count:
                    break
                continue

            if all(abs(t_min - s_min) >= 60 for s_min in selected_minutes):
                selected.append(t)
                selected_minutes.append(t_min)
                if len(selected) >= max_count:
                    break

        if len(selected) < max_count:
            for t in time_options:
                if t in selected:
                    continue
                selected.append(t)
                if len(selected) >= max_count:
                    break

        return selected[:max_count]

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
        service_ids = self._get_current_service_ids(user_id)
        return service_ids[0] if service_ids else None

    def _get_service_id_by_name(self, service_name: str) -> Optional[str]:
        for _key, service_data in self.services.items():
            if isinstance(service_data, dict) and service_data.get("name") == service_name:
                return service_data.get("id")
        return None

    def _get_service_categories(self) -> List[Dict[str, Any]]:
        categories = self.config_data.get("service_categories", [])
        if not isinstance(categories, list):
            return []
        return sorted(
            [c for c in categories if isinstance(c, dict) and c.get("id")],
            key=lambda c: int(c.get("display_order", 999)),
        )

    def _get_featured_sets(self) -> List[Dict[str, Any]]:
        featured_sets = self.config_data.get("featured_sets", [])
        if not isinstance(featured_sets, list):
            return []
        valid_sets = []
        for item in featured_sets:
            if not isinstance(item, dict):
                continue
            if not item.get("is_active", True):
                continue
            if self._validate_featured_set(item):
                valid_sets.append(item)
        return sorted(valid_sets, key=lambda x: int(x.get("display_order", 999)))

    def _validate_featured_set(self, featured_set: Dict[str, Any]) -> bool:
        service_ids = featured_set.get("services", [])
        if not isinstance(service_ids, list) or not service_ids:
            return False
        for service_id in service_ids:
            service = self._get_service_by_id(service_id)
            if not service:
                return False
            if service.get("is_active", True) is False:
                return False
        return True

    def _get_featured_set_by_id(self, featured_set_id: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_input_text(featured_set_id)
        for item in self._get_featured_sets():
            if self._normalize_input_text(item.get("id")) == normalized:
                return item
        return None

    def _get_category_name_by_id(self, category_id: str) -> str:
        for category in self._get_service_categories():
            if str(category.get("id")) == str(category_id):
                return str(category.get("name"))
        return str(category_id)

    def _resolve_category_id_from_text(self, text: str) -> Optional[str]:
        normalized = self._normalize_input_text(text)
        if not normalized:
            return None

        for category in self._get_service_categories():
            if normalized == self._normalize_input_text(category.get("name")):
                return str(category.get("id"))
            if normalized == self._normalize_input_text(category.get("id")):
                return str(category.get("id"))

        for category_id, aliases in self.category_input_aliases.items():
            if normalized in aliases:
                return category_id

        return None

    def _get_services_by_category(self, category_id: str) -> List[Tuple[str, Dict[str, Any]]]:
        matched = []
        for _key, service_data in self.services.items():
            if not isinstance(service_data, dict):
                continue
            if service_data.get("is_active", True) is False:
                continue
            if str(service_data.get("category")) != str(category_id):
                continue
            if not service_data.get("id"):
                continue
            matched.append((service_data.get("id"), service_data))
        matched.sort(key=lambda x: (int(x[1].get("display_order", 999)), str(x[1].get("name", ""))))
        return matched

    def _build_initial_menu_selection_message(self) -> Dict[str, Any]:
        lines = [
            "ご希望のメニューをお選びください👇",
            "",
            "【人気セットメニュー】",
        ]
        items: List[Dict[str, Any]] = []
        for featured_set in self._get_featured_sets():
            lines.append(f"・{featured_set.get('name')}")
            items.append({
                "label": str(featured_set.get("name")),
                "type": "postback",
                "data": f"action=select_featured_set&set_id={featured_set.get('id')}",
            })

        lines.extend([
            "",
            "【その他】",
            "・メニューを見る",
        ])
        items.append({
            "label": "メニューを見る",
            "text": "メニューを見る",
        })
        return self._quick_reply_return("\n".join(lines), items, include_cancel=True, include_back=False)

    def _build_category_selection_message(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        lines = []
        if prefix:
            lines.extend([prefix, ""])
        lines.append("ご希望のメニューカテゴリをお選びください👇")
        lines.append("")
        items = []
        for category in self._get_service_categories():
            name = str(category.get("name"))
            lines.append(f"・{name}")
            items.append({"label": name, "text": name})
        return self._quick_reply_return("\n".join(lines), items, include_cancel=True, include_back=True)

    def _build_service_selection_message_for_category(self, user_id: str, category_id: str, prefix: Optional[str] = None) -> Dict[str, Any]:
        category_name = self._get_category_name_by_id(category_id)
        services = self._get_services_by_category(category_id)
        lines = []
        if prefix:
            lines.extend([prefix, ""])
        lines.append(f"{category_name}のメニューをお選びください👇")
        lines.append("")
        items: List[Dict[str, Any]] = []
        for service_id, service_data in services:
            name = str(service_data.get("name", service_id))
            lines.append(f"・{name}")
            items.append({
                "label": name,
                "type": "postback",
                "data": f"action=select_service&service_id={service_id}",
            })
        self.user_states[user_id]["step"] = "service_detail_selection"
        self.user_states[user_id].setdefault("data", {})["selected_service_category"] = category_id
        return self._quick_reply_return("\n".join(lines), items, include_cancel=True, include_back=True)

    def _add_featured_set_to_cart(self, user_id: str, featured_set_id: str) -> Dict[str, Any]:
        featured_set = self._get_featured_set_by_id(featured_set_id)
        if not featured_set:
            return {"ok": False, "reason": "invalid_set"}

        service_ids = featured_set.get("services", [])
        if not isinstance(service_ids, list) or not service_ids:
            return {"ok": False, "reason": "invalid_set"}

        for service_id in service_ids:
            if any(item.get("service_id") == service_id for item in self._get_cart(user_id)):
                return {"ok": False, "reason": "duplicate"}

        added_items = []
        for service_id in service_ids:
            add_result = self._add_service_to_cart(user_id, service_id)
            if not add_result.get("ok"):
                return add_result
            added_items.append(add_result["item"])

        data = self.user_states[user_id].setdefault("data", {})
        data["selected_menu_label"] = featured_set.get("name")
        data["featured_set_id"] = featured_set.get("id")
        data["total_price"] = int(featured_set.get("price", self._get_cart_total_price(user_id)) or 0)
        data["total_duration"] = int(featured_set.get("duration", self._get_cart_total_duration(user_id)) or 0)
        return {"ok": True, "set": featured_set, "items": added_items}

    def _build_cart_item_from_service(self, service_id: str) -> Optional[Dict[str, Any]]:
        service = self._get_service_by_id(service_id)
        if not service:
            return None
        return {
            "service_id": service.get("id"),
            "service_name": service.get("name", service_id),
            "price": int(service.get("price", 0) or 0),
            "duration": int(service.get("duration", 0) or 0),
        }

    def _get_cart(self, user_id: str) -> List[Dict[str, Any]]:
        data = self.user_states.get(user_id, {}).get("data", {})
        cart = data.get("cart", [])
        if isinstance(cart, list):
            normalized = []
            for item in cart:
                if not isinstance(item, dict):
                    continue
                service_id = item.get("service_id")
                service_name = item.get("service_name") or item.get("name")
                if not service_id and service_name:
                    service_id = self._get_service_id_by_name(service_name)
                if not service_id:
                    continue
                normalized.append({
                    "service_id": service_id,
                    "service_name": service_name or self._get_service_name_by_id(service_id),
                    "price": int(item.get("price", 0) or 0),
                    "duration": int(item.get("duration", 0) or 0),
                })
            return normalized

        sid = data.get("service_id")
        if sid:
            item = self._build_cart_item_from_service(sid)
            return [item] if item else []

        service_name = data.get("service")
        if service_name:
            sid = self._get_service_id_by_name(service_name)
            item = self._build_cart_item_from_service(sid) if sid else None
            return [item] if item else []

        return []

    def _get_current_service_ids(self, user_id: str) -> List[str]:
        return [
            item["service_id"]
            for item in self._get_cart(user_id)
            if item.get("service_id")
        ]

    def _get_cart_total_price(self, user_id: str) -> int:
        return sum(int(item.get("price", 0) or 0) for item in self._get_cart(user_id))

    def _get_cart_total_duration(self, user_id: str) -> int:
        return sum(int(item.get("duration", 0) or 0) for item in self._get_cart(user_id))

    def _format_service_summary(self, services: List[Dict[str, Any]]) -> str:
        names = [str(item.get("service_name", "")).strip() for item in services if item.get("service_name")]
        return " / ".join(names)

    def _sync_cart_to_reservation_fields(self, user_id: str) -> None:
        if user_id not in self.user_states:
            return
        data = self.user_states[user_id].setdefault("data", {})
        cart = self._get_cart(user_id)
        data["cart"] = cart
        data["services"] = [dict(item) for item in cart]
        data["service"] = self._format_service_summary(cart)
        data["service_id"] = cart[0]["service_id"] if cart else None
        data["total_duration"] = self._get_cart_total_duration(user_id)
        data["total_price"] = self._get_cart_total_price(user_id)
        if not cart:
            data.pop("selected_menu_label", None)
            data.pop("featured_set_id", None)

    def _add_service_to_cart(self, user_id: str, service_id: str) -> Dict[str, Any]:
        state = self.user_states.setdefault(user_id, {"step": "start", "data": {"user_id": user_id}})
        data = state.setdefault("data", {})
        cart = self._get_cart(user_id)
        if any(item.get("service_id") == service_id for item in cart):
            return {"ok": False, "reason": "duplicate"}

        service = self._get_service_by_id(service_id)
        item = self._build_cart_item_from_service(service_id)
        if not item or not service:
            return {"ok": False, "reason": "invalid"}

        can_combine = service.get("can_combine", True)
        if cart and can_combine is False:
            return {"ok": False, "reason": "cannot_combine"}

        for existing_item in cart:
            existing_service = self._get_service_by_id(existing_item.get("service_id"))
            if existing_service and existing_service.get("can_combine", True) is False:
                return {"ok": False, "reason": "cannot_combine"}

        cart.append(item)
        data["cart"] = cart
        if data.get("selected_menu_label") and data.get("featured_set_id"):
            # セット選択後に単品追加されたら、表示ラベルは維持しつつ内容は単品ベースで扱う
            pass
        self._sync_cart_to_reservation_fields(user_id)
        return {"ok": True, "item": item}

    def _remove_service_from_cart(self, user_id: str, service_id: str) -> bool:
        if user_id not in self.user_states:
            return False
        data = self.user_states[user_id].setdefault("data", {})
        cart = self._get_cart(user_id)
        new_cart = [item for item in cart if item.get("service_id") != service_id]
        if len(new_cart) == len(cart):
            return False
        data["cart"] = new_cart
        self._sync_cart_to_reservation_fields(user_id)
        return True

    def _has_staff_service_capability(self, staff_data: Dict[str, Any], service_ids: List[str]) -> bool:
        if not service_ids:
            return True
        configured = staff_data.get("service_ids")
        if isinstance(configured, list) and configured:
            return all(service_id in configured for service_id in service_ids)
        return True

    def _build_cart_summary_text(self, user_id: str, prefix: str = "現在のご予約内容です。") -> str:
        cart = self._get_cart(user_id)
        if not cart:
            return "選択中のメニューがありません。\nご希望のメニューをお選びください👇"

        lines = [prefix, ""]
        for item in cart:
            lines.append(f"・{item.get('service_name', '')}")
        lines.extend([
            "",
            f"合計金額：{self._get_cart_total_price(user_id):,}円",
            f"合計時間：{self._get_cart_total_duration(user_id)}分",
            "",
            "次の操作をお選びください👇",
        ])
        return "\n".join(lines)

    def _build_cart_action_message(self, user_id: str, prefix: Optional[str] = None) -> Dict[str, Any]:
        self.user_states[user_id]["step"] = "service_cart"
        text = self._build_cart_summary_text(user_id, prefix=prefix or "現在のご予約内容です。")
        items = [
            {"label": "他のメニューを追加", "text": "他のメニューを追加"},
            {"label": "このメニューで確定", "text": "このメニューで確定"},
            {"label": "メニューを削除", "text": "メニューを削除"},
        ]
        return self._quick_reply_return(
            text,
            items,
            include_cancel=True,
            include_back=True,
        )

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
            flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
            cancel_text = flow_cancel_keywords[0] if flow_cancel_keywords else "やめる"
            final_items.append({"label": cancel_text, "text": cancel_text})

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

        self._clear_runtime_cache(user_id)

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

        self._clear_runtime_cache(user_id)

    def _clear_time_selection(self, user_id: str):
        state = self.user_states.get(user_id, {})
        data = state.get("data", {})
        for key in ["start_time", "end_time", "time"]:
            data.pop(key, None)

        self._clear_runtime_cache(user_id)

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

    def _filter_time_options_by_deadline(
        self,
        user_id: str,
        selected_date: str,
        time_options: List[str],
    ) -> List[str]:
        valid_times = []

        for start_time in time_options:
            is_valid, _ = self._check_advance_booking_time(
                selected_date,
                start_time,
                user_id,
            )
            if is_valid:
                valid_times.append(start_time)

        return valid_times

    def _filter_time_options_by_actual_availability(
        self,
        user_id: str,
        selected_date: str,
        staff_name: Optional[str],
        time_options: List[str],
        service_duration: int,
    ) -> List[str]:
        valid_times = []
        exclude_reservation_id = self._get_exclude_reservation_id_for_date(user_id, selected_date)
        service_ids = self._get_current_service_ids(user_id) if user_id else []
        primary_service_id = service_ids[0] if service_ids else None

        staff_slots_map: Dict[str, List[Dict[str, Any]]] = {}
        if self._is_no_preference_staff(staff_name):
            staff_slots_map = self._get_staff_available_slots_map_for_date(
                user_id=user_id,
                selected_date=selected_date,
                service_id=primary_service_id,
                exclude_reservation_id=exclude_reservation_id,
            )

        for start_time in time_options:
            end_time = self._calculate_optimal_end_time(start_time, service_duration)

            if self._is_no_preference_staff(staff_name):
                assignable_staff = self._resolve_assignable_staff_locally(
                    user_id=user_id,
                    selected_date=selected_date,
                    start_time=start_time,
                    end_time=end_time,
                    service_id=primary_service_id,
                    exclude_reservation_id=exclude_reservation_id,
                )
                if not assignable_staff:
                    continue
            else:
                # 指名ありは、available_slots から既に絞られた枠をもとに time_options を作っているため
                # ここでは重い再問い合わせをせず、最終確定時に厳密チェックする。
                assignable_staff = staff_name

            if self._has_local_user_conflict(
                user_id=user_id,
                selected_date=selected_date,
                start_time=start_time,
                end_time=end_time,
                exclude_reservation_id=exclude_reservation_id,
            ):
                continue

            valid_times.append(start_time)

        return valid_times

    def _generate_valid_time_options(
        self,
        user_id: str,
        selected_date: str,
        filtered_periods: List[Dict[str, Any]],
        service_duration: int,
        staff_name: Optional[str] = None,
    ) -> List[str]:
        time_options = self._build_time_options_30min(filtered_periods, service_duration)
        time_options = self._filter_time_options_by_deadline(
            user_id=user_id,
            selected_date=selected_date,
            time_options=time_options,
        )
        time_options = self._filter_time_options_by_actual_availability(
            user_id=user_id,
            selected_date=selected_date,
            staff_name=staff_name,
            time_options=time_options,
            service_duration=service_duration,
        )
        time_options = self._sort_time_options_for_recommendation(
            selected_date=selected_date,
            time_options=time_options,
        )
        return time_options

    def _build_time_selection_text(
        self,
        selected_date: str,
        service_name: str,
        service_duration: int,
        time_options: List[str],
    ) -> str:
        if not time_options:
            return (
                f"{selected_date}ですね👌\n\n"
                f"{service_name}（{service_duration}分）のご案内可能な時間がありません。"
            )

        recommend_count = self._get_recommend_count(2)
        other_count = 3

        display_options = self._compress_time_options_for_text(
            time_options=time_options,
            recommend_count=recommend_count,
            other_count=other_count,
        )

        recommended = display_options[:recommend_count]
        others = display_options[recommend_count:recommend_count + other_count]

        lines = [
            f"{selected_date}ですね👌",
            "",
            f"{service_name}（{service_duration}分）の空き状況はこちら。",
            "",
        ]

        if recommended:
            lines.append("【🔥おすすめ】")
            for t in recommended:
                lines.append(f"・{t}～")
            lines.append("")

        if others:
            lines.append("【その他】")
            for t in others:
                lines.append(f"・{t}～")
            lines.append("")

        lines.append("ご希望の時間をお選びください👇")
        return "\n".join(lines)

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

    # =========================================================
    # staff helper
    # =========================================================
    def _get_active_staff_records(self) -> List[Tuple[str, Dict[str, Any]]]:
        """
        config.json の staff から
        - is_active != False
        - name != 未指定
        のスタッフだけを order 順で返す
        """
        active_staff = []

        for staff_key, staff_data in self.staff_members.items():
            if not isinstance(staff_data, dict):
                continue

            staff_name = staff_data.get("name", staff_key)
            is_active = staff_data.get("is_active", True)

            if not is_active:
                continue
            if staff_name == "未指定":
                continue

            active_staff.append((staff_key, staff_data))

        active_staff.sort(
            key=lambda item: (
                item[1].get("order", 999),
                item[1].get("name", item[0]),
            )
        )
        return active_staff

    def _staff_can_handle_service(self, staff_data: Dict[str, Any], service_id: Optional[str]) -> bool:
        """
        service_ids が空 / 未設定なら制限なし扱い。
        service_ids が設定されていれば、その中に service_id があるときだけ対応可。
        """
        if not service_id:
            return True
        return self._has_staff_service_capability(staff_data, [service_id])

    def _get_selectable_staff_records(
        self,
        service_id: Optional[str] = None,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        service_ids = [service_id] if service_id else []
        return self._get_selectable_staff_records_for_cart(service_ids)

    def _get_selectable_staff_records_for_cart(
        self,
        service_ids: Optional[List[str]] = None,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        staff_records = self._get_active_staff_records()
        normalized_ids = [sid for sid in (service_ids or []) if sid]
        return [
            (staff_key, staff_data)
            for staff_key, staff_data in staff_records
            if self._has_staff_service_capability(staff_data, normalized_ids)
        ]

    def _has_single_staff(self, service_id: Optional[str] = None) -> bool:
        return len(self._get_selectable_staff_records(service_id)) == 1

    def _get_single_staff_name(self, service_id: Optional[str] = None) -> Optional[str]:
        staff_records = self._get_selectable_staff_records(service_id)
        if len(staff_records) == 1:
            return staff_records[0][1].get("name", staff_records[0][0])
        return None

    def _find_staff_record_by_name(
        self,
        input_name: str,
        service_id: Optional[str] = None,
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        normalized_input = self._normalize_input_text(input_name).lower()
        if not normalized_input:
            return None

        staff_records = self._get_selectable_staff_records(service_id)

        # まず完全一致
        for staff_key, staff_data in staff_records:
            staff_name = str(staff_data.get("name", staff_key)).strip()
            if normalized_input == staff_name.lower():
                return staff_key, staff_data

        # 次に部分一致
        for staff_key, staff_data in staff_records:
            staff_name = str(staff_data.get("name", staff_key)).strip()
            if normalized_input in staff_name.lower() or staff_name.lower() in normalized_input:
                return staff_key, staff_data


        return None

    def _is_no_preference_staff(self, staff_name: Optional[str]) -> bool:
        if staff_name is None:
            return True
        return str(staff_name).strip() in {"指名なし", "おまかせ", "未指定", "", "free"}

    def _get_staff_display_name(self, staff_name: Optional[str]) -> str:
        if self._is_no_preference_staff(staff_name):
            return "指名なし"
        return str(staff_name)

    def _resolve_final_staff_for_reservation(
        self,
        reservation_data: Dict[str, Any],
        exclude_reservation_id: str = None,
    ) -> Optional[str]:
        staff_name = reservation_data.get("staff")
        if not self._is_no_preference_staff(staff_name):
            return staff_name

        date_str = reservation_data["date"]
        start_time = reservation_data.get("start_time", reservation_data.get("time", ""))
        end_time = reservation_data.get("end_time", "")
        services = reservation_data.get("services") or reservation_data.get("cart") or []
        service_ids = [
            item.get("service_id")
            for item in services
            if isinstance(item, dict) and item.get("service_id")
        ]
        service_id = service_ids[0] if service_ids else reservation_data.get("service_id") or self._get_service_id_by_name(
            reservation_data.get("service")
        )

        if not end_time:
            duration = int(reservation_data.get("total_duration", 0) or 0) or 60
        else:
            duration = self._calculate_time_duration_minutes(start_time, end_time)
            if duration <= 0:
                duration = int(reservation_data.get("total_duration", 0) or 0) or 60

        assigned = self.google_calendar.assign_staff_for_free_reservation(
            date_str=date_str,
            start_time=start_time,
            duration_minutes=duration,
            service_id=service_id,
            service_ids=service_ids,
            exclude_reservation_id=exclude_reservation_id,
        )
        if not assigned:
            return None
        return assigned["staff_name"]

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

        exclude_reservation_id = None
        if original_reservation and original_reservation.get("date") == selected_date:
            exclude_reservation_id = original_reservation.get("reservation_id")

        current_service_id = self._get_current_service_id(user_id) if user_id else None
        current_service_ids = self._get_current_service_ids(user_id) if user_id else []
        service_cache_key = ",".join(sorted(current_service_ids)) if current_service_ids else current_service_id

        runtime_cache = self._ensure_runtime_cache(user_id)
        cache_key = self._make_available_slots_cache_key(
            selected_date=selected_date,
            staff_name=staff_name,
            current_service_id=service_cache_key,
            exclude_reservation_id=exclude_reservation_id,
        )

        if runtime_cache and cache_key in runtime_cache["available_slots"]:
            cached_slots = runtime_cache["available_slots"][cache_key]
            return [dict(slot) for slot in cached_slots]

        if staff_name and not self._is_no_preference_staff(staff_name):
            staff_slots = self.google_calendar.get_available_slots_for_modification(
                date_str=selected_date,
                exclude_reservation_id=exclude_reservation_id,
                staff_name=staff_name,
                service_id=current_service_id,
                service_ids=current_service_ids,
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

            if runtime_cache is not None:
                runtime_cache["available_slots"][cache_key] = [dict(slot) for slot in staff_slots]

            return staff_slots

        start_date = datetime.strptime(selected_date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_date = start_date

        date_slots = self.google_calendar.get_available_slots(
            start_date=start_date,
            end_date=end_date,
            staff_name=None,
            service_id=current_service_id,
            service_ids=current_service_ids,
            exclude_reservation_id=exclude_reservation_id,
        )
        date_slots = [slot for slot in date_slots if slot["date"] == selected_date]

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

        if runtime_cache is not None:
            runtime_cache["available_slots"][cache_key] = [dict(slot) for slot in date_slots]

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
            filtered_periods = self._periods_fittable_for_service(available_periods, service_duration)

            time_options = self._generate_valid_time_options(
                user_id=user_id,
                selected_date=date_str,
                filtered_periods=filtered_periods,
                service_duration=service_duration,
                staff_name=staff_name,
            )
            return bool(time_options)
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
                duration = self._get_cart_total_duration(user_id)

                if duration <= 0:
                    continue

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

        example_date = (datetime.now().date() + timedelta(days=7)).strftime("%Y-%m-%d")

        header = "📅 ご希望の日付をお選びください👇\n\n"
        header += "※土日・午前中は埋まりやすいためお早めのご予約がおすすめです！\n\n"
        header += f"※{limit_days}日以降は「{example_date}」の形式でご入力ください。"

        text = (f"{error_prefix}\n\n" if error_prefix else "") + header
        return self._quick_reply_return(
            text,
            items,
            include_cancel=True,
            include_back=True,
        )

    
    def _build_staff_selection_message(self, user_id: str) -> Dict[str, Any]:
        service_ids = self._get_current_service_ids(user_id)
        service_name = self._format_service_summary(self._get_cart(user_id))

        self.user_states[user_id]["step"] = "staff_selection"

        selectable_staff = self._get_selectable_staff_records_for_cart(service_ids)

        if not selectable_staff:
            return self._build_cart_action_message(
                user_id,
                prefix="選択されたメニュー内容に対応可能なスタッフがいないため、別のメニューをご確認ください。\n\n現在のご予約内容です。",
            )

        staff_items = [{"label": "指名なし", "text": "指名なし"}]
        staff_lines = ["・指名なし"]

        for staff_id, staff_data in selectable_staff:
            staff_name = staff_data.get("name", staff_id)
            staff_items.append({"label": staff_name, "text": staff_name})
            staff_lines.append(f"・{staff_name}")

        text = f"""{service_name}承ります👌

担当スタッフをお選びください👇

{chr(10).join(staff_lines)}"""

        return self._quick_reply_return(text, staff_items, include_cancel=True, include_back=True)

    def _go_back_one_step(self, user_id: str) -> Union[str, Dict[str, Any]]:
        if user_id not in self.user_states:
            return "現在進行中の予約はありません。"

        state = self.user_states[user_id]
        step = state.get("step")
        data = state.get("data", {})

        if step == "service_selection":
            return self._build_cart_action_message(user_id) if self._get_cart(user_id) else self._build_initial_menu_selection_message()

        if step == "service_category_selection":
            state["step"] = "service_selection"
            return self._build_initial_menu_selection_message()

        if step == "service_detail_selection":
            state["step"] = "service_category_selection"
            return self._build_category_selection_message()

        if step == "service_delete_selection":
            return self._build_cart_action_message(user_id)

        if step == "service_cart":
            text = "この画面では戻れません。"
            return self._build_cart_action_message(user_id, prefix=text + "\n\n現在のご予約内容です。")

        if step == "staff_selection":
            data.pop("staff", None)
            self._clear_reservation_selection_after_staff(user_id)
            state["step"] = "service_cart"
            return self._build_cart_action_message(user_id)

        if step == "date_selection":
            self._clear_reservation_selection_after_staff(user_id)
            back_target = state.get("date_selection_back_target", "staff_selection")

            if back_target in {"service_selection", "service_cart"}:
                state["step"] = "service_cart"
                return self._build_cart_action_message(user_id)

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

        service_duration = self._get_cart_total_duration(user_id) or 60
        service_name = self._format_service_summary(self._get_cart(user_id))

        filtered_periods = self._periods_fittable_for_service(available_periods, service_duration)

        if not filtered_periods:
            self.user_states[user_id]["step"] = "date_selection"
            attendance_message = self._get_staff_unavailability_message(staff_name, selected_date)
            err = attendance_message or f"""申し訳ございませんが、{selected_date}は{service_name}（{service_duration}分）の予約可能な時間がありません。

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

        time_options = self._generate_valid_time_options(
            user_id=user_id,
            selected_date=selected_date,
            filtered_periods=filtered_periods,
            service_duration=service_duration,
            staff_name=staff_name,
        )

        if not time_options:
            self.user_states[user_id]["step"] = "date_selection"
            err = (
                f"申し訳ございませんが、{selected_date}は締切時間を過ぎたため、"
                f"現在ご案内可能な時間がありません。\n\n"
                f"他の日付をお選びください。"
            )
            return self._build_date_week_selection_message(
                user_id,
                context="new_reservation",
                error_prefix=err,
            )

        self.user_states[user_id]["time_options"] = time_options
        self.user_states[user_id]["time_slot_page"] = 0
        self.user_states[user_id]["time_selection_date"] = selected_date
        self.user_states[user_id]["time_selection_service_duration"] = service_duration
        self.user_states[user_id]["time_filtered_periods"] = filtered_periods

        text = self._build_time_selection_text(
            selected_date=selected_date,
            service_name=service_name,
            service_duration=service_duration,
            time_options=time_options,
        )
        return self._build_time_selection_quick_reply(user_id, text, page=0)

    def detect_intent(self, message: str, user_id: str = None) -> str:
        message_normalized = self._normalize_input_text(message)

        if user_id and user_id in self.user_states:
            state = self.user_states[user_id]
            step = state.get("step", "")

            if step in [
                "service_selection",
                "service_category_selection",
                "service_detail_selection",
                "service_cart",
                "service_delete_selection",
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

        if self._match_keyword_group(message_normalized, modify_keywords):
            return "modify"
        elif self._match_keyword_group(message_normalized, cancel_keywords):
            return "cancel"
        elif self._match_keyword_group(message_normalized, reservation_keywords):
            return "reservation"
        else:
            return "general"

    def handle_reservation_flow(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        if user_id not in self.user_states:
            self.user_states[user_id] = {"step": "start", "data": {"user_id": user_id}}

        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = self._normalize_input_text(message)

        if self._match_keyword_group(message_normalized, flow_cancel_keywords):
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
        elif step == "service_category_selection":
            return self._handle_service_category_selection(user_id, message)
        elif step == "service_detail_selection":
            return self._handle_service_detail_selection(user_id, message)
        elif step == "service_cart":
            return self._handle_service_cart(user_id, message)
        elif step == "service_delete_selection":
            return self._handle_service_delete_selection(user_id, message)
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
        self.user_states[user_id].setdefault("data", {"user_id": user_id})
        return self._build_initial_menu_selection_message()

    def _build_service_quick_reply_postback_items(self, category_id: Optional[str] = None) -> List[Dict[str, str]]:
        items = []
        if category_id:
            for sid, data in self._get_services_by_category(category_id):
                name = data.get("name", sid)
                items.append({
                    "label": name,
                    "type": "postback",
                    "data": f"action=select_service&service_id={sid}",
                })
            return items

        for featured_set in self._get_featured_sets():
            items.append({
                "label": str(featured_set.get("name")),
                "type": "postback",
                "data": f"action=select_featured_set&set_id={featured_set.get('id')}",
            })
        items.append({
            "label": "メニューを見る",
            "text": "メニューを見る",
        })
        return items

    def start_reservation_with_featured_set(
        self,
        user_id: str,
        featured_set_id: str,
    ) -> Union[str, Dict[str, Any]]:
        if not featured_set_id or not str(featured_set_id).strip():
            return self._build_initial_menu_selection_message()

        existing_state = self.user_states.get(user_id, {})
        existing_data = existing_state.get("data", {})
        existing_data["user_id"] = user_id
        existing_state["step"] = "service_selection"
        existing_state["data"] = existing_data
        self.user_states[user_id] = existing_state

        add_result = self._add_featured_set_to_cart(user_id, featured_set_id)
        if not add_result.get("ok"):
            if add_result.get("reason") == "duplicate":
                text = "すでにセット由来で追加済みのメニューが含まれています。\n別のメニューをお選びください。"
            else:
                text = "もう一度メニューをお選びください。"
            return self._quick_reply_return(
                text,
                self._build_service_quick_reply_postback_items(),
                include_cancel=True,
                include_back=False,
            )

        self._clear_reservation_selection_after_service(user_id)
        featured_set = add_result["set"]
        return self._build_cart_action_message(
            user_id,
            prefix=f"{featured_set.get('name')}を追加しました。\n\n現在のご予約内容です。",
        )

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
        existing_state["step"] = "service_selection"
        existing_state["data"] = existing_data
        self.user_states[user_id] = existing_state

        add_result = self._add_service_to_cart(user_id, service_id)
        if not add_result.get("ok"):
            if add_result.get("reason") == "duplicate":
                text = "すでに追加済みのメニューです。\n別のメニューをお選びください。"
            else:
                text = "もう一度メニューをお選びください。"
            return self._quick_reply_return(
                text,
                self._build_service_quick_reply_postback_items(),
                include_cancel=True,
                include_back=True,
            )

        self._clear_reservation_selection_after_service(user_id)
        added_item = add_result["item"]
        return self._build_cart_action_message(
            user_id,
            prefix=f"{added_item['service_name']}を追加しました。\n\n現在のご予約内容です。",
        )

    def start_reservation_with_staff(self, user_id: str, staff_identifier: str) -> Union[str, Dict[str, Any]]:
        existing_state = self.user_states.get(user_id, {})
        existing_data = existing_state.get("data", {})
        service_ids = self._get_current_service_ids(user_id)

        staff_record = None

        # staff_id で来た場合
        if staff_identifier in self.staff_members:
            candidate = self.staff_members[staff_identifier]
            if isinstance(candidate, dict):
                is_active = candidate.get("is_active", True)
                if is_active and self._has_staff_service_capability(candidate, service_ids):
                    staff_record = (staff_identifier, candidate)

        # 名前で来た場合
        if not staff_record:
            staff_record = self._find_staff_record_by_name(staff_identifier, None)

        if self._is_no_preference_staff(staff_identifier):
            existing_data["user_id"] = user_id
            existing_data["staff"] = "指名なし"
            existing_state["step"] = "service_selection"
            existing_state["data"] = existing_data
            self.user_states[user_id] = existing_state
            return self._start_reservation(user_id)

        if not staff_record:
            return "申し訳ございませんが、選択されたスタッフは現在ご指定いただけません。"

        _, staff_data = staff_record
        if not self._has_staff_service_capability(staff_data, service_ids):
            return "選択されたメニュー内容に対応可能なスタッフではありません。別のスタッフをご選択ください。"

        staff_name = staff_data.get("name")

        existing_data["user_id"] = user_id
        existing_data["staff"] = staff_name

        existing_state["step"] = "service_selection"
        existing_state["data"] = existing_data

        self.user_states[user_id] = existing_state

        return self._start_reservation(user_id)

    def _proceed_after_cart_confirmed(self, user_id: str) -> Union[str, Dict[str, Any]]:
        self._sync_cart_to_reservation_fields(user_id)
        cart = self._get_cart(user_id)
        service_ids = self._get_current_service_ids(user_id)
        service_name = self._format_service_summary(cart)
        preselected_staff = self.user_states[user_id]["data"].get("staff")

        self._clear_reservation_selection_after_service(user_id)

        selectable_staff = self._get_selectable_staff_records_for_cart(service_ids)
        if not selectable_staff:
            self.user_states[user_id]["step"] = "service_cart"
            return self._build_cart_action_message(
                user_id,
                prefix="選択されたメニュー内容に対応可能なスタッフがいないため、別のメニューをご確認ください。\n\n現在のご予約内容です。",
            )

        if preselected_staff:
            staff_record = self._find_staff_record_by_name(preselected_staff, None)
            if staff_record:
                _, staff_data = staff_record
                if self._has_staff_service_capability(staff_data, service_ids):
                    resolved_staff_name = staff_data.get("name")
                    self.user_states[user_id]["data"]["staff"] = resolved_staff_name
                    self.user_states[user_id]["step"] = "date_selection"
                    self.user_states[user_id]["date_selection_back_target"] = "service_cart"
                    staff_display = f"{resolved_staff_name}さん" if resolved_staff_name != "未指定" else resolved_staff_name
                    intro = f"""{service_name}ですね👌
担当者は{staff_display}になります😊
"""
                    self.user_states[user_id]["date_selection_week_start"] = self._calendar_week_monday(
                        datetime.now().date()
                    ).strftime("%Y-%m-%d")
                    reply = self._build_date_week_selection_message(user_id, context="new_reservation")
                    reply["text"] = intro + reply["text"]
                    return reply

            self.user_states[user_id]["data"].pop("staff", None)

        if len(selectable_staff) == 1:
            single_staff_name = selectable_staff[0][1].get("name", selectable_staff[0][0])
            self.user_states[user_id]["data"]["staff"] = single_staff_name
            self.user_states[user_id]["step"] = "date_selection"
            self.user_states[user_id]["date_selection_back_target"] = "service_cart"
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

    def _handle_service_cart(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        raw = self._normalize_input_text(message)

        if raw in ["他のメニューを追加", "メニューを追加"]:
            self.user_states[user_id]["step"] = "service_category_selection"
            return self._build_category_selection_message(prefix="追加するメニューカテゴリをお選びください👇")

        if raw == "メニューを削除":
            cart = self._get_cart(user_id)
            if not cart:
                self.user_states[user_id]["step"] = "service_selection"
                return self._build_initial_menu_selection_message()

            self.user_states[user_id]["step"] = "service_delete_selection"
            text_lines = ["削除するメニューをお選びください👇", ""]
            items = []
            for item in cart:
                text_lines.append(f"・{item.get('service_name', '')}")
                items.append({"label": item.get("service_name", ""), "text": item.get("service_name", "")})
            return self._quick_reply_return(
                "\n".join(text_lines),
                items,
                include_cancel=True,
                include_back=True,
            )

        if raw == "このメニューで確定":
            if not self._get_cart(user_id):
                self.user_states[user_id]["step"] = "service_selection"
                return self._build_initial_menu_selection_message()
            return self._proceed_after_cart_confirmed(user_id)

        return self._build_cart_action_message(user_id)

    def _handle_service_delete_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        raw = self._normalize_input_text(message)
        cart = self._get_cart(user_id)

        target_item = None
        for item in cart:
            if raw == item.get("service_name") or raw == item.get("service_id"):
                target_item = item
                break

        if not target_item:
            text_lines = ["削除するメニューをお選びください👇", ""]
            items = []
            for item in cart:
                text_lines.append(f"・{item.get('service_name', '')}")
                items.append({"label": item.get("service_name", ""), "text": item.get("service_name", "")})
            return self._quick_reply_return(
                "\n".join(text_lines),
                items,
                include_cancel=True,
                include_back=True,
            )

        self._remove_service_from_cart(user_id, target_item["service_id"])

        if not self._get_cart(user_id):
            self.user_states[user_id]["step"] = "service_selection"
            return self._quick_reply_return(
                "選択中のメニューがありません。\nご希望のメニューをお選びください👇",
                self._build_service_quick_reply_postback_items(),
                include_cancel=True,
                include_back=True,
            )

        return self._build_cart_action_message(
            user_id,
            prefix=f"{target_item['service_name']}を削除しました。\n\n現在のご予約内容です。",
        )

    def _handle_service_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        raw = self._normalize_input_text(message)
        if self._match_keyword_group(raw, flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        for featured_set in self._get_featured_sets():
            if raw == str(featured_set.get("name")):
                return self.start_reservation_with_featured_set(user_id, str(featured_set.get("id")))

        if raw in ["メニューを見る", "単品メニューを見る", "単品メニュー"]:
            self.user_states[user_id]["step"] = "service_category_selection"
            return self._build_category_selection_message()

        return self._build_initial_menu_selection_message()

    def _handle_service_category_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        raw = self._normalize_input_text(message)
        category_id = self._resolve_category_id_from_text(raw)
        if not category_id:
            self.user_states[user_id]["step"] = "service_category_selection"
            return self._build_category_selection_message(prefix="カテゴリをお選びください。")
        return self._build_service_selection_message_for_category(user_id, category_id)

    def _handle_service_detail_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        raw = self._normalize_input_text(message)
        if self._match_keyword_group(raw, flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        normalized_input = self._normalize_service_input(raw)
        matches = self._fallback_match_service_by_text(normalized_input)
        category_id = self.user_states[user_id].get("data", {}).get("selected_service_category")
        if category_id:
            category_service_ids = {sid for sid, _data in self._get_services_by_category(category_id)}
            matches = [m for m in matches if m[0] in category_service_ids]

        if not matches:
            return self._build_service_selection_message_for_category(user_id, category_id, prefix="メニューを選択してください。")

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
                include_back=True,
            )

        service_id, _ = matches[0]
        add_result = self._add_service_to_cart(user_id, service_id)
        if not add_result.get("ok"):
            if add_result.get("reason") == "duplicate":
                text = "すでに追加済みのメニューです。\n別のメニューをお選びください。"
            elif add_result.get("reason") == "cannot_combine":
                text = "このメニューは他のメニューと組み合わせできません。\n内容をご確認ください。"
            else:
                text = "メニューを選択してください。"
            return self._build_service_selection_message_for_category(user_id, category_id, prefix=text)

        self._clear_reservation_selection_after_service(user_id)
        self.user_states[user_id].setdefault("data", {}).pop("selected_menu_label", None)
        self.user_states[user_id].setdefault("data", {}).pop("featured_set_id", None)
        added_item = add_result["item"]
        return self._build_cart_action_message(
            user_id,
            prefix=f"{added_item['service_name']}を追加しました。\n\n現在のご予約内容です。",
        )

    def _handle_staff_selection(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = self._normalize_input_text(message)
        if self._match_keyword_group(message_normalized, flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        if self._is_no_preference_staff(message_normalized):
            self.user_states[user_id]["data"]["staff"] = "指名なし"
            self.user_states[user_id]["data"]["selected_staff"] = "free"
            self.user_states[user_id]["data"].pop("assigned_staff", None)
            self.user_states[user_id]["step"] = "date_selection"
            self.user_states[user_id]["date_selection_back_target"] = "staff_selection"
            intro = "担当者：指名なしですね。ご予約確定時に自動で最適なスタッフを決定します。\n\n"
            self.user_states[user_id]["date_selection_week_start"] = self._calendar_week_monday(
                datetime.now().date()
            ).strftime("%Y-%m-%d")
            reply = self._build_date_week_selection_message(user_id, context="new_reservation")
            reply["text"] = intro + reply["text"]
            return reply

        service_ids = self._get_current_service_ids(user_id)
        staff_record = self._find_staff_record_by_name(message_normalized, None)

        if staff_record and not self._has_staff_service_capability(staff_record[1], service_ids):
            staff_record = None

        if not staff_record:
            selectable_staff = self._get_selectable_staff_records_for_cart(service_ids)
            staff_items = [{"label": "指名なし", "text": "指名なし"}] + [
                {"label": s.get("name", sid), "text": s.get("name", sid)}
                for sid, s in selectable_staff
            ]
            staff_lines = ["・指名なし"] + [f"・{s.get('name', sid)}" for sid, s in selectable_staff]
            text = "申し訳ございませんが、そのスタッフは選択できません。下記からお選びください。\n\n" + "\n".join(staff_lines)
            return self._quick_reply_return(text, staff_items, include_cancel=True, include_back=True)

        _, staff_data = staff_record
        selected_staff = staff_data.get("name")

        self.user_states[user_id]["data"]["staff"] = selected_staff
        self.user_states[user_id]["data"]["selected_staff"] = selected_staff
        self.user_states[user_id]["data"]["assigned_staff"] = selected_staff
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
        message_normalized = self._normalize_input_text(message)
        if self._match_keyword_group(message_normalized, flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

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

    def _get_named_staff_attendance_result(self, staff_name: Optional[str], selected_date: str) -> Dict[str, Any]:
        try:
            if self._is_no_preference_staff(staff_name):
                return {"is_working": True, "source": "default", "periods": []}
            target_date = datetime.strptime(selected_date, "%Y-%m-%d").date()
            for _, staff_data in self.staff_members.items():
                if isinstance(staff_data, dict) and staff_data.get("name") == staff_name:
                    return get_staff_attendance_for_date(staff_data, target_date, fallback_to_store_hours=True)
        except Exception as e:
            logging.error(f"Failed to get named staff attendance result: {e}")
        return {"is_working": False, "source": "weekly", "periods": []}

    def _get_staff_unavailability_message(self, staff_name: Optional[str], selected_date: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> Optional[str]:
        if self._is_no_preference_staff(staff_name):
            return None

        attendance = self._get_named_staff_attendance_result(staff_name, selected_date)
        if not attendance.get("is_working"):
            return "ご希望の日程は担当スタッフの勤務対象外となっております。\n別の日程をご確認ください。"

        if start_time and end_time:
            reason = self.google_calendar.check_staff_availability_reason(
                date_str=selected_date,
                start_time=start_time,
                end_time=end_time,
                staff_name=staff_name,
                exclude_reservation_id=self._get_exclude_reservation_id_for_date(None, selected_date),
            )
            if reason == "outside":
                return "ご希望の日時は担当スタッフの受付時間外のため、お選びいただけません。\n別のお時間をお選びください。"
        return None

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
        message_normalized = self._normalize_input_text(message)
        if self._match_keyword_group(message_normalized, flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        if message_normalized in ("前へ", "次へ"):
            selected_date = self.user_states[user_id].get(
                "time_selection_date",
                self.user_states[user_id]["data"]["date"],
            )
            service_name = self._format_service_summary(self._get_cart(user_id))
            service_duration = self._get_cart_total_duration(user_id) or self.user_states[user_id].get("time_selection_service_duration", 60)

            staff_name = self.user_states[user_id]["data"].get("staff")
            available_slots = self._get_available_slots(selected_date, staff_name, user_id)
            available_periods = [slot for slot in available_slots if slot["available"]]
            filtered_periods = self._periods_fittable_for_service(available_periods, service_duration)

            time_options = self._generate_valid_time_options(
                user_id=user_id,
                selected_date=selected_date,
                filtered_periods=filtered_periods,
                service_duration=service_duration,
                staff_name=staff_name,
            )

            if not time_options:
                self.user_states[user_id]["step"] = "date_selection"
                err = (
                    f"申し訳ございませんが、{selected_date}は締切時間を過ぎたため、"
                    f"現在ご案内可能な時間がありません。\n\n"
                    f"他の日付をお選びください。"
                )
                return self._build_date_week_selection_message(
                    user_id,
                    context="new_reservation",
                    error_prefix=err,
                )

            self.user_states[user_id]["time_options"] = time_options
            self.user_states[user_id]["time_filtered_periods"] = filtered_periods

            current_page = self.user_states[user_id].get("time_slot_page", 0)
            per_page = 8
            total_pages = max(1, (len(time_options) + per_page - 1) // per_page)

            if message_normalized == "前へ":
                new_page = max(0, current_page - 1)
            else:
                new_page = min(total_pages - 1, current_page + 1)

            text = self._build_time_selection_text(
                selected_date=selected_date,
                service_name=service_name,
                service_duration=service_duration,
                time_options=time_options,
            )
            return self._build_time_selection_quick_reply(user_id, text, new_page)

        selected_date = self.user_states[user_id]["data"]["date"]
        staff_name = self.user_states[user_id]["data"].get("staff")

        try:
            available_slots = self._get_available_slots(selected_date, staff_name, user_id)
            available_periods = [slot for slot in available_slots if slot["available"]]

            service_duration = self._get_cart_total_duration(user_id) or 60

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

❌ 入力をやめる場合は「やめる」とお送りください"""

        is_valid_time, time_error_message = self._check_advance_booking_time(
            selected_date,
            start_time,
            user_id,
        )
        if not is_valid_time:
            return time_error_message

        required_duration = self._get_cart_total_duration(user_id) or 60

        end_time = self._calculate_optimal_end_time(start_time, required_duration)

        is_valid_range = False
        for period in available_periods:
            period_start = period["time"]
            period_end = period["end_time"]
            if period_start <= start_time and end_time <= period_end:
                is_valid_range = True
                break

        if not is_valid_range:
            attendance_message = self._get_staff_unavailability_message(staff_name, selected_date, start_time, end_time)
            if attendance_message:
                return attendance_message

            period_strings = [f"・{period['time']}~{period['end_time']}" for period in available_periods]
            return f"""申し訳ございませんが、{start_time}から{required_duration}分の予約は空いていません。

{selected_date}の予約可能な時間帯：
{chr(10).join(period_strings)}

上記の空き時間からお選びください。

❌ 入力をやめる場合は「やめる」とお送りください"""

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

❌ 入力をやめる場合は「やめる」とお送りください"""

        self.user_states[user_id]["data"]["start_time"] = start_time
        self.user_states[user_id]["data"]["end_time"] = end_time
        self.user_states[user_id]["data"]["time"] = start_time
        self.user_states[user_id]["step"] = "confirmation"
        self._sync_cart_to_reservation_fields(user_id)

        service = self._format_service_summary(self._get_cart(user_id))
        staff = self.user_states[user_id]["data"].get("staff")
        staff_display = "指名なし（担当は自動で決定）" if self._is_no_preference_staff(staff) else staff
        price_val = self._get_cart_total_price(user_id)

        text = f"""ご予約内容の確認です😊

日時：{selected_date} {start_time}~{end_time}
メニュー：{service}
担当：{staff_display}
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
            staff_name = reservation_data.get("staff")
            user_id = reservation_data.get("user_id", "")
            services = reservation_data.get("services") or reservation_data.get("cart") or []
            service_ids = [
                item.get("service_id")
                for item in services
                if isinstance(item, dict) and item.get("service_id")
            ]
            total_duration = int(reservation_data.get("total_duration", 0) or 0)

            if not end_time:
                duration = total_duration or 60
                start_dt = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
                end_dt = start_dt + timedelta(minutes=duration)
                end_time = end_dt.strftime("%H:%M")
            else:
                start_dt = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
                end_dt = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
                duration = int((end_dt - start_dt).total_seconds() // 60)

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

            resolved_staff = staff_name
            assignment_detail = None

            if self._is_no_preference_staff(staff_name):
                assignment_detail = self.google_calendar.assign_staff_for_free_reservation(
                    date_str=date_str,
                    start_time=start_time,
                    duration_minutes=duration,
                    service_ids=service_ids,
                    exclude_reservation_id=exclude_reservation_id,
                )
                if not assignment_detail:
                    return {
                        "available": False,
                        "message": "ご希望の日時ではご案内可能なスタッフがいないため、別のお時間をご確認ください。",
                    }
                resolved_staff = assignment_detail["staff_name"]

            if not self._is_no_preference_staff(resolved_staff):
                staff_record = self._find_staff_record_by_name(resolved_staff, None)
                if not staff_record or not self._has_staff_service_capability(staff_record[1], service_ids):
                    return {
                        "available": False,
                        "message": "選択されたメニュー内容に対応可能なスタッフがいないため、別のメニューをご確認ください。",
                    }

            staff_available = self.google_calendar.check_staff_availability_for_time(
                date_str,
                start_time,
                end_time,
                resolved_staff,
                exclude_reservation_id,
            )

            if not staff_available:
                if self._is_no_preference_staff(staff_name):
                    reassigned = self.google_calendar.assign_staff_for_free_reservation(
                        date_str=date_str,
                        start_time=start_time,
                        duration_minutes=duration,
                        service_ids=service_ids,
                        exclude_reservation_id=exclude_reservation_id,
                    )
                    if not reassigned:
                        return {
                            "available": False,
                            "message": "ご希望の日時ではご案内可能なスタッフがいないため、別のお時間をご確認ください。",
                        }
                    resolved_staff = reassigned["staff_name"]
                    assignment_detail = reassigned
                    staff_available = self.google_calendar.check_staff_availability_for_time(
                        date_str,
                        start_time,
                        end_time,
                        resolved_staff,
                        exclude_reservation_id,
                    )

                if not staff_available:
                    reason = self.google_calendar.check_staff_availability_reason(
                        date_str,
                        start_time,
                        end_time,
                        resolved_staff,
                        exclude_reservation_id,
                    )
                    if reason == "off":
                        message = "ご希望の日程は担当スタッフの勤務対象外となっております。\n別の日程をご確認ください。"
                    elif reason == "outside":
                        message = "ご希望の日時は担当スタッフの受付時間外のため、お選びいただけません。\n別のお時間をお選びください。"
                    else:
                        message = f"👨‍💼 {resolved_staff}さんの{start_time}~{end_time}の時間帯は既に予約が入っております。"
                    return {
                        "available": False,
                        "message": message,
                    }

            user_conflict = self.google_calendar.check_user_time_conflict(
                date_str,
                start_time,
                end_time,
                user_id,
                exclude_reservation_id,
                resolved_staff,
            )

            if user_conflict:
                return {
                    "available": False,
                    "message": "⚠️ 同じ時間帯に他のご予約がございます。",
                }

            return {
                "available": True,
                "message": "",
                "resolved_staff": resolved_staff,
                "assignment_detail": assignment_detail,
            }

        except Exception as e:
            logging.error(f"Error checking final availability: {e}", exc_info=True)
            return {
                "available": False,
                "message": "空き状況の最終確認中にエラーが発生しました。もう一度お試しください。"
            }

    def _execute_new_reservation(self, user_id: str) -> str:
        self._sync_cart_to_reservation_fields(user_id)
        reservation_data = self.user_states[user_id]["data"].copy()

        if "staff" not in reservation_data:
            logging.error(
                f"[_execute_new_reservation] ERROR: Staff not found in reservation_data! Data: {reservation_data}"
            )
            return "申し訳ございませんがエラーが発生しました。「やめる」とお送りして、もう一度最初からやり直してください。"

        services = reservation_data.get("services") or reservation_data.get("cart") or []
        if not services:
            return "メニューが選択されていません。ご希望のメニューをお選びください👇"

        reservation_data["services"] = [dict(item) for item in services]
        reservation_data["service"] = self._format_service_summary(reservation_data["services"])
        reservation_data["total_duration"] = int(reservation_data.get("total_duration", 0) or 0) or sum(
            int(item.get("duration", 0) or 0) for item in reservation_data["services"]
        )
        reservation_data["total_price"] = int(reservation_data.get("total_price", 0) or 0) or sum(
            int(item.get("price", 0) or 0) for item in reservation_data["services"]
        )
        reservation_data["service_id"] = reservation_data["services"][0].get("service_id") if reservation_data["services"] else None

        availability_check = self._check_final_availability(reservation_data)
        if not availability_check["available"]:
            del self.user_states[user_id]
            return f"""❌ 申し訳ございませんが、選択された時間帯は既に他のお客様にご予約いただいておりました。

{availability_check["message"]}

別の時間帯でご予約いただけますでしょうか？
「予約したい」とお送りください。"""

        resolved_staff = availability_check.get("resolved_staff")
        if resolved_staff:
            reservation_data["assigned_staff"] = resolved_staff
            reservation_data["staff"] = resolved_staff

        if self._is_no_preference_staff(reservation_data.get("selected_staff")) or reservation_data.get("selected_staff") == "free":
            reservation_data["selected_staff"] = "free"
        else:
            reservation_data["selected_staff"] = reservation_data.get("selected_staff") or reservation_data["staff"]

        reservation_id = self.google_calendar.generate_reservation_id(reservation_data["date"])
        reservation_data["reservation_id"] = reservation_id

        client_name = self._get_line_display_name(user_id)

        calendar_success = self.google_calendar.create_reservation_event(
            reservation_data,
            client_name,
        )
        if not calendar_success:
            return "申し訳ございません。予約登録中にエラーが発生しました。時間をおいてもう一度お試しください。"

        try:
            sheets_logger = self.sheets_logger

            sheet_reservation_data = {
                "reservation_id": reservation_id,
                "user_id": user_id,
                "client_name": client_name,
                "date": reservation_data["date"],
                "start_time": reservation_data.get("start_time", reservation_data.get("time", "")),
                "end_time": reservation_data.get("end_time", ""),
                "service": reservation_data["service"],
                "services": reservation_data["services"],
                "selected_menu_label": reservation_data.get("selected_menu_label", ""),
                "selected_staff": reservation_data.get("selected_staff", ""),
                "assigned_staff": reservation_data.get("assigned_staff") or reservation_data["staff"],
                "staff": reservation_data.get("assigned_staff") or reservation_data["staff"],
                "duration": reservation_data["total_duration"],
                "price": reservation_data["total_price"],
            }

            sheets_success = sheets_logger.save_reservation(sheet_reservation_data)
            if not sheets_success:
                logging.warning("Failed to save reservation to sheets, but calendar creation succeeded")
        except Exception as e:
            logging.error(f"Failed to save reservation to sheets: {e}", exc_info=True)

        try:
            from api.notification_manager import send_reservation_confirmation_notification
            send_reservation_confirmation_notification(reservation_data, client_name)
        except Exception as e:
            logging.error(f"Failed to send reservation notification: {e}", exc_info=True)

        assigned_staff = reservation_data.get("assigned_staff") or reservation_data["staff"]

        if user_id in self.user_states:
            del self.user_states[user_id]

        return f"""ご予約が確定しました😊

日時：{reservation_data['date']} {reservation_data.get('start_time', reservation_data.get('time', ''))}~{reservation_data.get('end_time', '')}
メニュー：{reservation_data['service']}
担当スタッフ：{assigned_staff}

ご来店を心よりお待ちしております。"""

    def _execute_reservation_modification(self, user_id: str) -> str:
        try:
            self._sync_cart_to_reservation_fields(user_id)
            state = self.user_states.get(user_id, {})
            new_data = dict(state.get("data", {}))
            original_reservation = state.get("original_reservation")

            if not original_reservation:
                logging.error("[_execute_reservation_modification] original_reservation not found")
                return "予約変更元の情報が見つかりません。もう一度お試しください。"

            original_reservation_id = original_reservation.get("reservation_id")
            if not original_reservation_id:
                logging.error("[_execute_reservation_modification] original reservation_id not found")
                return "元の予約IDが見つかりません。もう一度お試しください。"

            if "staff" not in new_data:
                logging.error(
                    f"[_execute_reservation_modification] ERROR: Staff not found in new_data! Data: {new_data}"
                )
                return "申し訳ございませんがエラーが発生しました。「やめる」とお送りして、もう一度最初からやり直してください。"

            services = new_data.get("services") or new_data.get("cart") or []
            if not services:
                return "メニューが選択されていません。ご希望のメニューをお選びください👇"

            new_data["services"] = [dict(item) for item in services]
            new_data["service"] = self._format_service_summary(new_data["services"])
            new_data["total_duration"] = int(new_data.get("total_duration", 0) or 0) or sum(
                int(item.get("duration", 0) or 0) for item in new_data["services"]
            )
            new_data["total_price"] = int(new_data.get("total_price", 0) or 0) or sum(
                int(item.get("price", 0) or 0) for item in new_data["services"]
            )
            new_data["service_id"] = new_data["services"][0].get("service_id") if new_data["services"] else None
            new_data["reservation_id"] = original_reservation_id
            new_data["user_id"] = user_id

            availability_check = self._check_final_availability(new_data)
            if not availability_check["available"]:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return f"""❌ 申し訳ございませんが、選択された時間帯は既に他のお客様にご予約いただいておりました。

{availability_check["message"]}

別の時間帯で予約変更をお願いいたします。"""

            resolved_staff = availability_check.get("resolved_staff")
            if resolved_staff:
                new_data["assigned_staff"] = resolved_staff
                new_data["staff"] = resolved_staff

            if self._is_no_preference_staff(new_data.get("selected_staff")) or new_data.get("selected_staff") == "free":
                new_data["selected_staff"] = "free"
            else:
                new_data["selected_staff"] = new_data.get("selected_staff") or new_data["staff"]

            client_name = self._get_line_display_name(user_id)

            old_staff_name = original_reservation.get("staff")
            cancel_success = self.google_calendar.cancel_reservation_by_id(original_reservation_id, old_staff_name)
            if not cancel_success:
                logging.error(
                    f"[_execute_reservation_modification] Failed to cancel original reservation in calendar: {original_reservation_id}"
                )
                return "申し訳ございません。元の予約の更新処理に失敗しました。時間をおいてもう一度お試しください。"

            calendar_success = self.google_calendar.create_reservation_event(
                new_data,
                client_name,
            )
            if not calendar_success:
                return "申し訳ございません。予約変更中にエラーが発生しました。時間をおいてもう一度お試しください。"

            try:
                field_updates = {
                    "Date": new_data["date"],
                    "Start Time": new_data.get("start_time", new_data.get("time", "")),
                    "End Time": new_data.get("end_time", ""),
                    "Service": new_data["service"],
                    "Services JSON": new_data["services"],
                    "selected_menu_label": new_data.get("selected_menu_label", ""),
                    "Selected Staff": new_data.get("selected_staff", ""),
                    "Assigned Staff": new_data.get("assigned_staff") or new_data["staff"],
                    "Staff": new_data.get("assigned_staff") or new_data["staff"],
                    "Duration (min)": new_data["total_duration"],
                    "Price": new_data["total_price"],
                    "Status": "Modified",
                }
                self.sheets_logger.update_reservation_data(original_reservation_id, field_updates)
            except Exception as e:
                logging.error(f"Failed to update reservation data in sheets: {e}", exc_info=True)

            try:
                from api.notification_manager import send_reservation_modification_notification
                send_reservation_modification_notification(
                    original_reservation,
                    {
                        "reservation_id": original_reservation_id,
                        "date": new_data["date"],
                        "start_time": new_data.get("start_time", new_data.get("time", "")),
                        "end_time": new_data.get("end_time", ""),
                        "service": new_data["service"],
                        "staff": new_data.get("assigned_staff") or new_data["staff"],
                    },
                    client_name,
                )
            except Exception as e:
                logging.error(f"Failed to send reservation modification notification: {e}", exc_info=True)

            assigned_staff = new_data.get("assigned_staff") or new_data["staff"]

            if user_id in self.user_states:
                del self.user_states[user_id]

            return f"""予約変更が完了しました😊

日時：{new_data['date']} {new_data.get('start_time', new_data.get('time', ''))}~{new_data.get('end_time', '')}
メニュー：{new_data['service']}
担当スタッフ：{assigned_staff}

ご来店を心よりお待ちしております。"""

        except Exception as e:
            logging.error(f"Reservation modification execution failed: {e}", exc_info=True)
            return "申し訳ございません。予約変更中にエラーが発生しました。もう一度お試しください。"

    def _handle_confirmation(self, user_id: str, message: str) -> str:
        yes_keywords = self.confirmation_keywords.get("yes", [])
        no_keywords = self.confirmation_keywords.get("no", [])
        message_normalized = self._normalize_input_text(message)

        if self._match_keyword_group(message_normalized, yes_keywords):
            is_modification = self.user_states.get(user_id, {}).get("is_modification", False)
            if is_modification:
                return self._execute_reservation_modification(user_id)
            return self._execute_new_reservation(user_id)
        elif self._match_keyword_group(message_normalized, no_keywords):
            if user_id in self.user_states:
                del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"

        return "「確定」とお送りください。"

    def _handle_modify_request(self, user_id: str, message: str) -> Union[str, Dict[str, Any]]:
        state = self.user_states.get(user_id)

        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = self._normalize_input_text(message)
        if self._match_keyword_group(message_normalized, flow_cancel_keywords):
            if user_id in self.user_states:
                del self.user_states[user_id]
            return "予約変更をキャンセルいたします。またのご利用をお待ちしております。"

        if state and state.get("step") == "modify_select_reservation":
            return self._handle_modify_reservation_selection(user_id, message)

        return self._show_user_reservations_for_modification(user_id)

    def _show_user_reservations_for_modification(self, user_id: str) -> Union[str, Dict[str, Any]]:
        try:
            import pytz

            sheets_logger = self.sheets_logger
            reservations = sheets_logger.get_user_reservations_by_user_id(user_id)

            if not reservations:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "申し訳ございませんが、あなたの予約が見つかりませんでした。\n新しくご予約される場合は「予約したい」とお送りください。"

            tokyo_tz = pytz.timezone("Asia/Tokyo")
            current_time = datetime.now(tokyo_tz)
            future_reservations = []

            for res in reservations:
                try:
                    if res.get("status") == "Cancelled":
                        continue

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
                        f"変更をやめる場合は「やめる」とお送りください。"
                    )
            else:
                return (
                    f"申し訳ございませんが、正しい形式で入力してください。\n"
                    f"番号（1-{len(reservations)}）または予約ID（RES-YYYYMMDD-XXXX）を入力してください。\n\n"
                    f"変更をやめる場合は「やめる」とお送りください。"
                )

            if not selected_reservation:
                return self._quick_reply_return(
                    "申し訳ございませんが、その予約IDが見つからないか、あなたの予約ではありません。\n"
                    "正しい予約IDまたは番号を入力してください。\n\n"
                    "変更をやめる場合は「やめる」とお送りください。",
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
・カット＋カラー

【その他】
・カット
・カラー
・パーマ
・トリートメント

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
                "変更をやめる場合は「やめる」とお送りください。"
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
            message_normalized = self._normalize_input_text(message)
            if self._match_keyword_group(message_normalized, flow_cancel_keywords):
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
            import pytz

            sheets_logger = self.sheets_logger
            reservations = sheets_logger.get_user_reservations_by_user_id(user_id)

            if not reservations:
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "申し訳ございませんが、あなたの予約が見つかりませんでした。\n新しくご予約される場合は「予約したい」とお送りください。"

            tokyo_tz = pytz.timezone("Asia/Tokyo")
            current_time = datetime.now(tokyo_tz)
            future_reservations = []

            for res in reservations:
                try:
                    if res.get("status") == "Cancelled":
                        continue

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
        message_normalized = self._normalize_input_text(message)

        if self._match_keyword_group(message_normalized, yes_keywords):
            return self._execute_reservation_cancellation(user_id, reservation)
        elif self._match_keyword_group(message_normalized, no_keywords):
            del self.user_states[user_id]
            return "予約取り消しをキャンセルいたします。予約はそのまま残ります。\nまたのご利用をお待ちしております。"
        else:
            return "「はい」または「確定」でキャンセルを確定するか、「やめる」で中止してください。"

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
            sheets_logger = self.sheets_logger

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
            sheets_logger = self.sheets_logger
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
    print("  • 予約したい - Start reservation")
    print("  • カット, カラー, パーマ, トリートメント - Select service")
    print("  • 田中, 佐藤, 山田 - Select staff")
    print("  • 2025-01-15 (or any date) - Select date")
    print("  • 10:00, 10:30, 10時, 10時30分 - Select start time")
    print("  • はい, 確定 - Confirm reservation")
    print("  • いいえ, やめます - Decline confirmation")
    print("  • ← 戻る, 戻る - Go back one step in supported reservation flows")
    print()
    print("🔄 NAVIGATION COMMANDS:")
    print("  • やめる, 中止 - Cancel current flow")
    print()
    print("📋 RESERVATION MANAGEMENT:")
    print("  • 予約取り消ししたい - Cancel existing reservation")
    print("  • 予約変更したい - Modify existing reservation (re-reservation flow)")
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
