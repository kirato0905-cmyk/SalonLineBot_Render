import os
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional


def _config_path() -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "data", "config.json")


def _load_config() -> Dict[str, Any]:
    path = _config_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.warning(f"config.json not found: {path}")
        return {}
    except Exception as e:
        logging.error(f"Failed to load config.json: {e}")
        return {}


def _calendar_config() -> Dict[str, Any]:
    return _load_config().get("calendar", {})


def _booking_config() -> Dict[str, Any]:
    return _load_config().get("booking", {})


def _salon_config() -> Dict[str, Any]:
    return _load_config().get("salon", {})


def get_timezone() -> str:
    return _salon_config().get("timezone", "Asia/Tokyo")


def get_slot_minutes(default: int = 30) -> int:
    booking = _booking_config()
    value = booking.get("slot_minutes", default)

    try:
        if isinstance(value, bool):
            return default
        if isinstance(value, str):
            value = int(value.strip())
        if not isinstance(value, (int, float)):
            return default
        value = int(value)
        if value <= 0:
            return default
        return value
    except Exception:
        return default


def get_reservation_ui_limit_days(default: int = 45) -> int:
    booking = _booking_config()
    value = booking.get("reservation_ui_limit_days", default)

    try:
        if isinstance(value, bool):
            return default
        if isinstance(value, str):
            value = int(value.strip())
        if not isinstance(value, (int, float)):
            return default
        value = int(value)
        if value < 0:
            return default
        return value
    except Exception:
        return default


def _weekday_key(d: date) -> str:
    keys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    return keys[d.weekday()]


def _normalize_hours(hours: Any) -> List[Dict[str, str]]:
    if not isinstance(hours, list):
        return []

    normalized: List[Dict[str, str]] = []
    for item in hours:
        if not isinstance(item, dict):
            continue
        start = item.get("start")
        end = item.get("end")
        if not start or not end:
            continue
        normalized.append({"start": str(start), "end": str(end)})
    return normalized


def _is_nth_weekday_of_month(target_date: date, weekday: str, weeks: List[int]) -> bool:
    weekday_map = {
        "mon": 0,
        "tue": 1,
        "wed": 2,
        "thu": 3,
        "fri": 4,
        "sat": 5,
        "sun": 6,
    }

    if weekday not in weekday_map:
        return False
    if target_date.weekday() != weekday_map[weekday]:
        return False

    count = 0
    d = target_date.replace(day=1)
    while d <= target_date:
        if d.weekday() == weekday_map[weekday]:
            count += 1
        d += timedelta(days=1)

    return count in weeks


def is_closed_date(target_date: date) -> bool:
    calendar_cfg = _calendar_config()

    closed_dates = calendar_cfg.get("closed_dates", [])
    if target_date.strftime("%Y-%m-%d") in closed_dates:
        return True

    monthly_closed = calendar_cfg.get("monthly_closed", [])
    for rule in monthly_closed:
        if not isinstance(rule, dict):
            continue
        weekday = rule.get("weekday")
        weeks = rule.get("weeks", [])
        if isinstance(weeks, list) and _is_nth_weekday_of_month(target_date, weekday, weeks):
            return True

    business_hours = calendar_cfg.get("business_hours", {})
    weekday_hours = business_hours.get(_weekday_key(target_date), [])
    if not _normalize_hours(weekday_hours):
        return True

    return False


def is_open_date(target_date: date) -> bool:
    return bool(get_hours_for_date(target_date))


def get_hours_for_date(target_date: date) -> List[Dict[str, str]]:
    calendar_cfg = _calendar_config()

    if not isinstance(target_date, date):
        raise TypeError("target_date must be datetime.date")

    target_date_str = target_date.strftime("%Y-%m-%d")

    closed_dates = calendar_cfg.get("closed_dates", [])
    if target_date_str in closed_dates:
        return []

    monthly_closed = calendar_cfg.get("monthly_closed", [])
    for rule in monthly_closed:
        if not isinstance(rule, dict):
            continue
        weekday = rule.get("weekday")
        weeks = rule.get("weeks", [])
        if isinstance(weeks, list) and _is_nth_weekday_of_month(target_date, weekday, weeks):
            return []

    special_hours = calendar_cfg.get("special_hours", [])
    for item in special_hours:
        if not isinstance(item, dict):
            continue
        if item.get("date") == target_date_str:
            return _normalize_hours(item.get("hours", []))

    business_hours = calendar_cfg.get("business_hours", {})
    return _normalize_hours(business_hours.get(_weekday_key(target_date), []))
