"""
営業関連設定の読み込みと営業可否判定（settings.json 準拠）。
予約作成・予約変更・空き枠算出で同一ロジックを参照する。
"""
import os
import json
from datetime import date, datetime
from typing import Dict, Any, List, Optional

# weekday key in settings (mon..sun) -> Python weekday (0=Monday, 6=Sunday)
WEEKDAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

_settings: Optional[Dict[str, Any]] = None


def _settings_path() -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "data", "settings.json")


def load_settings(reload: bool = False) -> Dict[str, Any]:
    """Load settings from settings.json. Cached unless reload=True."""
    global _settings
    if _settings is not None and not reload:
        return _settings
    path = _settings_path()
    if not os.path.isfile(path):
        _settings = _default_settings()
        return _settings
    try:
        with open(path, "r", encoding="utf-8") as f:
            _settings = json.load(f)
    except Exception:
        _settings = _default_settings()
    return _settings


def _default_settings() -> Dict[str, Any]:
    return {
        "timezone": "Asia/Tokyo",
        "business_hours": {
            "mon": [{"start": "10:00", "end": "13:00"}, {"start": "14:00", "end": "20:00"}],
            "tue": [],
            "wed": [{"start": "10:00", "end": "13:00"}, {"start": "14:00", "end": "20:00"}],
            "thu": [{"start": "10:00", "end": "13:00"}, {"start": "14:00", "end": "20:00"}],
            "fri": [{"start": "10:00", "end": "13:00"}, {"start": "14:00", "end": "20:00"}],
            "sat": [{"start": "10:00", "end": "20:00"}],
            "sun": [],
        },
        "monthly_closed": [],
        "closed_dates": [],
        "special_hours": [],
        "booking_rules": {"slot_minutes": 30},
    }


def get_timezone() -> str:
    return load_settings().get("timezone", "Asia/Tokyo")


def get_slot_minutes() -> int:
    return int(load_settings().get("booking_rules", {}).get("slot_minutes", 30))


def _parse_date(d: str) -> date:
    """Parse YYYY-MM-DD to date."""
    return datetime.strptime(d, "%Y-%m-%d").date()


def _weekday_key(d: date) -> str:
    return WEEKDAY_KEYS[d.weekday()]


def _nth_weekday_of_month(d: date) -> int:
    """Calendar occurrence of this weekday in the month (1-based). E.g. first Wed=1, second Wed=2."""
    day = d.day
    n = 1
    while day > 7:
        day -= 7
        n += 1
    return n


def is_closed_date(d: date) -> bool:
    """
    営業可否の優先順位に従い、その日が終日休業かどうか判定する。
    True = 終日休業（予約不可）。
    """
    s = load_settings()
    date_str = d.strftime("%Y-%m-%d")

    # 1. closed_dates に含まれる → 終日休業
    closed_dates = s.get("closed_dates") or []
    if date_str in closed_dates:
        return True

    # 2. special_hours に該当 → 営業時間は上書き（休業ではない）。ここでは False
    special = s.get("special_hours") or []
    for sh in special:
        if sh.get("date") == date_str:
            hours = sh.get("hours") or []
            if not hours:
                break
            return False

    # 3. monthly_closed に該当 → 終日休業
    monthly = s.get("monthly_closed") or []
    wk = _weekday_key(d)
    nth = _nth_weekday_of_month(d)
    for mc in monthly:
        if mc.get("weekday") == wk and nth in (mc.get("weeks") or []):
            return True

    # 4. business_hours[weekday] が空配列 → 終日休業
    bh = s.get("business_hours") or {}
    slots = bh.get(wk, [])
    if not slots:
        return True

    # 5. 上記以外 → 営業日
    return False


def is_open_date(d: date) -> bool:
    """その日が営業日か（予約可能日か）。"""
    return not is_closed_date(d)


def get_hours_for_date(d: date) -> List[Dict[str, str]]:
    """
    指定日の営業時間スロットを返す。終日休業の場合は []。
    優先順位: closed_dates → 休業; special_hours → その hours; monthly_closed → 休業;
    business_hours[weekday] 空 → 休業; それ以外 → business_hours[weekday]。
    各スロットは {"start": "HH:MM", "end": "HH:MM"}。
    """
    s = load_settings()
    date_str = d.strftime("%Y-%m-%d")

    # 1. closed_dates → 終日休業
    if date_str in (s.get("closed_dates") or []):
        return []

    # 2. special_hours に該当 → その日の営業時間を上書き
    for sh in (s.get("special_hours") or []):
        if sh.get("date") == date_str:
            hours = sh.get("hours") or []
            if hours:
                return [_normalize_slot(slot) for slot in hours]
            break

    # 3. monthly_closed に該当 → 終日休業
    wk = _weekday_key(d)
    nth = _nth_weekday_of_month(d)
    for mc in (s.get("monthly_closed") or []):
        if mc.get("weekday") == wk and nth in (mc.get("weeks") or []):
            return []

    # 4. business_hours[weekday] が空 → 終日休業
    slots = (s.get("business_hours") or {}).get(wk, [])
    if not slots:
        return []

    # 5. business_hours[weekday] を採用
    return [_normalize_slot(slot) for slot in slots]


def _normalize_slot(slot: Dict) -> Dict[str, str]:
    """Ensure start/end are "HH:MM" strings."""
    return {
        "start": str(slot.get("start", "00:00"))[:5],
        "end": str(slot.get("end", "00:00"))[:5],
    }


def get_max_end_time_for_date(d: date) -> Optional[str]:
    """
    指定日の営業終了時刻（最も遅い end）を返す。休業日なら None。
    予約変更時の「営業時間外」チェックに使用。
    """
    hours = get_hours_for_date(d)
    if not hours:
        return None
    return max(slot["end"] for slot in hours)


def get_min_start_time_for_date(d: date) -> Optional[str]:
    """指定日の営業開始時刻（最も早い start）を返す。休業日なら None。"""
    hours = get_hours_for_date(d)
    if not hours:
        return None
    return min(slot["start"] for slot in hours)
