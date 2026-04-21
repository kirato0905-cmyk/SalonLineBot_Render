import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from api.business_hours import get_hours_for_date

WEEKDAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def _weekday_key(target_date: date) -> str:
    return WEEKDAY_KEYS[target_date.weekday()]


def _normalize_time_str(value: Any) -> Optional[str]:
    try:
        if value is None:
            return None
        s = str(value).strip()
        parts = s.split(":")
        if len(parts) != 2:
            return None
        h, m = parts
        if not h.isdigit() or not m.isdigit():
            return None
        if len(h) == 1:
            h = f"0{h}"
        if len(m) == 1:
            m = f"0{m}"
        normalized = f"{h}:{m}"
        datetime.strptime(normalized, "%H:%M")
        return normalized
    except Exception:
        return None


def _time_to_minutes(value: str) -> Optional[int]:
    normalized = _normalize_time_str(value)
    if not normalized:
        return None
    h, m = map(int, normalized.split(':'))
    return h * 60 + m


def _minutes_to_time(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{h:02d}:{m:02d}"


def _normalize_store_periods(periods: Any) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    if not isinstance(periods, list):
        return results
    for item in periods:
        if not isinstance(item, dict):
            continue
        start = _normalize_time_str(item.get('start'))
        end = _normalize_time_str(item.get('end'))
        if not start or not end:
            continue
        if (_time_to_minutes(end) or 0) <= (_time_to_minutes(start) or 0):
            continue
        results.append({'start': start, 'end': end})
    return results


def _single_period_result(source: str, start: str, end: str) -> Dict[str, Any]:
    return {
        'is_working': True,
        'start': start,
        'end': end,
        'source': source,
        'periods': [{'start': start, 'end': end}],
    }


def _default_result(store_periods: List[Dict[str, str]]) -> Dict[str, Any]:
    if not store_periods:
        return {
            'is_working': False,
            'start': None,
            'end': None,
            'source': 'default',
            'periods': [],
        }
    return {
        'is_working': True,
        'start': store_periods[0]['start'],
        'end': store_periods[-1]['end'],
        'source': 'default',
        'periods': store_periods,
    }


def _invalid_result(source: str, reason: str) -> Dict[str, Any]:
    logging.error(f"[attendance] invalid attendance data. source={source}, reason={reason}")
    return {
        'is_working': False,
        'start': None,
        'end': None,
        'source': source,
        'periods': [],
        'error': reason,
    }


def _resolve_attendance_entry(entry: Any, source: str) -> Dict[str, Any]:
    if not isinstance(entry, dict):
        return _invalid_result(source, f'entry is not dict: {entry}')

    is_working = entry.get('is_working')
    if not isinstance(is_working, bool):
        return _invalid_result(source, f'is_working is not bool: {entry}')

    if is_working is False:
        return {
            'is_working': False,
            'start': None,
            'end': None,
            'source': source,
            'periods': [],
        }

    start = _normalize_time_str(entry.get('start'))
    end = _normalize_time_str(entry.get('end'))
    if not start or not end:
        return _invalid_result(source, f'missing/invalid start or end: {entry}')

    start_min = _time_to_minutes(start)
    end_min = _time_to_minutes(end)
    if start_min is None or end_min is None or end_min <= start_min:
        return _invalid_result(source, f'end <= start or invalid time: {entry}')

    return _single_period_result(source, start, end)


def get_staff_attendance_for_date(
    staff_record: Optional[Dict[str, Any]],
    target_date: date,
    fallback_to_store_hours: bool = True,
) -> Dict[str, Any]:
    """
    Resolve staff attendance with priority:
    1) attendance_shifts[date]
    2) attendance_exceptions[date]
    3) attendance[weekday]
    4) default store hours when enabled
    """
    if not isinstance(target_date, date):
        raise TypeError('target_date must be datetime.date')

    store_periods = _normalize_store_periods(get_hours_for_date(target_date) or [])
    if not staff_record or not isinstance(staff_record, dict):
        return _default_result(store_periods) if fallback_to_store_hours else {
            'is_working': False, 'start': None, 'end': None, 'source': 'default', 'periods': []
        }

    date_key = target_date.strftime('%Y-%m-%d')

    shifts = staff_record.get('attendance_shifts')
    if isinstance(shifts, dict) and date_key in shifts:
        return _resolve_attendance_entry(shifts.get(date_key), 'shift')

    exceptions = staff_record.get('attendance_exceptions')
    if isinstance(exceptions, dict) and date_key in exceptions:
        return _resolve_attendance_entry(exceptions.get(date_key), 'exception')

    weekly = staff_record.get('attendance')
    weekday_key = _weekday_key(target_date)
    if isinstance(weekly, dict) and weekday_key in weekly:
        return _resolve_attendance_entry(weekly.get(weekday_key), 'weekly')

    if fallback_to_store_hours:
        return _default_result(store_periods)

    return {
        'is_working': False,
        'start': None,
        'end': None,
        'source': 'default',
        'periods': [],
    }


def get_staff_effective_periods_for_date(
    staff_record: Optional[Dict[str, Any]],
    target_date: date,
    fallback_to_store_hours: bool = True,
) -> List[Dict[str, str]]:
    result = get_staff_attendance_for_date(
        staff_record=staff_record,
        target_date=target_date,
        fallback_to_store_hours=fallback_to_store_hours,
    )
    return result.get('periods', []) if result.get('is_working') else []


def is_staff_working_for_time(
    staff_record: Optional[Dict[str, Any]],
    target_date: date,
    start_time: str,
    end_time: str,
    fallback_to_store_hours: bool = True,
) -> bool:
    result = get_staff_attendance_for_date(staff_record, target_date, fallback_to_store_hours)
    if not result.get('is_working'):
        return False

    start_min = _time_to_minutes(start_time)
    end_min = _time_to_minutes(end_time)
    if start_min is None or end_min is None:
        return False

    for period in result.get('periods', []):
        p_start = _time_to_minutes(period.get('start', ''))
        p_end = _time_to_minutes(period.get('end', ''))
        if p_start is None or p_end is None:
            continue
        if start_min >= p_start and end_min <= p_end:
            return True
    return False
