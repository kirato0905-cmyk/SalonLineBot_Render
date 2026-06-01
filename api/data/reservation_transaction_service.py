"""Reservation transaction service for Beauty Links.

This service centralizes reservation creation order and guards against
Calendar/repository inconsistencies.

Transaction order:
1. validate payload
2. final availability check
3. generate reservation_id
4. create Google Calendar event and capture event_id
5. save reservation to configured reservation repository
6. rollback Calendar event if repository save fails
7. notify operators for warning/critical states
8. clear runtime caches
"""
import logging
from typing import Any, Dict, Callable, Optional, Tuple


class ReservationTransactionService:
    REQUIRED_FIELDS = ("date", "start_time", "end_time")

    def __init__(self, calendar_helper, reservation_repository, notification_manager=None):
        self.calendar_helper = calendar_helper
        self.reservation_repository = reservation_repository
        self.notification_manager = notification_manager

    def _notify_critical(
        self,
        title: str,
        message: str,
        reservation_data: Optional[Dict[str, Any]] = None,
        error: Exception = None,
    ) -> None:
        try:
            if self.notification_manager and hasattr(self.notification_manager, "notify_critical_error"):
                self.notification_manager.notify_critical_error(title, message, reservation_data or {}, error)
            else:
                logging.critical("%s\n%s", title, message)
        except Exception as notify_error:
            logging.critical(
                "Failed to send critical notification. title=%s notify_error=%s",
                title,
                notify_error,
                exc_info=True,
            )

    def _notify_warning(self, title: str, message: str, reservation_data: Optional[Dict[str, Any]] = None) -> None:
        try:
            if self.notification_manager and hasattr(self.notification_manager, "notify_critical_error"):
                # Use critical channel with WARNING title because this still needs operator visibility.
                self.notification_manager.notify_critical_error(f"WARNING: {title}", message, reservation_data or None, None)
            else:
                logging.warning("%s\n%s", title, message)
        except Exception:
            logging.warning("Failed to send warning notification. title=%s", title, exc_info=True)

    def _validate_payload(self, data: Dict[str, Any]) -> Tuple[bool, str]:
        for field in self.REQUIRED_FIELDS:
            if not data.get(field):
                return False, f"missing_{field}"

        if not (data.get("staff") or data.get("assigned_staff")):
            return False, "missing_staff"

        if not (data.get("service") or data.get("services")):
            return False, "missing_service"

        if not data.get("store_id"):
            data["store_id"] = "store_default"

        return True, "ok"

    def _save_repository(self, data: Dict[str, Any]) -> bool:
        if hasattr(self.reservation_repository, "save"):
            return bool(self.reservation_repository.save(data))
        if hasattr(self.reservation_repository, "save_reservation"):
            return bool(self.reservation_repository.save_reservation(data))
        raise AttributeError("reservation_repository must have save() or save_reservation()")

    def _rollback_calendar(self, data: Dict[str, Any]) -> bool:
        calendar_id = data.get("calendar_id")
        event_id = data.get("calendar_event_id")

        if calendar_id and event_id and hasattr(self.calendar_helper, "cancel_event_by_event_id"):
            return bool(self.calendar_helper.cancel_event_by_event_id(calendar_id, event_id))

        return bool(
            self.calendar_helper.cancel_reservation_by_id(
                data.get("reservation_id"),
                data.get("assigned_staff") or data.get("staff"),
            )
        )

    def create_reservation(
        self,
        reservation_data: Dict[str, Any],
        client_name: str,
        final_availability_check: Callable[[Dict[str, Any]], Dict[str, Any]],
        clear_cache: Optional[Callable[[], None]] = None,
    ) -> Dict[str, Any]:
        data = dict(reservation_data or {})
        valid, reason = self._validate_payload(data)
        if not valid:
            return {"success": False, "stage": "validation", "reason": reason}

        availability = final_availability_check(data)
        if not availability.get("available"):
            return {
                "success": False,
                "stage": "availability",
                "reason": availability.get("reason", "not_available"),
                "message": availability.get("message", ""),
            }

        resolved_staff = availability.get("resolved_staff")
        if resolved_staff:
            data["assigned_staff"] = resolved_staff
            data["staff"] = resolved_staff

        reservation_id = data.get("reservation_id") or self.calendar_helper.generate_reservation_id(data["date"])
        data["reservation_id"] = reservation_id

        calendar_result = self.calendar_helper.create_reservation_event_with_result(data, client_name)
        if not calendar_result.get("success"):
            return {
                "success": False,
                "stage": "calendar",
                "reason": calendar_result.get("reason", "calendar_failed"),
                "message": calendar_result.get("message", ""),
                "reservation_id": reservation_id,
            }

        data["calendar_event_id"] = calendar_result.get("event_id", "")
        data["calendar_id"] = calendar_result.get("calendar_id", "")
        data["calendar_html_link"] = calendar_result.get("html_link", "")

        try:
            saved = self._save_repository(data)
        except Exception as e:
            logging.error("Repository save raised after Calendar succeeded. reservation_id=%s", reservation_id, exc_info=True)
            saved = False
            save_error = e
        else:
            save_error = None

        if not saved:
            rollback_success = False
            try:
                rollback_success = self._rollback_calendar(data)
            except Exception as rollback_error:
                logging.error("Calendar rollback raised. reservation_id=%s", reservation_id, exc_info=True)
                self._notify_critical(
                    title="予約不整合：予約不整合：Calendar作成済み / 保存先Repository保存失敗 / rollback例外",
                    message="Google Calendarには予定が残っている可能性があります。手動確認してください。",
                    reservation_data=data,
                    error=rollback_error,
                )
                return {
                    "success": False,
                    "stage": "repository",
                    "rollback_success": False,
                    "requires_manual_check": True,
                    "reservation_id": reservation_id,
                    "reservation_data": data,
                }

            if not rollback_success:
                self._notify_critical(
                    title="予約不整合：予約不整合：Calendar作成済み / 保存先Repository保存失敗 / rollback失敗",
                    message="Google Calendarには予定が残っている可能性があります。保存先Repositoryには保存されていません。手動確認してください。",
                    reservation_data=data,
                    error=save_error,
                )
                return {
                    "success": False,
                    "stage": "repository",
                    "rollback_success": False,
                    "requires_manual_check": True,
                    "reservation_id": reservation_id,
                    "reservation_data": data,
                }

            self._notify_warning(
                title="Calendar作成後に保存先Repositoryへの保存失敗。rollback済み",
                message="Calendar予定は削除済みです。ユーザーには再試行案内を出してください。",
                reservation_data=data,
            )
            return {
                "success": False,
                "stage": "repository",
                "rollback_success": True,
                "requires_manual_check": False,
                "reservation_id": reservation_id,
            }

        if clear_cache:
            try:
                clear_cache()
            except Exception:
                logging.warning("clear_cache failed after reservation success", exc_info=True)

        return {"success": True, "reservation_id": reservation_id, "reservation_data": data}
