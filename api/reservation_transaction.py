"""Reservation transaction helpers.

This file centralizes the intended transaction order for future refactors. The
current ReservationFlow still performs most operations inline for compatibility,
but new reservation code should route through this service.
"""
import logging
from typing import Any, Dict, Callable, Optional


class ReservationTransactionService:
    def __init__(self, calendar_helper, reservation_repository, notification_manager=None):
        self.calendar_helper = calendar_helper
        self.reservation_repository = reservation_repository
        self.notification_manager = notification_manager

    def create_reservation(
        self,
        reservation_data: Dict[str, Any],
        client_name: str,
        final_availability_check: Callable[[Dict[str, Any]], Dict[str, Any]],
        clear_cache: Optional[Callable[[], None]] = None,
    ) -> Dict[str, Any]:
        availability = final_availability_check(reservation_data)
        if not availability.get("available"):
            return {"success": False, "stage": "availability", "message": availability.get("message", "")}

        reservation_id = self.calendar_helper.generate_reservation_id(reservation_data["date"])
        reservation_data["reservation_id"] = reservation_id

        if not self.calendar_helper.create_reservation_event(reservation_data, client_name):
            return {"success": False, "stage": "calendar", "reservation_id": reservation_id}

        if not self.reservation_repository.save(reservation_data):
            logging.error(f"Calendar succeeded but repository save failed. reservation_id={reservation_id}")
            try:
                self.calendar_helper.cancel_reservation_by_id(reservation_id, reservation_data.get("staff"))
            except Exception as e:
                logging.error(f"Calendar rollback failed. reservation_id={reservation_id}: {e}", exc_info=True)
            return {"success": False, "stage": "repository", "reservation_id": reservation_id}

        if clear_cache:
            clear_cache()

        return {"success": True, "reservation_id": reservation_id, "reservation_data": reservation_data}
