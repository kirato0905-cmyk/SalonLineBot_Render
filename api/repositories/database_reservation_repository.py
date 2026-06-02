import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

from api.db.session import SessionLocal
from api.db.models import Store, Customer, Reservation


class DatabaseReservationRepository:
    """Supabase/PostgreSQL を予約データの正本として扱う Repository。"""

    STATUS_TO_DB = {
        "Confirmed": "confirmed",
        "confirmed": "confirmed",
        "予約済み": "confirmed",
        "Modified": "modified",
        "modified": "modified",
        "変更済み": "modified",
        "Cancelled": "cancelled",
        "Canceled": "cancelled",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "キャンセル済み": "cancelled",
    }

    STATUS_TO_FLOW = {
        "confirmed": "Confirmed",
        "modified": "Modified",
        "cancelled": "Cancelled",
    }

    def __init__(self, default_store_code: str = "store_default"):
        self.default_store_code = default_store_code

    def _get_default_store(self, db) -> Optional[Store]:
        return db.query(Store).filter(
            Store.store_code == self.default_store_code,
            Store.is_active == True
        ).first()

    def _parse_date(self, value: Any):
        if hasattr(value, "strftime") and not isinstance(value, str):
            return value
        return datetime.strptime(str(value), "%Y-%m-%d").date()

    def _parse_time(self, value: Any):
        if hasattr(value, "strftime") and not isinstance(value, str):
            return value
        text = str(value)
        fmt = "%H:%M:%S" if text.count(":") == 2 else "%H:%M"
        return datetime.strptime(text, fmt).time()

    def _to_int(self, value: Any) -> int:
        if value in (None, ""):
            return 0
        if isinstance(value, str):
            value = value.replace(",", "").replace("円", "").strip()
        return int(value or 0)

    def _db_status(self, value: Any) -> str:
        return self.STATUS_TO_DB.get(str(value), str(value or "confirmed").lower())

    def _flow_status(self, value: Any) -> str:
        return self.STATUS_TO_FLOW.get(str(value).lower(), str(value))

    def _get_or_create_customer(self, db, store_id, data: Dict[str, Any]) -> Optional[Customer]:
        line_user_id = data.get("user_id") or data.get("line_user_id")
        if not line_user_id:
            return None

        customer = db.query(Customer).filter(
            Customer.store_id == store_id,
            Customer.line_user_id == line_user_id,
        ).first()

        if customer:
            if data.get("client_name"):
                customer.display_name = data.get("client_name")
            if data.get("phone_number"):
                customer.phone_number = data.get("phone_number")
            return customer

        customer = Customer(
            store_id=store_id,
            line_user_id=line_user_id,
            display_name=data.get("client_name") or "",
            phone_number=data.get("phone_number") or None,
            status="active",
        )
        db.add(customer)
        db.flush()
        return customer

    def _to_flow_dict(self, reservation: Reservation, customer: Optional[Customer] = None) -> Dict[str, Any]:
        return {
            "store_id": str(reservation.store_id),
            "reservation_id": reservation.reservation_code,
            "reservation_code": reservation.reservation_code,
            "calendar_id": reservation.calendar_id or "",
            "calendar_event_id": reservation.calendar_event_id or "",
            "calendar_html_link": reservation.calendar_html_link or "",
            "user_id": customer.line_user_id if customer else "",
            "client_name": reservation.client_name or (customer.display_name if customer else ""),
            "phone_number": reservation.phone_number or (customer.phone_number if customer else ""),
            "date": reservation.date.strftime("%Y-%m-%d") if reservation.date else "",
            "start_time": reservation.start_time.strftime("%H:%M") if reservation.start_time else "",
            "end_time": reservation.end_time.strftime("%H:%M") if reservation.end_time else "",
            "service": reservation.service_summary or "",
            "services": reservation.services_json or [],
            "selected_staff": reservation.selected_staff_name or "",
            "assigned_staff": reservation.assigned_staff_name or "",
            "staff": reservation.assigned_staff_name or "",
            "duration": reservation.duration_minutes or 0,
            "total_duration": reservation.duration_minutes or 0,
            "price": reservation.total_price or 0,
            "total_price": reservation.total_price or 0,
            "status": self._flow_status(reservation.status),
            "remarks": reservation.remarks or "",
        }

    def save(self, reservation_data: Dict[str, Any]) -> bool:
        return self.save_reservation(reservation_data)

    def save_reservation(self, reservation_data: Dict[str, Any]) -> bool:
        db = SessionLocal()
        try:
            store = self._get_default_store(db)
            if not store:
                logging.error("Default store not found in database.")
                return False

            reservation_code = reservation_data.get("reservation_id") or reservation_data.get("reservation_code")
            if not reservation_code:
                logging.error("reservation_id is missing.")
                return False

            customer = self._get_or_create_customer(db, store.id, reservation_data)
            services = reservation_data.get("services") or reservation_data.get("cart") or []
            service_summary = reservation_data.get("service") or reservation_data.get("selected_menu_label") or ""

            if not service_summary and isinstance(services, list):
                service_summary = " / ".join(
                    str(item.get("service_name") or item.get("name"))
                    for item in services
                    if isinstance(item, dict) and (item.get("service_name") or item.get("name"))
                )
            if not service_summary:
                service_summary = "選択メニュー"

            reservation = Reservation(
                store_id=store.id,
                customer_id=customer.id if customer else None,
                reservation_code=reservation_code,
                status="confirmed",
                date=self._parse_date(reservation_data["date"]),
                start_time=self._parse_time(reservation_data["start_time"]),
                end_time=self._parse_time(reservation_data["end_time"]),
                client_name=reservation_data.get("client_name") or "",
                phone_number=reservation_data.get("phone_number") or "",
                selected_staff_name=reservation_data.get("selected_staff") or "",
                assigned_staff_name=reservation_data.get("assigned_staff") or reservation_data.get("staff") or "",
                service_summary=service_summary,
                services_json=services if isinstance(services, list) else [],
                duration_minutes=self._to_int(reservation_data.get("total_duration") or reservation_data.get("duration")),
                total_price=self._to_int(reservation_data.get("total_price") or reservation_data.get("price")),
                calendar_id=reservation_data.get("calendar_id") or "",
                calendar_event_id=reservation_data.get("calendar_event_id") or "",
                calendar_html_link=reservation_data.get("calendar_html_link") or "",
                remarks=reservation_data.get("remarks") or reservation_data.get("note") or "",
                source="line",
            )
            db.add(reservation)
            db.commit()
            return True
        except Exception as e:
            db.rollback()
            logging.error(f"Failed to save reservation to database: {e}", exc_info=True)
            return False
        finally:
            db.close()

    def get_user_reservations_by_user_id(self, user_id: str) -> List[Dict[str, Any]]:
        db = SessionLocal()
        try:
            results = (
                db.query(Reservation, Customer)
                .join(Customer, Reservation.customer_id == Customer.id)
                .filter(Customer.line_user_id == user_id)
                .filter(Reservation.status.in_(["confirmed", "modified"]))
                .order_by(Reservation.date.asc(), Reservation.start_time.asc())
                .all()
            )
            return [self._to_flow_dict(reservation, customer) for reservation, customer in results]
        except Exception as e:
            logging.error(f"Failed to get user reservations from database: {e}", exc_info=True)
            return []
        finally:
            db.close()

    def get_reservation_by_id(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        db = SessionLocal()
        try:
            result = (
                db.query(Reservation, Customer)
                .outerjoin(Customer, Reservation.customer_id == Customer.id)
                .filter(Reservation.reservation_code == reservation_id)
                .first()
            )
            if not result:
                return None
            reservation, customer = result
            return self._to_flow_dict(reservation, customer)
        except Exception as e:
            logging.error(f"Failed to get reservation from database: {e}", exc_info=True)
            return None
        finally:
            db.close()

    def update_reservation_status(self, reservation_id: str, status: str) -> bool:
        return self.update_reservation_data(reservation_id, {"status": status})

    def update_reservation_data(self, reservation_id: str, field_updates: Dict[str, Any]) -> bool:
        db = SessionLocal()
        try:
            reservation = db.query(Reservation).filter(
                Reservation.reservation_code == reservation_id
            ).first()
            if not reservation:
                logging.warning(f"Reservation {reservation_id} not found in database.")
                return False

            data = field_updates or {}
            if "status" in data or "Status" in data:
                reservation.status = self._db_status(data.get("status", data.get("Status")))
            if data.get("date") or data.get("Date"):
                reservation.date = self._parse_date(data.get("date", data.get("Date")))
            if data.get("start_time") or data.get("Start Time"):
                reservation.start_time = self._parse_time(data.get("start_time", data.get("Start Time")))
            if data.get("end_time") or data.get("End Time"):
                reservation.end_time = self._parse_time(data.get("end_time", data.get("End Time")))

            reservation.service_summary = data.get("service", data.get("Service", reservation.service_summary))
            services = data.get("services", data.get("Services JSON"))
            if isinstance(services, list):
                reservation.services_json = services

            reservation.selected_staff_name = data.get(
                "selected_staff", data.get("Selected Staff", reservation.selected_staff_name)
            )
            reservation.assigned_staff_name = data.get(
                "assigned_staff", data.get("Assigned Staff", data.get("staff", reservation.assigned_staff_name))
            )

            duration = data.get("total_duration", data.get("Duration (min)"))
            if duration is not None:
                reservation.duration_minutes = self._to_int(duration)
            price = data.get("total_price", data.get("Price"))
            if price is not None:
                reservation.total_price = self._to_int(price)

            reservation.calendar_id = data.get(
                "calendar_id", data.get("Calendar ID", reservation.calendar_id)
            )
            reservation.calendar_event_id = data.get(
                "calendar_event_id", data.get("Calendar Event ID", reservation.calendar_event_id)
            )
            reservation.calendar_html_link = data.get(
                "calendar_html_link", reservation.calendar_html_link
            )
            reservation.client_name = data.get("client_name", reservation.client_name)
            reservation.phone_number = data.get("phone_number", reservation.phone_number)
            reservation.remarks = data.get("remarks", data.get("note", reservation.remarks))
            reservation.modified_at = datetime.now()

            db.commit()
            return True
        except Exception as e:
            db.rollback()
            logging.error(f"Failed to update reservation in database: {e}", exc_info=True)
            return False
        finally:
            db.close()

