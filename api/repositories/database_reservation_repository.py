import logging
from datetime import datetime
from typing import Dict, Any, Optional

from api.db.session import SessionLocal
from api.db.models import Store, Customer, Reservation


class DatabaseReservationRepository:
    def __init__(self, default_store_code: str = "store_default"):
        self.default_store_code = default_store_code

    def _get_default_store(self, db) -> Optional[Store]:
        return db.query(Store).filter(
            Store.store_code == self.default_store_code,
            Store.is_active == True
        ).first()

    def _parse_date(self, value: str):
        return datetime.strptime(value, "%Y-%m-%d").date()

    def _parse_time(self, value: str):
        return datetime.strptime(value, "%H:%M").time()

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

    def save(self, reservation_data: Dict[str, Any]) -> bool:
        return self.save_reservation(reservation_data)

    def save_reservation(self, reservation_data: Dict[str, Any]) -> bool:
        db = SessionLocal()

        try:
            store = self._get_default_store(db)
            if not store:
                logging.error("Default store not found in database.")
                return False

            customer = self._get_or_create_customer(db, store.id, reservation_data)

            services = reservation_data.get("services") or reservation_data.get("cart") or []
            service_summary = reservation_data.get("service") or reservation_data.get("selected_menu_label") or ""

            if not service_summary and isinstance(services, list):
                names = []
                for item in services:
                    if isinstance(item, dict):
                        name = item.get("service_name") or item.get("name")
                        if name:
                            names.append(str(name))
                service_summary = " / ".join(names)

            if not service_summary:
                service_summary = "選択メニュー"

            reservation = Reservation(
                store_id=store.id,
                customer_id=customer.id if customer else None,

                reservation_code=reservation_data.get("reservation_id") or reservation_data.get("reservation_code"),
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

                duration_minutes=int(reservation_data.get("total_duration") or reservation_data.get("duration") or 0),
                total_price=int(reservation_data.get("total_price") or reservation_data.get("price") or 0),

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
