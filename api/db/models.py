from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,
    Date,
    Time,
    DateTime,
    Text,
    ForeignKey,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
import uuid

from api.db.session import Base


class Store(Base):
    __tablename__ = "stores"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    store_code = Column(String, nullable=False, unique=True)
    store_name = Column(String, nullable=False)

    line_channel_id = Column(String, nullable=True, unique=True)
    timezone = Column(String, nullable=False, default="Asia/Tokyo")

    google_sheet_id = Column(String, nullable=True)
    slack_webhook_url = Column(Text, nullable=True)

    is_active = Column(Boolean, nullable=False, default=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class Customer(Base):
    __tablename__ = "customers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)

    line_user_id = Column(String, nullable=False)
    display_name = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)

    status = Column(String, nullable=False, default="active")
    consented = Column(Boolean, nullable=False, default=False)
    consented_at = Column(DateTime(timezone=True), nullable=True)
    input_state = Column(String, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    store = relationship("Store")

    __table_args__ = (
        UniqueConstraint("store_id", "line_user_id", name="uq_customers_store_line_user"),
    )


class Reservation(Base):
    __tablename__ = "reservations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=True)

    reservation_code = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")

    date = Column(Date, nullable=False)
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)

    client_name = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)

    selected_staff_name = Column(String, nullable=True)
    assigned_staff_name = Column(String, nullable=True)

    service_summary = Column(Text, nullable=False)
    services_json = Column(JSONB, nullable=False, default=list)

    duration_minutes = Column(Integer, nullable=False, default=0)
    total_price = Column(Integer, nullable=False, default=0)

    calendar_id = Column(Text, nullable=True)
    calendar_event_id = Column(Text, nullable=True)
    calendar_html_link = Column(Text, nullable=True)

    remarks = Column(Text, nullable=True)
    source = Column(String, nullable=False, default="line")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    cancelled_at = Column(DateTime(timezone=True), nullable=True)
    modified_at = Column(DateTime(timezone=True), nullable=True)

    store = relationship("Store")
    customer = relationship("Customer")

    __table_args__ = (
        UniqueConstraint("store_id", "reservation_code", name="uq_reservations_store_code"),
    )
