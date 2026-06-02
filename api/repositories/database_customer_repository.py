import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from api.db.session import SessionLocal
from api.db.models import Store, Customer


class DatabaseCustomerRepository:
    """顧客情報をSupabase/PostgreSQLで管理するRepository。"""

    def __init__(self, default_store_code: str = "store_default"):
        self.default_store_code = default_store_code

    def _get_default_store(self, db) -> Optional[Store]:
        return (
            db.query(Store)
            .filter(
                Store.store_code == self.default_store_code,
                Store.is_active == True,
            )
            .first()
        )

    def _get_customer(self, db, store_id, line_user_id: str) -> Optional[Customer]:
        return (
            db.query(Customer)
            .filter(
                Customer.store_id == store_id,
                Customer.line_user_id == line_user_id,
            )
            .first()
        )

    def _to_dict(self, customer: Customer) -> Dict[str, Any]:
        return {
            "customer_id": str(customer.id),
            "store_id": str(customer.store_id),
            "line_user_id": customer.line_user_id,
            "display_name": customer.display_name or "",
            "phone_number": customer.phone_number or "",
            "status": customer.status,
            "consented": customer.consented,
            "consented_at": customer.consented_at.isoformat() if customer.consented_at else None,
            "input_state": customer.input_state,
        }

    def get_or_create_customer(
        self,
        line_user_id: str,
        display_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        LINEユーザーが初めて操作した時に顧客を作成する。
        既に存在する場合は表示名のみ必要に応じて更新する。
        """
        db = SessionLocal()
        try:
            store = self._get_default_store(db)
            if not store:
                logging.error("Default store not found in database.")
                return None

            customer = self._get_customer(db, store.id, line_user_id)

            if customer:
                if display_name and customer.display_name != display_name:
                    customer.display_name = display_name
                    db.commit()
                    db.refresh(customer)

                return self._to_dict(customer)

            customer = Customer(
                store_id=store.id,
                line_user_id=line_user_id,
                display_name=display_name or "",
                status="active",
                consented=False,
                input_state=None,
            )

            db.add(customer)
            db.commit()
            db.refresh(customer)

            return self._to_dict(customer)

        except Exception as e:
            db.rollback()
            logging.error(f"Failed to get or create customer: {e}", exc_info=True)
            return None
        finally:
            db.close()

    def get_customer_by_line_user_id(self, line_user_id: str) -> Optional[Dict[str, Any]]:
        """LINEユーザーIDから顧客情報を取得する。"""
        db = SessionLocal()
        try:
            store = self._get_default_store(db)
            if not store:
                return None

            customer = self._get_customer(db, store.id, line_user_id)
            if not customer:
                return None

            return self._to_dict(customer)

        except Exception as e:
            logging.error(f"Failed to get customer: {e}", exc_info=True)
            return None
        finally:
            db.close()

    def update_profile(
        self,
        line_user_id: str,
        display_name: Optional[str] = None,
        phone_number: Optional[str] = None,
    ) -> bool:
        """氏名・電話番号を更新する。"""
        db = SessionLocal()
        try:
            store = self._get_default_store(db)
            if not store:
                logging.error("Default store not found in database.")
                return False

            customer = self._get_customer(db, store.id, line_user_id)
            if not customer:
                logging.warning(f"Customer not found: {line_user_id}")
                return False

            if display_name is not None:
                customer.display_name = display_name

            if phone_number is not None:
                customer.phone_number = phone_number

            db.commit()
            return True

        except Exception as e:
            db.rollback()
            logging.error(f"Failed to update customer profile: {e}", exc_info=True)
            return False
        finally:
            db.close()

    def set_consent(self, line_user_id: str, consented: bool = True) -> bool:
        """個人情報利用への同意状態を保存する。"""
        db = SessionLocal()
        try:
            store = self._get_default_store(db)
            if not store:
                return False

            customer = self._get_customer(db, store.id, line_user_id)
            if not customer:
                logging.warning(f"Customer not found: {line_user_id}")
                return False

            customer.consented = consented
            customer.consented_at = datetime.now(timezone.utc) if consented else None

            db.commit()
            return True

        except Exception as e:
            db.rollback()
            logging.error(f"Failed to update customer consent: {e}", exc_info=True)
            return False
        finally:
            db.close()

    def update_input_state(self, line_user_id: str, input_state: Optional[str]) -> bool:
        """
        現在どの入力段階にいるかを保存する。
        例: waiting_name / waiting_phone / selecting_menu / confirming_reservation
        完了時は None を渡す。
        """
        db = SessionLocal()
        try:
            store = self._get_default_store(db)
            if not store:
                return False

            customer = self._get_customer(db, store.id, line_user_id)
            if not customer:
                logging.warning(f"Customer not found: {line_user_id}")
                return False

            customer.input_state = input_state

            db.commit()
            return True

        except Exception as e:
            db.rollback()
            logging.error(f"Failed to update customer input state: {e}", exc_info=True)
            return False
        finally:
            db.close()

    def deactivate_customer(self, line_user_id: str) -> bool:
        """ブロック・利用停止などで顧客を無効化する場合に使う。"""
        db = SessionLocal()
        try:
            store = self._get_default_store(db)
            if not store:
                return False

            customer = self._get_customer(db, store.id, line_user_id)
            if not customer:
                return False

            customer.status = "inactive"
            customer.input_state = None

            db.commit()
            return True

        except Exception as e:
            db.rollback()
            logging.error(f"Failed to deactivate customer: {e}", exc_info=True)
            return False
        finally:
            db.close()
