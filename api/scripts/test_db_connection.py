from api.db.session import SessionLocal
from api.db.models import Store


def main():
    db = SessionLocal()
    try:
        stores = db.query(Store).all()
        print(f"stores count: {len(stores)}")

        for store in stores:
            print(store.store_code, store.store_name)

    finally:
        db.close()


if __name__ == "__main__":
    main()
