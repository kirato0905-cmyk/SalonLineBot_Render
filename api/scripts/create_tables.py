from api.db.session import engine, Base
from api.db import models


def main():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Done.")


if __name__ == "__main__":
    main()
