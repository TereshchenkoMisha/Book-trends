import sqlite3
import os
from contextlib import contextmanager

@contextmanager
def conncet_database():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    db_path = os.path.join(BASE_DIR, "data", "database", "books.db")
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
