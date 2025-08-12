from sqlmodel import SQLModel, create_engine
from .models import *

import os

DB_FILE = os.environ.get('SQLITE_FILE', 'data/warehouse.db')
engine = create_engine(f'sqlite:///{DB_FILE}', echo=False)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

if __name__ == '__main__':
    create_db_and_tables()
    print('DB and tables created at', DB_FILE)
