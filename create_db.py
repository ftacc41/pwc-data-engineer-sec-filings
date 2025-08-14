# create_db.py
print("Attempting to run create_db.py...") # <-- Add this line

from data_access.db import engine
from data_access.models import SQLModel, CompanyDim, FilingDim

def create_db_and_tables():
    print("Creating database and tables...")
    SQLModel.metadata.create_all(engine)
    print("Database and tables created successfully!")

if __name__ == "__main__":
    create_db_and_tables()
