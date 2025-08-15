# create_db.py

# Import the function that already has all the logic
from data_access.db import create_db_and_tables, DB_FILE

if __name__ == "__main__":
    print("--- Running Database and Table Creation ---")
    
    # Call the function from the data_access layer
    create_db_and_tables()
    
    print(f"✅ Database and tables created successfully at: {DB_FILE}")