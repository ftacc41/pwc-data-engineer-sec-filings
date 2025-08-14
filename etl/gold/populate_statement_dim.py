import pandas as pd
import pathlib
from sqlmodel import Session
from data_access.db import engine
from data_access.models import StatementDim

# Define the paths to the Silver layer Parquet files
SILVER_DIR = pathlib.Path('data/silver/financials')
PRE_PARQUET_FILE = SILVER_DIR / 'pre.parquet'

def populate_statement_dim():
    """
    Reads the PRE.parquet file, populates the StatementDim table.
    """
    if not PRE_PARQUET_FILE.exists():
        print(f"Error: {PRE_PARQUET_FILE} not found. Please run the Silver ETL script first.")
        return

    # 1. Read the clean Silver layer data
    pre_df = pd.read_parquet(PRE_PARQUET_FILE)
    print(f"Read {len(pre_df)} records from {PRE_PARQUET_FILE}")

    # 2. Deduplicate statement types based on stmt and plabel
    unique_statements_df = pre_df.drop_duplicates(subset=['stmt', 'plabel']).copy()
    print(f"Found {len(unique_statements_df)} unique statement types.")

    #remove rows with NaN in 'stmt' column
    unique_statements_df.dropna(subset=['stmt'], inplace=True)
    
    with Session(engine) as session:
        # Create a list of StatementDim objects
        statements_to_add = []
        for _, row in unique_statements_df.iterrows():
            statement = StatementDim(
                stmt=row['stmt'],
                plabel=row['plabel']
            )
            statements_to_add.append(statement)
        
        # Add new statements to the session
        session.add_all(statements_to_add)
        session.commit()
        print(f"Successfully added {len(statements_to_add)} unique statements to StatementDim.")

    print("Populating StatementDim complete.")

if __name__ == '__main__':
    # Make sure to run this script from the project root
    populate_statement_dim()