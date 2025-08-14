import pandas as pd
import pathlib

# Define the path to the Silver layer Parquet file
SILVER_DIR = pathlib.Path('data/silver/financials')
SUB_PARQUET_FILE = SILVER_DIR / 'sub.parquet'

try:
    if not SUB_PARQUET_FILE.exists():
        print(f"Error: {SUB_PARQUET_FILE} not found. Please run the Silver ETL script first.")
    else:
        # Read the clean Silver layer data
        sub_df = pd.read_parquet(SUB_PARQUET_FILE)
        print(f"Successfully read {len(sub_df)} records.")
        print("Column names in the DataFrame:")
        print(list(sub_df.columns))

except Exception as e:
    print(f"An error occurred: {e}")