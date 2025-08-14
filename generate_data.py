import pandas as pd
from pathlib import Path
import os

def generate_sample_data():
    """
    Generates a small, consistent sample dataset for testing the ETL pipeline.
    The data is hard-coded to ensure all lookups will succeed.
    """
    print("Generating sample financial data...")

    # Define the data for the SUB (Company/Filing) table
    sub_data = pd.DataFrame(
        {
            "adsh": ["0000000001-19-000001", "0000000002-19-000002", "0000000003-19-000003"],
            "cik": [1000180, 1000180, 1000200],
            "name": ["Tesla Inc.", "Tesla Inc.", "Google Inc."],
            "form": ["10-Q", "10-K", "10-Q"],
        }
    )

    # Define the data for the TAG (Metric) table
    tag_data = pd.DataFrame(
        {
            "tag": ["Revenues", "NetIncomeLoss", "Assets"],
            "version": ["us-gaap/2019"] * 3,
            "custom": [1] * 3,
            "label": ["Revenues", "Net Income (Loss)", "Assets"],
        }
    )

    # Define the data for the PRE (Statement) table
    # This links filings (adsh) to metrics (tag)
    pre_data = pd.DataFrame(
        {
            "adsh": ["0000000001-19-000001", "0000000002-19-000002", "0000000003-19-000003"],
            "stmt": ["IS", "BS", "IS"],
            "tag": ["Revenues", "Assets", "NetIncomeLoss"],
        }
    )

    # Define the data for the NUM (Fact) table
    # This contains the actual values
    num_data = pd.DataFrame(
        {
            "adsh": ["0000000001-19-000001", "0000000002-19-000002", "0000000003-19-000003"],
            "tag": ["Revenues", "Assets", "NetIncomeLoss"],
            "version": ["us-gaap/2019"] * 3,
            "ddate": [20231231, 20231231, 20231231],
            "value": [25000000000, 75000000000, 15000000000],
        }
    )

    # Define the output directory and create it if it doesn't exist
    SILVER_PARQUET_DIR = Path("data/silver/financials")
    SILVER_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Save the DataFrames to Parquet files
    sub_data.to_parquet(SILVER_PARQUET_DIR / "sub.parquet")
    tag_data.to_parquet(SILVER_PARQUET_DIR / "tag.parquet")
    pre_data.to_parquet(SILVER_PARQUET_DIR / "pre.parquet")
    num_data.to_parquet(SILVER_PARQUET_DIR / "num.parquet")

    print("Sample data successfully generated and saved to Parquet files.")

if __name__ == "__main__":
    generate_sample_data()