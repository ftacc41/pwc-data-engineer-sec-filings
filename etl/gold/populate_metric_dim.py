import pandas as pd
import pathlib
from sqlmodel import Session
from data_access.db import engine
from data_access.models import MetricDim

# Define the paths to the Silver layer Parquet files
SILVER_DIR = pathlib.Path('data/silver/financials')
TAG_PARQUET_FILE = SILVER_DIR / 'tag.parquet'

def populate_metric_dim():
    """
    Reads the TAG.parquet file, populates the MetricDim table.
    """
    if not TAG_PARQUET_FILE.exists():
        print(f"Error: {TAG_PARQUET_FILE} not found. Please run the Silver ETL script first.")
        return

    # 1. Read the clean Silver layer data
    tag_df = pd.read_parquet(TAG_PARQUET_FILE)
    print(f"Read {len(tag_df)} records from {TAG_PARQUET_FILE}")

    # 2. Deduplicate metrics based on the unique key (tag and version)
    # The README specified that a unique key is a combination of tag and version
    unique_metrics_df = tag_df.drop_duplicates(subset=['tag', 'version']).copy()
    print(f"Found {len(unique_metrics_df)} unique metrics.")

    with Session(engine) as session:
        # Create a list of MetricDim objects
        metrics_to_add = []
        for _, row in unique_metrics_df.iterrows():
            metric = MetricDim(
                tag=row['tag'],
                version=row['version'],
                tlabel=row.get('tlabel'), # .get() is used for optional columns
                datatype=row.get('datatype'),
                iord=row.get('iord')
            )
            metrics_to_add.append(metric)
        
        # Add new metrics to the session
        session.add_all(metrics_to_add)
        session.commit()
        print(f"Successfully added {len(metrics_to_add)} unique metrics to MetricDim.")

    print("Populating MetricDim complete.")

if __name__ == '__main__':
    # Make sure to run this script from the project root
    populate_metric_dim()