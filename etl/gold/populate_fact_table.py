import pandas as pd
import pathlib
from sqlmodel import Session, select
import pyarrow.parquet as pq
from data_access.db import engine
from data_access.models import FactFinancials, CompanyDim, FilingDim, TagDim, DateDim

# Define the paths to the Silver layer Parquet files
SILVER_DIR = pathlib.Path('data/silver/financials')
PRE_PARQUET_FILE = SILVER_DIR / 'pre.parquet'
SUB_PARQUET_FILE = SILVER_DIR / 'sub.parquet'
NUM_PARQUET_FILE = SILVER_DIR / 'num.parquet'

def populate_fact_financials_chunked():
    """
    Populates the FactFinancials table by chunking the large NUM.parquet file.
    """
    print("Populating FactFinancials (Chunked)...")

    # Load lookup tables into memory once
    try:
        pre_df = pd.read_parquet(PRE_PARQUET_FILE)
        sub_df = pd.read_parquet(SUB_PARQUET_FILE)
        print("  - Loading PRE and SUB files into memory...")
    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure the Silver ETL has been run.")
        return
    
    # Create lookup maps from the database
    with Session(engine) as session:
        print("  - Creating dimension lookup maps...")
        company_map = {c.cik: c.id for c in session.exec(select(CompanyDim)).all()}
        filing_map = {f.accession_number: f.id for f in session.exec(select(FilingDim)).all()}
        tag_map = {t.tag: t.id for t in session.exec(select(TagDim)).all()}
        date_map = {d.date_key: d.id for d in session.exec(select(DateDim)).all()}
        print("  - Dimension lookup maps created.")

    total_added = 0
    # Process the NUM file in chunks
    try:
        parquet_file = pq.ParquetFile(NUM_PARQUET_FILE)
    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure the Silver ETL has been run.")
        return

    for i, chunk in enumerate(parquet_file.iter_batches(batch_size=100000)):
        num_chunk = chunk.to_pandas()
        print(f"  - Processing chunk {i + 1}...")

        # Convert ddate to string for consistent lookup
        num_chunk['ddate'] = num_chunk['ddate'].astype(str)

        # Merge with pre_df to get statement info
        merged_df = pd.merge(num_chunk, pre_df, on=['adsh', 'tag'], how='left')

        # Merge with sub_df to get company info (CIK)
        merged_df = pd.merge(merged_df, sub_df, on=['adsh'], how='left')

        # Filter out rows with missing merge data
        merged_df.dropna(subset=['adsh', 'tag', 'cik'], inplace=True)
        
        # Ensure CIK is a string for lookup consistency
        merged_df['cik'] = merged_df['cik'].astype(str)

        # Map CIK, ADSH, Tag, and DDate to their respective dimension IDs
        merged_df['company_id'] = merged_df['cik'].apply(lambda c: company_map.get(c))
        merged_df['filing_id'] = merged_df['adsh'].apply(lambda a: filing_map.get(a))
        merged_df['tag_id'] = merged_df['tag'].apply(lambda t: tag_map.get(t))
        merged_df['date_id'] = merged_df['ddate'].apply(lambda d: date_map.get(d))

        # Filter out rows where any ID lookup failed
        valid_facts_df = merged_df.dropna(subset=['company_id', 'filing_id', 'tag_id', 'date_id'])
        
        # Check if the dataframe has any records
        if not valid_facts_df.empty:
            with Session(engine) as session:
                fact_records = [
                    FactFinancials(
                        value=row['value'],
                        company_id=int(row['company_id']),
                        filing_id=int(row['filing_id']),
                        tag_id=int(row['tag_id']),
                        date_id=int(row['date_id'])
                    )
                    for _, row in valid_facts_df.iterrows()
                ]
                session.add_all(fact_records)
                session.commit()
                print(f"  - Adding {len(fact_records)} valid fact records from chunk {i + 1}...")
                total_added += len(fact_records)
        else:
            print(f"  - Adding 0 valid fact records from chunk {i + 1}...")

    print(f"\nPopulating FactFinancials complete. Total records added: {total_added}.")

if __name__ == '__main__':
    populate_fact_financials_chunked()