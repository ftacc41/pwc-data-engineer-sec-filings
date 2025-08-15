import pandas as pd
from pathlib import Path
from sqlmodel import Session
from data_access.db import engine
from data_access.models import (
    CompanyDim, FilingDim, TagDim, DateDim, StatementDim, FactFinancials
)
import sys
import traceback

def main():
    # ... (the entire main function from before, no changes inside it) ...
    print("--- Starting Silver to Gold ETL Process ---")

    # --- 1. Set Up Paths & Load Silver Data ---
    SCRIPT_DIR = Path(__file__).resolve().parent
    ROOT_DIR = SCRIPT_DIR.parent
    SILVER_DIR = ROOT_DIR / "data" / "silver"

    print(f"Reading clean data from Silver layer: {SILVER_DIR}")
    dfs = {}
    for parquet_file in SILVER_DIR.glob("*.parquet"):
        table_name = parquet_file.stem
        dfs[table_name] = pd.read_parquet(parquet_file)
    print(f"✓ Loaded {len(dfs)} tables: {list(dfs.keys())}")

    with Session(engine) as session:
        # --- 2. Prepare and Populate Dimension Tables ---
        print("\nStep 1: Populating Dimension tables...")

        company_df = dfs['sub'][['cik', 'name', 'sic']].drop_duplicates(subset=['cik'])
        company_records = [CompanyDim(**row) for row in company_df.to_dict(orient='records')]
        session.add_all(company_records)
        print(f"  - Staged {len(company_records)} records for CompanyDim")

        filing_df = dfs['sub'][['adsh', 'form']].drop_duplicates(subset=['adsh'])
        filing_df.rename(columns={'adsh': 'accession_number', 'form': 'form_type'}, inplace=True)
        filing_records = [FilingDim(**row) for row in filing_df.to_dict(orient='records')]
        session.add_all(filing_records)
        print(f"  - Staged {len(filing_records)} records for FilingDim")
        
        tag_df = dfs['tag'].drop_duplicates(subset=['tag'])
        tag_records = [TagDim(**row) for row in tag_df.to_dict(orient='records')]
        session.add_all(tag_records)
        print(f"  - Staged {len(tag_records)} records for TagDim")

        date_df = dfs['num'][['ddate']].drop_duplicates()
        date_df['date_key'] = date_df['ddate'].dt.strftime('%Y%m%d')
        date_records = [DateDim(date_key=d) for d in date_df['date_key']]
        session.add_all(date_records)
        print(f"  - Staged {len(date_records)} records for DateDim")
        
        stmt_df = dfs['pre'][['stmt']].drop_duplicates()
        stmt_map = {'IS': 'Income Statement', 'BS': 'Balance Sheet', 'CF': 'Cash Flow'}
        stmt_df['statement_name'] = stmt_df['stmt'].map(stmt_map).fillna('Other')
        stmt_df.rename(columns={'stmt': 'statement_code'}, inplace=True)
        stmt_records = [StatementDim(**row) for row in stmt_df.to_dict(orient='records')]
        session.add_all(stmt_records)
        print(f"  - Staged {len(stmt_records)} records for StatementDim")

        session.commit()
        print("✓ Committed all dimension records to the database.")

        # --- 3. Prepare and Populate the Fact Table ---
        print("\nStep 2: Preparing and Populating the FactFinancials table...")

        company_map = pd.read_sql("SELECT id, cik FROM companydim", engine)
        filing_map = pd.read_sql("SELECT id, accession_number FROM filingdim", engine)
        tag_map = pd.read_sql("SELECT id, tag FROM tagdim", engine)
        date_map = pd.read_sql("SELECT id, date_key FROM datedim", engine)
        stmt_map = pd.read_sql("SELECT id, statement_code FROM statementdim", engine)

        facts = dfs['num'][['adsh', 'tag_id', 'ddate', 'value']]
        facts = facts.merge(dfs['pre'][['adsh', 'tag_id', 'stmt']], on=['adsh', 'tag_id'])
        facts = facts.merge(dfs['sub'][['adsh', 'cik']], on='adsh')
        facts = facts.merge(dfs['tag'][['tag_id', 'tag']], on='tag_id')

        facts['date_key'] = facts['ddate'].dt.strftime('%Y%m%d')
        facts = facts.merge(company_map.rename(columns={'id': 'company_id'}), on='cik')
        facts = facts.merge(filing_map.rename(columns={'id': 'filing_id', 'accession_number': 'adsh'}), on='adsh')
        facts = facts.merge(tag_map.rename(columns={'id': 'tag_id_fk', 'tag_id':'tag_id_orig'}), left_on='tag', right_on='tag')
        facts = facts.merge(date_map.rename(columns={'id': 'date_id'}), on='date_key')
        facts = facts.merge(stmt_map.rename(columns={'id': 'statement_id', 'statement_code': 'stmt'}), on='stmt')

        fact_df = facts[['value', 'company_id', 'filing_id', 'tag_id_fk', 'date_id', 'statement_id']]
        fact_df.rename(columns={'tag_id_fk': 'tag_id'}, inplace=True)
        
        fact_records = [FactFinancials(**row) for row in fact_df.to_dict(orient='records')]
        session.add_all(fact_records)
        print(f"  - Staged {len(fact_records)} records for FactFinancials")
        
        session.commit()
        print("✓ Committed fact records to the database.")

    print("\n--- ✅ Silver to Gold ETL Process Complete ---")

# --- UPDATED PART ---
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n--- ❌ ERROR ---", file=sys.stderr)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        traceback.print_exc()