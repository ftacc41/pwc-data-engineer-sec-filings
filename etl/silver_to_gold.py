import pandas as pd
from pathlib import Path
from sqlmodel import Session, select, delete
from datetime import datetime
from data_access.db import engine
from data_access.models import (
    CompanyDim, FilingDim, TagDim, DateDim, StatementDim, FactFinancials
)
import sys
import traceback

def main():
    """
    Main ETL script to process data from the Silver layer to the Gold layer (Data Warehouse).
    This script is idempotent and handles incremental loads for all dimensions.
    """
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

        # --- CompanyDim with SCD Type 2 Logic ---
        source_companies_df = dfs['sub'][['cik', 'name', 'sic']].drop_duplicates(subset=['cik']).astype({'cik': str})
        target_companies_df = pd.read_sql("SELECT id, cik, name, sic FROM companydim WHERE is_current = TRUE", engine)
        merged_df = pd.merge(source_companies_df, target_companies_df, on='cik', how='left', indicator=True, suffixes=('', '_target'))
        new_companies = merged_df[merged_df['_merge'] == 'left_only']
        if not new_companies.empty:
            new_records = [CompanyDim(**row) for row in new_companies[['cik', 'name', 'sic']].to_dict(orient='records')]
            session.add_all(new_records)
            print(f"  - Staged {len(new_records)} new records for CompanyDim")
        changed_companies = merged_df[(merged_df['_merge'] == 'both') & ((merged_df['name'] != merged_df['name_target']) | (merged_df['sic'].astype(str) != merged_df['sic_target'].astype(str)))]
        if not changed_companies.empty:
            ciks_to_update = changed_companies['cik'].tolist()
            statement = select(CompanyDim).where(CompanyDim.cik.in_(ciks_to_update), CompanyDim.is_current == True)
            records_to_expire = session.exec(statement).all()
            for record in records_to_expire:
                record.is_current = False
                record.valid_to = datetime.utcnow()
            new_versions_df = changed_companies[['cik', 'name', 'sic']]
            new_version_records = [CompanyDim(**row) for row in new_versions_df.to_dict(orient='records')]
            session.add_all(new_version_records)
            print(f"  - Staged {len(new_version_records)} updated (SCD2) records for CompanyDim")
        
        # --- FilingDim (Idempotent Load) ---
        source_filings_df = dfs['sub'][['adsh', 'form']].drop_duplicates(subset=['adsh'])
        source_filings_df.rename(columns={'adsh': 'accession_number', 'form': 'form_type'}, inplace=True)
        existing_filings = pd.read_sql("SELECT accession_number FROM filingdim", engine)
        new_filings_df = source_filings_df[~source_filings_df['accession_number'].isin(existing_filings['accession_number'])]
        if not new_filings_df.empty:
            filing_records = [FilingDim(**row) for row in new_filings_df.to_dict(orient='records')]
            session.add_all(filing_records)
            print(f"  - Staged {len(filing_records)} new records for FilingDim")

        # --- TagDim (Idempotent Load) ---
        source_tags_df = dfs['tag'].drop_duplicates(subset=['tag'])
        existing_tags = pd.read_sql("SELECT tag FROM tagdim", engine)
        new_tags_df = source_tags_df[~source_tags_df['tag'].isin(existing_tags['tag'])]
        if not new_tags_df.empty:
            tag_records = [TagDim(**row) for row in new_tags_df.to_dict(orient='records')]
            session.add_all(tag_records)
            print(f"  - Staged {len(tag_records)} new records for TagDim")

        # --- DateDim (Idempotent Load) ---
        source_date_keys = dfs['num']['ddate'].dt.strftime('%Y%m%d').drop_duplicates()
        existing_dates = pd.read_sql("SELECT date_key FROM datedim", engine)
        new_date_keys = source_date_keys[~source_date_keys.isin(existing_dates['date_key'])].tolist()
        if new_date_keys:
            date_records = [DateDim(date_key=key) for key in new_date_keys]
            session.add_all(date_records)
            print(f"  - Staged {len(date_records)} new records for DateDim")
        
        # --- StatementDim (Idempotent Load) ---
        source_stmts_df = dfs['pre'][['stmt']].drop_duplicates()
        source_stmts_df.rename(columns={'stmt': 'statement_code'}, inplace=True)
        existing_stmts = pd.read_sql("SELECT statement_code FROM statementdim", engine)
        new_stmts_df = source_stmts_df[~source_stmts_df['statement_code'].isin(existing_stmts['statement_code'])]
        if not new_stmts_df.empty:
            stmt_map = {'IS': 'Income Statement', 'BS': 'Balance Sheet', 'CF': 'Cash Flow'}
            new_stmts_df['statement_name'] = new_stmts_df['statement_code'].map(stmt_map).fillna('Other')
            stmt_records = [StatementDim(**row) for row in new_stmts_df.to_dict(orient='records')]
            session.add_all(stmt_records)
            print(f"  - Staged {len(stmt_records)} new records for StatementDim")

        session.commit()
        print("✓ Committed all dimension records to the database.")

        # --- 3. Prepare and Populate the Fact Table ---
        print("\nStep 2: Preparing and Populating the FactFinancials table...")
        # Clear existing facts for a full reload
        statement = delete(FactFinancials)
        session.exec(statement)
        print("  - Cleared existing records from FactFinancials table.")

        company_map = pd.read_sql("SELECT id, cik FROM companydim WHERE is_current = TRUE", engine)
        filing_map = pd.read_sql("SELECT id, accession_number FROM filingdim", engine)
        tag_map = pd.read_sql("SELECT id, tag FROM tagdim", engine)
        date_map = pd.read_sql("SELECT id, date_key FROM datedim", engine)
        stmt_map = pd.read_sql("SELECT id, statement_code FROM statementdim", engine)

        facts = dfs['num'][['adsh', 'tag_id', 'ddate', 'value']]
        facts = facts.merge(dfs['pre'][['adsh', 'tag_id', 'stmt']], on=['adsh', 'tag_id'])
        facts = facts.merge(dfs['sub'][['adsh', 'cik']], on='adsh')
        facts = facts.merge(dfs['tag'][['tag_id', 'tag']], on='tag_id')

        facts['date_key'] = facts['ddate'].dt.strftime('%Y%m%d')
        facts['cik'] = facts['cik'].astype(str)
        facts = facts.merge(company_map.rename(columns={'id': 'company_id'}), on='cik')
        facts = facts.merge(filing_map.rename(columns={'id': 'filing_id', 'accession_number': 'adsh'}), on='adsh')
        facts = facts.merge(tag_map.rename(columns={'id': 'tag_id_fk'}), left_on='tag', right_on='tag')
        facts = facts.merge(date_map.rename(columns={'id': 'date_id'}), on='date_key')
        facts = facts.merge(stmt_map.rename(columns={'id': 'statement_id', 'statement_code': 'stmt'}), on='stmt')
        
        fact_df = facts[['value', 'company_id', 'filing_id', 'tag_id_fk', 'date_id', 'statement_id']]
        fact_df.rename(columns={'tag_id_fk': 'tag_id'}, inplace=True)
        
        fact_records = [FactFinancials(**row) for row in fact_df.to_dict(orient='records')]
        session.add_all(fact_records)
        print(f"  - Staged {len(fact_records)} new records for FactFinancials")
        
        session.commit()
        print("✓ Committed fact records to the database.")

    print("\n--- ✅ Silver to Gold ETL Process Complete ---")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n--- ❌ ERROR ---", file=sys.stderr)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        traceback.print_exc()