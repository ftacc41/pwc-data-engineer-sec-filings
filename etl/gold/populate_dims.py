import pandas as pd
import pathlib
from sqlmodel import Session, select
from data_access.db import engine
from data_access.models import CompanyDim, FilingDim

# Define the paths to the Silver layer Parquet files
SILVER_DIR = pathlib.Path('data/silver/financials')
SUB_PARQUET_FILE = SILVER_DIR / 'sub.parquet'

def populate_company_and_filing_dims():
    """
    Reads the SUB.parquet file and populates the CompanyDim and FilingDim tables.
    """
    if not SUB_PARQUET_FILE.exists():
        print(f"Error: {SUB_PARQUET_FILE} not found. Please run the Silver ETL script first.")
        return

    # 1. Read the clean Silver layer data
    sub_df = pd.read_parquet(SUB_PARQUET_FILE)
    print(f"Read {len(sub_df)} records from {SUB_PARQUET_FILE}")

    #clean CIK data
    sub_df['cik'] = sub_df['cik'].astype(str).str.strip()

    with Session(engine) as session:
        # --- Populate CompanyDim ---
        print("Populating CompanyDim...")
        # Deduplicate companies by their CIK
        companies_df = sub_df.drop_duplicates(subset=['cik']).copy()
        
        # Create a list of CompanyDim objects
        companies_to_add = []
        for _, row in companies_df.iterrows():
            company = CompanyDim(
                cik=row['cik'],
                name=row['name'],
                sic=row['sic'],
                country_of_incorporation=row['countryinc'],
                country_of_business=row['countryba']
            )
            companies_to_add.append(company)
        
        # Add new companies to the session
        session.add_all(companies_to_add)
        session.commit()
        print(f"Successfully added {len(companies_to_add)} unique companies to CompanyDim.")

        # --- Populate FilingDim ---
        print("Populating FilingDim...")
        
        # Get the newly created CompanyDim records to create a lookup map
        companies_in_db = session.exec(select(CompanyDim)).all()
        cik_to_company_id = {c.cik: c.id for c in companies_in_db}

        # Create a list of FilingDim objects
        filings_to_add = []
        for _, row in sub_df.iterrows():
            company_id = cik_to_company_id.get(row['cik'])
            if company_id:
                filing = FilingDim(
                    accession_number=row['adsh'],
                    form_type=row['form'],
                    period_of_report=row['period'],
                    date_filed=row['filed'],
                    company_id=company_id  # Foreign key link!
                )
                filings_to_add.append(filing)
        
        # Add all filings to the session
        session.add_all(filings_to_add)
        session.commit()
        print(f"Successfully added {len(filings_to_add)} filings to FilingDim.")

    print("Populating CompanyDim and FilingDim complete.")

if __name__ == '__main__':
    # Make sure to run this script from the project root
    populate_company_and_filing_dims()
