import pandas as pd
import pathlib
from sqlmodel import Session, select
from data_access.db import engine
from data_access.models import CompanyDim, FilingDim, TagDim, DateDim

# Define the paths to the Silver layer Parquet files
SILVER_DIR = pathlib.Path('data/silver/financials')
SUB_PARQUET_FILE = SILVER_DIR / 'sub.parquet'
TAG_PARQUET_FILE = SILVER_DIR / 'tag.parquet'
PRE_PARQUET_FILE = SILVER_DIR / 'pre.parquet'
NUM_PARQUET_FILE = SILVER_DIR / 'num.parquet' # Using NUM to get date data

def populate_all_dims():
    """
    Reads the Silver layer Parquet files and populates all dimension tables.
    """
    if not SUB_PARQUET_FILE.exists():
        print(f"Error: {SUB_PARQUET_FILE} not found. Please run the data generation script first.")
        return

    # --- Read all necessary dataframes once ---
    sub_df = pd.read_parquet(SUB_PARQUET_FILE)
    tag_df = pd.read_parquet(TAG_PARQUET_FILE)
    pre_df = pd.read_parquet(PRE_PARQUET_FILE)
    num_df = pd.read_parquet(NUM_PARQUET_FILE)
    
    with Session(engine) as session:
        # --- Populate CompanyDim ---
        print("Populating CompanyDim...")
        # Deduplicate companies by their CIK
        companies_df = sub_df.drop_duplicates(subset=['cik']).copy()
        
        companies_to_add = []
        for _, row in companies_df.iterrows():
            company = CompanyDim(
                cik=str(row['cik']), # <-- CIK is now explicitly a string here
                name=row['name'],
                sic=None,
                country_of_incorporation=None,
                country_of_business=None
            )
            companies_to_add.append(company)
        
        session.add_all(companies_to_add)
        session.commit()
        print(f"Successfully added {len(companies_to_add)} unique companies to CompanyDim.")

        # --- Populate FilingDim ---
        print("Populating FilingDim...")
        companies_in_db = session.exec(select(CompanyDim)).all()
        cik_to_company_id = {c.cik: c.id for c in companies_in_db}

        filings_to_add = []
        for _, row in sub_df.iterrows():
            # Correcting the lookup by converting the CIK from the DataFrame to a string
            company_id = cik_to_company_id.get(str(row['cik']))
            if company_id:
                filing = FilingDim(
                    accession_number=row['adsh'],
                    form_type=row['form'],
                    period_of_report=None,
                    date_filed=None,
                    company_id=company_id
                )
                filings_to_add.append(filing)
        session.add_all(filings_to_add)
        session.commit()
        print(f"Successfully added {len(filings_to_add)} filings to FilingDim.")

        # --- Populate TagDim ---
        print("Populating TagDim...")
        tags_to_add = []
        for _, row in tag_df.iterrows():
            tag = TagDim(
                tag=row['tag'],
                version=row['version'],
                custom=row['custom'],
                label=row['label']
            )
            tags_to_add.append(tag)
        session.add_all(tags_to_add)
        session.commit()
        print(f"Successfully added {len(tags_to_add)} unique tags to TagDim.")
        
        # --- Populate DateDim ---
        print("Populating DateDim...")
        unique_dates = num_df['ddate'].unique()
        dates_to_add = []
        for date_str in unique_dates:
            dates_to_add.append(DateDim(date_key=str(date_str)))
        session.add_all(dates_to_add)
        session.commit()
        print(f"Successfully added {len(dates_to_add)} unique dates to DateDim.")

    print("Populating all dimensions complete.")

if __name__ == '__main__':
    populate_all_dims()