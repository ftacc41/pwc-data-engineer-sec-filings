import pandas as pd
import pathlib
from sqlmodel import Session, select
from data_access.db import engine
from data_access.models import FactFinancials, CompanyDim, FilingDim, MetricDim, StatementDim, DateDim

# Use pyarrow to handle chunked Parquet reading
import pyarrow.parquet as pq

# Define the paths to the Silver layer Parquet files
SILVER_DIR = pathlib.Path('data/silver/financials')
NUM_PARQUET_FILE = SILVER_DIR / 'num.parquet'
PRE_PARQUET_FILE = SILVER_DIR / 'pre.parquet'
SUB_PARQUET_FILE = SILVER_DIR / 'sub.parquet'

def get_dimension_lookup_maps(session: Session) -> dict:
    """Fetches all dimension tables and creates lookup maps for efficient lookups."""
    
    print("  - Creating dimension lookup maps...")
    
    companies = session.exec(select(CompanyDim)).all()
    cik_to_company_id = {c.cik: c.id for c in companies}
    
    filings = session.exec(select(FilingDim)).all()
    adsh_to_filing_id = {f.accession_number: f.id for f in filings}

    metrics = session.exec(select(MetricDim)).all()
    tag_version_to_metric_id = {(m.tag, m.version): m.id for m in metrics}

    statements = session.exec(select(StatementDim)).all()
    stmt_plabel_to_statement_id = {(s.stmt, s.plabel): s.id for s in statements}

    dates = session.exec(select(DateDim)).all()
    date_to_date_id = {d.date: d.id for d in dates}

    print("  - Dimension lookup maps created.")
    return {
        'cik_to_company_id': cik_to_company_id,
        'adsh_to_filing_id': adsh_to_filing_id,
        'tag_version_to_metric_id': tag_version_to_metric_id,
        'stmt_plabel_to_statement_id': stmt_plabel_to_statement_id,
        'date_to_date_id': date_to_date_id,
    }

def populate_fact_financials_chunked():
    """
    Reads the NUM file in chunks, joins with smaller files, and populates the FactFinancials table.
    """
    print("Populating FactFinancials (Chunked)...")

    if not all([NUM_PARQUET_FILE.exists(), PRE_PARQUET_FILE.exists(), SUB_PARQUET_FILE.exists()]):
        print("Error: Required Parquet files not found. Please run the Silver ETL script first.")
        return

    try:
        # Load smaller, necessary files into memory once
        print("  - Loading PRE and SUB files into memory...")
        pre_df = pd.read_parquet(PRE_PARQUET_FILE)
        sub_df = pd.read_parquet(SUB_PARQUET_FILE, columns=['adsh', 'cik'])
        sub_df['cik'] = sub_df['cik'].astype(str).str.strip()
        print("  - PRE and SUB files loaded.")

        total_facts_added = 0
        with Session(engine) as session:
            lookups = get_dimension_lookup_maps(session)
            
            print("  - Starting chunked reading of NUM file...")
            
            # Use pyarrow.ParquetFile to read in batches
            pq_file = pq.ParquetFile(NUM_PARQUET_FILE)
            
            # Use iter_batches to get the data in chunks (pyarrow.Table format)
            for i, batch in enumerate(pq_file.iter_batches(batch_size=100000)):
                print(f"    - Processing chunk {i+1}...")
                
                # Convert the pyarrow batch to a pandas DataFrame
                num_chunk = batch.to_pandas()
                
                # Join the current NUM chunk with the smaller dataframes
                fact_chunk = pd.merge(num_chunk, sub_df, on='adsh', how='inner')
                fact_chunk = pd.merge(fact_chunk, pre_df, on='adsh', how='inner')
                
                # Prepare columns for lookup
                fact_chunk['ddate'] = fact_chunk['ddate'].astype(str)
                fact_chunk = fact_chunk.rename(columns={'ddate': 'date'})
                
                facts_to_add = []
                for _, row in fact_chunk.iterrows():
                    # Perform all the dimension lookups
                    company_id = lookups['cik_to_company_id'].get(row.get('cik'))
                    filing_id = lookups['adsh_to_filing_id'].get(row.get('adsh'))
                    
                    # Use .get() for 'tag' and 'version' to handle missing data gracefully
                    tag_val = row.get('tag')
                    version_val = row.get('version')
                    metric_id = lookups['tag_version_to_metric_id'].get((tag_val, version_val))
                    
                    statement_id = lookups['stmt_plabel_to_statement_id'].get((row.get('stmt'), row.get('plabel')))
                    date_id = lookups['date_to_date_id'].get(row.get('date'))

                    # Only create a fact record if all lookups succeed
                    if all([company_id, filing_id, metric_id, statement_id, date_id]):
                        fact = FactFinancials(
                            company_id=company_id,
                            filing_id=filing_id,
                            metric_id=metric_id,
                            statement_id=statement_id,
                            date_id=date_id,
                            value=row.get('value'),
                            unit_of_measure=row.get('uom'),
                            footnote=row.get('foot')
                        )
                        facts_to_add.append(fact)
                
                print(f"      - Adding {len(facts_to_add)} valid fact records from chunk {i+1}...")
                session.add_all(facts_to_add)
                
                session.commit()
                total_facts_added += len(facts_to_add)

    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()

    print(f"\nPopulating FactFinancials complete. Total records added: {total_facts_added}.")

if __name__ == '__main__':
    populate_fact_financials_chunked()