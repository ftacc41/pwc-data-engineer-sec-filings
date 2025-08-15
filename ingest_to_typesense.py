import pandas as pd
from pathlib import Path
import typesense
import os
import time

# --- Setup and Configuration ---
# Path to your Silver layer data
SILVER_DIR = Path("./data/silver/financials")

# Typesense client connection to the Docker container
# It will automatically use the TYPESENSE_API_KEY environment variable.
try:
    client = typesense.Client({
        'nodes': [{
            'host': os.environ.get('TYPESENSE_HOST', 'typesense'),
            'port': '8108',
            'protocol': 'http'
        }],
        'api_key': os.environ.get('TYPESENSE_API_KEY', 'xyz'),
        'connection_timeout_seconds': 2
    })
    
    # Wait for the Typesense container to be healthy
    print("Waiting for Typesense container to be ready...")
    for i in range(10):
        try:
            client.collections.retrieve()
            print("Successfully connected to Typesense server.")
            break
        except typesense.exceptions.TypesenseClientError:
            time.sleep(2)
    else:
        print("Error: Could not connect to Typesense server.")
        exit()
        
except Exception as e:
    print(f"Error connecting to Typesense: {e}")
    print("Please ensure your Typesense Docker container is running and accessible.")
    exit()

# Define the schema for our Typesense collection
# It includes fields for searching and filtering.
collection_schema = {
    'name': 'financial_filings',
    'fields': [
        {'name': 'company_name', 'type': 'string', 'facet': True, 'sort': True},
        {'name': 'cik', 'type': 'string', 'facet': True},
        {'name': 'filing_id', 'type': 'string'},
        {'name': 'report_item', 'type': 'string'},
        {'name': 'content', 'type': 'string'} # This field will hold the text we want to search
    ],
    'default_sorting_field': 'company_name'
}


# --- Data Loading and Processing ---
print("Loading data from Parquet files...")
try:
    # Load the filings details (company info) from sub.parquet
    sub_df = pd.read_parquet(SILVER_DIR / "sub.parquet")
    # Load the presentation data (text content) from pre.parquet
    pre_df = pd.read_parquet(SILVER_DIR / "pre.parquet")
    
    # Merge the two dataframes on the filing ID ('adsh')
    merged_df = pd.merge(pre_df, sub_df, on="adsh", how="inner", suffixes=('_pre', '_sub'))
    print(f"Loaded and merged {len(merged_df)} rows of data.")
except FileNotFoundError as e:
    print(f"Error: Missing a required Parquet file. Please check the '{SILVER_DIR}' directory.")
    print(e)
    exit()
except Exception as e:
    print(f"An unexpected error occurred during data loading: {e}")
    exit()

# --- Typesense Collection Management ---
collection_name = 'financial_filings'

# Check if the collection already exists and drop it if it does
try:
    client.collections[collection_name].delete()
    print(f"Dropped old '{collection_name}' collection.")
except typesense.exceptions.TypesenseClientError as e:
    pass

# Create the collection with the defined schema
client.collections.create(collection_schema)
print(f"Created new '{collection_name}' collection.")

# --- Ingestion Loop ---
print(f"Starting to ingest documents into the '{collection_name}' collection...")
documents = []
for index, row in merged_df.iterrows():
    # Construct a document from the data to match the schema
    doc = {
        "company_name": row['name'],
        "cik": str(row['cik']),
        "filing_id": row['adsh'],
        "report_item": row['tag'],
        "content": row['stmt']
    }
    documents.append(doc)
    
    # Typesense client has a bulk import method
    # It's more efficient to send documents in batches
    if len(documents) >= 1000:
        try:
            client.collections[collection_name].documents.import_(documents)
            print(f"Ingested a batch of {len(documents)} documents.")
            documents = [] # Reset list
        except typesense.exceptions.TypesenseClientError as e:
            print(f"Failed to ingest a batch: {e}")
            documents = []
            
# Ingest any remaining documents
if documents:
    try:
        client.collections[collection_name].documents.import_(documents)
        print(f"Ingested the final batch of {len(documents)} documents.")
    except typesense.exceptions.TypesenseClientError as e:
        print(f"Failed to ingest the final batch: {e}")

print(f"\nIngestion complete. Total documents in collection: {client.collections[collection_name].retrieve()['num_documents']}.")