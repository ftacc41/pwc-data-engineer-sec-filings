import pandas as pd
import chromadb
from pathlib import Path
from chromadb.utils import embedding_functions
from chromadb.config import Settings

# --- Setup and Configuration ---
# Path to your Silver layer data
SILVER_DIR = Path("./data/silver/financials")

# ChromaDB client connection to the Docker container
# The host and port must match the settings in your docker-compose.yml file.
# We use port 8001 on the host, which is mapped to port 8000 in the container.
try:
    client = chromadb.Client()
    client.heartbeat()
    print("Successfully connected to ChromaDB server.")
except Exception as e:
    print(f"Error connecting to ChromaDB: {e}")
    print("Please ensure your ChromaDB Docker container is running and accessible.")
    exit()

# Define an embedding function. We'll use a popular open-source model.
# This model will be downloaded and used by ChromaDB to convert text to vectors.
embedding_model_name = "sentence-transformers/all-MiniLM-L6-v2"
sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_model_name)

# --- Data Loading and Processing ---
print("Loading data from Parquet files...")
try:
    # Load the filings details (company info) from sub.parquet
    sub_df = pd.read_parquet(SILVER_DIR / "sub.parquet")
    # Load the presentation data (text content) from pre.parquet
    pre_df = pd.read_parquet(SILVER_DIR / "pre.parquet")

    # Merge the two dataframes on the filing ID ('adsh')
    # This links the presentation text to the company and filing details.
    merged_df = pd.merge(pre_df, sub_df, on="adsh", how="inner", suffixes=('_pre', '_sub'))
    print(f"Loaded and merged {len(merged_df)} rows of data.")
except FileNotFoundError as e:
    print(f"Error: Missing a required Parquet file. Please check the '{SILVER_DIR}' directory.")
    print(e)
    exit()
except Exception as e:
    print(f"An unexpected error occurred during data loading: {e}")
    exit()

# --- ChromaDB Collection Management ---
collection_name = "sec_filings"
# Get or create the collection to make the script idempotent
collection = client.get_or_create_collection(
    name=collection_name,
    embedding_function=sentence_transformer_ef
)

# --- Ingestion Loop ---
print(f"Starting to ingest documents into the '{collection_name}' collection...")
documents = []
metadatas = []
ids = []

for index, row in merged_df.iterrows():
    # Construct a meaningful document from the data
    doc_content = f"Company: {row['name']} | Filing ID: {row['adsh']} | Report Item: {row['tag']} | Text: {row['stmt']}"
    
    # Create a unique ID for each document
    # Using a combination of adsh and a unique tag to avoid conflicts
    unique_id = f"{row['adsh']}-{index}"
    
    # Define metadata for filtering and retrieval
    doc_metadata = {
        "company_name": row['name'],
        "cik": str(row['cik']),  # CIK should be a string for metadata filtering
        "filing_id": row['adsh'],
        "report_item": row['tag']
    }
    
    documents.append(doc_content)
    metadatas.append(doc_metadata)
    ids.append(unique_id)
    
    # Add documents in batches to improve performance
    if len(documents) >= 1000:
        try:
            collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            print(f"Ingested a batch of {len(documents)} documents.")
            documents, metadatas, ids = [], [], [] # Reset lists
        except Exception as e:
            print(f"Failed to ingest a batch: {e}")
            # Continue to the next batch even if one fails
            documents, metadatas, ids = [], [], []

# Ingest any remaining documents
if documents:
    try:
        collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids
        )
        print(f"Ingested the final batch of {len(documents)} documents.")
    except Exception as e:
        print(f"Failed to ingest the final batch: {e}")

print(f"\nIngestion complete. Total documents in collection: {collection.count()}")

