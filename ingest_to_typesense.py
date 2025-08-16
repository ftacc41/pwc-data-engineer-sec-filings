import pandas as pd
from pathlib import Path
import typesense
from sentence_transformers import SentenceTransformer
import os
from dotenv import load_dotenv
import sys
import traceback
from tqdm import tqdm

# --- CONFIGURATION ---
load_dotenv()

TYPESENSE_API_KEY = os.environ.get('TYPESENSE_API_KEY', 'xyz')
TYPESENSE_HOST = os.environ.get('TYPESENSE_HOST', 'localhost')
TYPESENSE_PORT = int(os.environ.get('TYPESENSE_PORT', 8108))

EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
COLLECTION_NAME = 'sec_filings'

def main():
    print("--- Starting Typesense Ingestion Process ---")
    try:
        # --- 1. Initialize Clients ---
        print(f"Step 1: Initializing Sentence Transformer model ('{EMBEDDING_MODEL}')...")
        model = SentenceTransformer(EMBEDDING_MODEL)
        print("✓ Model initialized.")

        client = typesense.Client({
            'nodes': [{'host': TYPESENSE_HOST, 'port': TYPESENSE_PORT, 'protocol': 'http'}],
            'api_key': TYPESENSE_API_KEY,
            'connection_timeout_seconds': 2
        })
        print("✓ Typesense client initialized.")

        # --- 2. Define and Create Typesense Collection ---
        print("\nStep 2: Defining and creating Typesense collection...")
        try:
            client.collections[COLLECTION_NAME].delete()
            print(f"  - Dropped existing collection '{COLLECTION_NAME}'.")
        except typesense.exceptions.ObjectNotFound:
            pass

        vector_dimension = model.get_sentence_embedding_dimension()
        print(f"  - Vector dimension determined by model: {vector_dimension}")
        collection_schema = {
            'name': COLLECTION_NAME,
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'cik', 'type': 'string', 'facet': True},
                {'name': 'name', 'type': 'string', 'facet': True},
                {'name': 'form', 'type': 'string', 'facet': True},
                {'name': 'filing_summary', 'type': 'string'},
                {'name': 'extracted_pdf_text', 'type': 'string'},
                {'name': 'embedding', 'type': 'float[]', 'num_dim': vector_dimension}
            ]
        }
        client.collections.create(collection_schema)
        print(f"✓ Collection '{COLLECTION_NAME}' created successfully.")

        # --- 3. Load and Prepare Data ---
        print("\nStep 3: Loading and preparing data from the Silver layer...")
        SCRIPT_DIR = Path(__file__).resolve().parent
        SILVER_DIR = SCRIPT_DIR / "data" / "silver"
        data_df = pd.read_parquet(SILVER_DIR / "sub.parquet")
        
        data_df.drop_duplicates(subset=['adsh'], inplace=True, keep='first')
        
        # --- THIS IS THE FIX FOR THE INDEXERROR ---
        data_df.reset_index(drop=True, inplace=True)
        print(f"  - Removed duplicate filings and reset index. Kept {len(data_df)} unique documents.")

        data_df['filing_summary'] = data_df['filing_summary'].fillna('')
        data_df['extracted_pdf_text'] = data_df['extracted_pdf_text'].fillna('')
        data_df['full_text'] = data_df['filing_summary'] + "\n\n" + data_df['extracted_pdf_text']
        print(f"✓ Loaded and prepared {len(data_df)} documents.")

    
        # --- 4. Generate Vector Embeddings ---
        print("\nStep 4: Generating vector embeddings for documents...")
        embeddings = model.encode(data_df['full_text'].tolist(), show_progress_bar=True)
        print("✓ Embeddings generated successfully.")

        # --- 5. Prepare and Import Documents ---
        print("\nStep 5: Preparing and importing documents into Typesense...")
        documents_to_import = []
        for i, row in tqdm(data_df.iterrows(), total=len(data_df), desc="Preparing documents"):
            documents_to_import.append({
                'id': str(row['adsh']),
                'cik': str(row['cik']),
                'name': row['name'],
                'form': row['form'],
                'filing_summary': row['filing_summary'],
                'extracted_pdf_text': row['extracted_pdf_text'],
                'embedding': embeddings[i].tolist()
            })
        
        results = client.collections[COLLECTION_NAME].documents.import_(
            documents_to_import, {'action': 'create', 'batch_size': 100}
        )
        print("✓ Document import complete.")

        num_successes = sum(1 for result in results if isinstance(result, dict) and result.get('success'))
        print(f"  - Successfully imported {num_successes}/{len(documents_to_import)} documents.")

        if num_successes != len(documents_to_import):
            print(f"\n--- ⚠️ WARNING: {len(documents_to_import) - num_successes} documents failed to import. ---")
            print("Showing details for the first 5 failures:")
            failures = [r for r in results if not (isinstance(r, dict) and r.get('success'))]
            for i, failure in enumerate(failures[:5]):
                 print(f"\n--- Failure {i+1} ---\n{failure}")

    except Exception as e:
        print("\n--- ❌ ERROR ---", file=sys.stderr)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        traceback.print_exc()

if __name__ == "__main__":
    main()