import os
import typesense
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

print("Loading sentence transformer model...")
EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
print("✓ Model loaded.")

# Define constants
COLLECTION_NAME = 'sec_filings'
TYPESENSE_API_KEY = os.environ.get('TYPESENSE_API_KEY', 'xyz')
TYPESENSE_HOST = os.environ.get('TYPESENSE_HOST', 'typesense')
TYPESENSE_PORT = int(os.environ.get('TYPESENSE_PORT', 8108))

# Initialize the Typesense client once at startup
TYPESENSE_CLIENT = typesense.Client({
    'nodes': [{
        'host': TYPESENSE_HOST,
        'port': TYPESENSE_PORT,
        'protocol': 'http'
    }],
    'api_key': TYPESENSE_API_KEY,
    'connection_timeout_seconds': 5
})