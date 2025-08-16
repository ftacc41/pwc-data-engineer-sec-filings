import os
from typing import List, Optional
import json
import requests
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sentence_transformers import SentenceTransformer
from sqlmodel import Session

from api.api_schemas import SearchResult, SearchResponse
from data_access.db import engine


# --- INITIALIZATION ---
load_dotenv()
app = FastAPI(
    title="SEC Filings API",
    description="API for querying and searching SEC financial documents.",
    version="1.0.0",
)
security = HTTPBasic()

print("Loading sentence transformer model...")
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
print("✓ Model loaded.")

COLLECTION_NAME = 'sec_filings'
TYPESENSE_API_KEY = os.environ.get('TYPESENSE_API_KEY', 'xyz')
TYPESENSE_HOST = os.environ.get('TYPESENSE_HOST', 'typesense')
TYPESENSE_PORT = int(os.environ.get('TYPESENSE_PORT', 8108))

# --- DEPENDENCIES ---
def get_db_session():
    with Session(engine) as session:
        yield session

def check_auth(credentials: HTTPBasicCredentials = Security(security)):
    correct_username = os.environ.get('API_USERNAME', 'admin')
    correct_password = os.environ.get('API_PASSWORD', 'supersecret')
    if not (credentials.username == correct_username and credentials.password == correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# --- API ENDPOINTS ---
@app.get("/", tags=["Status"])
def read_root(username: str = Depends(check_auth)):
    return {"message": f"Welcome, {username}! The SEC Filings API is running."}

@app.get("/search", response_model=SearchResponse, tags=["Search"])
def vector_search(
    q: str,
    form_type: Optional[str] = None,
    k: int = 10,
    username: str = Depends(check_auth)
):
    print(f"Received search query: '{q}'")
    
    query_vector = embedding_model.encode(q).tolist()
    vector_as_string = json.dumps(query_vector, separators=(',', ':'))
    
    # Define the request components for a raw HTTP POST request
    url = f"http://{TYPESENSE_HOST}:{TYPESENSE_PORT}/multi_search"
    headers = {
        'Content-Type': 'application/json',
        'X-TYPESENSE-API-KEY': TYPESENSE_API_KEY
    }
    search_requests = {
        'searches': [
            {
                'collection': COLLECTION_NAME,
                'q': '*',
                # --- THIS IS THE FINAL FIX ---
                'vector_query': f"embedding:({vector_as_string}, k:{k})", # Use a colon for k
            }
        ]
    }

    if form_type:
        search_requests['searches'][0]['filter_by'] = f'form:={form_type}'

    try:
        response = requests.post(url, headers=headers, json=search_requests)
        response.raise_for_status()
        search_results = response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    hits = search_results['results'][0].get('hits', [])
    if not hits:
        return SearchResponse(results=[])

    results = [SearchResult(
        id=hit['document']['id'],
        cik=hit['document']['cik'],
        name=hit['document']['name'],
        form=hit['document']['form'],
        score=hit.get('vector_distance', 0.0)
    ) for hit in hits]
    
    return SearchResponse(results=results)


@app.get("/query-gold", tags=["Database"])
def query_gold_layer(username: str = Depends(check_auth)):
    return {"message": "Endpoint for querying the data warehouse not implemented yet."}