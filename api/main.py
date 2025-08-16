import os
from typing import List, Optional
import json
import requests
import secrets
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sentence_transformers import SentenceTransformer
from sqlmodel import Session, select, func

from api.api_schemas import SearchResult, SearchResponse, CompanyTotalsResponse, CompanyTotal
from data_access.db import engine
from data_access.models import CompanyDim, FactFinancials


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
    is_user_ok = secrets.compare_digest(
        credentials.username.encode("utf8"), correct_username.encode("utf8")
    )
    is_pass_ok = secrets.compare_digest(
        credentials.password.encode("utf8"), correct_password.encode("utf8")
    )
    if not (is_user_ok and is_pass_ok):
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
    
    url = f"http://{TYPESENSE_HOST}:{TYPESENSE_PORT}/multi_search"
    headers = { 'Content-Type': 'application/json', 'X-TYPESENSE-API-KEY': TYPESENSE_API_KEY }
    search_requests = {
        'searches': [{
            'collection': COLLECTION_NAME, 'q': '*',
            'vector_query': f"embedding:({vector_as_string}, k:{k})",
        }]
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
        id=hit['document']['id'], cik=hit['document']['cik'],
        name=hit['document']['name'], form=hit['document']['form'],
        score=hit.get('vector_distance', 0.0)
    ) for hit in hits]
    
    return SearchResponse(results=results)


@app.get("/query/company-totals", response_model=CompanyTotalsResponse, tags=["Database Queries"])
def get_company_totals(
    limit: int = 10,
    db: Session = Depends(get_db_session), 
    username: str = Depends(check_auth)
):
    """
    Performs an analytical query on the Gold Layer data warehouse.
    Calculates the sum of all reported financial values for each company and
    returns the top companies by total value.
    """
    statement = (
        select(
            CompanyDim.name, 
            func.sum(FactFinancials.value).label("total_value")
        )
        .join(CompanyDim, FactFinancials.company_id == CompanyDim.id)
        .group_by(CompanyDim.name)
        .order_by(func.sum(FactFinancials.value).desc())
        .limit(limit)
    )
    
    results = db.exec(statement).all()
    company_totals = [CompanyTotal(company_name=name, total_value=value) for name, value in results]
    
    return CompanyTotalsResponse(results=company_totals)