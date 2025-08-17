import os
from typing import List, Optional
import json
import requests
import secrets
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Security, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sentence_transformers import SentenceTransformer
from sqlmodel import Session, select, func

from api.api_schemas import (
    SearchResult, SearchResponse, CompanyTotalsResponse, CompanyTotal,
    SubMission, SubMissionCreate, SubMissionUpdate
)
from data_access.db import engine
from data_access.models import CompanyDim, FactFinancials
from api import services # Import the new services module

# --- INITIALIZATION ---
load_dotenv()
app = FastAPI(
    title="SEC Filings API",
    description="API for querying and searching SEC financial documents.",
    version="1.0.0",
)
security = HTTPBasic()

# --- Global Clients (loaded once at startup) ---
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
    is_user_ok = secrets.compare_digest(credentials.username.encode("utf8"), correct_username.encode("utf8"))
    is_pass_ok = secrets.compare_digest(credentials.password.encode("utf8"), correct_password.encode("utf8"))
    if not (is_user_ok and is_pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# --- API ROUTERS ---

# Router for main application endpoints
main_router = APIRouter()

@main_router.get("/", tags=["Status"])
def read_root(username: str = Depends(check_auth)):
    return {"message": f"Welcome, {username}! The SEC Filings API is running."}

@main_router.get("/search", response_model=SearchResponse, tags=["Search"])
def vector_search(q: str, form_type: Optional[str] = None, k: int = 10, username: str = Depends(check_auth)):
    query_vector = embedding_model.encode(q).tolist()
    vector_as_string = json.dumps(query_vector, separators=(',', ':'))
    url = f"http://{TYPESENSE_HOST}:{TYPESENSE_PORT}/multi_search"
    headers = { 'Content-Type': 'application/json', 'X-TYPESENSE-API-KEY': TYPESENSE_API_KEY }
    search_requests = {'searches': [{'collection': COLLECTION_NAME, 'q': '*', 'vector_query': f"embedding:({vector_as_string}, k:{k})"}]}
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
    results = [SearchResult(id=hit['document']['id'], cik=hit['document']['cik'], name=hit['document']['name'], form=hit['document']['form'], score=hit.get('vector_distance', 0.0)) for hit in hits]
    return SearchResponse(results=results)

@main_router.get("/query/company-totals", response_model=CompanyTotalsResponse, tags=["Database Queries"])
def get_company_totals(limit: int = 10, db: Session = Depends(get_db_session), username: str = Depends(check_auth)):
    statement = (select(CompanyDim.name, func.sum(FactFinancials.value).label("total_value")).join(CompanyDim, FactFinancials.company_id == CompanyDim.id).group_by(CompanyDim.name).order_by(func.sum(FactFinancials.value).desc()).limit(limit))
    results = db.exec(statement).all()
    company_totals = [CompanyTotal(company_name=name, total_value=value) for name, value in results]
    return CompanyTotalsResponse(results=company_totals)

# --- NEW ROUTER FOR RAW DATA CRUD ---
crud_router = APIRouter(prefix="/raw/submissions", tags=["Raw Data CRUD"], dependencies=[Depends(check_auth)])

@crud_router.post("/", response_model=List[SubMission], status_code=status.HTTP_201_CREATED)
def create_new_submissions(submissions: List[SubMissionCreate]):
    """
    Create one or more new raw submission records (batch-friendly).
    """
    return services.create_submissions(submissions)

@crud_router.get("/", response_model=List[SubMission])
def read_all_submissions(skip: int = 0, limit: int = 100):
    """
    Retrieve all raw submission records with pagination.
    """
    return services.get_all_submissions(skip=skip, limit=limit)

@crud_router.get("/{adsh}", response_model=SubMission)
def read_submission(adsh: str):
    """
    Retrieve a single raw submission record by its accession number (adsh).
    """
    submission = services.get_submission_by_adsh(adsh)
    if submission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found.")
    return submission

@crud_router.put("/{adsh}", response_model=SubMission)
def update_existing_submission(adsh: str, submission_update: SubMissionUpdate):
    """
    Update a raw submission record. Only fields provided in the request body will be updated.
    """
    return services.update_submission(adsh, submission_update)

@crud_router.delete("/{adsh}")
def delete_existing_submission(adsh: str):
    """
    Delete a raw submission record by its accession number (adsh).
    """
    return services.delete_submission(adsh)

# Include all routers in the main application
app.include_router(main_router)
app.include_router(crud_router)