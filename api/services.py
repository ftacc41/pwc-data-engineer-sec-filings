import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from fastapi import HTTPException, status
import threading
import json
import requests
from sqlmodel import Session, select, func

from .api_schemas import (
    SubMission, SubMissionCreate, SubMissionUpdate, 
    CompanyTotal, SearchResult
)
from data_access.models import CompanyDim, FactFinancials
from . import config

# Raw Data (Bronze Layer) Service
BRONZE_SUB_CSV_PATH = Path("data/bronze/structured_filings/sub.csv")
csv_lock = threading.Lock()

def get_all_submissions(skip: int = 0, limit: int = 100) -> List[SubMission]:
    if not BRONZE_SUB_CSV_PATH.exists():
        return []
    df = pd.read_csv(BRONZE_SUB_CSV_PATH)
    paginated_df = df.iloc[skip : skip + limit]
    return [SubMission(**row) for row in paginated_df.to_dict(orient='records')]

def get_submission_by_adsh(adsh: str) -> Optional[SubMission]:
    if not BRONZE_SUB_CSV_PATH.exists():
        return None
    df = pd.read_csv(BRONZE_SUB_CSV_PATH)
    record = df[df['adsh'] == adsh]
    if record.empty:
        return None
    return SubMission(**record.iloc[0].to_dict())

def create_submissions(submissions: List[SubMissionCreate]) -> List[SubMission]:
    with csv_lock:
        df = pd.read_csv(BRONZE_SUB_CSV_PATH) if BRONZE_SUB_CSV_PATH.exists() else pd.DataFrame()
        new_records_df = pd.DataFrame([s.model_dump() for s in submissions])
        existing_adsh = df['adsh'].isin(new_records_df['adsh'])
        if existing_adsh.any():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="One or more submissions with these adsh values already exist.")
        df = pd.concat([df, new_records_df], ignore_index=True)
        df.to_csv(BRONZE_SUB_CSV_PATH, index=False)
    return [SubMission(**s.model_dump()) for s in submissions]

def update_submission(adsh: str, submission_update: SubMissionUpdate) -> SubMission:
    with csv_lock:
        if not BRONZE_SUB_CSV_PATH.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission file not found.")
        df = pd.read_csv(BRONZE_SUB_CSV_PATH)
        record_index = df.index[df['adsh'] == adsh].tolist()
        if not record_index:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Submission with adsh '{adsh}' not found.")
        update_data = submission_update.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            df.loc[record_index[0], key] = value
        df.to_csv(BRONZE_SUB_CSV_PATH, index=False)
        updated_record = df.iloc[record_index[0]]
        return SubMission(**updated_record.to_dict())

def delete_submission(adsh: str) -> Dict[str, str]:
    with csv_lock:
        if not BRONZE_SUB_CSV_PATH.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission file not found.")
        df = pd.read_csv(BRONZE_SUB_CSV_PATH)
        original_len = len(df)
        df = df[df['adsh'] != adsh]
        if len(df) == original_len:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Submission with adsh '{adsh}' not found.")
        df.to_csv(BRONZE_SUB_CSV_PATH, index=False)
    return {"message": f"Submission with adsh '{adsh}' deleted successfully."}

# Data Warehouse (Gold Layer) Service
def get_company_totals_from_db(limit: int, db: Session) -> List[CompanyTotal]:
    statement = (select(CompanyDim.name, func.sum(FactFinancials.value).label("total_value")).join(CompanyDim, FactFinancials.company_id == CompanyDim.id).group_by(CompanyDim.name).order_by(func.sum(FactFinancials.value).desc()).limit(limit))
    results = db.exec(statement).all()
    company_totals = [CompanyTotal(company_name=name, total_value=value) for name, value in results]
    return company_totals

# Vector Search (Typesense) Service
def perform_vector_search(q: str, form_type: Optional[str], k: int) -> List[SearchResult]:
    query_vector = config.EMBEDDING_MODEL.encode(q).tolist()
    vector_as_string = json.dumps(query_vector, separators=(',', ':'))
    
    url = f"http://{config.TYPESENSE_HOST}:{config.TYPESENSE_PORT}/multi_search"
    headers = { 'Content-Type': 'application/json', 'X-TYPESENSE-API-KEY': config.TYPESENSE_API_KEY }
    search_requests = {
        'searches': [{
            'collection': config.COLLECTION_NAME, 'q': '*',
            'vector_query': f"embedding:({vector_as_string}, k:{k})",
        }]
    }
    if form_type:
        search_requests['searches'][0]['filter_by'] = f'form:={form_type}'

    response = requests.post(url, headers=headers, json=search_requests)
    response.raise_for_status()
    search_results = response.json()
    
    hits = search_results['results'][0].get('hits', [])
    if not hits:
        return []

    results = [SearchResult(
        id=hit['document']['id'],
        cik=hit['document']['cik'],
        name=hit['document']['name'],
        form=hit['document']['form'],
        score=hit.get('vector_distance', 0.0)
    ) for hit in hits]
    
    return results