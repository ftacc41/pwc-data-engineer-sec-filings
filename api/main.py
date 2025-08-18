from typing import List, Optional
import secrets
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Security, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlmodel import Session

from api.api_schemas import (
    SearchResponse, CompanyTotalsResponse, SubMission, SubMissionCreate, SubMissionUpdate
)
from data_access.db import engine
from api import services
import os

# --- INITIALIZATION ---
app = FastAPI(
    title="SEC Filings API",
    description="API for querying and searching SEC financial documents.",
    version="1.0.0",
)
security = HTTPBasic()

# --- DEPENDENCIES ---
def get_db_session():
    with Session(engine) as session:
        yield session

def check_auth(credentials: HTTPBasicCredentials = Security(security)):
    # Logic is now in the service layer, but we keep the dependency here for FastAPI integration
    # A more advanced refactor could move this, but this is clear and effective.
    # We will just reuse the code here for simplicity as it's not complex business logic.
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
main_router = APIRouter()

@main_router.get("/", tags=["Status"])
def read_root(username: str = Depends(check_auth)):
    return {"message": f"Welcome, {username}! The SEC Filings API is running."}

@main_router.get("/search", response_model=SearchResponse, tags=["Search"])
def vector_search(q: str, form_type: Optional[str] = None, k: int = 10, username: str = Depends(check_auth)):
    try:
        results = services.perform_vector_search(q=q, form_type=form_type, k=k)
        return SearchResponse(results=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@main_router.get("/query/company-totals", response_model=CompanyTotalsResponse, tags=["Database Queries"])
def get_company_totals(limit: int = 10, db: Session = Depends(get_db_session), username: str = Depends(check_auth)):
    results = services.get_company_totals_from_db(limit=limit, db=db)
    return CompanyTotalsResponse(results=results)

crud_router = APIRouter(prefix="/raw/submissions", tags=["Raw Data CRUD"], dependencies=[Depends(check_auth)])

@crud_router.post("/", response_model=List[SubMission], status_code=status.HTTP_201_CREATED)
def create_new_submissions(submissions: List[SubMissionCreate]):
    return services.create_submissions(submissions)

@crud_router.get("/", response_model=List[SubMission])
def read_all_submissions(skip: int = 0, limit: int = 100):
    return services.get_all_submissions(skip=skip, limit=limit)

@crud_router.get("/{adsh}", response_model=SubMission)
def read_submission(adsh: str):
    submission = services.get_submission_by_adsh(adsh)
    if submission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found.")
    return submission

@crud_router.put("/{adsh}", response_model=SubMission)
def update_existing_submission(adsh: str, submission_update: SubMissionUpdate):
    return services.update_submission(adsh, submission_update)

@crud_router.delete("/{adsh}")
def delete_existing_submission(adsh: str):
    return services.delete_submission(adsh)

# Include all routers in the main application
app.include_router(main_router)
app.include_router(crud_router)