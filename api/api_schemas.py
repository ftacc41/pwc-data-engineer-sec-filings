from pydantic import BaseModel
from typing import List, Optional

class SearchResult(BaseModel):
    id: str
    cik: str
    name: str
    form: str
    score: float

class SearchResponse(BaseModel):
    results: List[SearchResult]

class CompanyTotal(BaseModel):
    company_name: str
    total_value: float

class CompanyTotalsResponse(BaseModel):
    results: List[CompanyTotal]

# --- ADD THESE NEW MODELS FOR CRUD ---

class SubMissionBase(BaseModel):
    """
    Domain Entity for a raw submission record.
    """
    adsh: str
    cik: int
    name: str
    form: str
    sic: int
    filing_summary: str

class SubMissionCreate(SubMissionBase):
    """
    The model used when creating a new submission via the API.
    """
    pass

class SubMission(SubMissionBase):
    """
    The model used when returning a submission from the API.
    """
    pass # For now, it's the same as the base, but could be extended later.

class SubMissionUpdate(BaseModel):
    """
    The model used when updating an existing submission. All fields are optional.
    """
    name: Optional[str] = None
    form: Optional[str] = None
    sic: Optional[int] = None
    filing_summary: Optional[str] = None