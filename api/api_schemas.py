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

class SubMissionBase(BaseModel):
    adsh: str
    cik: int
    name: str
    form: str
    sic: int
    filing_summary: str

class SubMissionCreate(SubMissionBase):
    pass

class SubMission(SubMissionBase):
    pass

class SubMissionUpdate(BaseModel):
    name: Optional[str] = None
    form: Optional[str] = None
    sic: Optional[int] = None
    filing_summary: Optional[str] = None