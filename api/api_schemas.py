from pydantic import BaseModel
from typing import List

class SearchResult(BaseModel):
    id: str
    cik: str
    name: str
    form: str
    score: float

class SearchResponse(BaseModel):
    results: List[SearchResult]