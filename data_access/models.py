from typing import Optional
from sqlmodel import SQLModel, Field

class CompanyDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    cik: str
    company_name: str
    industry: Optional[str]
    sic: Optional[int]

class FilingDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    accession_number: str
    form_type: str
    filing_date: Optional[str]
    filing_path: Optional[str]

class PeriodDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    period_start: Optional[str]
    period_end: Optional[str]
    fiscal_year: Optional[int]

class MetricDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    metric_name: str
    metric_description: Optional[str]

class CalendarDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    date: str
    year: int
    month: int
    day: int
    is_quarter_end: bool = False

class FactFinancials(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    company_id: int
    filing_id: int
    period_id: int
    metric_id: int
    calendar_id: int
    value: float
