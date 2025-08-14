from typing import Optional
from sqlmodel import SQLModel, Field, Relationship

# --- Dimension Models ---

class CompanyDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    cik: str = Field(index=True)
    name: str
    sic: Optional[str] = None
    country_of_incorporation: Optional[str] = None
    country_of_business: Optional[str] = None

    filings: list["FilingDim"] = Relationship(back_populates="company")

class FilingDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    accession_number: str = Field(index=True)
    form_type: str
    period_of_report: Optional[str] = None
    date_filed: Optional[str] = None

    company_id: Optional[int] = Field(default=None, foreign_key="companydim.id")
    company: CompanyDim = Relationship(back_populates="filings")

class TagDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tag: str
    version: str
    custom: int
    label: str

class DateDim(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    date_key: str = Field(index=True)
    
# --- Fact Model ---

class FactFinancials(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    value: float
    
    # Foreign Keys
    filing_id: Optional[int] = Field(default=None, foreign_key="filingdim.id")
    company_id: Optional[int] = Field(default=None, foreign_key="companydim.id")
    tag_id: Optional[int] = Field(default=None, foreign_key="tagdim.id")
    date_id: Optional[int] = Field(default=None, foreign_key="datedim.id")