from typing import Optional, List
from sqlmodel import SQLModel, Field, Relationship
from datetime import datetime

# --- Dimension Models ---

class CompanyDim(SQLModel, table=True):
    __tablename__ = 'companydim'
    
    # This is now the Surrogate Key
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # This is now the Natural Key, which can have duplicates
    cik: str = Field(index=True)
    
    name: str
    sic: Optional[str] = None
    
    # --- NEW SCD Type 2 Columns ---
    valid_from: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    valid_to: datetime = Field(default=datetime(9999, 12, 31), nullable=False)
    is_current: bool = Field(default=True, nullable=False, index=True)

    # We remove the country fields for simplicity as they are not in our source data

class FilingDim(SQLModel, table=True):
    __tablename__ = 'filingdim'
    id: Optional[int] = Field(default=None, primary_key=True)
    accession_number: str = Field(unique=True, index=True) # adsh
    form_type: str
    period_of_report: Optional[str] = None
    date_filed: Optional[str] = None

class TagDim(SQLModel, table=True):
    __tablename__ = 'tagdim'
    id: Optional[int] = Field(default=None, primary_key=True)
    tag: str = Field(unique=True) # The specific financial metric
    version: str
    custom: int
    label: str

class DateDim(SQLModel, table=True):
    __tablename__ = 'datedim'
    id: Optional[int] = Field(default=None, primary_key=True)
    date_key: str = Field(unique=True, index=True) # YYYYMMDD format

# --- NEW 5th Dimension ---
class StatementDim(SQLModel, table=True):
    __tablename__ = 'statementdim'
    id: Optional[int] = Field(default=None, primary_key=True)
    statement_code: str = Field(unique=True) # e.g., 'IS', 'BS', 'CF'
    statement_name: str # e.g., 'Income Statement', 'Balance Sheet'

# --- Fact Model ---

class FactFinancials(SQLModel, table=True):
    __tablename__ = 'factfinancials'
    id: Optional[int] = Field(default=None, primary_key=True)
    value: float
    
    # Foreign Keys to all 5 Dimensions
    filing_id: Optional[int] = Field(default=None, foreign_key="filingdim.id")
    company_id: Optional[int] = Field(default=None, foreign_key="companydim.id")
    tag_id: Optional[int] = Field(default=None, foreign_key="tagdim.id")
    date_id: Optional[int] = Field(default=None, foreign_key="datedim.id")
    statement_id: Optional[int] = Field(default=None, foreign_key="statementdim.id")