from typing import List, Optional
from sqlmodel import SQLModel, Field, Relationship

# --- Dimension Tables ---

class CompanyDim(SQLModel, table=True):
    __tablename__ = "company_dim"

    id: Optional[int] = Field(default=None, primary_key=True)
    cik: str = Field(index=True)
    name: str
    sic: Optional[str] = None
    country_of_incorporation: Optional[str] = None
    country_of_business: Optional[str] = None

    filings: List["FilingDim"] = Relationship(back_populates="company")
    facts: List["FactFinancials"] = Relationship(back_populates="company")

class FilingDim(SQLModel, table=True):
    __tablename__ = "filing_dim"

    id: Optional[int] = Field(default=None, primary_key=True)
    accession_number: str = Field(index=True)
    form_type: str
    period_of_report: Optional[str] = None
    date_filed: Optional[str] = None
    
    company_id: Optional[int] = Field(default=None, foreign_key="company_dim.id")
    company: Optional[CompanyDim] = Relationship(back_populates="filings")
    facts: List["FactFinancials"] = Relationship(back_populates="filing")

class MetricDim(SQLModel, table=True):
    __tablename__ = "metric_dim"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    tag: str = Field(index=True)
    version: str = Field(index=True)
    tlabel: Optional[str] = None
    datatype: Optional[str] = None
    iord: Optional[str] = None # "I" for Instant, "D" for Duration

    facts: List["FactFinancials"] = Relationship(back_populates="metric")

class StatementDim(SQLModel, table=True):
    __tablename__ = "statement_dim"

    id: Optional[int] = Field(default=None, primary_key=True)
    stmt: str = Field(index=True) # Statement type (e.g., IS, BS, CF)
    plabel: Optional[str] = None # The presented label

    facts: List["FactFinancials"] = Relationship(back_populates="statement")

class DateDim(SQLModel, table=True):
    __tablename__ = "date_dim"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    date: str = Field(index=True) # YYYY-MM-DD
    year: int
    month: int
    day: int
    quarter: int
    fiscal_year: Optional[int] = None
    
    facts: List["FactFinancials"] = Relationship(back_populates="date")


# --- Fact Table ---

class FactFinancials(SQLModel, table=True):
    __tablename__ = "fact_financials"

    id: Optional[int] = Field(default=None, primary_key=True)
    value: Optional[float] = None
    unit_of_measure: Optional[str] = None
    footnote: Optional[str] = None
    
    # Foreign Keys to all 5 dimensions
    company_id: Optional[int] = Field(default=None, foreign_key="company_dim.id")
    filing_id: Optional[int] = Field(default=None, foreign_key="filing_dim.id")
    metric_id: Optional[int] = Field(default=None, foreign_key="metric_dim.id")
    statement_id: Optional[int] = Field(default=None, foreign_key="statement_dim.id")
    date_id: Optional[int] = Field(default=None, foreign_key="date_dim.id")

    company: Optional[CompanyDim] = Relationship(back_populates="facts")
    filing: Optional["FilingDim"] = Relationship(back_populates="facts") # Fix here
    metric: Optional[MetricDim] = Relationship(back_populates="facts")
    statement: Optional[StatementDim] = Relationship(back_populates="facts")
    date: Optional[DateDim] = Relationship(back_populates="facts")