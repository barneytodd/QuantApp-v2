from pydantic import BaseModel
from datetime import date
from typing import List, Optional

class ValidationIssueRow(BaseModel):
    code: str
    description: str
    affected_dates: Optional[List[date]] = None
    count: int

class ValidationResultRow(BaseModel):
    symbol: str
    start: date
    end: date
    coverage: float
    first_date: Optional[date] = None
    last_date: Optional[date] = None
    issues: List[ValidationIssueRow]

class ValidatePricesRequest(BaseModel):
    symbols: List[str]
    start: date
    end: date
