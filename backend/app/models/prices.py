from datetime import datetime
from pydantic import BaseModel, Field

class PriceDataRow(BaseModel):
    symbol: str = Field(..., max_length=32)
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

