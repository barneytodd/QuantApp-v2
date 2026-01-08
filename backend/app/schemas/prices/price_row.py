from datetime import datetime
from pydantic import BaseModel, Field


class PriceDataRow(BaseModel):
    symbol: str = Field(..., max_length=32, description="Ticker symbol")
    date: datetime = Field(..., description="Timestamp of the price data")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    close: float = Field(..., description="Closing price")
    volume: float = Field(..., description="Volume traded")

    model_config = {
        "json_schema_extra": {
            "example": {
                "symbol": "AAPL",
                "date": "2023-01-03T00:00:00Z",
                "open": 130.92,
                "high": 131.45,
                "low": 129.85,
                "close": 131.01,
                "volume": 85000000
            }
        }
    }
