from datetime import date
from pydantic import BaseModel, Field, field_validator
from typing import List, Literal, Optional

class FetchPricesRequest(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    start: date
    end: date
    interval: Literal["1d", "1h"] = "1d"
    coverage_threshold: float = Field(0.95, ge=0.0, le=1.0)
    max_attempts: int = Field(3, ge=1, le=10)
    max_concurrent: int = Field(5, ge=1, le=20)

    model_config = {
        "json_schema_extra": {
            "example": {
                "symbols": ["AAPL", "MSFT"],
                "start": "2023-01-01",
                "end": "2023-12-31",
                "interval": "1d",
                "coverage_threshold": 0.95,
                "max_attempts": 3,
                "max_concurrent": 5
            }
        }
    }

    @field_validator("symbols")
    def normalize_symbols(cls, v: List[str]) -> List[str]:
        normalized = [s.strip().upper() for s in v if s.strip()]
        if not normalized:
            raise ValueError("symbols list cannot be empty after normalization")
        return normalized

    @field_validator("end")
    def validate_date_range(cls, v: Optional[date], info) -> Optional[date]:
        start = info.data.get("start")
        if start and v and v < start:
            raise ValueError("end date must be >= start date")
        return v


class GetPricesPayload(BaseModel):
    """
    Payload for querying stored OHLCV data from the database.
    This endpoint NEVER fetches data externally.
    """
    symbols: List[str] = Field(
        ...,
        min_length=1,
        description="List of ticker symbols to query"
    )
    start: Optional[date] = Field(
        default=None,
        description="Optional start date (inclusive)"
    )
    end: Optional[date] = Field(
        default=None,
        description="Optional end date (inclusive)"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "symbols": ["AAPL", "MSFT"],
                "start": "2023-01-01",
                "end": "2023-12-31"
            }
        }
    }

    @field_validator("symbols")
    def normalize_symbols(cls, v: List[str]) -> List[str]:
        normalized = [s.strip().upper() for s in v if s.strip()]
        if not normalized:
            raise ValueError("symbols list cannot be empty after normalization")
        return normalized

    @field_validator("end")
    def validate_date_range(cls, v: Optional[date], info) -> Optional[date]:
        start = info.data.get("start")
        if start and v and v < start:
            raise ValueError("end date must be >= start date")
        return v
