from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field

from app.data_ingestion.models import RetryReason


class IngestPricesRequest(BaseModel):
    """
    Payload for requesting ingestion of OHLCV data for one or more symbols.
    """

    symbols: List[str] = Field(..., min_length=1, description="List of ticker symbols to ingest")
    start: date = Field(..., description="Start date of the ingestion window (inclusive)")
    end: date = Field(..., description="End date of the ingestion window (inclusive)")
    interval: str = Field(default="1d", description="Price interval ('1d', '1h', etc.)")
    max_attempts: int = Field(default=3, ge=1, description="Maximum number of fetch attempts per symbol/missing range")
    max_concurrent: int = Field(default=5, ge=1, description="Maximum number of symbols to fetch concurrently")
    coverage_threshold: float = Field(default=0.95, ge=0.0, le=1.0, description="Minimum coverage required to consider a fetch successful")
    dry_run: bool = Field(default=False, description="If True, fetch but do not insert into the database")

    model_config = {
        "json_schema_extra": {
            "example": {
                "symbols": ["AAPL", "MSFT"],
                "start": "2023-01-01",
                "end": "2023-12-31",
                "interval": "1d",
                "max_attempts": 3,
                "max_concurrent": 5,
                "coverage_threshold": 0.95,
                "dry_run": False
            }
        }
    }


class IngestSymbolResult(BaseModel):
    """
    Aggregated ingestion result for a single symbol.
    """

    symbol: str
    success: bool
    attempts: int
    retry_reason: RetryReason
    coverage: Optional[float] = None
    missing_dates: List[date] = Field(default_factory=list)
    rows_fetched: int
    rows_inserted: int
    elapsed_ms: int
    error: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "symbol": "AAPL",
                "success": True,
                "attempts": 2,
                "retry_reason": "none",
                "coverage": 1.0,
                "missing_dates": [],
                "rows_fetched": 252,
                "rows_inserted": 252,
                "elapsed_ms": 1345,
                "error": None
            }
        }
    }


class IngestPricesResponse(BaseModel):
    """
    Response returned after an ingestion request.
    """

    start: date
    end: date
    interval: str
    dry_run: bool
    total_symbols: int
    succeeded: int
    failed: int
    results: List[IngestSymbolResult]

    model_config = {
        "json_schema_extra": {
            "example": {
                "start": "2023-01-01",
                "end": "2023-12-31",
                "interval": "1d",
                "dry_run": False,
                "total_symbols": 2,
                "succeeded": 2,
                "failed": 0,
                "results": [
                    {
                        "symbol": "AAPL",
                        "success": True,
                        "attempts": 2,
                        "retry_reason": "none",
                        "coverage": 1.0,
                        "missing_dates": [],
                        "rows_fetched": 252,
                        "rows_inserted": 252,
                        "elapsed_ms": 1345,
                        "error": None
                    },
                    {
                        "symbol": "MSFT",
                        "success": True,
                        "attempts": 1,
                        "retry_reason": "none",
                        "coverage": 1.0,
                        "missing_dates": [],
                        "rows_fetched": 252,
                        "rows_inserted": 252,
                        "elapsed_ms": 1102,
                        "error": None
                    }
                ]
            }
        }
    }
