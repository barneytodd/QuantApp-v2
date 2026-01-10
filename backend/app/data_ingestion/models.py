import pandas as pd
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional


@dataclass(slots=True)
class FetchRequest:
    symbol: str
    start: date
    end: date
    interval: str = "1d"
    auto_adjust: bool = True


@dataclass(slots=True)
class FetchResult:
    request: FetchRequest
    data: Optional[pd.DataFrame]      
    empty: bool
    exception: Optional[Exception]
    elapsed_ms: int


@dataclass(slots=True)
class PriceInsertRow:
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float


class RetryReason(str, Enum):
    NONE = "none"
    EMPTY = "empty_data"
    PARTIAL = "partial_coverage"
    EXCEPTION = "fetch_exception"
