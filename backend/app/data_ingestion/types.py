from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence
import pandas as pd

@dataclass(slots=True)
class FetchRequest:
    symbols: Sequence[str]
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
