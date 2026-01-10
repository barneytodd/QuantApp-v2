import pandas as pd
from datetime import date

from app.core.dates import trading_days

def split_bdate_range(start: date, end: date) -> tuple[date, date] | None:
    bdays = trading_days(start, end)
    if len(bdays) <= 1:
        return None
    mid = bdays[len(bdays) // 2].date()
    return start, mid, (mid + pd.offsets.BDay(1)).date(), end