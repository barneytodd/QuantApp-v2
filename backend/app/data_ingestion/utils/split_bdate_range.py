import pandas as pd
from datetime import date

def split_bdate_range(start: date, end: date) -> tuple[date, date] | None:
    bdays = pd.bdate_range(start, end)
    if len(bdays) <= 1:
        return None
    mid = bdays[len(bdays) // 2].date()
    return start, mid, (mid + pd.offsets.BDay(1)).date(), end