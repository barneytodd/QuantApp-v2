from datetime import date
from typing import List
import pandas as pd

from app.core.dates import trading_days

def get_missing_date_ranges(existing_dates: pd.DatetimeIndex, start: date, end: date) -> List[tuple[date, date]]:
    """
    Compute contiguous missing ranges in the DB using business days.
    Returns list of (range_start, range_end), where ranges are contiguous trading days.
    """
    # full expected business dates
    all_bdays = trading_days(start, end)
    
    # missing dates
    missing_dates = sorted(set(all_bdays.date) - set(d.date() for d in existing_dates))
    if not missing_dates:
        return []

    ranges = []
    range_start = missing_dates[0]
    prev_date = range_start

    for d in missing_dates[1:]:
        # check if current date is the **next business day** after prev_date
        next_bday = trading_days(prev_date, d)[1].date() if len(trading_days(prev_date, d)) > 1 else None
        if d != next_bday:
            ranges.append((range_start, prev_date))
            range_start = d
        prev_date = d

    ranges.append((range_start, prev_date))
    return ranges
