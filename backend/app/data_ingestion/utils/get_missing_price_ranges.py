from datetime import date, timedelta
from typing import List
import pandas as pd


def get_missing_date_ranges(existing_dates: pd.DatetimeIndex, start: date, end: date) -> List[tuple[date, date]]:
    """
    Compute contiguous date ranges between start and end that are missing in the DB.
    Returns list of (range_start, range_end).
    """
    all_dates = pd.bdate_range(start, end)
    missing_dates = sorted(set(all_dates.date) - set(d.date() for d in existing_dates))
    if not missing_dates:
        return []

    ranges = []
    range_start = missing_dates[0]
    prev_date = range_start
    for d in missing_dates[1:]:
        if d != prev_date + timedelta(days=1):
            ranges.append((range_start, prev_date))
            range_start = d
        prev_date = d
    ranges.append((range_start, prev_date))
    return ranges