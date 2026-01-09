import pandas as pd
from datetime import date

def calculate_coverage(df: pd.DataFrame, start: date, end: date) -> float:
    """Return the fraction of expected business days covered by the DataFrame."""
    expected_days = pd.bdate_range(start=start, end=end)
    actual_days = pd.to_datetime(df.index).normalize().unique()
    return len(actual_days) / len(expected_days)

def detect_gaps(df: pd.DataFrame, start: date, end: date) -> list[date]:
    """Return a list of missing business dates."""
    df_index = pd.to_datetime(df.index)
    df_dates = df_index.normalize().to_pydatetime()  
    df_dates = [d.date() for d in df_dates]  

    # Expected business days
    expected_days = pd.bdate_range(start, end).to_pydatetime()
    expected_dates = [d.date() for d in expected_days]

    # Compute missing
    missing = sorted(set(expected_dates) - set(df_dates))
    return missing
