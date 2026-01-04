import pandas as pd
from datetime import date

def validate_columns(df: pd.DataFrame) -> bool:
    """Check that required columns exist."""
    expected = {"Open", "High", "Low", "Close", "Volume"}
    return expected.issubset(df.columns)

def validate_index_monotonic(df: pd.DataFrame) -> bool:
    """Check that index is datetime and monotonic ascending."""
    return isinstance(df.index, pd.DatetimeIndex) and df.index.is_monotonic_increasing

def calculate_coverage(df: pd.DataFrame, start: date, end: date) -> float:
    """Return the fraction of expected business days covered by the DataFrame."""
    expected_days = pd.bdate_range(start=start, end=end)
    actual_days = pd.to_datetime(df.index).normalize().unique()
    return len(actual_days) / len(expected_days)

def detect_gaps(df: pd.DataFrame) -> list[date]:
    """Return a list of missing business dates."""
    expected_days = pd.bdate_range(start=df.index.min(), end=df.index.max())
    actual_days = pd.to_datetime(df.index).normalize().unique()
    missing = sorted(set(expected_days.date) - set(actual_days))
    return missing
