from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
import pandas as pd

# Global business day frequency
US_BDAY = CustomBusinessDay(calendar=USFederalHolidayCalendar())

def trading_days(start: str | pd.Timestamp, end: str | pd.Timestamp) -> pd.DatetimeIndex:
    """Return business/trading days between start and end, excluding weekends and US federal holidays."""
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=start, end=end)
    return pd.bdate_range(start=start, end=end).difference(holidays)
