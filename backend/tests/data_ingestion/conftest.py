import pytest
import pandas as pd
from datetime import date

from app.core.dates import trading_days

@pytest.fixture
def date_range():
    """
    Standard test date range (business days).
    """
    return date(2023, 1, 2), date(2023, 1, 10)


@pytest.fixture
def full_price_df():
    """
    Perfect OHLCV dataframe with no gaps.
    """
    idx = trading_days("2023-01-02", "2023-01-10")

    columns = pd.MultiIndex.from_product(
        [["AAPL"], ["Open", "High", "Low", "Close", "Volume"]],
        names=["Ticker", "OHLCV"]
    )

    data = [
        [100.0, 101.0, 99.0, 100.5, 1000] for _ in range(len(idx))
    ]

    df = pd.DataFrame(data, index=idx, columns=columns)
    return df


@pytest.fixture
def gappy_price_df(full_price_df):
    """
    Price dataframe missing one business day.
    """
    return full_price_df.drop(full_price_df.index[2])
