import pytest
import pandas as pd
from datetime import date


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
    idx = pd.bdate_range("2023-01-02", "2023-01-10")

    return pd.DataFrame(
        {
            "Open": 100.0,
            "High": 101.0,
            "Low": 99.0,
            "Close": 100.5,
            "Volume": 1000,
        },
        index=idx,
    )


@pytest.fixture
def gappy_price_df(full_price_df):
    """
    Price dataframe missing one business day.
    """
    return full_price_df.drop(full_price_df.index[2])
