import pytest
from datetime import date
from unittest.mock import AsyncMock

import app.data_validation.queries as queries


@pytest.fixture
def date_range():
    return {
        "start": date(2024, 1, 1),
        "end": date(2024, 1, 10),
    }


@pytest.fixture
def symbol():
    return "AAPL"


@pytest.fixture
def mock_validation_queries(monkeypatch):
    # Mock get_symbol_summary (still returns a dict)
    monkeypatch.setattr(
        queries,
        "get_symbol_summary",
        AsyncMock(return_value={
            "first_date": date(2024, 1, 2),
            "last_date": date(2024, 1, 10),
            "observed_days": 7,
        })
    )

    # Mock get_existing_dates
    monkeypatch.setattr(
        queries,
        "get_existing_dates",
        AsyncMock(return_value=[
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
            date(2024, 1, 8),
            date(2024, 1, 9),
            date(2024, 1, 10),
        ])
    )

    # Mock missing OHLCV dates (list of dates)
    monkeypatch.setattr(
        queries,
        "get_missing_ohlcv_dates",
        AsyncMock(return_value=[
            date(2024, 1, 3),
            date(2024, 1, 5),
        ])
    )

    # Mock suspicious price dates (list of dates)
    monkeypatch.setattr(
        queries,
        "get_suspicious_price_dates",
        AsyncMock(return_value=[
            date(2024, 1, 4),
        ])
    )
