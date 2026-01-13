# test_queries.py
import pytest
from datetime import date

import app.data_validation.queries as queries


@pytest.mark.asyncio
async def test_get_symbol_summary(mock_validation_queries, symbol, date_range):
    summary = await queries.get_symbol_summary(symbol, date_range["start"], date_range["end"])
    assert summary["first_date"] == date(2024, 1, 2)
    assert summary["last_date"] == date(2024, 1, 10)
    assert summary["observed_days"] == 7


@pytest.mark.asyncio
async def test_get_existing_dates(mock_validation_queries, symbol, date_range):
    existing_dates = await queries.get_existing_dates(symbol, date_range["start"], date_range["end"])
    expected_dates = [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2024, 1, 9),
        date(2024, 1, 10),
    ]
    assert existing_dates == expected_dates


@pytest.mark.asyncio
async def test_get_missing_ohlcv_dates(mock_validation_queries, symbol, date_range):
    missing_dates = await queries.get_missing_ohlcv_dates(symbol, date_range["start"], date_range["end"])
    expected_dates = [
        date(2024, 1, 3),
        date(2024, 1, 5),
    ]
    assert missing_dates == expected_dates


@pytest.mark.asyncio
async def test_get_suspicious_price_dates(mock_validation_queries, symbol, date_range):
    suspicious_dates = await queries.get_suspicious_price_dates(symbol, date_range["start"], date_range["end"])
    expected_dates = [
        date(2024, 1, 4),
    ]
    assert suspicious_dates == expected_dates
