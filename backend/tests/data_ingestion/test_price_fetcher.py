import pandas as pd
import pytest
from datetime import date
from unittest.mock import patch

from app.data_ingestion.fetchers.prices import fetch_prices, _download_sync
from app.data_ingestion.types import FetchRequest, FetchResult


@pytest.mark.asyncio
async def test_fetch_prices_success(monkeypatch):
    """Test fetch_prices returns a valid FetchResult with data."""
    req = FetchRequest(symbol="AAPL", start=date(2023,1,2), end=date(2023,1,3))
    # Fake DataFrame to return
    fake_df = pd.DataFrame({"Open":[100], "Close":[101]}, index=pd.to_datetime(["2023-01-02"]))

    # Patch _download_sync to return the fake df
    monkeypatch.setattr("app.data_ingestion.fetchers.prices._download_sync", lambda r: fake_df)

    result = await fetch_prices(req)
    assert isinstance(result, FetchResult)
    assert result.data.equals(fake_df)
    assert result.empty is False
    assert result.exception is None
    assert result.elapsed_ms >= 0


@pytest.mark.asyncio
async def test_fetch_prices_empty(monkeypatch):
    """Test fetch_prices when download returns empty DataFrame."""
    req = FetchRequest(symbol="AAPL", start=date(2023,1,2), end=date(2023,1,3))
    monkeypatch.setattr("app.data_ingestion.fetchers.prices._download_sync", lambda r: pd.DataFrame())
    
    result = await fetch_prices(req)
    assert result.empty is True
    assert result.data.empty
    assert result.exception is None


@pytest.mark.asyncio
async def test_fetch_prices_exception(monkeypatch):
    """Test fetch_prices handles exceptions correctly."""
    req = FetchRequest(symbol="AAPL", start=date(2023,1,2), end=date(2023,1,3))

    def raise_error(req):
        raise RuntimeError("network failure")

    monkeypatch.setattr("app.data_ingestion.fetchers.prices._download_sync", raise_error)

    result = await fetch_prices(req)
    assert result.empty is True
    assert result.data is None
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "network failure"
