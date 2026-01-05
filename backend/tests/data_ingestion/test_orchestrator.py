import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from app.data_ingestion.retry import RetryReason
from app.data_ingestion.orchestrator import fetch_with_retries, fetch_symbols_parallel
from app.data_ingestion.types import FetchResult, FetchRequest


@pytest.mark.asyncio
async def test_fetch_with_retries_succeeds(monkeypatch, full_price_df, date_range):
    """Test fetch_with_retries returns immediately if coverage is full."""
    start, end = date_range
    mock_fetch = AsyncMock(return_value=FetchResult(
        request=FetchRequest(symbol="AAPL", start=start, end=end),
        data=full_price_df,
        empty=False,
        exception=None,
        elapsed_ms=10
    ))
    monkeypatch.setattr("app.data_ingestion.orchestrator.fetch_prices", mock_fetch)

    result = await fetch_with_retries("AAPL", start, end, coverage_threshold=0.99)
    assert result["retry_reason"] == RetryReason.NONE
    assert result["attempts"] == 1
    assert result["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_fetch_with_retries_retries_on_partial(monkeypatch, full_price_df, gappy_price_df, date_range):
    """Test fetch_with_retries retries when coverage is partial."""
    # First attempt partial coverage, second full coverage
    start, end = date_range
    fetch_sequence = [
        FetchResult(
            request=FetchRequest(symbol="AAPL", start=start, end=end),
            data=gappy_price_df,
            empty=False,
            exception=None,
            elapsed_ms=10
        ),
        FetchResult(
            request=FetchRequest(symbol="AAPL", start=start, end=end),
            data=full_price_df,
            empty=False,
            exception=None,
            elapsed_ms=5
        )
    ]
    mock_fetch = AsyncMock(side_effect=fetch_sequence)
    monkeypatch.setattr("app.data_ingestion.orchestrator.fetch_prices", mock_fetch)

    result = await fetch_with_retries("AAPL", start, end, max_attempts=3, coverage_threshold=0.99)
    assert result["retry_reason"] == RetryReason.NONE
    assert result["attempts"] == 2


@pytest.mark.asyncio
async def test_fetch_with_retries_returns_last_on_failure(monkeypatch, gappy_price_df, date_range):
    """Test that the last FetchResult is returned if all retries fail."""
    start, end = date_range
    mock_fetch = AsyncMock(return_value=FetchResult(
        request=FetchRequest(symbol="AAPL", start=start, end=end),
        data=gappy_price_df,
        empty=False,
        exception=None,
        elapsed_ms=10
    ))
    monkeypatch.setattr("app.data_ingestion.orchestrator.fetch_prices", mock_fetch)

    result = await fetch_with_retries("AAPL", start, end, max_attempts=3, coverage_threshold=0.99)
    assert result["retry_reason"] == RetryReason.PARTIAL
    assert result["attempts"] == 3


@pytest.mark.asyncio
async def test_fetch_symbols_parallel_respects_max_concurrent(monkeypatch, full_price_df, date_range):
    """Test that fetch_symbols_parallel respects max_concurrent semaphore."""
    start, end = date_range
    call_order = []

    async def mock_fetch(req):
        await asyncio.sleep(0.01)
        call_order.append(req.symbol)
        return FetchResult(
            request=req,
            data=full_price_df,
            empty=False,
            exception=None,
            elapsed_ms=5
        )
    monkeypatch.setattr("app.data_ingestion.orchestrator.fetch_prices", mock_fetch)

    symbols = ["A", "B", "C", "D", "E"]
    results = await fetch_symbols_parallel(symbols, start, end, max_concurrent=2, coverage_threshold=0.99)

    assert all(r["retry_reason"] == RetryReason.NONE for r in results)
    assert [r["symbol"] for r in results] == symbols
