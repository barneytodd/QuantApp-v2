import pandas as pd
import pytest
from datetime import date
from unittest.mock import AsyncMock

from app.core.dates import trading_days
from app.data_ingestion.orchestrator import fetch_missing_prices, fetch_symbols_parallel, orchestrate_fetch_and_insert
from app.data_ingestion.models import FetchRequest, FetchResult


@pytest.mark.asyncio
async def test_fetch_missing_prices_only_missing(monkeypatch, full_price_df):
    start = date(2023, 1, 2)
    end = date(2023, 1, 10)
    symbol = "AAPL"

    # DB already has first half
    existing_keys = {
        (symbol, d.date())
        for d in trading_days("2023-01-06", "2023-01-09")
    }

    monkeypatch.setattr(
        "app.data_ingestion.orchestrator.get_price_keys",
        AsyncMock(return_value=existing_keys),
    )

    # Expect fetch only for missing range
    mock_fetch = AsyncMock(
        return_value={
            "symbol": symbol,
            "result": FetchResult(
                request=FetchRequest(symbol, start, end),
                data=full_price_df,
                empty=False,
                exception=None,
                elapsed_ms=5,
            ),
            "attempts": 1,
            "retry_reason": None,
            "coverage": 1.0,
            "missing_dates": [],
            "elapsed_ms": 5,
        }
    )

    monkeypatch.setattr(
        "app.data_ingestion.orchestrator.fetch_with_retries",
        mock_fetch,
    )

    results = await fetch_missing_prices(
        symbol, start, end, interval="1d", max_attempts=3
    )

    calls = mock_fetch.call_args_list

    assert len(calls) == 2

    # First missing range: Mon-Thu
    assert calls[0].args[1] == date(2023, 1, 3)
    assert calls[0].args[2] == date(2023, 1, 5)

    # Second missing range: Tue only
    assert calls[1].args[1] == date(2023, 1, 10)
    assert calls[1].args[2] == date(2023, 1, 10)


@pytest.mark.asyncio
async def test_fetch_symbols_parallel_multiple(monkeypatch):
    start = date(2023, 1, 2)
    end = date(2023, 1, 10)

    mock_missing = AsyncMock(
        side_effect=[
            [{"symbol": "A", "result": "R1"}],
            [{"symbol": "B", "result": "R2"}],
        ]
    )

    monkeypatch.setattr(
        "app.data_ingestion.orchestrator.fetch_missing_prices",
        mock_missing,
    )

    results = await fetch_symbols_parallel(
        ["A", "B"], start, end, max_concurrent=2
    )

    assert len(results) == 2
    assert results[0][0]["symbol"] == "A"
    assert results[1][0]["symbol"] == "B"


@pytest.mark.asyncio
async def test_orchestrator_inserts_only_missing(monkeypatch, full_price_df):
    start = date(2023, 1, 2)
    end = date(2023, 1, 10)

    mock_fetch_results = [
        [
            {
                "symbol": "AAPL",
                "result": FetchResult(
                    request=FetchRequest("AAPL", start, end),
                    data=full_price_df,
                    empty=False,
                    exception=None,
                    elapsed_ms=5,
                ),
            }
        ]
    ]

    monkeypatch.setattr(
        "app.data_ingestion.orchestrator.fetch_symbols_parallel",
        AsyncMock(return_value=mock_fetch_results),
    )

    mock_insert = AsyncMock(return_value={"AAPL": len(full_price_df)})
    monkeypatch.setattr(
        "app.data_ingestion.orchestrator.bulk_insert_prices_chunked",
        mock_insert,
    )

    monkeypatch.setattr(
        "app.data_ingestion.orchestrator.get_price_keys",
        AsyncMock(return_value=[("AAPL", pd.Timestamp("2023-01-02"))]),
    )

    inserted, results = await orchestrate_fetch_and_insert(
        ["AAPL"], start, end
    )

    assert inserted["AAPL"] == len(full_price_df)
    assert mock_insert.call_count == 1