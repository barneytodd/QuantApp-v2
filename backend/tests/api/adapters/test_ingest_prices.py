# test_adapters_orchestration.py
import pytest
from datetime import date

from app.api.routes.data.adapters import adapt_orchestration_result
from app.data_ingestion.models import RetryReason
from app.schemas import IngestPricesResponse, IngestSymbolResult


def make_mock_result(symbol, attempts=1, elapsed_ms=100, retry_reason=RetryReason.NONE,
                     coverage=1.0, missing_dates=None, data_length=5, exception=None):
    """Helper to generate a mock fetch result dict."""
    class MockResult:
        def __init__(self, data, exception=None):
            self.data = data
            self.exception = exception

    return {
        "symbol": symbol,
        "attempts": attempts,
        "elapsed_ms": elapsed_ms,
        "retry_reason": retry_reason,
        "coverage": coverage,
        "missing_dates": missing_dates or [],
        "result": MockResult(data=[{}] * data_length if data_length else None, exception=exception)
    }


def test_adapt_orchestration_result_basic():
    symbols = ["AAPL", "GOOG", "MSFT"]
    start = date(2024, 1, 1)
    end = date(2024, 1, 5)
    interval = "1d"
    dry_run = False
    rows_inserted = {"AAPL": 5, "GOOG": 3, "MSFT": 0}

    fetch_results = [
        [
            make_mock_result("AAPL"),
            make_mock_result("AAPL", attempts=2, elapsed_ms=50, missing_dates=[date(2024,1,3)])
        ],
        [make_mock_result("GOOG", coverage=0.8)],
        []  # MSFT has no fetch attempts
    ]

    response: IngestPricesResponse = adapt_orchestration_result(
        symbols=symbols,
        start=start,
        end=end,
        interval=interval,
        dry_run=dry_run,
        fetch_results=fetch_results,
        rows_inserted=rows_inserted
    )

    # Top-level checks
    assert response.start == start
    assert response.end == end
    assert response.interval == interval
    assert response.dry_run == dry_run
    assert response.total_symbols == 3
    assert response.succeeded == 2
    assert response.failed == 1

    # Check AAPL
    aapl = next(r for r in response.results if r.symbol == "AAPL")
    assert aapl.attempts == 3
    assert aapl.elapsed_ms == 150  # 100 + 50
    assert aapl.retry_reason == RetryReason.NONE
    assert aapl.coverage == 1.0
    assert aapl.missing_dates == [date(2024,1,3)]
    assert aapl.rows_fetched == 10  # 5 + 5
    assert aapl.rows_inserted == 5
    assert aapl.success is True
    assert aapl.error is None

    # Check GOOG
    goog = next(r for r in response.results if r.symbol == "GOOG")
    assert goog.coverage == 0.8
    assert goog.rows_fetched == 5
    assert goog.rows_inserted == 3
    assert goog.success is True

    # Check MSFT (no fetch attempts)
    msft = next(r for r in response.results if r.symbol == "MSFT")
    assert msft.success is False
    assert msft.attempts == 0
    assert msft.retry_reason == RetryReason.EXCEPTION
    assert msft.rows_fetched == 0
    assert msft.rows_inserted == 0
    assert msft.error == "No fetch attempts executed"


def test_adapt_orchestration_result_with_exception():
    symbols = ["AAPL"]
    start = date(2024, 1, 1)
    end = date(2024, 1, 2)
    interval = "1d"
    dry_run = True
    rows_inserted = {"AAPL": 0}

    fetch_results = [
        [make_mock_result("AAPL", exception=ValueError("Fetch failed"))]
    ]

    response = adapt_orchestration_result(
        symbols=symbols,
        start=start,
        end=end,
        interval=interval,
        dry_run=dry_run,
        fetch_results=fetch_results,
        rows_inserted=rows_inserted
    )

    aapl = response.results[0]
    assert aapl.success is False
    assert aapl.error == "Fetch failed"
    assert aapl.rows_fetched == 5
    assert aapl.retry_reason == RetryReason.NONE


def test_adapt_orchestration_partial_fetch():
    symbols = ["AAPL"]
    start = date(2024,1,1)
    end = date(2024,1,3)
    interval = "1d"
    dry_run = False
    rows_inserted = {"AAPL": 2}

    fetch_results = [
        [
            make_mock_result("AAPL", data_length=2, coverage=0.5, missing_dates=[date(2024,1,2)]),
            make_mock_result("AAPL", data_length=1, coverage=0.3, missing_dates=[date(2024,1,3)])
        ]
    ]

    response = adapt_orchestration_result(
        symbols=symbols,
        start=start,
        end=end,
        interval=interval,
        dry_run=dry_run,
        fetch_results=fetch_results,
        rows_inserted=rows_inserted
    )

    aapl = response.results[0]
    assert aapl.success is True
    assert aapl.rows_fetched == 3  # sum of all fetched rows
    assert aapl.coverage == 0.3  # min coverage across runs
    assert aapl.missing_dates == [date(2024,1,2), date(2024,1,3)]

