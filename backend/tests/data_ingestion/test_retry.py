import pandas as pd
from app.data_ingestion.retry import should_retry, RetryReason
from app.data_ingestion.types import FetchRequest, FetchResult

def test_should_not_retry_full_coverage(full_price_df, date_range):
    """Full coverage should not trigger a retry."""
    start, end = date_range
    request = FetchRequest(symbol="AAPL", start=start, end=end)
    result = FetchResult(
        request=request,
        data=full_price_df,
        empty=False,
        exception=None,
        elapsed_ms=10,
    )
    decision = should_retry(result, coverage_threshold=0.99)
    assert decision == RetryReason.NONE


def test_retry_on_low_coverage(gappy_price_df, date_range):
    """Partial coverage below threshold triggers LOW_COVERAGE retry."""
    start, end = date_range
    request = FetchRequest(symbol="AAPL", start=start, end=end)
    result = FetchResult(
        request=request,
        data=gappy_price_df,
        empty=False,
        exception=None,
        elapsed_ms=15,
    )
    decision = should_retry(result, coverage_threshold=1.0)
    assert decision == RetryReason.PARTIAL


def test_retry_on_empty_data(date_range):
    """Empty DataFrame should trigger STRUCTURE_ERROR retry."""
    start, end = date_range
    empty_df = pd.DataFrame()
    request = FetchRequest(symbol="AAPL", start=start, end=end)
    result = FetchResult(
        request=request,
        data=empty_df,
        empty=True,
        exception=None,
        elapsed_ms=5,
    )
    decision = should_retry(result)
    assert decision == RetryReason.EMPTY


def test_retry_on_exception(full_price_df):
    """If the fetch errored, it should trigger FETCH_ERROR retry."""
    request = FetchRequest(symbol="AAPL", start=None, end=None)
    result = FetchResult(
        request=request,
        data=full_price_df,
        empty=False,
        exception=Exception("network failure"),
        elapsed_ms=20,
    )
    decision = should_retry(result)
    assert decision == RetryReason.EXCEPTION


