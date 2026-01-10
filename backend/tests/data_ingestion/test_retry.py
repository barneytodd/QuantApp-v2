import pandas as pd
import pytest
from app.data_ingestion.models import FetchRequest, FetchResult, RetryReason
from app.data_ingestion.retry import should_retry, retry_info

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


@pytest.mark.parametrize(
    "df, empty, exception, expected_reason",
    [
        ("full_price_df", False, None, RetryReason.NONE),
        ("gappy_price_df", False, None, RetryReason.PARTIAL),
        ("empty_df", True, None, RetryReason.EMPTY),
        ("full_price_df", False, Exception("network fail"), RetryReason.EXCEPTION),
    ]
)
def test_retry_info_all_cases(request, df, empty, exception, expected_reason, date_range, full_price_df, gappy_price_df):
    """Test retry_info returns correct retry_reason, coverage, and missing dates."""
    start, end = date_range
    # Select the fixture based on parameter name
    data_df = {"full_price_df": full_price_df,
               "gappy_price_df": gappy_price_df,
               "empty_df": pd.DataFrame()}[df]

    fetch_request = FetchRequest(symbol="AAPL", start=start, end=end)
    result = FetchResult(
        request=fetch_request,
        data=data_df,
        empty=empty,
        exception=exception,
        elapsed_ms=10
    )

    info = retry_info(result, start, end, coverage_threshold=1.0)

    # Assertions
    assert info["retry_reason"] == expected_reason
    assert isinstance(info["missing_dates"], list)
    assert isinstance(info["coverage"], float)
    assert info["elapsed_ms"] == 10

    if expected_reason == RetryReason.NONE:
        assert info["missing_dates"] == []
        assert info["coverage"] == 1.0
