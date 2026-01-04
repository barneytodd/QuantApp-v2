from enum import Enum
from data_ingestion.validators import calculate_coverage, detect_gaps


class RetryReason(Enum):
    NONE = "none"
    EMPTY = "empty_data"
    PARTIAL = "partial_coverage"
    EXCEPTION = "fetch_exception"


def should_retry(result, coverage_threshold=0.95) -> RetryReason:
    """
    Decide whether to retry fetching a symbol.
    
    Args:
        result: FetchResult from fetch_prices
        coverage_threshold: minimum acceptable coverage ratio
        
    Returns:
        RetryReason enum
    """
    if result.exception is not None:
        return RetryReason.EXCEPTION
    
    if result.empty:
        return RetryReason.EMPTY
    
    # Compute coverage if not empty
    coverage = calculate_coverage(result.data, result.request.start, result.request.end)
    if coverage < coverage_threshold:
        return RetryReason.PARTIAL
    
    return RetryReason.NONE


def retry_info(result, coverage_threshold=0.95):
    reason = should_retry(result, coverage_threshold)
    gaps = detect_gaps(result.data) if result.data is not None else []
    coverage = calculate_coverage(result.data, result.request.start, result.request.end) if result.data is not None else 0
    return {
        "retry_reason": reason,
        "coverage": coverage,
        "missing_dates": gaps,
        "elapsed_ms": result.elapsed_ms
    }
