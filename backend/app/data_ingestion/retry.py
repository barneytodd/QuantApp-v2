from app.data_ingestion.validators import calculate_coverage, detect_gaps
from .models import RetryReason


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
    
    if result.empty and result.request.start != result.request.end:
        return RetryReason.EMPTY
    
    # Compute coverage if not empty
    coverage = calculate_coverage(result.data, result.request.start, result.request.end)
    if coverage < coverage_threshold and result.request.start != result.request.end:
        return RetryReason.PARTIAL
    
    return RetryReason.NONE


def retry_info(result, start, end, coverage_threshold=0.95):
    reason = should_retry(result, coverage_threshold)
    gaps = detect_gaps(result.data, start, end) if result.data is not None else []
    coverage = calculate_coverage(result.data, result.request.start, result.request.end) if result.data is not None else 0
    return {
        "retry_reason": reason,
        "coverage": coverage,
        "missing_dates": gaps,
        "elapsed_ms": result.elapsed_ms
    }
