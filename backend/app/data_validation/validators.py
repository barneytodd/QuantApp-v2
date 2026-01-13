import time
from datetime import date
from typing import List

from app.core.dates import trading_days
from app.data_validation.models import ValidationResult, ValidationIssue, ValidationStatus
from app.data_validation import queries


async def validate_symbol(symbol: str, start: date, end: date) -> ValidationResult:
    """
    Perform full validation for a single symbol.
    """
    start_time = time.perf_counter()

    # 1️⃣ Get symbol summary
    summary = await queries.get_symbol_summary(symbol, start, end)
    first_date = summary.get("first_date")
    last_date = summary.get("last_date")
    observed_days = summary.get("observed_days", 0)

    # 2️⃣ Generate expected trading days
    expected_dates = trading_days(start, end)
    expected_days = len(expected_dates)

    # 3️⃣ Existing dates from DB
    existing_dates = await queries.get_existing_dates(symbol, start, end)
    existing_dates_set = set(existing_dates)

    # 4️⃣ Calculate coverage
    coverage = len(existing_dates_set) / len(expected_dates) if len(expected_dates) > 0 else 0.0

    # 5️⃣ Detect missing dates
    missing_dates = [d for d in expected_dates if d not in existing_dates_set]

    issues: List[ValidationIssue] = []

    if missing_dates:
        issues.append(
            ValidationIssue(
                code="MISSING_DAYS",
                description=f"{len(missing_dates)} missing trading days",
                affected_dates=missing_dates,
                count=len(missing_dates)
            )
        )

    # 6️⃣ Missing OHLCV
    missing_ohlcv_dates = await queries.get_missing_ohlcv_dates(symbol, start, end)
    missing_ohlcv_count = len(missing_ohlcv_dates)
    if missing_ohlcv_count:
        issues.append(
            ValidationIssue(
                code="MISSING_FIELDS",
                description=f"{missing_ohlcv_count} rows with missing OHLCV fields",
                affected_dates=missing_ohlcv_dates,
                count=missing_ohlcv_count
            )
        )

    # 7️⃣ Suspicious/outlier prices
    suspicious_dates = await queries.get_suspicious_price_dates(symbol, start, end)
    suspicious_count = len(suspicious_dates)
    if suspicious_count:
        issues.append(
            ValidationIssue(
                code="SUSPICIOUS_PRICE",
                description=f"{suspicious_count} rows with suspicious price data",
                affected_dates=suspicious_dates,
                count=suspicious_count
            )
        )

    fails = len(missing_dates) + missing_ohlcv_count + suspicious_count
    status = ValidationStatus.PARTIAL
    if fails == 0:
        status = ValidationStatus.OK
    elif fails == expected_days:
        status = ValidationStatus.FAIL

    end_time = time.perf_counter()
    elapsed = (end_time - start_time) * 1000

    # 8️⃣ Return full ValidationResult
    return ValidationResult(
        symbol=symbol,
        start=start,
        end=end,
        status=status,
        coverage=coverage,
        expected_days=expected_days,
        observed_days=observed_days,
        first_date=first_date,
        last_date=last_date,
        missing_dates=missing_dates,
        issues=issues,
        elapsed_ms=elapsed
    )
