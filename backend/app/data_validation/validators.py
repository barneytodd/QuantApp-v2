from datetime import date
from typing import List

from app.core.dates import trading_days
from app.data_validation.models import ValidationResult, ValidationIssue
from app.data_validation import queries


async def validate_symbol(symbol: str, start: date, end: date) -> ValidationResult:
    """
    Perform full validation for a single symbol.
    """
    # 1️⃣ Get symbol summary
    summary = await queries.get_symbol_summary(symbol, start, end)
    first_date = summary.get("first_date")
    last_date = summary.get("last_date")
    observed_days = summary.get("observed_days", 0)

    # 2️⃣ Generate expected trading days
    expected_dates = trading_days(start, end)

    # 3️⃣ Existing dates from DB
    existing_dates = await queries.get_existing_dates(symbol, start, end)
    existing_dates_set = set(existing_dates)

    # 4️⃣ Calculate coverage
    coverage = len(existing_dates_set) / len(expected_dates) if expected_dates.size > 0 else 0.0

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
    missing_ohlcv_count = await queries.count_missing_ohlcv(symbol, start, end)
    if missing_ohlcv_count:
        issues.append(
            ValidationIssue(
                code="MISSING_FIELDS",
                description=f"{missing_ohlcv_count} rows with missing OHLCV fields",
                count=missing_ohlcv_count
            )
        )

    # 7️⃣ Suspicious/outlier prices
    suspicious_count = await queries.count_suspicious_prices(symbol, start, end)
    if suspicious_count:
        issues.append(
            ValidationIssue(
                code="SUSPICIOUS_PRICE",
                description=f"{suspicious_count} rows with suspicious price data",
                count=suspicious_count
            )
        )

    # 8️⃣ Return full ValidationResult
    return ValidationResult(
        symbol=symbol,
        start=start,
        end=end,
        coverage=coverage,
        first_date=first_date,
        last_date=last_date,
        issues=issues
    )
