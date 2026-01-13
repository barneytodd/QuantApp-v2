import pytest
from datetime import date

from app.data_validation import validators
from app.data_validation.models import ValidationStatus


@pytest.mark.asyncio
async def test_validate_symbol_all_ok(mock_validation_queries, symbol, date_range):
    """
    Test validate_symbol when the database mocks return no missing or suspicious rows.
    """
    result = await validators.validate_symbol(symbol, date_range["start"], date_range["end"])

    # The conftest mock sets 0 missing OHLCV and 0 suspicious prices
    assert result.symbol == symbol
    assert result.start == date_range["start"]
    assert result.end == date_range["end"]

    # There are missing trading days according to the mock get_existing_dates
    assert "MISSING_DAYS" in [issue.code for issue in result.issues]

    # Status should be PARTIAL because there are missing days
    assert result.status == ValidationStatus.PARTIAL

    # Coverage should be between 0 and 1
    assert 0.0 <= result.coverage <= 1.0


@pytest.mark.asyncio
async def test_validate_symbol_with_issues(mock_validation_queries, symbol, date_range):
    """
    Test validate_symbol with issues returned by the mocked queries.
    """
    result = await validators.validate_symbol(symbol, date_range["start"], date_range["end"])

    # Check that issues include MISSING_FIELDS and SUSPICIOUS_PRICE from conftest mocks
    issue_codes = [issue.code for issue in result.issues]
    assert "MISSING_FIELDS" in issue_codes
    assert "SUSPICIOUS_PRICE" in issue_codes

    # Also, missing trading days should be detected
    assert "MISSING_DAYS" in issue_codes

    # Status should be PARTIAL
    assert result.status == ValidationStatus.PARTIAL

    # Ensure the ValidationResult contains expected attributes
    assert result.expected_days > 0
    assert result.observed_days >= 0
    assert result.first_date is not None
    assert result.last_date is not None
    assert isinstance(result.issues[0].affected_dates[0], date)
