# test_multi_validators.py
import pytest
from datetime import date
from unittest.mock import AsyncMock, patch

from app.data_validation import service  # adjust import path if needed
from app.data_validation.models import ValidationResult, ValidationStatus


@pytest.mark.asyncio
async def test_validate_symbols_concurrent():
    """
    Test that validate_symbols returns results for multiple symbols correctly.
    """
    symbols = ["AAPL", "GOOG", "MSFT"]
    start = date(2024, 1, 1)
    end = date(2024, 1, 5)

    # Create mock ValidationResult for each symbol
    mock_results = [
        ValidationResult(
            symbol=sym,
            start=start,
            end=end,
            status=ValidationStatus.OK,
            coverage=1.0,
            expected_days=5,
            observed_days=5,
            first_date=start,
            last_date=end,
            missing_dates=[],
            issues=[],
            elapsed_ms=100
        )
        for sym in symbols
    ]

    # Patch validate_symbol to return mock results
    with patch("app.data_validation.service.validate_symbol", new_callable=AsyncMock) as mock_validate:
        mock_validate.side_effect = mock_results  # one per call

        results = await service.validate_symbols(symbols, start, end, max_concurrent=2)

        # Ensure we got a result per symbol
        assert len(results) == len(symbols)

        # Check that each result has the correct symbol
        returned_symbols = [res.symbol for res in results]
        assert returned_symbols == symbols

        # Check that validate_symbol was called once per symbol
        assert mock_validate.call_count == len(symbols)

        # Check that the results are of type ValidationResult
        assert all(isinstance(r, ValidationResult) for r in results)
