from datetime import date
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes.data import validation  # replace with actual router module
from app.data_validation.models import ValidationResult, ValidationIssue, ValidationStatus
from app.schemas.prices.validation import ValidatePricesRequest, ValidationResultRow

# Create a minimal FastAPI app for testing
app = FastAPI()
app.include_router(validation.router)


def test_validate_prices_endpoint():
    """
    Test POST /market-data/validate endpoint.
    Mocks validate_symbols and checks the adapted response.
    """
    # Prepare test payload
    payload = {
        "symbols": ["AAPL", "GOOG"],
        "start": "2024-01-01",
        "end": "2024-01-05"
    }

    # Mock ValidationResult objects returned by validate_symbols
    mock_results = [
        ValidationResult(
            symbol="AAPL",
            start=date(2024, 1, 1),
            end=date(2024, 1, 5),
            status=ValidationStatus.OK,
            coverage=1.0,
            expected_days=5,
            observed_days=5,
            first_date=date(2024, 1, 1),
            last_date=date(2024, 1, 5),
            missing_dates=[],
            issues=[],
            elapsed_ms=100
        ),
        ValidationResult(
            symbol="GOOG",
            start=date(2024, 1, 1),
            end=date(2024, 1, 5),
            status=ValidationStatus.PARTIAL,
            coverage=0.8,
            expected_days=5,
            observed_days=4,
            first_date=date(2024, 1, 1),
            last_date=date(2024, 1, 5),
            missing_dates=[date(2024, 1, 3)],
            issues=[
                ValidationIssue(
                    code="MISSING_DAYS",
                    description="1 missing trading day",
                    affected_dates=[date(2024, 1, 3)],
                    count=1
                )
            ],
            elapsed_ms=100
        )
    ]

    # Patch the validate_symbols service
    with patch("app.api.routes.data.validation.validate_symbols", new_callable=AsyncMock) as mock_service:
        mock_service.return_value = mock_results

        client = TestClient(app)
        response = client.post("/market-data/validate/", json=payload)

        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2

        # Check first result (AAPL)
        aapl_result = data[0]
        assert aapl_result["symbol"] == "AAPL"
        assert aapl_result["coverage"] == 1.0
        assert aapl_result["issues"] == []

        # Check second result (GOOG)
        goog_result = data[1]
        assert goog_result["symbol"] == "GOOG"
        assert goog_result["coverage"] == 0.8
        assert len(goog_result["issues"]) == 1
        issue = goog_result["issues"][0]
        assert issue["code"] == "MISSING_DAYS"
        assert issue["count"] == 1
        assert issue["affected_dates"] == ["2024-01-03"]
