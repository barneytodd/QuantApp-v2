# test_adapt_validation_result.py
import pytest
from datetime import date

from app.api.routes.data.adapters import adapt_validation_result
from app.data_validation.models import ValidationResult, ValidationIssue
from app.schemas.prices.validation import ValidationResultRow, ValidationIssueRow


def test_adapt_validation_result_single():
    """
    Test adapting a single ValidationResult with one issue.
    """
    # Create a mock ValidationResult with one issue
    issue = ValidationIssue(
        code="MISSING_DAYS",
        description="2 missing trading days",
        affected_dates=[date(2024, 1, 3), date(2024, 1, 5)],
        count=2
    )

    result = ValidationResult(
        symbol="AAPL",
        start=date(2024, 1, 1),
        end=date(2024, 1, 5),
        status=None,  # status is not used in adapter
        coverage=0.6,
        expected_days=5,
        observed_days=3,
        first_date=date(2024, 1, 1),
        last_date=date(2024, 1, 5),
        missing_dates=[date(2024, 1, 3), date(2024, 1, 5)],
        issues=[issue],
        elapsed_ms=100
    )

    adapted = adapt_validation_result([result])

    # Should return a list with one ValidationResultRow
    assert isinstance(adapted, list)
    assert len(adapted) == 1
    adapted_result = adapted[0]
    assert isinstance(adapted_result, ValidationResultRow)

    # Check fields
    assert adapted_result.symbol == "AAPL"
    assert adapted_result.start == date(2024, 1, 1)
    assert adapted_result.end == date(2024, 1, 5)
    assert adapted_result.coverage == 0.6
    assert adapted_result.first_date == date(2024, 1, 1)
    assert adapted_result.last_date == date(2024, 1, 5)

    # Check nested issues
    assert len(adapted_result.issues) == 1
    adapted_issue = adapted_result.issues[0]
    assert isinstance(adapted_issue, ValidationIssueRow)
    assert adapted_issue.code == "MISSING_DAYS"
    assert adapted_issue.description == "2 missing trading days"
    assert adapted_issue.affected_dates == [date(2024, 1, 3), date(2024, 1, 5)]
    assert adapted_issue.count == 2


def test_adapt_validation_result_multiple():
    """
    Test adapting multiple ValidationResult objects.
    """
    results = []
    for i in range(3):
        issue = ValidationIssue(
            code=f"ISSUE_{i}",
            description=f"Description {i}",
            affected_dates=[date(2024, 1, i+1)],
            count=1
        )
        results.append(
            ValidationResult(
                symbol=f"SYM{i}",
                start=date(2024, 1, 1),
                end=date(2024, 1, 5),
                status=None,
                coverage=1.0,
                expected_days=5,
                observed_days=5,
                first_date=date(2024, 1, 1),
                last_date=date(2024, 1, 5),
                missing_dates=[],
                issues=[issue],
                elapsed_ms=100
            )
        )

    adapted = adapt_validation_result(results)

    assert len(adapted) == 3
    for i, adapted_result in enumerate(adapted):
        assert adapted_result.symbol == f"SYM{i}"
        assert len(adapted_result.issues) == 1
        assert adapted_result.issues[0].code == f"ISSUE_{i}"
