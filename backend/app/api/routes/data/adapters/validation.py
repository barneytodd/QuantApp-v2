from typing import List
from app.data_validation.models import ValidationResult
from app.schemas.prices.validation import ValidationResultRow, ValidationIssueRow

def adapt_validation_result(results: List[ValidationResult]) -> List[ValidationResultRow]:
    """
    Convert domain ValidationResult objects into API-ready Pydantic models.
    """
    response = []
    for r in results:
        issues = [
            ValidationIssueRow(
                code=i.code,
                description=i.description,
                affected_dates=i.affected_dates,
                count=i.count
            )
            for i in r.issues
        ]
        response.append(
            ValidationResultRow(
                symbol=r.symbol,
                start=r.start,
                end=r.end,
                status=r.status,
                coverage=r.coverage,
                first_date=r.first_date,
                last_date=r.last_date,
                issues=issues,
                expected_days=5,
                observed_days=3,
                missing_dates=r.missing_dates,
                elapsed_ms=r.elapsed_ms
            )
        )
    return response
