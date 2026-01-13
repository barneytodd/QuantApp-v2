from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any


class ValidationStatus(str, Enum):
    OK = "ok"
    PARTIAL = "partial"
    FAIL = "fail"


class ValidationSeverity(str, Enum):
    WARNING = "warning"
    ERROR = "error"


@dataclass(slots=True)
class ValidationIssue:
    """
    Atomic validation finding.

    Must be:
    - deterministic
    - serializable
    - frontend-safe
    """
    code: str                     # e.g. MISSING_DAYS, OUTLIER_RETURNS
    severity: ValidationSeverity  # warning | error
    message: str
    details: dict[str, Any]


@dataclass(slots=True)
class ValidationResult:
    """
    Single-symbol validation outcome.
    """
    symbol: str
    start: date
    end: date

    status: ValidationStatus

    coverage: float
    expected_days: int
    observed_days: int

    first_date: date | None
    last_date: date | None

    missing_dates: list[date]
    issues: list[ValidationIssue]

    elapsed_ms: int
