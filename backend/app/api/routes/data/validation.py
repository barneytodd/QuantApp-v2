from fastapi import APIRouter
from typing import List

from app.data_validation.service import validate_symbols
from .adapters.validation import adapt_validation_result
from app.schemas.prices.validation import ValidatePricesRequest, ValidationResultRow

router = APIRouter(prefix="/market-data/validate", tags=["validation"])


@router.post("/", response_model=List[ValidationResultRow])
async def validate_prices(payload: ValidatePricesRequest):
    """
    Validate OHLCV data for multiple symbols over a date range.
    Returns coverage %, missing trading days, missing OHLCV fields, and suspicious prices.
    """

    # 1️⃣ Run validations in parallel using the service helper
    domain_results = await validate_symbols(
        symbols=payload.symbols,
        start=payload.start,
        end=payload.end
    )

    # 2️⃣ Adapt domain results to API-ready Pydantic models
    response = adapt_validation_result(domain_results)

    # 3️⃣ Return
    return response
