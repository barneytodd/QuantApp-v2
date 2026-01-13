from fastapi import APIRouter, BackgroundTasks, Query
from datetime import date
from typing import List

from app.core.logging import get_logger
from app.schemas import GetPricesPayload, PriceDataRow, IngestPricesResponse, IngestPricesRequest
from app.db.crud import get_prices
from app.data_ingestion import orchestrate_fetch_and_insert
from .adapters.ingest_prices import adapt_orchestration_result


router = APIRouter(prefix="/market-data", tags=["ohlcv"])
logger = get_logger(__name__)


@router.get("/ohlcv/{symbol}", response_model=List[PriceDataRow])
async def get_prices_single(symbol: str, start: date | None = None, end: date | None = None):
    rows = []
    async for row in get_prices([symbol], start, end):
        rows.append(row)
    return rows


@router.post("/ohlcv/query", response_model=List[PriceDataRow])
async def get_prices_bulk(payload: GetPricesPayload):
    rows = []
    async for row in get_prices(
        symbols=payload.symbols,
        start=payload.start,
        end=payload.end
    ):
        rows.append(row)
    return rows


@router.post("/ingest", response_model=IngestPricesResponse)
async def ingest_prices(req: IngestPricesRequest):

    inserted_count, fetch_results = await orchestrate_fetch_and_insert(
        symbols=req.symbols,
        start=req.start,
        end=req.end,
        interval=req.interval,
        max_attempts=req.max_attempts,
        max_concurrent=req.max_concurrent,
        coverage_threshold=req.coverage_threshold,
        dry_run=req.dry_run
    )

    return adapt_orchestration_result(
        symbols=req.symbols,
        start=req.start,
        end=req.end,
        interval=req.interval,
        dry_run=req.dry_run,
        fetch_results=fetch_results,
        rows_inserted=inserted_count,
    )


# === Async ingestion endpoint ===
@router.post("/ingest/async", status_code=202)
async def ingest_prices_async(req: IngestPricesRequest, background: BackgroundTasks):
    """
    Schedule asynchronous ingestion of OHLCV data for multiple symbols.
    Returns immediately; ingestion runs in the background.
    """

    background.add_task(run_background_ingestion, req)
    return {"status": "accepted", "symbols": req.symbols}


async def run_background_ingestion(req: IngestPricesRequest):
    """
    Background task that orchestrates fetch & insert, and logs the outcome.
    """
    try:
        inserted_count, fetch_results = await orchestrate_fetch_and_insert(
            symbols=req.symbols,
            start=req.start,
            end=req.end,
            interval=req.interval,
            max_attempts=req.max_attempts,
            max_concurrent=req.max_concurrent,
            coverage_threshold=req.coverage_threshold,
            dry_run=req.dry_run
        )

        response = adapt_orchestration_result(
            symbols=req.symbols,
            start=req.start,
            end=req.end,
            interval=req.interval,
            dry_run=req.dry_run,
            fetch_results=fetch_results,
            rows_inserted=inserted_count,
        )

        # Log a summary
        for symbol_result in response.results:
            logger.info(
                "Async ingestion finished: symbol=%s success=%s rows_inserted=%d elapsed_ms=%d",
                symbol_result.symbol,
                symbol_result.success,
                symbol_result.rows_inserted,
                symbol_result.elapsed_ms,
            )

    except Exception as e:
        logger.exception("Async ingestion failed for symbols=%s: %s", req.symbols, e)