import asyncio
import pandas as pd
from typing import List
from datetime import date

from app.data_ingestion.fetchers.prices import fetch_prices
from app.data_ingestion.retry import retry_info, RetryReason
from app.data_ingestion.types import FetchRequest
from app.db.crud import bulk_insert_prices_chunked, get_price_keys
from app.schemas import PriceDataRow
from .utils import get_missing_date_ranges


async def fetch_with_retries(
    symbol: str,
    start: date,
    end: date,
    interval: str = "1d",
    max_attempts: int = 3,
    coverage_threshold: float = 0.95,
    backoff_seconds: float = 1.0
):
    """Fetch a single symbol with retry logic."""
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        result = await fetch_prices(
            FetchRequest(symbol=symbol, start=start, end=end, interval=interval)
        )
        info = retry_info(result, start, end, coverage_threshold)
        if info["retry_reason"] == RetryReason.NONE:
            return { "symbol": symbol, "result": result, "attempts": attempt, **info }
        else:
            # exponential backoff
            await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))
    # return last result if all attempts failed
    return { "symbol": symbol, "result": result, "attempts": attempt, **info }


async def fetch_missing_prices(symbol: str, start: date, end: date, interval: str, max_attempts: int, coverage_threshold: float = 0.95):
    """
    Fetch prices only for missing dates for a single symbol.
    Returns list of FetchResults.
    """
    # Get existing keys from DB
    existing_keys = await get_price_keys(symbol, start, end)
    existing_dates = pd.to_datetime([d for s, d in existing_keys if s == symbol])

    # Compute missing date ranges
    missing_ranges = get_missing_date_ranges(existing_dates, start, end)
    results = []

    # Fetch each missing range
    for r_start, r_end in missing_ranges:
        result = await fetch_with_retries(symbol, r_start, r_end, interval, max_attempts, coverage_threshold)
        results.append(result)

    return results


async def fetch_symbols_parallel(
    symbols: List[str],
    start: date,
    end: date,
    interval: str = "1d",
    max_attempts: int = 3,
    max_concurrent: int = 5,
    coverage_threshold: float = 0.95
):
    """Fetch multiple symbols in parallel with retries."""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def sem_fetch(symbol):
        async with semaphore:
            return await fetch_missing_prices(symbol, start, end, interval, max_attempts, coverage_threshold)

    tasks = [sem_fetch(s) for s in symbols]
    results = await asyncio.gather(*tasks)
    return results


async def orchestrate_fetch_and_insert(
    symbols: list[str],
    start: date,
    end: date,
    interval: str = "1d",
    max_attempts: int = 3,
    max_concurrent: int = 5,
    coverage_threshold: float = 0.95,
    chunk_size: int = 1000
):
    """
    Orchestrates fetching multiple symbols in parallel with retries
    and inserts the fetched results into the database.
    """
    fetch_results = await fetch_symbols_parallel(
        symbols,
        start,
        end,
        interval,
        max_attempts,
        max_concurrent,
        coverage_threshold
    )

    rows_to_insert: list[PriceDataRow] = []

    for symbol_results in fetch_results:
        for r in symbol_results:
            symbol = r["symbol"]
            fetch_result = r["result"]

            if fetch_result and not fetch_result.empty:
                for dt, row in fetch_result.data.iterrows():
                    rows_to_insert.append(
                        PriceDataRow(
                            symbol=symbol,
                            date=dt,
                            open=row["Open"],
                            high=row["High"],
                            low=row["Low"],
                            close=row["Close"],
                            volume=row["Volume"],
                        )
                    )

    # Insert all rows into the DB
    inserted_count = await bulk_insert_prices_chunked(rows_to_insert, chunk_size=chunk_size)
    return inserted_count, fetch_results