import asyncio
from typing import List
from datetime import date

from app.data_ingestion.fetchers.prices import fetch_prices
from app.data_ingestion.retry import retry_info, RetryReason
from app.data_ingestion.types import FetchRequest

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
            return await fetch_with_retries(symbol, start, end, interval, max_attempts, coverage_threshold)

    tasks = [sem_fetch(s) for s in symbols]
    results = await asyncio.gather(*tasks)
    return results
