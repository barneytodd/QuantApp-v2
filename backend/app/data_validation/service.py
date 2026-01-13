import asyncio
from datetime import date
from typing import List

from .models import ValidationResult
from .validators import validate_symbol

async def validate_symbols(symbols: List[str], start: date, end: date, max_concurrent: int = 5):
    """
    Validate multiple symbols concurrently with bounded concurrency.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    results: List[ValidationResult] = []

    async def sem_task(symbol):
        async with semaphore:
            return await validate_symbol(symbol, start, end)

    tasks = [sem_task(sym) for sym in symbols]
    results = await asyncio.gather(*tasks)
    return results
