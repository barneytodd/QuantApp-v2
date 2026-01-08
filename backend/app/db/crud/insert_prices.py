import asyncio
from typing import Iterable, List, Optional
from app.schemas.prices.price_row import PriceDataRow
from app.db.connection import get_connection, release_connection

def chunked(iterable: List, size: int = 1000):
    """Yield successive chunks from a list."""
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

async def bulk_insert_prices_chunked(
    rows: Iterable[PriceDataRow],
    chunk_size: int = 1000,
    return_count: bool = True
) -> Optional[int]:
    """
    Bulk insert PriceDataRow objects into dbo.prices in chunks.
    Duplicate (symbol, date) rows are skipped via unique constraint.
    Avoids IntegrityError by pre-filtering against DB and tracking intra-batch inserts.

    Args:
        rows: Iterable of PriceDataRow
        chunk_size: Number of rows per batch
        return_count: If True, returns approximate number of rows inserted

    Returns:
        Approximate number of rows inserted if return_count=True, else None
    """
    rows = list(rows)
    if not rows:
        return 0 if return_count else None

    total_inserted = 0
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            cursor.fast_executemany = True

            # 1️⃣ Pre-fetch existing keys from the DB
            symbols = list({r.symbol for r in rows})
            if symbols:
                sql = f"""
                    SELECT symbol, date
                    FROM dbo.prices
                    WHERE symbol IN ({','.join('?' for _ in symbols)})
                """
                await cursor.execute(sql, symbols)
                existing_rows = await cursor.fetchall()
                existing_keys = {(row[0], row[1]) for row in existing_rows}
            else:
                existing_keys = set()

            # 2️⃣ Track keys inserted during this run to avoid intra-batch duplicates
            inserted_keys = set()

            # 3️⃣ Insert in chunks
            for batch in chunked(rows, chunk_size):
                batch_filtered = [
                    r for r in batch
                    if (r.symbol, r.date.date()) not in existing_keys
                    and (r.symbol, r.date.date()) not in inserted_keys
                ]

                if not batch_filtered:
                    continue

                values = [
                    (r.symbol, r.date.date(), r.open, r.high, r.low, r.close, r.volume)
                    for r in batch_filtered
                ]

                await cursor.executemany(
                    """
                    INSERT INTO dbo.prices (
                        symbol, date, [open], [high], [low], [close], [volume]
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    values
                )

                # Update inserted_keys and total count
                for r in batch_filtered:
                    inserted_keys.add((r.symbol, r.date.date()))
                if return_count:
                    total_inserted += len(batch_filtered)

    finally:
        await release_connection(conn)

    return total_inserted if return_count else None
