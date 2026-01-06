from typing import AsyncGenerator, List
from datetime import date, timedelta
from app.db.connection import get_connection, release_connection
from app.schemas.prices import PriceDataRow
#from app.core.logging import get_logger

#log = get_logger(__name__)

async def get_prices(
    symbols: List[str],
    start: date,
    end: date,
    lookback: int = 0
) -> AsyncGenerator[PriceDataRow, None]:
    """Fetch price rows for the given symbols and date range."""
    if not symbols:
        return
    
    if lookback > 0:
        start = start - timedelta(days=lookback-1) 

    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            sql = f"""
                SELECT symbol, date, [open], [high], [low], [close], volume
                FROM dbo.prices
                WHERE symbol IN ({','.join('?' for _ in symbols)})
                  AND date >= ?
                  AND date <= ?
                ORDER BY symbol, date ASC
            """
            params = symbols + [start, end]
            await cursor.execute(sql, params)

            async for row in cursor:
                yield PriceDataRow(
                    symbol=row[0],
                    date=row[1],
                    open=row[2],
                    high=row[3],
                    low=row[4],
                    close=row[5],
                    volume=row[6]
                )
    finally:
        await release_connection(conn)
