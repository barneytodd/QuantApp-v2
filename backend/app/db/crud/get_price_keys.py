from typing import Set, Tuple
from datetime import date
from app.db.connection import get_connection, release_connection

async def get_price_keys(symbol: str, start: date, end: date) -> Set[Tuple[str, date]]:
    """
    Return a set of (symbol, date) tuples that already exist in the database
    for the given symbol and date range.
    """
    if not symbol:
        return set()

    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            sql = """
                SELECT symbol, [date]
                FROM dbo.prices
                WHERE symbol = ?
                  AND [date] >= ?
                  AND [date] <= ?
            """
            await cursor.execute(sql, [symbol, start, end])
            rows = await cursor.fetchall()
            return {(row[0], row[1]) for row in rows}
    finally:
        await release_connection(conn)
