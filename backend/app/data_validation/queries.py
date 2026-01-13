from typing import Any
from app.db.connection import get_connection

# --- SQL Queries ---

# 1. Symbol summary: first/last date and number of observed rows
SYMBOL_SUMMARY = """
SELECT
    symbol,
    MIN([date]) AS first_date,
    MAX([date]) AS last_date,
    COUNT(*) AS observed_days
FROM prices
WHERE symbol = ?
  AND [date] BETWEEN ? AND ?
GROUP BY symbol;
"""

# 2. Rows with missing OHLCV fields
MISSING_OHLCV = """
SELECT COUNT(*) AS invalid_rows
FROM prices
WHERE symbol = ?
  AND [date] BETWEEN ? AND ?
  AND (
      open IS NULL OR
      high IS NULL OR
      low IS NULL OR
      close IS NULL OR
      volume IS NULL
  );
"""

# 3. Suspicious or invalid price rows
SUSPICIOUS_PRICES = """
SELECT COUNT(*) AS suspicious_rows
FROM prices
WHERE symbol = ?
  AND [date] BETWEEN ? AND ?
  AND (
      open <= 0 OR
      high <= 0 OR
      low <= 0 OR
      close <= 0 OR
      high < low
  );
"""

# 4. Existing dates for gap detection
EXISTING_DATES = """
SELECT [date]
FROM prices
WHERE symbol = ?
  AND [date] BETWEEN ? AND ?
ORDER BY [date] ASC;
"""

# --- Async query helpers ---

async def fetch_one(conn, sql: str, params: tuple) -> Any:
    """
    Execute a SQL query that returns a single value.
    """
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        row = await cur.fetchone()
        return row[0] if row else None


async def fetch_all(conn, sql: str, params: tuple) -> list[Any]:
    """
    Execute a SQL query that returns multiple rows.
    """
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        rows = await cur.fetchall()
        return [r[0] if len(r) == 1 else r for r in rows]


# --- Convenience wrappers ---

async def get_symbol_summary(symbol: str, start: str, end: str) -> dict:
    """
    Returns a dict with first_date, last_date, and observed_days.
    """
    async with get_connection() as conn:
        row = await fetch_all(conn, SYMBOL_SUMMARY, (symbol, start, end))
        if row:
            return {"first_date": row[0][1], "last_date": row[0][2], "observed_days": row[0][3]}
        return {"first_date": None, "last_date": None, "observed_days": 0}


async def count_missing_ohlcv(symbol: str, start: str, end: str) -> int:
    async with get_connection() as conn:
        return await fetch_one(conn, MISSING_OHLCV, (symbol, start, end))


async def count_suspicious_prices(symbol: str, start: str, end: str) -> int:
    async with get_connection() as conn:
        return await fetch_one(conn, SUSPICIOUS_PRICES, (symbol, start, end))


async def get_existing_dates(symbol: str, start: str, end: str) -> list:
    async with get_connection() as conn:
        return await fetch_all(conn, EXISTING_DATES, (symbol, start, end))
