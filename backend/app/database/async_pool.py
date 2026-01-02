import asyncio
import pyodbc
import aioodbc
import aiosqlite

from app.core.config import (
    DB_ENGINE,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_INSTANCE,
    DB_USER,
    DB_PASSWORD,
    DB_POOL_MIN,
    DB_POOL_MAX,
)

# Global pool
_pool = None
_pool_lock = asyncio.Lock()


def _build_mssql_dsn() -> str:
    """Build ODBC connection string for SQL Server."""
    drivers = [driver for driver in pyodbc.drivers() if "SQL Server" in driver and "ODBC" in driver]
    if not drivers:
        raise RuntimeError("No SQL Server ODBC drivers found!")
    DB_DRIVER = sorted(drivers)[-1]
    server = f"{DB_HOST}\\{DB_INSTANCE}"

    return (
        f"Driver={DB_DRIVER};"
        f"Server={server};"
        f"Database={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "TrustServerCertificate=yes;"
    )


async def init_db_pool() -> None:
    """Initialize the global async DB connection pool."""
    global _pool

    async with _pool_lock:
        if _pool is not None:
            return

        if DB_ENGINE == "sqlite":
            _pool = await aiosqlite.connect(DB_NAME)
        else:
            dsn = _build_mssql_dsn()
            _pool = await aioodbc.create_pool(
                dsn=dsn,
                minsize=DB_POOL_MIN,
                maxsize=DB_POOL_MAX,
                autocommit=True,
            )


async def close_db_pool() -> None:
    """Gracefully close the DB connection pool."""
    global _pool

    if _pool is None:
        return

    if DB_ENGINE == "sqlite":
        await _pool.close()
    else:
        _pool.close()
        await _pool.wait_closed()

    _pool = None
