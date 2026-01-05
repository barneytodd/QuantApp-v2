import pytest_asyncio
import asyncio
from app.db import async_pool, get_connection, release_connection

# DB pool fixture (function-scoped to match pytest-asyncio event_loop)
@pytest_asyncio.fixture
async def test_db_pool():
    """Initialize async DB pool for tests."""
    # Use current running loop
    loop = asyncio.get_running_loop()
    await async_pool.init_db_pool()
    yield async_pool._pool
    await async_pool.close_db_pool()

# DB connection fixture
@pytest_asyncio.fixture
async def db_connection(test_db_pool):
    conn = await get_connection()
    try:
        yield conn
    finally:
        await release_connection(conn)

# Table cleanup fixture
@pytest_asyncio.fixture
async def clean_prices_table(db_connection):
    async with db_connection.cursor() as cur:
        await cur.execute("DELETE FROM dbo.prices;")
        await db_connection.commit() 
    yield