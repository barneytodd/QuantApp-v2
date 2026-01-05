import pytest
from app.db import async_pool
from app.db.connection import get_connection

@pytest.mark.asyncio
async def test_pool_is_initialized(test_db_pool):
    assert async_pool._pool is not None, "DB pool should be initialized"

@pytest.mark.asyncio
async def test_get_connection_returns_conn(db_connection):
    conn = db_connection
    assert conn is not None, "get_connection should return a valid connection"
    assert hasattr(conn, "cursor"), "Connection should have a cursor method"

@pytest.mark.asyncio
async def test_connection_executes_query(db_connection):
    conn = db_connection
    async with conn.cursor() as cur:
        await cur.execute("SELECT 1")
        row = await cur.fetchone()
    assert row[0] == 1
