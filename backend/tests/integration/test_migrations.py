import pytest
from app.db.connection import get_connection, release_connection
from scripts import run_migrations

@pytest.mark.asyncio
async def test_migrations_apply_cleanly(test_db_pool):
    """Apply migrations and verify schema_migrations table."""
    await run_migrations.run_migrations(pool=test_db_pool)

    conn = await get_connection()
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM schema_migrations")
            row = await cur.fetchone()
    finally:
        await release_connection(conn)

    assert row[0] > 0, "At least one migration should be applied"

@pytest.mark.asyncio
async def test_reserved_keywords_migration(db_connection):
    """Ensure reserved keyword columns exist."""
    conn = db_connection
    async with conn.cursor() as cur:
        await cur.execute("SELECT TOP 1 [open], [close] FROM prices")
        row = await cur.fetchone()

    # row may be None if table empty
    assert row is None or len(row) == 2
