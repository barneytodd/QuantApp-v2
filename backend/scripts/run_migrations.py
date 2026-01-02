import os
import glob
import asyncio
from app.db import async_pool
from app.db.async_pool import init_db_pool, close_db_pool, _pool
from app.db.connection import get_connection, release_connection

MIGRATIONS_DIR = "migrations"

async def run_migrations(pool=None):
    # Initialize the pool
    if pool:
        async_pool._pool = pool
    else:   
        try:
            await init_db_pool()
        except Exception as e:
            print("Failed to initialize DB pool:", e)
            return  # stop execution

    print("DB pool initialized")

    conn = None
    try:
        conn = await get_connection()
        cursor = await conn.cursor()

        # Create schema_migrations table if it doesn't exist
        await cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='schema_migrations'
            )
            CREATE TABLE dbo.schema_migrations (
                version VARCHAR(255) PRIMARY KEY,
                applied_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            )
        """)

        # Get applied migrations
        await cursor.execute("SELECT version FROM schema_migrations")
        applied = {row[0] for row in await cursor.fetchall()}

        # Apply pending migrations
        for path in sorted(glob.glob(f"{MIGRATIONS_DIR}/*.sql")):
            version = os.path.basename(path)
            if version in applied:
                continue

            print(f"Applying migration: {version}")
            with open(path) as f:
                sql = f.read()
            await cursor.execute(sql)
            await cursor.execute(
                "INSERT INTO schema_migrations (version) VALUES (?)",
                (version,)
            )

        await conn.commit()
        print("All migrations applied successfully!")

    finally:
        # Release connection and close pool
        if conn:
            await release_connection(conn)

        if pool is None:
            await close_db_pool()


if __name__ == "__main__":
    asyncio.run(run_migrations())
