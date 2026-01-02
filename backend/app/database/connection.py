import app.database.async_pool as async_pool


async def get_connection():
    """
    Acquire a database connection from the pool.
    """
    if async_pool._pool is None:
        raise RuntimeError("Database pool not initialized")

    return async_pool._pool if not hasattr(async_pool._pool, "acquire") else await async_pool._pool.acquire()


async def release_connection(conn):
    """
    Release a database connection back to the pool.
    """
    if async_pool._pool and hasattr(async_pool._pool, "release"):
        await async_pool._pool.release(conn)
