# app/db/connection.py

from .async_pool import _pool


async def get_connection():
    """
    Acquire a database connection from the pool.
    """
    if _pool is None:
        raise RuntimeError("Database pool not initialized")

    return _pool if not hasattr(_pool, "acquire") else await _pool.acquire()


async def release_connection(conn):
    """
    Release a database connection back to the pool.
    """
    if _pool and hasattr(_pool, "release"):
        await _pool.release(conn)
