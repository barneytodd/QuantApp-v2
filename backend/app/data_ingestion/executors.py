import asyncio
from concurrent.futures import ThreadPoolExecutor

_YF_EXECUTOR: ThreadPoolExecutor | None = None

def get_yfinance_executor() -> ThreadPoolExecutor:
    """Singleton executor for all yfinance downloads"""
    global _YF_EXECUTOR
    if _YF_EXECUTOR is None:
        _YF_EXECUTOR = ThreadPoolExecutor(
            max_workers=4,  
            thread_name_prefix="yfinance"
        )
    return _YF_EXECUTOR

async def run_in_yf_executor(func, *args, **kwargs):
    """Async wrapper to run a synchronous function in the executor"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        get_yfinance_executor(),
        lambda: func(*args, **kwargs)
    )
