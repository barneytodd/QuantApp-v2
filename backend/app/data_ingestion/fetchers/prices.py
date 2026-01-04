import time
import yfinance as yf
from datetime import date

from app.data_ingestion.types import FetchRequest, FetchResult
from app.data_ingestion.executors import run_in_yf_executor

def _download_sync(req: FetchRequest):
    """Synchronous yfinance download for a single symbol"""
    symbol = req.symbols[0] 
    df = yf.download(
        tickers=symbol,
        start=req.start,
        end=req.end,
        interval=req.interval,
        group_by="ticker",
        auto_adjust=req.auto_adjust,
        progress=False,
        threads=False,  
    )
    return df

async def fetch_prices(req: FetchRequest) -> FetchResult:
    """Async fetch wrapper that returns FetchResult"""
    t0 = time.perf_counter()
    try:
        df = await run_in_yf_executor(_download_sync, req)
        empty = df is None or df.empty
        exc = None
    except Exception as e:
        df = None
        empty = True
        exc = e

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return FetchResult(
        request=req,
        data=df,
        empty=empty,
        exception=exc,
        elapsed_ms=elapsed_ms,
    )
