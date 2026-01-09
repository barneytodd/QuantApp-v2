import pytest
from app.main import app
import app.db.async_pool as async_pool
from app.data_ingestion import orchestrator as orch
from app.api.routes.data import prices as prices_module
import pandas as pd

@pytest.fixture(autouse=True)
def override_dependencies(monkeypatch):
    """Override orchestrator, DB pool, and background ingestion for API tests."""

    class DummyResult:
        def __init__(self):
            self.data = [{"some": "value"}]
            self.exception = None

    # --- 1️⃣ Dummy orchestrator ---
    async def dummy_orchestrator(*args, **kwargs):
        row = {
            "symbol": "AAPL",
            "Open": 100.0,
            "High": 110.0,
            "Low": 90.0,
            "Close": 105.0,
            "Volume": 1000,
            "Date": "2023-01-01",
            "result": DummyResult(),
            "attempts": 1,
            "retry_reason": "none",
            "coverage": 1.0,
            "missing_dates": [],
            "elapsed_ms": 50
        }
        return {"AAPL": 10}, [[row]]

    monkeypatch.setattr("app.api.routes.data.prices.orchestrate_fetch_and_insert", dummy_orchestrator)
    app.dependency_overrides["app.api.routes.data.prices.orchestrate_fetch_and_insert"] = dummy_orchestrator

    # --- 2️⃣ Dummy DB pool ---
    class DummyCursor:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc_val, exc_tb): return None
        async def execute(self, sql, params=None): return self
        async def fetchall(self):
            # Return one dummy row
            return [("AAPL", "2023-01-01", 100.0, 110.0, 90.0, 105.0, 1000)]
        def __aiter__(self):
            async def gen():
                yield ("AAPL", "2023-01-01", 100.0, 110.0, 90.0, 105.0, 1000)
            return gen()

    class DummyConn:
        def cursor(self): return DummyCursor()

    class DummyPool:
        async def acquire(self): return DummyConn()
        async def release(self, conn): return

    async_pool._pool = DummyPool()

    # --- 3️⃣ Dummy background ingestion ---
    async def dummy_bg_task(req):
        return
    monkeypatch.setattr(prices_module, "run_background_ingestion", dummy_bg_task)

    yield

    # Cleanup
    async_pool._pool = None
    app.dependency_overrides = {}
