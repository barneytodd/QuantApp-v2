import pytest
from httpx import AsyncClient
from app.schemas import GetPricesPayload, PriceDataRow
from app.main import app


@pytest.mark.anyio
async def test_get_prices_single():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/market-data/ohlcv/AAPL?start=2023-01-01&end=2023-01-31")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data[0]["symbol"] == "AAPL"


@pytest.mark.anyio
async def test_get_prices_bulk():
    payload = {"symbols": ["AAPL", "MSFT"], "start": "2023-01-01", "end": "2023-01-31"}
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/market-data/ohlcv/query", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        row = data[0]
        assert "symbol" in row
        assert "date" in row


@pytest.mark.anyio
async def test_ingest_prices():
    payload = {
        "symbols": ["AAPL"],
        "start": "2023-01-01",
        "end": "2023-01-31",
        "interval": "1d",
        "max_attempts": 3,
        "max_concurrent": 2,
        "coverage_threshold": 0.95,
        "dry_run": False
    }
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/market-data/ingest", json=payload)
    assert response.status_code == 200
    json_data = response.json()
    assert json_data["total_symbols"] == 1
    assert json_data["succeeded"] == 1
    assert "results" in json_data
    result = json_data["results"][0]
    assert result["symbol"] == "AAPL"
    assert result["success"] is True


@pytest.mark.anyio
async def test_ingest_prices_async():
    payload = {
        "symbols": ["AAPL"],
        "start": "2023-01-01",
        "end": "2023-01-31",
        "interval": "1d",
        "max_attempts": 3,
        "max_concurrent": 2,
        "coverage_threshold": 0.95,
        "dry_run": False
    }
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/market-data/ingest/async", json=payload)
    assert response.status_code == 202
    json_data = response.json()
    assert json_data["status"] == "accepted"
    assert json_data["symbols"] == ["AAPL"]
