import pytest
from datetime import datetime, timedelta
from app.schemas.prices.price_row import PriceDataRow
from app.db.crud.insert_prices import bulk_insert_prices_chunked

@pytest.mark.asyncio
async def test_bulk_insert_basic(clean_test_prices, test_symbol_prefix):
    """Insert 5 unique rows into an empty table."""
    base_date = datetime(2026, 1, 1)
    rows = [
        PriceDataRow(
            symbol=f"{test_symbol_prefix}",
            date=base_date + timedelta(days=i),
            open=100 + i,
            high=101 + i,
            low=99 + i,
            close=100 + i,
            volume=1000 + i
        )
        for i in range(5)
    ]

    inserted = await bulk_insert_prices_chunked(rows)
    assert inserted == 5, "All 5 unique rows should be inserted"

@pytest.mark.asyncio
async def test_bulk_insert_duplicates(clean_test_prices, test_symbol_prefix):
    """Re-inserting the same rows should insert 0 new rows."""
    base_date = datetime(2026, 1, 1)
    rows = [
        PriceDataRow(
            symbol=f"{test_symbol_prefix}",
            date=base_date + timedelta(days=i),
            open=100 + i,
            high=101 + i,
            low=99 + i,
            close=100 + i,
            volume=1000 + i
        )
        for i in range(5)
    ]

    # First insert
    await bulk_insert_prices_chunked(rows)
    # Second insert (duplicates)
    inserted = await bulk_insert_prices_chunked(rows)
    assert inserted == 0, "Duplicate rows should be skipped"

@pytest.mark.asyncio
async def test_bulk_insert_chunked(clean_test_prices, test_symbol_prefix):
    """Insert 15 rows with 5 unique keys in chunks of 5."""
    base_date = datetime(2026, 1, 1)
    # Generate 15 rows, but only 5 unique (symbol, date) pairs
    rows = [
        PriceDataRow(
            symbol=f"{test_symbol_prefix}",
            date=base_date + timedelta(days=i % 5),
            open=100 + i,
            high=101 + i,
            low=99 + i,
            close=100 + i,
            volume=1000 + i
        )
        for i in range(15)
    ]

    inserted = await bulk_insert_prices_chunked(rows, chunk_size=5)
    assert inserted
