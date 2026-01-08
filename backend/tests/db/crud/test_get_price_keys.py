import pytest
from datetime import datetime, timedelta, date
from app.schemas.prices.price_row import PriceDataRow
from app.db.crud import bulk_insert_prices_chunked, get_price_keys

@pytest.mark.asyncio
async def test_get_price_keys_empty_db(test_symbol_prefix, clean_test_prices):
    """Returns empty set if DB has no matching rows."""
    keys = await get_price_keys(test_symbol_prefix, date(2026, 1, 1), date(2026, 1, 10))
    assert keys == set()

@pytest.mark.asyncio
async def test_get_price_keys_single_symbol(test_symbol_prefix, clean_test_prices):
    """Returns correct keys for a single symbol."""
    base_date = datetime(2026, 1, 1)
    symbol = f"{test_symbol_prefix}_SINGLE"

    # Insert 5 rows
    rows = [
        PriceDataRow(
            symbol=symbol,
            date=base_date + timedelta(days=i),
            open=100+i,
            high=101+i,
            low=99+i,
            close=100+i,
            volume=1000+i
        )
        for i in range(5)
    ]
    await bulk_insert_prices_chunked(rows)

    keys = await get_price_keys(symbol, base_date.date(), (base_date + timedelta(days=4)).date())
    expected = {(symbol, (base_date + timedelta(days=i)).date()) for i in range(5)}
    assert keys == expected


@pytest.mark.asyncio
async def test_get_price_keys_partial_range(test_symbol_prefix, clean_test_prices):
    """Ignores rows outside the specified date range."""
    base_date = datetime(2026, 1, 1)
    symbol = f"{test_symbol_prefix}_PARTIAL"

    rows = [
        PriceDataRow(
            symbol=symbol,
            date=base_date + timedelta(days=i),
            open=100+i,
            high=101+i,
            low=99+i,
            close=100+i,
            volume=1000+i
        )
        for i in range(5)
    ]
    await bulk_insert_prices_chunked(rows)

    # Only fetch last 3 dates
    start = (base_date + timedelta(days=2)).date()
    end = (base_date + timedelta(days=4)).date()
    keys = await get_price_keys(symbol, start, end)
    expected = {(symbol, start), (symbol, start + timedelta(days=1)), (symbol, start + timedelta(days=2))}
    assert keys == expected

@pytest.mark.asyncio
async def test_get_price_keys_empty_symbols(clean_test_prices):
    """Returns empty set if symbol list is empty."""
    keys = await get_price_keys("", date(2026, 1, 1), date(2026, 1, 10))
    assert keys == set()
