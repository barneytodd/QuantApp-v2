import pytest
from datetime import datetime, timedelta
from app.schemas.prices.price_row import PriceDataRow
from app.db.crud.insert_prices import bulk_insert_prices_chunked
from app.db.crud.get_prices import get_prices  # your async get_prices generator

@pytest.mark.asyncio
async def test_get_prices_basic(clean_test_prices, test_symbol_prefix):
    """Fetch simple inserted rows."""
    base_date = datetime(2026, 1, 1)
    symbol = f"{test_symbol_prefix}_AAPL"

    # Insert 5 consecutive days
    rows = [
        PriceDataRow(
            symbol=symbol,
            date=base_date + timedelta(days=i),
            open=100 + i,
            high=101 + i,
            low=99 + i,
            close=100 + i,
            volume=1000 + i
        )
        for i in range(5)
    ]
    await bulk_insert_prices_chunked(rows)

    # Fetch them
    fetched = [r async for r in get_prices(symbols=[symbol], start=base_date, end=base_date+timedelta(days=4))]

    # Assertions
    assert len(fetched) == 5
    assert all(r.symbol == symbol for r in fetched)
    assert [r.date for r in fetched] == [base_date + timedelta(days=i) for i in range(5)]
    assert [r.close for r in fetched] == [100 + i for i in range(5)]


@pytest.mark.asyncio
async def test_get_prices_with_date_filter(clean_test_prices, test_symbol_prefix):
    """Fetch only a subset of inserted rows using start/end dates."""
    base_date = datetime(2026, 1, 1)
    symbol = f"{test_symbol_prefix}_MSFT"

    rows = [
        PriceDataRow(
            symbol=symbol,
            date=base_date + timedelta(days=i),
            open=200 + i,
            high=201 + i,
            low=199 + i,
            close=200 + i,
            volume=2000 + i
        )
        for i in range(5)
    ]
    await bulk_insert_prices_chunked(rows)

    # Fetch middle 3 days
    fetched = [r async for r in get_prices(symbols=[symbol], start=base_date + timedelta(days=1), end=base_date + timedelta(days=3))]

    assert len(fetched) == 3
    assert [r.date for r in fetched] == [base_date + timedelta(days=i) for i in range(1, 4)]


@pytest.mark.asyncio
async def test_get_prices_multiple_symbols(clean_test_prices, test_symbol_prefix):
    """Fetch multiple symbols and ensure ordering and separation."""
    base_date = datetime(2026, 1, 1)
    symbols = [f"{test_symbol_prefix}_AAA", f"{test_symbol_prefix}_BBB"]

    for sym in symbols:
        rows = [
            PriceDataRow(
                symbol=sym,
                date=base_date + timedelta(days=i),
                open=50 + i,
                high=51 + i,
                low=49 + i,
                close=50 + i,
                volume=500 + i
            )
            for i in range(3)
        ]
        await bulk_insert_prices_chunked(rows)

    fetched = [r async for r in get_prices(symbols=symbols, start=base_date, end=base_date+timedelta(days=2))]

    # Verify that all rows are returned
    assert len(fetched) == 6
    # Verify ordering by symbol and date if your function does that
    assert fetched[0].symbol in symbols
    assert fetched[-1].symbol in symbols


@pytest.mark.asyncio
async def test_get_prices_lookback(clean_test_prices, test_symbol_prefix):
    base_date = datetime(2026, 1, 10)
    symbol = f"{test_symbol_prefix}_LOOK"

    # Insert 10 rows
    rows = [
        PriceDataRow(
            symbol=symbol,
            date=base_date - timedelta(days=9-i),  # 10 rows ending at base_date
            open=10 + i,
            high=11 + i,
            low=9 + i,
            close=10 + i,
            volume=100 + i
        )
        for i in range(10)
    ]
    await bulk_insert_prices_chunked(rows)

    # Fetch last 5 rows using lookback
    fetched = [r async for r in get_prices(symbols=[symbol], start=base_date, end=base_date, lookback=5)]

    assert len(fetched) == 5
    # Should be the last 5 dates
    expected_dates = [base_date - timedelta(days=i) for i in reversed(range(5))]
    assert [r.date for r in fetched] == expected_dates
