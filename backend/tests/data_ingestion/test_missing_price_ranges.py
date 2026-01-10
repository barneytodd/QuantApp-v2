import pandas as pd
from datetime import date
from app.core.dates import trading_days
from app.data_ingestion.utils import get_missing_date_ranges


def test_missing_ranges_all_missing():
    start = date(2026, 1, 5)
    end = date(2026, 1, 9)  # Mon–Fri

    existing = pd.DatetimeIndex([])

    ranges = get_missing_date_ranges(existing, start, end)

    assert ranges == [(start, end)]


def test_missing_ranges_none_missing():
    start = date(2026, 1, 5)
    end = date(2026, 1, 9)

    existing = trading_days(start, end)

    ranges = get_missing_date_ranges(existing, start, end)

    assert ranges == []


def test_missing_ranges_single_day():
    start = date(2026, 1, 5)
    end = date(2026, 1, 9)

    # Missing Wednesday
    existing = pd.to_datetime([
        "2026-01-05",
        "2026-01-06",
        "2026-01-08",
        "2026-01-09",
    ])

    ranges = get_missing_date_ranges(existing, start, end)

    assert ranges == [(date(2026, 1, 7), date(2026, 1, 7))]


def test_missing_ranges_contiguous_block():
    start = date(2026, 1, 5)
    end = date(2026, 1, 9)

    # Missing Tue–Thu
    existing = pd.to_datetime([
        "2026-01-05",
        "2026-01-09",
    ])

    ranges = get_missing_date_ranges(existing, start, end)

    assert ranges == [(date(2026, 1, 6), date(2026, 1, 8))]


def test_missing_ranges_multiple_blocks():
    start = date(2026, 1, 5)
    end = date(2026, 1, 16)

    existing = pd.to_datetime([
        "2026-01-05",
        "2026-01-06",
        "2026-01-09",
        "2026-01-13",
        "2026-01-16",
    ])

    ranges = get_missing_date_ranges(existing, start, end)

    assert ranges == [
        (date(2026, 1, 7), date(2026, 1, 8)),
        (date(2026, 1, 12), date(2026, 1, 12)),
        (date(2026, 1, 14), date(2026, 1, 15)),
    ]


def test_missing_ranges_skip_weekends():
    start = date(2026, 1, 9)   # Friday
    end = date(2026, 1, 12)    # Monday

    # Missing Monday only
    existing = pd.to_datetime(["2026-01-09"])

    ranges = get_missing_date_ranges(existing, start, end)

    assert ranges == [(date(2026, 1, 12), date(2026, 1, 12))]
