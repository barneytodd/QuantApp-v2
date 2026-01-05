from app.data_ingestion.validators import (
    calculate_coverage,
    detect_gaps,
)


def test_calculate_coverage_full(full_price_df, date_range):
    start, end = date_range
    coverage = calculate_coverage(full_price_df, start, end)

    assert coverage == 1.0


def test_calculate_coverage_with_gap(gappy_price_df, date_range):
    start, end = date_range
    coverage = calculate_coverage(gappy_price_df, start, end)

    assert coverage < 1.0


def test_detect_gaps_returns_missing_dates(gappy_price_df, date_range):
    start, end = date_range
    gaps = detect_gaps(gappy_price_df, start, end)

    assert len(gaps) == 1
