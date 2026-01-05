import pandas as pd

from app.data_ingestion.validators import (
    calculate_coverage,
    detect_gaps,
    validate_columns,
    validate_index_monotonic
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


def test_detect_gaps_full_data_returns_empty(full_price_df, date_range):
    start, end = date_range
    gaps = detect_gaps(full_price_df, start, end)
    assert gaps == []


def test_detect_gaps_empty_df_returns_all_dates(date_range):
    start, end = date_range
    df = pd.DataFrame()
    gaps = detect_gaps(df, start, end)
    expected_days = pd.bdate_range(start, end).to_pydatetime()
    expected_dates = [d.date() for d in expected_days]
    assert gaps == expected_dates


def test_check_required_columns_true(full_price_df):
    assert validate_columns(full_price_df) is True


def test_check_required_columns_false_missing_column(full_price_df):
    df = full_price_df.drop(columns=["Close"])
    assert validate_columns(df) is False


def test_check_required_columns_empty_df():
    df = pd.DataFrame()
    assert validate_columns(df) is False


def test_validate_index_monotonic_true(full_price_df):
    assert validate_index_monotonic(full_price_df) is True


def test_validate_index_monotonic_false_non_monotonic():
    df = pd.DataFrame(
        {"Open": [100, 101], "Close": [100.5, 101.5]},
        index=pd.to_datetime(["2023-01-03", "2023-01-02"])
    )
    assert validate_index_monotonic(df) is False


def test_validate_index_monotonic_false_non_datetime_index():
    df = pd.DataFrame(
        {"Open": [100, 101], "Close": [100.5, 101.5]},
        index=[1, 2]
    )
    assert validate_index_monotonic(df) is False
