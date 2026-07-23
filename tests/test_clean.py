from __future__ import annotations

import pandas as pd
import pytest

import src.clean as clean_module
from src.clean import (
    DATE_COLUMNS,
    NUMERIC_COLUMNS,
    add_derived_columns,
    build_data_quality_summary,
    clean_dataframe,
    clean_dataset,
    convert_date_columns,
    convert_numeric_columns,
    load_raw_files,
    remove_duplicates,
    replace_empty_strings_with_nulls,
    save_clean_dataset,
    standardize_column_names,
    trim_whitespace,
)
from src.contracts import normalize_contract_categories
from src.quality import dataset_fingerprint


def test_cleaning_pipeline_normalizes_public_sba_extract(raw_sample: pd.DataFrame) -> None:
    df = standardize_column_names(raw_sample.copy())
    assert "loan_amount" in df.columns
    assert "approval_fiscal_year" in df.columns

    df = trim_whitespace(df)
    df = replace_empty_strings_with_nulls(df)
    df = add_derived_columns(df)
    df, duplicate_count = remove_duplicates(df)
    df = convert_numeric_columns(df, NUMERIC_COLUMNS)
    df = convert_date_columns(df, DATE_COLUMNS)
    df = normalize_contract_categories(df)

    assert duplicate_count == 1
    assert len(df) == 4
    assert set(df["program"].dropna()) == {"7(a)", "504"}
    assert set(df["borrower_state"].dropna()) == {"CA", "TX", "NY", "OR"}

    bakery = df.loc[df["loan_id"] == "1001"].iloc[0]
    assert bakery["borrower_name"] == "Sunrise Bakery"
    assert bakery["loan_amount"] == 250_000
    assert bakery["sba_guaranteed_approval"] == 187_500
    assert bakery["initial_interest_rate"] == 6.5
    assert bakery["approval_date"] == pd.Timestamp("2020-01-15")


def test_integer_like_columns_use_nullable_integer_dtype(cleaned_sample: pd.DataFrame) -> None:
    assert str(cleaned_sample["jobs_supported"].dtype) == "Int64"
    assert str(cleaned_sample["approval_fiscal_year"].dtype) == "Int64"
    assert str(cleaned_sample["term_in_months"].dtype) == "Int64"


def test_clean_dataframe_produces_identical_fingerprint_across_runs(
    raw_sample: pd.DataFrame,
) -> None:
    first, first_duplicates = clean_dataframe(raw_sample.copy())
    second, second_duplicates = clean_dataframe(raw_sample.copy())

    assert first_duplicates == second_duplicates == 1
    assert dataset_fingerprint(first) == dataset_fingerprint(second)


def test_clean_dataframe_fingerprint_changes_with_input(raw_sample: pd.DataFrame) -> None:
    baseline, _ = clean_dataframe(raw_sample.copy())
    altered_input = raw_sample.copy()
    altered_input.loc[altered_input.index[0], "GrossApproval"] = "$999,999"
    altered, _ = clean_dataframe(altered_input)

    assert dataset_fingerprint(baseline) != dataset_fingerprint(altered)


def test_load_raw_files_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match="No input files"):
        load_raw_files([])


def test_clean_dataset_raises_without_raw_files(monkeypatch, tmp_path) -> None:
    empty_raw = tmp_path / "raw"
    empty_downloads = tmp_path / "downloads"
    empty_raw.mkdir()
    empty_downloads.mkdir()
    monkeypatch.setattr(clean_module, "RAW_DIR", empty_raw)
    monkeypatch.setattr(clean_module, "DOWNLOADS_DIR", empty_downloads)

    with pytest.raises(FileNotFoundError, match="No raw CSV files"):
        clean_dataset()


def test_save_clean_dataset_writes_dates_as_iso(cleaned_sample: pd.DataFrame, tmp_path) -> None:
    output_path = tmp_path / "processed" / "clean.csv"
    saved_path = save_clean_dataset(cleaned_sample, output_path)

    reloaded = pd.read_csv(saved_path, dtype=str)
    assert saved_path == output_path
    assert len(reloaded) == len(cleaned_sample)
    assert reloaded["approval_date"].iloc[0] == "2020-01-15"


def test_build_data_quality_summary_reports_shape(cleaned_sample: pd.DataFrame) -> None:
    summary = build_data_quality_summary(cleaned_sample, duplicate_count=1)

    assert summary["row_count"] == 4
    assert summary["column_count"] == len(cleaned_sample.columns)
    assert summary["duplicate_rows_removed"] == 1
    assert not summary["numeric_stats"].empty
