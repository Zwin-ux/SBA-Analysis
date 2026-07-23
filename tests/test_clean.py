from __future__ import annotations

import pandas as pd

from src.clean import (
    DATE_COLUMNS,
    NUMERIC_COLUMNS,
    add_derived_columns,
    convert_date_columns,
    convert_numeric_columns,
    remove_duplicates,
    replace_empty_strings_with_nulls,
    standardize_column_names,
    trim_whitespace,
)
from src.contracts import normalize_contract_categories


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
