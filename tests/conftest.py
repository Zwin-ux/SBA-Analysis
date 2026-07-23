from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

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

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sba_sample_raw.csv"


@pytest.fixture
def raw_sample() -> pd.DataFrame:
    return pd.read_csv(FIXTURE_PATH, dtype=str, keep_default_na=True, low_memory=False)


@pytest.fixture
def cleaned_sample(raw_sample: pd.DataFrame) -> pd.DataFrame:
    df = standardize_column_names(raw_sample.copy())
    df = trim_whitespace(df)
    df = replace_empty_strings_with_nulls(df)
    df = add_derived_columns(df)
    df, _ = remove_duplicates(df)
    df = convert_numeric_columns(df, NUMERIC_COLUMNS)
    df = convert_date_columns(df, DATE_COLUMNS)
    return normalize_contract_categories(df)
