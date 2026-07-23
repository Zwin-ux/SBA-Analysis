from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ml import RANDOM_SEED
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


def make_synthetic_loans(rows: int = 420, seed: int = RANDOM_SEED) -> pd.DataFrame:
    """Deterministic SYNTHETIC loans mirroring the cleaned analytical schema.

    Used only to prove ML pipeline mechanics in tests. Any metric computed from
    this frame is synthetic and must never be presented as a real-data result.
    Outcome (leakage) columns are intentionally present so tests can prove they
    are excluded from the model matrix.
    """
    rng = np.random.default_rng(seed)
    years = rng.integers(1995, 2024, size=rows)
    program = np.where(years < 2010, "504", "7(a)")
    loan_amount = np.round(rng.lognormal(mean=12.0, sigma=1.0, size=rows), 2)
    guaranteed_share = rng.uniform(0.4, 0.9, size=rows)
    term_in_months = rng.choice([60, 84, 120, 240, 300], size=rows)
    interest_rate = np.clip(rng.normal(6.5, 1.5, size=rows), 1.0, 15.0)
    states = rng.choice(["CA", "TX", "NY", "FL", "OR", "WA", "GA", "IL"], size=rows)
    naics = rng.choice(["311811", "332710", "624410", "238220", "722511", "541110"], size=rows)

    # Planted, approval-time-only signal so models have something honest to learn.
    logit = -1.9 + 0.5 * (interest_rate - 6.5) / 1.5 + 0.4 * (term_in_months < 120)
    charge_off_probability = 1.0 / (1.0 + np.exp(-logit))
    charged_off = rng.uniform(size=rows) < charge_off_probability

    loan_status = np.where(charged_off, "CHGOFF", "PIF").astype(object)
    non_terminal = rng.uniform(size=rows)
    loan_status[non_terminal < 0.06] = "CANCLD"
    loan_status[(non_terminal >= 0.06) & (non_terminal < 0.09)] = "EXEMPT"
    loan_status[(non_terminal >= 0.09) & (non_terminal < 0.12)] = None

    is_chgoff = pd.Series(loan_status, dtype="object") == "CHGOFF"
    charge_off_amount = np.where(is_chgoff, np.round(loan_amount * 0.6, 2), 0.0)
    approval_dates = pd.to_datetime(years.astype(str) + "-06-15")
    charge_off_date = pd.Series(approval_dates + pd.Timedelta(days=900)).where(is_chgoff)
    paid_in_full_date = pd.Series(approval_dates + pd.Timedelta(days=1200)).where(
        pd.Series(loan_status, dtype="object") == "PIF"
    )

    return pd.DataFrame(
        {
            "program": program,
            "loan_amount": loan_amount,
            "sba_guaranteed_approval": np.round(loan_amount * guaranteed_share, 2),
            "approval_fiscal_year": years,
            "approval_date": approval_dates,
            "term_in_months": term_in_months,
            "initial_interest_rate": np.round(interest_rate, 2),
            "borrower_state": states,
            "naics_code": naics,
            "jobs_supported": rng.integers(0, 60, size=rows),
            "loan_status": loan_status,
            "charge_off_amount": charge_off_amount,
            "charge_off_date": charge_off_date,
            "paid_in_full_date": paid_in_full_date,
        }
    )


@pytest.fixture
def synthetic_loans() -> pd.DataFrame:
    return make_synthetic_loans()
