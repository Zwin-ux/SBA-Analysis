from __future__ import annotations

import pandas as pd
import pytest

from ml.features import (
    LEAKAGE_DENYLIST,
    TARGET_COLUMN,
    LeakageError,
    assert_no_leakage,
    build_feature_matrix,
    prepare_modeling_frame,
)


def test_input_contains_denylisted_columns(synthetic_loans: pd.DataFrame) -> None:
    """The fixture must carry outcome columns, or these tests prove nothing."""
    present = LEAKAGE_DENYLIST.intersection(synthetic_loans.columns)
    assert {"charge_off_amount", "charge_off_date", "paid_in_full_date", "loan_status"} <= present


def test_denylisted_columns_are_excluded_from_feature_matrix(
    synthetic_loans: pd.DataFrame,
) -> None:
    frame = prepare_modeling_frame(synthetic_loans)
    features, _ = build_feature_matrix(frame)

    assert not LEAKAGE_DENYLIST.intersection(features.columns)
    assert TARGET_COLUMN not in features.columns


@pytest.mark.parametrize("denied_column", sorted(LEAKAGE_DENYLIST))
def test_assert_no_leakage_raises_for_each_denylisted_column(denied_column: str) -> None:
    with pytest.raises(LeakageError, match=denied_column):
        assert_no_leakage(["loan_amount", denied_column])


def test_assert_no_leakage_accepts_clean_columns() -> None:
    assert_no_leakage(["loan_amount", "program", "borrower_state"])


def test_transformed_feature_names_reference_no_outcome_fields(
    synthetic_loans: pd.DataFrame,
) -> None:
    pytest.importorskip("sklearn")
    from ml.train import build_preprocessor

    frame = prepare_modeling_frame(synthetic_loans)
    features, _ = build_feature_matrix(frame)
    preprocessor = build_preprocessor()
    preprocessor.fit(features)

    transformed_names = [str(name) for name in preprocessor.get_feature_names_out()]
    for name in transformed_names:
        for denied in LEAKAGE_DENYLIST:
            assert denied not in name
