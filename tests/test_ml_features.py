from __future__ import annotations

import pandas as pd
import pytest

from ml.features import (
    FEATURE_ALLOWLIST,
    LEAKAGE_DENYLIST,
    TARGET_COLUMN,
    build_feature_matrix,
    derive_target,
    prepare_modeling_frame,
)


def test_target_mapping_and_exclusions() -> None:
    df = pd.DataFrame(
        {
            "loan_status": ["PIF", "CHGOFF", "CANCLD", "EXEMPT", None, " pif "],
            "loan_amount": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }
    )

    frame = prepare_modeling_frame(df)

    assert list(frame[TARGET_COLUMN]) == [0, 1, 0]
    assert list(frame["loan_amount"]) == [1.0, 2.0, 6.0]


def test_derive_target_requires_loan_status() -> None:
    with pytest.raises(KeyError, match="loan_status"):
        derive_target(pd.DataFrame({"loan_amount": [1.0]}))


def test_feature_matrix_contains_only_allowlisted_columns(
    synthetic_loans: pd.DataFrame,
) -> None:
    frame = prepare_modeling_frame(synthetic_loans)
    features, target = build_feature_matrix(frame)

    assert tuple(features.columns) == FEATURE_ALLOWLIST
    assert len(features) == len(frame)
    assert set(target.unique()) <= {0, 1}


def test_feature_matrix_requires_target_column(synthetic_loans: pd.DataFrame) -> None:
    with pytest.raises(KeyError, match=TARGET_COLUMN):
        build_feature_matrix(synthetic_loans)


def test_derived_features_are_bounded(synthetic_loans: pd.DataFrame) -> None:
    frame = prepare_modeling_frame(synthetic_loans)
    features, _ = build_feature_matrix(frame)

    share = features["guaranteed_share"].dropna()
    assert ((share >= 0) & (share <= 1)).all()

    sectors = features["naics_sector"].dropna()
    assert sectors.map(lambda value: len(str(value)) == 2).all()


def test_allowlist_and_denylist_are_disjoint() -> None:
    assert not LEAKAGE_DENYLIST.intersection(FEATURE_ALLOWLIST)
