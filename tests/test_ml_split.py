from __future__ import annotations

import pandas as pd
import pytest

from ml.features import prepare_modeling_frame
from ml.split import temporal_split


@pytest.fixture
def modeling_frame(synthetic_loans: pd.DataFrame) -> pd.DataFrame:
    return prepare_modeling_frame(synthetic_loans)


def test_no_fiscal_year_is_shared_between_splits(modeling_frame: pd.DataFrame) -> None:
    split = temporal_split(modeling_frame)

    train_years = set(split.train_years)
    validation_years = set(split.validation_years)
    test_years = set(split.test_years)

    assert not train_years & validation_years
    assert not validation_years & test_years
    assert not train_years & test_years

    assert set(split.train["approval_fiscal_year"].unique()) <= train_years
    assert set(split.validation["approval_fiscal_year"].unique()) <= validation_years
    assert set(split.test["approval_fiscal_year"].unique()) <= test_years


def test_splits_are_time_ordered(modeling_frame: pd.DataFrame) -> None:
    split = temporal_split(modeling_frame)

    assert max(split.train_years) < min(split.validation_years)
    assert max(split.validation_years) < min(split.test_years)


def test_split_partitions_all_rows(modeling_frame: pd.DataFrame) -> None:
    split = temporal_split(modeling_frame)

    total = len(split.train) + len(split.validation) + len(split.test)
    assert total == len(modeling_frame)

    combined_index = (
        set(split.train.index) | set(split.validation.index) | set(split.test.index)
    )
    assert combined_index == set(modeling_frame.index)


def test_split_summary_reports_rows_and_year_bounds(modeling_frame: pd.DataFrame) -> None:
    summary = temporal_split(modeling_frame).summary()

    for name in ("train", "validation", "test"):
        assert summary[name]["rows"] > 0
        assert summary[name]["min_year"] <= summary[name]["max_year"]
    assert summary["train"]["max_year"] < summary["validation"]["min_year"]


def test_split_discloses_rows_dropped_for_unparseable_years(
    modeling_frame: pd.DataFrame,
) -> None:
    corrupted = modeling_frame.copy()
    bad_index = corrupted.index[:3]
    corrupted.loc[bad_index, "approval_fiscal_year"] = "not-a-year"

    split = temporal_split(corrupted)

    assert split.dropped_missing_year == 3
    assert split.summary()["dropped_missing_year"] == 3
    total = len(split.train) + len(split.validation) + len(split.test)
    assert total == len(corrupted) - 3


def test_split_requires_three_distinct_years() -> None:
    df = pd.DataFrame({"approval_fiscal_year": [2020, 2020, 2021], "x": [1, 2, 3]})

    with pytest.raises(ValueError, match="three distinct"):
        temporal_split(df)


def test_split_rejects_fractions_without_test_room(modeling_frame: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="test set"):
        temporal_split(modeling_frame, train_fraction=0.9, validation_fraction=0.2)


def test_split_requires_year_column() -> None:
    with pytest.raises(KeyError, match="approval_fiscal_year"):
        temporal_split(pd.DataFrame({"x": [1, 2, 3]}))
