"""Temporal train/validation/test split by SBA approval fiscal year.

The primary evaluation is always future-time: train on the earliest years,
validate on the middle years, and test on the newest years. There is no random
shuffling anywhere in the split; the only inputs are the year column and the
requested row fractions.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

YEAR_COLUMN = "approval_fiscal_year"


@dataclass(frozen=True)
class TemporalSplit:
    """A year-partitioned split with no year shared between subsets."""

    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    train_years: tuple[int, ...]
    validation_years: tuple[int, ...]
    test_years: tuple[int, ...]

    def summary(self) -> dict[str, dict[str, int]]:
        return {
            "train": {
                "rows": int(len(self.train)),
                "min_year": min(self.train_years),
                "max_year": max(self.train_years),
            },
            "validation": {
                "rows": int(len(self.validation)),
                "min_year": min(self.validation_years),
                "max_year": max(self.validation_years),
            },
            "test": {
                "rows": int(len(self.test)),
                "min_year": min(self.test_years),
                "max_year": max(self.test_years),
            },
        }


def temporal_split(
    df: pd.DataFrame,
    *,
    year_column: str = YEAR_COLUMN,
    train_fraction: float = 0.6,
    validation_fraction: float = 0.2,
) -> TemporalSplit:
    """Split rows by fiscal year so train years < validation years < test years.

    Year boundaries are chosen from cumulative row counts so the subsets
    approximate the requested fractions while whole years stay together.
    Raises ValueError when fewer than three distinct years are available.
    """
    if year_column not in df.columns:
        raise KeyError(f"Split column '{year_column}' is missing from the dataset.")
    if not 0 < train_fraction < 1 or not 0 < validation_fraction < 1:
        raise ValueError("Split fractions must be between 0 and 1.")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train_fraction + validation_fraction must leave room for a test set.")

    years = pd.to_numeric(df[year_column], errors="coerce")
    known = df.loc[years.notna()].copy()
    known["_split_year"] = years.loc[known.index].astype("int64")

    distinct_years = sorted(known["_split_year"].unique())
    if len(distinct_years) < 3:
        raise ValueError(
            "Temporal split needs at least three distinct approval fiscal years; "
            f"found {len(distinct_years)}."
        )

    counts = known["_split_year"].value_counts().reindex(distinct_years)
    cumulative = counts.cumsum() / counts.sum()

    train_years = [year for year in distinct_years if cumulative[year] <= train_fraction]
    if not train_years:
        train_years = [distinct_years[0]]

    remaining = [year for year in distinct_years if year not in train_years]
    validation_cutoff = train_fraction + validation_fraction
    validation_years = [year for year in remaining if cumulative[year] <= validation_cutoff]
    if not validation_years:
        validation_years = [remaining[0]]

    test_years = [year for year in remaining if year not in validation_years]
    if not test_years:
        raise ValueError(
            "Temporal split produced an empty test set; "
            "use smaller train/validation fractions or provide more years."
        )

    subsets = {}
    for name, subset_years in (
        ("train", train_years),
        ("validation", validation_years),
        ("test", test_years),
    ):
        mask = known["_split_year"].isin(subset_years)
        subsets[name] = known.loc[mask].drop(columns=["_split_year"])

    return TemporalSplit(
        train=subsets["train"],
        validation=subsets["validation"],
        test=subsets["test"],
        train_years=tuple(int(year) for year in train_years),
        validation_years=tuple(int(year) for year in validation_years),
        test_years=tuple(int(year) for year in test_years),
    )
