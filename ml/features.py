"""Approval-time feature policy and target derivation for the charge-off baseline.

Every feature decision here is documented in docs/MODEL_CARD.md. The policy is
enforced, not advisory: denylisted columns raise if they reach the model matrix,
and the matrix is built from an explicit allowlist rather than "everything else".
"""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

TARGET_COLUMN = "charged_off"

# Terminal outcomes only. CANCLD (cancelled) and EXEMPT never entered repayment
# in a comparable way; missing/other statuses are treated as unresolved and are
# excluded rather than guessed.
LABEL_MAP = {"PIF": 0, "CHGOFF": 1}

# Outcome or outcome-derived fields. These may legitimately exist in the cleaned
# analytical dataset, but they must never enter the model matrix.
LEAKAGE_DENYLIST: frozenset[str] = frozenset(
    {
        "charge_off_amount",
        "charge_off_date",
        "paid_in_full_date",
        "loan_status",
        TARGET_COLUMN,
    }
)

# Fields available at approval time, per docs/DATA_DICTIONARY.md.
# `approval_fiscal_year` is deliberately absent: it is the temporal split key,
# and letting models memorize calendar years does not transfer to future years.
# `jobs_supported` is deliberately absent: the data dictionary classifies it as
# source-reported and descriptive, not a verified approval-time attribute.
NUMERIC_FEATURES = (
    "loan_amount",
    "guaranteed_share",
    "term_in_months",
    "initial_interest_rate",
)
CATEGORICAL_FEATURES = (
    "program",
    "borrower_state",
    "naics_sector",
)
FEATURE_ALLOWLIST: tuple[str, ...] = NUMERIC_FEATURES + CATEGORICAL_FEATURES

# The two policies must never intersect; fail at import time if they ever do.
_OVERLAP = LEAKAGE_DENYLIST.intersection(FEATURE_ALLOWLIST)
if _OVERLAP:  # pragma: no cover - guarded by test_allowlist_and_denylist_are_disjoint
    raise RuntimeError(f"Feature allowlist overlaps the leakage denylist: {sorted(_OVERLAP)}")


class LeakageError(ValueError):
    """Raised when an outcome-derived column reaches the model matrix."""


def assert_no_leakage(columns: Iterable[str]) -> None:
    """Raise LeakageError if any denylisted column is present."""
    leaked = sorted(LEAKAGE_DENYLIST.intersection(columns))
    if leaked:
        raise LeakageError(
            f"Outcome-derived column(s) {leaked} must not enter the model matrix. "
            "See LEAKAGE_DENYLIST in ml/features.py and docs/MODEL_CARD.md."
        )


def derive_target(df: pd.DataFrame) -> pd.Series:
    """Map loan_status to the binary charge-off target (PIF=0, CHGOFF=1)."""
    if "loan_status" not in df.columns:
        raise KeyError("Column 'loan_status' is required to derive the charge-off target.")
    status = df["loan_status"].astype("string").str.strip().str.upper()
    return status.map(LABEL_MAP)


def prepare_modeling_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return only rows with a terminal outcome, with the target column attached.

    Rows whose loan_status is not PIF or CHGOFF (cancelled, exempt, missing,
    or still active) are excluded rather than labeled.
    """
    target = derive_target(df)
    resolved = df.loc[target.notna()].copy()
    resolved[TARGET_COLUMN] = target.loc[resolved.index].astype("int64")
    return resolved


def _derive_guaranteed_share(df: pd.DataFrame) -> pd.Series:
    """SBA-guaranteed share of the approval, an approval-time ratio in [0, 1]."""
    loan_amount = pd.to_numeric(df.get("loan_amount"), errors="coerce")
    guaranteed = pd.to_numeric(df.get("sba_guaranteed_approval"), errors="coerce")
    share = guaranteed / loan_amount
    return share.where((share >= 0) & (share <= 1))


def _derive_naics_sector(df: pd.DataFrame) -> pd.Series:
    """Two-digit NAICS sector prefix, kept as text."""
    if "naics_code" not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="string")
    return df["naics_code"].astype("string").str.strip().str[:2]


def build_feature_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Build (X, y) from a modeling frame using only allowlisted columns.

    The input may contain outcome columns (they exist in the cleaned dataset);
    they are used for nothing and are structurally excluded from X. The returned
    X is verified against the denylist as a final guard.
    """
    if TARGET_COLUMN not in df.columns:
        raise KeyError(
            f"Column '{TARGET_COLUMN}' is missing. Call prepare_modeling_frame() first."
        )

    derived = df.copy()
    derived["guaranteed_share"] = _derive_guaranteed_share(derived)
    derived["naics_sector"] = _derive_naics_sector(derived)

    features = pd.DataFrame(index=derived.index)
    for column in NUMERIC_FEATURES:
        features[column] = pd.to_numeric(derived.get(column), errors="coerce").astype("float64")
    for column in CATEGORICAL_FEATURES:
        source = derived.get(column)
        if source is None:
            source = pd.Series(pd.NA, index=derived.index)
        as_string = source.astype("string")
        # Plain object dtype with np.nan keeps sklearn imputers happy.
        features[column] = as_string.astype(object).where(as_string.notna(), np.nan)

    assert_no_leakage(features.columns)
    target = derived[TARGET_COLUMN].astype("int64")
    return features, target
