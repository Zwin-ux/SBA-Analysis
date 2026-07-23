"""Canonical analytical contracts for cleaned SBA loan records.

The contract intentionally focuses on fields used by the public dashboard and
future modeling work. It does not require borrower names, addresses, or other
fields that should not be necessary for recruiter-facing analysis.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ColumnContract:
    """Validation rules for one cleaned analytical column."""

    nullable: bool = True
    minimum: float | int | None = None
    maximum: float | int | None = None
    allowed_values: frozenset[str] | None = None
    pattern: str | None = None


@dataclass(frozen=True)
class ContractViolation:
    """One machine-readable data-contract violation."""

    code: str
    column: str
    count: int
    message: str
    severity: str = "error"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


PROGRAM_ALIASES = {
    "7a": "7(a)",
    "7(a)": "7(a)",
    "7 (a)": "7(a)",
    "504": "504",
    "cdc/504": "504",
    "cdc 504": "504",
}

REQUIRED_COLUMNS = (
    "program",
    "loan_amount",
    "approval_fiscal_year",
)

COLUMN_CONTRACTS: dict[str, ColumnContract] = {
    "program": ColumnContract(nullable=False, allowed_values=frozenset({"7(a)", "504"})),
    "loan_amount": ColumnContract(nullable=False, minimum=0),
    "sba_guaranteed_approval": ColumnContract(minimum=0),
    "third_party_dollars": ColumnContract(minimum=0),
    "charge_off_amount": ColumnContract(minimum=0),
    "jobs_supported": ColumnContract(minimum=0, maximum=1_000_000),
    "approval_fiscal_year": ColumnContract(
        nullable=False,
        minimum=1953,
        maximum=date.today().year + 1,
    ),
    "term_in_months": ColumnContract(minimum=0, maximum=600),
    "initial_interest_rate": ColumnContract(minimum=0, maximum=100),
    "borrower_state": ColumnContract(pattern=r"^[A-Z]{2}$"),
    "project_state": ColumnContract(pattern=r"^[A-Z]{2}$"),
    "bank_state": ColumnContract(pattern=r"^[A-Z]{2}$"),
    "third_party_lender_state": ColumnContract(pattern=r"^[A-Z]{2}$"),
}


def normalize_program_value(value: Any) -> Any:
    """Normalize common public SBA program labels without inventing a value."""
    if pd.isna(value):
        return pd.NA
    text = str(value).strip()
    return PROGRAM_ALIASES.get(text.lower(), text)


def normalize_contract_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with contract-backed category normalization applied."""
    normalized = df.copy()
    if "program" in normalized.columns:
        normalized["program"] = normalized["program"].map(normalize_program_value).astype("string")

    for column in ("borrower_state", "project_state", "bank_state", "third_party_lender_state"):
        if column in normalized.columns:
            normalized[column] = normalized[column].astype("string").str.strip().str.upper()

    return normalized


def validate_contract(df: pd.DataFrame) -> list[ContractViolation]:
    """Validate a cleaned DataFrame against the canonical analytical contract."""
    violations: list[ContractViolation] = []

    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            violations.append(
                ContractViolation(
                    code="MISSING_REQUIRED_COLUMN",
                    column=column,
                    count=len(df),
                    message=f"Required analytical column '{column}' is missing.",
                )
            )

    for column, contract in COLUMN_CONTRACTS.items():
        if column not in df.columns:
            continue

        series = df[column]
        if not contract.nullable:
            null_count = int(series.isna().sum())
            if null_count:
                violations.append(
                    ContractViolation(
                        code="NULL_NOT_ALLOWED",
                        column=column,
                        count=null_count,
                        message=f"Column '{column}' contains null values but is required.",
                    )
                )

        non_null = series.dropna()
        if non_null.empty:
            continue

        if contract.minimum is not None:
            numeric = pd.to_numeric(non_null, errors="coerce")
            invalid_count = int((numeric < contract.minimum).fillna(False).sum())
            if invalid_count:
                violations.append(
                    ContractViolation(
                        code="BELOW_MINIMUM",
                        column=column,
                        count=invalid_count,
                        message=f"Column '{column}' contains values below {contract.minimum}.",
                    )
                )

        if contract.maximum is not None:
            numeric = pd.to_numeric(non_null, errors="coerce")
            invalid_count = int((numeric > contract.maximum).fillna(False).sum())
            if invalid_count:
                violations.append(
                    ContractViolation(
                        code="ABOVE_MAXIMUM",
                        column=column,
                        count=invalid_count,
                        message=f"Column '{column}' contains values above {contract.maximum}.",
                    )
                )

        if contract.allowed_values is not None:
            invalid_mask = ~non_null.astype("string").isin(contract.allowed_values)
            invalid_count = int(invalid_mask.sum())
            if invalid_count:
                violations.append(
                    ContractViolation(
                        code="UNEXPECTED_CATEGORY",
                        column=column,
                        count=invalid_count,
                        message=(
                            f"Column '{column}' contains categories outside "
                            f"{sorted(contract.allowed_values)}."
                        ),
                    )
                )

        if contract.pattern is not None:
            valid_mask = non_null.astype("string").str.fullmatch(contract.pattern, na=False)
            invalid_count = int((~valid_mask).sum())
            if invalid_count:
                violations.append(
                    ContractViolation(
                        code="PATTERN_MISMATCH",
                        column=column,
                        count=invalid_count,
                        message=f"Column '{column}' contains values with an invalid format.",
                        severity="warning",
                    )
                )

    if {"charge_off_amount", "loan_amount"}.issubset(df.columns):
        charge_off = pd.to_numeric(df["charge_off_amount"], errors="coerce")
        loan_amount = pd.to_numeric(df["loan_amount"], errors="coerce")
        invalid_count = int((charge_off > loan_amount).fillna(False).sum())
        if invalid_count:
            violations.append(
                ContractViolation(
                    code="CHARGE_OFF_EXCEEDS_LOAN",
                    column="charge_off_amount",
                    count=invalid_count,
                    message="Charge-off amount exceeds original loan amount.",
                )
            )

    return violations


def has_errors(violations: Iterable[ContractViolation]) -> bool:
    """Return whether a violation collection contains an error-level result."""
    return any(violation.severity == "error" for violation in violations)
