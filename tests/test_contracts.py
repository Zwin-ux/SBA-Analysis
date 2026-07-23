from __future__ import annotations

import pandas as pd

from src.contracts import (
    ContractViolation,
    has_errors,
    normalize_program_value,
    validate_contract,
)


def make_valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "program": pd.array(["7(a)", "504"], dtype="string"),
            "loan_amount": [250_000.0, 1_200_000.0],
            "approval_fiscal_year": pd.array([2020, 2019], dtype="Int64"),
        }
    )


def codes_for(violations: list[ContractViolation]) -> set[str]:
    return {violation.code for violation in violations}


def test_valid_frame_has_no_violations() -> None:
    assert validate_contract(make_valid_frame()) == []


def test_missing_required_column_is_reported() -> None:
    df = make_valid_frame().drop(columns=["program"])
    violations = validate_contract(df)

    assert "MISSING_REQUIRED_COLUMN" in codes_for(violations)
    assert any(v.column == "program" for v in violations)
    assert has_errors(violations)


def test_null_in_non_nullable_column_is_reported() -> None:
    df = make_valid_frame()
    df.loc[0, "loan_amount"] = None
    violations = validate_contract(df)

    assert "NULL_NOT_ALLOWED" in codes_for(violations)


def test_value_above_maximum_is_reported() -> None:
    df = make_valid_frame()
    df["jobs_supported"] = pd.array([2_000_000, 5], dtype="Int64")
    violations = validate_contract(df)

    assert "ABOVE_MAXIMUM" in codes_for(violations)


def test_unexpected_program_category_is_reported() -> None:
    df = make_valid_frame()
    df.loc[0, "program"] = "Express"
    violations = validate_contract(df)

    assert "UNEXPECTED_CATEGORY" in codes_for(violations)


def test_state_pattern_mismatch_is_warning_not_error() -> None:
    df = make_valid_frame()
    df["borrower_state"] = pd.array(["California", "TX"], dtype="string")
    violations = validate_contract(df)

    pattern_violations = [v for v in violations if v.code == "PATTERN_MISMATCH"]
    assert len(pattern_violations) == 1
    assert pattern_violations[0].severity == "warning"
    assert pattern_violations[0].count == 1
    assert not has_errors(violations)


def test_normalize_program_value_maps_public_aliases() -> None:
    assert normalize_program_value("7a") == "7(a)"
    assert normalize_program_value("7 (a)") == "7(a)"
    assert normalize_program_value("CDC/504") == "504"
    assert normalize_program_value("Express") == "Express"
    assert normalize_program_value(pd.NA) is pd.NA


def test_has_errors_distinguishes_severity() -> None:
    warning = ContractViolation(
        code="PATTERN_MISMATCH", column="x", count=1, message="", severity="warning"
    )
    error = ContractViolation(code="NULL_NOT_ALLOWED", column="x", count=1, message="")

    assert not has_errors([warning])
    assert has_errors([warning, error])
    assert not has_errors([])
