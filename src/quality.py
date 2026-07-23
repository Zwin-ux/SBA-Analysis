"""Machine-readable quality checks for cleaned SBA analytical data."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from src.contracts import (
    ContractViolation,
    has_errors,
    normalize_contract_categories,
    validate_contract,
)

DEFAULT_NULL_THRESHOLDS = {
    "program": 0.0,
    "loan_amount": 0.0,
    "approval_fiscal_year": 0.0,
    "borrower_state": 0.10,
    "naics_code": 0.20,
    "loan_status": 0.25,
}


def dataset_fingerprint(df: pd.DataFrame) -> str:
    """Create a stable fingerprint for a DataFrame's values and schema."""
    ordered = df.reindex(sorted(df.columns), axis=1).copy()
    for column in ordered.columns:
        if pd.api.types.is_datetime64_any_dtype(ordered[column]):
            ordered[column] = ordered[column].dt.strftime("%Y-%m-%d")
        else:
            ordered[column] = ordered[column].astype("string")
    row_hashes = pd.util.hash_pandas_object(ordered, index=False).values.tobytes()
    schema = "|".join(f"{column}:{df[column].dtype}" for column in sorted(df.columns))
    return hashlib.sha256(schema.encode("utf-8") + row_hashes).hexdigest()


def _null_rate_violations(
    df: pd.DataFrame,
    thresholds: dict[str, float],
) -> list[ContractViolation]:
    violations: list[ContractViolation] = []
    for column, maximum_rate in thresholds.items():
        if column not in df.columns or len(df) == 0:
            continue
        null_count = int(df[column].isna().sum())
        null_rate = null_count / len(df)
        if null_rate > maximum_rate:
            violations.append(
                ContractViolation(
                    code="NULL_RATE_EXCEEDED",
                    column=column,
                    count=null_count,
                    message=(
                        f"Column '{column}' null rate {null_rate:.2%} exceeds "
                        f"the {maximum_rate:.2%} threshold."
                    ),
                )
            )
    return violations


def _relationship_violations(df: pd.DataFrame) -> list[ContractViolation]:
    violations: list[ContractViolation] = []

    if {"approval_date", "disbursement_date"}.issubset(df.columns):
        approval = pd.to_datetime(df["approval_date"], errors="coerce")
        disbursement = pd.to_datetime(df["disbursement_date"], errors="coerce")
        invalid_count = int((disbursement < approval).fillna(False).sum())
        if invalid_count:
            violations.append(
                ContractViolation(
                    code="DISBURSEMENT_BEFORE_APPROVAL",
                    column="disbursement_date",
                    count=invalid_count,
                    message="Disbursement date occurs before approval date.",
                )
            )

    if {"charge_off_date", "approval_date"}.issubset(df.columns):
        charge_off = pd.to_datetime(df["charge_off_date"], errors="coerce")
        approval = pd.to_datetime(df["approval_date"], errors="coerce")
        invalid_count = int((charge_off < approval).fillna(False).sum())
        if invalid_count:
            violations.append(
                ContractViolation(
                    code="CHARGE_OFF_BEFORE_APPROVAL",
                    column="charge_off_date",
                    count=invalid_count,
                    message="Charge-off date occurs before approval date.",
                )
            )

    return violations


def evaluate_quality(
    df: pd.DataFrame,
    *,
    null_thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Evaluate the analytical dataset and return a JSON-safe report."""
    normalized = normalize_contract_categories(df)
    duplicate_rows = int(normalized.duplicated().sum())

    violations = validate_contract(normalized)
    violations.extend(_null_rate_violations(normalized, null_thresholds or DEFAULT_NULL_THRESHOLDS))
    violations.extend(_relationship_violations(normalized))

    if duplicate_rows:
        violations.append(
            ContractViolation(
                code="DUPLICATE_ROWS",
                column="*",
                count=duplicate_rows,
                message="Exact duplicate rows remain in the cleaned analytical dataset.",
            )
        )

    null_rates = {
        column: round(float(rate), 6)
        for column, rate in normalized.isna().mean().sort_values(ascending=False).items()
    }

    program_distribution: dict[str, int] = {}
    if "program" in normalized.columns:
        program_distribution = {
            str(program): int(count)
            for program, count in normalized["program"].fillna("<null>").value_counts().items()
        }

    severity_counts = {
        "error": sum(violation.severity == "error" for violation in violations),
        "warning": sum(violation.severity == "warning" for violation in violations),
    }

    return {
        "status": "fail" if has_errors(violations) else "pass",
        "dataset": {
            "rows": int(len(normalized)),
            "columns": int(len(normalized.columns)),
            "fingerprint_sha256": dataset_fingerprint(normalized),
        },
        "schema": {column: str(dtype) for column, dtype in normalized.dtypes.items()},
        "duplicate_rows": duplicate_rows,
        "null_rates": null_rates,
        "program_distribution": program_distribution,
        "severity_counts": severity_counts,
        "violations": [violation.to_dict() for violation in violations],
    }


def write_report(report: dict[str, Any], output_path: Path) -> None:
    """Write a quality report as stable, human-readable JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a cleaned SBA analytical CSV.")
    parser.add_argument("--input", type=Path, required=True, help="Path to a cleaned CSV file.")
    parser.add_argument("--output", type=Path, required=True, help="Destination JSON report path.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.input.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {args.input}")

    df = pd.read_csv(args.input, low_memory=False)
    report = evaluate_quality(df)
    write_report(report, args.output)
    print(
        f"Data quality {report['status']}: {report['dataset']['rows']:,} rows, "
        f"{report['severity_counts']['error']} error(s), "
        f"{report['severity_counts']['warning']} warning(s)."
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
