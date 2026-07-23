from __future__ import annotations

import json

import pandas as pd

from src.quality import dataset_fingerprint, evaluate_quality, main


def violation_codes(report: dict) -> set[str]:
    return {violation["code"] for violation in report["violations"]}


def test_clean_fixture_passes_quality_contract(cleaned_sample: pd.DataFrame) -> None:
    report = evaluate_quality(cleaned_sample)

    assert report["status"] == "pass"
    assert report["dataset"]["rows"] == 4
    assert report["duplicate_rows"] == 0
    assert report["severity_counts"] == {"error": 0, "warning": 0}
    assert report["program_distribution"] == {"7(a)": 2, "504": 2}


def test_quality_report_detects_invalid_financial_relationships(
    cleaned_sample: pd.DataFrame,
) -> None:
    invalid = cleaned_sample.copy()
    invalid.loc[invalid.index[0], "jobs_supported"] = -1
    invalid.loc[invalid.index[1], "charge_off_amount"] = 2_000_000
    invalid.loc[invalid.index[2], "disbursement_date"] = pd.Timestamp("2020-01-01")

    report = evaluate_quality(invalid)
    codes = violation_codes(report)

    assert report["status"] == "fail"
    assert "BELOW_MINIMUM" in codes
    assert "CHARGE_OFF_EXCEEDS_LOAN" in codes
    assert "DISBURSEMENT_BEFORE_APPROVAL" in codes


def test_quality_report_detects_exact_duplicates(cleaned_sample: pd.DataFrame) -> None:
    duplicated = pd.concat([cleaned_sample, cleaned_sample.iloc[[0]]], ignore_index=True)
    report = evaluate_quality(duplicated)

    assert report["status"] == "fail"
    assert report["duplicate_rows"] == 1
    assert "DUPLICATE_ROWS" in violation_codes(report)


def test_fingerprint_is_deterministic(cleaned_sample: pd.DataFrame) -> None:
    assert dataset_fingerprint(cleaned_sample) == dataset_fingerprint(cleaned_sample.copy())


def test_cli_writes_json_and_returns_success(
    cleaned_sample: pd.DataFrame,
    tmp_path,
) -> None:
    input_path = tmp_path / "clean.csv"
    output_path = tmp_path / "quality.json"
    cleaned_sample.to_csv(input_path, index=False)

    exit_code = main(["--input", str(input_path), "--output", str(output_path)])
    report = json.loads(output_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert report["status"] == "pass"
    assert len(report["dataset"]["fingerprint_sha256"]) == 64
