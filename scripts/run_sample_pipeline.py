"""Run the public fixture through cleaning and data-quality validation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.clean import clean_dataframe, save_clean_dataset
from src.quality import evaluate_quality, write_report

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "tests" / "fixtures" / "sba_sample_raw.csv"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
CLEAN_PATH = ARTIFACTS_DIR / "sample_clean.csv"
REPORT_PATH = ARTIFACTS_DIR / "data_quality.json"


def main() -> int:
    raw_df = pd.read_csv(INPUT_PATH, dtype=str, keep_default_na=True, low_memory=False)
    cleaned_df, duplicate_count = clean_dataframe(raw_df)
    save_clean_dataset(cleaned_df, CLEAN_PATH)

    report = evaluate_quality(cleaned_df)
    report["pipeline"] = {
        "input_rows": int(len(raw_df)),
        "output_rows": int(len(cleaned_df)),
        "duplicate_rows_removed": duplicate_count,
        "fixture": str(INPUT_PATH.relative_to(PROJECT_ROOT)),
    }
    write_report(report, REPORT_PATH)

    print(
        f"Sample pipeline {report['status']}: "
        f"{len(raw_df)} input rows -> {len(cleaned_df)} cleaned rows; "
        f"report={REPORT_PATH.relative_to(PROJECT_ROOT)}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
