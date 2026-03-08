"""Inspect raw SBA loan extracts and save preview files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DOWNLOADS_DIR = Path.home() / "Downloads"
FALLBACK_FILENAMES = (
    "foia-7a-fy2020-present-as-of-251231.csv",
    "foia-7a-fy2020-present-asof-251231.csv",
    "foia-504-fy1991-fy2009-asof-251231.csv",
)


def configure_logging() -> None:
    """Configure a simple console logger for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def discover_raw_files() -> list[Path]:
    """Locate raw CSV files under data/raw, with a Downloads fallback."""
    local_files = sorted(RAW_DIR.glob("*.csv"))
    if local_files:
        LOGGER.info("Discovered %s raw file(s) in %s", len(local_files), RAW_DIR)
        return local_files

    fallback_files = [
        DOWNLOADS_DIR / filename
        for filename in FALLBACK_FILENAMES
        if (DOWNLOADS_DIR / filename).exists()
    ]
    if fallback_files:
        LOGGER.warning(
            "No CSV files were found in %s; using %s file(s) from %s instead.",
            RAW_DIR,
            len(fallback_files),
            DOWNLOADS_DIR,
        )
        return fallback_files

    wildcard_matches = sorted(DOWNLOADS_DIR.glob("foia-*.csv"))
    if wildcard_matches:
        LOGGER.warning(
            "No CSV files were found in %s; using %s FOIA file(s) from %s.",
            RAW_DIR,
            len(wildcard_matches),
            DOWNLOADS_DIR,
        )
        return wildcard_matches

    return []


def load_raw_file(path: Path) -> pd.DataFrame:
    """Load a raw CSV file into a DataFrame."""
    LOGGER.info("Loading %s", path)
    return pd.read_csv(path, low_memory=False)


def summarize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create a column-level schema summary for a DataFrame."""
    return pd.DataFrame(
        {
            "column_name": df.columns,
            "dtype": [str(dtype) for dtype in df.dtypes],
            "missing_values": df.isna().sum().reindex(df.columns).to_list(),
        }
    )


def log_schema_report(source_path: Path, df: pd.DataFrame, summary: pd.DataFrame) -> None:
    """Log a readable schema report for a raw dataset."""
    LOGGER.info("Schema report for %s", source_path.name)
    LOGGER.info("Row count: %s", len(df))
    LOGGER.info("Columns (%s): %s", len(df.columns), ", ".join(df.columns))
    LOGGER.info("Missing value counts by column:\n%s", summary.to_string(index=False))


def save_preview(df: pd.DataFrame, source_path: Path, row_limit: int = 10_000) -> Path:
    """Persist a preview file for quick inspection."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    preview_path = PROCESSED_DIR / f"preview_{source_path.stem}.csv"
    df.head(row_limit).to_csv(preview_path, index=False)
    LOGGER.info("Saved preview to %s", preview_path)
    return preview_path


def process_files(paths: Iterable[Path]) -> None:
    """Run schema inspection and preview export for every raw file."""
    for path in paths:
        df = load_raw_file(path)
        summary = summarize_dataframe(df)
        log_schema_report(path, df, summary)
        save_preview(df, path)


def main() -> None:
    """Execute the raw file ingestion inspection workflow."""
    configure_logging()
    raw_files = discover_raw_files()
    if not raw_files:
        message = (
            f"No raw CSV files were found in {RAW_DIR} or {DOWNLOADS_DIR}. "
            "Place the SBA FOIA extracts in data/raw and rerun the script."
        )
        raise FileNotFoundError(message)

    process_files(raw_files)
    LOGGER.info("Ingest inspection completed for %s file(s).", len(raw_files))


if __name__ == "__main__":
    main()
