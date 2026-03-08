"""Clean and standardize SBA FOIA loan datasets into one analytical CSV."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_numeric_dtype

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
CLEAN_OUTPUT_PATH = PROCESSED_DIR / "sba_loans_clean.csv"
DOWNLOADS_DIR = Path.home() / "Downloads"
FALLBACK_FILENAMES = (
    "foia-7a-fy2020-present-as-of-251231.csv",
    "foia-7a-fy2020-present-asof-251231.csv",
    "foia-504-fy1991-fy2009-asof-251231.csv",
)
RAW_NA_VALUES = ("", " ", "None", "NULL", "N/A", "n/a")
DATE_COLUMNS = [
    "as_of_date",
    "approval_date",
    "disbursement_date",
    "paid_in_full_date",
    "charge_off_date",
]
NUMERIC_COLUMNS = [
    "loan_amount",
    "jobs_supported",
    "charge_off_amount",
    "sba_guaranteed_approval",
    "third_party_dollars",
    "approval_fiscal_year",
    "term_in_months",
    "initial_interest_rate",
]
COLUMN_ALIASES = {
    "asofdate": "as_of_date",
    "l2locid": "loan_id",
    "borrname": "borrower_name",
    "borrstreet": "borrower_street",
    "borrcity": "borrower_city",
    "borrstate": "borrower_state",
    "borrzip": "borrower_zip",
    "bankname": "lender_name",
    "bankstreet": "bank_street",
    "bankcity": "bank_city",
    "bankstate": "bank_state",
    "bankzip": "bank_zip",
    "grossapproval": "loan_amount",
    "sbaguaranteedapproval": "sba_guaranteed_approval",
    "approvaldate": "approval_date",
    "approvalfiscalyear": "approval_fiscal_year",
    "firstdisbursementdate": "disbursement_date",
    "processingmethod": "processing_method",
    "deliverymethod": "delivery_method",
    "initialinterestrate": "initial_interest_rate",
    "fixedorvariableinterestind": "fixed_or_variable_interest_ind",
    "terminmonths": "term_in_months",
    "naicscode": "naics_code",
    "naicsdescription": "naics_description",
    "franchisecode": "franchise_code",
    "franchisename": "franchise_name",
    "projectcounty": "project_county",
    "projectstate": "project_state",
    "sbadistrictoffice": "sba_district_office",
    "congressionaldistrict": "congressional_district",
    "businesstype": "business_type",
    "businessage": "business_age",
    "loanstatus": "loan_status",
    "paidinfulldate": "paid_in_full_date",
    "chargeoffdate": "charge_off_date",
    "grosschargeoffamount": "charge_off_amount",
    "revolverstatus": "revolver_status",
    "jobssupported": "jobs_supported",
    "collateralind": "collateral_ind",
    "soldsecmrktind": "sold_secondary_market_ind",
    "bankfdicnumber": "bank_fdic_number",
    "bankncuanumber": "bank_ncua_number",
    "thirdpartylendername": "third_party_lender_name",
    "thirdpartylendercity": "third_party_lender_city",
    "thirdpartylenderstate": "third_party_lender_state",
    "thirdpartydollars": "third_party_dollars",
}


def configure_logging() -> None:
    """Configure a simple console logger for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def discover_raw_files() -> list[Path]:
    """Locate raw CSVs under data/raw, with a Downloads fallback."""
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


def load_raw_files(paths: list[Path]) -> pd.DataFrame:
    """Load all raw CSVs and concatenate them into one DataFrame."""
    frames: list[pd.DataFrame] = []
    for path in paths:
        LOGGER.info("Loading raw data from %s", path)
        df = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=True,
            na_values=RAW_NA_VALUES,
            low_memory=False,
        )
        df["source_file"] = path.name
        frames.append(df)

    if not frames:
        raise ValueError("No input files were loaded.")

    combined = pd.concat(frames, ignore_index=True, sort=False)
    LOGGER.info("Combined raw dataset shape: %s", combined.shape)
    return combined


def to_snake_case(value: str) -> str:
    """Convert a column name into snake_case."""
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^0-9A-Za-z]+", "_", value)
    return value.strip("_").lower()


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and apply business-friendly aliases."""
    renamed_columns = {
        column: COLUMN_ALIASES.get(to_snake_case(column).replace("_", ""), to_snake_case(column))
        for column in df.columns
    }
    return df.rename(columns=renamed_columns)


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading and trailing whitespace from text columns."""
    text_columns = df.select_dtypes(include=["object", "string"]).columns
    for column in text_columns:
        df[column] = df[column].astype("string").str.strip()
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Create unified analytical columns that span both SBA programs."""
    lender_sources = [
        column
        for column in ["lender_name", "third_party_lender_name", "cdc_name"]
        if column in df.columns
    ]
    if lender_sources:
        lender_series = df[lender_sources[0]]
        for column in lender_sources[1:]:
            lender_series = lender_series.fillna(df[column])
        df["lender_name"] = lender_series

    return df


def replace_empty_strings_with_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace blank strings with null values."""
    return df.replace(r"^\s*$", pd.NA, regex=True)


def convert_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert selected columns to numeric dtypes when present."""
    for column in columns:
        if column not in df.columns:
            continue

        series = df[column]
        if is_numeric_dtype(series):
            continue

        # Try the fast path first. Most SBA numeric fields are already plain digit strings.
        numeric_series = pd.to_numeric(series, errors="coerce")
        invalid_mask = series.notna() & numeric_series.isna()

        if invalid_mask.any():
            cleaned = series.astype("string")
            needs_symbol_cleanup = cleaned[invalid_mask].str.contains(r"[$,%]", regex=True, na=False).any()
            needs_comma_cleanup = cleaned[invalid_mask].str.contains(",", regex=False, na=False).any()

            if needs_symbol_cleanup:
                cleaned = cleaned.str.replace(r"[$,%]", "", regex=True)
            if needs_comma_cleanup:
                cleaned = cleaned.str.replace(",", "", regex=False)

            numeric_series = pd.to_numeric(cleaned, errors="coerce")

        df[column] = numeric_series

    integer_like_columns = {"jobs_supported", "approval_fiscal_year", "term_in_months"}
    for column in integer_like_columns.intersection(df.columns):
        df[column] = df[column].astype("Int64")

    return df


def convert_date_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert selected columns to datetimes when present."""
    for column in columns:
        if column not in df.columns:
            continue
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop duplicate rows and return the cleaned frame with the duplicate count."""
    duplicate_count = int(df.duplicated().sum())
    return df.drop_duplicates().copy(), duplicate_count


def build_data_quality_summary(df: pd.DataFrame, duplicate_count: int) -> dict[str, Any]:
    """Assemble a simple data quality summary for logging."""
    null_percentages = (df.isna().mean().mul(100).round(2)).sort_values(ascending=False)
    numeric_columns = df.select_dtypes(include=["number"]).columns
    numeric_stats = (
        df[numeric_columns].describe().transpose().round(2) if len(numeric_columns) > 0 else pd.DataFrame()
    )
    summary = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_rows_removed": duplicate_count,
        "null_percentages": null_percentages,
        "numeric_stats": numeric_stats,
    }
    return summary


def log_data_quality_summary(summary: dict[str, Any]) -> None:
    """Log the data quality summary in a readable format."""
    LOGGER.info("Cleaned row count: %s", summary["row_count"])
    LOGGER.info("Column count: %s", summary["column_count"])
    LOGGER.info("Duplicate rows removed: %s", summary["duplicate_rows_removed"])
    LOGGER.info("Null percentages by column:\n%s", summary["null_percentages"].to_string())
    numeric_stats: pd.DataFrame = summary["numeric_stats"]
    if not numeric_stats.empty:
        LOGGER.info("Basic numeric stats:\n%s", numeric_stats.to_string())


def save_clean_dataset(df: pd.DataFrame, output_path: Path = CLEAN_OUTPUT_PATH) -> Path:
    """Save the cleaned dataset to the processed directory."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, date_format="%Y-%m-%d")
    LOGGER.info("Saved cleaned dataset to %s", output_path)
    return output_path


def clean_dataset() -> Path:
    """Run the end-to-end data cleaning workflow."""
    raw_files = discover_raw_files()
    if not raw_files:
        message = (
            f"No raw CSV files were found in {RAW_DIR} or {DOWNLOADS_DIR}. "
            "Place the SBA FOIA extracts in data/raw and rerun the script."
        )
        raise FileNotFoundError(message)

    df = load_raw_files(raw_files)
    df = standardize_column_names(df)
    df = trim_whitespace(df)
    df = replace_empty_strings_with_nulls(df)
    df = add_derived_columns(df)
    df, duplicate_count = remove_duplicates(df)
    df = convert_numeric_columns(df, NUMERIC_COLUMNS)
    df = convert_date_columns(df, DATE_COLUMNS)

    summary = build_data_quality_summary(df, duplicate_count)
    log_data_quality_summary(summary)
    return save_clean_dataset(df)


def main() -> None:
    """Execute the cleaning pipeline."""
    configure_logging()
    output_path = clean_dataset()
    LOGGER.info("Cleaning pipeline completed successfully: %s", output_path)


if __name__ == "__main__":
    main()
