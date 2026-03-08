"""Load the cleaned SBA loans dataset into PostgreSQL."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "sba_loans_clean.csv"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"
DATE_COLUMNS = [
    "as_of_date",
    "approval_date",
    "disbursement_date",
    "paid_in_full_date",
    "charge_off_date",
]
FLOAT_COLUMNS = [
    "loan_amount",
    "charge_off_amount",
    "sba_guaranteed_approval",
    "third_party_dollars",
    "initial_interest_rate",
]
INTEGER_COLUMNS = [
    "jobs_supported",
    "approval_fiscal_year",
    "term_in_months",
]


def configure_logging() -> None:
    """Configure a simple console logger for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def get_database_url() -> str:
    """Read the database URL from the environment."""
    load_dotenv(PROJECT_ROOT / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise EnvironmentError("DATABASE_URL is not set.")

    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
    return database_url


def get_engine(database_url: str) -> Engine:
    """Build a SQLAlchemy engine for PostgreSQL."""
    return create_engine(database_url, future=True, pool_pre_ping=True)


def count_csv_rows(path: Path) -> int:
    """Count data rows in a CSV file without loading it fully into memory."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def prepare_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Coerce known columns to useful dtypes before loading to PostgreSQL."""
    for column in FLOAT_COLUMNS:
        if column in chunk.columns:
            chunk[column] = pd.to_numeric(chunk[column], errors="coerce")

    for column in INTEGER_COLUMNS:
        if column in chunk.columns:
            chunk[column] = pd.to_numeric(chunk[column], errors="coerce").astype("Int64")

    for column in DATE_COLUMNS:
        if column in chunk.columns:
            chunk[column] = pd.to_datetime(chunk[column], errors="coerce").dt.date

    return chunk


def initialize_schema(engine: Engine, schema_path: Path = SCHEMA_PATH) -> None:
    """Create the loans table from the checked-in schema.sql file."""
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found at {schema_path}.")

    sql_text = schema_path.read_text(encoding="utf-8")
    with engine.begin() as connection:
        raw_connection = connection.connection
        with raw_connection.cursor() as cursor:
            cursor.execute(sql_text)

    LOGGER.info("Initialized database schema from %s", schema_path)


def load_clean_dataset(
    engine: Engine,
    cleaned_path: Path = CLEAN_DATA_PATH,
    table_name: str = "loans",
    read_chunk_size: int = 25_000,
    insert_chunk_size: int = 5_000,
) -> None:
    """Load the cleaned CSV into PostgreSQL in chunks."""
    if not cleaned_path.exists():
        raise FileNotFoundError(
            f"Cleaned dataset not found at {cleaned_path}. Run python src/clean.py first."
        )

    total_rows = count_csv_rows(cleaned_path)
    LOGGER.info("Preparing to load %s rows from %s", total_rows, cleaned_path)

    with engine.begin() as connection:
        connection.execute(text("SELECT 1"))

    initialize_schema(engine)

    rows_loaded = 0
    for chunk_index, chunk in enumerate(
        pd.read_csv(cleaned_path, chunksize=read_chunk_size, low_memory=False),
        start=1,
    ):
        prepared_chunk = prepare_chunk(chunk)
        prepared_chunk.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            chunksize=insert_chunk_size,
            method="multi",
        )
        rows_loaded += len(prepared_chunk)
        LOGGER.info(
            "Loaded chunk %s (%s rows). Progress: %s/%s rows.",
            chunk_index,
            len(prepared_chunk),
            rows_loaded,
            total_rows,
        )

    LOGGER.info("Finished loading %s rows into table '%s'.", rows_loaded, table_name)


def main() -> None:
    """Execute the PostgreSQL loading workflow."""
    configure_logging()
    database_url = get_database_url()
    engine = get_engine(database_url)
    load_clean_dataset(engine)


if __name__ == "__main__":
    main()
