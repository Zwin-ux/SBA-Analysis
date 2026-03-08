"""Create analytical PostgreSQL views for SBA Capital Watch."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
VIEWS_PATH = PROJECT_ROOT / "sql" / "views.sql"


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


def create_views(engine: Engine, views_path: Path = VIEWS_PATH) -> None:
    """Create or replace analytical views from the checked-in SQL file."""
    if not views_path.exists():
        raise FileNotFoundError(f"Views file not found at {views_path}.")

    sql_text = views_path.read_text(encoding="utf-8")
    with engine.begin() as connection:
        connection.execute(text("SELECT 1 FROM loans LIMIT 1"))
        raw_connection = connection.connection
        with raw_connection.cursor() as cursor:
            cursor.execute(sql_text)

    LOGGER.info("Created analytical views from %s", views_path)


def main() -> None:
    """Execute the analytical transformation workflow."""
    configure_logging()
    database_url = get_database_url()
    engine = get_engine(database_url)
    create_views(engine)
    LOGGER.info("Analytical view creation completed successfully.")


if __name__ == "__main__":
    main()
