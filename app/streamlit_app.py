"""Streamlit dashboard for exploring SBA loan activity."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALL_OPTION = "All"


def get_secret_database_url() -> str | None:
    """Read a database URL from Streamlit secrets when deployed."""
    try:
        if "DATABASE_URL" in st.secrets:
            return str(st.secrets["DATABASE_URL"])
        if "database" in st.secrets and "url" in st.secrets["database"]:
            return str(st.secrets["database"]["url"])
    except Exception:
        return None

    return None


def get_database_url() -> str:
    """Read the database URL from Streamlit secrets or the environment."""
    secret_database_url = get_secret_database_url()
    if secret_database_url:
        database_url = secret_database_url
    else:
        load_dotenv(PROJECT_ROOT / ".env")
        database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise EnvironmentError("DATABASE_URL is not set.")

    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
    return database_url


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """Create a cached SQLAlchemy engine for Streamlit sessions."""
    return create_engine(get_database_url(), future=True, pool_pre_ping=True)


def read_sql(query: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Execute a SQL query and return the results as a DataFrame."""
    with get_engine().connect() as connection:
        return pd.read_sql_query(text(query), connection, params=params or {})


@st.cache_data(ttl=300, show_spinner=False)
def load_filter_options() -> tuple[list[str], list[Any], pd.DataFrame]:
    """Fetch filter options from the loans table."""
    states_df = read_sql(
        """
        SELECT DISTINCT borrower_state
        FROM loans
        WHERE borrower_state IS NOT NULL
        ORDER BY borrower_state
        """
    )
    years_df = read_sql(
        """
        SELECT DISTINCT approval_fiscal_year
        FROM loans
        WHERE approval_fiscal_year IS NOT NULL
        ORDER BY approval_fiscal_year
        """
    )
    industries_df = read_sql(
        """
        SELECT DISTINCT naics_code, COALESCE(naics_description, 'Unknown') AS naics_description
        FROM loans
        WHERE naics_code IS NOT NULL
        ORDER BY naics_code
        """
    )

    states = [ALL_OPTION] + states_df["borrower_state"].astype(str).tolist()
    years = [ALL_OPTION] + years_df["approval_fiscal_year"].astype(int).tolist()
    return states, years, industries_df


def build_filters(
    selected_state: str,
    selected_year: str | int,
    selected_industry: str,
) -> tuple[str, dict[str, Any]]:
    """Build a SQL WHERE clause and parameter set from UI filters."""
    clauses: list[str] = []
    params: dict[str, Any] = {}

    if selected_state != ALL_OPTION:
        clauses.append("borrower_state = :borrower_state")
        params["borrower_state"] = selected_state

    if selected_year != ALL_OPTION:
        clauses.append("approval_fiscal_year = :approval_fiscal_year")
        params["approval_fiscal_year"] = int(selected_year)

    if selected_industry != ALL_OPTION:
        clauses.append("naics_code = :naics_code")
        params["naics_code"] = selected_industry

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_clause, params


def fetch_overview(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return overview metrics for the current filter selection."""
    return read_sql(
        f"""
        SELECT
            COUNT(*) AS total_loans,
            SUM(COALESCE(loan_amount, 0)) AS total_funding,
            AVG(loan_amount) AS average_loan_size
        FROM loans
        {where_clause}
        """,
        params=params,
    )


def fetch_state_funding(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return state-level funding totals for the current filter selection."""
    return read_sql(
        f"""
        SELECT
            borrower_state,
            SUM(COALESCE(loan_amount, 0)) AS total_funding
        FROM loans
        {where_clause}
        {'AND' if where_clause else 'WHERE'} borrower_state IS NOT NULL
        GROUP BY borrower_state
        ORDER BY total_funding DESC
        """,
        params=params,
    )


def fetch_industry_analysis(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return NAICS-level funding aggregates for the current filter selection."""
    return read_sql(
        f"""
        SELECT
            naics_code,
            COALESCE(MAX(naics_description), 'Unknown') AS naics_description,
            COUNT(*) AS total_loans,
            SUM(COALESCE(loan_amount, 0)) AS total_funding
        FROM loans
        {where_clause}
        {'AND' if where_clause else 'WHERE'} naics_code IS NOT NULL
        GROUP BY naics_code
        ORDER BY total_funding DESC
        LIMIT 20
        """,
        params=params,
    )


def fetch_loan_status(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return loan status counts for the current filter selection."""
    return read_sql(
        f"""
        SELECT
            COALESCE(loan_status, 'Unknown') AS loan_status,
            COUNT(*) AS total_loans
        FROM loans
        {where_clause}
        GROUP BY COALESCE(loan_status, 'Unknown')
        ORDER BY total_loans DESC
        """,
        params=params,
    )


def format_currency(value: float | int | None) -> str:
    """Format a numeric value as currency for display."""
    if value is None or pd.isna(value):
        return "$0"
    return f"${value:,.0f}"


def format_count(value: float | int | None) -> str:
    """Format a numeric value as an integer count for display."""
    if value is None or pd.isna(value):
        return "0"
    return f"{int(value):,}"


def render_pie_chart(df: pd.DataFrame) -> None:
    """Render a pie chart using Vega-Lite."""
    st.vega_lite_chart(
        df,
        {
            "mark": {"type": "arc", "outerRadius": 120},
            "encoding": {
                "theta": {"field": "total_loans", "type": "quantitative"},
                "color": {"field": "loan_status", "type": "nominal"},
                "tooltip": [
                    {"field": "loan_status", "type": "nominal"},
                    {"field": "total_loans", "type": "quantitative"},
                ],
            },
        },
        use_container_width=True,
    )


def render_dashboard() -> None:
    """Render the SBA Capital Watch dashboard."""
    st.set_page_config(page_title="SBA Capital Watch", layout="wide")
    st.title("SBA Capital Watch")
    st.caption("Public SBA 7(a) and 504 FOIA lending data in PostgreSQL.")

    try:
        states, years, industries_df = load_filter_options()
    except Exception as exc:
        st.error(f"Unable to load dashboard data: {exc}")
        st.info("Ensure DATABASE_URL is set and run the load/transform scripts before starting Streamlit.")
        st.stop()

    industry_options = [ALL_OPTION] + industries_df["naics_code"].astype(str).tolist()
    industry_labels = {
        row["naics_code"]: f"{row['naics_code']} - {row['naics_description']}"
        for _, row in industries_df.iterrows()
    }

    st.sidebar.header("Interactive Filters")
    selected_state = st.sidebar.selectbox("State", states, index=0)
    selected_year = st.sidebar.selectbox("Year", years, index=0)
    selected_industry = st.sidebar.selectbox(
        "Industry",
        industry_options,
        index=0,
        format_func=lambda code: code if code == ALL_OPTION else industry_labels.get(code, code),
    )

    where_clause, params = build_filters(selected_state, selected_year, selected_industry)

    overview_df = fetch_overview(where_clause, params)
    state_funding_df = fetch_state_funding(where_clause, params)
    industry_df = fetch_industry_analysis(where_clause, params)
    loan_status_df = fetch_loan_status(where_clause, params)

    if overview_df.empty or int(overview_df.loc[0, "total_loans"]) == 0:
        st.warning("No loans match the current filters.")
        st.stop()

    st.subheader("Overview")
    metric_columns = st.columns(3)
    metric_columns[0].metric("Total Loans", format_count(overview_df.loc[0, "total_loans"]))
    metric_columns[1].metric("Total Funding", format_currency(overview_df.loc[0, "total_funding"]))
    metric_columns[2].metric(
        "Average Loan Size",
        format_currency(overview_df.loc[0, "average_loan_size"]),
    )

    st.subheader("State Funding")
    if state_funding_df.empty:
        st.info("No state funding results for the selected filters.")
    else:
        chart_df = state_funding_df.set_index("borrower_state")
        st.bar_chart(chart_df["total_funding"], use_container_width=True)

    st.subheader("Industry Analysis")
    if industry_df.empty:
        st.info("No industry results for the selected filters.")
    else:
        display_df = industry_df.copy()
        display_df["total_funding"] = display_df["total_funding"].map(format_currency)
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.bar_chart(
            industry_df.set_index("naics_code")["total_funding"],
            use_container_width=True,
        )

    st.subheader("Loan Status")
    if loan_status_df.empty:
        st.info("No loan status results for the selected filters.")
    else:
        render_pie_chart(loan_status_df)


if __name__ == "__main__":
    render_dashboard()
