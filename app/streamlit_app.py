"""Streamlit dashboard for exploring SBA loan activity."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib import error, request

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALL_OPTION = "All"
OPENAI_API_URL = "https://api.openai.com/v1/responses"
ACCENT_COLOR = "#0f766e"
HIGHLIGHT_COLOR = "#d97706"
CARD_BACKGROUND = "#f7fafc"
PLOT_COLOR_SEQUENCE = [
    "#0f766e",
    "#1d4ed8",
    "#d97706",
    "#0f172a",
    "#4f46e5",
    "#059669",
]
ALLOWED_VIEWS: dict[str, str] = {
    "vw_state_funding": (
        "Columns: borrower_state, total_loans, total_funding, average_loan_size, "
        "total_jobs_supported"
    ),
    "vw_industry_funding": (
        "Columns: naics_code, naics_description, total_loans, total_funding, "
        "average_loan_size, total_jobs_supported"
    ),
    "vw_loan_status_summary": (
        "Columns: loan_status, total_loans, total_funding, total_charge_off_amount"
    ),
    "vw_jobs_per_dollar": (
        "Columns: total_jobs_supported, total_funding, jobs_per_dollar"
    ),
}
SUGGESTED_QUESTIONS = [
    "Which states received the most SBA funding?",
    "Which industries had the highest total funding?",
    "What loan statuses are most common?",
    "How many jobs are supported per dollar overall?",
]


def get_secret_value(key: str) -> str | None:
    """Read a secret from Streamlit secrets when deployed."""
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        return None

    return None


def get_database_url() -> str:
    """Read the database URL from Streamlit secrets or the environment."""
    secret_database_url = get_secret_value("DATABASE_URL")
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


def get_openai_api_key() -> str | None:
    """Read the OpenAI API key from Streamlit secrets or the environment."""
    secret_api_key = get_secret_value("OPENAI_API_KEY")
    if secret_api_key:
        return secret_api_key

    load_dotenv(PROJECT_ROOT / ".env")
    return os.getenv("OPENAI_API_KEY")


def get_openai_model() -> str:
    """Read the configured OpenAI model, with a stable default."""
    secret_model = get_secret_value("OPENAI_MODEL")
    if secret_model:
        return secret_model

    load_dotenv(PROJECT_ROOT / ".env")
    return os.getenv("OPENAI_MODEL", "gpt-4.1-mini")


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
            AVG(loan_amount) AS average_loan_size,
            SUM(COALESCE(jobs_supported, 0)) AS total_jobs_supported
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


@st.cache_data(ttl=300, show_spinner=False)
def fetch_dataset_scope() -> pd.DataFrame:
    """Return dataset scope metadata for the top-of-page context note."""
    return read_sql(
        """
        SELECT
            COUNT(*) AS total_loans,
            MIN(approval_fiscal_year) AS min_year,
            MAX(approval_fiscal_year) AS max_year,
            MAX(as_of_date) AS latest_as_of_date
        FROM loans
        """
    )


def build_sector_case_sql() -> str:
    """Map NAICS prefixes into broad business sectors for risk analysis."""
    return """
        CASE
            WHEN naics_code IS NULL THEN 'Unknown'
            WHEN LEFT(naics_code, 2) = '11' THEN 'Agriculture, Forestry, Fishing and Hunting'
            WHEN LEFT(naics_code, 2) = '21' THEN 'Mining, Quarrying, and Oil and Gas Extraction'
            WHEN LEFT(naics_code, 2) = '22' THEN 'Utilities'
            WHEN LEFT(naics_code, 2) = '23' THEN 'Construction'
            WHEN LEFT(naics_code, 2) = '31'
              OR LEFT(naics_code, 2) = '32'
              OR LEFT(naics_code, 2) = '33' THEN 'Manufacturing'
            WHEN LEFT(naics_code, 2) = '42' THEN 'Wholesale Trade'
            WHEN LEFT(naics_code, 2) = '44'
              OR LEFT(naics_code, 2) = '45' THEN 'Retail Trade'
            WHEN LEFT(naics_code, 2) = '48'
              OR LEFT(naics_code, 2) = '49' THEN 'Transportation and Warehousing'
            WHEN LEFT(naics_code, 2) = '51' THEN 'Information'
            WHEN LEFT(naics_code, 2) = '52' THEN 'Finance and Insurance'
            WHEN LEFT(naics_code, 2) = '53' THEN 'Real Estate and Rental and Leasing'
            WHEN LEFT(naics_code, 2) = '54' THEN 'Professional, Scientific, and Technical Services'
            WHEN LEFT(naics_code, 2) = '55' THEN 'Management of Companies and Enterprises'
            WHEN LEFT(naics_code, 2) = '56' THEN 'Administrative and Support Services'
            WHEN LEFT(naics_code, 2) = '61' THEN 'Educational Services'
            WHEN LEFT(naics_code, 2) = '62' THEN 'Health Care and Social Assistance'
            WHEN LEFT(naics_code, 2) = '71' THEN 'Arts, Entertainment, and Recreation'
            WHEN LEFT(naics_code, 2) = '72' THEN 'Accommodation and Food Services'
            WHEN LEFT(naics_code, 2) = '81' THEN 'Other Services'
            WHEN LEFT(naics_code, 2) = '92' THEN 'Public Administration'
            ELSE 'Other / Unclassified'
        END
    """


def fetch_charge_off_rate_by_sector(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return broad sector charge-off rates ranked by severity."""
    sector_case = build_sector_case_sql()
    return read_sql(
        f"""
        SELECT
            {sector_case} AS sector,
            COUNT(*) AS total_loans,
            SUM(COALESCE(loan_amount, 0)) AS total_funding,
            SUM(COALESCE(charge_off_amount, 0)) AS total_charge_off_amount,
            CASE
                WHEN SUM(COALESCE(loan_amount, 0)) = 0 THEN 0
                ELSE SUM(COALESCE(charge_off_amount, 0)) / SUM(COALESCE(loan_amount, 0))
            END AS charge_off_rate
        FROM loans
        {where_clause}
        GROUP BY 1
        HAVING SUM(COALESCE(loan_amount, 0)) >= 50000000
        ORDER BY charge_off_rate DESC, total_charge_off_amount DESC
        LIMIT 10
        """,
        params=params,
    )


def fetch_average_loan_by_state(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return top states ranked by average loan size."""
    return read_sql(
        f"""
        SELECT
            borrower_state,
            COUNT(*) AS total_loans,
            AVG(loan_amount) AS average_loan_size,
            SUM(COALESCE(loan_amount, 0)) AS total_funding
        FROM loans
        {where_clause}
        {'AND' if where_clause else 'WHERE'} borrower_state IS NOT NULL
        GROUP BY borrower_state
        HAVING COUNT(*) >= 250
        ORDER BY average_loan_size DESC
        LIMIT 10
        """,
        params=params,
    )


def fetch_guarantee_share_by_industry(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return industries with the highest SBA guarantee share."""
    return read_sql(
        f"""
        SELECT
            naics_code,
            COALESCE(MAX(naics_description), 'Unknown') AS naics_description,
            COUNT(*) AS total_loans,
            SUM(COALESCE(loan_amount, 0)) AS total_funding,
            SUM(COALESCE(sba_guaranteed_approval, 0)) AS guaranteed_funding,
            CASE
                WHEN SUM(COALESCE(loan_amount, 0)) = 0 THEN 0
                ELSE SUM(COALESCE(sba_guaranteed_approval, 0)) / SUM(COALESCE(loan_amount, 0))
            END AS guarantee_share
        FROM loans
        {where_clause}
        {'AND' if where_clause else 'WHERE'} naics_code IS NOT NULL
        GROUP BY naics_code
        HAVING COUNT(*) >= 100
        ORDER BY guarantee_share DESC, total_funding DESC
        LIMIT 10
        """,
        params=params,
    )


def fetch_top_lenders(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return lenders ranked by total SBA funding."""
    return read_sql(
        f"""
        SELECT
            lender_name,
            COUNT(*) AS total_loans,
            SUM(COALESCE(loan_amount, 0)) AS total_funding,
            AVG(loan_amount) AS average_loan_size
        FROM loans
        {where_clause}
        {'AND' if where_clause else 'WHERE'} lender_name IS NOT NULL
        GROUP BY lender_name
        HAVING COUNT(*) >= 100
        ORDER BY total_funding DESC
        LIMIT 10
        """,
        params=params,
    )


def fetch_jobs_per_million_by_state(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return states with the strongest jobs-supported intensity."""
    return read_sql(
        f"""
        SELECT
            borrower_state,
            COUNT(*) AS total_loans,
            SUM(COALESCE(jobs_supported, 0)) AS total_jobs_supported,
            SUM(COALESCE(loan_amount, 0)) AS total_funding,
            CASE
                WHEN SUM(COALESCE(loan_amount, 0)) = 0 THEN 0
                ELSE (SUM(COALESCE(jobs_supported, 0)) / SUM(COALESCE(loan_amount, 0))) * 1000000
            END AS jobs_per_million
        FROM loans
        {where_clause}
        {'AND' if where_clause else 'WHERE'} borrower_state IS NOT NULL
        GROUP BY borrower_state
        HAVING SUM(COALESCE(loan_amount, 0)) >= 25000000
        ORDER BY jobs_per_million DESC
        LIMIT 10
        """,
        params=params,
    )


def fetch_program_comparison(where_clause: str, params: dict[str, Any]) -> pd.DataFrame:
    """Return side-by-side metrics for 7(a) versus 504 program activity."""
    return read_sql(
        f"""
        SELECT
            program,
            COUNT(*) AS total_loans,
            SUM(COALESCE(loan_amount, 0)) AS total_funding,
            AVG(loan_amount) AS average_loan_size,
            SUM(COALESCE(jobs_supported, 0)) AS total_jobs_supported
        FROM loans
        {where_clause}
        GROUP BY program
        ORDER BY total_funding DESC
        """
    , params=params)


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


def format_percent(value: float | int | None) -> str:
    """Format a decimal share as a percentage for display."""
    if value is None or pd.isna(value):
        return "0.0%"
    return f"{float(value) * 100:.1f}%"


def apply_app_styles() -> None:
    """Inject a small custom style layer so the dashboard feels more polished."""
    st.markdown(
        f"""
        <style>
            .stApp {{
                background:
                    radial-gradient(circle at top left, rgba(15,118,110,0.08), transparent 28%),
                    linear-gradient(180deg, #f8fafc 0%, #eef2f7 100%);
            }}
            div[data-testid="stMetric"] {{
                background: rgba(255, 255, 255, 0.92);
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 18px;
                padding: 1rem 1.1rem;
                box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
            }}
            .insight-card {{
                background: linear-gradient(180deg, #ffffff 0%, {CARD_BACKGROUND} 100%);
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 18px;
                padding: 1rem 1.1rem;
                min-height: 150px;
                box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
            }}
            .insight-label {{
                color: #475569;
                font-size: 0.82rem;
                font-weight: 700;
                letter-spacing: 0.02em;
                text-transform: uppercase;
                margin-bottom: 0.6rem;
            }}
            .insight-value {{
                color: #0f172a;
                font-size: 1.6rem;
                font-weight: 800;
                line-height: 1.2;
                margin-bottom: 0.35rem;
            }}
            .insight-note {{
                color: #475569;
                font-size: 0.95rem;
                line-height: 1.45;
            }}
            .scope-note {{
                background: rgba(255, 255, 255, 0.82);
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 16px;
                padding: 0.9rem 1rem;
                margin-bottom: 1rem;
                color: #334155;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def style_figure(figure: Any) -> Any:
    """Apply a consistent visual theme across Plotly charts."""
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.95)",
        font={"family": "Arial, sans-serif", "color": "#0f172a"},
        margin={"l": 12, "r": 12, "t": 48, "b": 12},
        title={"x": 0.0, "xanchor": "left"},
        colorway=PLOT_COLOR_SEQUENCE,
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(148, 163, 184, 0.18)", zeroline=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(148, 163, 184, 0.18)", zeroline=False)
    return figure


def render_bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    orientation: str = "h",
    text_auto: str | bool = False,
    color: str | None = None,
) -> None:
    """Render a formatted Plotly bar chart."""
    if df.empty:
        st.info("No results available for this chart.")
        return

    figure = px.bar(
        df,
        x=x,
        y=y,
        orientation=orientation,
        title=title,
        text_auto=text_auto,
        color=color,
        color_discrete_sequence=PLOT_COLOR_SEQUENCE,
    )
    style_figure(figure)
    if any(term in x for term in ("funding", "amount", "size", "approval")):
        figure.update_xaxes(tickprefix="$", separatethousands=True)
        figure.update_traces(hovertemplate="%{y}<br>%{x:$,.0f}<extra></extra>")
    elif "rate" in x or "share" in x:
        figure.update_xaxes(tickformat=".0%")
        figure.update_traces(hovertemplate="%{y}<br>%{x:.1%}<extra></extra>")
    elif "jobs_per_million" in x:
        figure.update_xaxes(tickformat=",.0f")
        figure.update_traces(hovertemplate="%{y}<br>%{x:,.0f} jobs per $1M<extra></extra>")
    else:
        figure.update_xaxes(tickformat=",.0f")
    st.plotly_chart(figure, use_container_width=True)


def render_pie_chart(df: pd.DataFrame) -> None:
    """Render a pie chart using Plotly."""
    figure = px.pie(
        df,
        names="loan_status",
        values="total_loans",
        title="Loan Status Distribution",
        color_discrete_sequence=PLOT_COLOR_SEQUENCE,
    )
    style_figure(figure)
    st.plotly_chart(figure, use_container_width=True)


def render_scope_note(scope_df: pd.DataFrame) -> None:
    """Render a concise note about dataset timing and coverage."""
    if scope_df.empty:
        return

    scope = scope_df.iloc[0]
    latest_as_of_date = pd.to_datetime(scope["latest_as_of_date"], errors="coerce")
    latest_text = (
        latest_as_of_date.strftime("%B %d, %Y")
        if pd.notna(latest_as_of_date)
        else "Unknown"
    )
    st.markdown(
        (
            '<div class="scope-note"><strong>Dataset scope.</strong> '
            f"This dashboard combines SBA 504 loans from fiscal years {int(scope['min_year'])}-{2009} "
            f"and SBA 7(a) loans from fiscal years 2020-{int(scope['max_year'])}. "
            f"Latest FOIA extract date: {latest_text}."
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_insight_card(label: str, value: str, note: str) -> None:
    """Render a styled key-finding card."""
    st.markdown(
        f"""
        <div class="insight-card">
            <div class="insight-label">{label}</div>
            <div class="insight-value">{value}</div>
            <div class="insight-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_key_findings(
    state_funding_df: pd.DataFrame,
    industry_df: pd.DataFrame,
    sector_risk_df: pd.DataFrame,
    top_lenders_df: pd.DataFrame,
) -> None:
    """Render top-of-page cards with the clearest analytical signals."""
    st.subheader("Key Findings")
    finding_columns = st.columns(4)

    with finding_columns[0]:
        if state_funding_df.empty:
            render_insight_card("Top State", "No data", "No state-level funding matched the current filters.")
        else:
            top_state = state_funding_df.iloc[0]
            render_insight_card(
                "Top State",
                str(top_state["borrower_state"]),
                f"{format_currency(top_state['total_funding'])} in approved funding.",
            )

    with finding_columns[1]:
        if industry_df.empty:
            render_insight_card("Top Industry", "No data", "No industry results matched the current filters.")
        else:
            top_industry = industry_df.iloc[0]
            render_insight_card(
                "Top Industry",
                str(top_industry["naics_description"]),
                f"{format_currency(top_industry['total_funding'])} across {format_count(top_industry['total_loans'])} loans.",
            )

    with finding_columns[2]:
        if sector_risk_df.empty:
            render_insight_card("Highest Risk Sector", "No data", "No charge-off benchmark met the minimum funding threshold.")
        else:
            top_sector = sector_risk_df.iloc[0]
            render_insight_card(
                "Highest Risk Sector",
                str(top_sector["sector"]),
                f"{format_percent(top_sector['charge_off_rate'])} charge-off rate on {format_currency(top_sector['total_funding'])}.",
            )

    with finding_columns[3]:
        if top_lenders_df.empty:
            render_insight_card("Top Lender", "No data", "No lender-level funding results matched the current filters.")
        else:
            top_lender = top_lenders_df.iloc[0]
            render_insight_card(
                "Top Lender",
                str(top_lender["lender_name"]),
                f"{format_currency(top_lender['total_funding'])} across {format_count(top_lender['total_loans'])} loans.",
            )


def extract_response_text(response_json: dict[str, Any]) -> str:
    """Extract assistant text from an OpenAI Responses API payload."""
    output_parts: list[str] = []
    for item in response_json.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                output_parts.append(content.get("text", ""))

    return "\n".join(part for part in output_parts if part).strip()


def normalize_json_text(response_text: str) -> str:
    """Strip markdown fences from model output before JSON parsing."""
    normalized_text = response_text.strip()
    if normalized_text.startswith("```"):
        normalized_text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", normalized_text)
        normalized_text = re.sub(r"\s*```$", "", normalized_text)
    return normalized_text.strip()


def call_openai_json(prompt: str, api_key: str, model: str) -> dict[str, Any]:
    """Call the OpenAI Responses API and parse JSON output."""
    payload = {
        "model": model,
        "input": prompt,
        "text": {"format": {"type": "text"}},
    }
    request_data = json.dumps(payload).encode("utf-8")
    http_request = request.Request(
        OPENAI_API_URL,
        data=request_data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=60) as response:
            response_json = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"OpenAI API request failed: {exc.code} {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"OpenAI API request failed: {exc.reason}") from exc

    response_text = extract_response_text(response_json)
    if not response_text:
        raise RuntimeError("The model returned an empty response.")

    normalized_text = normalize_json_text(response_text)
    try:
        return json.loads(normalized_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse model response as JSON: {response_text}") from exc


def build_sql_generation_prompt(question: str) -> str:
    """Build the prompt for SQL generation against the analytical views."""
    view_descriptions = "\n".join(
        f"- {view_name}: {description}" for view_name, description in ALLOWED_VIEWS.items()
    )
    allowed_views = ", ".join(ALLOWED_VIEWS.keys())
    return f"""
You are a PostgreSQL analytics assistant for SBA Capital Watch.

Use only these views:
{view_descriptions}

Rules:
- Return valid JSON only.
- Output keys must be: can_answer, sql, answer_title, chart_type, notes.
- chart_type must be one of: table, bar, pie, metric.
- Only write read-only SQL using SELECT or WITH.
- Use only these relations: {allowed_views}.
- Never query the raw loans table.
- Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, GRANT, COPY, or information_schema.
- Prefer ORDER BY and LIMIT 20 for ranked lists unless the question clearly needs a single metric.
- If the question cannot be answered from the allowed views, set can_answer to false and sql to an empty string.

User question:
{question}
""".strip()


def validate_generated_sql(sql: str) -> str:
    """Validate that generated SQL is read-only and limited to allowed views."""
    normalized_sql = sql.strip().rstrip(";").strip()
    lowered_sql = normalized_sql.lower()

    if not normalized_sql:
        raise ValueError("The model did not return a SQL query.")
    if not (lowered_sql.startswith("select") or lowered_sql.startswith("with")):
        raise ValueError("Only SELECT queries are allowed.")
    if ";" in normalized_sql:
        raise ValueError("Only a single SQL statement is allowed.")

    blocked_patterns = [
        r"\binsert\b",
        r"\bupdate\b",
        r"\bdelete\b",
        r"\bdrop\b",
        r"\balter\b",
        r"\bcreate\b",
        r"\bgrant\b",
        r"\bcopy\b",
        r"\btruncate\b",
        r"\binformation_schema\b",
        r"\bpg_catalog\b",
        r"\bloans\b",
    ]
    for pattern in blocked_patterns:
        if re.search(pattern, lowered_sql):
            raise ValueError("The generated SQL references a blocked command or table.")

    referenced_relations = {
        match.group(1)
        for match in re.finditer(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", lowered_sql)
    }
    if not referenced_relations:
        raise ValueError("The generated SQL does not reference any allowed analytical view.")

    disallowed_relations = referenced_relations.difference(ALLOWED_VIEWS)
    if disallowed_relations:
        raise ValueError(
            "The generated SQL referenced disallowed relations: "
            + ", ".join(sorted(disallowed_relations))
        )

    return normalized_sql


def summarize_result_preview(df: pd.DataFrame) -> str:
    """Create a short deterministic summary for tabular results."""
    if df.empty:
        return "The query ran successfully but returned no rows."

    if len(df.columns) == 1 and len(df) == 1:
        column_name = str(df.columns[0])
        return f"{column_name}: {df.iloc[0, 0]}"

    preview_rows = df.head(3).to_dict(orient="records")
    return f"Returned {len(df):,} row(s). Top rows: {preview_rows}"


def initialize_chat_state() -> None:
    """Initialize Streamlit session state for the chat experience."""
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []


def run_data_chat(question: str) -> dict[str, Any]:
    """Generate SQL from a natural-language question and execute it safely."""
    api_key = get_openai_api_key()
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY is not set.")

    model = get_openai_model()
    prompt = build_sql_generation_prompt(question)
    response_payload = call_openai_json(prompt=prompt, api_key=api_key, model=model)

    if not response_payload.get("can_answer", False):
        return {
            "question": question,
            "sql": "",
            "answer_title": response_payload.get("answer_title", "Question not supported"),
            "notes": response_payload.get(
                "notes",
                "This question could not be answered from the allowed analytical views.",
            ),
            "result_df": pd.DataFrame(),
            "chart_type": "table",
        }

    validated_sql = validate_generated_sql(str(response_payload.get("sql", "")))
    result_df = read_sql(validated_sql)
    return {
        "question": question,
        "sql": validated_sql,
        "answer_title": response_payload.get("answer_title", "Ask the Data"),
        "notes": response_payload.get("notes", summarize_result_preview(result_df)),
        "result_df": result_df,
        "chart_type": response_payload.get("chart_type", "table"),
    }


def render_chat_result(result: dict[str, Any]) -> None:
    """Render a chat response with SQL and result output."""
    st.markdown(f"**{result['answer_title']}**")
    st.write(result["notes"])

    if result["sql"]:
        st.code(result["sql"], language="sql")

    result_df: pd.DataFrame = result["result_df"]
    if result_df.empty:
        return

    if result["chart_type"] == "bar" and len(result_df.columns) >= 2:
        chart_df = result_df.copy()
        label_column = chart_df.columns[0]
        value_column = chart_df.columns[1]
        chart_df = chart_df.sort_values(by=value_column, ascending=True)
        render_bar_chart(
            chart_df,
            x=value_column,
            y=label_column,
            title=str(result["answer_title"]),
            orientation="h",
        )
    elif result["chart_type"] == "pie" and len(result_df.columns) >= 2:
        render_pie_chart(
            pd.DataFrame(
                {
                    "loan_status": result_df.iloc[:, 0].astype(str),
                    "total_loans": pd.to_numeric(result_df.iloc[:, 1], errors="coerce"),
                }
            )
        )
    elif result["chart_type"] == "metric" and len(result_df.columns) == 1 and len(result_df) == 1:
        st.metric(str(result_df.columns[0]), str(result_df.iloc[0, 0]))

    st.dataframe(result_df, use_container_width=True, hide_index=True)


def render_ask_the_data() -> None:
    """Render the OpenAI-backed chat interface."""
    st.subheader("Ask the Data")
    st.caption("Natural-language questions are converted into safe SQL against analytical views only.")

    api_key = get_openai_api_key()
    if not api_key:
        st.info("Add OPENAI_API_KEY to your local .env or Streamlit secrets to enable chat.")
        return

    initialize_chat_state()

    suggestion_columns = st.columns(len(SUGGESTED_QUESTIONS))
    for index, question in enumerate(SUGGESTED_QUESTIONS):
        if suggestion_columns[index].button(question, key=f"suggested-question-{index}"):
            st.session_state["pending_question"] = question

    pending_question = st.session_state.pop("pending_question", None)
    typed_question = st.chat_input("Ask a question about the SBA lending data")
    question = pending_question or typed_question

    for message in st.session_state.chat_messages:
        with st.chat_message(message["role"]):
            if message["role"] == "user":
                st.write(message["content"])
            else:
                render_chat_result(message["payload"])

    if not question:
        return

    st.session_state.chat_messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Querying analytical views..."):
            try:
                result = run_data_chat(question)
            except Exception as exc:
                st.error(str(exc))
                return

        render_chat_result(result)
        st.session_state.chat_messages.append({"role": "assistant", "payload": result})


def render_dashboard() -> None:
    """Render the SBA Capital Watch dashboard."""
    st.set_page_config(page_title="SBA Capital Watch", layout="wide")
    apply_app_styles()
    st.title("SBA Capital Watch")
    st.caption("An analytical view of public SBA 7(a) and 504 FOIA lending activity.")

    try:
        scope_df = fetch_dataset_scope()
        states, years, industries_df = load_filter_options()
    except Exception as exc:
        st.error(f"Unable to load dashboard data: {exc}")
        st.info("Ensure DATABASE_URL is set and run the load/transform scripts before starting Streamlit.")
        st.stop()

    render_scope_note(scope_df)

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
    sector_risk_df = fetch_charge_off_rate_by_sector(where_clause, params)
    average_loan_state_df = fetch_average_loan_by_state(where_clause, params)
    guarantee_share_df = fetch_guarantee_share_by_industry(where_clause, params)
    top_lenders_df = fetch_top_lenders(where_clause, params)
    jobs_per_million_df = fetch_jobs_per_million_by_state(where_clause, params)
    program_comparison_df = fetch_program_comparison(where_clause, params)

    if overview_df.empty or int(overview_df.loc[0, "total_loans"]) == 0:
        st.warning("No loans match the current filters.")
        st.stop()

    active_filters: list[str] = []
    if selected_state != ALL_OPTION:
        active_filters.append(f"State: {selected_state}")
    if selected_year != ALL_OPTION:
        active_filters.append(f"Year: {selected_year}")
    if selected_industry != ALL_OPTION:
        active_filters.append(f"Industry: {industry_labels.get(selected_industry, selected_industry)}")
    if active_filters:
        st.caption("Current slice: " + " | ".join(active_filters))

    render_key_findings(state_funding_df, industry_df, sector_risk_df, top_lenders_df)

    st.subheader("Overview")
    metric_columns = st.columns(4)
    metric_columns[0].metric("Total Loans", format_count(overview_df.loc[0, "total_loans"]))
    metric_columns[1].metric("Total Funding", format_currency(overview_df.loc[0, "total_funding"]))
    metric_columns[2].metric(
        "Average Loan Size",
        format_currency(overview_df.loc[0, "average_loan_size"]),
    )
    metric_columns[3].metric(
        "Jobs Supported",
        format_count(overview_df.loc[0, "total_jobs_supported"]),
    )

    overview_left, overview_right = st.columns((1.4, 1))

    with overview_left:
        st.subheader("State Funding")
        top_states_df = state_funding_df.head(10).sort_values("total_funding", ascending=True)
        render_bar_chart(
            top_states_df,
            x="total_funding",
            y="borrower_state",
            title="Top 10 States by Approved Funding",
            orientation="h",
        )

    with overview_right:
        st.subheader("Loan Status")
        if loan_status_df.empty:
            st.info("No loan status results for the selected filters.")
        else:
            render_pie_chart(loan_status_df)

    st.subheader("Industry Analysis")
    industry_left, industry_right = st.columns((1.2, 1))
    with industry_left:
        if industry_df.empty:
            st.info("No industry results for the selected filters.")
        else:
            industry_display_df = industry_df.copy()
            industry_display_df["total_funding"] = industry_display_df["total_funding"].map(format_currency)
            industry_display_df["total_loans"] = industry_display_df["total_loans"].map(format_count)
            st.dataframe(industry_display_df, use_container_width=True, hide_index=True)

    with industry_right:
        top_industries_df = industry_df.head(10).sort_values("total_funding", ascending=True)
        render_bar_chart(
            top_industries_df,
            x="total_funding",
            y="naics_description",
            title="Top 10 Industries by Approved Funding",
            orientation="h",
        )

    st.subheader("Analyst Metrics")
    metrics_row_left, metrics_row_right = st.columns(2)

    with metrics_row_left:
        render_bar_chart(
            sector_risk_df.sort_values("charge_off_rate", ascending=True),
            x="charge_off_rate",
            y="sector",
            title="Charge-Off Risk by Sector",
            orientation="h",
            color="sector",
        )
        render_bar_chart(
            jobs_per_million_df.sort_values("jobs_per_million", ascending=True),
            x="jobs_per_million",
            y="borrower_state",
            title="Jobs Supported per $1M by State",
            orientation="h",
            color="borrower_state",
        )

    with metrics_row_right:
        render_bar_chart(
            average_loan_state_df.sort_values("average_loan_size", ascending=True),
            x="average_loan_size",
            y="borrower_state",
            title="Average Loan Size by State",
            orientation="h",
            color="borrower_state",
        )
        render_bar_chart(
            guarantee_share_df.sort_values("guarantee_share", ascending=True),
            x="guarantee_share",
            y="naics_description",
            title="SBA Guarantee Share by Industry",
            orientation="h",
            color="naics_description",
        )

    secondary_left, secondary_right = st.columns(2)

    with secondary_left:
        st.markdown("**Top Lenders by Funding**")
        lenders_display_df = top_lenders_df.copy()
        if lenders_display_df.empty:
            st.info("No lender-level results for the selected filters.")
        else:
            lenders_display_df["total_funding"] = lenders_display_df["total_funding"].map(format_currency)
            lenders_display_df["average_loan_size"] = lenders_display_df["average_loan_size"].map(format_currency)
            lenders_display_df["total_loans"] = lenders_display_df["total_loans"].map(format_count)
            st.dataframe(lenders_display_df, use_container_width=True, hide_index=True)

    with secondary_right:
        st.markdown("**7(a) vs 504 Program Comparison**")
        if program_comparison_df.empty:
            st.info("No program comparison results for the selected filters.")
        else:
            program_cards = st.columns(len(program_comparison_df))
            for index, (_, row) in enumerate(program_comparison_df.iterrows()):
                with program_cards[index]:
                    render_insight_card(
                        f"{row['program']} Program",
                        format_currency(row["total_funding"]),
                        (
                            f"{format_count(row['total_loans'])} loans | "
                            f"Avg {format_currency(row['average_loan_size'])} | "
                            f"{format_count(row['total_jobs_supported'])} jobs"
                        ),
                    )

    with st.expander("Methodology / Caveats"):
        st.markdown(
            """
            - This app combines two public SBA FOIA extracts: 504 loans from fiscal years 1992-2009 and 7(a) loans from fiscal years 2020-2026.
            - Because the time windows are discontinuous, comparisons should be read as directional patterns in the assembled dataset, not as a complete year-by-year history of SBA lending.
            - Charge-off risk is shown as `sum(charge_off_amount) / sum(loan_amount)` and broad sectors are inferred from NAICS prefixes.
            - Industry-level and lender-level rankings use minimum row thresholds to avoid noisy leaders driven by tiny samples.
            - Filter selections update all charts and cards on this page, while the `Ask the Data` chat remains restricted to the analytical SQL views for safety.
            """
        )

    render_ask_the_data()


if __name__ == "__main__":
    render_dashboard()
