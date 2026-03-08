"""Streamlit dashboard for exploring SBA loan activity."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib import error, request

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALL_OPTION = "All"
OPENAI_API_URL = "https://api.openai.com/v1/responses"
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
        width="stretch",
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
        chart_df = result_df.set_index(result_df.columns[0])
        st.bar_chart(chart_df[result_df.columns[1]], width="stretch")
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

    st.dataframe(result_df, width="stretch", hide_index=True)


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
        st.bar_chart(chart_df["total_funding"], width="stretch")

    st.subheader("Industry Analysis")
    if industry_df.empty:
        st.info("No industry results for the selected filters.")
    else:
        display_df = industry_df.copy()
        display_df["total_funding"] = display_df["total_funding"].map(format_currency)
        st.dataframe(display_df, width="stretch", hide_index=True)
        st.bar_chart(
            industry_df.set_index("naics_code")["total_funding"],
            width="stretch",
        )

    st.subheader("Loan Status")
    if loan_status_df.empty:
        st.info("No loan status results for the selected filters.")
    else:
        render_pie_chart(loan_status_df)

    render_ask_the_data()


if __name__ == "__main__":
    render_dashboard()
