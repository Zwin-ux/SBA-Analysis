# SBA Capital Watch

**A reproducible data engineering and analytics project built on public SBA 7(a) and 504 loan data.**

<p align="center">
  <img width="1100" alt="SBA Capital Watch dashboard" src="https://github.com/user-attachments/assets/671d73f8-5d38-4100-95bd-6c5ee103b0c3" />
</p>

SBA Capital Watch converts large, inconsistent public FOIA extracts into a clean analytical dataset, validates that dataset through executable contracts, loads it into PostgreSQL, and presents decision-oriented aggregates in Streamlit.

The project currently reports **467,294 cleaned loan records** across SBA 7(a) and 504 programs.

## Review the project

- **Live dashboard:** [Open SBA Capital Watch](https://appapppy-noirldcwsrzrbeqqfaeuqt.streamlit.app/)
- **Case study:** [`docs/CASE_STUDY.md`](docs/CASE_STUDY.md)
- **Data dictionary:** [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md)
- **Engineering roadmap:** [`docs/ENGINEERING_ROADMAP.md`](docs/ENGINEERING_ROADMAP.md)

## What this project demonstrates

- Python ingestion and deterministic cleaning of large public datasets
- Explicit schema, range, category, duplicate, null-rate, and relationship checks
- Machine-readable JSON quality reports and stable dataset fingerprints
- PostgreSQL schema design, chunked loading, indexes, and reusable analytical views
- Streamlit and Plotly dashboards with state, year, industry, program, and lender analysis
- Clear separation between source data, transformations, storage, queries, and presentation
- A roadmap for a leakage-safe and interpretable charge-off risk baseline

## Public-data scope

This is a standalone portfolio project built from public SBA data. It does not require private employer code, proprietary business rules, customer records, or an internal application repository.

Recruiter-facing views should remain aggregate and should not expose borrower contact information or present individual loan scores as lending decisions.

## Questions explored

1. Which states receive the most SBA-backed funding?
2. Which industries account for the largest funded volume?
3. Which broad sectors show the greatest charge-off stress?
4. How do 7(a) and 504 activity differ?
5. Which lenders and regions support the highest reported volume?
6. How many source-reported jobs are associated with each $1 million of funding?

## Reported findings

Using the current combined cleaned dataset:

### Funding by state

| State | Total funding |
|---|---:|
| California | $38.82B |
| Texas | $22.61B |
| Florida | $17.77B |
| New York | $10.24B |
| Georgia | $9.74B |

### Largest industries by funded dollars

| Industry | Total funding |
|---|---:|
| Hotels and motels | $17.75B |
| Full-service restaurants | $8.64B |
| Limited-service restaurants | $5.65B |
| Child day care services | $5.11B |
| Offices of dentists | $4.79B |

### Broad-sector charge-off stress

| Sector | Charge-off rate by funded dollars |
|---|---:|
| Accommodation and Food Services | 4.12% |
| Arts, Entertainment, and Recreation | 3.31% |
| Manufacturing | 2.94% |
| Real Estate and Rental and Leasing | 2.42% |
| Retail Trade | 2.36% |

These values depend on the current source snapshot and transformation rules. Public claims should remain only while they can be regenerated from the current database or evidence artifacts.

## Architecture

```mermaid
flowchart LR
    A[Public SBA FOIA CSVs] --> B[Python ingest]
    B --> C[Deterministic cleaning]
    C --> D[Contracts and quality report]
    D --> E[(PostgreSQL)]
    E --> F[SQL analytical views]
    F --> G[Streamlit dashboard]
    D --> H[Planned risk baseline]
    H --> G
```

## Reproducible sample pipeline

The included synthetic fixture covers both programs and exercises:

- Currency and comma parsing
- Percentage parsing
- Whitespace cleanup
- Duplicate removal
- Optional values
- Program aliases
- State normalization

From a clean environment:

```bash
python -m venv .venv

# Windows
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\python scripts/run_sample_pipeline.py
.venv\Scripts\python -m pytest

# macOS / Linux
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python scripts/run_sample_pipeline.py
.venv/bin/python -m pytest
```

The sample run produces:

```text
artifacts/sample_clean.csv
artifacts/data_quality.json
```

Validate any cleaned CSV directly:

```bash
python -m src.quality \
  --input data/processed/sba_loans_clean.csv \
  --output artifacts/data_quality.json
```

The command returns a non-zero exit code when error-level contract violations are present.

## Full PostgreSQL workflow

Install the runtime dependencies:

```bash
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

Configure the database connection:

```env
DATABASE_URL=postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/sba_capital_watch
```

Run the pipeline:

```powershell
.venv\Scripts\python src\ingest.py
.venv\Scripts\python src\clean.py
.venv\Scripts\python -m src.quality --input data\processed\sba_loans_clean.csv --output artifacts\data_quality.json
.venv\Scripts\python src\load.py
.venv\Scripts\python src\transform.py
.venv\Scripts\python -m streamlit run app\streamlit_app.py
```

## Repository structure

```text
SBA-Analysis/
├── .devcontainer/
│   └── devcontainer.json
├── .github/
│   └── workflows/ci.yml
├── app/
│   └── streamlit_app.py
├── data/
│   ├── raw/          # FOIA extracts (not committed)
│   └── processed/    # cleaned output (not committed)
├── docs/
│   ├── CASE_STUDY.md
│   ├── DATA_DICTIONARY.md
│   └── ENGINEERING_ROADMAP.md
├── scripts/
│   └── run_sample_pipeline.py
├── sql/
│   ├── schema.sql
│   └── views.sql
├── src/
│   ├── clean.py
│   ├── contracts.py
│   ├── ingest.py
│   ├── load.py
│   ├── quality.py
│   └── transform.py
├── tests/
│   ├── conftest.py
│   ├── fixtures/sba_sample_raw.csv
│   ├── test_clean.py
│   ├── test_contracts.py
│   └── test_quality.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Current limitations

- The full public extracts are not committed because of their size.
- Hosted dashboard availability depends on Streamlit and the remote PostgreSQL database.
- Public FOIA records may be missing, delayed, revised, or inconsistently categorized.
- Charge-off patterns are descriptive and do not establish causation.
- `jobs_supported` is source-reported and should not be treated as independently verified job creation.
- The planned predictive baseline is portfolio research, not a production underwriting system.

## Authorship

Created by **Mazen Zwin**.

AI tools supported coding and implementation. Dataset selection, analytical framing, project direction, interpretation, and final presentation decisions were made by Mazen Zwin.
