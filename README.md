# SBA Capital Watch

Created by Mazen Zwin.

This project started with a basic question: if the SBA is helping small businesses get access to capital, where is that money actually going, which industries lean on it the most, and where do the warning signs show up? The public FOIA files are useful, but not in the form they come in. They are wide, inconsistent across programs, and awkward to query directly. The point of this repo is to turn that raw material into something clean enough to analyze without pretending it is more complicated than it needs to be.

The repo takes two public SBA loan extracts, cleans them into one working dataset, loads the result into PostgreSQL, creates reusable SQL views, and exposes the main patterns in a Streamlit dashboard. It is meant to be readable first and clever second.

## What This Project Covers

- SBA 7(a) and 504 FOIA loan data
- raw file inspection and schema checks
- cleaning and standardization in Python
- PostgreSQL loading and analytical views
- a dashboard for state, industry, and loan status analysis

## Data Source

This project uses two public SBA FOIA CSV files:

- `foia-7a-fy2020-present-as-of-251231.csv`
- `foia-504-fy1991-fy2009-asof-251231.csv`

They belong in `data/raw/`. The pipeline reads them, creates small preview files for inspection, and writes one cleaned output to `data/processed/sba_loans_clean.csv`.

## Why The Structure Looks Like This

I kept the project split into a few small layers so each step has a clear job:

- `src/ingest.py`
  Checks the raw files, prints columns and missingness, and saves preview CSVs.
- `src/clean.py`
  Standardizes names, trims whitespace, converts types, removes duplicates, and writes one cleaned file.
- `src/load.py`
  Initializes the database schema from `sql/schema.sql` and loads the cleaned CSV into PostgreSQL in chunks.
- `src/transform.py`
  Applies the analytical views defined in `sql/views.sql`.
- `app/streamlit_app.py`
  Runs the dashboard against the PostgreSQL data.
- `sql/schema.sql`
  Defines the `loans` table and a few practical indexes.
- `sql/views.sql`
  Defines reusable analytical views for state funding, industry funding, loan status, and jobs-per-dollar.

That is the whole idea: keep the flow obvious enough that someone else can open the repo and understand where the numbers came from.

## The Analytical Theory Behind It

This is not a machine learning project and it is not trying to look like one. The analysis is built around a few simple ideas that are worth getting right before doing anything more advanced.

First, capital concentration matters. If a small number of states capture a large share of SBA lending, that tells you something about where access, demand, lender relationships, and business density are strongest.

Second, industry dependence matters. A high loan count is one thing, but heavy dollar concentration in a handful of NAICS groups can tell a different story about where federal-backed credit is doing the most work.

Third, charge-offs matter because they are one of the clearest signs of stress in the portfolio. They are not the whole risk story, but they are a solid starting point for asking which sectors or business types appear more fragile.

Fourth, jobs-per-dollar is a rough efficiency lens. It is not perfect and it should not be treated as a final verdict on impact, but it is useful for comparing how much employment support is reported relative to funded dollars.

The broader point is simple: build a clean baseline, then ask better questions from that baseline.

## Resources Built In This Repo

The main resources created for this project are:

- a raw-data inspection step with preview exports
- a cleaned, analysis-ready CSV
- a PostgreSQL schema for the unified loan table
- reusable SQL views for recurring analysis
- a Streamlit dashboard with filters for state, year, and industry

The most important output is not any single chart. It is the fact that the same underlying data can now be checked at every stage, from raw CSV to cleaned file to SQL view to dashboard.

## Project Questions

These are the three questions the project is built around:

1. Which states receive the most SBA funding?
2. Which industries rely most on SBA-backed loans?
3. Which sectors show the strongest signs of charge-off risk?

## Running The Project

Create a virtual environment and install the dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Set your database connection in `.env`:

```env
DATABASE_URL=postgresql+psycopg2://postgres@localhost:5432/sba_capital_watch
```

Then run the pipeline in order:

```bash
python src/ingest.py
python src/clean.py
python src/load.py
python src/transform.py
streamlit run app/streamlit_app.py
```

## Notes

- The raw CSVs are intentionally not tracked in Git because they are large public source files.
- The processed CSV output is also ignored so the repo stays lightweight and reproducible.
- If the current shell still points to the Windows Store `python` stub after installation, open a fresh terminal or use `.venv\Scripts\python.exe`.

## Author

Mazen Zwin
