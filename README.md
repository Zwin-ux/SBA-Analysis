# SBA Capital Watch

A public-data pipeline and dashboard for understanding where SBA-backed capital goes, which industries depend on it, and where charge-off stress appears.

<img width="100%" alt="SBA Capital Watch funding overview dashboard" src="https://github.com/user-attachments/assets/671d73f8-5d38-4100-95bd-6c5ee103b0c3" />

<img width="100%" alt="SBA Capital Watch industry and charge-off analysis" src="https://github.com/user-attachments/assets/4fb06260-d856-4c69-ba5b-b95af2ac35b8" />

[Open the Streamlit dashboard](https://appapppy-noirldcwsrzrbeqqfaeuqt.streamlit.app/) · The free deployment may need to wake after inactivity.

## The engineering work

- Standardized and combined public SBA 7(a) and 504 FOIA extracts
- Loaded `467,294` cleaned loan records into PostgreSQL
- Kept ingestion, cleaning, loading, and analytical transformations as separate Python stages
- Built reusable SQL views for state funding, industry funding, loan status, and jobs-per-dollar analysis
- Served the views through a filterable Streamlit dashboard

```mermaid
flowchart LR
    A[Public SBA CSV extracts] --> B[Inspect and ingest]
    B --> C[Clean and standardize]
    C --> D[(PostgreSQL)]
    D --> E[Analytical SQL views]
    E --> F[Streamlit dashboard]
```

## Questions the product answers

1. Which states receive the most SBA-backed funding?
2. Which industries depend most on SBA programs?
3. Which sectors show the strongest charge-off stress?
4. How do the answers change by year, state, and industry?

## Selected findings

| Signal | Result |
| --- | --- |
| Largest state total | California, `$38.82B` |
| Next-largest state totals | Texas, `$22.61B`; Florida, `$17.77B` |
| Largest industry total | Hotels and motels, `$17.75B` |
| Highest highlighted sector charge-off rate | Accommodation and Food Services, `4.12%` by funded dollars |
| Records in the analytical database | `467,294` |

These are descriptive results from the project dataset, not a claim about current program performance.

## What to review

- `src/ingest.py` — source inspection and preview generation
- `src/clean.py` — schema normalization, deduplication, type conversion, and cleaned output
- `src/load.py` — chunked PostgreSQL loading
- `src/transform.py` — analytical view creation
- `sql/schema.sql` and `sql/views.sql` — database contract and reusable analysis
- `app/streamlit_app.py` — dashboard queries, filters, and presentation

## Stack

Python 3.11 · pandas · SQLAlchemy · PostgreSQL · Streamlit

## Run locally

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Create `.env`:

```env
DATABASE_URL=postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/sba_capital_watch
```

Run the pipeline:

```powershell
.\.venv\Scripts\python.exe src\ingest.py
.\.venv\Scripts\python.exe src\clean.py
.\.venv\Scripts\python.exe src\load.py
.\.venv\Scripts\python.exe src\transform.py
.\.venv\Scripts\python.exe -m streamlit run app\streamlit_app.py
```

## Authorship

Built by Mazen Zwin. AI tools assisted implementation; the dataset choice, analytical framing, pipeline structure, interpretation, and product decisions are mine.

## License

[MIT](LICENSE)
