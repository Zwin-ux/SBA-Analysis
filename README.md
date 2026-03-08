# SBA Capital Watch

Created by Mazen Zwin.

<img width="1917" height="871" alt="image" src="https://github.com/user-attachments/assets/671d73f8-5d38-4100-95bd-6c5ee103b0c3" />


<img width="1913" height="862" alt="image" src="https://github.com/user-attachments/assets/4fb06260-d856-4c69-ba5b-b95af2ac35b8" />


SBA Capital Watch is a data engineering and analytics project built around public SBA 7(a) and 504 FOIA loan data. The goal was to take large, messy public loan extracts and turn them into a clean analytical workflow: inspect raw data, standardize it, load it into PostgreSQL, build reusable SQL views, and surface the results in an interactive Streamlit dashboard.

This repo is organized to show both the engineering side and the analytical side of the project. It is meant to be readable, reproducible, and easy to walk through in an interview or recruiter review.

## Live App

- Streamlit deployment: [appapppy-noirldcwsrzrbeqqfaeuqt.streamlit.app](https://appapppy-noirldcwsrzrbeqqfaeuqt.streamlit.app/)

## Project Summary

- Built a Python data pipeline for SBA loan analysis
- Combined raw 7(a) and 504 FOIA datasets into one cleaned analytical dataset
- Loaded `467,294` cleaned loan records into PostgreSQL
- Created SQL views for state funding, industry funding, loan status, and jobs-per-dollar analysis
- Built a Streamlit dashboard with interactive filters for state, year, and industry

##  Questions I asked before  that require this type of analysis

1. Which states receive the most SBA funding?
2. Which industries rely most on SBA-backed loans?
3. Which sectors show the strongest signs of charge-off risk?

## Key Findings worth noting!!!!

Using the combined cleaned dataset:

- Top states by total SBA funding:
  - California: `$38.82B`
  - Texas: `$22.61B`
  - Florida: `$17.77B`
  - New York: `$10.24B`
  - Georgia: `$9.74B`

- Industries with the heaviest SBA dependence by funded dollars:
  - Hotels (except Casino Hotels) and Motels: `$17.75B`
  - Full-Service Restaurants: `$8.64B`
  - Limited-Service Restaurants: `$5.65B`
  - Child Day Care Services: `$5.11B`
  - Offices of Dentists: `$4.79B`

- Sectors with the strongest charge-off stress:
  - Accommodation and Food Services: `4.12%` charge-off rate by funded dollars, about `$1.65B` charged off
  - Arts, Entertainment, and Recreation: `3.31%`
  - Manufacturing: `2.94%`
  - Real Estate and Rental and Leasing: `2.42%`
  - Retail Trade: `2.36%`

## Tech Stack

- Python 3.11
- pandas
- SQLAlchemy
- psycopg2
- PostgreSQL
- Streamlit
- python-dotenv

## Repository Structure

```text
sba-capital-watch/
|-- app/
|   `-- streamlit_app.py
|-- data/
|   |-- raw/
|   `-- processed/
|-- docs/
|   `-- techforce_brief.md
|-- sql/
|   |-- schema.sql
|   `-- views.sql
|-- src/
|   |-- ingest.py
|   |-- clean.py
|   |-- load.py
|   `-- transform.py
|-- Analysis.pdf
|-- README.md
`-- requirements.txt
```

## Pipeline Flow

1. `src/ingest.py` reads the raw SBA CSV files, logs schema information, and creates preview files.
2. `src/clean.py` standardizes column names, removes duplicates, trims whitespace, converts numeric and date fields, and writes a cleaned CSV.
3. `src/load.py` initializes PostgreSQL from `sql/schema.sql` and loads the cleaned dataset into the `loans` table in chunks.
4. `src/transform.py` creates analytical views from `sql/views.sql`.
5. `app/streamlit_app.py` serves the dashboard using SQL queries against PostgreSQL.

## Deliverables

- Working Python ETL pipeline
- PostgreSQL analytical data model
- Streamlit dashboard
- Analysis report in [Analysis.pdf](Analysis.pdf)
- Recruiter summary in [docs/techforce_brief.md](docs/techforce_brief.md)

## Running Locally

Create and use the virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Set the database connection in `.env`:

```env
DATABASE_URL=postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/sba_capital_watch
```

Run the full pipeline:

```powershell
.\.venv\Scripts\python.exe src\ingest.py
.\.venv\Scripts\python.exe src\clean.py
.\.venv\Scripts\python.exe src\load.py
.\.venv\Scripts\python.exe src\transform.py
.\.venv\Scripts\python.exe -m streamlit run app/streamlit_app.py
```

## Deployment Note

The deployed app uses a remote PostgreSQL database and reads `DATABASE_URL` from Streamlit secrets. The local development version can continue using `.env`.

## Notes on Authorship

OpenAI tools were used to support coding and implementation work. The project direction, dataset choice, analytical framing, interpretation of findings, and final presentation decisions were done by Mazen Zwin.
