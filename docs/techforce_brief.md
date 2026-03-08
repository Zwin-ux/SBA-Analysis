# TechForce Project Brief

## Candidate

Mazen Zwin

## Project

SBA Capital Watch

## What This Project Demonstrates

- Data cleaning at realistic file scale
- ETL pipeline design in Python
- Relational modeling in PostgreSQL
- SQL view creation for analytics
- Interactive dashboard delivery with Streamlit
- Ability to move from raw public data to a usable analytical product

## Problem Framing

The SBA publishes useful loan data, but the raw extracts are not ready for direct analysis. The files are wide, inconsistent across programs, and contain missing values, mixed formats, and naming differences. This project solves that by creating a clean and inspectable pipeline from source files to dashboard.

## Workflow

1. Raw SBA CSV files are inspected and previewed.
2. Data is standardized and cleaned into a single CSV.
3. The cleaned dataset is loaded into PostgreSQL.
4. SQL views are created for recurring analytical questions.
5. A Streamlit app presents the results with filters and summary metrics.

## Dataset Scope

- SBA 7(a) FOIA extract
- SBA 504 FOIA extract
- Combined cleaned dataset size: `467,294` rows

## Main Findings

- California leads the dataset in total SBA funding.
- Hospitality and food service industries rely heavily on SBA-backed lending.
- Accommodation and Food Services shows the strongest charge-off stress in the combined dataset.

## Files To Review First

- [README.md](../README.md)
- [Analysis.pdf](../Analysis.pdf)
- [app/streamlit_app.py](../app/streamlit_app.py)
- [src/clean.py](../src/clean.py)
- [sql/views.sql](../sql/views.sql)

## Technical Notes

- The local version uses PostgreSQL via `DATABASE_URL`.
- The Streamlit app has been updated to support both local `.env` configuration and deployed Streamlit secrets.
- Large raw and processed data files are intentionally excluded from Git tracking to keep the repository lightweight.
