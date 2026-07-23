# Engineering Roadmap

This roadmap turns SBA Capital Watch into a recruiter-verifiable data science project. Work is ordered by dependency: trust the data, build the model, expose the analysis, then publish the evidence.

## Release 1 — Trusted analytical dataset

### Goal
A clean run over a small included fixture must produce deterministic output and a machine-readable quality report without requiring the private production database or full FOIA extracts.

### Build
- `src/contracts.py`: canonical columns, dtypes, nullable rules, valid ranges, and category normalization.
- `src/quality.py`: row counts, duplicates, null rates, invalid ranges, category drift, and program coverage.
- `tests/fixtures/sba_sample_raw.csv`: synthetic sample covering 7(a), 504, missing values, duplicates, currency strings, and charge-offs.
- `tests/test_clean.py`: column aliases, numeric parsing, date parsing, trimming, deduplication, derived lender behavior, and pipeline determinism.
- `tests/test_contracts.py`: contract violation codes, severity levels, and program alias normalization.
- `tests/test_quality.py`: quality thresholds, JSON report shape, and CLI exit codes.
- `pyproject.toml`: pytest, coverage, Ruff, and project configuration.

### Done when
- `python -m pytest` passes from a clean environment.
- The sample pipeline creates a deterministic cleaned file.
- `python -m src.quality --input <file> --output artifacts/data_quality.json` exits non-zero on contract violations.
- No raw FOIA file or database is required by CI.

## Release 2 — Leakage-safe charge-off risk baseline

### Goal
Build an honest predictive baseline that demonstrates statistical modeling without presenting the project as a production underwriting system.

### Build
- `ml/features.py`: use only fields available at approval time.
- `ml/train.py`: temporal train/validation/test split by approval fiscal year.
- `ml/evaluate.py`: ROC-AUC, PR-AUC, Brier score, calibration, confusion matrix, and threshold table.
- Baseline models: regularized logistic regression and one tree-based model.
- Handle imbalance with class weighting; do not oversample across time splits.
- Explicitly exclude leakage fields such as charge-off amount/date, paid-in-full date, and post-outcome status.
- `artifacts/metrics.json`, `artifacts/feature_importance.csv`, and reproducible plots.
- `docs/MODEL_CARD.md`: target, population, features, exclusions, intended use, limitations, bias risks, and non-use for lending decisions.
- Unit tests proving leakage columns cannot enter the feature matrix.

### Done when
- One command trains and evaluates from the documented dataset.
- Metrics are produced on a held-out future-time test set.
- Results are reproducible with a fixed seed.
- The model card states what the model cannot support.

## Release 3 — Decision-oriented Streamlit experience

### Goal
Turn the dashboard into a concise analytical case study rather than a collection of charts.

### Build
- Page 1: Executive overview with dataset scope and three supported findings.
- Page 2: Explorer with state, year, program, and industry filters.
- Page 3: Risk model with evaluation metrics, calibration, feature effects, and threshold tradeoffs.
- Page 4: Data quality with missingness, duplicate removal, coverage, and latest source date.
- Add methodology and limitations beside every risk visualization.
- Add cached aggregate queries and friendly database failure states.
- Add downloadable aggregate CSVs; do not expose borrower-level personal or business contact fields.
- Add a `Demo mode` backed by the included fixture when the database is unavailable.

### Done when
- The app remains useful without an OpenAI API key.
- The app loads in demo mode from a fresh clone.
- No individual borrower is scored or displayed.
- A recruiter can understand the problem, method, result, and limitation in under two minutes.

## Release 4 — Public evidence and CI

### Goal
Make every portfolio claim independently verifiable.

### Build
- GitHub Actions: Python 3.11 and 3.12, lint, tests, coverage, sample pipeline, and model smoke test.
- Dependency and secret scanning.
- `Makefile` or `justfile` commands: `setup`, `test`, `quality`, `train`, `app`, and `demo`.
- Architecture diagram covering ingest → clean → validate → PostgreSQL/views → model → Streamlit.
- `docs/CASE_STUDY.md` with problem, constraints, methods, findings, decisions, and limitations.
- One dashboard GIF and three static screenshots.
- Verify all numerical README claims from generated artifacts.

### Done when
- CI is green from a public clone.
- The live app and demo-mode app both work.
- No secrets, private or proprietary data, or oversized raw datasets are committed.
- The README links to generated evidence rather than unsupported claims.
