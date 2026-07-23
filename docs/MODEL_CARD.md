# Model Card — SBA Charge-Off Risk Baseline

**Status: no real-data metrics have been generated yet.** The training code and
its leakage controls are fully implemented and tested against deterministic
synthetic data that proves the mechanics only. Real-data metrics exist only
after an operator runs the documented command below on the actual cleaned FOIA
extract; until then, no number from this pipeline may be presented as a
real-world result.

## What this model is

Two interpretable binary classifiers estimating the probability that an
approved SBA loan is eventually charged off, using only information available
at approval time:

- Regularized logistic regression (`class_weight="balanced"`, L2, C=1.0)
- A shallow random forest (200 trees, `max_depth=5`, `min_samples_leaf=10`,
  `class_weight="balanced"`)

Both run behind an identical preprocessing pipeline (median imputation +
standardization for numeric features; constant-fill imputation + one-hot
encoding with `handle_unknown="ignore"` for categorical features). All
randomness is seeded (`ml.RANDOM_SEED = 42`); two runs on the same input
produce byte-identical metrics artifacts.

## Intended use

- Portfolio-level analytical exploration of historical charge-off patterns.
- A demonstration of leakage-safe modeling discipline on public data.

## Explicit non-use

- **Not for credit decisions.** This model must not be used to approve, deny,
  price, or prioritize any loan, applicant, or business, nor as an input to any
  system that does.
- Not for scoring or displaying individual borrowers.
- Not a production underwriting, servicing, or collections system.

## Dataset

- Source: public SBA FOIA extracts for the 7(a) and 504 programs
  (https://data.sba.gov/dataset/7-a-504-foia), cleaned by `src/clean.py` and
  validated by `src/quality.py`.
- The project's current cleaned snapshot reports 467,294 records; the extract
  is not bundled with the repository and this baseline has not yet been trained
  on it.
- **The corpus is discontinuous:** the assembled extracts cover 504 loans from
  roughly FY1992-2009 and 7(a) loans from roughly FY2020 onward. Program and
  era are therefore confounded, and a temporal split largely trains on 504-era
  loans while testing on 7(a)-era loans. This is a structural property of the
  assembled data, disclosed rather than hidden.

## Target definition

`charged_off = 1` when `loan_status == "CHGOFF"`, `0` when
`loan_status == "PIF"` (comparison is case-insensitive after trimming).

Excluded from the modeling population (neither 0 nor 1):

- `CANCLD` (cancelled) and `EXEMPT` — never entered comparable repayment.
- Missing status — unknown outcome.
- Any other status (e.g. still-active loans) — outcome not yet observed.

### Label weaknesses

- **Right-censoring:** recent loans have not had time to resolve. Restricting
  to terminal outcomes biases recent cohorts toward loans that resolved
  unusually fast (early payoffs and early defaults).
- The label reflects the source snapshot date (`as_of_date`); statuses may be
  revised in later FOIA releases.
- Charge-off is a servicing decision, not a pure economic default measure.

## Feature policy

Enforced in `ml/features.py`, not just documented: the model matrix is built
from an explicit allowlist, and a hard denylist raises `LeakageError` if an
outcome field ever reaches it. Tests in `tests/test_ml_leakage.py` prove
denylisted columns are excluded even when present in the input.

### Allowlist (approval-time features)

| Feature | Why it is approval-time |
|---|---|
| `loan_amount` | Gross approval amount, fixed at approval. |
| `guaranteed_share` | `sba_guaranteed_approval / loan_amount`, both set at approval; bounded to [0, 1]. |
| `term_in_months` | Original contract term. |
| `initial_interest_rate` | Initial reported rate; per the data dictionary it is the *initial* value, not a revised one. |
| `program` | 7(a) vs 504, known at approval (see era confound above). |
| `borrower_state` | Borrower location at application. |
| `naics_sector` | Two-digit prefix of the NAICS code declared at application. |

### Denylist (hard leakage exclusions)

`charge_off_amount`, `charge_off_date`, `paid_in_full_date`, `loan_status`,
and the derived target column itself. These encode the outcome.

### Deliberately excluded despite being available

- `approval_fiscal_year` — it is the temporal split key; letting models
  memorize calendar years does not transfer to future years.
- `jobs_supported` — the data dictionary classifies it as source-reported and
  descriptive, not a verified approval-time attribute.
- `collateral_ind`, `sold_secondary_market_ind` — approval-time availability
  not confirmed in source documentation (the data dictionary requires timing
  review before modeling use).
- `business_type`, `business_age`, `processing_method`, `delivery_method` —
  candidate approval-time features deferred from this baseline; category
  vocabularies differ across extracts and need normalization first.
- Borrower name/address/zip, lender identifiers — identification, not risk
  structure; excluded for privacy discipline.

## Evaluation method

- **Temporal split** by `approval_fiscal_year` (`ml/split.py`): train on the
  earliest years (~60% of rows), validate on middle years (~20%), test on the
  newest years (~20%). Whole years stay together; no year appears in two
  subsets; no random shuffling anywhere.
- Metrics per model and split (`ml/evaluate.py`): ROC-AUC, PR-AUC, Brier
  score, base rate, sample counts, confusion matrices and precision/recall at
  thresholds 0.25 / 0.50 / 0.75, calibration (reliability) table, and
  coefficients/importances.
- Artifacts (all gitignored, regenerated on demand): `artifacts/model_metrics.json`,
  `artifacts/threshold_table.csv`, `artifacts/feature_effects.csv`,
  `artifacts/calibration.csv`.

### Reproduction command

```bash
python -m pip install -e ".[dev,ml]"
python -m ml.train --input data/processed/sba_loans_clean.csv
```

The command exits with instructions if the cleaned dataset is absent.

## Limitations, bias, and representativeness

- **Selection on approval:** the data contains only approved loans. The model
  learns nothing about rejected applicants and cannot be used to reason about
  approval decisions.
- **Program/era confound:** 504 records dominate the early years and 7(a) the
  recent years, so program effects, macro-era effects, and split boundaries
  are entangled.
- **Macro regime shifts:** the 504 window includes the 2008 financial crisis;
  the 7(a) window includes COVID-era programs. Charge-off behavior under one
  regime does not generalize to another, which is exactly what a temporal
  split will expose.
- **Censoring bias** in recent cohorts (see label weaknesses).
- Geographic and industry composition reflects historical SBA lending, not the
  economy at large; performance may be materially worse for small states and
  sparse NAICS sectors.
- State and sector features can proxy for demographic composition. This is a
  further reason the model must not touch lending decisions.
- **No causal claims:** coefficients and importances describe associations in
  historical data, not levers. Nothing here says *why* loans charge off.
- Historical SBA data does not generalize to future lending: program rules,
  guarantee shares, fee structures, and underwriting standards have all
  changed over the covered decades.
