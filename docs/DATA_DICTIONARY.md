# Analytical Data Dictionary

This dictionary documents the cleaned fields used by SBA Capital Watch. Source extracts can contain additional columns; recruiter-facing analysis should rely on the normalized names below.

## Core identifiers and scope

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `as_of_date` | date | Source extract snapshot date | Indicates data freshness, not loan activity date. |
| `program` | text | Normalized SBA program | Canonical values: `7(a)` and `504`. |
| `loan_id` | text / integer-like | Public source loan identifier | Treat as an identifier, not a measurable quantity. |
| `source_file` | text | Input extract filename | Used for lineage and debugging. |

## Borrower and geography

| Column | Type | Meaning | Recruiter-facing use |
|---|---|---|---|
| `borrower_name` | text | Borrower or business name | Do not expose in aggregate portfolio demos unless necessary. |
| `borrower_city` | text | Borrower city | Aggregate analysis only. |
| `borrower_state` | two-character text | Borrower state or territory | Primary geographic filter. |
| `borrower_zip` | text | Borrower ZIP code | Preserve leading zeros; do not treat as numeric. |
| `project_county` | text | Project county | Aggregate geographic analysis. |
| `project_state` | two-character text | Project state | Can differ from borrower state. |
| `congressional_district` | text | Congressional district designation | Preserve as text. |

## Lender fields

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `lender_name` | text | Unified lender display name | Filled from available bank, third-party lender, or CDC fields. |
| `bank_fdic_number` | text | FDIC institution identifier | Identifier, not a metric. |
| `bank_ncua_number` | text | NCUA institution identifier | Identifier, not a metric. |
| `bank_state` | two-character text | Bank state | Normalized uppercase when present. |
| `cdc_name` | text | Certified Development Company name | Primarily relevant to 504 records. |
| `third_party_lender_name` | text | Third-party lender name | Primarily relevant to 504 records. |
| `third_party_lender_state` | two-character text | Third-party lender state | Normalized uppercase when present. |
| `third_party_dollars` | decimal | Third-party lender dollars | Must be non-negative when present. |

## Loan amounts and approval

| Column | Type | Meaning | Contract |
|---|---|---|---|
| `loan_amount` | decimal | Original gross approval or normalized loan amount | Required; non-negative. |
| `sba_guaranteed_approval` | decimal | SBA-guaranteed portion of approval | Non-negative when present. |
| `approval_date` | date | Loan approval date | Core time dimension. |
| `approval_fiscal_year` | integer | SBA approval fiscal year | Required; bounded to plausible SBA years. |
| `disbursement_date` | date | First disbursement date | Should not precede approval. |
| `term_in_months` | integer | Original term in months | Expected range: 0–600. |
| `initial_interest_rate` | decimal | Reported initial interest rate | Stored as percentage points; expected range: 0–100. |
| `fixed_or_variable_interest_ind` | text | Fixed/variable indicator | Category values depend on source extract. |

## Industry and business attributes

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `naics_code` | text | NAICS industry code | Preserve leading zeros and hierarchical structure. |
| `naics_description` | text | NAICS industry description | Used for readable dashboard labels. |
| `business_type` | text | Reported business organization type | Category normalization may be needed across extracts. |
| `business_age` | text | Reported new/existing business indicator | Source terminology may vary. |
| `franchise_code` | text | Franchise identifier | Preserve as text. |
| `franchise_name` | text | Franchise name | Optional. |
| `jobs_supported` | nullable integer | Source-reported jobs supported | Non-negative; descriptive, not independently verified. |

## Status and outcome fields

| Column | Type | Meaning | Modeling rule |
|---|---|---|---|
| `loan_status` | text | Current or reported loan status | Descriptive dashboard use; exclude post-outcome versions from approval-time modeling. |
| `paid_in_full_date` | date | Paid-in-full date | Outcome leakage; never use as an approval-time feature. |
| `charge_off_date` | date | Charge-off date | Outcome leakage; never use as an approval-time feature. |
| `charge_off_amount` | decimal | Reported gross charge-off amount | Outcome/target information; non-negative and should not exceed original loan amount. |
| `revolver_status` | text | Revolver indicator/status | Interpret only with source documentation. |
| `collateral_ind` | text | Reported collateral indicator | Candidate approval-time feature only after source review. |
| `sold_secondary_market_ind` | text | Secondary-market sale indicator | Timing must be verified before any modeling use. |

## Planned modeling feature policy

### Potential approval-time features

- Program
- Approval fiscal year and date-derived features
- Loan amount
- SBA guarantee share
- Term
- Initial interest rate
- State
- NAICS sector
- Business type and business age
- Processing and delivery method
- Collateral indicator, only if confirmed available at approval

### Explicit leakage exclusions

- `charge_off_amount`
- `charge_off_date`
- `paid_in_full_date`
- Outcome-derived `loan_status`
- Fields updated after approval without a reliable historical snapshot

## Quality expectations

The executable contract currently checks:

- Required presence of `program`, `loan_amount`, and `approval_fiscal_year`
- Canonical program values
- Non-negative financial and jobs fields
- Plausible fiscal years, terms, and interest rates
- Two-character state formatting
- Charge-off amount not exceeding original loan amount
- Disbursement and charge-off dates not preceding approval
- Duplicate rows and configurable null-rate thresholds
