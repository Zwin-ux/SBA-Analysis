CREATE OR REPLACE VIEW vw_state_funding AS
SELECT
    borrower_state,
    COUNT(*) AS total_loans,
    SUM(COALESCE(loan_amount, 0)) AS total_funding,
    AVG(loan_amount) AS average_loan_size,
    SUM(COALESCE(jobs_supported, 0)) AS total_jobs_supported
FROM loans
WHERE borrower_state IS NOT NULL
GROUP BY borrower_state;

CREATE OR REPLACE VIEW vw_industry_funding AS
SELECT
    naics_code,
    COALESCE(MAX(naics_description), 'Unknown') AS naics_description,
    COUNT(*) AS total_loans,
    SUM(COALESCE(loan_amount, 0)) AS total_funding,
    AVG(loan_amount) AS average_loan_size,
    SUM(COALESCE(jobs_supported, 0)) AS total_jobs_supported
FROM loans
WHERE naics_code IS NOT NULL
GROUP BY naics_code;

CREATE OR REPLACE VIEW vw_loan_status_summary AS
SELECT
    loan_status,
    COUNT(*) AS total_loans,
    SUM(COALESCE(loan_amount, 0)) AS total_funding,
    SUM(COALESCE(charge_off_amount, 0)) AS total_charge_off_amount
FROM loans
WHERE loan_status IS NOT NULL
GROUP BY loan_status;

CREATE OR REPLACE VIEW vw_jobs_per_dollar AS
SELECT
    SUM(COALESCE(jobs_supported, 0)) AS total_jobs_supported,
    SUM(COALESCE(loan_amount, 0)) AS total_funding,
    CASE
        WHEN SUM(COALESCE(loan_amount, 0)) = 0 THEN NULL
        ELSE SUM(COALESCE(jobs_supported, 0)) / SUM(COALESCE(loan_amount, 0))
    END AS jobs_per_dollar
FROM loans;
