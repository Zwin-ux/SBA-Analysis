# Test Fixtures

Fixtures in this directory must be synthetic or safely de-identified and small enough for CI.

The first fixture should cover:
- both 7(a) and 504 programs;
- column aliases from multiple source extracts;
- missing optional values;
- duplicate records;
- currency, percent, comma-separated, and invalid numeric strings;
- valid and invalid dates;
- active, paid-in-full, and charged-off outcomes;
- records from multiple states, years, and NAICS sectors.

Do not copy borrower names, addresses, contact information, credentials, or proprietary AmPac data into test fixtures.
