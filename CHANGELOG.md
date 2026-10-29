# Changelog

Notable project changes are recorded here.

## Week 1–3 foundation

- Established the Python and SQLite database foundation for assets, inspections, observations, and maintenance records.
- Added CSV/JSON import with input validation, duplicate-source detection, and transaction rollback.
- Added synthetic sample data and repeatable acceptance checks.
- Added unit tests for database operations and import behavior.
- Fixed SQLite connection cleanup so the acceptance script can remove temporary database files on Windows.
- Verified 49 unit tests and all Week 1–3 acceptance checks in the reported test run.

The sample dataset is synthetic and is not real solar research data.
