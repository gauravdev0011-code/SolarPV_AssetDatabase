# Solar PV Asset Database

A Python and SQLite project for maintaining solar asset records, preserving inspection history, recording observations and maintenance, and preparing trustworthy data for condition-trend analysis.

## Project status

**Implemented and tested:** SQLite schema and initialization, asset operations, inspection history, observations, maintenance records, JSON/CSV import, source-content duplicate protection, input validation, and transaction rollback.

**Week 1–3 acceptance dataset:** 20 synthetic assets, 40 inspections (at least two per asset), 80 observations, and 10 maintenance records. The dataset is illustrative only and is not real solar-panel research data.

**Next phase:** build the dashboard and condition-trend views after acceptance checks pass.

## Data integrity principles

- Each asset has a stable external identifier such as `PV-001`.
- An inspection is a separate historical event; later inspections do not overwrite earlier inspections.
- Observations belong to an inspection, and maintenance records belong to an asset.
- Measurement values include units and a data-quality label.
- Real, synthetic, and unknown data origins are distinguished.
- Imports are validated before records are written.
- Import writes run in a database transaction; a database constraint failure rolls back the batch.
- Re-importing the same source content is skipped using a SHA-256 content fingerprint.
- Sample data is explicitly marked synthetic. Do not present it as measured research data.

## Requirements

- Python 3.13 or later
- pip
- Git (for cloning the repository)

The project uses the Python standard library for its database and importer. Pytest is used for automated tests.

## Set up on Windows

From PowerShell, clone the repository and enter its folder:

```powershell
git clone https://github.com/gauravdev0011-code/SolarPV_AssetDatabase.git
cd SolarPV_AssetDatabase
```

Create and activate a virtual environment:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project and development dependencies:

```powershell
py -m pip install --upgrade pip
py -m pip install -e ".[dev]"
```

If PowerShell blocks virtual-environment activation, use the Python executable inside the environment directly, for example `.venv\Scripts\python.exe -m pytest -v`.

## Run automated tests

Run the complete unit-test suite:

```powershell
py -m pytest -v
```

The current suite contains 49 tests covering database setup and the asset, inspection, observation, maintenance, and importer operations. The expected result is 49 passing tests; rerun this command after code changes.

## Run Week 1–3 acceptance checks

Run from the repository root:

```powershell
py scripts/verify_week3_acceptance.py
```

The acceptance script creates temporary databases and checks:

1. The sample CSV dataset imports 20 assets, 40 inspections, 80 observations, and 10 maintenance records.
2. Every asset has at least two inspections.
3. A joined SQL query retrieves inspection and observation history for `PV-001`.
4. Re-importing the identical dataset is skipped and does not duplicate records.
5. An observation referencing a nonexistent inspection is rejected before any sample records are written.
6. A deliberate uniqueness conflict after an earlier insert causes the complete import transaction to roll back.

The script stores its test databases in a temporary directory and removes them when it finishes. It does not use or overwrite the normal project database.

## Import data

The importer accepts either:

- A JSON file with exactly four top-level arrays: `assets`, `inspections`, `observations`, and `maintenance`; or
- A directory containing exactly these four CSV files: `assets.csv`, `inspections.csv`, `observations.csv`, and `maintenance.csv`.

The committed example is in `data/sample/`. It is synthetic demonstration data.

After the project is installed, a CSV folder can be imported into the default database with:

```powershell
py -c "from solarpv_db.database import initialize_database; from solarpv_db.importer import import_data; initialize_database(); print(import_data('data/sample'))"
```

The default database path is `data/solarpv.db`. The importer creates an `imported_files` table on first use to record source hashes. Re-importing identical file content returns `status: skipped`. A changed dataset is not automatically merged or updated; records that conflict with existing unique identifiers will fail rather than silently overwrite existing history.

## Database tables

| Table | Purpose |
|---|---|
| `assets` | Stable asset identity, type, name, description, and location |
| `inspections` | Timestamped inspection events and their origin/status |
| `observations` | Measurements or findings linked to an inspection |
| `maintenance_records` | Maintenance history linked to an asset |
| `imported_files` | Source-content fingerprints used for duplicate-import protection |

The detailed field-level data dictionary is in [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md).

## Repository layout

```text
migrations/001_initial_schema.sql   Initial database schema
src/solarpv_db/                     Database and domain operations
tests/unit/                         Automated unit tests
data/sample/                        Approved synthetic CSV dataset
scripts/verify_week3_acceptance.py  Reproducible acceptance checks
docs/DATA_DICTIONARY.md             Field definitions and allowed values
```

## Research-data and privacy policy

Do not commit private research files, credentials, personally identifiable information, or a real local database. Only share research data when it has been approved for repository use. Keep data origin, units, quality, and source references intact when importing real observations.

## Local ingestion API and worker (Phase 2B)

Set explicit local paths before starting either process. Keep the storage path private and outside the repository:

```powershell
$env:SOLARPV_DB_PATH = "C:\SolarPV\solarpv.db"
$env:SOLARPV_STORAGE_PATH = "C:\SolarPV\private-storage"
python -m uvicorn solarpv_api.app:app --host 127.0.0.1 --port 8000
```

In a separate terminal with the same environment, run the sequential durable worker:

```powershell
python -m solarpv_ingestion.worker
```

The API accepts individual `.csv` and `.json` uploads at `POST /api/v1/uploads`. Check source metadata at `GET /api/v1/uploads/{source_file_id}`, job state at `GET /api/v1/jobs/{job_id}`, request cancellation at `POST /api/v1/jobs/{job_id}/cancel`, and retrieve a completed processing profile at `GET /api/v1/jobs/{job_id}/result`. Processing creates generic extraction summaries only; it does not insert uploaded records into Solar PV domain tables. Configure `SOLARPV_MAX_FILE_BYTES`, `SOLARPV_MAX_JSON_BYTES`, `SOLARPV_LEASE_SECONDS`, `SOLARPV_RETRY_DELAY_SECONDS`, and `SOLARPV_WORKER_POLL_SECONDS` to adjust resource and worker limits.
