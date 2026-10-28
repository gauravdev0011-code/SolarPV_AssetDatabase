# Data Dictionary

This document describes the current SQLite schema in `migrations/001_initial_schema.sql` and the `imported_files` tracking table created by the importer.

All timestamps should be supplied as ISO 8601 text. The sample dataset uses UTC timestamps ending in `Z`. Do not assume a synthetic value is a real sensor reading.

## Relationship overview

```text
assets 1 ───── * inspections 1 ───── * observations
  │
  └─────────── * maintenance_records

imported_files stores source fingerprints; it is not a research asset table.
```

## `assets`

One row per tracked physical or logical solar asset.

| Field | Type | Meaning |
|---|---|---|
| `asset_id` | INTEGER, primary key | Internal database identifier |
| `external_id` | TEXT, unique, nullable | Stable identifier from a source system, e.g. `PV-001`; required by the sample importer |
| `asset_type` | TEXT, required | Asset category, e.g. `panel` or `inverter` |
| `name` | TEXT, required | Human-readable asset name |
| `description` | TEXT, nullable | Additional context |
| `location` | TEXT, nullable | Site, array, row, or other location description |
| `created_at` | TEXT | UTC timestamp generated when the row is created |
| `updated_at` | TEXT | UTC timestamp generated at initial creation; update logic should maintain it when editing |

## `inspections`

A historical inspection event for an asset. Keep separate rows for separate inspection dates.

| Field | Type | Meaning |
|---|---|---|
| `inspection_id` | INTEGER, primary key | Internal inspection identifier |
| `asset_id` | INTEGER, foreign key | References `assets.asset_id` |
| `inspected_at` | TEXT, required | Date/time of the inspection |
| `inspection_method` | TEXT, nullable in schema | Method, e.g. visual inspection or manual demo reading; importer requires it |
| `data_source` | TEXT, nullable in schema | Source/system or dataset name; importer requires it |
| `data_origin` | TEXT, required | Allowed: `real`, `synthetic`, `unknown` |
| `status` | TEXT, required | Allowed: `planned`, `in_progress`, `completed`, `cancelled` |
| `notes` | TEXT, nullable | Context about the inspection |
| `created_at` | TEXT | Timestamp when the database row was created |

The import file uses `inspection_key` as a temporary, dataset-local reference to connect observations to inspections. It is not a column in the SQLite `inspections` table.

## `observations`

A measured value or qualitative finding associated with an inspection.

| Field | Type | Meaning |
|---|---|---|
| `observation_id` | INTEGER, primary key | Internal observation identifier |
| `inspection_id` | INTEGER, foreign key | References `inspections.inspection_id` |
| `observation_type` | TEXT, required | What was observed, e.g. `surface_temperature` or `string_voltage` |
| `value` | REAL, nullable | Numeric measurement; must be finite when provided |
| `unit` | TEXT, nullable | Measurement unit, e.g. `degC` or `V`; required by importer for numeric values |
| `finding` | TEXT, nullable | Qualitative finding when no numeric value is available |
| `data_quality` | TEXT, required | Allowed: `good`, `suspect`, `invalid`, `unknown` |
| `source_reference` | TEXT, nullable | Reference to the source row, file, sensor record, or measurement |
| `recorded_at` | TEXT | Timestamp when the database row was created |

At least one of `value` or `finding` must be present. A missing value is not the same as zero. Keep unknown or unavailable measurements explicitly missing instead of inventing values.

## `maintenance_records`

A maintenance or service event linked to an asset.

| Field | Type | Meaning |
|---|---|---|
| `maintenance_id` | INTEGER, primary key | Internal maintenance identifier |
| `asset_id` | INTEGER, foreign key | References `assets.asset_id` |
| `performed_at` | TEXT, nullable in schema | Date/time of maintenance; importer currently requires it |
| `maintenance_type` | TEXT, required | Category such as cleaning or inspection |
| `description` | TEXT, required | Description of work or event |
| `performed_by` | TEXT, nullable | Person/team/system, if known |
| `data_origin` | TEXT, required | Allowed: `real`, `synthetic`, `unknown` |
| `created_at` | TEXT | Timestamp when the database row was created |

## `imported_files`

Created by `solarpv_db.importer` the first time an import is attempted.

| Field | Type | Meaning |
|---|---|---|
| `source_hash` | TEXT, primary key | SHA-256 fingerprint of the JSON file bytes or of the named CSV files and their contents |
| `source_name` | TEXT, required | Source filename or CSV directory name |
| `imported_at` | TEXT | Time the successful import was recorded |

Only successfully imported sources receive a fingerprint row. Re-importing identical source content is skipped. A changed source has a different hash and is attempted as a new import; it is not an automatic update or merge.

## Import file formats

### CSV folder

The folder must contain exactly the following four CSV files:

- `assets.csv`: `external_id, asset_type, name, description, location`
- `inspections.csv`: `inspection_key, asset_external_id, inspected_at, inspection_method, data_source, data_origin, status, notes`
- `observations.csv`: `inspection_key, observation_type, value, unit, finding, data_quality, source_reference`
- `maintenance.csv`: `asset_external_id, performed_at, maintenance_type, description, performed_by, data_origin`

Empty optional CSV cells are treated as missing. An observation must have either a numeric value with a unit or a qualitative finding. References must point to records in the same import dataset.

### JSON file

The JSON object must contain exactly these top-level arrays: `assets`, `inspections`, `observations`, and `maintenance`. The field names for each record match the CSV headers above.

## Synthetic sample dataset

Files under `data/sample/` are illustrative synthetic data created for import and acceptance testing. The numeric values do not describe actual equipment, actual operating conditions, or real research findings. Do not use them to make engineering or maintenance decisions.
