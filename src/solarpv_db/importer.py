
"""Validate and import solar PV asset data from JSON or CSV files."""

import csv
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path

from solarpv_db.database import DEFAULT_DATABASE, get_connection


VALID_DATA_ORIGINS = {"real", "synthetic", "unknown"}
VALID_STATUSES = {"planned", "in_progress", "completed", "cancelled"}
VALID_DATA_QUALITIES = {"good", "suspect", "invalid", "unknown"}

CSV_FILES = (
    "assets.csv",
    "inspections.csv",
    "observations.csv",
    "maintenance.csv",
)

REQUIRED_SECTIONS = {
    "assets",
    "inspections",
    "observations",
    "maintenance",
}


def _read_csv_file(path):
    """Read CSV records, converting empty cells to None."""
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        if not reader.fieldnames:
            raise ValueError(f"CSV file has no header: {path.name}")

        return [
            {
                key: value if value != "" else None
                for key, value in row.items()
            }
            for row in reader
        ]


def _read_source(source_path):
    """Read a JSON file or a folder containing the four required CSV files."""
    source = Path(source_path)

    if not source.exists():
        raise FileNotFoundError(f"Import source does not exist: {source}")

    if source.is_file():
        if source.suffix.lower() != ".json":
            raise ValueError("A file import source must be a JSON file.")

        raw_bytes = source.read_bytes()

        try:
            data = json.loads(raw_bytes.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                f"Could not read valid JSON from {source.name}."
            ) from error

        return data, hashlib.sha256(raw_bytes).hexdigest(), source.name

    if not source.is_dir():
        raise ValueError("Import source must be a JSON file or a CSV folder.")

    expected_files = set(CSV_FILES)
    actual_csv_files = {
        path.name
        for path in source.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    }

    missing_files = expected_files - actual_csv_files
    extra_files = actual_csv_files - expected_files

    if missing_files or extra_files:
        problems = []

        if missing_files:
            problems.append(
                f"missing files: {', '.join(sorted(missing_files))}"
            )

        if extra_files:
            problems.append(
                f"unexpected CSV files: {', '.join(sorted(extra_files))}"
            )

        raise ValueError("Invalid CSV folder; " + "; ".join(problems) + ".")

    data = {
        "assets": _read_csv_file(source / "assets.csv"),
        "inspections": _read_csv_file(source / "inspections.csv"),
        "observations": _read_csv_file(source / "observations.csv"),
        "maintenance": _read_csv_file(source / "maintenance.csv"),
    }

    digest = hashlib.sha256()

    for filename in sorted(CSV_FILES):
        digest.update(filename.encode("utf-8"))
        digest.update(b"\0")
        digest.update((source / filename).read_bytes())
        digest.update(b"\0")

    return data, digest.hexdigest(), source.name


def _required_text(record, field, context):
    """Return a required, non-empty string."""
    value = record.get(field)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: '{field}' is required.")

    return value.strip()


def _optional_text(record, field):
    """Return trimmed optional text, or None when it is empty."""
    value = record.get(field)

    if value is None:
        return None

    if not isinstance(value, str):
        value = str(value)

    value = value.strip()
    return value if value else None


def _validate_timestamp(value, context, field):
    """Validate a non-empty ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: '{field}' is required.")

    try:
        datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            f"{context}: '{field}' must be a valid ISO 8601 timestamp."
        ) from error

    return value.strip()


def _validate_records(data):
    """Validate all records and references before starting database writes."""
    if not isinstance(data, dict):
        raise ValueError("Import data must be a JSON object or CSV dataset.")

    if set(data) != REQUIRED_SECTIONS:
        missing = REQUIRED_SECTIONS - set(data)
        extra = set(data) - REQUIRED_SECTIONS
        problems = []

        if missing:
            problems.append(f"missing sections: {', '.join(sorted(missing))}")

        if extra:
            problems.append(f"unexpected sections: {', '.join(sorted(extra))}")

        raise ValueError("Invalid dataset; " + "; ".join(problems) + ".")

    for section in REQUIRED_SECTIONS:
        if not isinstance(data[section], list):
            raise ValueError(
                f"Section '{section}' must contain a list of records."
            )

        for index, record in enumerate(data[section], start=1):
            if not isinstance(record, dict):
                raise ValueError(
                    f"Section '{section}', record {index}, must be an object."
                )

    asset_ids = set()

    for index, asset in enumerate(data["assets"], start=1):
        context = f"Asset record {index}"

        external_id = _required_text(asset, "external_id", context)
        _required_text(asset, "asset_type", context)
        _required_text(asset, "name", context)

        if external_id in asset_ids:
            raise ValueError(
                f"{context}: duplicate external_id '{external_id}'."
            )

        asset_ids.add(external_id)

    inspection_keys = set()

    for index, inspection in enumerate(data["inspections"], start=1):
        context = f"Inspection record {index}"

        inspection_key = _required_text(
            inspection, "inspection_key", context
        )
        asset_external_id = _required_text(
            inspection, "asset_external_id", context
        )

        if inspection_key in inspection_keys:
            raise ValueError(
                f"{context}: duplicate inspection_key '{inspection_key}'."
            )

        if asset_external_id not in asset_ids:
            raise ValueError(
                f"{context}: unknown asset_external_id "
                f"'{asset_external_id}'."
            )

        inspection_keys.add(inspection_key)

        _validate_timestamp(
            inspection.get("inspected_at"), context, "inspected_at"
        )
        _required_text(inspection, "inspection_method", context)
        _required_text(inspection, "data_source", context)

        data_origin = _required_text(inspection, "data_origin", context)
        if data_origin not in VALID_DATA_ORIGINS:
            raise ValueError(
                f"{context}: invalid data_origin '{data_origin}'."
            )

        status = _required_text(inspection, "status", context)
        if status not in VALID_STATUSES:
            raise ValueError(f"{context}: invalid status '{status}'.")

    for index, observation in enumerate(data["observations"], start=1):
        context = f"Observation record {index}"

        inspection_key = _required_text(
            observation, "inspection_key", context
        )

        if inspection_key not in inspection_keys:
            raise ValueError(
                f"{context}: unknown inspection_key '{inspection_key}'."
            )

        _required_text(observation, "observation_type", context)

        raw_value = observation.get("value")
        value = None

        if raw_value is not None and str(raw_value).strip():
            try:
                value = float(raw_value)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"{context}: 'value' must be a number or empty."
                ) from error

            if not math.isfinite(value):
                raise ValueError(
                    f"{context}: 'value' must be finite; "
                    "NaN and infinity are invalid."
                )

        unit = _optional_text(observation, "unit")
        finding = _optional_text(observation, "finding")

        if value is None and finding is None:
            raise ValueError(
                f"{context}: provide a measurement value or a finding."
            )

        if value is not None and unit is None:
            raise ValueError(
                f"{context}: 'unit' is required when a value is provided."
            )

        data_quality = _required_text(
            observation, "data_quality", context
        )
        if data_quality not in VALID_DATA_QUALITIES:
            raise ValueError(
                f"{context}: invalid data_quality '{data_quality}'."
            )

    for index, maintenance in enumerate(data["maintenance"], start=1):
        context = f"Maintenance record {index}"

        asset_external_id = _required_text(
            maintenance, "asset_external_id", context
        )

        if asset_external_id not in asset_ids:
            raise ValueError(
                f"{context}: unknown asset_external_id "
                f"'{asset_external_id}'."
            )

        _validate_timestamp(
            maintenance.get("performed_at"), context, "performed_at"
        )
        _required_text(maintenance, "maintenance_type", context)
        _required_text(maintenance, "description", context)

        data_origin = _required_text(maintenance, "data_origin", context)
        if data_origin not in VALID_DATA_ORIGINS:
            raise ValueError(
                f"{context}: invalid data_origin '{data_origin}'."
            )


def import_data(source_path, database_path=DEFAULT_DATABASE):
    """Validate and transactionally import JSON or CSV data.

    Identical source content is skipped on subsequent imports. If any database
    insert fails, all database changes from that import are rolled back.
    """
    data, source_hash, source_name = _read_source(source_path)
    _validate_records(data)

    with get_connection(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS imported_files (
                source_hash TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.commit()

        connection.execute("BEGIN IMMEDIATE")

        existing_import = connection.execute(
            """
            SELECT source_hash
            FROM imported_files
            WHERE source_hash = ?
            """,
            (source_hash,),
        ).fetchone()

        if existing_import is not None:
            connection.commit()
            return {
                "status": "skipped",
                "source_name": source_name,
                "assets": 0,
                "inspections": 0,
                "observations": 0,
                "maintenance": 0,
                "message": "Identical source content has already been imported.",
            }

        asset_map = {}
        inspection_map = {}

        for asset in data["assets"]:
            external_id = _required_text(asset, "external_id", "Asset")

            cursor = connection.execute(
                """
                INSERT INTO assets (
                    external_id,
                    asset_type,
                    name,
                    description,
                    location
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    external_id,
                    _required_text(asset, "asset_type", "Asset"),
                    _required_text(asset, "name", "Asset"),
                    _optional_text(asset, "description"),
                    _optional_text(asset, "location"),
                ),
            )

            asset_map[external_id] = cursor.lastrowid

        for inspection in data["inspections"]:
            inspection_key = inspection["inspection_key"].strip()
            asset_external_id = inspection["asset_external_id"].strip()

            cursor = connection.execute(
                """
                INSERT INTO inspections (
                    asset_id,
                    inspected_at,
                    inspection_method,
                    data_source,
                    data_origin,
                    status,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset_map[asset_external_id],
                    inspection["inspected_at"].strip(),
                    inspection["inspection_method"].strip(),
                    inspection["data_source"].strip(),
                    inspection["data_origin"].strip(),
                    inspection["status"].strip(),
                    _optional_text(inspection, "notes"),
                ),
            )

            inspection_map[inspection_key] = cursor.lastrowid

        for observation in data["observations"]:
            raw_value = observation.get("value")
            value = (
                float(raw_value)
                if raw_value is not None and str(raw_value).strip()
                else None
            )

            connection.execute(
                """
                INSERT INTO observations (
                    inspection_id,
                    observation_type,
                    value,
                    unit,
                    finding,
                    data_quality,
                    source_reference
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inspection_map[observation["inspection_key"].strip()],
                    observation["observation_type"].strip(),
                    value,
                    _optional_text(observation, "unit"),
                    _optional_text(observation, "finding"),
                    observation["data_quality"].strip(),
                    _optional_text(observation, "source_reference"),
                ),
            )

        for maintenance in data["maintenance"]:
            connection.execute(
                """
                INSERT INTO maintenance_records (
                    asset_id,
                    performed_at,
                    maintenance_type,
                    description,
                    performed_by,
                    data_origin
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    asset_map[maintenance["asset_external_id"].strip()],
                    maintenance["performed_at"].strip(),
                    maintenance["maintenance_type"].strip(),
                    maintenance["description"].strip(),
                    _optional_text(maintenance, "performed_by"),
                    maintenance["data_origin"].strip(),
                ),
            )

        connection.execute(
            """
            INSERT INTO imported_files (source_hash, source_name)
            VALUES (?, ?)
            """,
            (source_hash, source_name),
        )

        connection.commit()

    return {
        "status": "imported",
        "source_name": source_name,
        "assets": len(data["assets"]),
        "inspections": len(data["inspections"]),
        "observations": len(data["observations"]),
        "maintenance": len(data["maintenance"]),
        "message": "Import completed successfully.",
    }
