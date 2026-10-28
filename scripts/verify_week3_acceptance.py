"""Reproducible acceptance checks for the Week 1-3 database foundation.

Run from the repository root:
    py scripts/verify_week3_acceptance.py

All databases created by this script live in a temporary directory.
The committed sample records are synthetic and are not research measurements.
"""

import csv
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from solarpv_db.database import get_connection, initialize_database
from solarpv_db.importer import import_data


SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"


def count_rows(connection, table):
    """Return the row count for a fixed, trusted table name."""
    return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def verify_sample_import(work_dir):
    """Import the committed sample data and verify counts and history."""
    database_path = work_dir / "sample_acceptance.db"
    initialize_database(database_path)

    result = import_data(SAMPLE_DIR, database_path)

    expected_counts = {
        "assets": 20,
        "inspections": 40,
        "observations": 80,
        "maintenance": 10,
    }

    assert result["status"] == "imported", result
    for section, expected in expected_counts.items():
        assert result[section] == expected, (
            f"{section}: expected {expected}, got {result[section]}"
        )

    with get_connection(database_path) as connection:
        actual_counts = {
            "assets": count_rows(connection, "assets"),
            "inspections": count_rows(connection, "inspections"),
            "observations": count_rows(connection, "observations"),
            "maintenance": count_rows(connection, "maintenance_records"),
        }
        assert actual_counts == expected_counts, actual_counts

        # Every sample asset must have at least two inspection records.
        inspection_coverage = connection.execute(
            """
            SELECT a.external_id, COUNT(i.inspection_id) AS inspection_count
            FROM assets AS a
            LEFT JOIN inspections AS i ON i.asset_id = a.asset_id
            GROUP BY a.asset_id, a.external_id
            ORDER BY a.external_id
            """
        ).fetchall()

        assert len(inspection_coverage) == 20
        under_inspected = [
            row["external_id"]
            for row in inspection_coverage
            if row["inspection_count"] < 2
        ]
        assert not under_inspected, (
            f"Assets with fewer than two inspections: {under_inspected}"
        )

        # Joined asset history: one row per inspection-observation pair.
        history = connection.execute(
            """
            SELECT
                a.external_id,
                a.name AS asset_name,
                i.inspection_key,
                i.inspected_at,
                i.data_origin,
                o.observation_type,
                o.value,
                o.unit,
                o.data_quality
            FROM assets AS a
            JOIN inspections AS i ON i.asset_id = a.asset_id
            JOIN observations AS o ON o.inspection_id = i.inspection_id
            WHERE a.external_id = ?
            ORDER BY i.inspected_at, o.observation_type
            """.replace("i.inspection_key", "i.inspection_id"),
            ("PV-001",),
        ).fetchall()

        # The current schema uses integer inspection_id, not inspection_key.
        assert len(history) == 4, (
            f"Expected four joined history rows for PV-001, got {len(history)}"
        )
        assert all(row["data_origin"] == "synthetic" for row in history)

    # Re-importing byte-identical CSV files must be skipped without duplicates.
    second_result = import_data(SAMPLE_DIR, database_path)
    assert second_result["status"] == "skipped", second_result

    with get_connection(database_path) as connection:
        assert count_rows(connection, "assets") == 20
        assert count_rows(connection, "inspections") == 40
        assert count_rows(connection, "observations") == 80
        assert count_rows(connection, "maintenance_records") == 10

    print("PASS: sample import counts and inspection coverage")
    print("PASS: joined inspection/observation history for PV-001")
    print("PASS: identical re-import is skipped without duplicates")
    print("Sample database row counts:", actual_counts)
    print("Joined history rows for PV-001:", len(history))
    for row in history:
        print(
            f"  {row['inspected_at']} | {row['observation_type']} | "
            f"{row['value']} {row['unit']} | {row['data_quality']}"
        )


def verify_invalid_import_is_rejected(work_dir):
    """An invalid observation reference must fail before writing assets."""
    invalid_dir = work_dir / "invalid_sample"
    shutil.copytree(SAMPLE_DIR, invalid_dir)

    observations_path = invalid_dir / "observations.csv"
    with observations_path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = reader.fieldnames
        rows = list(reader)

    assert rows, "Sample observations CSV must not be empty."
    rows[0]["inspection_key"] = "INSP-DOES-NOT-EXIST"

    with observations_path.open(
        "w", encoding="utf-8", newline=""
    ) as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    database_path = work_dir / "invalid_import.db"
    initialize_database(database_path)

    try:
        import_data(invalid_dir, database_path)
    except ValueError as error:
        assert "unknown inspection_key" in str(error), str(error)
    else:
        raise AssertionError("Invalid import unexpectedly succeeded.")

    with get_connection(database_path) as connection:
        assert count_rows(connection, "assets") == 0
        assert count_rows(connection, "inspections") == 0
        assert count_rows(connection, "observations") == 0
        assert count_rows(connection, "maintenance_records") == 0

    print("PASS: invalid observation reference rejected before database writes")


def verify_transaction_rollback(work_dir):
    """A later uniqueness failure must roll back earlier inserts in the batch."""
    database_path = work_dir / "rollback_acceptance.db"
    initialize_database(database_path)

    with get_connection(database_path) as connection:
        connection.execute(
            """
            INSERT INTO assets (external_id, asset_type, name, description, location)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "PV-001",
                "panel",
                "Pre-existing Panel",
                "Existing record for rollback acceptance test",
                "Acceptance Test Site",
            ),
        )

    conflict_path = work_dir / "rollback_conflict.json"
    conflict_data = {
        "assets": [
            {
                "external_id": "PV-ROLLBACK",
                "asset_type": "panel",
                "name": "Temporary Panel",
                "description": "Must be rolled back",
                "location": "Acceptance Test Site",
            },
            {
                "external_id": "PV-001",
                "asset_type": "panel",
                "name": "Conflicting Panel",
                "description": "This external ID already exists",
                "location": "Acceptance Test Site",
            },
        ],
        "inspections": [],
        "observations": [],
        "maintenance": [],
    }
    conflict_path.write_text(
        json.dumps(conflict_data, indent=2),
        encoding="utf-8",
    )

    try:
        import_data(conflict_path, database_path)
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("Expected uniqueness conflict did not occur.")

    with get_connection(database_path) as connection:
        assets = connection.execute(
            "SELECT external_id FROM assets ORDER BY external_id"
        ).fetchall()
        assert [row["external_id"] for row in assets] == ["PV-001"]

        assert count_rows(connection, "inspections") == 0
        assert count_rows(connection, "observations") == 0
        assert count_rows(connection, "maintenance_records") == 0
        assert count_rows(connection, "imported_files") == 0

    print("PASS: database constraint failure rolls back the whole import")


def main():
    if not SAMPLE_DIR.is_dir():
        raise FileNotFoundError(f"Sample dataset directory not found: {SAMPLE_DIR}")

    with tempfile.TemporaryDirectory(prefix="solarpv_acceptance_") as temp_dir:
        work_dir = Path(temp_dir)
        verify_sample_import(work_dir)
        verify_invalid_import_is_rejected(work_dir)
        verify_transaction_rollback(work_dir)

    print("\nALL WEEK 1-3 ACCEPTANCE CHECKS PASSED.")


if __name__ == "__main__":
    main()
