
"""Tests for the Solar PV database setup and integrity rules."""

import sqlite3

import pytest

from solarpv_db.database import get_connection, initialize_database


def test_initialize_database_creates_expected_tables(tmp_path):
    """Initialization should create all four core tables."""
    database_path = tmp_path / "test_solar.db"

    initialize_database(database_path)

    with get_connection(database_path) as connection:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()

    table_names = {row["name"] for row in rows}

    assert {
        "assets",
        "inspections",
        "observations",
        "maintenance_records",
    }.issubset(table_names)


def test_initialize_database_can_run_twice(tmp_path):
    """Repeated initialization should not fail."""
    database_path = tmp_path / "test_solar.db"

    initialize_database(database_path)
    initialize_database(database_path)

    assert database_path.exists()


def test_migration_010_preserves_legacy_domain_rows_and_assigns_default_workspace(tmp_path):
    database_path = tmp_path / "populated-legacy.db"
    legacy = sqlite3.connect(database_path)
    from solarpv_db.database import SCHEMA_FILE
    legacy.executescript(SCHEMA_FILE.read_text(encoding="utf-8"))
    asset_id = legacy.execute(
        "INSERT INTO assets(external_id,asset_type,name,location) VALUES('LEGACY-1','module','Legacy panel','Row A')"
    ).lastrowid
    inspection_id = legacy.execute(
        "INSERT INTO inspections(asset_id,inspected_at,data_source) VALUES(?,?,?)",
        (asset_id, "2025-04-03T10:00:00Z", "legacy log"),
    ).lastrowid
    observation_id = legacy.execute(
        "INSERT INTO observations(inspection_id,observation_type,value,unit,source_reference) VALUES(?,?,?,?,?)",
        (inspection_id, "temperature", 0, "C", "sheet-2/row-7"),
    ).lastrowid
    maintenance_id = legacy.execute(
        "INSERT INTO maintenance_records(asset_id,performed_at,maintenance_type,description) VALUES(?,?,?,?)",
        (asset_id, "2025-05-01", "inspection", "Historical maintenance"),
    ).lastrowid
    legacy.execute(
        "CREATE TABLE imported_files(source_hash TEXT PRIMARY KEY,source_name TEXT NOT NULL,imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    legacy.execute("INSERT INTO imported_files(source_hash,source_name) VALUES('legacy-hash','legacy.csv')")
    legacy.commit()
    legacy.close()

    initialize_database(database_path)

    with get_connection(database_path) as migrated:
        assert tuple(migrated.execute(
            "SELECT asset_id,external_id,name,location,workspace_id,site_id,data_origin FROM assets"
        ).fetchone()) == (asset_id, "LEGACY-1", "Legacy panel", "Row A", "default", None, "unknown")
        assert tuple(migrated.execute(
            "SELECT inspection_id,asset_id,inspected_at,data_source,workspace_id FROM inspections"
        ).fetchone()) == (inspection_id, asset_id, "2025-04-03T10:00:00Z", "legacy log", "default")
        assert tuple(migrated.execute(
            "SELECT observation_id,inspection_id,value,source_reference,workspace_id FROM observations"
        ).fetchone()) == (observation_id, inspection_id, 0.0, "sheet-2/row-7", "default")
        assert tuple(migrated.execute(
            "SELECT maintenance_id,asset_id,description,workspace_id FROM maintenance_records"
        ).fetchone()) == (maintenance_id, asset_id, "Historical maintenance", "default")
        assert tuple(migrated.execute(
            "SELECT workspace_id,source_hash,source_name FROM imported_files"
        ).fetchone()) == ("default", "legacy-hash", "legacy.csv")
        assert migrated.execute("PRAGMA foreign_key_check").fetchall() == []
        assert migrated.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert migrated.execute("PRAGMA user_version").fetchone()[0] == 10


def test_foreign_keys_are_enabled(tmp_path):
    """Connections should enforce foreign-key constraints."""
    database_path = tmp_path / "test_solar.db"
    initialize_database(database_path)

    with get_connection(database_path) as connection:
        result = connection.execute(
            "PRAGMA foreign_keys"
        ).fetchone()[0]

    assert result == 1


def test_inspection_requires_existing_asset(tmp_path):
    """An inspection cannot reference a nonexistent asset."""
    database_path = tmp_path / "test_solar.db"
    initialize_database(database_path)

    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO inspections (
                    asset_id,
                    inspected_at
                )
                VALUES (?, ?)
                """,
                (9999, "2026-10-27T10:00:00Z"),
            )


def test_invalid_inspection_status_is_rejected(tmp_path):
    """Inspection status must be one of the allowed values."""
    database_path = tmp_path / "test_solar.db"
    initialize_database(database_path)

    with get_connection(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO assets (asset_type, name)
            VALUES (?, ?)
            """,
            ("panel", "Test Panel"),
        )
        asset_id = cursor.lastrowid

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO inspections (
                    asset_id,
                    inspected_at,
                    status
                )
                VALUES (?, ?, ?)
                """,
                (asset_id, "2026-10-27T10:00:00Z", "invalid_status"),
            )


def test_asset_with_inspection_history_cannot_be_deleted(tmp_path):
    """Existing inspection history should prevent asset deletion."""
    database_path = tmp_path / "test_solar.db"
    initialize_database(database_path)

    with get_connection(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO assets (asset_type, name)
            VALUES (?, ?)
            """,
            ("panel", "Historical Test Panel"),
        )
        asset_id = cursor.lastrowid

        connection.execute(
            """
            INSERT INTO inspections (asset_id, inspected_at)
            VALUES (?, ?)
            """,
            (asset_id, "2026-10-27T10:00:00Z"),
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "DELETE FROM assets WHERE asset_id = ?",
                (asset_id,),
            )
