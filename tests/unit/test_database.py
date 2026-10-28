
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
