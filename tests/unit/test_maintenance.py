
"""Tests for maintenance management."""

import sqlite3

import pytest

from solarpv_db.assets import create_asset
from solarpv_db.database import initialize_database
from solarpv_db.maintenance import (
    create_maintenance_record,
    get_maintenance_record,
    list_maintenance_records,
)


@pytest.fixture
def database_path(tmp_path):
    path = tmp_path / "test_solar.db"
    initialize_database(path)
    return path


@pytest.fixture
def asset_id(database_path):
    return create_asset(
        "panel",
        "Test Panel",
        database_path=database_path,
    )


def test_create_and_get_maintenance_record(asset_id, database_path):
    record_id = create_maintenance_record(
        asset_id=asset_id,
        performed_at="2026-10-27T10:00:00Z",
        maintenance_type="cleaning",
        description="Cleaned the panel surface",
        performed_by="Research team",
        data_origin="synthetic",
        database_path=database_path,
    )

    record = get_maintenance_record(record_id, database_path)

    assert record is not None
    assert record["asset_id"] == asset_id
    assert record["performed_at"] == "2026-10-27T10:00:00Z"
    assert record["maintenance_type"] == "cleaning"
    assert record["description"] == "Cleaned the panel surface"
    assert record["performed_by"] == "Research team"
    assert record["data_origin"] == "synthetic"


def test_missing_record_returns_none(database_path):
    assert get_maintenance_record(999, database_path) is None


def test_list_maintenance_records_newest_first(asset_id, database_path):
    create_maintenance_record(
        asset_id=asset_id,
        performed_at="2026-10-20T10:00:00Z",
        maintenance_type="inspection",
        description="Checked the panel condition",
        database_path=database_path,
    )
    create_maintenance_record(
        asset_id=asset_id,
        performed_at="2026-10-27T10:00:00Z",
        maintenance_type="cleaning",
        description="Cleaned the panel surface",
        database_path=database_path,
    )

    records = list_maintenance_records(asset_id, database_path)

    assert len(records) == 2
    assert records[0]["maintenance_type"] == "cleaning"
    assert records[1]["maintenance_type"] == "inspection"


def test_empty_performed_at_is_rejected(asset_id, database_path):
    with pytest.raises(ValueError, match="date cannot be empty"):
        create_maintenance_record(
            asset_id=asset_id,
            performed_at=" ",
            maintenance_type="cleaning",
            description="Cleaned the panel",
            database_path=database_path,
        )


def test_empty_maintenance_type_is_rejected(asset_id, database_path):
    with pytest.raises(ValueError, match="type cannot be empty"):
        create_maintenance_record(
            asset_id=asset_id,
            performed_at="2026-10-27T10:00:00Z",
            maintenance_type=" ",
            description="Cleaned the panel",
            database_path=database_path,
        )


@pytest.mark.parametrize("description", ["", "   "])
def test_empty_description_is_rejected(
    asset_id, database_path, description
):
    with pytest.raises(ValueError, match="description cannot be empty"):
        create_maintenance_record(
            asset_id=asset_id,
            performed_at="2026-10-27T10:00:00Z",
            maintenance_type="cleaning",
            description=description,
            database_path=database_path,
        )


def test_invalid_data_origin_is_rejected(asset_id, database_path):
    with pytest.raises(ValueError, match="Invalid data origin"):
        create_maintenance_record(
            asset_id=asset_id,
            performed_at="2026-10-27T10:00:00Z",
            maintenance_type="cleaning",
            description="Cleaned the panel",
            data_origin="estimated",
            database_path=database_path,
        )


def test_nonexistent_asset_is_rejected(database_path):
    with pytest.raises(sqlite3.IntegrityError):
        create_maintenance_record(
            asset_id=999,
            performed_at="2026-10-27T10:00:00Z",
            maintenance_type="cleaning",
            description="Cleaned the panel",
            database_path=database_path,
        )


def test_maintenance_history_is_scoped_to_asset(database_path):
    first_asset = create_asset(
        "panel",
        "First Panel",
        database_path=database_path,
    )
    second_asset = create_asset(
        "panel",
        "Second Panel",
        database_path=database_path,
    )

    create_maintenance_record(
        asset_id=first_asset,
        performed_at="2026-10-27T10:00:00Z",
        maintenance_type="cleaning",
        description="Cleaned the first panel",
        database_path=database_path,
    )
    create_maintenance_record(
        asset_id=second_asset,
        performed_at="2026-10-27T11:00:00Z",
        maintenance_type="repair",
        description="Repaired the second panel",
        database_path=database_path,
    )

    records = list_maintenance_records(first_asset, database_path)

    assert len(records) == 1
    assert records[0]["asset_id"] == first_asset
