
"""Tests for inspection management."""

import sqlite3

import pytest

from solarpv_db.assets import create_asset
from solarpv_db.database import initialize_database
from solarpv_db.inspections import (
    create_inspection,
    get_inspection,
    list_inspections,
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


def test_create_and_get_inspection(database_path, asset_id):
    inspection_id = create_inspection(
        asset_id,
        "2026-10-27T10:00:00Z",
        inspection_method="visual",
        data_origin="synthetic",
        database_path=database_path,
    )

    inspection = get_inspection(inspection_id, database_path)

    assert inspection["asset_id"] == asset_id
    assert inspection["inspection_method"] == "visual"
    assert inspection["data_origin"] == "synthetic"
    assert inspection["status"] == "completed"


def test_missing_inspection_returns_none(database_path):
    assert get_inspection(999, database_path) is None


def test_list_inspections_keeps_history(database_path, asset_id):
    create_inspection(
        asset_id,
        "2026-10-20T10:00:00Z",
        database_path=database_path,
    )
    create_inspection(
        asset_id,
        "2026-10-27T10:00:00Z",
        database_path=database_path,
    )

    inspections = list_inspections(asset_id, database_path)

    assert len(inspections) == 2
    assert inspections[0]["inspected_at"] == "2026-10-27T10:00:00Z"
    assert inspections[1]["inspected_at"] == "2026-10-20T10:00:00Z"


def test_empty_inspection_date_is_rejected(database_path, asset_id):
    with pytest.raises(ValueError):
        create_inspection(
            asset_id,
            " ",
            database_path=database_path,
        )


def test_invalid_status_is_rejected(database_path, asset_id):
    with pytest.raises(ValueError):
        create_inspection(
            asset_id,
            "2026-10-27T10:00:00Z",
            status="unknown_status",
            database_path=database_path,
        )


def test_invalid_data_origin_is_rejected(database_path, asset_id):
    with pytest.raises(ValueError):
        create_inspection(
            asset_id,
            "2026-10-27T10:00:00Z",
            data_origin="unverified",
            database_path=database_path,
        )


def test_nonexistent_asset_is_rejected(database_path):
    with pytest.raises(sqlite3.IntegrityError):
        create_inspection(
            999,
            "2026-10-27T10:00:00Z",
            database_path=database_path,
        )
