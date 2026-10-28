
"""Tests for solar asset management."""

import sqlite3

import pytest

from solarpv_db.assets import (
    create_asset,
    get_asset,
    list_assets,
    update_asset,
)
from solarpv_db.database import initialize_database


@pytest.fixture
def database_path(tmp_path):
    path = tmp_path / "test_solar.db"
    initialize_database(path)
    return path


def test_create_and_get_asset(database_path):
    asset_id = create_asset(
        "panel",
        "Panel A",
        external_id="SITE-PANEL-001",
        database_path=database_path,
    )

    asset = get_asset(asset_id, database_path)

    assert asset["name"] == "Panel A"
    assert asset["asset_type"] == "panel"
    assert asset["external_id"] == "SITE-PANEL-001"


def test_create_asset_rejects_empty_name(database_path):
    with pytest.raises(ValueError):
        create_asset("panel", "   ", database_path=database_path)


def test_duplicate_external_id_is_rejected(database_path):
    create_asset(
        "panel", "Panel A",
        external_id="SITE-001",
        database_path=database_path,
    )

    with pytest.raises(sqlite3.IntegrityError):
        create_asset(
            "panel", "Panel B",
            external_id="SITE-001",
            database_path=database_path,
        )


def test_get_missing_asset_returns_none(database_path):
    assert get_asset(999, database_path) is None


def test_list_assets_returns_all_assets(database_path):
    create_asset("panel", "Panel A", database_path=database_path)
    create_asset("inverter", "Inverter A", database_path=database_path)

    assets = list_assets(database_path)

    assert len(assets) == 2
    assert [asset["name"] for asset in assets] == [
        "Panel A",
        "Inverter A",
    ]


def test_update_asset_preserves_internal_id(database_path):
    asset_id = create_asset(
        "panel", "Panel A", database_path=database_path
    )

    updated = update_asset(
        asset_id,
        database_path,
        name="Panel A Updated",
        location="Row 1",
    )

    asset = get_asset(asset_id, database_path)

    assert updated is True
    assert asset["asset_id"] == asset_id
    assert asset["name"] == "Panel A Updated"
    assert asset["location"] == "Row 1"


def test_update_missing_asset_returns_false(database_path):
    assert update_asset(
        999, database_path, name="Not Found"
    ) is False


def test_update_rejects_empty_name(database_path):
    asset_id = create_asset(
        "panel", "Panel A", database_path=database_path
    )

    with pytest.raises(ValueError):
        update_asset(asset_id, database_path, name="   ")


def test_update_requires_at_least_one_field(database_path):
    asset_id = create_asset(
        "panel", "Panel A", database_path=database_path
    )

    with pytest.raises(ValueError):
        update_asset(asset_id, database_path)
