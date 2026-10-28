
"""Tests for observation management."""

import sqlite3

import pytest

from solarpv_db.assets import create_asset
from solarpv_db.database import initialize_database
from solarpv_db.inspections import create_inspection
from solarpv_db.observations import (
    create_observation,
    get_observation,
    list_observations,
)


@pytest.fixture
def database_path(tmp_path):
    path = tmp_path / "test_solar.db"
    initialize_database(path)
    return path


@pytest.fixture
def inspection_id(database_path):
    asset_id = create_asset(
        "panel",
        "Test Panel",
        database_path=database_path,
    )
    return create_inspection(
        asset_id,
        "2026-10-27T10:00:00Z",
        database_path=database_path,
    )


def test_create_and_get_measurement(inspection_id, database_path):
    observation_id = create_observation(
        inspection_id=inspection_id,
        observation_type="surface_temperature",
        value=48.5,
        unit="°C",
        data_quality="good",
        database_path=database_path,
    )

    observation = get_observation(observation_id, database_path)

    assert observation is not None
    assert observation["observation_type"] == "surface_temperature"
    assert observation["value"] == 48.5
    assert observation["unit"] == "°C"
    assert observation["data_quality"] == "good"


def test_create_finding_without_measurement(inspection_id, database_path):
    observation_id = create_observation(
        inspection_id=inspection_id,
        observation_type="visual_inspection",
        finding="Visible discoloration",
        database_path=database_path,
    )

    observation = get_observation(observation_id, database_path)

    assert observation["value"] is None
    assert observation["finding"] == "Visible discoloration"


def test_missing_observation_returns_none(database_path):
    assert get_observation(999, database_path) is None


def test_list_observations(inspection_id, database_path):
    create_observation(
        inspection_id=inspection_id,
        observation_type="surface_temperature",
        value=48.5,
        unit="°C",
        database_path=database_path,
    )
    create_observation(
        inspection_id=inspection_id,
        observation_type="voltage",
        value=32.1,
        unit="V",
        database_path=database_path,
    )

    observations = list_observations(inspection_id, database_path)

    assert len(observations) == 2
    assert observations[0]["observation_type"] == "surface_temperature"
    assert observations[1]["observation_type"] == "voltage"


def test_empty_observation_type_is_rejected(inspection_id, database_path):
    with pytest.raises(ValueError, match="Observation type"):
        create_observation(
            inspection_id=inspection_id,
            observation_type=" ",
            value=10.0,
            database_path=database_path,
        )


def test_observation_requires_value_or_finding(inspection_id, database_path):
    with pytest.raises(ValueError, match="measurement or a finding"):
        create_observation(
            inspection_id=inspection_id,
            observation_type="visual_inspection",
            database_path=database_path,
        )


def test_invalid_data_quality_is_rejected(inspection_id, database_path):
    with pytest.raises(ValueError, match="Invalid data quality"):
        create_observation(
            inspection_id=inspection_id,
            observation_type="voltage",
            value=10.0,
            data_quality="excellent",
            database_path=database_path,
        )


def test_nonexistent_inspection_is_rejected(database_path):
    with pytest.raises(sqlite3.IntegrityError):
        create_observation(
            inspection_id=999,
            observation_type="voltage",
            value=10.0,
            database_path=database_path,
        )


@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), float("-inf")],
)
def test_non_finite_measurement_is_rejected(
    inspection_id,
    database_path,
    value,
):
    with pytest.raises(ValueError, match="finite number"):
        create_observation(
            inspection_id=inspection_id,
            observation_type="voltage",
            value=value,
            database_path=database_path,
        )
