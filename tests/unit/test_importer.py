
"""Tests for JSON and CSV importing."""

import csv
import json
import sqlite3

import pytest

from solarpv_db.assets import create_asset, list_assets
from solarpv_db.database import get_connection, initialize_database
from solarpv_db.importer import import_data


@pytest.fixture
def database_path(tmp_path):
    """Create a new database for each test."""
    path = tmp_path / "test_solarpv.db"
    initialize_database(path)
    return path


@pytest.fixture
def sample_data():
    """Provide a valid dataset containing every record type."""
    return {
        "assets": [
            {
                "external_id": "PV-001",
                "asset_type": "panel",
                "name": "Test Solar Panel",
                "description": "Synthetic test panel",
                "location": "Test Site A",
            }
        ],
        "inspections": [
            {
                "inspection_key": "INSP-001",
                "asset_external_id": "PV-001",
                "inspected_at": "2026-10-01T09:00:00Z",
                "inspection_method": "visual",
                "data_source": "importer_test",
                "data_origin": "synthetic",
                "status": "completed",
                "notes": "Synthetic test inspection",
            }
        ],
        "observations": [
            {
                "inspection_key": "INSP-001",
                "observation_type": "surface_temperature",
                "value": 42.5,
                "unit": "C",
                "finding": None,
                "data_quality": "good",
                "source_reference": "test-reading-001",
            }
        ],
        "maintenance": [
            {
                "asset_external_id": "PV-001",
                "performed_at": "2026-10-02T10:00:00Z",
                "maintenance_type": "cleaning",
                "description": "Synthetic test cleaning record",
                "performed_by": "Test Technician",
                "data_origin": "synthetic",
            }
        ],
    }


def write_json(path, data):
    """Write data to a JSON file."""
    path.write_text(
        json.dumps(data, indent=2),
        encoding="utf-8",
    )
    return path


def test_import_json_creates_all_record_types(
    tmp_path, database_path, sample_data
):
    """A valid JSON import creates every expected record type."""
    source = write_json(tmp_path / "sample.json", sample_data)

    result = import_data(source, database_path)

    # Check the expected fields individually. The result may include
    # additional useful fields such as source_name and message.
    assert result["status"] == "imported"
    assert result["assets"] == 1
    assert result["inspections"] == 1
    assert result["observations"] == 1
    assert result["maintenance"] == 1

    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM assets"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM inspections"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM maintenance_records"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM imported_files"
        ).fetchone()[0] == 1


def test_identical_import_content_is_deduplicated_within_each_workspace_only(
    tmp_path, database_path, sample_data
):
    source = write_json(tmp_path / "shared.json", sample_data)
    with get_connection(database_path) as connection:
        connection.execute(
            "INSERT INTO workspaces(workspace_id,name) VALUES('workspace-b','Workspace B')"
        )

    first = import_data(source, database_path, workspace_id="default")
    replay = import_data(source, database_path, workspace_id="default")
    second = import_data(source, database_path, workspace_id="workspace-b")

    assert first["status"] == "imported"
    assert replay["status"] == "skipped"
    assert second["status"] == "imported"
    assert len(list_assets(database_path, workspace_id="default")) == 1
    assert len(list_assets(database_path, workspace_id="workspace-b")) == 1
    with get_connection(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM imported_files").fetchone()[0] == 2


def test_reimporting_identical_json_is_skipped(
    tmp_path, database_path, sample_data
):
    """Importing identical source content twice must not duplicate records."""
    source = write_json(tmp_path / "sample.json", sample_data)

    first_result = import_data(source, database_path)
    second_result = import_data(source, database_path)

    assert first_result["status"] == "imported"
    assert second_result["status"] == "skipped"

    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM assets"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM inspections"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()[0] == 1

        assert connection.execute(
            "SELECT COUNT(*) FROM maintenance_records"
        ).fetchone()[0] == 1


def test_invalid_asset_reference_is_rejected(
    tmp_path, database_path, sample_data
):
    """Reject inspections that reference an asset absent from the dataset."""
    sample_data["inspections"][0]["asset_external_id"] = "PV-MISSING"
    source = write_json(tmp_path / "invalid_reference.json", sample_data)

    with pytest.raises(ValueError):
        import_data(source, database_path)

    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM assets"
        ).fetchone()[0] == 0

        assert connection.execute(
            "SELECT COUNT(*) FROM inspections"
        ).fetchone()[0] == 0


def test_non_finite_measurement_is_rejected(
    tmp_path, database_path, sample_data
):
    """Reject NaN measurements."""
    sample_data["observations"][0]["value"] = float("nan")
    source = write_json(tmp_path / "invalid_measurement.json", sample_data)

    with pytest.raises(ValueError):
        import_data(source, database_path)

    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM assets"
        ).fetchone()[0] == 0

        assert connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()[0] == 0


def test_failed_database_insert_rolls_back_import(
    tmp_path, database_path, sample_data
):
    """Roll back earlier inserts when a later insert violates a constraint."""
    # This record already exists in the database.
    create_asset(
        "panel",
        "Existing Panel",
        external_id="PV-001",
        database_path=database_path,
    )

    # The importer inserts PV-NEW first, then encounters the duplicate PV-001.
    # The failed import must roll back the PV-NEW insert as well.
    sample_data["assets"].insert(
        0,
        {
            "external_id": "PV-NEW",
            "asset_type": "panel",
            "name": "New Panel",
            "description": "Must be rolled back",
            "location": "Test Site B",
        },
    )

    source = write_json(tmp_path / "conflict.json", sample_data)

    with pytest.raises(sqlite3.IntegrityError):
        import_data(source, database_path)

    with get_connection(database_path) as connection:
        assets = connection.execute(
            "SELECT external_id FROM assets ORDER BY external_id"
        ).fetchall()

        # Only the original PV-001 must remain.
        assert [row["external_id"] for row in assets] == ["PV-001"]

        assert connection.execute(
            "SELECT COUNT(*) FROM inspections"
        ).fetchone()[0] == 0

        assert connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()[0] == 0

        assert connection.execute(
            "SELECT COUNT(*) FROM maintenance_records"
        ).fetchone()[0] == 0

        assert connection.execute(
            "SELECT COUNT(*) FROM imported_files"
        ).fetchone()[0] == 0


def test_import_csv_folder(tmp_path, database_path, sample_data):
    """Import a dataset from the four required CSV files."""
    source_dir = tmp_path / "csv_dataset"
    source_dir.mkdir()

    csv_sections = {
        "assets.csv": (
            sample_data["assets"],
            [
                "external_id",
                "asset_type",
                "name",
                "description",
                "location",
            ],
        ),
        "inspections.csv": (
            sample_data["inspections"],
            [
                "inspection_key",
                "asset_external_id",
                "inspected_at",
                "inspection_method",
                "data_source",
                "data_origin",
                "status",
                "notes",
            ],
        ),
        "observations.csv": (
            sample_data["observations"],
            [
                "inspection_key",
                "observation_type",
                "value",
                "unit",
                "finding",
                "data_quality",
                "source_reference",
            ],
        ),
        "maintenance.csv": (
            sample_data["maintenance"],
            [
                "asset_external_id",
                "performed_at",
                "maintenance_type",
                "description",
                "performed_by",
                "data_origin",
            ],
        ),
    }

    for filename, (records, fieldnames) in csv_sections.items():
        with (source_dir / filename).open(
            "w",
            newline="",
            encoding="utf-8",
        ) as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

    result = import_data(source_dir, database_path)

    assert result["status"] == "imported"
    assert result["assets"] == 1
    assert result["inspections"] == 1
    assert result["observations"] == 1
    assert result["maintenance"] == 1

    assets = list_assets(database_path=database_path)
    assert len(assets) == 1
    assert assets[0]["external_id"] == "PV-001"
