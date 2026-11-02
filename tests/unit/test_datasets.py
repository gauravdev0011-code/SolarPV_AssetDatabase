"""Unit tests for bounded dataset previews and deterministic validation."""

import pytest
import sqlite3

from solarpv_db import database
from solarpv_db.database import initialize_database
from solarpv_ingestion.storage import LocalArtifactStorage
from solarpv_ingestion.validation import (
    MAX_ERROR_DETAILS,
    InvalidInputError,
    preview_records,
    validate_records,
)


def _stored(tmp_path, name, payload):
    storage = LocalArtifactStorage(tmp_path / "private")
    key = f"originals/{name}"
    storage.put_bytes(key, payload)
    return storage, key


def test_csv_validation_counts_errors_and_bounds_error_details(tmp_path):
    payload = "temperature\n" + "bad\n" * (MAX_ERROR_DETAILS + 5)
    storage, key = _stored(tmp_path, "invalid.csv", payload.encode())
    result = validate_records(
        storage, key, "invalid.csv",
        [{"source_column": "temperature", "target_field": "temperature",
          "expected_type": "number", "is_required": True}],
        max_json_bytes=1024,
    )
    assert result["outcome"] == "failed"
    assert result["row_count"] == MAX_ERROR_DETAILS + 5
    assert result["error_count"] == MAX_ERROR_DETAILS + 5
    assert len(result["errors"]) == MAX_ERROR_DETAILS
    assert result["error_details_truncated"] is True


def test_preview_is_bounded_and_reports_source_headers(tmp_path):
    storage, key = _stored(tmp_path, "preview.csv", b"id,value\n1,a\n2,b\n3,c\n")
    result = preview_records(storage, key, "preview.csv", limit=2, max_json_bytes=100)
    assert result == {
        "columns": ["id", "value"],
        "rows": [{"id": "1", "value": "a"}, {"id": "2", "value": "b"}],
        "returned_rows": 2,
        "limit": 2,
    }
    with pytest.raises(ValueError, match="between 1 and 100"):
        preview_records(storage, key, "preview.csv", limit=101, max_json_bytes=100)


def test_json_records_validate_and_oversized_json_is_rejected(tmp_path):
    storage, key = _stored(tmp_path, "records.json", b'[{"id":1},{"id":2}]')
    result = validate_records(
        storage, key, "records.json",
        [{"source_column": "id", "target_field": "asset_id",
          "expected_type": "integer", "is_required": True}],
        max_json_bytes=100,
    )
    assert result["outcome"] == "passed"
    assert result["row_count"] == 2
    with pytest.raises(InvalidInputError, match="byte validation limit"):
        validate_records(storage, key, "records.json", [], max_json_bytes=5)


def test_json_non_object_records_and_malformed_csv_fail_deterministically(tmp_path):
    json_storage, json_key = _stored(tmp_path, "scalars.json", b"[1,2]")
    with pytest.raises(InvalidInputError, match="records must be objects"):
        preview_records(json_storage, json_key, "scalars.json", limit=2, max_json_bytes=20)

    csv_storage, csv_key = _stored(tmp_path, "bad.csv", b"a,b\n1\n")
    result = validate_records(csv_storage, csv_key, "bad.csv", [], max_json_bytes=100)
    assert result["outcome"] == "failed"
    assert result["errors"][0]["code"] == "malformed_dataset"


def test_migration_009_upgrades_populated_version_008_database_without_data_loss(tmp_path):
    path = tmp_path / "populated-v8.sqlite3"
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    migrations = database._get_migrations()
    for version, name, checksum, sql in migrations[:8]:
        connection.execute("BEGIN IMMEDIATE")
        database._execute_sql_script(connection, sql)
        if version == 1:
            connection.execute(
                """CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL,
                    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"""
            )
        connection.execute(
            "INSERT INTO schema_migrations(version, name, checksum) VALUES (?, ?, ?)",
            (version, name, checksum),
        )
        connection.execute(f"PRAGMA user_version = {version}")
        connection.commit()
    connection.execute(
        "INSERT INTO assets(asset_id, external_id, asset_type, name) VALUES (71, 'LEGACY-71', 'inverter', 'Legacy inverter')"
    )
    connection.execute(
        "INSERT INTO datasets(dataset_id, workspace_id, name) VALUES ('legacy-dataset', 'default', 'Legacy dataset')"
    )
    connection.execute(
        """INSERT INTO dataset_versions(dataset_version_id, dataset_id, version_number, state)
           VALUES ('legacy-version', 'legacy-dataset', 1, 'draft')"""
    )
    connection.commit()
    connection.close()

    initialize_database(path)
    with sqlite3.connect(path) as upgraded:
        upgraded.row_factory = sqlite3.Row
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 9
        assert tuple(upgraded.execute("SELECT external_id, name FROM assets WHERE asset_id = 71").fetchone()) == (
            "LEGACY-71", "Legacy inverter"
        )
        assert tuple(upgraded.execute(
            "SELECT dataset_id, version_number, state FROM dataset_versions WHERE dataset_version_id = 'legacy-version'"
        ).fetchone()) == ("legacy-dataset", 1, "draft")
        assert upgraded.execute("SELECT COUNT(*) FROM dataset_mapping_revisions").fetchone()[0] == 0
        assert upgraded.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 9
        assert upgraded.execute("PRAGMA foreign_key_check").fetchall() == []
