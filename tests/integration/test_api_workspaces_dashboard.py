"""Workspace isolation and live dashboard API acceptance tests."""

import sqlite3

import pytest
from fastapi.testclient import TestClient

from solarpv_api.app import create_app
from solarpv_api.config import Settings
from solarpv_db.assets import create_asset
from solarpv_db.database import get_connection
from solarpv_db.inspections import create_inspection
from solarpv_db.maintenance import create_maintenance_record
from solarpv_db.observations import create_observation
from solarpv_ingestion.worker import IngestionWorker


@pytest.fixture
def workspace_system(tmp_path):
    settings = Settings(
        database_path=tmp_path / "workspace.sqlite3",
        storage_path=tmp_path / "private",
        max_file_bytes=1024 * 1024,
        max_json_bytes=128 * 1024,
        lease_seconds=9,
        retry_delay_seconds=0,
        worker_poll_seconds=0.01,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        worker = IngestionWorker(settings.database_path, app.state.storage, lease_seconds=9)
        yield settings, app, client, worker


def _workspace(client, name):
    response = client.post("/api/v1/workspaces", json={"name": name})
    assert response.status_code == 201, response.text
    return response.json()["workspace_id"]


def test_same_external_asset_id_is_unique_per_workspace_and_domain_reads_are_isolated(workspace_system):
    settings, _, client, _ = workspace_system
    first, second = _workspace(client, "North"), _workspace(client, "South")
    site_response = client.post(f"/api/v1/workspaces/{first}/sites", json={
        "name": "North field", "location_text": "Recorded site label",
    })
    assert site_response.status_code == 201, site_response.text
    site_id = site_response.json()["site_id"]

    first_asset = create_asset("inverter", "Inverter North", "INV-01",
                               database_path=settings.database_path,
                               workspace_id=first, site_id=site_id, data_origin="real")
    second_asset = create_asset("inverter", "Inverter South", "INV-01",
                                database_path=settings.database_path,
                                workspace_id=second, data_origin="real")
    create_asset("panel", "Illustrative example", "DEMO-1",
                 database_path=settings.database_path, workspace_id=first,
                 data_origin="synthetic")
    assert first_asset != second_asset
    with pytest.raises(sqlite3.IntegrityError):
        create_asset("module", "Duplicate North", "INV-01",
                     database_path=settings.database_path, workspace_id=first)

    inspection_id = create_inspection(
        first_asset, "2026-10-01T12:00:00Z", data_source="operator log",
        database_path=settings.database_path, workspace_id=first,
    )
    observation_id = create_observation(
        inspection_id, "module_temperature", 0, "C", data_quality="good",
        source_reference="sheet-1/row-2", database_path=settings.database_path,
        workspace_id=first,
    )
    maintenance_id = create_maintenance_record(
        first_asset, "2026-10-02", "inspection", "Recorded maintenance",
        database_path=settings.database_path, workspace_id=first,
    )
    assert observation_id > 0 and maintenance_id > 0

    assert client.get(f"/api/v1/workspaces/{first}/assets/{first_asset}").status_code == 200
    assert client.get(f"/api/v1/workspaces/{second}/assets/{first_asset}").status_code == 404
    assert client.get(f"/api/v1/workspaces/{first}/sites/{site_id}").status_code == 200
    assert client.get(f"/api/v1/workspaces/{second}/sites/{site_id}").status_code == 404
    assert client.put(f"/api/v1/workspaces/{second}/assets/{second_asset}/site",
                      json={"site_id": site_id}).status_code == 404
    assert client.get(f"/api/v1/workspaces/{second}/inspections/{inspection_id}").status_code == 404
    assert client.get(f"/api/v1/workspaces/{second}/maintenance").json()["total"] == 0

    first_overview = client.get(f"/api/v1/workspaces/{first}/dashboard/overview").json()
    second_overview = client.get(f"/api/v1/workspaces/{second}/dashboard/overview").json()
    assert first_overview["metrics"]["total_assets"]["value"] == 1
    assert first_overview["metrics"]["inspection_count"]["value"] == 1
    assert first_overview["metrics"]["observation_count"]["value"] == 1
    assert first_overview["metrics"]["inspection_coverage"]["value"] == 1
    assert first_overview["metrics"]["synthetic_records_excluded"]["value"] == 1
    assert client.get(f"/api/v1/workspaces/{first}/assets/{first_asset}").json()["latitude"] is None
    assert second_overview["metrics"]["total_assets"]["value"] == 1
    assert client.get(f"/api/v1/workspaces/{first}/assets/{first_asset}/trends?metric=module_temperature").json()["items"][0]["value"] == 0

    with pytest.raises(sqlite3.IntegrityError):
        create_inspection(second_asset, "2026-10-01", database_path=settings.database_path,
                          workspace_id=first)


def test_workspace_scopes_upload_jobs_datasets_and_unknown_workspace(workspace_system):
    settings, _, client, worker = workspace_system
    first, second = _workspace(client, "Alpha"), _workspace(client, "Beta")
    upload = client.post(f"/api/v1/workspaces/{first}/uploads", files={
        "file": ("observations.csv", b"asset,value\nA-1,12\n", "text/csv")
    })
    assert upload.status_code == 202, upload.text
    details = upload.json()
    assert details["source"]["workspace_id"] == first
    assert "storage" not in str(details).lower()
    assert client.get(f"/api/v1/workspaces/{second}/uploads/{details['source']['source_file_id']}").status_code == 404
    assert client.get(f"/api/v1/workspaces/{second}/jobs/{details['job_id']}").status_code == 404
    assert client.get(f"/api/v1/workspaces/{second}/jobs/{details['job_id']}/result").status_code == 404
    assert client.post(f"/api/v1/workspaces/{second}/jobs/{details['job_id']}/cancel").status_code == 404
    # The old unscoped surface remains fixed to the default workspace even if
    # a caller supplies a forged workspace_id query parameter.
    assert client.get(f"/api/v1/jobs/{details['job_id']}?workspace_id={first}").status_code == 404
    assert client.get(f"/api/v1/datasets?workspace_id={first}").json()["total"] == 0

    dataset = client.post(f"/api/v1/workspaces/{second}/datasets", json={
        "name": "Foreign job", "job_id": details["job_id"], "idempotency_key": "foreign-job"
    })
    assert dataset.status_code == 422
    assert client.post("/api/v1/workspaces/does-not-exist/uploads", files={
        "file": ("x.csv", b"a\n1\n", "text/csv")
    }).status_code == 404
    oversized = client.post(f"/api/v1/workspaces/{first}/uploads", files={
        "file": ("oversized.csv", b"x" * (3 * 1024 * 1024), "text/csv")
    })
    assert oversized.status_code == 413

    second_upload = client.post(f"/api/v1/workspaces/{second}/uploads", files={
        "file": ("observations.csv", b"asset,value\nA-1,12\n", "text/csv")
    })
    assert second_upload.status_code == 202, second_upload.text
    assert second_upload.json()["source"]["sha256"] == details["source"]["sha256"]
    assert second_upload.json()["source"]["source_file_id"] != details["source"]["source_file_id"]

    assert worker.run_once()
    assert worker.run_once()
    registered = client.post(f"/api/v1/workspaces/{first}/datasets", json={
        "name": "Alpha observations", "job_id": details["job_id"], "idempotency_key": "alpha-dataset"
    })
    assert registered.status_code == 201, registered.text
    dataset_id = registered.json()["dataset_id"]
    version_id = registered.json()["dataset_version_id"]
    assert client.get(f"/api/v1/workspaces/{second}/datasets/{dataset_id}").status_code == 404
    assert client.get(
        f"/api/v1/workspaces/{second}/datasets/{dataset_id}/versions/{version_id}"
    ).status_code == 404
    assert client.get(f"/api/v1/workspaces/{first}/datasets/{dataset_id}").status_code == 200
    next_version = client.post(f"/api/v1/workspaces/{first}/datasets/{dataset_id}/versions", json={
        "job_id": details["job_id"], "idempotency_key": "alpha-version-2",
    })
    assert next_version.status_code == 201, next_version.text
    assert next_version.json()["version_number"] == 2
    assert client.get(
        f"/api/v1/workspaces/{second}/datasets/{dataset_id}/versions/{next_version.json()['dataset_version_id']}"
    ).status_code == 404
    assert client.post(f"/api/v1/workspaces/{first}/datasets/{dataset_id}/versions", json={
        "job_id": second_upload.json()["job_id"], "idempotency_key": "foreign-version-job",
    }).status_code == 422
    version_base = f"/api/v1/workspaces/{first}/datasets/{dataset_id}/versions/{version_id}"
    mapping = client.put(f"{version_base}/mapping", json={
        "idempotency_key": "alpha-map",
        "columns": [{"source_column": "asset", "target_field": "asset_id",
                     "expected_type": "string", "is_required": True},
                    {"source_column": "value", "target_field": "temperature",
                     "expected_type": "number", "is_required": True}],
    })
    assert mapping.status_code == 201, mapping.text
    validation = client.post(f"{version_base}/validate", json={"idempotency_key": "alpha-check"})
    assert validation.status_code == 201, validation.text
    approval = client.post(f"{version_base}/approvals", json={
        "validation_run_id": validation.json()["validation_run_id"],
        "idempotency_key": "alpha-review",
    })
    assert approval.status_code == 201, approval.text
    approval_id = approval.json()["approval_id"]
    assert client.get(f"/api/v1/workspaces/{second}/approvals/{approval_id}").status_code == 404
    assert client.post(f"/api/v1/workspaces/{second}/approvals/{approval_id}/decision", json={
        "decision": "approved", "reviewer_identity": "operator",
    }).status_code == 404
    decided = client.post(f"/api/v1/workspaces/{first}/approvals/{approval_id}/decision", json={
        "decision": "approved", "reviewer_identity": "operator",
    })
    assert decided.status_code == 200, decided.text
    with get_connection(settings.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM inspections").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM maintenance_records").fetchone()[0] == 0


def test_empty_dashboard_marks_unsupported_metrics_unavailable_and_preserves_default_api(workspace_system):
    _, _, client, _ = workspace_system
    response = client.get("/api/v1/workspaces/default/dashboard/overview")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["data_mode"] == "live"
    assert body["metrics"]["total_assets"]["value"] == 0
    assert body["metrics"]["inspection_coverage"]["value"] is None
    assert body["metrics"]["condition_distribution"]["available"] is False
    assert body["metrics"]["open_findings"]["available"] is False
    assert body["metrics"]["priority_actions"]["available"] is False
    assert client.get("/api/v1/datasets").status_code == 200
    assert client.get("/api/v1/uploads/not-a-source").status_code == 404
