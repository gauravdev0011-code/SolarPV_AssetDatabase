"""Dataset API workflow tests with disposable SQLite and private storage."""

import hashlib

import pytest
from fastapi.testclient import TestClient
from concurrent.futures import ThreadPoolExecutor

from solarpv_api.app import create_app
from solarpv_api.config import Settings
from solarpv_db.database import get_connection
from solarpv_ingestion.worker import IngestionWorker


@pytest.fixture
def dataset_system(tmp_path):
    settings = Settings(
        database_path=tmp_path / "datasets.sqlite3",
        storage_path=tmp_path / "private",
        max_file_bytes=2 * 1024 * 1024,
        max_json_bytes=256 * 1024,
        lease_seconds=9,
        retry_delay_seconds=0,
        worker_poll_seconds=0.01,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        worker = IngestionWorker(settings.database_path, app.state.storage, lease_seconds=9)
        yield settings, app, client, worker


def _processed(client, worker, content=b"asset_id,temperature\nA-1,31.5\nA-2,0\n", filename="sample.csv"):
    uploaded = client.post("/api/v1/uploads", files={"file": (filename, content, "text/csv")})
    assert uploaded.status_code == 202, uploaded.text
    body = uploaded.json()
    assert worker.run_once()
    return body


def test_dataset_registration_and_subsequent_versions_are_idempotent(dataset_system):
    settings, _, client, worker = dataset_system
    first_job = _processed(client, worker)
    payload = {"name": "Plant measurements", "job_id": first_job["job_id"], "idempotency_key": "create-1"}
    created = client.post("/api/v1/datasets", json=payload)
    assert created.status_code == 201, created.text
    first = created.json()
    assert first["version_number"] == 1
    assert first["source_sha256"] == first_job["source"]["sha256"]
    assert first["row_count"] == 2
    replay = client.post("/api/v1/datasets", json=payload)
    assert replay.json()["dataset_version_id"] == first["dataset_version_id"]

    with get_connection(settings.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM datasets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM dataset_versions").fetchone()[0] == 1
    second_job = _processed(client, worker, b"asset_id,temperature\nA-1,32\n")
    second_payload = {"job_id": second_job["job_id"], "idempotency_key": "version-2"}
    second_response = client.post(
        f"/api/v1/datasets/{first['dataset_id']}/versions", json=second_payload
    )
    assert second_response.status_code == 201, second_response.text
    second = second_response.json()
    assert second["version_number"] == 2
    assert client.post(
        f"/api/v1/datasets/{first['dataset_id']}/versions", json=second_payload
    ).json()["dataset_version_id"] == second["dataset_version_id"]
    conflict = client.post("/api/v1/datasets", json=payload | {"name": "Changed name"})
    assert conflict.status_code == 409


def test_failed_processing_job_cannot_be_registered(dataset_system):
    _, _, client, worker = dataset_system
    uploaded = client.post("/api/v1/uploads", files={
        "file": ("invalid.csv", b"a,b\n1\n", "text/csv")
    }).json()
    assert worker.run_once()
    response = client.post("/api/v1/datasets", json={
        "name": "Bad", "job_id": uploaded["job_id"], "idempotency_key": "failed-job"
    })
    assert response.status_code == 422


def test_mapping_validation_approval_and_domain_separation(dataset_system):
    settings, _, client, worker = dataset_system
    job = _processed(client, worker)
    dataset = client.post("/api/v1/datasets", json={
        "name": "Temperature readings", "job_id": job["job_id"], "idempotency_key": "dataset"
    }).json()
    base = f"/api/v1/datasets/{dataset['dataset_id']}/versions/{dataset['dataset_version_id']}"
    preview = client.get(f"{base}/preview?limit=1")
    assert preview.status_code == 200
    assert preview.json()["returned_rows"] == 1
    assert preview.json()["rows"] == [{"asset_id": "A-1", "temperature": "31.5"}]
    profile = client.get(f"{base}/profile").json()
    assert profile["source_sha256"] == hashlib.sha256(b"asset_id,temperature\nA-1,31.5\nA-2,0\n").hexdigest()

    mapping = client.put(f"{base}/mapping", json={
        "idempotency_key": "mapping-1",
        "columns": [
            {"source_column": "asset_id", "target_field": "asset_id", "expected_type": "string", "is_required": True},
            {"source_column": "temperature", "target_field": "temperature", "expected_type": "number", "is_required": True, "measurement_unit": "C"},
        ],
    })
    assert mapping.status_code == 201, mapping.text
    validated = client.post(f"{base}/validate", json={"idempotency_key": "validate-1"})
    assert validated.status_code == 201, validated.text
    run = validated.json()
    assert run["outcome"] == "passed"
    assert run["error_count"] == 0
    assert run["source_sha256"] == profile["source_sha256"]
    assert run["mapping_revision_id"] == mapping.json()["mapping_revision_id"]
    assert len(run["result_checksum"]) == 64
    fetched = client.get(f"/api/v1/validation-runs/{run['validation_run_id']}")
    assert fetched.status_code == 200
    assert fetched.json()["result_checksum"] == run["result_checksum"]
    approval = client.post(f"{base}/approvals", json={
        "validation_run_id": run["validation_run_id"], "idempotency_key": "review-1"
    })
    assert approval.status_code == 201, approval.text
    approved = client.post(f"/api/v1/approvals/{approval.json()['approval_id']}/decision", json={
        "decision": "approved", "reviewer_identity": "local-operator"
    })
    assert approved.status_code == 200, approved.text
    assert approved.json()["state"] == "approved"
    replayed_approval = client.post(
        f"/api/v1/approvals/{approval.json()['approval_id']}/decision", json={
            "decision": "approved", "reviewer_identity": "local-operator"
        }
    )
    assert replayed_approval.status_code == 200
    assert replayed_approval.json()["decision_at"] == approved.json()["decision_at"]

    with get_connection(settings.database_path) as connection:
        for table in ("assets", "inspections", "observations", "maintenance_records"):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


def test_validation_errors_block_approval_and_stale_mapping_is_rejected(dataset_system):
    _, _, client, worker = dataset_system
    job = _processed(client, worker, b"asset_id,temperature\nA-1,not-a-number\n")
    dataset = client.post("/api/v1/datasets", json={
        "name": "Invalid readings", "job_id": job["job_id"], "idempotency_key": "invalid-dataset"
    }).json()
    base = f"/api/v1/datasets/{dataset['dataset_id']}/versions/{dataset['dataset_version_id']}"
    client.put(f"{base}/mapping", json={
        "idempotency_key": "invalid-map",
        "columns": [{"source_column": "temperature", "target_field": "temperature", "expected_type": "number", "is_required": True}],
    })
    result = client.post(f"{base}/validate", json={"idempotency_key": "invalid-validation"})
    assert result.status_code == 201
    assert result.json()["outcome"] == "failed"
    assert result.json()["errors"][0]["code"] == "invalid_number"
    assert client.post(f"{base}/approvals", json={
        "validation_run_id": result.json()["validation_run_id"], "idempotency_key": "must-not-approve"
    }).status_code == 409
    assert client.put(f"{base}/mapping", json={
        "idempotency_key": "late-map", "columns": []
    }).status_code == 409


def test_version_creation_preserves_approved_version(dataset_system):
    _, _, client, worker = dataset_system
    first_job = _processed(client, worker, b"x\n1\n")
    first = client.post("/api/v1/datasets", json={
        "name": "Simple data", "job_id": first_job["job_id"], "idempotency_key": "approved-dataset"
    }).json()
    base = f"/api/v1/datasets/{first['dataset_id']}/versions/{first['dataset_version_id']}"
    result = client.post(f"{base}/validate", json={"idempotency_key": "no-map-warning"}).json()
    assert result["outcome"] == "warnings"
    approval = client.post(f"{base}/approvals", json={
        "validation_run_id": result["validation_run_id"], "idempotency_key": "approval"
    }).json()
    assert client.post(f"/api/v1/approvals/{approval['approval_id']}/decision", json={
        "decision": "approved", "reviewer_identity": "operator"
    }).status_code == 200

    second_job = _processed(client, worker, b"x\n2\n")
    second = client.post(f"/api/v1/datasets/{first['dataset_id']}/versions", json={
        "job_id": second_job["job_id"], "idempotency_key": "new-version"
    }).json()
    dataset = client.get(f"/api/v1/datasets/{first['dataset_id']}").json()
    assert second["version_number"] == 2
    assert dataset["approved_version_id"] == first["dataset_version_id"]
    assert dataset["versions"][0]["state"] == "draft"


def test_concurrent_version_requests_allocate_distinct_ordered_numbers(dataset_system):
    _, _, client, worker = dataset_system
    first_job = _processed(client, worker, b"v\n0\n")
    dataset = client.post("/api/v1/datasets", json={
        "name": "Concurrent versions", "job_id": first_job["job_id"], "idempotency_key": "concurrent-base"
    }).json()
    jobs = [_processed(client, worker, f"v\n{i}\n".encode())["job_id"] for i in (1, 2)]

    def add(index):
        return client.post(f"/api/v1/datasets/{dataset['dataset_id']}/versions", json={
            "job_id": jobs[index], "idempotency_key": f"concurrent-{index}"
        })

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(add, (0, 1)))
    assert all(response.status_code == 201 for response in responses)
    assert sorted(response.json()["version_number"] for response in responses) == [2, 3]


def test_duplicate_mapping_fields_are_rejected_and_validation_rollback_cleans_artifact(dataset_system):
    settings, app, client, worker = dataset_system
    job = _processed(client, worker, b"id,value\nA,not-number\n")
    dataset = client.post("/api/v1/datasets", json={
        "name": "Rollback dataset", "job_id": job["job_id"], "idempotency_key": "rollback-dataset"
    }).json()
    base = f"/api/v1/datasets/{dataset['dataset_id']}/versions/{dataset['dataset_version_id']}"
    duplicate_target = client.put(f"{base}/mapping", json={
        "idempotency_key": "duplicate-target",
        "columns": [
            {"source_column": "id", "target_field": "same", "expected_type": "string"},
            {"source_column": "value", "target_field": "same", "expected_type": "string"},
        ],
    })
    assert duplicate_target.status_code == 422

    with get_connection(settings.database_path) as connection:
        connection.execute(
            f"""CREATE TRIGGER fail_dataset_validation BEFORE INSERT ON validation_runs
                WHEN NEW.dataset_version_id = '{dataset['dataset_version_id']}'
                BEGIN SELECT RAISE(ABORT, 'injected validation failure'); END"""
        )
    response = client.post(f"{base}/validate", json={"idempotency_key": "rollback-validation"})
    assert response.status_code == 409
    version = client.get(base).json()
    assert version["state"] == "draft"
    with get_connection(settings.database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM validation_runs WHERE dataset_version_id = ?",
            (dataset["dataset_version_id"],),
        ).fetchone()[0] == 0
    artifact_dir = app.state.storage._root / "dataset-validations"
    assert not artifact_dir.exists() or not list(artifact_dir.iterdir())


def test_mapping_and_validation_replay_reject_changed_request_identity(dataset_system):
    _, _, client, worker = dataset_system
    job = _processed(client, worker)
    dataset = client.post("/api/v1/datasets", json={
        "name": "Replay dataset", "job_id": job["job_id"], "idempotency_key": "replay-dataset"
    }).json()
    base = f"/api/v1/datasets/{dataset['dataset_id']}/versions/{dataset['dataset_version_id']}"
    mapping_payload = {"idempotency_key": "replay-map", "columns": []}
    first_mapping = client.put(f"{base}/mapping", json=mapping_payload).json()
    replay = client.put(f"{base}/mapping", json=mapping_payload).json()
    assert replay["mapping_revision_id"] == first_mapping["mapping_revision_id"]
    changed = client.put(f"{base}/mapping", json={
        "idempotency_key": "replay-map", "columns": [{
            "source_column": "asset_id", "target_field": "id", "expected_type": "string"
        }],
    })
    assert changed.status_code == 409
    first_run = client.post(f"{base}/validate", json={"idempotency_key": "replay-validation"}).json()
    second_run = client.post(f"{base}/validate", json={"idempotency_key": "replay-validation"}).json()
    assert second_run["validation_run_id"] == first_run["validation_run_id"]
    assert second_run["result_checksum"] == first_run["result_checksum"]
