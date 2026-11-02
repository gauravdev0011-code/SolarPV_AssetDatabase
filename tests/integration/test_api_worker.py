"""End-to-end API and durable worker tests using disposable local resources."""

from datetime import datetime, timedelta, timezone
import hashlib
import sqlite3

import pytest
from fastapi.testclient import TestClient

from solarpv_api.app import create_app
from solarpv_api.config import Settings
from solarpv_db.database import DEFAULT_DATABASE, get_connection
from solarpv_ingestion.jobs import JobQueue
from solarpv_ingestion.worker import IngestionWorker


@pytest.fixture
def system(tmp_path):
    settings = Settings(
        database_path=tmp_path / "isolated.sqlite3",
        storage_path=tmp_path / "private",
        max_file_bytes=2 * 1024 * 1024,
        max_json_bytes=256 * 1024,
        lease_seconds=9,
        retry_delay_seconds=0,
        worker_poll_seconds=0.01,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        worker = IngestionWorker(
            settings.database_path, app.state.storage, lease_seconds=settings.lease_seconds,
            max_json_bytes=settings.max_json_bytes, retry_delay_seconds=0,
        )
        yield settings, app, client, worker


def _upload(client, name="plant.csv", content=b"asset_id,temperature\nA-1,31.5\n"):
    return client.post("/api/v1/uploads", files={"file": (name, content, "application/octet-stream")})


def test_default_local_configuration_does_not_target_project_database(monkeypatch):
    monkeypatch.delenv("SOLARPV_DB_PATH", raising=False)
    monkeypatch.delenv("SOLARPV_STORAGE_PATH", raising=False)
    settings = Settings.from_environment()
    assert settings.database_path != DEFAULT_DATABASE.resolve()
    assert settings.storage_path.is_absolute()


@pytest.mark.parametrize(
    ("filename", "content", "expected_format"),
    [
        ("measurements.csv", b"asset_id,temperature\nA-1,31.5\nA-2,0\n", "csv"),
        ("metadata.json", b'{"site":"north","measurements":[1,2,3]}', "json"),
    ],
)
def test_upload_worker_result_status_and_provenance(system, filename, content, expected_format):
    settings, app, client, worker = system
    uploaded = _upload(client, filename, content)
    assert uploaded.status_code == 202, uploaded.text
    body = uploaded.json()
    assert body["source"]["state"] == "available"
    assert body["source"]["sha256"] == hashlib.sha256(content).hexdigest()
    assert "storage_key" not in body["source"]
    assert worker.run_once() is True

    status = client.get(f"/api/v1/jobs/{body['job_id']}")
    assert status.status_code == 200
    assert status.json()["state"] == "succeeded"
    assert "lease_token" not in status.text
    result_response = client.get(f"/api/v1/jobs/{body['job_id']}/result")
    assert result_response.status_code == 200
    result = result_response.json()
    encoded = app.state.storage.read_bytes(
        app.state.repository.get_job_result(body["job_id"]).storage_key
    )
    assert hashlib.sha256(encoded).hexdigest() == result["checksum"]
    assert result["result"]["format"] == expected_format
    assert result["source_file_id"] == body["source"]["source_file_id"]
    assert result["result"]["source"]["sha256"] == body["source"]["sha256"]

    with get_connection(settings.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM extraction_artifacts").fetchone()[0] == 1
        for table in ("assets", "inspections", "observations", "maintenance_records"):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


def test_duplicate_upload_reuses_source_and_compatible_job(system):
    _, _, client, _ = system
    content = b"asset_id,power\nINV-1,10\n"
    first = _upload(client, "first.csv", content).json()
    second = _upload(client, "renamed.csv", content).json()
    assert second["source"]["source_file_id"] == first["source"]["source_file_id"]
    assert second["source"]["duplicate"] is True
    assert second["job_id"] == first["job_id"]
    assert second["job_reused"] is True


def test_malformed_and_oversized_uploads_fail_safely(system):
    _, _, client, worker = system
    malformed = _upload(client, "bad.csv", b"a,b\n1\n")
    assert malformed.status_code == 202
    malformed_id = malformed.json()["job_id"]
    worker.run_once()
    status = client.get(f"/api/v1/jobs/{malformed_id}").json()
    assert status["state"] == "failed_terminal"
    assert status["error_code"] == "invalid_input"
    assert client.get(f"/api/v1/jobs/{malformed_id}/result").status_code == 409

    malformed_json = _upload(client, "bad.json", b"{not valid json}").json()
    worker.run_once()
    assert client.get(f"/api/v1/jobs/{malformed_json['job_id']}").json()["state"] == "failed_terminal"

    oversized = _upload(client, "large.json", b" " * (2 * 1024 * 1024 + 1))
    assert oversized.status_code == 413


def test_queued_cancellation_and_worker_restart_recovery(system):
    settings, app, client, worker = system
    uploaded = _upload(client).json()
    cancelled = client.post(f"/api/v1/jobs/{uploaded['job_id']}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["state"] == "cancelled"
    assert worker.run_once() is False

    recovered_upload = _upload(client, "after-crash.json", b'{"recovered":true}').json()
    first_queue = JobQueue(settings.database_path)
    lease = first_queue.claim_next_job("crashed-worker", lease_seconds=1)
    assert lease is not None and lease.job.job_id == recovered_upload["job_id"]
    future = datetime.now(timezone.utc) + timedelta(seconds=2)
    assert first_queue.recover_expired_leases(now=future) == [recovered_upload["job_id"]]
    with get_connection(settings.database_path) as connection:
        connection.execute(
            "UPDATE ingestion_job_runtime SET available_at = ? WHERE job_id = ?",
            (datetime.now(timezone.utc).isoformat(), recovered_upload["job_id"]),
        )
    restarted_worker = IngestionWorker(settings.database_path, app.state.storage, lease_seconds=9)
    assert restarted_worker.run_once() is True
    assert client.get(f"/api/v1/jobs/{recovered_upload['job_id']}").json()["state"] == "succeeded"


def test_running_cancellation_is_acknowledged_without_result(system, monkeypatch):
    _, app, client, worker = system
    uploaded = _upload(client, "cancel-running.csv").json()
    import solarpv_ingestion.worker as worker_module

    def cancel_during_processing(source, storage, **kwargs):
        app.state.queue.request_cancellation(uploaded["job_id"])
        assert kwargs["cancelled"]() is True
        raise worker_module.ProcessingCancelled

    monkeypatch.setattr(worker_module, "profile_source", cancel_during_processing)
    assert worker.run_once() is True
    assert client.get(f"/api/v1/jobs/{uploaded['job_id']}").json()["state"] == "cancelled"
    assert client.get(f"/api/v1/jobs/{uploaded['job_id']}/result").status_code == 409


def test_transient_processing_failure_retries_without_duplicate_result(system, monkeypatch):
    _, app, client, worker = system
    uploaded = _upload(client, "retry.json", b'{"retry":1}').json()
    import solarpv_ingestion.worker as worker_module

    original = worker_module.profile_source
    attempts = 0

    def transient_once(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("temporary read failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(worker_module, "profile_source", transient_once)
    assert worker.run_once() is True
    assert client.get(f"/api/v1/jobs/{uploaded['job_id']}").json()["state"] == "retry_wait"
    assert worker.run_once() is True
    assert client.get(f"/api/v1/jobs/{uploaded['job_id']}").json()["state"] == "succeeded"
    assert app.state.repository.get_job_result(uploaded["job_id"]) is not None
    with get_connection(app.state.settings.database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM extraction_artifacts WHERE job_id = ?", (uploaded["job_id"],)
        ).fetchone()[0] == 1


def test_published_result_survives_commit_interruption_without_duplicate_result(system, monkeypatch):
    settings, app, client, worker = system
    uploaded = _upload(client, "commit-retry.csv").json()
    complete = worker.queue.complete_job_with_artifact
    failed_once = False

    def fail_before_commit(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise sqlite3.OperationalError("simulated commit interruption")
        return complete(*args, **kwargs)

    monkeypatch.setattr(worker.queue, "complete_job_with_artifact", fail_before_commit)
    assert worker.run_once() is True
    assert client.get(f"/api/v1/jobs/{uploaded['job_id']}").json()["state"] == "retry_wait"
    orphan_key = f"extractions/{uploaded['job_id']}/attempt-1.json"
    assert not app.state.storage.exists(orphan_key)

    assert worker.run_once() is True
    assert client.get(f"/api/v1/jobs/{uploaded['job_id']}").json()["state"] == "succeeded"
    assert client.get(f"/api/v1/jobs/{uploaded['job_id']}/result").status_code == 200
    with get_connection(settings.database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM extraction_artifacts WHERE job_id = ?", (uploaded["job_id"],)
        ).fetchone()[0] == 1


def test_result_metadata_rolls_back_if_job_completion_fails(system):
    settings, app, client, worker = system
    uploaded = _upload(client, "rollback.csv").json()
    job_id = uploaded["job_id"]
    trigger = f"""
        CREATE TRIGGER fail_test_job_completion
        BEFORE UPDATE OF state ON ingestion_job_runtime
        WHEN OLD.job_id = '{job_id}' AND NEW.state = 'succeeded'
        BEGIN
            SELECT RAISE(ABORT, 'simulated job completion failure');
        END
    """
    with get_connection(settings.database_path) as connection:
        connection.execute(trigger)

    assert worker.run_once() is True
    assert client.get(f"/api/v1/jobs/{job_id}").json()["state"] == "failed_terminal"
    assert client.get(f"/api/v1/jobs/{job_id}/result").status_code == 409
    assert not app.state.storage.exists(f"extractions/{job_id}/attempt-1.json")
    with get_connection(settings.database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM extraction_artifacts WHERE job_id = ?", (job_id,)
        ).fetchone()[0] == 0
