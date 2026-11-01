"""Phase 2A intake and durable job queue tests using disposable storage."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from io import BytesIO
from hashlib import sha256
import multiprocessing
import os
import sqlite3
import time

import pytest

from solarpv_db.database import get_connection, initialize_database
from solarpv_ingestion.intake import (
    DuplicateContentUnavailable,
    FileIntakeService,
    IntakeRecoveryRequired,
    UnsafeUploadError,
)
from solarpv_ingestion.jobs import JobLeaseError, JobQueue
from solarpv_ingestion.repository import IngestionRepository, JobLifecycleOwnershipError
from solarpv_ingestion.states import IngestionJobState, SourceFileState
from solarpv_ingestion.storage import LocalArtifactStorage, UploadTooLargeError


@pytest.fixture
def environment(tmp_path):
    database_path = tmp_path / "phase2a.db"
    initialize_database(database_path)
    storage = LocalArtifactStorage(tmp_path / "private-storage")
    intake = FileIntakeService(
        database_path=database_path, storage=storage,
        max_file_bytes=32, chunk_size=4,
    )
    return database_path, storage, intake, JobQueue(database_path)


def test_stream_upload_persists_available_metadata_and_private_content(environment, tmp_path):
    database_path, storage, intake, _ = environment
    payload = b"research-file-data"
    result = intake.accept(
        BytesIO(payload), workspace_id="default", original_filename="site.csv"
    )
    assert result.duplicate is False
    assert result.source_file.state == SourceFileState.AVAILABLE
    assert result.source_file.size_bytes == len(payload)
    assert result.source_file.sha256 == sha256(payload).hexdigest()
    assert result.source_file.storage_key.startswith("originals/")
    assert storage.read_bytes(result.source_file.storage_key) == payload
    assert not (tmp_path / "site.csv").exists()
    with get_connection(database_path) as connection:
        row = connection.execute(
            "SELECT state, original_name FROM source_files WHERE source_file_id = ?",
            (result.source_file.source_file_id,),
        ).fetchone()
        assert tuple(row) == ("available", "site.csv")
        with pytest.raises(sqlite3.IntegrityError, match="invalid source file intake"):
            connection.execute(
                "UPDATE source_files SET state = 'receiving' WHERE source_file_id = ?",
                (result.source_file.source_file_id,),
            )


def test_duplicate_content_reuses_existing_source_without_overwrite(environment):
    _, storage, intake, _ = environment
    first = intake.accept(BytesIO(b"same payload"), workspace_id="default", original_filename="first.csv")
    second = intake.accept(BytesIO(b"same payload"), workspace_id="default", original_filename="second.csv")
    assert second.duplicate is True
    assert second.source_file.source_file_id == first.source_file.source_file_id
    assert second.source_file.original_name == "first.csv"
    assert storage.read_bytes(first.source_file.storage_key) == b"same payload"
    assert not list((storage._root / ".staging").glob("*.partial"))


def test_size_limit_rejects_stream_and_cleans_partial_file(environment):
    _, storage, intake, _ = environment
    with pytest.raises(UploadTooLargeError, match="maximum size"):
        intake.accept(BytesIO(b"x" * 33), workspace_id="default", original_filename="large.csv")
    assert not list((storage._root / ".staging").glob("*.partial"))


@pytest.mark.parametrize("name", ["../escape.csv", "folder\\file.csv", "C:evil.csv", "..", "NUL.txt", "bad\nname.csv"])
def test_unsafe_filename_is_rejected_before_reading(environment, name):
    _, _, intake, _ = environment
    with pytest.raises(UnsafeUploadError):
        intake.accept(BytesIO(b"x"), workspace_id="default", original_filename=name)


class FailingStream:
    def __init__(self):
        self.calls = 0

    def read(self, _size):
        self.calls += 1
        if self.calls == 1:
            return b"part"
        raise OSError("simulated stream interruption")


def test_interrupted_stream_leaves_no_partial_or_database_record(environment):
    database_path, storage, intake, _ = environment
    with pytest.raises(OSError, match="interruption"):
        intake.accept(FailingStream(), workspace_id="default", original_filename="broken.csv")
    assert not list((storage._root / ".staging").glob("*.partial"))
    with get_connection(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_files").fetchone()[0] == 0


def test_executable_signature_is_rejected_without_publishing(environment):
    database_path, storage, intake, _ = environment
    with pytest.raises(UnsafeUploadError, match="Executable or script"):
        intake.accept(BytesIO(b"MZ" + b"\x00" * 20), workspace_id="default", original_filename="notes.csv")
    assert not list((storage._root / ".staging").glob("*.partial"))
    with get_connection(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_files").fetchone()[0] == 0


def test_atomic_publish_never_exposes_partial_content(environment):
    _, storage, intake, _ = environment
    result = intake.accept(BytesIO(b"1234567890"), workspace_id="default", original_filename="atomic.txt")
    published = storage._resolve_key(result.source_file.storage_key)
    assert published.read_bytes() == b"1234567890"
    assert result.source_file.state == SourceFileState.AVAILABLE
    assert not list(published.parent.glob("*.partial"))


def test_database_failure_does_not_publish_file(environment):
    database_path, storage, _, _ = environment
    intake = FileIntakeService(database_path=database_path, storage=storage, max_file_bytes=100)
    with pytest.raises(sqlite3.IntegrityError):
        intake.accept(BytesIO(b"content"), workspace_id="missing", original_filename="x.csv")
    assert not list((storage._root / "originals").glob("*"))
    assert not list((storage._root / ".staging").glob("*.partial"))


class FailBeforePublishStorage(LocalArtifactStorage):
    fail = True

    def publish(self, staged, key):
        if self.fail:
            raise OSError("simulated failure before publication")
        super().publish(staged, key)


class FailAfterPublishStorage(LocalArtifactStorage):
    fail = True

    def publish(self, staged, key):
        super().publish(staged, key)
        if self.fail:
            raise OSError("simulated interruption after hard link")


def _create_upload_session(intake, upload_id, filename="recover.csv"):
    token = f"token-{upload_id}"
    intake._create_session(
        upload_id, "default", filename, intake.storage.staging_key(upload_id),
        f"originals/{upload_id}", f"pid:{os.getpid()}", token,
    )
    return token


def _expire_upload(database_path, upload_id):
    with get_connection(database_path) as connection:
        connection.execute(
            "UPDATE intake_uploads SET lease_expires_at = '2000-01-01T00:00:00.000000Z' WHERE intake_id = ?",
            (upload_id,),
        )


def _hold_active_staging_process(storage_root, upload_id, ready, finish):
    storage = LocalArtifactStorage(storage_root)
    staged = storage.stage_stream(
        BytesIO(b"active content"), max_bytes=100, upload_id=upload_id
    )
    old = time.time() - 3 * 24 * 60 * 60
    os.utime(staged.path, (old, old))
    ready.set()
    finish.wait(20)
    storage.release_staged(staged)


def _interrupt_staging_process(storage_root, upload_id, ready):
    storage = LocalArtifactStorage(storage_root)
    storage.stage_stream(
        BytesIO(b"interrupted process"), max_bytes=100, upload_id=upload_id
    )
    ready.set()
    os._exit(0)


def test_recovery_does_not_delete_active_upload_older_than_threshold(environment):
    database_path, storage, intake, _ = environment
    upload_id = "active-cross-process"
    _create_upload_session(intake, upload_id)
    context = multiprocessing.get_context("spawn")
    ready, finish = context.Event(), context.Event()
    process = context.Process(
        target=_hold_active_staging_process,
        args=(str(storage._root), upload_id, ready, finish),
    )
    process.start()
    try:
        assert ready.wait(15)
        _expire_upload(database_path, upload_id)
        staged_path = storage._resolve_key(storage.staging_key(upload_id))
        assert staged_path.exists()
        intake.recover_incomplete_intakes(older_than_seconds=1)
        assert staged_path.exists()
        with get_connection(database_path) as connection:
            assert connection.execute(
                "SELECT state FROM intake_uploads WHERE intake_id = ?", (upload_id,)
            ).fetchone()[0] == "receiving"
    finally:
        finish.set()
        process.join(20)
        if process.is_alive():
            process.terminate()
            process.join()
    assert process.exitcode == 0
    intake.recover_incomplete_intakes(older_than_seconds=1)
    assert not storage._resolve_key(storage.staging_key(upload_id)).exists()
    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT state FROM intake_uploads WHERE intake_id = ?", (upload_id,)
        ).fetchone()[0] == "rejected"


def test_abandoned_partial_is_rejected_and_cleaned(environment):
    database_path, storage, intake, _ = environment
    upload_id = "abandoned-stage"
    _create_upload_session(intake, upload_id)
    staged = storage.stage_stream(BytesIO(b"abandoned"), max_bytes=100, upload_id=upload_id)
    old = time.time() - 3 * 24 * 60 * 60
    os.utime(staged.path, (old, old))
    storage.release_staged(staged)
    _expire_upload(database_path, upload_id)

    intake.recover_incomplete_intakes(older_than_seconds=1)

    assert not storage._resolve_key(storage.staging_key(upload_id)).exists()
    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT state FROM intake_uploads WHERE intake_id = ?", (upload_id,)
        ).fetchone()[0] == "rejected"


def test_process_interruption_leaves_a_recoverable_receiving_session(environment):
    database_path, storage, intake, _ = environment
    upload_id = "process-interruption"
    _create_upload_session(intake, upload_id)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(
        target=_interrupt_staging_process,
        args=(str(storage._root), upload_id, ready),
    )
    process.start()
    assert ready.wait(15)
    process.join(15)
    assert process.exitcode == 0
    _expire_upload(database_path, upload_id)

    intake.recover_incomplete_intakes()

    assert not storage._resolve_key(storage.staging_key(upload_id)).exists()
    with get_connection(database_path) as connection:
        session = connection.execute(
            "SELECT state FROM intake_uploads WHERE intake_id = ?", (upload_id,)
        ).fetchone()
        assert session["state"] == "rejected"


def test_failure_before_source_creation_can_be_retried(environment):
    database_path, storage, intake, _ = environment
    with pytest.raises(OSError, match="interruption"):
        intake.accept(FailingStream(), workspace_id="default", original_filename="retry.csv")
    with get_connection(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_files").fetchone()[0] == 0
        assert connection.execute(
            "SELECT state FROM intake_uploads"
        ).fetchone()[0] == "rejected"
    result = intake.accept(BytesIO(b"part"), workspace_id="default", original_filename="retry.csv")
    assert result.duplicate is False
    assert storage.read_bytes(result.source_file.storage_key) == b"part"


def test_published_file_before_database_commit_is_recovered(environment, tmp_path):
    database_path, _, _, _ = environment
    storage = FailAfterPublishStorage(tmp_path / "post-link-storage")
    intake = FileIntakeService(database_path=database_path, storage=storage, max_file_bytes=100)
    with pytest.raises(IntakeRecoveryRequired):
        intake.accept(BytesIO(b"post-link"), workspace_id="default", original_filename="post-link.csv")
    with get_connection(database_path) as connection:
        session = connection.execute("SELECT * FROM intake_uploads").fetchone()
        assert session["state"] == "staged"
        assert connection.execute("SELECT COUNT(*) FROM source_files").fetchone()[0] == 0
    storage.fail = False
    _expire_upload(database_path, session["intake_id"])

    recovered = intake.recover_incomplete_intakes()

    assert len(recovered) == 1 and recovered[0].state == SourceFileState.AVAILABLE
    assert storage.read_bytes(session["storage_key"]) == b"post-link"


def test_publish_failure_before_link_preserves_staged_retry(environment, tmp_path):
    database_path, _, _, _ = environment
    storage = FailBeforePublishStorage(tmp_path / "pre-link-storage")
    intake = FileIntakeService(database_path=database_path, storage=storage, max_file_bytes=100)
    with pytest.raises(IntakeRecoveryRequired):
        intake.accept(BytesIO(b"pre-link"), workspace_id="default", original_filename="pre-link.csv")
    with get_connection(database_path) as connection:
        session = connection.execute("SELECT * FROM intake_uploads").fetchone()
        assert session["state"] == "staged"
        assert connection.execute("SELECT COUNT(*) FROM source_files").fetchone()[0] == 0
    storage.fail = False
    _expire_upload(database_path, session["intake_id"])
    recovered = intake.recover_incomplete_intakes()
    assert len(recovered) == 1
    assert storage.read_bytes(session["storage_key"]) == b"pre-link"


def test_recovery_reconciles_crash_after_hard_link_before_commit(environment):
    database_path, storage, intake, _ = environment
    upload_id = "link-before-commit"
    token = _create_upload_session(intake, upload_id)
    staged = storage.stage_stream(BytesIO(b"linked"), max_bytes=100, upload_id=upload_id)
    intake._mark_staged(upload_id, f"pid:{os.getpid()}", token, staged, media_type="text/plain")
    storage.publish(staged, f"originals/{upload_id}")
    storage.release_staged(staged)
    _expire_upload(database_path, upload_id)

    recovered = intake.recover_incomplete_intakes()

    assert [row.source_file_id for row in recovered] == [upload_id]
    assert storage.read_bytes(f"originals/{upload_id}") == b"linked"


def test_concurrent_identical_uploads_create_one_original(environment):
    _, storage, intake, _ = environment
    def upload(filename):
        return intake.accept(
            BytesIO(b"same concurrent bytes"), workspace_id="default",
            original_filename=filename,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(upload, ("one.csv", "two.csv")))
    assert sorted(result.duplicate for result in results) == [False, True]
    source_ids = {result.source_file.source_file_id for result in results}
    assert len(source_ids) == 1
    source = results[0].source_file
    assert storage.read_bytes(source.storage_key) == b"same concurrent bytes"


def _available_source(intake):
    return intake.accept(
        BytesIO(b"job source"), workspace_id="default", original_filename="job.csv"
    ).source_file


def test_job_persists_across_reconnection_and_atomic_claim(environment):
    database_path, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(
        job_id="persistent-job", workspace_id="default", source_file_id=source.source_file_id,
        idempotency_key="persistent-key",
    )
    reopened = JobQueue(database_path)
    assert reopened.get_job("persistent-job").state == IngestionJobState.QUEUED
    claims = (reopened.claim_next_job("worker-a"), reopened.claim_next_job("worker-b"))
    assert sum(claim is not None for claim in claims) == 1


def test_database_rejects_invalid_runtime_state_transition(environment):
    database_path, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(
        job_id="sql-transition", workspace_id="default",
        source_file_id=source.source_file_id, idempotency_key="sql-transition",
    )
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="invalid ingestion job runtime"):
            connection.execute(
                "UPDATE ingestion_job_runtime SET state = 'succeeded' WHERE job_id = 'sql-transition'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="must start queued"):
            connection.execute(
                """INSERT INTO ingestion_job_runtime (job_id, state, lease_owner,
                       lease_token, lease_expires_at)
                   VALUES ('forged-runtime', 'running', 'forged', 'token', '2099-01-01T00:00:00Z')"""
            )
        with pytest.raises(sqlite3.IntegrityError, match="legacy job state"):
            connection.execute(
                "UPDATE ingestion_jobs SET state = 'cancelled' WHERE job_id = 'sql-transition'"
            )


def test_job_creation_requires_workspace_match_and_available_source(environment):
    database_path, _, intake, queue = environment
    source = _available_source(intake)
    with get_connection(database_path) as connection:
        connection.execute(
            "INSERT INTO workspaces (workspace_id, name) VALUES ('other', 'Other')"
        )
    with pytest.raises(ValueError, match="same workspace"):
        queue.create_job(job_id="wrong-workspace", workspace_id="other", source_file_id=source.source_file_id, idempotency_key="wrong-workspace")
    with get_connection(database_path) as connection:
        connection.execute(
            "UPDATE source_files SET state = 'deletion_pending' WHERE source_file_id = ?",
            (source.source_file_id,),
        )
    with pytest.raises(ValueError, match="available source"):
        queue.create_job(job_id="unavailable-source", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="unavailable-source")


def test_competing_workers_cannot_claim_same_job(environment):
    _, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(job_id="race-job", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="race-key")
    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(queue.claim_next_job, ["worker-a", "worker-b"]))
    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert queue.get_job("race-job").attempt_count == 1


def test_lease_renewal_expiration_recovery_and_reclaim(environment):
    _, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(job_id="lease-job", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="lease-key", max_attempts=3)
    now = datetime(2030, 10, 1, tzinfo=timezone.utc)
    lease = queue.claim_next_job("worker", lease_seconds=5, now=now)
    assert lease is not None
    renewed = queue.renew_lease("lease-job", lease.lease_token, lease_seconds=10, now=now + timedelta(seconds=1))
    assert renewed > lease.lease_expires_at
    assert queue.recover_expired_leases(now=now + timedelta(seconds=2)) == []
    assert queue.recover_expired_leases(now=now + timedelta(seconds=12)) == ["lease-job"]
    assert queue.get_job("lease-job").state == IngestionJobState.RETRY_WAIT
    with pytest.raises(JobLeaseError):
        queue.renew_lease("lease-job", lease.lease_token, now=now + timedelta(seconds=13))
    second = queue.claim_next_job("worker-2", now=now + timedelta(seconds=13))
    assert second is not None and second.attempt_number == 2


def test_retry_schedule_terminal_failure_and_linked_new_attempt(environment):
    _, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(job_id="retry-job", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="retry-key", max_attempts=2)
    now = datetime(2030, 10, 1, tzinfo=timezone.utc)
    first = queue.claim_next_job("worker", now=now)
    retry = queue.fail_job("retry-job", first.lease_token, error_code="temporary/db busy", error_message="transient issue", retryable=True, retry_delay_seconds=10, now=now)
    assert retry.state == IngestionJobState.RETRY_WAIT
    assert queue.claim_next_job("early", now=now + timedelta(seconds=9)) is None
    second = queue.claim_next_job("worker", now=now + timedelta(seconds=10))
    terminal = queue.fail_job("retry-job", second.lease_token, error_code="bad_input", error_message="x" * 800, retryable=True, now=now + timedelta(seconds=10))
    assert terminal.state == IngestionJobState.FAILED_TERMINAL
    linked = queue.create_retry_job(prior_job_id="retry-job", job_id="retry-job-2", idempotency_key="retry-key-2")
    assert linked.retry_of_job_id == "retry-job"
    assert queue.get_job("retry-job-2").state == IngestionJobState.QUEUED
    with get_connection(environment[0]) as connection:
        attempts = connection.execute("SELECT outcome, length(error_message) AS n FROM job_attempts WHERE job_id = 'retry-job' ORDER BY attempt_number").fetchall()
        assert [row["outcome"] for row in attempts] == ["retryable_failure", "terminal_failure"]
        assert attempts[1]["n"] <= 500


def test_cancellation_for_queued_and_running_jobs(environment):
    _, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(job_id="cancel-queued", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="cancel-q")
    assert queue.request_cancellation("cancel-queued").state == IngestionJobState.CANCELLED
    queue.create_job(job_id="cancel-running", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="cancel-r")
    lease = queue.claim_next_job("worker")
    assert lease.job.job_id == "cancel-running"
    assert queue.request_cancellation("cancel-running").state == IngestionJobState.CANCEL_REQUESTED
    assert queue.request_cancellation("cancel-running").state == IngestionJobState.CANCEL_REQUESTED
    assert queue.complete_job("cancel-running", lease.lease_token, cancelled=True).state == IngestionJobState.CANCELLED


def test_legacy_repository_cancellation_cannot_leave_job_claimable(environment):
    _, _, intake, queue = environment
    repository = IngestionRepository(queue.database_path)
    source = _available_source(intake)
    repository.create_job(
        job_id="legacy-cancel", workspace_id="default",
        source_file_id=source.source_file_id, idempotency_key="legacy-cancel",
    )
    with pytest.raises(JobLifecycleOwnershipError, match="current worker lease"):
        repository.transition_job("legacy-cancel", IngestionJobState.RUNNING)
    cancelled = repository.transition_job("legacy-cancel", IngestionJobState.CANCELLED)
    assert cancelled.state == IngestionJobState.CANCELLED
    assert queue.get_job("legacy-cancel").state == IngestionJobState.CANCELLED
    assert queue.claim_next_job("worker") is None


@pytest.mark.parametrize("stale_action", ["complete", "fail"])
def test_stale_worker_cannot_complete_or_fail_after_reassignment(environment, stale_action):
    _, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(
        job_id=f"stale-{stale_action}", workspace_id="default",
        source_file_id=source.source_file_id, idempotency_key=f"stale-{stale_action}",
        max_attempts=3,
    )
    now = datetime(2030, 10, 1, tzinfo=timezone.utc)
    first = queue.claim_next_job("worker-old", lease_seconds=1, now=now)
    queue.recover_expired_leases(now=now + timedelta(seconds=2))
    second = queue.claim_next_job("worker-new", now=now + timedelta(seconds=3))
    assert second is not None and second.attempt_number == 2

    with pytest.raises(JobLeaseError):
        if stale_action == "complete":
            queue.complete_job(first.job.job_id, first.lease_token, now=now + timedelta(seconds=4))
        else:
            queue.fail_job(
                first.job.job_id, first.lease_token, error_code="stale",
                error_message="old worker", retryable=True, now=now + timedelta(seconds=4),
            )
    assert queue.get_job(first.job.job_id).state == IngestionJobState.RUNNING
    assert queue.complete_job(second.job.job_id, second.lease_token, now=now + timedelta(seconds=4)).state == IngestionJobState.SUCCEEDED
    assert IngestionRepository(queue.database_path).get_job(second.job.job_id).state == IngestionJobState.SUCCEEDED


def test_reconciliation_required_is_not_blindly_retried(environment):
    _, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(job_id="reconcile", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="reconcile", max_attempts=1)
    now = datetime(2030, 10, 1, tzinfo=timezone.utc)
    lease = queue.claim_next_job("worker", lease_seconds=1, now=now)
    assert queue.recover_expired_leases(now=now + timedelta(seconds=2)) == ["reconcile"]
    assert queue.get_job("reconcile").state == IngestionJobState.RECONCILIATION_REQUIRED
    assert queue.claim_next_job("worker-2", now=now + timedelta(seconds=3)) is None
    with pytest.raises(ValueError, match="terminally failed"):
        queue.create_retry_job(prior_job_id="reconcile", job_id="reconcile-2", idempotency_key="reconcile-2")


def test_job_attempt_history_is_written_transactionally(environment):
    database_path, _, intake, queue = environment
    source = _available_source(intake)
    queue.create_job(job_id="attempt-history", workspace_id="default", source_file_id=source.source_file_id, idempotency_key="attempt-history")
    lease = queue.claim_next_job("worker")
    with get_connection(database_path) as connection:
        attempt = connection.execute("SELECT * FROM job_attempts WHERE job_id = ?", (lease.job.job_id,)).fetchone()
        assert attempt["attempt_number"] == 1
        assert attempt["started_at"] is not None
