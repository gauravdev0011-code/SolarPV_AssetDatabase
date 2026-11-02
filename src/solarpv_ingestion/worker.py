"""Sequential durable worker for generic CSV and JSON profiling."""

import hashlib
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from uuid import uuid4

from solarpv_db.database import initialize_database
from solarpv_ingestion.intake import FileIntakeService
from solarpv_ingestion.jobs import JobLeaseError, JobQueue
from solarpv_ingestion.processing import (
    PARSER_ID,
    PARSER_VERSION,
    RESULT_SCHEMA_VERSION,
    InvalidInputError,
    ProcessingCancelled,
    profile_source,
)
from solarpv_ingestion.repository import IngestionRepository
from solarpv_ingestion.storage import LocalArtifactStorage

LOGGER = logging.getLogger("solarpv.worker")


class IngestionWorker:
    def __init__(
        self,
        database_path: str | Path,
        storage: LocalArtifactStorage,
        *,
        worker_id: str = "solarpv-worker-1",
        lease_seconds: int = 90,
        max_json_bytes: int = 5 * 1024 * 1024,
        retry_delay_seconds: int = 5,
    ) -> None:
        self.database_path = Path(database_path)
        self.storage = storage
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.max_json_bytes = max_json_bytes
        self.retry_delay_seconds = retry_delay_seconds
        self.queue = JobQueue(database_path)
        self.repository = IngestionRepository(database_path)
        self.intake = FileIntakeService(database_path=self.database_path, storage=storage)

    def recover(self) -> None:
        """Recover interrupted intake sessions and expired worker leases."""
        self.intake.recover_incomplete_intakes()
        self.queue.recover_expired_leases()

    def run_once(self) -> bool:
        lease = self.queue.claim_next_job(self.worker_id, lease_seconds=self.lease_seconds)
        if lease is None:
            return False
        job_id = lease.job.job_id
        self._remove_previous_attempt_orphans(job_id, lease.attempt_number)
        stop_heartbeat = threading.Event()
        lease_lost = threading.Event()

        def heartbeat() -> None:
            interval = max(1, self.lease_seconds // 3)
            while not stop_heartbeat.wait(interval):
                try:
                    self.queue.renew_lease(job_id, lease.lease_token, lease_seconds=self.lease_seconds)
                except Exception:
                    lease_lost.set()
                    return

        thread = threading.Thread(target=heartbeat, name=f"lease-{job_id[:8]}", daemon=True)
        thread.start()
        result_key = None
        try:
            source = self.repository.get_source_file(lease.job.source_file_id)
            if source is None:
                raise InvalidInputError("The source file metadata is missing.")
            if not self.storage.matches(source.storage_key, sha256=source.sha256, size_bytes=source.size_bytes):
                raise InvalidInputError("Stored source does not match its recorded fingerprint.")

            def cancelled() -> bool:
                if lease_lost.is_set():
                    return True
                runtime = self.queue.get_job(job_id)
                return runtime is None or runtime.state.value == "cancel_requested"

            if cancelled():
                self.queue.complete_job(job_id, lease.lease_token, cancelled=True)
                return True
            result = profile_source(source, self.storage, max_json_bytes=self.max_json_bytes, cancelled=cancelled)
            if lease_lost.is_set():
                return True
            encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
            if len(encoded) > 1024 * 1024:
                raise InvalidInputError("Processing summary exceeds the 1 MiB result limit.")
            checksum = hashlib.sha256(encoded).hexdigest()
            key = f"extractions/{job_id}/attempt-{lease.attempt_number}.json"
            result_key = key
            self.storage.put_bytes(key, encoded)
            try:
                completed = self.queue.complete_job_with_artifact(
                    job_id, lease.lease_token, artifact_id=uuid4().hex,
                    storage_key=key, parser_id=PARSER_ID, parser_version=PARSER_VERSION,
                    schema_version=RESULT_SCHEMA_VERSION, checksum=checksum,
                )
                if completed.state.value != "succeeded":
                    self._delete_if_unreferenced(job_id, key)
            except JobLeaseError:
                # A stale worker may have published an orphan but cannot expose it as a result.
                self._delete_if_unreferenced(job_id, key)
                return True
            return True
        except ProcessingCancelled:
            try:
                self.queue.complete_job(job_id, lease.lease_token, cancelled=True)
            except JobLeaseError:
                pass
            return True
        except InvalidInputError as error:
            try:
                self.queue.fail_job(job_id, lease.lease_token, error_code="invalid_input", error_message=str(error), retryable=False)
            except JobLeaseError:
                LOGGER.info("Lease lost before recording validation failure for job %s", job_id)
            if result_key is not None:
                self._delete_if_unreferenced(job_id, result_key)
            return True
        except (OSError, TimeoutError, sqlite3.OperationalError) as error:
            self._record_failure(
                job_id, lease.lease_token, "processing_io",
                "Temporary storage or database I/O failure.",
                retryable=True, result_key=result_key,
            )
            return True
        except Exception:
            LOGGER.exception("Unexpected processing failure for job %s", job_id)
            self._record_failure(
                job_id, lease.lease_token, "processing_error",
                "Unexpected processing failure.", retryable=False,
                result_key=result_key,
            )
            return True
        finally:
            stop_heartbeat.set()
            thread.join(timeout=2)

    def _record_failure(self, job_id, token, code, message, *, retryable, result_key=None):
        try:
            self.queue.fail_job(
                job_id, token, error_code=code, error_message=message,
                retryable=retryable, retry_delay_seconds=self.retry_delay_seconds,
            )
        except JobLeaseError:
            LOGGER.info("Lease lost before recording failure for job %s", job_id)
        if result_key is not None:
            self._delete_if_unreferenced(job_id, result_key)

    def _delete_if_unreferenced(self, job_id: str, key: str) -> None:
        from solarpv_db.database import get_connection

        with get_connection(self.database_path) as connection:
            row = connection.execute(
                "SELECT 1 FROM extraction_artifacts WHERE job_id = ? AND storage_key = ?",
                (job_id, key),
            ).fetchone()
        if row is None:
            self.storage.delete_uncommitted(key)

    def _remove_previous_attempt_orphans(self, job_id: str, current_attempt: int) -> None:
        """Remove only prior attempt result keys that have no committed metadata."""
        from solarpv_db.database import get_connection

        for attempt in range(1, current_attempt):
            key = f"extractions/{job_id}/attempt-{attempt}.json"
            with get_connection(self.database_path) as connection:
                row = connection.execute(
                    "SELECT 1 FROM extraction_artifacts WHERE job_id = ? AND storage_key = ?",
                    (job_id, key),
                ).fetchone()
            if row is None:
                self.storage.delete_uncommitted(key)

    def run_forever(self, *, poll_seconds: float = 1.0, stop_event: threading.Event | None = None) -> None:
        self.recover()
        stopping = stop_event or threading.Event()
        recovery_interval = max(1.0, min(30.0, self.lease_seconds / 3))
        next_recovery = time.monotonic() + recovery_interval
        while not stopping.is_set():
            try:
                if time.monotonic() >= next_recovery:
                    self.queue.recover_expired_leases()
                    next_recovery = time.monotonic() + recovery_interval
                if not self.run_once():
                    stopping.wait(poll_seconds)
            except Exception:
                LOGGER.exception("Worker iteration failed; continuing after delay")
                stopping.wait(poll_seconds)


def main() -> None:
    from solarpv_api.config import Settings

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_environment()
    initialize_database(settings.database_path)
    storage = LocalArtifactStorage(settings.storage_path)
    worker = IngestionWorker(
        settings.database_path, storage, lease_seconds=settings.lease_seconds,
        max_json_bytes=settings.max_json_bytes, retry_delay_seconds=settings.retry_delay_seconds,
    )
    worker.run_forever(poll_seconds=settings.worker_poll_seconds)


if __name__ == "__main__":
    main()
