"""Durable SQLite job queue with expiring worker leases."""

from datetime import datetime, timedelta, timezone
import re
import sqlite3
from pathlib import Path
from uuid import uuid4

from solarpv_db.database import DEFAULT_DATABASE, get_connection
from solarpv_ingestion.models import IngestionJob, JobLease, JobRuntime
from solarpv_ingestion.repository import IngestionRepository
from solarpv_ingestion.states import IngestionJobState
from solarpv_ingestion.transitions import validate_transition


_ERROR_CODE = re.compile(r"[^a-zA-Z0-9_.-]+")


class JobLeaseError(RuntimeError):
    """Raised when a worker does not hold the current unexpired lease."""


class JobQueue:
    """Create and operate durable jobs in the Phase 1 SQLite database."""

    def __init__(self, database_path: str | Path = DEFAULT_DATABASE) -> None:
        self.database_path = database_path

    def create_job(
        self, *, job_id: str, workspace_id: str, source_file_id: str,
        idempotency_key: str, max_attempts: int = 3,
    ) -> JobRuntime:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1.")
        if not job_id.strip() or not workspace_id.strip() or not idempotency_key.strip():
            raise ValueError("Job, workspace, and idempotency identifiers are required.")
        IngestionRepository(self.database_path).create_job(
            job_id=job_id, workspace_id=workspace_id, source_file_id=source_file_id,
            idempotency_key=idempotency_key, max_attempts=max_attempts,
            require_available=True,
        )
        return self.get_job(job_id)  # type: ignore[return-value]

    def create_retry_job(
        self, *, prior_job_id: str, job_id: str, idempotency_key: str,
        max_attempts: int | None = None,
    ) -> JobRuntime:
        """Create a linked new job after a terminal failure; never reuse it."""
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be at least 1.")
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                """SELECT job.job_id, job.workspace_id, job.source_file_id,
                          runtime.state, runtime.max_attempts
                   FROM ingestion_jobs AS job
                   JOIN ingestion_job_runtime AS runtime USING (job_id)
                   WHERE job.job_id = ?""",
                (prior_job_id,),
            ).fetchone()
            if prior is None:
                raise KeyError(f"Job not found: {prior_job_id}")
            if prior["state"] != IngestionJobState.FAILED_TERMINAL.value:
                raise ValueError("A linked retry requires a terminally failed job.")
            source = connection.execute(
                "SELECT state FROM source_files WHERE source_file_id = ?",
                (prior["source_file_id"],),
            ).fetchone()
            if source["state"] != "available":
                raise ValueError("The original source file is not available.")
            connection.execute(
                """INSERT INTO ingestion_jobs (
                    job_id, workspace_id, source_file_id, state, idempotency_key
                ) VALUES (?, ?, ?, 'queued', ?)""",
                (job_id, prior["workspace_id"], prior["source_file_id"], idempotency_key),
            )
            connection.execute(
                """INSERT INTO ingestion_job_runtime (
                    job_id, max_attempts, retry_of_job_id
                ) VALUES (?, ?, ?)""",
                (job_id, max_attempts or prior["max_attempts"], prior_job_id),
            )
        return self.get_job(job_id)  # type: ignore[return-value]

    def get_job(self, job_id: str) -> JobRuntime | None:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                "SELECT * FROM ingestion_job_runtime WHERE job_id = ?", (job_id,)
            ).fetchone()
        if row is None:
            return None
        return JobRuntime(
            row["job_id"], IngestionJobState(row["state"]), row["attempt_count"],
            row["max_attempts"], row["available_at"], row["lease_owner"],
            row["lease_token"], row["lease_expires_at"], row["retry_of_job_id"],
        )

    def claim_next_job(
        self, worker_id: str, *, lease_seconds: int = 60,
        now: datetime | None = None,
    ) -> JobLease | None:
        if not worker_id.strip() or len(worker_id) > 120:
            raise ValueError("worker_id must contain 1–120 nonblank characters.")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive.")
        claimed_at = _utc(now)
        lease_until = claimed_at + timedelta(seconds=lease_seconds)
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT runtime.job_id, runtime.state, runtime.attempt_count,
                          runtime.max_attempts, job.workspace_id, job.source_file_id,
                          job.idempotency_key
                   FROM ingestion_job_runtime AS runtime
                   JOIN ingestion_jobs AS job USING (job_id)
                   JOIN source_files AS source USING (source_file_id)
                   WHERE runtime.state IN ('queued', 'retry_wait')
                     AND runtime.available_at <= ? AND source.state = 'available'
                   ORDER BY runtime.available_at, runtime.created_at, runtime.job_id
                   LIMIT 1""",
                (_timestamp(claimed_at),),
            ).fetchone()
            if row is None:
                return None
            validate_transition(IngestionJobState, row["state"], IngestionJobState.RUNNING)
            attempt_number = row["attempt_count"] + 1
            token = uuid4().hex
            expiry = _timestamp(lease_until)
            cursor = connection.execute(
                """UPDATE ingestion_job_runtime
                   SET state = 'running', attempt_count = ?, lease_owner = ?,
                       lease_token = ?, lease_expires_at = ?, updated_at = ?
                   WHERE job_id = ? AND state = ?""",
                (attempt_number, worker_id, token, expiry, _timestamp(claimed_at),
                 row["job_id"], row["state"]),
            )
            if cursor.rowcount != 1:
                return None
            connection.execute(
                """UPDATE ingestion_jobs SET state = 'running', attempt_count = ?,
                   lease_expires_at = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?""",
                (attempt_number, expiry, row["job_id"]),
            )
            connection.execute(
                """INSERT INTO job_attempts (job_id, attempt_number, started_at)
                   VALUES (?, ?, ?)""",
                (row["job_id"], attempt_number, _timestamp(claimed_at)),
            )
            job = IngestionJob(
                row["job_id"], row["workspace_id"], row["source_file_id"],
                IngestionJobState.RUNNING, attempt_number, row["idempotency_key"],
            )
        return JobLease(job, worker_id, token, expiry, attempt_number)

    def renew_lease(
        self, job_id: str, lease_token: str, *, lease_seconds: int = 60,
        now: datetime | None = None,
    ) -> str:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive.")
        current = _utc(now)
        expiry = _timestamp(current + timedelta(seconds=lease_seconds))
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """UPDATE ingestion_job_runtime
                   SET lease_expires_at = ?, updated_at = ?
                   WHERE job_id = ? AND state IN ('running', 'cancel_requested')
                     AND lease_token = ? AND lease_expires_at > ?""",
                (expiry, _timestamp(current), job_id, lease_token, _timestamp(current)),
            )
            if cursor.rowcount != 1:
                raise JobLeaseError("Job lease is missing, expired, or no longer owned.")
            connection.execute(
                "UPDATE ingestion_jobs SET lease_expires_at = ? WHERE job_id = ?",
                (expiry, job_id),
            )
        return expiry

    def request_cancellation(self, job_id: str) -> JobRuntime:
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._runtime_row(connection, job_id)
            current = IngestionJobState(row["state"])
            if current == IngestionJobState.CANCEL_REQUESTED:
                return JobRuntime(
                    row["job_id"], current, row["attempt_count"], row["max_attempts"],
                    row["available_at"], row["lease_owner"], row["lease_token"],
                    row["lease_expires_at"], row["retry_of_job_id"],
                )
            if current == IngestionJobState.CANCELLED:
                return JobRuntime(
                    row["job_id"], current, row["attempt_count"], row["max_attempts"],
                    row["available_at"], row["lease_owner"], row["lease_token"],
                    row["lease_expires_at"], row["retry_of_job_id"],
                )
            target = (
                IngestionJobState.CANCEL_REQUESTED
                if current == IngestionJobState.RUNNING
                else IngestionJobState.CANCELLED
            )
            validate_transition(IngestionJobState, current, target)
            if target == IngestionJobState.CANCEL_REQUESTED:
                connection.execute(
                    "UPDATE ingestion_job_runtime SET state = ?, updated_at = ? WHERE job_id = ?",
                    (target.value, _timestamp(_utc(None)), job_id),
                )
                connection.execute(
                    "UPDATE ingestion_jobs SET state = ? WHERE job_id = ?",
                    (target.value, job_id),
                )
            else:
                connection.execute(
                    """UPDATE ingestion_job_runtime SET state = ?, lease_owner = NULL,
                       lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                       WHERE job_id = ?""",
                    (target.value, _timestamp(_utc(None)), job_id),
                )
                connection.execute(
                    "UPDATE ingestion_jobs SET state = ?, lease_expires_at = NULL WHERE job_id = ?",
                    (target.value, job_id),
                )
        return self.get_job(job_id)  # type: ignore[return-value]

    def complete_job(
        self, job_id: str, lease_token: str, *, cancelled: bool = False,
        now: datetime | None = None,
    ) -> JobRuntime:
        target = IngestionJobState.CANCELLED if cancelled else IngestionJobState.SUCCEEDED
        return self._finish_with_lease(job_id, lease_token, target, now=now)

    def fail_job(
        self, job_id: str, lease_token: str, *, error_code: str,
        error_message: str, retryable: bool,
        retry_delay_seconds: int = 30, now: datetime | None = None,
    ) -> JobRuntime:
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds cannot be negative.")
        current_time = _utc(now)
        code = _safe_error_code(error_code)
        message = _safe_error_message(error_message)
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._leased_row(connection, job_id, lease_token, current_time)
            if row["state"] == IngestionJobState.CANCEL_REQUESTED.value:
                can_retry = False
                target = IngestionJobState.CANCELLED
                outcome = "cancelled"
            else:
                can_retry = retryable and row["attempt_count"] < row["max_attempts"]
                target = (
                    IngestionJobState.RETRY_WAIT
                    if can_retry else IngestionJobState.FAILED_TERMINAL
                )
                outcome = "retryable_failure" if can_retry else "terminal_failure"
            validate_transition(IngestionJobState, row["state"], target)
            due = current_time + timedelta(seconds=retry_delay_seconds)
            connection.execute(
                """UPDATE ingestion_job_runtime SET state = ?, available_at = ?,
                   lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                   last_error_code = ?, last_error_message = ?, updated_at = ?
                   WHERE job_id = ?""",
                (target.value, _timestamp(due), code, message, _timestamp(current_time), job_id),
            )
            connection.execute(
                """UPDATE ingestion_jobs SET state = ?, lease_expires_at = NULL,
                   error_code = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE job_id = ?""",
                (target.value, code, message, job_id),
            )
            self._finish_attempt(connection, job_id, row["attempt_count"],
                                 outcome, code, message, current_time)
        return self.get_job(job_id)  # type: ignore[return-value]

    def recover_expired_leases(self, *, now: datetime | None = None) -> list[str]:
        current = _utc(now)
        stamp = _timestamp(current)
        recovered: list[str] = []
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """SELECT job_id, state, attempt_count, max_attempts FROM ingestion_job_runtime
                   WHERE state IN ('running', 'cancel_requested') AND lease_expires_at <= ?
                   ORDER BY lease_expires_at, job_id""", (stamp,),
            ).fetchall()
            for row in rows:
                if row["state"] == IngestionJobState.CANCEL_REQUESTED.value:
                    target = IngestionJobState.CANCELLED
                    outcome = "cancelled"
                elif row["attempt_count"] < row["max_attempts"]:
                    target = IngestionJobState.RETRY_WAIT
                    outcome = "retryable_failure"
                else:
                    target = IngestionJobState.RECONCILIATION_REQUIRED
                    outcome = "terminal_failure"
                validate_transition(IngestionJobState, row["state"], target)
                connection.execute(
                    """UPDATE ingestion_job_runtime SET state = ?, available_at = ?,
                       lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                       last_error_code = 'lease_expired',
                       last_error_message = 'Worker lease expired; outcome requires recovery.',
                       updated_at = ? WHERE job_id = ?""",
                    (target.value, stamp, stamp, row["job_id"]),
                )
                # The Phase 1 table cannot represent reconciliation_required. Keep its
                # conservative terminal state while runtime metadata remains authoritative.
                legacy_state = (
                    IngestionJobState.FAILED_TERMINAL.value
                    if target == IngestionJobState.RECONCILIATION_REQUIRED
                    else target.value
                )
                connection.execute(
                    """UPDATE ingestion_jobs SET state = ?, lease_expires_at = NULL,
                       error_code = 'lease_expired',
                       error_message = 'Worker lease expired.', updated_at = CURRENT_TIMESTAMP
                       WHERE job_id = ?""",
                    (legacy_state, row["job_id"]),
                )
                self._finish_attempt(
                    connection, row["job_id"], row["attempt_count"], outcome,
                    "lease_expired", "Worker lease expired; outcome requires recovery.", current,
                )
                recovered.append(row["job_id"])
        return recovered

    def _finish_with_lease(
        self, job_id: str, lease_token: str, target: IngestionJobState,
        *, now: datetime | None,
    ) -> JobRuntime:
        current = _utc(now)
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._leased_row(connection, job_id, lease_token, current)
            validate_transition(IngestionJobState, row["state"], target)
            connection.execute(
                """UPDATE ingestion_job_runtime SET state = ?, lease_owner = NULL,
                   lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                   WHERE job_id = ?""",
                (target.value, _timestamp(current), job_id),
            )
            connection.execute(
                "UPDATE ingestion_jobs SET state = ?, lease_expires_at = NULL WHERE job_id = ?",
                (target.value, job_id),
            )
            self._finish_attempt(
                connection, job_id, row["attempt_count"],
                "cancelled" if target == IngestionJobState.CANCELLED else "succeeded",
                None, None, current,
            )
        return self.get_job(job_id)  # type: ignore[return-value]

    @staticmethod
    def _runtime_row(connection: sqlite3.Connection, job_id: str):
        row = connection.execute(
            "SELECT * FROM ingestion_job_runtime WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"Job not found: {job_id}")
        return row

    @staticmethod
    def _leased_row(
        connection: sqlite3.Connection, job_id: str, token: str, now: datetime
    ):
        row = JobQueue._runtime_row(connection, job_id)
        if (
            row["state"] not in {"running", "cancel_requested"}
            or row["lease_token"] != token
            or row["lease_expires_at"] <= _timestamp(now)
        ):
            raise JobLeaseError("Job lease is missing, expired, or no longer owned.")
        return row

    @staticmethod
    def _finish_attempt(
        connection: sqlite3.Connection, job_id: str, attempt_number: int,
        outcome: str, code: str | None, message: str | None, now: datetime,
    ) -> None:
        cursor = connection.execute(
            """UPDATE job_attempts SET finished_at = ?, outcome = ?,
               error_code = ?, error_message = ?
               WHERE job_id = ? AND attempt_number = ? AND outcome IS NULL""",
            (_timestamp(now), outcome, code, message, job_id, attempt_number),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(
                f"Expected one open attempt for job '{job_id}' #{attempt_number}."
            )


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamps must be timezone-aware.")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _safe_error_code(value: str) -> str:
    clean = _ERROR_CODE.sub("_", value.strip())[:80]
    return clean or "processing_error"


def _safe_error_message(value: str) -> str:
    clean = " ".join("".join(char for char in value if char.isprintable()).split())
    return clean[:500] or "Job failed without an error message."
