"""Durable bounded file intake with cross-process recovery ownership."""

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import secrets
import sqlite3
from typing import BinaryIO
from uuid import uuid4

from solarpv_db.database import DEFAULT_DATABASE, get_connection
from solarpv_ingestion.models import IntakeResult, SourceFile, StagedFile
from solarpv_ingestion.repository import IngestionRepository
from solarpv_ingestion.states import SourceFileState
from solarpv_ingestion.storage import LocalArtifactStorage


class UnsafeUploadError(ValueError):
    """Raised when upload metadata or content is not safe to accept."""


class IntakeRecoveryRequired(RuntimeError):
    """An upload session is durable but needs a recovery pass."""

    def __init__(self, intake_id: str) -> None:
        self.intake_id = intake_id
        super().__init__(f"Upload intake '{intake_id}' is staged for recovery.")


class DuplicateContentUnavailable(RuntimeError):
    """Matching historical content exists but its original is not available."""


_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


class FileIntakeService:
    """Stream originals into private storage and publish with recoverable metadata."""

    def __init__(
        self,
        *,
        database_path: str | Path = DEFAULT_DATABASE,
        storage: LocalArtifactStorage,
        max_file_bytes: int = 100 * 1024 * 1024,
        chunk_size: int = 1024 * 1024,
        lease_seconds: int = 120,
    ) -> None:
        if min(max_file_bytes, chunk_size, lease_seconds) < 1:
            raise ValueError("File, chunk, and lease limits must be positive.")
        self.database_path = database_path
        self.repository = IngestionRepository(database_path)
        self.storage = storage
        self.max_file_bytes = max_file_bytes
        self.chunk_size = chunk_size
        self.lease_seconds = lease_seconds

    def accept(
        self,
        stream: BinaryIO,
        *,
        workspace_id: str,
        original_filename: str,
        media_type: str | None = None,
    ) -> IntakeResult:
        filename = _validate_filename(original_filename)
        if not workspace_id or not workspace_id.strip():
            raise ValueError("workspace_id is required.")

        intake_id = uuid4().hex
        owner = f"pid:{os.getpid()}"
        token = secrets.token_hex(32)
        staging_key = self.storage.staging_key(intake_id)
        storage_key = f"originals/{intake_id}"
        self._create_session(
            intake_id, workspace_id, filename, staging_key, storage_key, owner, token
        )
        staged: StagedFile | None = None
        state_is_staged = False
        preserve_stage = False
        try:
            try:
                staged = self.storage.stage_stream(
                    stream,
                    max_bytes=self.max_file_bytes,
                    chunk_size=self.chunk_size,
                    upload_id=intake_id,
                    on_progress=lambda _size: self._renew_session(intake_id, owner, token),
                )
                if staged.size_bytes == 0:
                    raise UnsafeUploadError("Empty files are not accepted.")
                detected_type = _detect_media_type(staged.path)
                self._mark_staged(
                    intake_id, owner, token, staged,
                    media_type=detected_type,
                )
                state_is_staged = True
            except BaseException as error:
                if not state_is_staged:
                    self._reject_receiving_session(intake_id, token, error)
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                raise

            try:
                result = self._finalize_staged_upload(intake_id, token)
            except DuplicateContentUnavailable:
                preserve_stage = True
                raise
            except Exception as error:
                preserve_stage = True
                raise IntakeRecoveryRequired(intake_id) from error
            return result
        finally:
            if staged is not None:
                if preserve_stage:
                    self.storage.release_staged(staged)
                else:
                    self.storage.discard(staged)

    def recover_incomplete_intakes(
        self, *, older_than_seconds: int = 24 * 60 * 60
    ) -> list[SourceFile]:
        """Recover expired staged sessions and reject expired partial streams.

        A cross-process OS lock is acquired before any stage is inspected or
        removed. An active process retains the lock even if its DB lease expires.
        """
        now = _now()
        recovered: list[SourceFile] = []
        self._reject_expired_receiving_sessions(now)

        for row in self._expired_staged_sessions(now):
            upload_id = row["intake_id"]
            lock = self.storage.acquire_upload_lock(upload_id)
            if lock is None:
                continue
            try:
                recovery_token = self._claim_staged_recovery(upload_id, now)
                if recovery_token is None:
                    continue
                try:
                    result = self._finalize_staged_upload(upload_id, recovery_token)
                    recovered.append(result.source_file)
                    if result.duplicate:
                        self.storage.delete_uncommitted(row["storage_key"])
                    self.storage._resolve_key(row["staging_key"]).unlink(missing_ok=True)
                except DuplicateContentUnavailable:
                    continue
                except Exception:
                    # The renewed session lease makes the next recovery retry safe.
                    continue
            finally:
                self.storage.release_upload_lock(upload_id)

        def authorize_delete(upload_id: str) -> bool:
            return self._authorize_stale_stage_delete(upload_id, _now())

        self.storage.cleanup_stale_staging(
            authorize_delete=authorize_delete,
            older_than_seconds=older_than_seconds,
        )
        return recovered

    def _create_session(
        self, intake_id: str, workspace_id: str, filename: str,
        staging_key: str, storage_key: str, owner: str, token: str,
    ) -> None:
        now = _timestamp(_now())
        expiry = _timestamp(_now() + timedelta(seconds=self.lease_seconds))
        with get_connection(self.database_path) as connection:
            connection.execute(
                """INSERT INTO intake_uploads (
                    intake_id, workspace_id, original_name, staging_key, storage_key,
                    state, lease_owner, lease_token, lease_expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'receiving', ?, ?, ?, ?, ?)""",
                (intake_id, workspace_id, filename, staging_key, storage_key,
                 owner, token, expiry, now, now),
            )

    def _renew_session(self, intake_id: str, owner: str, token: str) -> None:
        now = _now()
        with get_connection(self.database_path) as connection:
            cursor = connection.execute(
                """UPDATE intake_uploads SET lease_expires_at = ?, updated_at = ?
                   WHERE intake_id = ? AND state = 'receiving'
                     AND lease_owner = ? AND lease_token = ?""",
                (_timestamp(now + timedelta(seconds=self.lease_seconds)),
                 _timestamp(now), intake_id, owner, token),
            )
            if cursor.rowcount != 1:
                raise IntakeRecoveryRequired(intake_id)

    def _mark_staged(
        self, intake_id: str, owner: str, token: str, staged: StagedFile,
        *, media_type: str,
    ) -> None:
        now = _now()
        with get_connection(self.database_path) as connection:
            cursor = connection.execute(
                """UPDATE intake_uploads SET state = 'staged', size_bytes = ?,
                   sha256 = ?, media_type = ?, lease_expires_at = ?, updated_at = ?
                   WHERE intake_id = ? AND state = 'receiving'
                     AND lease_owner = ? AND lease_token = ?""",
                (staged.size_bytes, staged.sha256, media_type,
                 _timestamp(now + timedelta(seconds=self.lease_seconds)),
                 _timestamp(now), intake_id, owner, token),
            )
            if cursor.rowcount != 1:
                raise IntakeRecoveryRequired(intake_id)

    def _reject_receiving_session(
        self, intake_id: str, token: str, error: BaseException
    ) -> None:
        try:
            lock = self.storage.acquire_upload_lock(intake_id)
        except OSError:
            lock = None
        try:
            message = _safe_message(str(error))
            with get_connection(self.database_path) as connection:
                connection.execute(
                    """UPDATE intake_uploads SET state = 'rejected',
                       lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                       error_code = 'intake_rejected', error_message = ?, updated_at = ?
                       WHERE intake_id = ? AND state = 'receiving' AND lease_token = ?""",
                    (message, _timestamp(_now()), intake_id, token),
                )
            if lock is not None:
                self.storage._resolve_key(self.storage.staging_key(intake_id)).unlink(missing_ok=True)
        except Exception:
            # Recovery can safely reject this session after its lease expires.
            return
        finally:
            if lock is not None:
                self.storage.release_upload_lock(intake_id)

    def _finalize_staged_upload(self, intake_id: str, token: str) -> IntakeResult:
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            upload = connection.execute(
                "SELECT * FROM intake_uploads WHERE intake_id = ?", (intake_id,)
            ).fetchone()
            if upload is None or upload["state"] != "staged" or upload["lease_token"] != token:
                raise IntakeRecoveryRequired(intake_id)
            if not _upload_content_matches(self.storage, upload):
                connection.execute(
                    """UPDATE intake_uploads SET state = 'rejected', lease_owner = NULL,
                       lease_token = NULL, lease_expires_at = NULL,
                       error_code = 'staged_content_mismatch',
                       error_message = 'Staged bytes did not match persisted fingerprint.',
                       updated_at = ? WHERE intake_id = ?""",
                    (_timestamp(_now()), intake_id),
                )
                connection.commit()
                raise UnsafeUploadError("Staged upload failed its persisted fingerprint check.")

            existing = connection.execute(
                "SELECT * FROM source_files WHERE workspace_id = ? AND sha256 = ?",
                (upload["workspace_id"], upload["sha256"]),
            ).fetchone()
            if existing is not None:
                valid_duplicate = (
                    existing["state"] == SourceFileState.AVAILABLE.value
                    and self.storage.matches(
                        existing["storage_key"], sha256=upload["sha256"],
                        size_bytes=upload["size_bytes"],
                    )
                )
                if valid_duplicate:
                    connection.execute(
                        """UPDATE intake_uploads SET state = 'duplicate', source_file_id = ?,
                           lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                           updated_at = ? WHERE intake_id = ?""",
                        (existing["source_file_id"], _timestamp(_now()), intake_id),
                    )
                    source = _source_from_row(existing)
                    return IntakeResult(source, True)
                connection.execute(
                    """UPDATE intake_uploads SET state = 'reconciliation_required',
                       lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                       error_code = 'duplicate_source_unavailable',
                       error_message = 'Matching source exists but is not safely reusable.',
                       updated_at = ? WHERE intake_id = ?""",
                    (_timestamp(_now()), intake_id),
                )
                connection.commit()
                raise DuplicateContentUnavailable(
                    "Matching source content exists but is not safely available."
                )

            stage_key = upload["staging_key"]
            final_key = upload["storage_key"]
            stage_matches = self.storage.matches(
                stage_key, sha256=upload["sha256"], size_bytes=upload["size_bytes"]
            )
            final_exists = self.storage.exists(final_key)
            final_matches = final_exists and self.storage.matches(
                final_key, sha256=upload["sha256"], size_bytes=upload["size_bytes"]
            )
            if final_exists and not final_matches:
                connection.execute(
                    """UPDATE intake_uploads SET state = 'reconciliation_required',
                       lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                       error_code = 'storage_key_collision',
                       error_message = 'Published key contains different content.',
                       updated_at = ? WHERE intake_id = ?""",
                    (_timestamp(_now()), intake_id),
                )
                connection.commit()
                raise IntakeRecoveryRequired(intake_id)
            if not final_matches:
                if not stage_matches:
                    connection.execute(
                        """UPDATE intake_uploads SET state = 'rejected', lease_owner = NULL,
                           lease_token = NULL, lease_expires_at = NULL,
                           error_code = 'staged_file_missing',
                           error_message = 'No valid staged or published bytes remain.',
                           updated_at = ? WHERE intake_id = ?""",
                        (_timestamp(_now()), intake_id),
                    )
                    connection.commit()
                    raise UnsafeUploadError("No verified staged bytes are available to publish.")
                staged = StagedFile(
                    self.storage._resolve_key(stage_key), upload["size_bytes"],
                    upload["sha256"], intake_id,
                )
                self.storage.publish(staged, final_key)

            connection.execute(
                """INSERT INTO source_files (
                    source_file_id, workspace_id, original_name, storage_key,
                    media_type, size_bytes, sha256, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'receiving')""",
                (intake_id, upload["workspace_id"], upload["original_name"],
                 final_key, upload["media_type"], upload["size_bytes"], upload["sha256"]),
            )
            connection.execute(
                "UPDATE source_files SET state = 'quarantined' WHERE source_file_id = ?",
                (intake_id,),
            )
            connection.execute(
                "UPDATE source_files SET state = 'available' WHERE source_file_id = ?",
                (intake_id,),
            )
            connection.execute(
                """UPDATE intake_uploads SET state = 'completed', source_file_id = ?,
                   lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ? WHERE intake_id = ?""",
                (intake_id, _timestamp(_now()), intake_id),
            )
            source = connection.execute(
                "SELECT * FROM source_files WHERE source_file_id = ?", (intake_id,)
            ).fetchone()
        return IntakeResult(_source_from_row(source), False)

    def _reject_expired_receiving_sessions(self, now: datetime) -> None:
        with get_connection(self.database_path) as connection:
            rows = connection.execute(
                """SELECT intake_id FROM intake_uploads
                   WHERE state = 'receiving' AND lease_expires_at <= ?""",
                (_timestamp(now),),
            ).fetchall()
        for row in rows:
            intake_id = row["intake_id"]
            lock = self.storage.acquire_upload_lock(intake_id)
            if lock is None:
                continue
            try:
                with get_connection(self.database_path) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    cursor = connection.execute(
                        """UPDATE intake_uploads SET state = 'rejected',
                           lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                           error_code = 'abandoned_upload',
                           error_message = 'Upload lease expired before staging completed.',
                           updated_at = ?
                           WHERE intake_id = ? AND state = 'receiving'
                             AND lease_expires_at <= ?""",
                        (_timestamp(_now()), intake_id, _timestamp(_now())),
                    )
                if cursor.rowcount == 1:
                    self.storage._resolve_key(self.storage.staging_key(intake_id)).unlink(missing_ok=True)
            finally:
                self.storage.release_upload_lock(intake_id)

    def _expired_staged_sessions(self, now: datetime):
        with get_connection(self.database_path) as connection:
            return connection.execute(
                """SELECT intake_id, storage_key FROM intake_uploads
                   WHERE state = 'staged' AND lease_expires_at <= ?
                   ORDER BY lease_expires_at, intake_id""",
                (_timestamp(now),),
            ).fetchall()

    def _claim_staged_recovery(self, intake_id: str, now: datetime) -> str | None:
        token = secrets.token_hex(32)
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """UPDATE intake_uploads SET lease_owner = ?, lease_token = ?,
                   lease_expires_at = ?, updated_at = ?
                   WHERE intake_id = ? AND state = 'staged' AND lease_expires_at <= ?""",
                (f"recovery:{os.getpid()}", token,
                 _timestamp(now + timedelta(seconds=self.lease_seconds)),
                 _timestamp(now), intake_id, _timestamp(now)),
            )
            return token if cursor.rowcount == 1 else None

    def _authorize_stale_stage_delete(self, intake_id: str, now: datetime) -> bool:
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state, lease_expires_at FROM intake_uploads WHERE intake_id = ?",
                (intake_id,),
            ).fetchone()
            if row is None:
                return True
            if row["state"] in {"completed", "duplicate", "rejected"}:
                return True
            if row["state"] == "receiving" and row["lease_expires_at"] <= _timestamp(now):
                cursor = connection.execute(
                    """UPDATE intake_uploads SET state = 'rejected', lease_owner = NULL,
                       lease_token = NULL, lease_expires_at = NULL,
                       error_code = 'abandoned_upload',
                       error_message = 'Upload lease expired before staging completed.',
                       updated_at = ? WHERE intake_id = ? AND state = 'receiving'
                         AND lease_expires_at <= ?""",
                    (_timestamp(now), intake_id, _timestamp(now)),
                )
                return cursor.rowcount == 1
            return False


def _validate_filename(value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 255:
        raise UnsafeUploadError("Filename must contain 1-255 characters.")
    if value in {".", ".."} or any(char in value for char in ("/", "\\", ":")):
        raise UnsafeUploadError("Filename must be a plain name without path components.")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise UnsafeUploadError("Filename contains control characters.")
    if value[-1] in {".", " "} or value.split(".")[0].upper() in _WINDOWS_RESERVED:
        raise UnsafeUploadError("Filename is not safe on supported filesystems.")
    return value


def _detect_media_type(path: Path) -> str:
    with path.open("rb") as source:
        prefix = source.read(4096)
    executable_signatures = (
        b"MZ", b"\x7fELF", b"\xfe\xed\xfa\xce", b"\xce\xfa\xed\xfe",
        b"\xfe\xed\xfa\xcf", b"\xcf\xfa\xed\xfe", b"#!",
    )
    if prefix.startswith(executable_signatures):
        raise UnsafeUploadError("Executable or script content is not accepted.")
    if prefix.startswith(b"%PDF-"):
        return "application/pdf"
    if prefix.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if prefix.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if prefix.startswith((b"II*\x00", b"MM\x00*")):
        return "image/tiff"
    if prefix.startswith(b"SQLite format 3\x00"):
        return "application/vnd.sqlite3"
    if prefix.startswith(b"PK\x03\x04"):
        return "application/zip"
    if prefix.lstrip().startswith((b"{", b"[")):
        return "application/json"
    try:
        prefix.decode("utf-8")
    except UnicodeDecodeError:
        return "application/octet-stream"
    return "text/plain"


def _source_from_row(row) -> SourceFile:
    return SourceFile(
        row["source_file_id"], row["workspace_id"], row["original_name"],
        row["storage_key"], row["media_type"], row["size_bytes"],
        row["sha256"], SourceFileState(row["state"]),
    )


def _upload_content_matches(storage: LocalArtifactStorage, upload) -> bool:
    if storage.matches(
        upload["staging_key"], sha256=upload["sha256"], size_bytes=upload["size_bytes"]
    ):
        return True
    return storage.matches(
        upload["storage_key"], sha256=upload["sha256"], size_bytes=upload["size_bytes"]
    )


def _safe_message(value: str) -> str:
    clean = " ".join("".join(char for char in value if char.isprintable()).split())
    return clean[:500] or "Upload was rejected."


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
