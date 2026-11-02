"""Typed records representing persisted ingestion metadata."""

from dataclasses import dataclass
from pathlib import Path

from solarpv_ingestion.states import (
    ApprovalState,
    DatasetVersionState,
    IngestionJobState,
    PromotionOperationState,
    SourceFileState,
)


@dataclass(frozen=True, slots=True)
class SourceFile:
    source_file_id: str
    workspace_id: str
    original_name: str
    storage_key: str
    media_type: str | None
    size_bytes: int
    sha256: str
    state: SourceFileState


@dataclass(frozen=True, slots=True)
class IngestionJob:
    job_id: str
    workspace_id: str
    source_file_id: str
    state: IngestionJobState
    attempt_count: int
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class JobLease:
    job: IngestionJob
    lease_owner: str
    lease_token: str
    lease_expires_at: str
    attempt_number: int


@dataclass(frozen=True, slots=True)
class JobRuntime:
    job_id: str
    state: IngestionJobState
    attempt_count: int
    max_attempts: int
    available_at: str
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: str | None
    retry_of_job_id: str | None


@dataclass(frozen=True, slots=True)
class IntakeResult:
    source_file: SourceFile
    duplicate: bool


@dataclass(frozen=True, slots=True)
class IntakeUpload:
    intake_id: str
    workspace_id: str
    original_name: str
    staging_key: str
    storage_key: str
    state: str
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: str | None
    size_bytes: int
    sha256: str | None
    media_type: str | None
    source_file_id: str | None


@dataclass(frozen=True, slots=True)
class StagedFile:
    path: Path
    size_bytes: int
    sha256: str
    upload_id: str | None = None


@dataclass(frozen=True, slots=True)
class ExtractionArtifact:
    artifact_id: str
    workspace_id: str
    source_file_id: str
    job_id: str
    artifact_type: str
    storage_key: str
    parser_id: str
    parser_version: str
    schema_version: str
    checksum: str
    created_at: str


@dataclass(frozen=True, slots=True)
class JobStatus:
    job_id: str
    workspace_id: str
    source_file_id: str
    state: IngestionJobState
    attempt_count: int
    max_attempts: int
    available_at: str
    error_code: str | None
    error_message: str | None
    progress_stage: str | None
    artifact_id: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class DatasetVersion:
    dataset_version_id: str
    dataset_id: str
    version_number: int
    state: DatasetVersionState


@dataclass(frozen=True, slots=True)
class Approval:
    approval_id: str
    dataset_version_id: str
    validation_run_id: str
    state: ApprovalState


@dataclass(frozen=True, slots=True)
class PromotionOperation:
    promotion_id: str
    approval_id: str
    source_hash: str
    source_name: str
    state: PromotionOperationState
    outcome: str | None
    result_json: str | None

