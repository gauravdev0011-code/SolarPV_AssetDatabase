"""Typed records representing persisted ingestion metadata."""

from dataclasses import dataclass

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

