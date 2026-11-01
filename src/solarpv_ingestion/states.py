"""Persistent ingestion lifecycle states and allowed transitions."""

from enum import StrEnum


class SourceFileState(StrEnum):
    RECEIVING = "receiving"
    QUARANTINED = "quarantined"
    AVAILABLE = "available"
    INTAKE_REJECTED = "intake_rejected"
    DELETION_PENDING = "deletion_pending"
    DELETED = "deleted"


class IngestionJobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED_TERMINAL = "failed_terminal"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"


class DatasetVersionState(StrEnum):
    DRAFT = "draft"
    VALIDATING = "validating"
    VALID = "valid"
    INVALID = "invalid"
    ARCHIVED = "archived"


class ApprovalState(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    REVOKED = "revoked"


class PromotionOperationState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"


class PromotionOutcome(StrEnum):
    IMPORTED = "imported"
    ALREADY_IMPORTED = "already_imported"
