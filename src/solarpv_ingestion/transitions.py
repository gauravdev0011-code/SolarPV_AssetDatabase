"""Validation for independent ingestion state machines."""

from enum import StrEnum

from solarpv_ingestion.states import (
    ApprovalState,
    DatasetVersionState,
    IngestionJobState,
    PromotionOperationState,
    SourceFileState,
)


class InvalidStateTransition(ValueError):
    """Raised when a lifecycle state change is not permitted."""


TRANSITIONS: dict[type[StrEnum], dict[StrEnum, frozenset[StrEnum]]] = {
    SourceFileState: {
        SourceFileState.RECEIVING: frozenset({SourceFileState.QUARANTINED, SourceFileState.INTAKE_REJECTED}),
        SourceFileState.QUARANTINED: frozenset({SourceFileState.AVAILABLE, SourceFileState.INTAKE_REJECTED}),
        SourceFileState.AVAILABLE: frozenset({SourceFileState.DELETION_PENDING}),
        SourceFileState.DELETION_PENDING: frozenset({SourceFileState.DELETED}),
    },
    IngestionJobState: {
        IngestionJobState.QUEUED: frozenset({IngestionJobState.RUNNING, IngestionJobState.CANCELLED}),
        IngestionJobState.RUNNING: frozenset({IngestionJobState.SUCCEEDED, IngestionJobState.RETRY_WAIT, IngestionJobState.FAILED_TERMINAL, IngestionJobState.CANCEL_REQUESTED, IngestionJobState.RECONCILIATION_REQUIRED}),
        IngestionJobState.RETRY_WAIT: frozenset({IngestionJobState.QUEUED, IngestionJobState.RUNNING, IngestionJobState.CANCELLED}),
        IngestionJobState.CANCEL_REQUESTED: frozenset({IngestionJobState.CANCELLED, IngestionJobState.SUCCEEDED, IngestionJobState.FAILED_TERMINAL}),
        IngestionJobState.RECONCILIATION_REQUIRED: frozenset(),
    },
    DatasetVersionState: {
        DatasetVersionState.DRAFT: frozenset({DatasetVersionState.VALIDATING, DatasetVersionState.ARCHIVED}),
        DatasetVersionState.VALIDATING: frozenset({DatasetVersionState.VALID, DatasetVersionState.INVALID}),
        DatasetVersionState.VALID: frozenset({DatasetVersionState.ARCHIVED}),
        DatasetVersionState.INVALID: frozenset({DatasetVersionState.ARCHIVED}),
    },
    ApprovalState: {
        ApprovalState.PENDING: frozenset({ApprovalState.APPROVED, ApprovalState.REJECTED, ApprovalState.CANCELLED}),
        ApprovalState.APPROVED: frozenset({ApprovalState.REVOKED}),
    },
    PromotionOperationState: {
        PromotionOperationState.QUEUED: frozenset({PromotionOperationState.RUNNING, PromotionOperationState.CANCELLED, PromotionOperationState.RETRYABLE_FAILURE, PromotionOperationState.TERMINAL_FAILURE, PromotionOperationState.RECONCILIATION_REQUIRED}),
        PromotionOperationState.RUNNING: frozenset({PromotionOperationState.SUCCEEDED, PromotionOperationState.RETRYABLE_FAILURE, PromotionOperationState.TERMINAL_FAILURE, PromotionOperationState.RECONCILIATION_REQUIRED}),
        PromotionOperationState.RETRYABLE_FAILURE: frozenset({PromotionOperationState.QUEUED}),
        PromotionOperationState.RECONCILIATION_REQUIRED: frozenset({PromotionOperationState.SUCCEEDED, PromotionOperationState.RETRYABLE_FAILURE, PromotionOperationState.TERMINAL_FAILURE}),
    },
}


def validate_transition(
    state_type: type[StrEnum], current: StrEnum | str, target: StrEnum | str
) -> None:
    """Raise a useful error unless target is a valid next state."""
    try:
        current_state = state_type(current)
        target_state = state_type(target)
    except ValueError as error:
        raise InvalidStateTransition(str(error)) from error

    allowed = TRANSITIONS[state_type].get(current_state, frozenset())
    if target_state not in allowed:
        raise InvalidStateTransition(
            f"Cannot transition {state_type.__name__} from "
            f"'{current_state.value}' to '{target_state.value}'."
        )
