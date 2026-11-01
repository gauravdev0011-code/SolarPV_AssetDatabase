"""Small SQLite repositories for ingestion metadata."""

from pathlib import Path

from solarpv_db.database import DEFAULT_DATABASE, get_connection
from solarpv_ingestion.models import (
    Approval,
    DatasetVersion,
    IngestionJob,
    PromotionOperation,
    SourceFile,
)
from solarpv_ingestion.states import (
    ApprovalState,
    DatasetVersionState,
    IngestionJobState,
    PromotionOperationState,
    SourceFileState,
)
from solarpv_ingestion.transitions import validate_transition


class ConcurrentStateChangeError(RuntimeError):
    """Raised when a state record changes before its conditional update."""


class ApprovalRevocationError(ValueError):
    """Raised when an approval is revoked after a promotion has started."""


class IngestionRepository:
    """Persist foundational metadata in the shared SQLite database."""

    def __init__(self, database_path: str | Path = DEFAULT_DATABASE) -> None:
        self.database_path = database_path

    def create_source_file(
        self,
        *,
        source_file_id: str,
        workspace_id: str,
        original_name: str,
        storage_key: str,
        media_type: str | None,
        size_bytes: int,
        sha256: str,
        state: SourceFileState = SourceFileState.RECEIVING,
    ) -> SourceFile:
        with get_connection(self.database_path) as connection:
            connection.execute(
                """INSERT INTO source_files (
                    source_file_id, workspace_id, original_name, storage_key,
                    media_type, size_bytes, sha256, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (source_file_id, workspace_id, original_name, storage_key,
                 media_type, size_bytes, sha256, state.value),
            )
        return SourceFile(source_file_id, workspace_id, original_name, storage_key,
                          media_type, size_bytes, sha256, state)

    def get_source_file(self, source_file_id: str) -> SourceFile | None:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                "SELECT * FROM source_files WHERE source_file_id = ?",
                (source_file_id,),
            ).fetchone()
        if row is None:
            return None
        return SourceFile(
            row["source_file_id"], row["workspace_id"], row["original_name"],
            row["storage_key"], row["media_type"], row["size_bytes"],
            row["sha256"], SourceFileState(row["state"]),
        )

    def transition_source_file(
        self, source_file_id: str, target: SourceFileState
    ) -> SourceFile:
        return self._transition(
            "source_files", "source_file_id", source_file_id,
            SourceFileState, target,
        )

    def create_job(
        self,
        *,
        job_id: str,
        workspace_id: str,
        source_file_id: str,
        idempotency_key: str,
    ) -> IngestionJob:
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            source = connection.execute(
                "SELECT workspace_id FROM source_files WHERE source_file_id = ?",
                (source_file_id,),
            ).fetchone()
            if source is not None and source["workspace_id"] != workspace_id:
                raise ValueError(
                    "An ingestion job and its source file must belong to the same workspace."
                )
            connection.execute(
                """INSERT INTO ingestion_jobs (
                    job_id, workspace_id, source_file_id, state, idempotency_key
                ) VALUES (?, ?, ?, ?, ?)""",
                (job_id, workspace_id, source_file_id,
                 IngestionJobState.QUEUED.value, idempotency_key),
            )
        return IngestionJob(job_id, workspace_id, source_file_id,
                            IngestionJobState.QUEUED, 0, idempotency_key)

    def get_job(self, job_id: str) -> IngestionJob | None:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                "SELECT * FROM ingestion_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        if row is None:
            return None
        return IngestionJob(row["job_id"], row["workspace_id"],
                            row["source_file_id"], IngestionJobState(row["state"]),
                            row["attempt_count"], row["idempotency_key"])

    def transition_job(
        self, job_id: str, target: IngestionJobState
    ) -> IngestionJob:
        return self._transition(
            "ingestion_jobs", "job_id", job_id, IngestionJobState, target,
            update_timestamp=True,
        )

    def create_dataset_version(
        self,
        *,
        dataset_id: str,
        dataset_version_id: str,
        name: str,
        version_number: int = 1,
    ) -> DatasetVersion:
        with get_connection(self.database_path) as connection:
            connection.execute(
                "INSERT INTO datasets (dataset_id, workspace_id, name) "
                "VALUES (?, 'default', ?)",
                (dataset_id, name),
            )
            connection.execute(
                """INSERT INTO dataset_versions (
                    dataset_version_id, dataset_id, version_number, state
                ) VALUES (?, ?, ?, ?)""",
                (dataset_version_id, dataset_id, version_number,
                 DatasetVersionState.DRAFT.value),
            )
        return DatasetVersion(dataset_version_id, dataset_id, version_number,
                              DatasetVersionState.DRAFT)

    def transition_dataset_version(
        self, dataset_version_id: str, target: DatasetVersionState
    ) -> DatasetVersion:
        return self._transition(
            "dataset_versions", "dataset_version_id", dataset_version_id,
            DatasetVersionState, target,
        )

    def link_dataset_source(
        self,
        *,
        dataset_source_id: str,
        dataset_version_id: str,
        source_file_id: str,
        artifact_id: str | None = None,
    ) -> None:
        """Record which source file and optional extraction produced a dataset."""
        with get_connection(self.database_path) as connection:
            connection.execute(
                """INSERT INTO dataset_sources (
                    dataset_source_id, dataset_version_id, source_file_id, artifact_id
                ) VALUES (?, ?, ?, ?)""",
                (dataset_source_id, dataset_version_id, source_file_id, artifact_id),
            )

    def create_validation_run(
        self,
        *,
        validation_run_id: str,
        dataset_version_id: str,
        rule_set_version: str,
        outcome: str,
    ) -> None:
        if outcome not in {"passed", "failed", "warnings"}:
            raise ValueError(f"Invalid validation outcome: {outcome}")
        with get_connection(self.database_path) as connection:
            connection.execute(
                """INSERT INTO validation_runs (
                    validation_run_id, dataset_version_id, rule_set_version, outcome
                ) VALUES (?, ?, ?, ?)""",
                (validation_run_id, dataset_version_id, rule_set_version, outcome),
            )

    def create_approval(
        self,
        *,
        approval_id: str,
        dataset_version_id: str,
        validation_run_id: str,
    ) -> Approval:
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            version = connection.execute(
                "SELECT state FROM dataset_versions WHERE dataset_version_id = ?",
                (dataset_version_id,),
            ).fetchone()
            if version is None or version["state"] != DatasetVersionState.VALID.value:
                raise ValueError("Approval requires a valid dataset version.")
            validation = connection.execute(
                """SELECT outcome FROM validation_runs
                   WHERE validation_run_id = ? AND dataset_version_id = ?""",
                (validation_run_id, dataset_version_id),
            ).fetchone()
            if validation is None or validation["outcome"] == "failed":
                raise ValueError("Approval requires a passing validation run.")
            connection.execute(
                """INSERT INTO approvals (
                    approval_id, dataset_version_id, validation_run_id, state
                ) VALUES (?, ?, ?, ?)""",
                (approval_id, dataset_version_id, validation_run_id,
                 ApprovalState.PENDING.value),
            )
        return Approval(approval_id, dataset_version_id, validation_run_id,
                        ApprovalState.PENDING)

    def transition_approval(
        self, approval_id: str, target: ApprovalState, *, decision_by: str | None = None
    ) -> Approval:
        if target == ApprovalState.APPROVED and not (decision_by and decision_by.strip()):
            raise ValueError("An approving operator must be recorded.")
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state FROM approvals WHERE approval_id = ?", (approval_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Approval not found: {approval_id}")
            current = ApprovalState(row["state"])
            validate_transition(ApprovalState, current, target)
            if target == ApprovalState.REVOKED:
                promotion = connection.execute(
                    """SELECT 1 FROM promotion_operations
                       WHERE approval_id = ? AND state <> 'cancelled' LIMIT 1""",
                    (approval_id,),
                ).fetchone()
                if promotion is not None:
                    raise ApprovalRevocationError(
                        "Approval cannot be revoked after promotion has started; "
                        "correct promoted records through a separate corrective operation."
                    )
            cursor = connection.execute(
                """UPDATE approvals SET state = ?, decision_by = ?,
                   decision_at = CURRENT_TIMESTAMP WHERE approval_id = ? AND state = ?""",
                (target.value, decision_by, approval_id, current.value),
            )
            if cursor.rowcount != 1:
                raise ConcurrentStateChangeError(
                    f"Approval '{approval_id}' changed concurrently."
                )
            result = connection.execute(
                "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,)
            ).fetchone()
        return Approval(result["approval_id"], result["dataset_version_id"],
                        result["validation_run_id"], ApprovalState(result["state"]))

    def get_promotion_operation(self, promotion_id: str) -> PromotionOperation | None:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                "SELECT * FROM promotion_operations WHERE promotion_id = ?",
                (promotion_id,),
            ).fetchone()
        if row is None:
            return None
        return PromotionOperation(
            row["promotion_id"], row["approval_id"], row["source_hash"],
            row["source_name"], PromotionOperationState(row["state"]),
            row["outcome"], row["result_json"],
        )

    def transition_promotion_operation(
        self, promotion_id: str, target: PromotionOperationState
    ) -> PromotionOperation:
        return self._transition(
            "promotion_operations", "promotion_id", promotion_id,
            PromotionOperationState, target,
        )

    def _transition(
        self,
        table: str,
        id_column: str,
        entity_id: str,
        state_type: type,
        target,
        *,
        update_timestamp: bool = False,
    ):
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"SELECT state FROM {table} WHERE {id_column} = ?", (entity_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"{table} record not found: {entity_id}")
            current = state_type(row["state"])
            validate_transition(state_type, current, target)
            if table == "promotion_operations" and target == PromotionOperationState.SUCCEEDED:
                operation = connection.execute(
                    "SELECT outcome, result_json FROM promotion_operations "
                    "WHERE promotion_id = ?",
                    (entity_id,),
                ).fetchone()
                if operation["outcome"] is None or operation["result_json"] is None:
                    raise ValueError(
                        "A promotion can succeed only with a stored outcome and result."
                    )
            timestamp_sql = ", updated_at = CURRENT_TIMESTAMP" if update_timestamp else ""
            cursor = connection.execute(
                f"UPDATE {table} SET state = ?{timestamp_sql} "
                f"WHERE {id_column} = ? AND state = ?",
                (target.value, entity_id, current.value),
            )
            if cursor.rowcount != 1:
                raise ConcurrentStateChangeError(
                    f"{table} record '{entity_id}' changed concurrently."
                )
            result = connection.execute(
                f"SELECT * FROM {table} WHERE {id_column} = ?", (entity_id,)
            ).fetchone()
        if table == "source_files":
            return SourceFile(result["source_file_id"], result["workspace_id"],
                              result["original_name"], result["storage_key"],
                              result["media_type"], result["size_bytes"],
                              result["sha256"], SourceFileState(result["state"]))
        if table == "ingestion_jobs":
            return IngestionJob(result["job_id"], result["workspace_id"],
                                result["source_file_id"], IngestionJobState(result["state"]),
                                result["attempt_count"], result["idempotency_key"])
        if table == "dataset_versions":
            return DatasetVersion(result["dataset_version_id"], result["dataset_id"],
                                  result["version_number"], DatasetVersionState(result["state"]))
        if table == "promotion_operations":
            return PromotionOperation(
                result["promotion_id"], result["approval_id"], result["source_hash"],
                result["source_name"], PromotionOperationState(result["state"]),
                result["outcome"], result["result_json"],
            )
        raise TypeError(f"Unsupported state machine table: {table}")
