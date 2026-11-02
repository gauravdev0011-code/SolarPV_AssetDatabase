"""Dataset registration, versioning, mapping, validation, and review services."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

from solarpv_db.database import DEFAULT_DATABASE, get_connection
from solarpv_ingestion.processing import InvalidInputError
from solarpv_ingestion.repository import IngestionRepository
from solarpv_ingestion.states import ApprovalState, DatasetVersionState
from solarpv_ingestion.storage import LocalArtifactStorage
from solarpv_ingestion.transitions import InvalidStateTransition
from solarpv_ingestion.validation import (
    MAX_RESULT_BYTES,
    VALIDATION_RULE_SET,
    preview_records,
    validate_records,
)


class DatasetError(ValueError):
    """Invalid dataset request or lifecycle operation."""


class DatasetNotFound(KeyError):
    """A requested dataset resource does not exist in the default workspace."""


class DatasetConflict(RuntimeError):
    """A dataset idempotency key or lifecycle precondition conflicts."""


class DatasetService:
    def __init__(
        self,
        database_path: str | Path = DEFAULT_DATABASE,
        storage: LocalArtifactStorage | None = None,
        *,
        max_json_bytes: int = 5 * 1024 * 1024,
    ) -> None:
        self.database_path = database_path
        self.storage = storage or LocalArtifactStorage(Path(database_path).parent / "private-storage")
        self.max_json_bytes = max_json_bytes
        self.repository = IngestionRepository(database_path)

    def create_dataset(self, *, name: str, job_id: str, idempotency_key: str,
                       workspace_id: str = "default") -> dict:
        name = _required_text(name, "name", 200)
        key = _required_text(idempotency_key, "idempotency_key", 200)
        request_fingerprint = _fingerprint({"operation": "create_dataset", "name": name, "job_id": job_id})
        dataset_id, version_id, request_id = uuid4().hex, uuid4().hex, uuid4().hex
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            replay = self._find_request(connection, key, request_fingerprint, workspace_id)
            if replay:
                return self._version_dict(connection, replay["dataset_version_id"], workspace_id)
            source, artifact, profile = self._processed_job(connection, job_id, workspace_id)
            cursor = connection.execute(
                "INSERT INTO datasets(dataset_id, workspace_id, name) VALUES (?, ?, ?)",
                (dataset_id, workspace_id, name),
            )
            connection.execute(
                """INSERT INTO dataset_versions (
                    dataset_version_id, dataset_id, version_number, state, artifact_key,
                    checksum, schema_version, source_job_id, source_artifact_id,
                    source_sha256, row_count, profile_json
                ) VALUES (?, ?, 1, 'draft', ?, ?, ?, ?, ?, ?, ?, ?)""",
                (version_id, dataset_id, artifact["storage_key"], source["sha256"],
                 artifact["schema_version"], job_id, artifact["artifact_id"], source["sha256"],
                 _row_count(profile), _canonical_json(profile)),
            )
            connection.execute(
                """INSERT INTO dataset_sources(
                    dataset_source_id, dataset_version_id, source_file_id, artifact_id
                ) VALUES (?, ?, ?, ?)""",
                (uuid4().hex, version_id, source["source_file_id"], artifact["artifact_id"]),
            )
            connection.execute(
                """INSERT INTO dataset_version_requests(
                    request_id, workspace_id, dataset_id, dataset_version_id,
                    job_id, idempotency_key, request_fingerprint
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (request_id, workspace_id, dataset_id, version_id, job_id, key, request_fingerprint),
            )
            connection.execute("UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE dataset_id = ?", (dataset_id,))
            return self._version_dict(connection, version_id, workspace_id)

    def create_version(self, dataset_id: str, *, job_id: str, idempotency_key: str,
                       workspace_id: str = "default") -> dict:
        key = _required_text(idempotency_key, "idempotency_key", 200)
        request_fingerprint = _fingerprint({"operation": "create_version", "dataset_id": dataset_id, "job_id": job_id})
        version_id, request_id = uuid4().hex, uuid4().hex
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            replay = self._find_request(connection, key, request_fingerprint, workspace_id)
            if replay:
                if replay["dataset_id"] != dataset_id:
                    raise DatasetConflict("Idempotency key is already used by another dataset request.")
                return self._version_dict(connection, replay["dataset_version_id"], workspace_id)
            dataset = connection.execute(
                "SELECT dataset_id FROM datasets WHERE dataset_id = ? AND workspace_id = ?",
                (dataset_id, workspace_id),
            ).fetchone()
            if dataset is None:
                raise DatasetNotFound(dataset_id)
            source, artifact, profile = self._processed_job(connection, job_id, workspace_id)
            number = connection.execute(
                "SELECT COALESCE(MAX(version_number), 0) + 1 FROM dataset_versions WHERE dataset_id = ?",
                (dataset_id,),
            ).fetchone()[0]
            connection.execute(
                """INSERT INTO dataset_versions (
                    dataset_version_id, dataset_id, version_number, state, artifact_key,
                    checksum, schema_version, source_job_id, source_artifact_id,
                    source_sha256, row_count, profile_json
                ) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?, ?)""",
                (version_id, dataset_id, number, artifact["storage_key"], source["sha256"],
                 artifact["schema_version"], job_id, artifact["artifact_id"], source["sha256"],
                 _row_count(profile), _canonical_json(profile)),
            )
            connection.execute(
                "INSERT INTO dataset_sources(dataset_source_id, dataset_version_id, source_file_id, artifact_id) VALUES (?, ?, ?, ?)",
                (uuid4().hex, version_id, source["source_file_id"], artifact["artifact_id"]),
            )
            connection.execute(
                """INSERT INTO dataset_version_requests(
                    request_id, workspace_id, dataset_id, dataset_version_id,
                    job_id, idempotency_key, request_fingerprint
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (request_id, workspace_id, dataset_id, version_id, job_id, key, request_fingerprint),
            )
            connection.execute("UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE dataset_id = ?", (dataset_id,))
            return self._version_dict(connection, version_id, workspace_id)

    def list_datasets(self, *, limit: int = 50, offset: int = 0,
                      workspace_id: str = "default") -> dict:
        _page(limit, offset)
        with get_connection(self.database_path) as connection:
            rows = connection.execute(
                """SELECT dataset.dataset_id, dataset.name, dataset.created_at, dataset.updated_at,
                          (SELECT MAX(version_number) FROM dataset_versions v
                           WHERE v.dataset_id = dataset.dataset_id) AS latest_version_number,
                          (SELECT state FROM dataset_versions v WHERE v.dataset_id = dataset.dataset_id
                           ORDER BY version_number DESC LIMIT 1) AS latest_version_state,
                          (SELECT v.version_number FROM dataset_versions v
                           JOIN approvals a ON a.dataset_version_id = v.dataset_version_id
                           WHERE v.dataset_id = dataset.dataset_id AND a.state = 'approved'
                           ORDER BY a.decision_at DESC, a.created_at DESC LIMIT 1) AS approved_version_number
                   FROM datasets dataset WHERE workspace_id = ?
                   ORDER BY created_at DESC, dataset_id LIMIT ? OFFSET ?""",
                (workspace_id, limit, offset),
            ).fetchall()
            count = connection.execute(
                "SELECT COUNT(*) FROM datasets WHERE workspace_id = ?", (workspace_id,)
            ).fetchone()[0]
        return {"items": [dict(row) for row in rows], "total": count, "limit": limit, "offset": offset}

    def get_dataset(self, dataset_id: str, *, workspace_id: str = "default") -> dict:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                "SELECT * FROM datasets WHERE dataset_id = ? AND workspace_id = ?",
                (dataset_id, workspace_id),
            ).fetchone()
            if row is None:
                raise DatasetNotFound(dataset_id)
            versions = connection.execute(
                """SELECT version.*, dataset.name, dataset.workspace_id, source.source_file_id
                   FROM dataset_versions version JOIN datasets dataset
                     ON dataset.dataset_id = version.dataset_id
                   LEFT JOIN dataset_sources source
                     ON source.dataset_version_id = version.dataset_version_id
                   WHERE version.dataset_id = ? AND dataset.workspace_id = ?
                   ORDER BY version.version_number DESC, source.dataset_source_id""",
                (dataset_id, workspace_id),
            ).fetchall()
            latest_approval = connection.execute(
                """SELECT approval.* FROM approvals approval
                   JOIN dataset_versions version ON version.dataset_version_id = approval.dataset_version_id
                   JOIN datasets dataset ON dataset.dataset_id = version.dataset_id
                   WHERE version.dataset_id = ? AND approval.state = 'approved'
                     AND dataset.workspace_id = ?
                   ORDER BY approval.decision_at DESC, approval.created_at DESC LIMIT 1""",
                (dataset_id, workspace_id),
            ).fetchone()
        return {"dataset_id": row["dataset_id"], "workspace_id": row["workspace_id"],
                "name": row["name"], "created_at": row["created_at"], "updated_at": row["updated_at"],
                "versions": [self._version_dict_from_row(v) for v in versions],
                "approved_version_id": latest_approval["dataset_version_id"] if latest_approval else None}

    def list_versions(self, dataset_id: str, *, workspace_id: str = "default") -> list[dict]:
        with get_connection(self.database_path) as connection:
            if connection.execute(
                "SELECT 1 FROM datasets WHERE dataset_id = ? AND workspace_id = ?", (dataset_id, workspace_id)
            ).fetchone() is None:
                raise DatasetNotFound(dataset_id)
            rows = connection.execute(
                "SELECT * FROM dataset_versions WHERE dataset_id = ? ORDER BY version_number DESC",
                (dataset_id,),
            ).fetchall()
        return [self._version_dict_from_row(row) for row in rows]

    def get_version(self, dataset_id: str, version_id: str, *, workspace_id: str = "default") -> dict:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                """SELECT version.*, dataset.name, dataset.workspace_id, source.source_file_id
                   FROM dataset_versions version JOIN datasets dataset
                   ON dataset.dataset_id = version.dataset_id
                   LEFT JOIN dataset_sources source
                     ON source.dataset_version_id = version.dataset_version_id
                   WHERE dataset.dataset_id = ? AND version.dataset_version_id = ?
                     AND dataset.workspace_id = ?
                   ORDER BY source.dataset_source_id LIMIT 1""", (dataset_id, version_id, workspace_id)
            ).fetchone()
            if row is None:
                raise DatasetNotFound(version_id)
            return self._version_dict_from_row(row)

    def get_profile(self, dataset_id: str, version_id: str, *, workspace_id: str = "default") -> dict:
        version = self.get_version(dataset_id, version_id, workspace_id=workspace_id)
        if version["profile"] is None:
            raise DatasetError("Dataset profile is unavailable.")
        return {"dataset_id": dataset_id, "dataset_version_id": version_id,
                "source_file_id": version["source_file_id"], "source_sha256": version["source_sha256"],
                "processing_job_id": version["source_job_id"], "profile": version["profile"]}

    def get_preview(self, dataset_id: str, version_id: str, *, limit: int = 20,
                    workspace_id: str = "default") -> dict:
        version = self.get_version(dataset_id, version_id, workspace_id=workspace_id)
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                """SELECT file.storage_key, file.original_name, file.sha256, file.size_bytes FROM dataset_sources source
                   JOIN source_files file ON file.source_file_id = source.source_file_id
                   WHERE source.dataset_version_id = ? ORDER BY source.dataset_source_id LIMIT 1""",
                (version_id,),
            ).fetchone()
        if row is None:
            raise DatasetError("Dataset source is unavailable.")
        if not self.storage.matches(row["storage_key"], sha256=row["sha256"], size_bytes=row["size_bytes"]):
            raise DatasetError("Original source does not match its recorded fingerprint.")
        preview = preview_records(self.storage, row["storage_key"], row["original_name"],
                                  limit=limit, max_json_bytes=self.max_json_bytes)
        return {"dataset_id": dataset_id, "dataset_version_id": version_id,
                "source_file_id": version["source_file_id"], "source_sha256": version["source_sha256"],
                **preview}

    def save_mapping(self, dataset_id: str, version_id: str, *, columns: list[dict],
                     idempotency_key: str, workspace_id: str = "default") -> dict:
        if not isinstance(columns, list) or len(columns) > 500:
            raise DatasetError("Mapping must be an array of at most 500 column mappings.")
        key = _required_text(idempotency_key, "idempotency_key", 200)
        normalized = [_normalize_mapping(item) for item in columns]
        if len({item["source_column"] for item in normalized}) != len(normalized):
            raise DatasetError("Each source column can be mapped only once.")
        if len({item["target_field"] for item in normalized}) != len(normalized):
            raise DatasetError("Each target field can be mapped only once per revision.")
        fingerprint = _fingerprint(normalized)
        mapping_id = uuid4().hex
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            version = self._require_version(connection, dataset_id, version_id, workspace_id)
            if version["state"] != "draft":
                raise DatasetConflict("Mappings are frozen after validation begins.")
            prior = connection.execute(
                "SELECT * FROM dataset_mapping_revisions WHERE dataset_version_id = ? AND idempotency_key = ?",
                (version_id, key),
            ).fetchone()
            if prior:
                if prior["fingerprint"] != fingerprint:
                    raise DatasetConflict("Mapping idempotency key was reused with different content.")
                return self._mapping_dict(connection, prior["mapping_revision_id"])
            revision_number = connection.execute(
                "SELECT COALESCE(MAX(revision_number), 0) + 1 FROM dataset_mapping_revisions WHERE dataset_version_id = ?",
                (version_id,),
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO dataset_mapping_revisions(mapping_revision_id, dataset_version_id, revision_number, fingerprint, idempotency_key) VALUES (?, ?, ?, ?, ?)",
                (mapping_id, version_id, revision_number, fingerprint, key),
            )
            for column in normalized:
                connection.execute(
                    """INSERT INTO dataset_column_mappings(
                        column_mapping_id, mapping_revision_id, source_column, target_field,
                        expected_type, is_required, measurement_unit, timestamp_format, timezone
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (uuid4().hex, mapping_id, column["source_column"], column["target_field"],
                     column["expected_type"], int(column["is_required"]), column["measurement_unit"],
                     column["timestamp_format"], column["timezone"]),
                )
            connection.execute(
                "UPDATE dataset_versions SET mapping_revision_id = ? WHERE dataset_version_id = ?",
                (mapping_id, version_id),
            )
            connection.execute(
                "UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE dataset_id = ?", (dataset_id,)
            )
            return self._mapping_dict(connection, mapping_id)

    def get_mapping(self, dataset_id: str, version_id: str, *, workspace_id: str = "default") -> dict | None:
        version = self.get_version(dataset_id, version_id, workspace_id=workspace_id)
        if version["mapping_revision_id"] is None:
            return None
        with get_connection(self.database_path) as connection:
            return self._mapping_dict(connection, version["mapping_revision_id"])

    def validate(self, dataset_id: str, version_id: str, *, idempotency_key: str,
                 workspace_id: str = "default") -> dict:
        key = _required_text(idempotency_key, "idempotency_key", 200)
        version = self.get_version(dataset_id, version_id, workspace_id=workspace_id)
        source = self._source_for_version(version_id, workspace_id)
        if not self.storage.matches(source["storage_key"], sha256=source["sha256"], size_bytes=source["size_bytes"]):
            raise DatasetError("Original source does not match its recorded fingerprint.")
        artifact = self.repository.get_artifact(version["source_artifact_id"])
        if artifact is None:
            raise DatasetError("Processing result metadata is unavailable.")
        result = self._read_processing_result(artifact)
        if (result.get("source", {}).get("source_file_id") != source["source_file_id"]
                or result.get("source", {}).get("sha256") != source["sha256"]
                or result.get("processing", {}).get("parser_id") != artifact.parser_id
                or result.get("processing", {}).get("parser_version") != artifact.parser_version
                or result.get("schema_version") != artifact.schema_version):
            raise DatasetError("Processing result provenance does not match its source metadata.")
        input_fingerprint = _fingerprint({
            "source_file_id": source["source_file_id"], "source_sha256": source["sha256"],
            "source_artifact_id": artifact.artifact_id,
            "processing_result_checksum": artifact.checksum,
            "result_schema_version": artifact.schema_version,
            "parser_id": artifact.parser_id,
            "parser_version": artifact.parser_version,
            "mapping_revision_id": version["mapping_revision_id"],
            "rule_set_version": VALIDATION_RULE_SET,
        })
        with get_connection(self.database_path) as connection:
            prior = connection.execute(
                "SELECT * FROM validation_runs WHERE dataset_version_id = ? AND idempotency_key = ?",
                (version_id, key),
            ).fetchone()
            if prior:
                if prior["input_fingerprint"] != input_fingerprint:
                    raise DatasetConflict("Validation idempotency key was reused with different inputs.")
                return self.get_validation(prior["validation_run_id"], workspace_id=workspace_id)
            pending = connection.execute(
                "SELECT 1 FROM approvals WHERE dataset_version_id = ? AND state = 'pending' LIMIT 1",
                (version_id,),
            ).fetchone()
        if pending:
            raise DatasetConflict("A validation result under review cannot be replaced.")
        if version["state"] not in {"draft", "validating"}:
            raise DatasetConflict("A validated dataset version is immutable; create a new version to change its inputs.")

        mappings = self._get_mapping_rows(version["mapping_revision_id"])
        try:
            report = validate_records(
                self.storage, source["storage_key"], source["original_name"], mappings,
                max_json_bytes=self.max_json_bytes,
            )
        except InvalidInputError as error:
            report = {
                "rule_set_version": VALIDATION_RULE_SET, "outcome": "failed", "row_count": 0,
                "error_count": 1, "warning_count": 0,
                "error_details_truncated": False, "warning_details_truncated": False,
                "errors": [{"row": None, "field": None, "code": "malformed_dataset", "message": str(error)[:300]}],
                "warnings": [], "limits": {"max_error_details": 1},
            }
        report.update({
            "dataset_id": dataset_id, "dataset_version_id": version_id,
            "source_file_id": source["source_file_id"], "source_sha256": source["sha256"],
            "processing_job_id": version["source_job_id"], "parser_id": artifact.parser_id,
            "parser_version": artifact.parser_version,
            "source_artifact_id": artifact.artifact_id,
            "processing_result_checksum": artifact.checksum,
            "result_schema_version": artifact.schema_version,
            "mapping_revision_id": version["mapping_revision_id"],
            "input_fingerprint": input_fingerprint,
        })
        encoded = _canonical_json(report).encode("utf-8")
        if len(encoded) > MAX_RESULT_BYTES:
            raise DatasetError("Validation report exceeds its 1 MB storage limit.")
        validation_id = uuid4().hex
        # Keep the private storage key short enough for Windows paths. The
        # immutable validation ID and database provenance provide association.
        result_key = f"dataset-validations/{validation_id}.json"
        checksum = hashlib.sha256(encoded).hexdigest()
        self.storage.put_bytes(result_key, encoded)
        try:
            with get_connection(self.database_path) as connection:
                connection.execute("BEGIN IMMEDIATE")
                current = self._require_version(connection, dataset_id, version_id, workspace_id)
                if current["state"] not in {"draft", "validating"} or current["mapping_revision_id"] != version["mapping_revision_id"]:
                    raise DatasetConflict("Dataset inputs changed during validation.")
                if connection.execute(
                    "SELECT 1 FROM approvals WHERE dataset_version_id = ? AND state = 'pending' LIMIT 1",
                    (version_id,),
                ).fetchone():
                    raise DatasetConflict("A review was opened while validation was running.")
                connection.execute(
                    """INSERT INTO validation_runs(
                        validation_run_id, dataset_version_id, rule_set_version, outcome,
                        result_key, source_sha256, parser_id, parser_version,
                        source_file_id, source_artifact_id, processing_result_checksum,
                        result_schema_version, mapping_revision_id, input_fingerprint, result_checksum,
                        error_count, warning_count, idempotency_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (validation_id, version_id, VALIDATION_RULE_SET, report["outcome"], result_key,
                     source["sha256"], artifact.parser_id, artifact.parser_version,
                     source["source_file_id"], artifact.artifact_id, artifact.checksum,
                     artifact.schema_version,
                     version["mapping_revision_id"], input_fingerprint, checksum,
                     report["error_count"], report["warning_count"], key),
                )
                if current["state"] == "draft":
                    connection.execute(
                        "UPDATE dataset_versions SET state = 'validating' WHERE dataset_version_id = ?",
                        (version_id,),
                    )
                connection.execute(
                    "UPDATE dataset_versions SET current_validation_run_id = ? WHERE dataset_version_id = ?",
                    (validation_id, version_id),
                )
                target = "invalid" if report["error_count"] else "valid"
                connection.execute(
                    "UPDATE dataset_versions SET state = ? WHERE dataset_version_id = ?",
                    (target, version_id),
                )
                connection.execute(
                    "UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE dataset_id = ?", (dataset_id,)
                )
                row = connection.execute(
                    "SELECT * FROM validation_runs WHERE validation_run_id = ?", (validation_id,)
                ).fetchone()
        except Exception:
            self.storage.delete_uncommitted(result_key)
            raise
        report["validation_run_id"] = validation_id
        report["result_checksum"] = checksum
        return report

    def get_validation(self, validation_id: str, *, workspace_id: str = "default") -> dict:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                """SELECT validation.* FROM validation_runs validation
                   JOIN dataset_versions version ON version.dataset_version_id=validation.dataset_version_id
                   JOIN datasets dataset ON dataset.dataset_id=version.dataset_id
                   WHERE validation.validation_run_id=? AND dataset.workspace_id=?""",
                (validation_id, workspace_id),
            ).fetchone()
        if row is None:
            raise DatasetNotFound(validation_id)
        if not row["result_key"] or not row["result_checksum"]:
            raise DatasetError("Validation result artifact is unavailable.")
        content = self.storage.read_bytes(row["result_key"])
        if hashlib.sha256(content).hexdigest() != row["result_checksum"]:
            raise DatasetError("Validation result failed checksum verification.")
        try:
            result = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DatasetError("Validation result artifact is malformed.") from error
        result["validation_run_id"] = validation_id
        result["result_checksum"] = row["result_checksum"]
        return result

    def create_approval(self, dataset_id: str, version_id: str, *, validation_run_id: str,
                        idempotency_key: str, workspace_id: str = "default") -> dict:
        key = _required_text(idempotency_key, "idempotency_key", 200)
        fingerprint = _fingerprint({"dataset_id": dataset_id, "version_id": version_id, "validation_run_id": validation_run_id})
        approval_id = uuid4().hex
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            version = self._require_version(connection, dataset_id, version_id, workspace_id)
            if version["state"] != "valid" or version["current_validation_run_id"] != validation_run_id:
                raise DatasetConflict("Approval requires the current validation for a valid version.")
            run = connection.execute(
                "SELECT * FROM validation_runs WHERE validation_run_id = ? AND dataset_version_id = ?",
                (validation_run_id, version_id),
            ).fetchone()
            if run is None or run["outcome"] not in {"passed", "warnings"} or run["error_count"] != 0:
                raise DatasetConflict("Validation errors must be resolved before review.")
            prior = connection.execute(
                "SELECT * FROM approvals WHERE dataset_version_id = ? AND idempotency_key = ?",
                (version_id, key),
            ).fetchone()
            if prior:
                if prior["validation_run_id"] != validation_run_id:
                    raise DatasetConflict("Approval idempotency key was reused with different validation.")
                return self._approval_dict(prior)
            connection.execute(
                """INSERT INTO approvals(
                    approval_id, dataset_version_id, validation_run_id, state, idempotency_key
                ) VALUES (?, ?, ?, 'pending', ?)""",
                (approval_id, version_id, validation_run_id, key),
            )
            row = connection.execute("SELECT * FROM approvals WHERE approval_id = ?", (approval_id,)).fetchone()
            connection.execute(
                "UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE dataset_id = ?", (dataset_id,)
            )
            return self._approval_dict(row)

    def decide_approval(self, approval_id: str, *, decision: str, reviewer_identity: str,
                        workspace_id: str = "default") -> dict:
        try:
            target = ApprovalState(decision)
        except ValueError as error:
            raise DatasetError("Decision must be approved or rejected.") from error
        if target not in {ApprovalState.APPROVED, ApprovalState.REJECTED}:
            raise DatasetError("Decision must be approved or rejected.")
        reviewer = _required_text(reviewer_identity, "reviewer_identity", 200)
        try:
            self.repository.transition_approval(
                approval_id, target, decision_by=reviewer, idempotent=True,
                workspace_id=workspace_id,
            )
        except KeyError as error:
            raise DatasetNotFound(approval_id) from error
        except (InvalidStateTransition, ValueError) as error:
            raise DatasetConflict(str(error)) from error
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                """SELECT approval.* FROM approvals approval
                   JOIN dataset_versions version ON version.dataset_version_id=approval.dataset_version_id
                   JOIN datasets dataset ON dataset.dataset_id=version.dataset_id
                   WHERE approval.approval_id=? AND dataset.workspace_id=?""",
                (approval_id, workspace_id),
            ).fetchone()
            if row is not None:
                connection.execute(
                    """UPDATE datasets SET updated_at = CURRENT_TIMESTAMP
                       WHERE dataset_id = (SELECT dataset_id FROM dataset_versions WHERE dataset_version_id = ?)""",
                    (row["dataset_version_id"],),
                )
        return self._approval_dict(row)

    def get_approval(self, approval_id: str, *, workspace_id: str = "default") -> dict:
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                """SELECT approval.* FROM approvals approval
                   JOIN dataset_versions version ON version.dataset_version_id = approval.dataset_version_id
                   JOIN datasets dataset ON dataset.dataset_id = version.dataset_id
                   WHERE approval.approval_id = ? AND dataset.workspace_id = ?""", (approval_id, workspace_id)
            ).fetchone()
        if row is None:
            raise DatasetNotFound(approval_id)
        return self._approval_dict(row)

    def _processed_job(self, connection, job_id: str, workspace_id: str):
        row = connection.execute(
            """SELECT file.*, job.state AS job_state, job.workspace_id AS job_workspace,
                      runtime.state AS runtime_state,
                      artifact.*
               FROM ingestion_jobs job
               JOIN ingestion_job_runtime runtime ON runtime.job_id = job.job_id
               JOIN source_files file ON file.source_file_id = job.source_file_id
               JOIN extraction_artifacts artifact ON artifact.job_id = job.job_id
               WHERE job.job_id = ? AND job.workspace_id = ?
                 AND artifact.artifact_type = 'processing_result'
               ORDER BY artifact.created_at DESC LIMIT 1""", (job_id, workspace_id)
        ).fetchone()
        if (row is None or row["job_state"] != "succeeded" or row["runtime_state"] != "succeeded"
                or row["state"] != "available" or row["job_workspace"] != workspace_id
                or row["workspace_id"] != workspace_id):
            raise DatasetError("Dataset registration requires a successful job and available source in this workspace.")
        source = connection.execute("SELECT * FROM source_files WHERE source_file_id = ?", (row["source_file_id"],)).fetchone()
        artifact = connection.execute("SELECT * FROM extraction_artifacts WHERE artifact_id = ?", (row["artifact_id"],)).fetchone()
        if not self.storage.matches(source["storage_key"], sha256=source["sha256"], size_bytes=source["size_bytes"]):
            raise DatasetError("Original source does not match its recorded fingerprint.")
        result = self._read_processing_result(artifact)
        if (result.get("source", {}).get("source_file_id") != source["source_file_id"]
                or result.get("source", {}).get("sha256") != source["sha256"]
                or result.get("processing", {}).get("parser_id") != artifact["parser_id"]
                or result.get("processing", {}).get("parser_version") != artifact["parser_version"]
                or result.get("schema_version") != artifact["schema_version"]
                or result.get("source", {}).get("size_bytes") != source["size_bytes"]):
            raise DatasetError("Processing result provenance does not match its source metadata.")
        return source, artifact, result["profile"]

    def _read_processing_result(self, artifact) -> dict:
        try:
            content = self.storage.read_bytes(artifact["storage_key"] if isinstance(artifact, sqlite3.Row) else artifact.storage_key)
            checksum = artifact["checksum"] if isinstance(artifact, sqlite3.Row) else artifact.checksum
            if hashlib.sha256(content).hexdigest() != checksum:
                raise DatasetError("Processing result checksum verification failed.")
            result = json.loads(content.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DatasetError("Processing result is unavailable or malformed.") from error
        if not isinstance(result, dict) or not isinstance(result.get("profile"), dict):
            raise DatasetError("Processing result does not contain a valid profile.")
        return result

    def _find_request(self, connection, key: str, fingerprint: str, workspace_id: str):
        row = connection.execute(
            "SELECT * FROM dataset_version_requests WHERE workspace_id = ? AND idempotency_key = ?",
            (workspace_id, key),
        ).fetchone()
        if row and row["request_fingerprint"] != fingerprint:
            raise DatasetConflict("Dataset idempotency key was reused with different request content.")
        return row

    def _require_version(self, connection, dataset_id: str, version_id: str,
                         workspace_id: str = "default"):
        row = connection.execute(
            """SELECT version.*, dataset.workspace_id FROM dataset_versions version
               JOIN datasets dataset ON dataset.dataset_id = version.dataset_id
               WHERE version.dataset_version_id = ? AND version.dataset_id = ?
                 AND dataset.workspace_id = ?""", (version_id, dataset_id, workspace_id)
        ).fetchone()
        if row is None:
            raise DatasetNotFound(version_id)
        return row

    def _version_dict(self, connection, version_id: str,
                      workspace_id: str = "default") -> dict:
        row = connection.execute(
            """SELECT version.*, dataset.name, dataset.workspace_id, source.source_file_id
               FROM dataset_versions version JOIN datasets dataset ON dataset.dataset_id = version.dataset_id
               LEFT JOIN dataset_sources source ON source.dataset_version_id = version.dataset_version_id
               WHERE version.dataset_version_id = ? AND dataset.workspace_id = ?
               ORDER BY source.dataset_source_id LIMIT 1""",
            (version_id, workspace_id),
        ).fetchone()
        if row is None:
            raise DatasetNotFound(version_id)
        return self._version_dict_from_row(row)

    @staticmethod
    def _version_dict_from_row(row) -> dict:
        profile = None
        if row["profile_json"]:
            try:
                profile = json.loads(row["profile_json"])
            except json.JSONDecodeError:
                profile = None
        return {"dataset_id": row["dataset_id"], "dataset_version_id": row["dataset_version_id"],
                "workspace_id": row["workspace_id"], "name": row["name"],
                "version_number": row["version_number"], "state": row["state"],
                "source_file_id": row["source_file_id"], "source_job_id": row["source_job_id"],
                "source_artifact_id": row["source_artifact_id"], "source_sha256": row["source_sha256"],
                "row_count": row["row_count"], "schema_version": row["schema_version"],
                "mapping_revision_id": row["mapping_revision_id"],
                "current_validation_run_id": row["current_validation_run_id"],
                "profile": profile, "created_at": row["created_at"]}

    @staticmethod
    def _mapping_dict(connection, mapping_id: str) -> dict:
        revision = connection.execute(
            "SELECT * FROM dataset_mapping_revisions WHERE mapping_revision_id = ?", (mapping_id,)
        ).fetchone()
        columns = connection.execute(
            "SELECT source_column, target_field, expected_type, is_required, measurement_unit, timestamp_format, timezone FROM dataset_column_mappings WHERE mapping_revision_id = ? ORDER BY source_column",
            (mapping_id,),
        ).fetchall()
        return {"mapping_revision_id": revision["mapping_revision_id"],
                "dataset_version_id": revision["dataset_version_id"],
                "revision_number": revision["revision_number"], "fingerprint": revision["fingerprint"],
                "columns": [dict(item) for item in columns]}

    def _get_mapping_rows(self, mapping_id: str | None) -> list[dict]:
        if mapping_id is None:
            return []
        with get_connection(self.database_path) as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM dataset_column_mappings WHERE mapping_revision_id = ? ORDER BY source_column",
                (mapping_id,),
            ).fetchall()]

    def _source_for_version(self, version_id: str, workspace_id: str = "default"):
        with get_connection(self.database_path) as connection:
            row = connection.execute(
                """SELECT file.* FROM dataset_sources source
                   JOIN source_files file ON file.source_file_id = source.source_file_id
               WHERE source.dataset_version_id = ? AND file.workspace_id = ?
               ORDER BY source.dataset_source_id LIMIT 1""",
            (version_id, workspace_id),
            ).fetchone()
        if row is None:
            raise DatasetError("Dataset source is unavailable.")
        return row

    @staticmethod
    def _validation_result(row) -> dict:
        return {"validation_run_id": row["validation_run_id"], "dataset_version_id": row["dataset_version_id"],
                "rule_set_version": row["rule_set_version"], "outcome": row["outcome"],
                "source_sha256": row["source_sha256"], "parser_id": row["parser_id"],
                "parser_version": row["parser_version"], "mapping_revision_id": row["mapping_revision_id"],
                "source_file_id": row["source_file_id"], "source_artifact_id": row["source_artifact_id"],
                "processing_result_checksum": row["processing_result_checksum"],
                "result_schema_version": row["result_schema_version"],
                "input_fingerprint": row["input_fingerprint"], "result_checksum": row["result_checksum"],
                "error_count": row["error_count"], "warning_count": row["warning_count"]}

    @staticmethod
    def _approval_dict(row) -> dict:
        return {"approval_id": row["approval_id"], "dataset_version_id": row["dataset_version_id"],
                "validation_run_id": row["validation_run_id"], "state": row["state"],
                "reviewer_identity": row["decision_by"], "decision_at": row["decision_at"],
                "created_at": row["created_at"]}


def _required_text(value, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum:
        raise DatasetError(f"{field} must be a non-empty string of at most {maximum} characters.")
    return value.strip()


def _normalize_mapping(value: dict) -> dict:
    if not isinstance(value, dict):
        raise DatasetError("Each mapping must be an object.")
    source = _required_text(value.get("source_column"), "source_column", 500)
    target = _required_text(value.get("target_field"), "target_field", 200)
    expected = value.get("expected_type")
    if expected not in {"string", "integer", "number", "boolean", "date", "datetime"}:
        raise DatasetError("expected_type must be string, integer, number, boolean, date, or datetime.")
    required = value.get("is_required", False)
    if not isinstance(required, bool):
        raise DatasetError("is_required must be a boolean.")
    optional = {}
    for field, max_len in (("measurement_unit", 100), ("timestamp_format", 100), ("timezone", 100)):
        item = value.get(field)
        if item is not None and (not isinstance(item, str) or len(item) > max_len):
            raise DatasetError(f"{field} must be a string of at most {max_len} characters.")
        optional[field] = item.strip() if isinstance(item, str) and item.strip() else None
    if expected != "datetime" and (optional["timestamp_format"] or optional["timezone"]):
        raise DatasetError("Timestamp format and timezone apply only to datetime mappings.")
    return {"source_column": source, "target_field": target, "expected_type": expected,
            "is_required": required, **optional}


def _fingerprint(value: dict | list) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _row_count(profile: dict) -> int | None:
    value = profile.get("row_count")
    if value is None:
        value = profile.get("record_count")
    return value if isinstance(value, int) and value >= 0 else None


def _page(limit: int, offset: int) -> None:
    if not isinstance(limit, int) or not 1 <= limit <= 100 or not isinstance(offset, int) or offset < 0:
        raise DatasetError("limit must be 1–100 and offset must be nonnegative.")
