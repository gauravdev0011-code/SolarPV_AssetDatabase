
"""Validate and import solar PV asset data from JSON or CSV files."""

import csv
import hashlib
import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path

from solarpv_db.database import DEFAULT_DATABASE, get_connection
from solarpv_ingestion.states import PromotionOperationState
from solarpv_ingestion.transitions import validate_transition


VALID_DATA_ORIGINS = {"real", "synthetic", "unknown"}
VALID_STATUSES = {"planned", "in_progress", "completed", "cancelled"}
VALID_DATA_QUALITIES = {"good", "suspect", "invalid", "unknown"}

CSV_FILES = (
    "assets.csv",
    "inspections.csv",
    "observations.csv",
    "maintenance.csv",
)

REQUIRED_SECTIONS = {
    "assets",
    "inspections",
    "observations",
    "maintenance",
}


def _read_csv_file(path):
    """Read CSV records, converting empty cells to None."""
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        if not reader.fieldnames:
            raise ValueError(f"CSV file has no header: {path.name}")

        return [
            {
                key: value if value != "" else None
                for key, value in row.items()
            }
            for row in reader
        ]


def _read_source(source_path):
    """Read a JSON file or a folder containing the four required CSV files."""
    source = Path(source_path)

    if not source.exists():
        raise FileNotFoundError(f"Import source does not exist: {source}")

    if source.is_file():
        if source.suffix.lower() != ".json":
            raise ValueError("A file import source must be a JSON file.")

        raw_bytes = source.read_bytes()

        try:
            data = json.loads(raw_bytes.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                f"Could not read valid JSON from {source.name}."
            ) from error

        return data, hashlib.sha256(raw_bytes).hexdigest(), source.name

    if not source.is_dir():
        raise ValueError("Import source must be a JSON file or a CSV folder.")

    expected_files = set(CSV_FILES)
    actual_csv_files = {
        path.name
        for path in source.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    }

    missing_files = expected_files - actual_csv_files
    extra_files = actual_csv_files - expected_files

    if missing_files or extra_files:
        problems = []

        if missing_files:
            problems.append(
                f"missing files: {', '.join(sorted(missing_files))}"
            )

        if extra_files:
            problems.append(
                f"unexpected CSV files: {', '.join(sorted(extra_files))}"
            )

        raise ValueError("Invalid CSV folder; " + "; ".join(problems) + ".")

    data = {
        "assets": _read_csv_file(source / "assets.csv"),
        "inspections": _read_csv_file(source / "inspections.csv"),
        "observations": _read_csv_file(source / "observations.csv"),
        "maintenance": _read_csv_file(source / "maintenance.csv"),
    }

    digest = hashlib.sha256()

    for filename in sorted(CSV_FILES):
        digest.update(filename.encode("utf-8"))
        digest.update(b"\0")
        digest.update((source / filename).read_bytes())
        digest.update(b"\0")

    return data, digest.hexdigest(), source.name


def _required_text(record, field, context):
    """Return a required, non-empty string."""
    value = record.get(field)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: '{field}' is required.")

    return value.strip()


def _optional_text(record, field):
    """Return trimmed optional text, or None when it is empty."""
    value = record.get(field)

    if value is None:
        return None

    if not isinstance(value, str):
        value = str(value)

    value = value.strip()
    return value if value else None


def _validate_timestamp(value, context, field):
    """Validate a non-empty ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: '{field}' is required.")

    try:
        datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            f"{context}: '{field}' must be a valid ISO 8601 timestamp."
        ) from error

    return value.strip()


def _validate_records(data):
    """Validate all records and references before starting database writes."""
    if not isinstance(data, dict):
        raise ValueError("Import data must be a JSON object or CSV dataset.")

    if set(data) != REQUIRED_SECTIONS:
        missing = REQUIRED_SECTIONS - set(data)
        extra = set(data) - REQUIRED_SECTIONS
        problems = []

        if missing:
            problems.append(f"missing sections: {', '.join(sorted(missing))}")

        if extra:
            problems.append(f"unexpected sections: {', '.join(sorted(extra))}")

        raise ValueError("Invalid dataset; " + "; ".join(problems) + ".")

    for section in REQUIRED_SECTIONS:
        if not isinstance(data[section], list):
            raise ValueError(
                f"Section '{section}' must contain a list of records."
            )

        for index, record in enumerate(data[section], start=1):
            if not isinstance(record, dict):
                raise ValueError(
                    f"Section '{section}', record {index}, must be an object."
                )

    asset_ids = set()

    for index, asset in enumerate(data["assets"], start=1):
        context = f"Asset record {index}"

        external_id = _required_text(asset, "external_id", context)
        _required_text(asset, "asset_type", context)
        _required_text(asset, "name", context)
        data_origin = asset.get("data_origin", "unknown")
        if data_origin not in VALID_DATA_ORIGINS:
            raise ValueError(f"{context}: invalid data_origin '{data_origin}'.")

        if external_id in asset_ids:
            raise ValueError(
                f"{context}: duplicate external_id '{external_id}'."
            )

        asset_ids.add(external_id)

    inspection_keys = set()

    for index, inspection in enumerate(data["inspections"], start=1):
        context = f"Inspection record {index}"

        inspection_key = _required_text(
            inspection, "inspection_key", context
        )
        asset_external_id = _required_text(
            inspection, "asset_external_id", context
        )

        if inspection_key in inspection_keys:
            raise ValueError(
                f"{context}: duplicate inspection_key '{inspection_key}'."
            )

        if asset_external_id not in asset_ids:
            raise ValueError(
                f"{context}: unknown asset_external_id "
                f"'{asset_external_id}'."
            )

        inspection_keys.add(inspection_key)

        _validate_timestamp(
            inspection.get("inspected_at"), context, "inspected_at"
        )
        _required_text(inspection, "inspection_method", context)
        _required_text(inspection, "data_source", context)

        data_origin = _required_text(inspection, "data_origin", context)
        if data_origin not in VALID_DATA_ORIGINS:
            raise ValueError(
                f"{context}: invalid data_origin '{data_origin}'."
            )

        status = _required_text(inspection, "status", context)
        if status not in VALID_STATUSES:
            raise ValueError(f"{context}: invalid status '{status}'.")

    for index, observation in enumerate(data["observations"], start=1):
        context = f"Observation record {index}"

        inspection_key = _required_text(
            observation, "inspection_key", context
        )

        if inspection_key not in inspection_keys:
            raise ValueError(
                f"{context}: unknown inspection_key '{inspection_key}'."
            )

        _required_text(observation, "observation_type", context)

        raw_value = observation.get("value")
        value = None

        if raw_value is not None and str(raw_value).strip():
            try:
                value = float(raw_value)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"{context}: 'value' must be a number or empty."
                ) from error

            if not math.isfinite(value):
                raise ValueError(
                    f"{context}: 'value' must be finite; "
                    "NaN and infinity are invalid."
                )

        unit = _optional_text(observation, "unit")
        finding = _optional_text(observation, "finding")

        if value is None and finding is None:
            raise ValueError(
                f"{context}: provide a measurement value or a finding."
            )

        if value is not None and unit is None:
            raise ValueError(
                f"{context}: 'unit' is required when a value is provided."
            )

        data_quality = _required_text(
            observation, "data_quality", context
        )
        if data_quality not in VALID_DATA_QUALITIES:
            raise ValueError(
                f"{context}: invalid data_quality '{data_quality}'."
            )

    for index, maintenance in enumerate(data["maintenance"], start=1):
        context = f"Maintenance record {index}"

        asset_external_id = _required_text(
            maintenance, "asset_external_id", context
        )

        if asset_external_id not in asset_ids:
            raise ValueError(
                f"{context}: unknown asset_external_id "
                f"'{asset_external_id}'."
            )

        _validate_timestamp(
            maintenance.get("performed_at"), context, "performed_at"
        )
        _required_text(maintenance, "maintenance_type", context)
        _required_text(maintenance, "description", context)

        data_origin = _required_text(maintenance, "data_origin", context)
        if data_origin not in VALID_DATA_ORIGINS:
            raise ValueError(
                f"{context}: invalid data_origin '{data_origin}'."
            )


def _promotion_request_fingerprint(
    *,
    source_hash: str,
    source_name: str,
    approval_id: str,
    dataset_id: str,
    dataset_version_id: str,
) -> str:
    """Hash all source and approval identity fields that affect promotion."""
    identity = {
        "approval_id": approval_id,
        "dataset_id": dataset_id,
        "dataset_version_id": dataset_version_id,
        "source_hash": source_hash,
        "source_name": source_name,
    }
    canonical = json.dumps(
        identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _record_promotion_failure(
    database_path: str | Path,
    promotion_id: str,
    error: sqlite3.Error,
) -> dict[str, str | int] | None:
    """Record a safe failure after the attempted transaction has rolled back.

    Error details are intentionally fixed, bounded messages; raw SQLite
    exception text can disclose paths or submitted values.
    """
    if isinstance(error, sqlite3.IntegrityError):
        target = PromotionOperationState.TERMINAL_FAILURE
        error_code = "constraint_violation"
        error_message = "The promotion violated a database integrity constraint."
    elif isinstance(error, sqlite3.OperationalError):
        code = getattr(error, "sqlite_errorcode", None)
        base_code = code & 0xFF if code is not None else None
        if base_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            target = PromotionOperationState.RETRYABLE_FAILURE
            error_code = "database_busy"
            error_message = "The database was busy; retry the same promotion ID."
        else:
            target = PromotionOperationState.RECONCILIATION_REQUIRED
            error_code = "transaction_outcome_uncertain"
            error_message = "The promotion outcome needs reconciliation before retry."
    else:
        target = PromotionOperationState.RECONCILIATION_REQUIRED
        error_code = "transaction_outcome_uncertain"
        error_message = "The promotion outcome needs reconciliation before retry."

    try:
        with get_connection(database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = connection.execute(
                "SELECT state, result_json FROM promotion_operations "
                "WHERE promotion_id = ?",
                (promotion_id,),
            ).fetchone()
            if operation is None:
                connection.commit()
                return None
            state = PromotionOperationState(operation["state"])
            if state == PromotionOperationState.SUCCEEDED:
                connection.commit()
                return json.loads(operation["result_json"])
            if state == PromotionOperationState.RUNNING:
                target = PromotionOperationState.RECONCILIATION_REQUIRED
                error_code = "interrupted_transaction"
                error_message = "A prior promotion attempt needs reconciliation."
            elif state != PromotionOperationState.QUEUED:
                connection.commit()
                return None
            validate_transition(PromotionOperationState, state, target)
            connection.execute(
                """UPDATE promotion_operations
                   SET state = ?, error_code = ?, error_message = ?
                   WHERE promotion_id = ? AND state = ?""",
                (target.value, error_code, error_message, promotion_id, state.value),
            )
            connection.commit()
    except (sqlite3.Error, ValueError):
        # Preserve the original import error. A later retry will reconcile the
        # durable operation state before attempting writes again.
        return None
    return None

def import_data(
    source_path: str | Path,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    promotion_id: str | None = None,
    approval_id: str | None = None,
    workspace_id: str | None = None,
) -> dict[str, str | int]:
    """Import legacy data or execute an approved, idempotent promotion."""
    try:
        return _import_data(
            source_path,
            database_path,
            promotion_id=promotion_id,
            approval_id=approval_id,
            workspace_id=workspace_id,
        )
    except sqlite3.Error as error:
        if promotion_id is not None:
            recovered_result = _record_promotion_failure(
                database_path, promotion_id, error
            )
            if recovered_result is not None:
                return recovered_result
        raise


def _import_data(
    source_path: str | Path,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    promotion_id: str | None = None,
    approval_id: str | None = None,
    workspace_id: str | None = None,
) -> dict[str, str | int]:
    """Validate and transactionally import JSON or CSV data.

    Identical source content is skipped on subsequent imports. If any database
    insert fails, all database changes from that import are rolled back.
    """
    if promotion_id is not None:
        if not isinstance(promotion_id, str) or not promotion_id.strip():
            raise ValueError("Promotion ID must be a non-empty string.")
        promotion_id = promotion_id.strip()
        if not isinstance(approval_id, str) or not approval_id.strip():
            raise ValueError("An approved approval ID is required for promotion.")
        approval_id = approval_id.strip()
    elif approval_id is not None:
        raise ValueError("Approval ID can only be supplied with a promotion ID.")
    requested_workspace_id = workspace_id
    if workspace_id is None and promotion_id is None:
        workspace_id = "default"
    if workspace_id is not None:
        if not isinstance(workspace_id, str) or not workspace_id.strip():
            raise ValueError("Workspace ID must be a non-empty string.")
        workspace_id = workspace_id.strip()

    data, source_hash, source_name = _read_source(source_path)
    _validate_records(data)

    with get_connection(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS imported_files (
                workspace_id TEXT NOT NULL DEFAULT 'default'
                    REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
                source_hash TEXT NOT NULL,
                source_name TEXT NOT NULL,
                imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (workspace_id, source_hash)
            )
            """
        )
        connection.commit()

        if promotion_id is not None:
            connection.execute("BEGIN IMMEDIATE")
            existing_promotion = connection.execute(
                "SELECT operation.source_hash, operation.approval_id, operation.source_name, "
                "operation.dataset_id, operation.dataset_version_id, operation.request_fingerprint, "
                "operation.state, operation.result_json, operation.outcome, operation.attempt_count, "
                "operation.already_imported_before, dataset.workspace_id "
                "FROM promotion_operations AS operation LEFT JOIN datasets AS dataset "
                "ON dataset.dataset_id = operation.dataset_id WHERE operation.promotion_id = ?",
                (promotion_id,),
            ).fetchone()
            if existing_promotion is not None:
                if (
                    existing_promotion["source_hash"] != source_hash
                    or existing_promotion["approval_id"] != approval_id
                    or existing_promotion["source_name"] != source_name
                    or (requested_workspace_id is not None
                        and existing_promotion["workspace_id"] != requested_workspace_id)
                ):
                    raise ValueError(
                        "Promotion ID conflict: source, source name, or approval "
                        "differs from the original request."
                    )
                state = PromotionOperationState(existing_promotion["state"])
                if existing_promotion["request_fingerprint"] is not None:
                    expected_fingerprint = _promotion_request_fingerprint(
                        source_hash=source_hash,
                        source_name=source_name,
                        approval_id=approval_id,
                        dataset_id=existing_promotion["dataset_id"],
                        dataset_version_id=existing_promotion["dataset_version_id"],
                    )
                    if existing_promotion["request_fingerprint"] != expected_fingerprint:
                        raise ValueError("Promotion ID conflict: request identity differs.")
                if state == PromotionOperationState.SUCCEEDED:
                    connection.commit()
                    return json.loads(existing_promotion["result_json"])
                if state in {
                    PromotionOperationState.TERMINAL_FAILURE,
                    PromotionOperationState.CANCELLED,
                }:
                    raise ValueError(
                        f"Promotion operation is terminal in state '{state.value}'."
                    )

            approved_source = connection.execute(
                """SELECT version.dataset_id, version.dataset_version_id,
                          dataset.workspace_id
                   FROM approvals AS approval
                   JOIN validation_runs AS validation
                     ON validation.validation_run_id = approval.validation_run_id
                    AND validation.dataset_version_id = approval.dataset_version_id
                   JOIN dataset_versions AS version
                     ON version.dataset_version_id = approval.dataset_version_id
                   JOIN datasets AS dataset
                     ON dataset.dataset_id = version.dataset_id
                   JOIN dataset_sources AS source
                     ON source.dataset_version_id = version.dataset_version_id
                   JOIN source_files AS file
                     ON file.source_file_id = source.source_file_id
                   WHERE approval.approval_id = ?
                     AND approval.state = 'approved'
                     AND approval.destination = 'solarpv_domain'
                     AND approval.decision_by IS NOT NULL
                     AND length(trim(approval.decision_by)) > 0
                     AND approval.decision_at IS NOT NULL
                     AND version.state = 'valid'
                     AND validation.outcome IN ('passed', 'warnings')
                     AND file.workspace_id = dataset.workspace_id
                     AND file.sha256 = ?
                   LIMIT 1""",
                (approval_id, source_hash),
            ).fetchone()
            if approved_source is None:
                raise ValueError(
                    "Promotion requires an approved dataset version, a valid "
                    "operator decision, and a link to this source content."
                )
            dataset_id = approved_source["dataset_id"]
            dataset_version_id = approved_source["dataset_version_id"]
            promotion_workspace_id = approved_source["workspace_id"]
            if (requested_workspace_id is not None
                    and requested_workspace_id != promotion_workspace_id):
                raise ValueError(
                    "Promotion workspace does not match the approved dataset workspace."
                )
            workspace_id = promotion_workspace_id
            request_fingerprint = _promotion_request_fingerprint(
                source_hash=source_hash,
                source_name=source_name,
                approval_id=approval_id,
                dataset_id=dataset_id,
                dataset_version_id=dataset_version_id,
            )
            if existing_promotion is not None and (
                existing_promotion["dataset_id"] not in (None, dataset_id)
                or existing_promotion["dataset_version_id"] not in (None, dataset_version_id)
            ):
                raise ValueError(
                    "Promotion ID conflict: dataset version differs from the original request."
                )
            if existing_promotion is None:
                already_imported_before = _import_marker_exists(
                    connection, workspace_id, source_hash
                ) is not None
                connection.execute(
                    """INSERT INTO promotion_operations (
                        promotion_id, approval_id, source_hash, source_name,
                        dataset_id, dataset_version_id, request_fingerprint,
                        state, attempt_count, already_imported_before
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?)""",
                    (promotion_id, approval_id, source_hash, source_name,
                     dataset_id, dataset_version_id, request_fingerprint,
                     int(already_imported_before)),
                )
            else:
                state = PromotionOperationState(existing_promotion["state"])
                if state == PromotionOperationState.RUNNING or (
                    state == PromotionOperationState.QUEUED
                    and existing_promotion["attempt_count"] > 0
                ):
                    validate_transition(
                        PromotionOperationState, state,
                        PromotionOperationState.RECONCILIATION_REQUIRED,
                    )
                    connection.execute(
                        """UPDATE promotion_operations
                           SET state = 'reconciliation_required',
                               error_code = 'process_interrupted',
                               error_message = 'A prior attempt needs reconciliation.'
                           WHERE promotion_id = ?""",
                        (promotion_id,),
                    )
                    connection.commit()
                    connection.execute("BEGIN IMMEDIATE")
                    recovered = connection.execute(
                        "SELECT state, outcome, result_json FROM promotion_operations "
                        "WHERE promotion_id = ?",
                        (promotion_id,),
                    ).fetchone()
                    if recovered["outcome"] is not None or recovered["result_json"] is not None:
                        raise RuntimeError(
                            "Promotion has a reconciliation marker with a result; "
                            "manual review is required."
                        )
                    state = PromotionOperationState(recovered["state"])
                if state == PromotionOperationState.RECONCILIATION_REQUIRED:
                    raise ValueError(
                        "Promotion requires explicit reconciliation before retry."
                    )
                if state == PromotionOperationState.RETRYABLE_FAILURE:
                    validate_transition(
                        PromotionOperationState, state,
                        PromotionOperationState.QUEUED,
                    )
                    connection.execute(
                        "UPDATE promotion_operations SET state = 'queued' WHERE promotion_id = ?",
                        (promotion_id,),
                    )
                    state = PromotionOperationState.QUEUED
                if state != PromotionOperationState.QUEUED:
                    raise ValueError(
                        f"Promotion operation cannot run from state '{state.value}'."
                    )
                already_imported = _import_marker_exists(connection, workspace_id, source_hash)
                if (
                    already_imported is not None
                    and existing_promotion["already_imported_before"] == 0
                ):
                    connection.execute(
                        "UPDATE promotion_operations SET already_imported_before = 1 "
                        "WHERE promotion_id = ? AND state = 'queued'",
                        (promotion_id,),
                    )
                connection.execute(
                    """UPDATE promotion_operations
                       SET attempt_count = attempt_count + 1,
                           error_code = NULL, error_message = NULL
                       WHERE promotion_id = ?""",
                    (promotion_id,),
                )
            connection.commit()

        connection.execute("BEGIN IMMEDIATE")

        if promotion_id is not None:
            operation = connection.execute(
                "SELECT state, source_hash, approval_id, source_name, result_json, "
                "dataset_id, dataset_version_id, already_imported_before "
                "FROM promotion_operations WHERE promotion_id = ?",
                (promotion_id,),
            ).fetchone()
            if operation is None:
                raise RuntimeError("Promotion operation disappeared before execution.")
            if (
                operation["source_hash"] != source_hash
                or operation["approval_id"] != approval_id
                or operation["source_name"] != source_name
            ):
                raise ValueError("Promotion ID is bound to a different request.")
            state = PromotionOperationState(operation["state"])
            if state == PromotionOperationState.SUCCEEDED:
                connection.commit()
                return json.loads(operation["result_json"])
            import_marker = _import_marker_exists(connection, workspace_id, source_hash)
            if import_marker is not None and operation["already_imported_before"] == 0:
                connection.execute(
                    "UPDATE promotion_operations SET already_imported_before = 1 "
                    "WHERE promotion_id = ? AND state = 'queued'",
                    (promotion_id,),
                )
            if state == PromotionOperationState.RETRYABLE_FAILURE:
                validate_transition(
                    PromotionOperationState, state, PromotionOperationState.QUEUED
                )
                connection.execute(
                    "UPDATE promotion_operations SET state = 'queued' WHERE promotion_id = ?",
                    (promotion_id,),
                )
                state = PromotionOperationState.QUEUED
            validate_transition(
                PromotionOperationState, state, PromotionOperationState.RUNNING
            )
            connection.execute(
                "UPDATE promotion_operations SET state = 'running' WHERE promotion_id = ?",
                (promotion_id,),
            )
            approved_source = connection.execute(
                """SELECT 1
                   FROM approvals AS approval
                   JOIN validation_runs AS validation
                     ON validation.validation_run_id = approval.validation_run_id
                    AND validation.dataset_version_id = approval.dataset_version_id
                   JOIN dataset_versions AS version
                     ON version.dataset_version_id = approval.dataset_version_id
                   JOIN datasets AS dataset
                     ON dataset.dataset_id = version.dataset_id
                   JOIN dataset_sources AS source
                     ON source.dataset_version_id = version.dataset_version_id
                   JOIN source_files AS file
                     ON file.source_file_id = source.source_file_id
                   WHERE approval.approval_id = ?
                     AND approval.state = 'approved'
                     AND approval.destination = 'solarpv_domain'
                     AND approval.decision_by IS NOT NULL
                     AND length(trim(approval.decision_by)) > 0
                     AND approval.decision_at IS NOT NULL
                     AND version.state = 'valid'
                     AND validation.outcome IN ('passed', 'warnings')
                     AND version.dataset_id = ?
                     AND version.dataset_version_id = ?
                     AND file.workspace_id = dataset.workspace_id
                     AND file.sha256 = ?
                   LIMIT 1""",
                (
                    approval_id,
                    operation["dataset_id"],
                    operation["dataset_version_id"],
                    source_hash,
                ),
            ).fetchone()
            if approved_source is None:
                raise ValueError(
                    "Promotion approval or operator decision is no longer valid "
                    "for this source."
                )

        existing_import = _import_marker_exists(connection, workspace_id, source_hash)

        if existing_import is not None:
            result = {
                "status": "skipped",
                "source_name": source_name,
                "assets": 0,
                "inspections": 0,
                "observations": 0,
                "maintenance": 0,
                "message": "Identical source content has already been imported.",
            }
            if promotion_id is not None:
                result["promotion_id"] = promotion_id
                result["outcome"] = "already_imported"
                validate_transition(
                    PromotionOperationState,
                    PromotionOperationState.RUNNING,
                    PromotionOperationState.SUCCEEDED,
                )
                connection.execute(
                    """UPDATE promotion_operations
                       SET state = 'succeeded', outcome = 'already_imported',
                           result_json = ?
                       WHERE promotion_id = ?""",
                    (json.dumps(result, sort_keys=True), promotion_id),
                )
            connection.commit()
            return result

        asset_map = {}
        inspection_map = {}
        inserted_record_ids = {
            "asset": [],
            "inspection": [],
            "observation": [],
            "maintenance": [],
        }
        domain_workspace_scoped = _table_has_column(connection, "assets", "workspace_id")
        if not domain_workspace_scoped and workspace_id != "default":
            raise ValueError(
                "This database schema supports imports only in the default workspace; "
                "apply the current workspace migration before importing elsewhere."
            )

        for asset in data["assets"]:
            external_id = _required_text(asset, "external_id", "Asset")
            if domain_workspace_scoped:
                cursor = connection.execute(
                    """
                INSERT INTO assets (
                    workspace_id,
                    external_id,
                    asset_type,
                    name,
                    description,
                    location,
                    data_origin
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workspace_id,
                        external_id,
                        _required_text(asset, "asset_type", "Asset"),
                        _required_text(asset, "name", "Asset"),
                        _optional_text(asset, "description"),
                        _optional_text(asset, "location"),
                        asset.get("data_origin", "unknown"),
                    ),
                )
            else:
                cursor = connection.execute(
                    """INSERT INTO assets(
                        external_id, asset_type, name, description, location
                    ) VALUES (?, ?, ?, ?, ?)""",
                    (external_id, _required_text(asset, "asset_type", "Asset"),
                     _required_text(asset, "name", "Asset"),
                     _optional_text(asset, "description"), _optional_text(asset, "location")),
                )

            asset_map[external_id] = cursor.lastrowid
            inserted_record_ids["asset"].append(cursor.lastrowid)

        for inspection in data["inspections"]:
            inspection_key = inspection["inspection_key"].strip()
            asset_external_id = inspection["asset_external_id"].strip()

            if domain_workspace_scoped:
                cursor = connection.execute(
                """
                INSERT INTO inspections (
                    workspace_id,
                    asset_id,
                    inspected_at,
                    inspection_method,
                    data_source,
                    data_origin,
                    status,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workspace_id,
                        asset_map[asset_external_id],
                        inspection["inspected_at"].strip(),
                        inspection["inspection_method"].strip(),
                        inspection["data_source"].strip(),
                        inspection["data_origin"].strip(),
                        inspection["status"].strip(),
                        _optional_text(inspection, "notes"),
                    ),
                )
            else:
                cursor = connection.execute(
                    """INSERT INTO inspections(
                        asset_id, inspected_at, inspection_method, data_source,
                        data_origin, status, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (asset_map[asset_external_id], inspection["inspected_at"].strip(),
                     inspection["inspection_method"].strip(), inspection["data_source"].strip(),
                     inspection["data_origin"].strip(), inspection["status"].strip(),
                     _optional_text(inspection, "notes")),
                )

            inspection_map[inspection_key] = cursor.lastrowid
            inserted_record_ids["inspection"].append(cursor.lastrowid)

        for observation in data["observations"]:
            raw_value = observation.get("value")
            value = (
                float(raw_value)
                if raw_value is not None and str(raw_value).strip()
                else None
            )

            if domain_workspace_scoped:
                cursor = connection.execute(
                    """
                INSERT INTO observations (
                    workspace_id,
                    inspection_id,
                    observation_type,
                    value,
                    unit,
                    finding,
                    data_quality,
                    source_reference
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workspace_id,
                        inspection_map[observation["inspection_key"].strip()],
                        observation["observation_type"].strip(),
                        value,
                        _optional_text(observation, "unit"),
                        _optional_text(observation, "finding"),
                        observation["data_quality"].strip(),
                        _optional_text(observation, "source_reference"),
                    ),
                )
            else:
                cursor = connection.execute(
                    """INSERT INTO observations(
                        inspection_id, observation_type, value, unit, finding,
                        data_quality, source_reference
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (inspection_map[observation["inspection_key"].strip()],
                     observation["observation_type"].strip(), value,
                     _optional_text(observation, "unit"), _optional_text(observation, "finding"),
                     observation["data_quality"].strip(),
                     _optional_text(observation, "source_reference")),
                )
            inserted_record_ids["observation"].append(cursor.lastrowid)

        for maintenance in data["maintenance"]:
            if domain_workspace_scoped:
                cursor = connection.execute(
                    """
                INSERT INTO maintenance_records (
                    workspace_id,
                    asset_id,
                    performed_at,
                    maintenance_type,
                    description,
                    performed_by,
                    data_origin
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workspace_id,
                        asset_map[maintenance["asset_external_id"].strip()],
                        maintenance["performed_at"].strip(),
                        maintenance["maintenance_type"].strip(),
                        maintenance["description"].strip(),
                        _optional_text(maintenance, "performed_by"),
                        maintenance["data_origin"].strip(),
                    ),
                )
            else:
                cursor = connection.execute(
                    """INSERT INTO maintenance_records(
                        asset_id, performed_at, maintenance_type, description, performed_by, data_origin
                    ) VALUES (?, ?, ?, ?, ?, ?)""",
                    (asset_map[maintenance["asset_external_id"].strip()],
                     maintenance["performed_at"].strip(), maintenance["maintenance_type"].strip(),
                     maintenance["description"].strip(), _optional_text(maintenance, "performed_by"),
                     maintenance["data_origin"].strip()),
                )
            inserted_record_ids["maintenance"].append(cursor.lastrowid)

        _insert_import_marker(connection, workspace_id, source_hash, source_name)

        result = {
            "status": "imported",
            "source_name": source_name,
            "assets": len(data["assets"]),
            "inspections": len(data["inspections"]),
            "observations": len(data["observations"]),
            "maintenance": len(data["maintenance"]),
            "message": "Import completed successfully.",
        }
        if promotion_id is not None:
            result["promotion_id"] = promotion_id
            result["outcome"] = "imported"
            validate_transition(
                PromotionOperationState,
                PromotionOperationState.RUNNING,
                PromotionOperationState.SUCCEEDED,
            )
            connection.executemany(
                """INSERT INTO promotion_domain_records (
                    promotion_id, record_type, record_id
                ) VALUES (?, ?, ?)""",
                [
                    (promotion_id, record_type, record_id)
                    for record_type, record_ids in inserted_record_ids.items()
                    for record_id in record_ids
                ],
            )
            connection.execute(
                """UPDATE promotion_operations
                   SET state = 'succeeded', outcome = 'imported', result_json = ?
                   WHERE promotion_id = ?""",
                (json.dumps(result, sort_keys=True), promotion_id),
            )

        connection.commit()

    return result


def _table_has_column(connection: sqlite3.Connection, table: str, column: str) -> bool:
    return any(row["name"] == column for row in connection.execute(f"PRAGMA table_info({table})"))


def _import_marker_exists(connection: sqlite3.Connection, workspace_id: str, source_hash: str):
    if _table_has_column(connection, "imported_files", "workspace_id"):
        return connection.execute(
            "SELECT 1 FROM imported_files WHERE workspace_id = ? AND source_hash = ?",
            (workspace_id, source_hash),
        ).fetchone()
    if workspace_id != "default":
        raise ValueError(
            "This database schema supports imports only in the default workspace; "
            "apply the current workspace migration before importing elsewhere."
        )
    return connection.execute(
        "SELECT 1 FROM imported_files WHERE source_hash = ?", (source_hash,)
    ).fetchone()


def _insert_import_marker(
    connection: sqlite3.Connection, workspace_id: str, source_hash: str, source_name: str
) -> None:
    if _table_has_column(connection, "imported_files", "workspace_id"):
        connection.execute(
            "INSERT INTO imported_files(workspace_id, source_hash, source_name) VALUES (?, ?, ?)",
            (workspace_id, source_hash, source_name),
        )
        return
    if workspace_id != "default":
        raise ValueError(
            "This database schema supports imports only in the default workspace; "
            "apply the current workspace migration before importing elsewhere."
        )
    connection.execute(
        "INSERT INTO imported_files(source_hash, source_name) VALUES (?, ?)",
        (source_hash, source_name),
    )
