"""Tests for ingestion state, metadata and promotion foundations."""

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from solarpv_db.database import get_connection, initialize_database
import solarpv_db.database as database_module
from solarpv_db.importer import _promotion_request_fingerprint, import_data
from solarpv_ingestion.repository import (
    ApprovalRevocationError,
    ConcurrentStateChangeError,
    IngestionRepository,
)
from solarpv_ingestion.states import (
    ApprovalState,
    DatasetVersionState,
    IngestionJobState,
    PromotionOperationState,
    SourceFileState,
)
from solarpv_ingestion.storage import LocalArtifactStorage
from solarpv_ingestion.transitions import InvalidStateTransition, validate_transition


def _dataset():
    return {
        "assets": [{"external_id": "PROMO-01", "asset_type": "panel", "name": "Panel"}],
        "inspections": [{
            "inspection_key": "INSP-01", "asset_external_id": "PROMO-01",
            "inspected_at": "2026-10-01T09:00:00Z", "inspection_method": "visual",
            "data_source": "test", "data_origin": "synthetic", "status": "completed",
        }],
        "observations": [{
            "inspection_key": "INSP-01", "observation_type": "temperature",
            "value": 0, "unit": "degC", "data_quality": "good",
        }],
        "maintenance": [{
            "asset_external_id": "PROMO-01", "performed_at": "2026-10-02T10:00:00Z",
            "maintenance_type": "cleaning", "description": "Synthetic test service",
            "data_origin": "synthetic",
        }],
    }


def _approved_source(tmp_path, database_path, data=None):
    source = tmp_path / "approved.json"
    source.write_text(json.dumps(data or _dataset()), encoding="utf-8")
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    repository = IngestionRepository(database_path)
    source_file = repository.create_source_file(
        source_file_id="source-1", workspace_id="default",
        original_name=source.name, storage_key="source/source-1",
        media_type="application/json", size_bytes=source.stat().st_size,
        sha256=source_hash, state=SourceFileState.AVAILABLE,
    )
    repository.create_dataset_version(
        dataset_id="dataset-1", dataset_version_id="dataset-version-1",
        name="Approved test dataset",
    )
    repository.transition_dataset_version("dataset-version-1", DatasetVersionState.VALIDATING)
    repository.link_dataset_source(
        dataset_source_id="dataset-source-1",
        dataset_version_id="dataset-version-1",
        source_file_id=source_file.source_file_id,
    )
    repository.create_validation_run(
        validation_run_id="validation-1", dataset_version_id="dataset-version-1",
        rule_set_version="test-1", outcome="passed",
    )
    repository.transition_dataset_version("dataset-version-1", DatasetVersionState.VALID)
    repository.create_approval(
        approval_id="approval-1", dataset_version_id="dataset-version-1",
        validation_run_id="validation-1",
    )
    repository.transition_approval("approval-1", ApprovalState.APPROVED, decision_by="local-operator")
    return source, repository


def _add_approved_version(repository, source_file_id, suffix="2"):
    dataset_id = f"dataset-{suffix}"
    version_id = f"dataset-version-{suffix}"
    validation_id = f"validation-{suffix}"
    approval_id = f"approval-{suffix}"
    repository.create_dataset_version(
        dataset_id=dataset_id, dataset_version_id=version_id,
        name=f"Approved dataset {suffix}",
    )
    repository.transition_dataset_version(version_id, DatasetVersionState.VALIDATING)
    repository.link_dataset_source(
        dataset_source_id=f"dataset-source-{suffix}",
        dataset_version_id=version_id,
        source_file_id=source_file_id,
    )
    repository.create_validation_run(
        validation_run_id=validation_id, dataset_version_id=version_id,
        rule_set_version="test-1", outcome="passed",
    )
    repository.transition_dataset_version(version_id, DatasetVersionState.VALID)
    repository.create_approval(
        approval_id=approval_id, dataset_version_id=version_id,
        validation_run_id=validation_id,
    )
    repository.transition_approval(
        approval_id, ApprovalState.APPROVED, decision_by="local-operator"
    )
    return approval_id, version_id


def _insert_queued_promotion(database_path, source, promotion_id, approval_id="approval-1"):
    content_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    source_name = source.name
    with get_connection(database_path) as connection:
        approved = connection.execute(
            """SELECT version.dataset_id, version.dataset_version_id
               FROM approvals AS approval
               JOIN dataset_versions AS version
                 ON version.dataset_version_id = approval.dataset_version_id
               WHERE approval.approval_id = ?""",
            (approval_id,),
        ).fetchone()
        already_imported = connection.execute(
            "SELECT 1 FROM imported_files WHERE source_hash = ?", (content_hash,)
        ).fetchone() is not None
        request_hash = _promotion_request_fingerprint(
            source_hash=content_hash, source_name=source_name,
            approval_id=approval_id, dataset_id=approved["dataset_id"],
            dataset_version_id=approved["dataset_version_id"],
        )
        connection.execute(
            """INSERT INTO promotion_operations (
                promotion_id, approval_id, source_hash, source_name, dataset_id,
                dataset_version_id, request_fingerprint, state, attempt_count,
                already_imported_before
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 0, ?)""",
            (promotion_id, approval_id, content_hash, source_name,
             approved["dataset_id"], approved["dataset_version_id"], request_hash,
             int(already_imported)),
        )


def _copy_migrations_through(tmp_path, monkeypatch, maximum_version):
    migration_dir = tmp_path / f"migrations-through-{maximum_version}"
    migration_dir.mkdir()
    source_dir = database_module.MIGRATIONS_DIR
    for path in sorted(source_dir.glob("*.sql")):
        if int(path.name[:3]) <= maximum_version:
            (migration_dir / path.name).write_text(
                path.read_text(encoding="utf-8"), encoding="utf-8"
            )
    monkeypatch.setattr(database_module, "MIGRATIONS_DIR", migration_dir)
    return migration_dir


def test_legacy_database_migrates_without_changing_existing_rows(tmp_path):
    database_path = tmp_path / "legacy.db"
    connection = sqlite3.connect(database_path)
    # Use the actual initial migration while deliberately omitting migration metadata.
    from solarpv_db.database import SCHEMA_FILE
    connection.executescript(SCHEMA_FILE.read_text(encoding="utf-8"))
    connection.execute(
        "INSERT INTO assets (external_id, asset_type, name) VALUES ('OLD-1', 'panel', 'Existing')"
    )
    connection.execute(
        "CREATE TABLE imported_files (source_hash TEXT PRIMARY KEY, source_name TEXT NOT NULL, imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "INSERT INTO imported_files (source_hash, source_name) VALUES ('old-hash', 'legacy.json')"
    )
    connection.commit()
    connection.close()

    initialize_database(database_path)

    with get_connection(database_path) as migrated:
        row = migrated.execute("SELECT external_id, name FROM assets").fetchone()
        assert (row["external_id"], row["name"]) == ("OLD-1", "Existing")
        assert migrated.execute("SELECT source_hash FROM imported_files").fetchone()[0] == "old-hash"
        assert migrated.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 6
        assert migrated.execute("PRAGMA user_version").fetchone()[0] == 6


def test_incompatible_untracked_schema_is_rejected_without_baseline(tmp_path):
    database_path = tmp_path / "partial.db"
    connection = sqlite3.connect(database_path)
    connection.execute("CREATE TABLE assets (asset_id INTEGER PRIMARY KEY)")
    connection.execute("INSERT INTO assets VALUES (7)")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="Solar PV schema is incompatible"):
        initialize_database(database_path)

    with sqlite3.connect(database_path) as unchanged:
        assert unchanged.execute("SELECT asset_id FROM assets").fetchone()[0] == 7
        assert unchanged.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
        ).fetchone() is None


@pytest.mark.parametrize(
    "schema_fault", ["missing_column", "missing_constraint", "missing_primary_key"]
)
def test_incompatible_legacy_table_definitions_are_rejected(tmp_path, schema_fault):
    database_path = tmp_path / f"legacy-{schema_fault}.db"
    connection = sqlite3.connect(database_path)
    from solarpv_db.database import SCHEMA_FILE
    schema = SCHEMA_FILE.read_text(encoding="utf-8")
    if schema_fault == "missing_column":
        schema = schema.replace("    description TEXT,\n", "")
    elif schema_fault == "missing_constraint":
        schema = schema.replace("external_id TEXT UNIQUE,", "external_id TEXT,")
    else:
        schema = schema.replace("asset_id INTEGER PRIMARY KEY,", "asset_id INTEGER,")
    connection.executescript(schema)
    connection.execute(
        "INSERT INTO assets (external_id, asset_type, name) VALUES ('KEEP', 'panel', 'Retained')"
    )
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="incompatible"):
        initialize_database(database_path)

    with sqlite3.connect(database_path) as unchanged:
        assert unchanged.execute(
            "SELECT external_id FROM assets WHERE external_id = 'KEEP'"
        ).fetchone()[0] == "KEEP"
        assert unchanged.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'schema_migrations'"
        ).fetchone() is None


def test_missing_legacy_index_is_rejected_without_rebuilding(tmp_path):
    database_path = tmp_path / "legacy-index.db"
    from solarpv_db.database import SCHEMA_FILE
    connection = sqlite3.connect(database_path)
    connection.executescript(SCHEMA_FILE.read_text(encoding="utf-8"))
    connection.execute("DROP INDEX idx_assets_type")
    connection.execute(
        "INSERT INTO assets (external_id, asset_type, name) VALUES ('KEEP', 'panel', 'Retained')"
    )
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="missing objects"):
        initialize_database(database_path)

    with sqlite3.connect(database_path) as unchanged:
        assert unchanged.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 1
        assert unchanged.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'idx_assets_type'"
        ).fetchone() is None


def test_empty_migration_ledger_is_treated_as_untracked_legacy(tmp_path):
    database_path = tmp_path / "empty-ledger.db"
    from solarpv_db.database import SCHEMA_FILE
    connection = sqlite3.connect(database_path)
    connection.executescript(SCHEMA_FILE.read_text(encoding="utf-8"))
    connection.execute(
        "INSERT INTO assets (external_id, asset_type, name) VALUES ('OLD', 'panel', 'Retained')"
    )
    connection.execute(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.commit()
    connection.close()

    initialize_database(database_path)

    with get_connection(database_path) as migrated:
        assert migrated.execute("SELECT name FROM assets WHERE external_id = 'OLD'").fetchone()[0] == "Retained"
        assert migrated.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 6


@pytest.mark.parametrize(("ledger_version", "pragma_version"), [(6, 5), (5, 6)])
def test_migration_ledger_and_user_version_mismatch_is_rejected(
    tmp_path, ledger_version, pragma_version
):
    database_path = tmp_path / f"mismatch-{ledger_version}-{pragma_version}.db"
    initialize_database(database_path)
    with get_connection(database_path) as connection:
        if ledger_version == 5:
            connection.execute("DELETE FROM schema_migrations WHERE version = 6")
        connection.execute(f"PRAGMA user_version = {pragma_version}")

    with pytest.raises(RuntimeError, match="user_version does not match"):
        initialize_database(database_path)


def test_unknown_future_migration_version_is_rejected(tmp_path):
    database_path = tmp_path / "future-version.db"
    initialize_database(database_path)
    with get_connection(database_path) as connection:
        connection.execute(
            "INSERT INTO schema_migrations (version, name, checksum) VALUES (7, 'future', 'future')"
        )
        connection.execute("PRAGMA user_version = 7")

    with pytest.raises(RuntimeError, match="newer than available"):
        initialize_database(database_path)


def test_migration_reapplication_is_idempotent(tmp_path):
    database_path = tmp_path / "migrations.db"
    initialize_database(database_path)
    initialize_database(database_path)
    with get_connection(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 6
        assert connection.execute("SELECT COUNT(*) FROM workspaces").fetchone()[0] == 1


def test_migration_005_preserves_proven_success_and_reconciles_ambiguous_history(
    tmp_path, monkeypatch
):
    migration_005 = database_module.MIGRATIONS_DIR / "005_final_integrity.sql"
    migration_dir = _copy_migrations_through(tmp_path, monkeypatch, 4)

    proven_dir = tmp_path / "proven"
    proven_dir.mkdir()
    proven_db = proven_dir / "proven.db"
    initialize_database(proven_db)
    proven_source, _ = _approved_source(proven_dir, proven_db)
    imported = import_data(
        proven_source, proven_db, promotion_id="proven-imported", approval_id="approval-1"
    )
    duplicate = import_data(
        proven_source, proven_db, promotion_id="proven-duplicate", approval_id="approval-1"
    )
    assert imported["outcome"] == "imported"
    assert duplicate["outcome"] == "already_imported"
    for state in ("queued", "running", "retryable_failure", "reconciliation_required"):
        promotion_id = f"historical-{state}"
        _insert_queued_promotion(proven_db, proven_source, promotion_id)
        if state != "queued":
            with get_connection(proven_db) as connection:
                connection.execute(
                    "UPDATE promotion_operations SET state = ? WHERE promotion_id = ?",
                    (state, promotion_id),
                )
    ambiguous_dir = tmp_path / "ambiguous"
    ambiguous_dir.mkdir()
    ambiguous_db = ambiguous_dir / "ambiguous.db"
    initialize_database(ambiguous_db)
    ambiguous_source, _ = _approved_source(ambiguous_dir, ambiguous_db)
    import_data(ambiguous_source, ambiguous_db)
    import_data(
        ambiguous_source, ambiguous_db,
        promotion_id="ambiguous-duplicate", approval_id="approval-1",
    )
    source_hash = hashlib.sha256(ambiguous_source.read_bytes()).hexdigest()
    with get_connection(ambiguous_db) as connection:
        connection.execute(
            "UPDATE promotion_operations SET result_json = 'not json' "
            "WHERE promotion_id = 'ambiguous-duplicate'"
        )
        connection.execute("DROP TRIGGER trg_imported_marker_referenced_immutable")
        connection.execute("DELETE FROM imported_files WHERE source_hash = ?", (source_hash,))
    (migration_dir / migration_005.name).write_text(
        migration_005.read_text(encoding="utf-8"), encoding="utf-8"
    )
    initialize_database(proven_db)
    with get_connection(proven_db) as connection:
        proven = connection.execute(
            "SELECT state, outcome, already_imported_before FROM promotion_operations "
            "WHERE promotion_id IN ('proven-imported', 'proven-duplicate') "
            "ORDER BY promotion_id"
        ).fetchall()
        assert [(row["state"], row["outcome"], row["already_imported_before"]) for row in proven] == [
            ("succeeded", "already_imported", 1),
            ("succeeded", "imported", 0),
        ]
        unfinished = connection.execute(
            "SELECT state, already_imported_before FROM promotion_operations "
            "WHERE promotion_id LIKE 'historical-%' ORDER BY promotion_id"
        ).fetchall()
        assert [(row["state"], row["already_imported_before"]) for row in unfinished] == [
            ("reconciliation_required", 0),
        ] * 4
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM imported_files").fetchone()[0] == 1

    initialize_database(ambiguous_db)
    with get_connection(ambiguous_db) as connection:
        operation = connection.execute(
            "SELECT state, outcome, result_json, already_imported_before "
            "FROM promotion_operations WHERE promotion_id = 'ambiguous-duplicate'"
        ).fetchone()
        assert (
            operation["state"], operation["outcome"], operation["result_json"],
            operation["already_imported_before"],
        ) == ("reconciliation_required", "already_imported", "not json", 0)
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM imported_files").fetchone()[0] == 0


@pytest.mark.parametrize(
    "mutation",
    ["missing_promotion_id", "mismatched_promotion_id", "mismatched_outcome",
     "mismatched_source_name", "mismatched_record_count", "malformed_json"],
)
def test_migration_006_reconciles_success_with_inconsistent_result(
    tmp_path, monkeypatch, mutation
):
    migration_006 = database_module.MIGRATIONS_DIR / "006_promotion_result_lineage.sql"
    migration_dir = _copy_migrations_through(tmp_path, monkeypatch, 5)
    case_dir = tmp_path / mutation
    case_dir.mkdir()
    database_path = case_dir / "historical-result.db"
    initialize_database(database_path)
    source, _ = _approved_source(case_dir, database_path)
    import_data(source, database_path)
    valid_result = import_data(
        source, database_path,
        promotion_id="historical-result", approval_id="approval-1",
    )
    inconsistent_result = dict(valid_result)
    if mutation == "malformed_json":
        stored_result = "not json"
    elif mutation == "missing_promotion_id":
        del inconsistent_result["promotion_id"]
        stored_result = json.dumps(inconsistent_result, sort_keys=True)
    elif mutation == "mismatched_promotion_id":
        inconsistent_result["promotion_id"] = "another-promotion"
        stored_result = json.dumps(inconsistent_result, sort_keys=True)
    elif mutation == "mismatched_outcome":
        inconsistent_result["outcome"] = "imported"
        stored_result = json.dumps(inconsistent_result, sort_keys=True)
    elif mutation == "mismatched_source_name":
        inconsistent_result["source_name"] = "different-source.json"
        stored_result = json.dumps(inconsistent_result, sort_keys=True)
    else:
        inconsistent_result["assets"] = 1
        stored_result = json.dumps(inconsistent_result, sort_keys=True)

    with get_connection(database_path) as connection:
        connection.execute("DROP TRIGGER trg_successful_promotion_immutable")
        if mutation == "malformed_json":
            connection.execute("DROP TRIGGER trg_successful_promotion_json")
        connection.execute(
            "UPDATE promotion_operations SET result_json = ? WHERE promotion_id = ?",
            (stored_result, "historical-result"),
        )
    (migration_dir / migration_006.name).write_text(
        migration_006.read_text(encoding="utf-8"), encoding="utf-8"
    )

    initialize_database(database_path)

    with get_connection(database_path) as connection:
        operation = connection.execute(
            "SELECT state, result_json FROM promotion_operations "
            "WHERE promotion_id = 'historical-result'"
        ).fetchone()
        assert operation["state"] == "reconciliation_required"
        if mutation == "malformed_json":
            assert operation["result_json"] == stored_result
        else:
            assert json.loads(operation["result_json"]) == inconsistent_result
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM imported_files").fetchone()[0] == 1


def test_concurrent_migration_initialization_is_serialized(tmp_path):
    database_path = tmp_path / "concurrent-init.db"
    barrier = Barrier(2)

    def initialize():
        barrier.wait()
        initialize_database(database_path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(lambda _: initialize(), range(2)))

    with get_connection(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 6
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6


def test_migration_runner_ignores_trailing_sql_comments(tmp_path, monkeypatch):
    migration_dir = tmp_path / "comment-migrations"
    migration_dir.mkdir()
    (migration_dir / "001_initial.sql").write_text(
        "CREATE TABLE retained (id INTEGER PRIMARY KEY);\n"
        "-- A trailing comment is not an incomplete SQL statement.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(database_module, "MIGRATIONS_DIR", migration_dir)
    database_path = tmp_path / "trailing-comment.db"

    initialize_database(database_path)

    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'retained'"
        ).fetchone()[0] == "retained"
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1


def test_failed_migration_rolls_back_its_schema_and_version(tmp_path, monkeypatch):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "001_initial.sql").write_text(
        "CREATE TABLE baseline (id INTEGER PRIMARY KEY);", encoding="utf-8"
    )
    (migration_dir / "002_broken.sql").write_text(
        "CREATE TABLE partial_change (id INTEGER);\nINVALID SQL;", encoding="utf-8"
    )
    monkeypatch.setattr(database_module, "MIGRATIONS_DIR", migration_dir)
    database_path = tmp_path / "failed-migration.db"

    with pytest.raises(sqlite3.OperationalError):
        initialize_database(database_path)

    with get_connection(database_path) as connection:
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 1
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'partial_change'"
        ).fetchone() is None
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1


def test_changed_applied_migration_is_rejected(tmp_path, monkeypatch):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    initial = migration_dir / "001_initial.sql"
    initial.write_text("CREATE TABLE stable (id INTEGER PRIMARY KEY);", encoding="utf-8")
    monkeypatch.setattr(database_module, "MIGRATIONS_DIR", migration_dir)
    database_path = tmp_path / "checksum.db"
    initialize_database(database_path)

    initial.write_text("CREATE TABLE altered (id INTEGER PRIMARY KEY);", encoding="utf-8")
    with pytest.raises(RuntimeError, match="does not match"):
        initialize_database(database_path)

    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'stable'"
        ).fetchone() is not None
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'altered'"
        ).fetchone() is None


@pytest.mark.parametrize(
    ("state_type", "current", "target"),
    [
        (SourceFileState, "receiving", "quarantined"),
        (IngestionJobState, "running", "retry_wait"),
        (DatasetVersionState, "validating", "valid"),
        (ApprovalState, "pending", "approved"),
        (PromotionOperationState, "queued", "running"),
        (PromotionOperationState, "reconciliation_required", "succeeded"),
    ],
)
def test_valid_state_transitions(state_type, current, target):
    validate_transition(state_type, current, target)


@pytest.mark.parametrize(
    ("state_type", "current", "target"),
    [
        (SourceFileState, "deleted", "available"),
        (IngestionJobState, "succeeded", "queued"),
        (DatasetVersionState, "valid", "draft"),
        (ApprovalState, "rejected", "approved"),
        (PromotionOperationState, "succeeded", "queued"),
    ],
)
def test_terminal_or_invalid_transitions_are_rejected(state_type, current, target):
    with pytest.raises(InvalidStateTransition):
        validate_transition(state_type, current, target)


def test_metadata_repositories_enforce_foreign_keys_and_state_changes(tmp_path):
    database_path = tmp_path / "metadata.db"
    initialize_database(database_path)
    repository = IngestionRepository(database_path)
    repository.create_source_file(
        source_file_id="source", workspace_id="default", original_name="a.csv",
        storage_key="source/a", media_type="text/csv", size_bytes=1,
        sha256="abc",
    )
    job = repository.create_job(
        job_id="job", workspace_id="default", source_file_id="source",
        idempotency_key="job-key",
    )
    assert repository.transition_job(job.job_id, IngestionJobState.RUNNING).state == IngestionJobState.RUNNING
    with pytest.raises(sqlite3.IntegrityError):
        repository.create_job(
            job_id="orphan", workspace_id="default", source_file_id="missing",
            idempotency_key="orphan-key",
        )
    with pytest.raises(InvalidStateTransition):
        repository.transition_job(job.job_id, IngestionJobState.QUEUED)


def test_job_workspace_must_match_source_file_workspace(tmp_path):
    database_path = tmp_path / "job-workspace.db"
    initialize_database(database_path)
    repository = IngestionRepository(database_path)
    repository.create_source_file(
        source_file_id="workspace-source", workspace_id="default",
        original_name="site.csv", storage_key="workspace/site",
        media_type="text/csv", size_bytes=1, sha256="workspace-hash",
    )
    with get_connection(database_path) as connection:
        connection.execute(
            "INSERT INTO workspaces (workspace_id, name) VALUES ('other', 'Other')"
        )
        with pytest.raises(sqlite3.IntegrityError, match="same workspace"):
            connection.execute(
                """INSERT INTO ingestion_jobs (
                    job_id, workspace_id, source_file_id, state, idempotency_key
                ) VALUES ('raw-job', 'other', 'workspace-source', 'queued', 'raw-key')"""
            )
    with pytest.raises(ValueError, match="same workspace"):
        repository.create_job(
            job_id="repo-job", workspace_id="other",
            source_file_id="workspace-source", idempotency_key="repo-key",
        )

    repository.create_job(
        job_id="owned-job", workspace_id="default",
        source_file_id="workspace-source", idempotency_key="owned-key",
    )
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="fingerprint cannot change"):
            connection.execute(
                "UPDATE source_files SET sha256 = 'changed-hash' "
                "WHERE source_file_id = 'workspace-source'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="source workspace change"):
            connection.execute(
                "UPDATE source_files SET workspace_id = 'other' "
                "WHERE source_file_id = 'workspace-source'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="job ownership"):
            connection.execute(
                "UPDATE ingestion_jobs SET workspace_id = 'other' WHERE job_id = 'owned-job'"
            )


def test_competing_repository_state_transitions_are_serialized(tmp_path):
    database_path = tmp_path / "state-race.db"
    initialize_database(database_path)
    repository = IngestionRepository(database_path)
    repository.create_source_file(
        source_file_id="race-source", workspace_id="default", original_name="race.csv",
        storage_key="source/race", media_type="text/csv", size_bytes=1,
        sha256="race-hash",
    )
    repository.create_job(
        job_id="race-job", workspace_id="default", source_file_id="race-source",
        idempotency_key="race-job-key",
    )
    barrier = Barrier(2)

    def transition(target):
        barrier.wait()
        try:
            return repository.transition_job("race-job", target)
        except (InvalidStateTransition, ConcurrentStateChangeError) as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(transition, (IngestionJobState.RUNNING, IngestionJobState.CANCELLED)))
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(isinstance(result, Exception) for result in results) == 1


def test_cross_record_metadata_relationships_are_enforced(tmp_path):
    database_path = tmp_path / "metadata-integrity.db"
    initialize_database(database_path)
    source, repository = _approved_source(tmp_path, database_path)
    with get_connection(database_path) as connection:
        connection.execute(
            "INSERT INTO workspaces (workspace_id, name) VALUES ('other', 'Other')"
        )
        connection.execute(
            """INSERT INTO source_files (
                source_file_id, workspace_id, original_name, storage_key, size_bytes,
                sha256, state
            ) VALUES ('other-source', 'other', 'other.json', 'other/object', 1, 'other-hash', 'available')"""
        )
    repository.create_dataset_version(
        dataset_id="metadata-dataset", dataset_version_id="metadata-version",
        name="Default workspace dataset",
    )
    with pytest.raises(sqlite3.IntegrityError, match="ownership must agree"):
        repository.link_dataset_source(
            dataset_source_id="cross-workspace-link",
            dataset_version_id="metadata-version", source_file_id="other-source",
        )
    repository.link_dataset_source(
        dataset_source_id="valid-workspace-link",
        dataset_version_id="metadata-version", source_file_id="source-1",
    )
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="dataset workspace change"):
            connection.execute(
                "UPDATE datasets SET workspace_id = 'other' WHERE dataset_id = 'metadata-dataset'"
            )

    repository.create_job(
        job_id="artifact-job", workspace_id="default", source_file_id="source-1",
        idempotency_key="artifact-job-key",
    )
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="source, job, and workspace"):
            connection.execute(
                """INSERT INTO extraction_artifacts (
                    artifact_id, workspace_id, source_file_id, job_id, artifact_type,
                    storage_key, parser_id, parser_version, schema_version, checksum
                ) VALUES ('bad-artifact', 'default', 'other-source', 'artifact-job',
                          'table', 'artifact/bad', 'test', '1', '1', 'checksum')"""
            )

    _insert_queued_promotion(database_path, source, "bad-lineage")
    with get_connection(database_path) as connection:
        connection.execute(
            "UPDATE promotion_operations SET state = 'running' WHERE promotion_id = 'bad-lineage'"
        )
        with pytest.raises(sqlite3.IntegrityError, match="lineage is invalid"):
            connection.execute(
                """INSERT INTO promotion_domain_records (
                    promotion_id, record_type, record_id
                ) VALUES ('bad-lineage', 'asset', 999999)"""
            )


def test_local_artifact_storage_rejects_traversal_and_round_trips(tmp_path):
    storage = LocalArtifactStorage(tmp_path / "private")
    storage.put_bytes("files/a.bin", b"sample")
    assert storage.read_bytes("files/a.bin") == b"sample"
    with pytest.raises(FileExistsError):
        storage.put_bytes("files/a.bin", b"replacement")
    with pytest.raises(ValueError):
        storage.put_bytes("../outside.bin", b"no")


def test_promotion_result_replays_without_duplicate_domain_records(tmp_path):
    database_path = tmp_path / "promotion.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)

    first = import_data(source, database_path, promotion_id="promotion-1", approval_id="approval-1")
    second = import_data(source, database_path, promotion_id="promotion-1", approval_id="approval-1")
    assert first == second
    assert first["outcome"] == "imported"
    operation = IngestionRepository(database_path).get_promotion_operation("promotion-1")
    assert operation is not None
    assert operation.state == PromotionOperationState.SUCCEEDED
    with get_connection(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM inspections").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM maintenance_records").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM promotion_operations").fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM promotion_domain_records WHERE promotion_id = 'promotion-1'"
        ).fetchone()[0] == 4


def test_new_promotion_of_previously_imported_content_records_already_imported(tmp_path):
    database_path = tmp_path / "duplicate-promotion.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    original = import_data(source, database_path)
    assert original["status"] == "imported"

    result = import_data(source, database_path, promotion_id="promotion-2", approval_id="approval-1")
    assert result["status"] == "skipped"
    assert result["outcome"] == "already_imported"
    assert result["assets"] == 0
    with get_connection(database_path) as connection:
        assert connection.execute("SELECT outcome FROM promotion_operations").fetchone()[0] == "already_imported"
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM inspections").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM maintenance_records").fetchone()[0] == 1


def test_database_rejects_promotion_success_without_import_evidence(tmp_path):
    database_path = tmp_path / "promotion-success-evidence.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    _insert_queued_promotion(database_path, source, "invalid-imported")
    _insert_queued_promotion(database_path, source, "invalid-duplicate")

    with get_connection(database_path) as connection:
        for promotion_id in ("invalid-imported", "invalid-duplicate"):
            connection.execute(
                "UPDATE promotion_operations SET state = 'running' WHERE promotion_id = ?",
                (promotion_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """UPDATE promotion_operations
                   SET state = 'succeeded', outcome = 'imported', result_json = '{}'
                   WHERE promotion_id = 'invalid-imported'"""
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """UPDATE promotion_operations
                   SET state = 'succeeded', outcome = 'already_imported', result_json = '{}'
                   WHERE promotion_id = 'invalid-duplicate'"""
            )

    with get_connection(database_path) as connection:
        states = connection.execute(
            "SELECT state FROM promotion_operations ORDER BY promotion_id"
        ).fetchall()
        assert [row[0] for row in states] == ["running", "running"]


def test_successful_promotion_keeps_its_import_marker(tmp_path):
    database_path = tmp_path / "promotion-marker-retention.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    result = import_data(
        source, database_path, promotion_id="marker-retention", approval_id="approval-1"
    )
    assert result["outcome"] == "imported"

    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="required by a successful promotion"):
            connection.execute(
                "DELETE FROM imported_files WHERE source_hash = ?",
                (hashlib.sha256(source.read_bytes()).hexdigest(),),
            )


def test_successful_promotion_and_evidence_are_immutable(tmp_path):
    database_path = tmp_path / "promotion-immutable.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    result = import_data(
        source, database_path, promotion_id="immutable", approval_id="approval-1"
    )
    assert result["outcome"] == "imported"
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()

    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE promotion_operations SET result_json = '{}' WHERE promotion_id = 'immutable'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE promotion_operations SET outcome = 'already_imported' WHERE promotion_id = 'immutable'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE promotion_operations SET source_hash = 'changed-hash' "
                "WHERE promotion_id = 'immutable'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE promotion_operations SET state = 'reconciliation_required' WHERE promotion_id = 'immutable'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "DELETE FROM promotion_operations WHERE promotion_id = 'immutable'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """INSERT OR REPLACE INTO promotion_operations (
                    promotion_id, approval_id, source_hash, source_name, dataset_id,
                    dataset_version_id, request_fingerprint, state, attempt_count,
                    already_imported_before
                ) SELECT promotion_id, approval_id, source_hash, source_name, dataset_id,
                         dataset_version_id, request_fingerprint, 'queued', attempt_count,
                         already_imported_before
                  FROM promotion_operations WHERE promotion_id = 'immutable'"""
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "DELETE FROM promotion_domain_records WHERE promotion_id = 'immutable'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE promotion_domain_records SET record_id = record_id + 100 "
                "WHERE promotion_id = 'immutable'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """INSERT INTO promotion_domain_records (
                    promotion_id, record_type, record_id
                ) VALUES ('immutable', 'asset', 1)"""
            )
        for field in ("source_name", "imported_at"):
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                connection.execute(
                    f"UPDATE imported_files SET {field} = 'changed' WHERE source_hash = ?",
                    (source_hash,),
                )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "INSERT OR REPLACE INTO imported_files (source_hash, source_name) "
                "VALUES (?, 'replacement')",
                (source_hash,),
            )


def test_source_location_artifact_is_immutable_after_interpretation(tmp_path):
    database_path = tmp_path / "source-location-parent.db"
    initialize_database(database_path)
    _, repository = _approved_source(tmp_path, database_path)
    repository.create_job(
        job_id="location-job", workspace_id="default", source_file_id="source-1",
        idempotency_key="location-job-key",
    )
    with get_connection(database_path) as connection:
        for artifact_id, storage_key in (
            ("location-artifact-1", "artifacts/one"),
            ("location-artifact-2", "artifacts/two"),
        ):
            connection.execute(
                """INSERT INTO extraction_artifacts (
                    artifact_id, workspace_id, source_file_id, job_id, artifact_type,
                    storage_key, parser_id, parser_version, schema_version, checksum
                ) VALUES (?, 'default', 'source-1', 'location-job', 'table', ?,
                          'test', '1', '1', 'checksum')""",
                (artifact_id, storage_key),
            )
        connection.execute(
            """INSERT INTO source_locations (
                source_location_id, artifact_id, locator_type, locator_json, precision
            ) VALUES ('location-1', 'location-artifact-1', 'row', '{}', 'exact')"""
        )
        connection.execute(
            """INSERT INTO dataset_sources (
                dataset_source_id, dataset_version_id, source_file_id, artifact_id
            ) VALUES ('location-dataset-source', 'dataset-version-1', 'source-1',
                      'location-artifact-1')"""
        )
        connection.execute(
            """INSERT INTO interpretations (
                interpretation_id, workspace_id, source_location_id, proposed_field,
                proposed_value_json, method, status, dataset_version_id
            ) VALUES ('interpretation-1', 'default', 'location-1', 'type', '"panel"',
                      'human', 'suggested', 'dataset-version-1')"""
        )
        with pytest.raises(sqlite3.IntegrityError, match="cannot change after linkage"):
            connection.execute(
                "UPDATE source_locations SET artifact_id = 'location-artifact-2' "
                "WHERE source_location_id = 'location-1'"
            )


def test_artifact_source_identity_freezes_after_interpretation_lineage(tmp_path):
    database_path = tmp_path / "artifact-source-lineage.db"
    initialize_database(database_path)
    _, repository = _approved_source(tmp_path, database_path)
    repository.create_job(
        job_id="artifact-source-job-1", workspace_id="default",
        source_file_id="source-1", idempotency_key="artifact-source-key-1",
    )
    alternate_source = repository.create_source_file(
        source_file_id="source-2", workspace_id="default",
        original_name="alternate.json", storage_key="source/source-2",
        media_type="application/json", size_bytes=1,
        sha256=hashlib.sha256(b"alternate").hexdigest(),
        state=SourceFileState.AVAILABLE,
    )
    repository.create_job(
        job_id="artifact-source-job-2", workspace_id="default",
        source_file_id=alternate_source.source_file_id,
        idempotency_key="artifact-source-key-2",
    )

    with get_connection(database_path) as connection:
        connection.execute(
            """INSERT INTO extraction_artifacts (
                artifact_id, workspace_id, source_file_id, job_id, artifact_type,
                storage_key, parser_id, parser_version, schema_version, checksum
            ) VALUES ('lineage-artifact', 'default', 'source-1',
                      'artifact-source-job-1', 'table', 'artifacts/lineage',
                      'test', '1', '1', 'checksum')"""
        )
        # Before the artifact is referenced as provenance, a consistent source
        # and job reassignment remains available.
        connection.execute(
            """UPDATE extraction_artifacts
               SET source_file_id = 'source-2', job_id = 'artifact-source-job-2'
               WHERE artifact_id = 'lineage-artifact'"""
        )
        connection.execute(
            """UPDATE extraction_artifacts
               SET source_file_id = 'source-1', job_id = 'artifact-source-job-1'
               WHERE artifact_id = 'lineage-artifact'"""
        )
        connection.execute(
            """INSERT INTO source_locations (
                source_location_id, artifact_id, locator_type, locator_json, precision
            ) VALUES ('lineage-location', 'lineage-artifact', 'row', '{}', 'exact')"""
        )
        connection.execute(
            """INSERT INTO interpretations (
                interpretation_id, workspace_id, source_location_id, proposed_field,
                proposed_value_json, method, status
            ) VALUES ('lineage-interpretation', 'default', 'lineage-location',
                      'module_type', '"panel"', 'human', 'suggested')"""
        )
        with pytest.raises(sqlite3.IntegrityError, match="after lineage linkage"):
            connection.execute(
                """UPDATE extraction_artifacts
                   SET source_file_id = 'source-2', job_id = 'artifact-source-job-2'
                   WHERE artifact_id = 'lineage-artifact'"""
            )


def test_dataset_source_and_version_identity_are_protected_after_interpretation(tmp_path):
    database_path = tmp_path / "interpretation-parent.db"
    initialize_database(database_path)
    _, repository = _approved_source(tmp_path, database_path)
    repository.create_job(
        job_id="interpretation-job", workspace_id="default", source_file_id="source-1",
        idempotency_key="interpretation-job-key",
    )
    repository.create_dataset_version(
        dataset_id="interpretation-dataset", dataset_version_id="interpretation-version",
        name="Interpreted dataset",
    )
    repository.create_dataset_version(
        dataset_id="replacement-dataset", dataset_version_id="replacement-version",
        name="Replacement dataset",
    )
    with get_connection(database_path) as connection:
        connection.execute(
            """INSERT INTO extraction_artifacts (
                artifact_id, workspace_id, source_file_id, job_id, artifact_type,
                storage_key, parser_id, parser_version, schema_version, checksum
            ) VALUES ('interpretation-artifact', 'default', 'source-1',
                      'interpretation-job', 'table', 'artifacts/interpretation',
                      'test', '1', '1', 'checksum')"""
        )
        connection.execute(
            """INSERT INTO source_locations (
                source_location_id, artifact_id, locator_type, locator_json, precision
            ) VALUES ('interpretation-location', 'interpretation-artifact',
                      'row', '{}', 'exact')"""
        )
        connection.execute(
            """INSERT INTO dataset_sources (
                dataset_source_id, dataset_version_id, source_file_id, artifact_id
            ) VALUES ('interpretation-source', 'interpretation-version', 'source-1',
                      'interpretation-artifact')"""
        )
        connection.execute(
            """INSERT INTO interpretations (
                interpretation_id, workspace_id, dataset_version_id, source_location_id,
                proposed_field, proposed_value_json, method, status
            ) VALUES ('version-interpretation', 'default', 'interpretation-version',
                      'interpretation-location', 'type', '"panel"', 'human', 'suggested')"""
        )
        with pytest.raises(sqlite3.IntegrityError, match="dataset source cannot change"):
            connection.execute(
                "UPDATE dataset_sources SET artifact_id = NULL "
                "WHERE dataset_source_id = 'interpretation-source'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="dataset source cannot be deleted"):
            connection.execute(
                "DELETE FROM dataset_sources WHERE dataset_source_id = 'interpretation-source'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="owner cannot change after linkage"):
            connection.execute(
                "UPDATE dataset_versions SET dataset_id = 'replacement-dataset' "
                "WHERE dataset_version_id = 'interpretation-version'"
            )


def test_validation_run_identity_and_approved_outcome_are_immutable(tmp_path):
    database_path = tmp_path / "validation-parent.db"
    initialize_database(database_path)
    _, repository = _approved_source(tmp_path, database_path)
    other_approval, other_version = _add_approved_version(
        repository, "source-1", suffix="validation-parent"
    )
    assert other_approval == "approval-validation-parent"
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="validation run dataset version"):
            connection.execute(
                "UPDATE validation_runs SET dataset_version_id = ? "
                "WHERE validation_run_id = 'validation-1'",
                (other_version,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE approvals SET validation_run_id = 'validation-validation-parent' "
                "WHERE approval_id = 'approval-1'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="outcome is immutable"):
            connection.execute(
                "UPDATE validation_runs SET outcome = 'failed' "
                "WHERE validation_run_id = 'validation-1'"
            )


def test_approval_insert_requires_matching_dataset_version_and_validation(tmp_path):
    database_path = tmp_path / "approval-parent.db"
    initialize_database(database_path)
    _, repository = _approved_source(tmp_path, database_path)
    _, other_version = _add_approved_version(repository, "source-1", suffix="approval-parent")
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="version and validation run must agree"):
            connection.execute(
                """INSERT INTO approvals (
                    approval_id, dataset_version_id, validation_run_id, state
                ) VALUES ('mismatched-approval', 'dataset-version-1',
                          'validation-approval-parent', 'pending')"""
            )
    assert other_version == "dataset-version-approval-parent"


def test_promotion_json_result_must_be_an_object(tmp_path):
    database_path = tmp_path / "promotion-invalid-json.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    import_data(source, database_path)
    _insert_queued_promotion(database_path, source, "invalid-json")
    with get_connection(database_path) as connection:
        connection.execute(
            "UPDATE promotion_operations SET state = 'running' WHERE promotion_id = 'invalid-json'"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """UPDATE promotion_operations
                   SET state = 'succeeded', outcome = 'already_imported', result_json = 'not json'
                   WHERE promotion_id = 'invalid-json'"""
            )


@pytest.mark.parametrize(
    "mutation",
    ["missing_promotion_id", "mismatched_promotion_id", "mismatched_outcome",
     "mismatched_source_name", "mismatched_record_count"],
)
def test_successful_promotion_requires_matching_result_identity(tmp_path, mutation):
    database_path = tmp_path / f"promotion-result-{mutation}.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    import_data(source, database_path)
    valid_result = import_data(
        source, database_path, promotion_id="valid-duplicate", approval_id="approval-1"
    )
    inconsistent_result = dict(valid_result)
    if mutation == "missing_promotion_id":
        del inconsistent_result["promotion_id"]
    elif mutation == "mismatched_promotion_id":
        inconsistent_result["promotion_id"] = "another-promotion"
    elif mutation == "mismatched_outcome":
        inconsistent_result["outcome"] = "imported"
    elif mutation == "mismatched_source_name":
        inconsistent_result["source_name"] = "different-source.json"
    else:
        inconsistent_result["assets"] = 1
    _insert_queued_promotion(database_path, source, "invalid-result")

    with get_connection(database_path) as connection:
        connection.execute(
            "UPDATE promotion_operations SET state = 'running' "
            "WHERE promotion_id = 'invalid-result'"
        )
        with pytest.raises(sqlite3.IntegrityError, match="does not match its operation"):
            connection.execute(
                """UPDATE promotion_operations
                   SET state = 'succeeded', outcome = 'already_imported', result_json = ?
                   WHERE promotion_id = 'invalid-result'""",
                (json.dumps(inconsistent_result, sort_keys=True),),
            )


def test_failed_promotion_rolls_back_domain_rows_and_marker(tmp_path):
    database_path = tmp_path / "promotion-rollback.db"
    initialize_database(database_path)
    data = _dataset()
    data["assets"].append({
        "external_id": "PROMO-02", "asset_type": "panel", "name": "Conflicting panel",
    })
    source, _ = _approved_source(tmp_path, database_path, data)
    with get_connection(database_path) as connection:
        connection.execute(
            "INSERT INTO assets (external_id, asset_type, name) VALUES ('PROMO-02', 'panel', 'Existing conflict')"
        )

    with pytest.raises(sqlite3.IntegrityError):
        import_data(source, database_path, promotion_id="promotion-fail", approval_id="approval-1")
    with get_connection(database_path) as connection:
        operation = connection.execute(
            "SELECT state, outcome, error_code, error_message FROM promotion_operations WHERE promotion_id = 'promotion-fail'"
        ).fetchone()
        assert operation["state"] == "terminal_failure"
        assert operation["outcome"] is None
        assert operation["error_code"] == "constraint_violation"
        assert len(operation["error_message"]) <= 120
        remaining_asset_ids = [
            row[0] for row in connection.execute(
                "SELECT external_id FROM assets ORDER BY external_id"
            ).fetchall()
        ]
        assert remaining_asset_ids == ["PROMO-02"]
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM imported_files").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM inspections").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM maintenance_records").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM promotion_domain_records").fetchone()[0] == 0


def test_promotion_requires_approval_and_rejects_key_reuse(tmp_path):
    database_path = tmp_path / "promotion-approval.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    with pytest.raises(ValueError, match="approved"):
        import_data(source, database_path, promotion_id="missing-approval")

    result = import_data(source, database_path, promotion_id="promotion-3", approval_id="approval-1")
    changed_data = _dataset() | {"maintenance": [{
        "asset_external_id": "PROMO-01", "performed_at": "2026-10-02T10:00:00Z",
        "maintenance_type": "cleaning", "description": "Changed", "data_origin": "synthetic",
    }]}
    source.write_text(json.dumps(changed_data), encoding="utf-8")
    with pytest.raises(ValueError, match="conflict"):
        import_data(source, database_path, promotion_id="promotion-3", approval_id="approval-1")
    assert result["outcome"] == "imported"


def test_promotion_rejects_content_not_linked_to_approved_source(tmp_path):
    database_path = tmp_path / "unlinked-content.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    changed_content = tmp_path / "unlinked.json"
    changed_data = _dataset()
    changed_data["assets"][0]["name"] = "Different solar panel"
    changed_content.write_text(json.dumps(changed_data), encoding="utf-8")

    with pytest.raises(ValueError, match="link to this source content"):
        import_data(
            changed_content,
            database_path,
            promotion_id="unlinked-content",
            approval_id="approval-1",
        )

    with get_connection(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM promotion_operations"
        ).fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_approval_must_be_operator_decided_to_promote(tmp_path):
    database_path = tmp_path / "approval-decision.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)

    for field in ("decision_by", "decision_at"):
        if field == "decision_at":
            with get_connection(database_path) as connection:
                connection.execute(
                    "UPDATE approvals SET decision_by = 'local-operator' "
                    "WHERE approval_id = 'approval-1'"
                )
        with get_connection(database_path) as connection:
            connection.execute(
                f"UPDATE approvals SET {field} = NULL WHERE approval_id = 'approval-1'"
            )
        with pytest.raises(ValueError, match="operator decision"):
            import_data(
                source, database_path,
                promotion_id=f"missing-{field}", approval_id="approval-1",
            )


def test_database_rejects_direct_approved_approval_insert(tmp_path):
    database_path = tmp_path / "direct-approved-insert.db"
    initialize_database(database_path)
    _approved_source(tmp_path, database_path)
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="must start pending"):
            connection.execute(
                """INSERT INTO approvals (
                    approval_id, dataset_version_id, validation_run_id, state
                ) VALUES ('forged-approval', 'dataset-version-1', 'validation-1', 'approved')"""
            )


def test_pending_approval_can_be_approved_with_operator_decision(tmp_path):
    database_path = tmp_path / "normal-approval.db"
    initialize_database(database_path)
    _approved_source(tmp_path, database_path)
    with get_connection(database_path) as connection:
        row = connection.execute(
            "SELECT state, decision_by, decision_at FROM approvals WHERE approval_id = ?",
            ("approval-1",),
        ).fetchone()
        assert row["state"] == "approved"
        assert row["decision_by"] == "local-operator"
        assert row["decision_at"] is not None


def test_promotion_identity_fingerprint_distinguishes_dataset_version():
    original = _promotion_request_fingerprint(
        source_hash="sha256", source_name="source.json", approval_id="approval",
        dataset_id="dataset", dataset_version_id="version-1",
    )
    changed_version = _promotion_request_fingerprint(
        source_hash="sha256", source_name="source.json", approval_id="approval",
        dataset_id="dataset", dataset_version_id="version-2",
    )
    assert original != changed_version


def test_promotion_identity_rejects_changed_source_name_approval_and_version(tmp_path):
    database_path = tmp_path / "identity.db"
    initialize_database(database_path)
    source, repository = _approved_source(tmp_path, database_path)
    import_data(source, database_path, promotion_id="identity-1", approval_id="approval-1")

    renamed = tmp_path / "renamed.json"
    renamed.write_bytes(source.read_bytes())
    with pytest.raises(ValueError, match="source name"):
        import_data(renamed, database_path, promotion_id="identity-1", approval_id="approval-1")

    repository.create_approval(
        approval_id="approval-alias", dataset_version_id="dataset-version-1",
        validation_run_id="validation-1",
    )
    repository.transition_approval(
        "approval-alias", ApprovalState.APPROVED, decision_by="local-operator"
    )
    with pytest.raises(ValueError, match="approval"):
        import_data(source, database_path, promotion_id="identity-1", approval_id="approval-alias")

    alternate_approval, alternate_version = _add_approved_version(
        repository, "source-1", suffix="alternate"
    )
    assert alternate_version != "dataset-version-1"
    with pytest.raises(ValueError, match="approval"):
        import_data(source, database_path, promotion_id="identity-1", approval_id=alternate_approval)


def test_approval_can_be_revoked_before_promotion_starts(tmp_path):
    database_path = tmp_path / "revoke-before.db"
    initialize_database(database_path)
    source, repository = _approved_source(tmp_path, database_path)
    revoked = repository.transition_approval("approval-1", ApprovalState.REVOKED)
    assert revoked.state == ApprovalState.REVOKED
    with pytest.raises(ValueError, match="approved"):
        import_data(source, database_path, promotion_id="revoked-promotion", approval_id="approval-1")


@pytest.mark.parametrize("state", ["queued", "running"])
def test_approval_cannot_be_revoked_after_promotion_is_queued(tmp_path, state):
    database_path = tmp_path / f"revoke-{state}.db"
    initialize_database(database_path)
    source, repository = _approved_source(tmp_path, database_path)
    _insert_queued_promotion(database_path, source, f"revoke-{state}")
    if state == "running":
        with get_connection(database_path) as connection:
            connection.execute(
                "UPDATE promotion_operations SET state = 'running' WHERE promotion_id = ?",
                (f"revoke-{state}",),
            )
    with pytest.raises(ApprovalRevocationError, match="corrective operation"):
        repository.transition_approval("approval-1", ApprovalState.REVOKED)
    with get_connection(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="cannot be revoked"):
            connection.execute(
                "UPDATE approvals SET state = 'revoked' WHERE approval_id = 'approval-1'"
            )


def test_approval_cannot_be_revoked_after_successful_promotion(tmp_path):
    database_path = tmp_path / "revoke-after-success.db"
    initialize_database(database_path)
    source, repository = _approved_source(tmp_path, database_path)
    import_data(source, database_path, promotion_id="successful", approval_id="approval-1")
    with pytest.raises(ApprovalRevocationError, match="corrective operation"):
        repository.transition_approval("approval-1", ApprovalState.REVOKED)


def test_racing_approval_revocation_and_promotion_cannot_both_succeed(tmp_path):
    database_path = tmp_path / "revoke-race.db"
    initialize_database(database_path)
    source, repository = _approved_source(tmp_path, database_path)
    barrier = Barrier(2)

    def revoke():
        barrier.wait()
        try:
            return repository.transition_approval("approval-1", ApprovalState.REVOKED)
        except Exception as error:
            return error

    def promote():
        barrier.wait()
        try:
            return import_data(source, database_path, promotion_id="race", approval_id="approval-1")
        except Exception as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        revoke_result, promotion_result = list(executor.map(lambda fn: fn(), (revoke, promote)))
    if isinstance(revoke_result, ApprovalRevocationError):
        assert not isinstance(promotion_result, Exception)
        assert promotion_result["outcome"] == "imported"
    else:
        assert revoke_result.state == ApprovalState.REVOKED
        assert isinstance(promotion_result, ValueError)
        with get_connection(database_path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_retryable_promotion_retries_but_interrupted_attempt_requires_reconciliation(tmp_path):
    database_path = tmp_path / "promotion-recovery.db"
    initialize_database(database_path)
    source, _ = _approved_source(tmp_path, database_path)
    _insert_queued_promotion(database_path, source, "retryable")
    with get_connection(database_path) as connection:
        connection.execute(
            "UPDATE promotion_operations SET state = 'retryable_failure', "
            "error_code = 'database_busy' WHERE promotion_id = 'retryable'"
        )
    retried = import_data(source, database_path, promotion_id="retryable", approval_id="approval-1")
    assert retried["outcome"] == "imported"

    _insert_queued_promotion(database_path, source, "interrupted")
    with get_connection(database_path) as connection:
        connection.execute(
            "UPDATE promotion_operations SET state = 'running', attempt_count = 1 "
            "WHERE promotion_id = 'interrupted'"
        )
    with pytest.raises(ValueError, match="explicit reconciliation"):
        import_data(source, database_path, promotion_id="interrupted", approval_id="approval-1")
    with get_connection(database_path) as connection:
        row = connection.execute(
            "SELECT state, outcome, attempt_count FROM promotion_operations "
            "WHERE promotion_id = 'interrupted'"
        ).fetchone()
        assert (row["state"], row["outcome"], row["attempt_count"]) == (
            "reconciliation_required", None, 1
        )


def test_importer_backward_compatible_without_promotion(tmp_path):
    database_path = tmp_path / "legacy-call.db"
    initialize_database(database_path)
    source = tmp_path / "legacy-call.json"
    source.write_text(json.dumps(_dataset()), encoding="utf-8")
    first = import_data(source, database_path)
    duplicate = import_data(source, database_path)
    assert first == {
        "status": "imported", "source_name": "legacy-call.json",
        "assets": 1, "inspections": 1, "observations": 1, "maintenance": 1,
        "message": "Import completed successfully.",
    }
    assert duplicate == {
        "status": "skipped", "source_name": "legacy-call.json",
        "assets": 0, "inspections": 0, "observations": 0, "maintenance": 0,
        "message": "Identical source content has already been imported.",
    }
