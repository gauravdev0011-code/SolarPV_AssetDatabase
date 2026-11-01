-- Additive metadata for the local-first ingestion workspace.
-- Solar PV domain tables from 001 are intentionally unchanged.

CREATE TABLE IF NOT EXISTS imported_files (
    source_hash TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workspaces (
    workspace_id TEXT PRIMARY KEY,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO workspaces (workspace_id, name)
VALUES ('default', 'Default Workspace');

CREATE TABLE IF NOT EXISTS source_files (
    source_file_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    original_name TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    media_type TEXT,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('receiving', 'quarantined', 'available', 'intake_rejected', 'deletion_pending', 'deleted')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (workspace_id, sha256),
    UNIQUE (workspace_id, storage_key)
);

CREATE TABLE IF NOT EXISTS ingestion_jobs (
    job_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    source_file_id TEXT NOT NULL REFERENCES source_files(source_file_id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'retry_wait', 'succeeded', 'failed_terminal', 'cancel_requested', 'cancelled')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    idempotency_key TEXT NOT NULL,
    progress_stage TEXT,
    error_code TEXT,
    error_message TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (workspace_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS job_attempts (
    job_id TEXT NOT NULL REFERENCES ingestion_jobs(job_id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    parser_id TEXT,
    parser_version TEXT,
    started_at TEXT,
    finished_at TEXT,
    outcome TEXT CHECK (outcome IS NULL OR outcome IN ('succeeded', 'retryable_failure', 'terminal_failure', 'cancelled')),
    error_code TEXT,
    error_message TEXT,
    PRIMARY KEY (job_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS extraction_artifacts (
    artifact_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    source_file_id TEXT NOT NULL REFERENCES source_files(source_file_id) ON DELETE RESTRICT,
    job_id TEXT NOT NULL REFERENCES ingestion_jobs(job_id) ON DELETE RESTRICT,
    artifact_type TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    parser_id TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (workspace_id, storage_key)
);

CREATE TABLE IF NOT EXISTS source_locations (
    source_location_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES extraction_artifacts(artifact_id) ON DELETE RESTRICT,
    locator_type TEXT NOT NULL,
    locator_json TEXT NOT NULL,
    precision TEXT NOT NULL CHECK (precision IN ('exact', 'approximate', 'limited'))
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dataset_versions (
    dataset_version_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    version_number INTEGER NOT NULL CHECK (version_number > 0),
    state TEXT NOT NULL CHECK (state IN ('draft', 'validating', 'valid', 'invalid', 'archived')),
    artifact_key TEXT,
    checksum TEXT,
    schema_version TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_id, version_number)
);

CREATE TABLE IF NOT EXISTS dataset_sources (
    dataset_source_id TEXT PRIMARY KEY,
    dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE RESTRICT,
    source_file_id TEXT NOT NULL REFERENCES source_files(source_file_id) ON DELETE RESTRICT,
    artifact_id TEXT REFERENCES extraction_artifacts(artifact_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS interpretations (
    interpretation_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    dataset_version_id TEXT REFERENCES dataset_versions(dataset_version_id) ON DELETE RESTRICT,
    source_location_id TEXT NOT NULL REFERENCES source_locations(source_location_id) ON DELETE RESTRICT,
    proposed_field TEXT NOT NULL,
    proposed_value_json TEXT NOT NULL,
    method TEXT NOT NULL,
    model_version TEXT,
    confidence REAL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    status TEXT NOT NULL CHECK (status IN ('suggested', 'accepted', 'rejected')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS validation_runs (
    validation_run_id TEXT PRIMARY KEY,
    dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE RESTRICT,
    rule_set_version TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('passed', 'failed', 'warnings')),
    result_key TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id TEXT PRIMARY KEY,
    dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE RESTRICT,
    validation_run_id TEXT NOT NULL REFERENCES validation_runs(validation_run_id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('pending', 'approved', 'rejected', 'cancelled', 'revoked')),
    decision_by TEXT,
    decision_at TEXT,
    destination TEXT NOT NULL DEFAULT 'solarpv_domain',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS promotion_operations (
    promotion_id TEXT PRIMARY KEY,
    approval_id TEXT NOT NULL REFERENCES approvals(approval_id) ON DELETE RESTRICT,
    source_hash TEXT NOT NULL,
    source_name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'retryable_failure', 'terminal_failure', 'reconciliation_required', 'succeeded', 'cancelled')),
    outcome TEXT CHECK (outcome IS NULL OR outcome IN ('imported', 'already_imported')),
    result_json TEXT,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS promotion_domain_records (
    promotion_id TEXT NOT NULL REFERENCES promotion_operations(promotion_id) ON DELETE RESTRICT,
    record_type TEXT NOT NULL CHECK (record_type IN ('asset', 'inspection', 'observation', 'maintenance')),
    record_id INTEGER NOT NULL,
    source_location_id TEXT REFERENCES source_locations(source_location_id) ON DELETE RESTRICT,
    PRIMARY KEY (promotion_id, record_type, record_id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    audit_event_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_source_files_workspace_state
    ON source_files(workspace_id, state);
CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_state_created
    ON ingestion_jobs(state, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_source_file
    ON ingestion_jobs(source_file_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_source_file
    ON extraction_artifacts(source_file_id);
CREATE INDEX IF NOT EXISTS idx_dataset_versions_state
    ON dataset_versions(state);
CREATE INDEX IF NOT EXISTS idx_approvals_state
    ON approvals(state);
CREATE INDEX IF NOT EXISTS idx_interpretations_dataset
    ON interpretations(dataset_version_id, status);
