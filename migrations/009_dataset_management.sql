-- Additive dataset registration, mapping, and validation provenance.

CREATE TABLE IF NOT EXISTS dataset_version_requests (
    request_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE RESTRICT,
    dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE RESTRICT,
    job_id TEXT NOT NULL REFERENCES ingestion_jobs(job_id) ON DELETE RESTRICT,
    idempotency_key TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
    request_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (workspace_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS dataset_mapping_revisions (
    mapping_revision_id TEXT PRIMARY KEY,
    dataset_version_id TEXT NOT NULL REFERENCES dataset_versions(dataset_version_id) ON DELETE RESTRICT,
    revision_number INTEGER NOT NULL CHECK (revision_number > 0),
    fingerprint TEXT NOT NULL,
    idempotency_key TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_version_id, revision_number),
    UNIQUE (dataset_version_id, idempotency_key),
    UNIQUE (mapping_revision_id, dataset_version_id)
);

CREATE TABLE IF NOT EXISTS dataset_column_mappings (
    column_mapping_id TEXT PRIMARY KEY,
    mapping_revision_id TEXT NOT NULL REFERENCES dataset_mapping_revisions(mapping_revision_id) ON DELETE RESTRICT,
    source_column TEXT NOT NULL CHECK (length(trim(source_column)) > 0),
    target_field TEXT NOT NULL CHECK (length(trim(target_field)) > 0),
    expected_type TEXT NOT NULL CHECK (expected_type IN ('string', 'integer', 'number', 'boolean', 'date', 'datetime')),
    is_required INTEGER NOT NULL DEFAULT 0 CHECK (is_required IN (0, 1)),
    measurement_unit TEXT,
    timestamp_format TEXT,
    timezone TEXT,
    UNIQUE (mapping_revision_id, source_column),
    UNIQUE (mapping_revision_id, target_field)
);

ALTER TABLE datasets ADD COLUMN updated_at TEXT;
UPDATE datasets SET updated_at = created_at WHERE updated_at IS NULL;

ALTER TABLE dataset_versions ADD COLUMN source_job_id TEXT
    REFERENCES ingestion_jobs(job_id) ON DELETE RESTRICT;
ALTER TABLE dataset_versions ADD COLUMN source_artifact_id TEXT
    REFERENCES extraction_artifacts(artifact_id) ON DELETE RESTRICT;
ALTER TABLE dataset_versions ADD COLUMN mapping_revision_id TEXT
    REFERENCES dataset_mapping_revisions(mapping_revision_id) ON DELETE RESTRICT;
ALTER TABLE dataset_versions ADD COLUMN current_validation_run_id TEXT
    REFERENCES validation_runs(validation_run_id) ON DELETE RESTRICT;
ALTER TABLE dataset_versions ADD COLUMN source_sha256 TEXT;
ALTER TABLE dataset_versions ADD COLUMN row_count INTEGER
    CHECK (row_count IS NULL OR row_count >= 0);
ALTER TABLE dataset_versions ADD COLUMN profile_json TEXT;

ALTER TABLE validation_runs ADD COLUMN source_sha256 TEXT;
ALTER TABLE validation_runs ADD COLUMN source_file_id TEXT
    REFERENCES source_files(source_file_id) ON DELETE RESTRICT;
ALTER TABLE validation_runs ADD COLUMN source_artifact_id TEXT
    REFERENCES extraction_artifacts(artifact_id) ON DELETE RESTRICT;
ALTER TABLE validation_runs ADD COLUMN parser_id TEXT;
ALTER TABLE validation_runs ADD COLUMN parser_version TEXT;
ALTER TABLE validation_runs ADD COLUMN processing_result_checksum TEXT;
ALTER TABLE validation_runs ADD COLUMN result_schema_version TEXT;
ALTER TABLE validation_runs ADD COLUMN mapping_revision_id TEXT
    REFERENCES dataset_mapping_revisions(mapping_revision_id) ON DELETE RESTRICT;
ALTER TABLE validation_runs ADD COLUMN input_fingerprint TEXT;
ALTER TABLE validation_runs ADD COLUMN result_checksum TEXT;
ALTER TABLE validation_runs ADD COLUMN error_count INTEGER NOT NULL DEFAULT 0
    CHECK (error_count >= 0);
ALTER TABLE validation_runs ADD COLUMN warning_count INTEGER NOT NULL DEFAULT 0
    CHECK (warning_count >= 0);
ALTER TABLE validation_runs ADD COLUMN idempotency_key TEXT;
ALTER TABLE approvals ADD COLUMN idempotency_key TEXT;

CREATE INDEX IF NOT EXISTS idx_dataset_versions_job
    ON dataset_versions(source_job_id);
CREATE INDEX IF NOT EXISTS idx_dataset_versions_latest
    ON dataset_versions(dataset_id, version_number DESC);
CREATE INDEX IF NOT EXISTS idx_dataset_requests_dataset
    ON dataset_version_requests(dataset_id, dataset_version_id);
CREATE INDEX IF NOT EXISTS idx_mapping_revision_version
    ON dataset_mapping_revisions(dataset_version_id, revision_number);
CREATE INDEX IF NOT EXISTS idx_validation_version_created
    ON validation_runs(dataset_version_id, created_at);

CREATE TRIGGER IF NOT EXISTS trg_dataset_updated_at_insert
AFTER INSERT ON datasets
WHEN NEW.updated_at IS NULL
BEGIN
    UPDATE datasets SET updated_at = NEW.created_at WHERE dataset_id = NEW.dataset_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_updated_at_change
AFTER UPDATE ON datasets
WHEN NEW.updated_at IS OLD.updated_at
BEGIN
    UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE dataset_id = NEW.dataset_id;
END;
CREATE UNIQUE INDEX IF NOT EXISTS idx_validation_idempotency
    ON validation_runs(dataset_version_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_approval_idempotency
    ON approvals(dataset_version_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Keep old Phase 1 records usable; the stronger provenance rule applies to
-- dataset versions registered through the new source_job_id workflow.
CREATE TRIGGER IF NOT EXISTS trg_dataset_version_source_matches_job_insert
BEFORE INSERT ON dataset_versions
WHEN NEW.source_job_id IS NOT NULL
 AND NOT EXISTS (
     SELECT 1 FROM datasets AS dataset
     JOIN ingestion_jobs AS job ON job.job_id = NEW.source_job_id
     JOIN source_files AS file ON file.source_file_id = job.source_file_id
     JOIN extraction_artifacts AS artifact
       ON artifact.artifact_id = NEW.source_artifact_id
      AND artifact.job_id = job.job_id
      AND artifact.source_file_id = file.source_file_id
     WHERE dataset.dataset_id = NEW.dataset_id
       AND dataset.workspace_id = job.workspace_id
       AND file.workspace_id = job.workspace_id
       AND job.state = 'succeeded'
       AND artifact.artifact_type = 'processing_result'
       AND NEW.source_sha256 = file.sha256
       AND NEW.checksum = file.sha256
       AND NEW.artifact_key = artifact.storage_key
       AND NEW.schema_version = artifact.schema_version
 )
BEGIN
    SELECT RAISE(ABORT, 'dataset version requires a successful matching processing job');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_version_source_immutable
BEFORE UPDATE OF dataset_id, version_number, source_job_id, source_artifact_id,
                 source_sha256, artifact_key, checksum, schema_version, row_count,
                 profile_json
ON dataset_versions
WHEN OLD.source_job_id IS NOT NULL AND (
     NEW.dataset_id IS NOT OLD.dataset_id
     OR NEW.version_number IS NOT OLD.version_number
     OR NEW.source_job_id IS NOT OLD.source_job_id
     OR NEW.source_artifact_id IS NOT OLD.source_artifact_id
     OR NEW.source_sha256 IS NOT OLD.source_sha256
     OR NEW.artifact_key IS NOT OLD.artifact_key
     OR NEW.checksum IS NOT OLD.checksum
     OR NEW.schema_version IS NOT OLD.schema_version
     OR NEW.row_count IS NOT OLD.row_count
     OR NEW.profile_json IS NOT OLD.profile_json
)
BEGIN
    SELECT RAISE(ABORT, 'registered dataset version source identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_version_mapping_consistent
BEFORE UPDATE OF mapping_revision_id ON dataset_versions
WHEN NEW.mapping_revision_id IS NOT NULL
 AND NOT EXISTS (
     SELECT 1 FROM dataset_mapping_revisions AS revision
     WHERE revision.mapping_revision_id = NEW.mapping_revision_id
       AND revision.dataset_version_id = NEW.dataset_version_id
 )
BEGIN
    SELECT RAISE(ABORT, 'mapping revision must belong to the dataset version');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_mapping_revision_mutable
BEFORE INSERT ON dataset_mapping_revisions
WHEN NOT EXISTS (
    SELECT 1 FROM dataset_versions
    WHERE dataset_version_id = NEW.dataset_version_id AND state = 'draft'
)
BEGIN
    SELECT RAISE(ABORT, 'mappings can only be revised while a dataset version is draft');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_mapping_revision_no_update
BEFORE UPDATE ON dataset_mapping_revisions
BEGIN
    SELECT RAISE(ABORT, 'mapping revisions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_mapping_revision_no_delete
BEFORE DELETE ON dataset_mapping_revisions
BEGIN
    SELECT RAISE(ABORT, 'mapping revisions are retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_column_mapping_mutable_insert
BEFORE INSERT ON dataset_column_mappings
WHEN NOT EXISTS (
    SELECT 1 FROM dataset_mapping_revisions AS revision
    JOIN dataset_versions AS version
      ON version.dataset_version_id = revision.dataset_version_id
    WHERE revision.mapping_revision_id = NEW.mapping_revision_id
      AND version.state = 'draft'
)
BEGIN
    SELECT RAISE(ABORT, 'column mappings can only be added to draft versions');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_column_mapping_no_update
BEFORE UPDATE ON dataset_column_mappings
BEGIN
    SELECT RAISE(ABORT, 'column mappings are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_column_mapping_no_delete
BEFORE DELETE ON dataset_column_mappings
BEGIN
    SELECT RAISE(ABORT, 'column mappings are retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_version_mapping_draft_only
BEFORE UPDATE OF mapping_revision_id ON dataset_versions
WHEN OLD.state <> 'draft' AND NEW.mapping_revision_id IS NOT OLD.mapping_revision_id
BEGIN
    SELECT RAISE(ABORT, 'mapping revision is frozen after validation starts');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_registered_source_link_matches_version
BEFORE INSERT ON dataset_sources
WHEN EXISTS (
    SELECT 1 FROM dataset_versions version
    JOIN ingestion_jobs job ON job.job_id = version.source_job_id
    WHERE version.dataset_version_id = NEW.dataset_version_id
      AND version.source_job_id IS NOT NULL
      AND (NEW.source_file_id <> job.source_file_id
           OR NEW.artifact_id IS NOT version.source_artifact_id)
)
BEGIN
    SELECT RAISE(ABORT, 'dataset source link must match its registered processing job');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_registered_single_source
BEFORE INSERT ON dataset_sources
WHEN EXISTS (
    SELECT 1 FROM dataset_versions
    WHERE dataset_version_id = NEW.dataset_version_id AND source_job_id IS NOT NULL
)
 AND EXISTS (
    SELECT 1 FROM dataset_sources WHERE dataset_version_id = NEW.dataset_version_id
 )
BEGIN
    SELECT RAISE(ABORT, 'registered dataset version has one immutable processing source');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_registered_artifact_immutable
BEFORE UPDATE OF workspace_id, source_file_id, job_id, artifact_type, storage_key,
                 parser_id, parser_version, schema_version, checksum
ON extraction_artifacts
WHEN EXISTS (
    SELECT 1 FROM dataset_versions
    WHERE source_artifact_id = OLD.artifact_id AND source_job_id IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'processing artifact provenance is immutable after dataset registration');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_validation_run_consistent
BEFORE INSERT ON validation_runs
WHEN EXISTS (
    SELECT 1 FROM dataset_versions AS version
    WHERE version.dataset_version_id = NEW.dataset_version_id
      AND version.source_job_id IS NOT NULL
      AND NOT (
          NEW.source_sha256 IS NOT NULL
          AND NEW.source_file_id IS NOT NULL
          AND NEW.source_artifact_id IS NOT NULL
          AND NEW.parser_id IS NOT NULL
          AND NEW.parser_version IS NOT NULL
          AND NEW.processing_result_checksum IS NOT NULL
          AND NEW.result_schema_version IS NOT NULL
          AND NEW.input_fingerprint IS NOT NULL
          AND NEW.result_key IS NOT NULL
          AND NEW.result_checksum IS NOT NULL
          AND NEW.rule_set_version = 'solarpv.dataset-rules-v1'
          AND NEW.mapping_revision_id IS version.mapping_revision_id
          AND EXISTS (
              SELECT 1 FROM dataset_sources AS source
              JOIN source_files AS file ON file.source_file_id = source.source_file_id
              JOIN extraction_artifacts AS artifact
                ON artifact.artifact_id = source.artifact_id
              WHERE source.dataset_version_id = version.dataset_version_id
                AND file.sha256 = NEW.source_sha256
                AND artifact.parser_id = NEW.parser_id
                AND artifact.parser_version = NEW.parser_version
                AND artifact.artifact_id = version.source_artifact_id
                AND source.source_file_id = NEW.source_file_id
                AND artifact.artifact_id = NEW.source_artifact_id
                AND artifact.checksum = NEW.processing_result_checksum
                AND artifact.schema_version = NEW.result_schema_version
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'validation run provenance does not match its dataset version');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_validation_counts_consistent
BEFORE INSERT ON validation_runs
WHEN EXISTS (
    SELECT 1 FROM dataset_versions
    WHERE dataset_version_id = NEW.dataset_version_id AND source_job_id IS NOT NULL
)
 AND NOT (
     (NEW.outcome = 'passed' AND NEW.error_count = 0 AND NEW.warning_count = 0)
     OR (NEW.outcome = 'warnings' AND NEW.error_count = 0 AND NEW.warning_count > 0)
     OR (NEW.outcome = 'failed' AND NEW.error_count > 0)
 )
BEGIN
    SELECT RAISE(ABORT, 'validation outcome must agree with its error and warning counts');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_validation_result_immutable
BEFORE UPDATE OF dataset_version_id, rule_set_version, outcome, result_key,
                 source_sha256, source_file_id, source_artifact_id, parser_id, parser_version,
                 processing_result_checksum, result_schema_version, mapping_revision_id,
                 input_fingerprint, result_checksum, error_count, warning_count,
                 idempotency_key
ON validation_runs
WHEN OLD.validation_run_id IS NOT NULL AND (
     NEW.dataset_version_id IS NOT OLD.dataset_version_id
     OR NEW.rule_set_version IS NOT OLD.rule_set_version
     OR NEW.outcome IS NOT OLD.outcome
     OR NEW.result_key IS NOT OLD.result_key
     OR NEW.source_sha256 IS NOT OLD.source_sha256
     OR NEW.source_file_id IS NOT OLD.source_file_id
     OR NEW.source_artifact_id IS NOT OLD.source_artifact_id
     OR NEW.parser_id IS NOT OLD.parser_id
     OR NEW.parser_version IS NOT OLD.parser_version
     OR NEW.processing_result_checksum IS NOT OLD.processing_result_checksum
     OR NEW.result_schema_version IS NOT OLD.result_schema_version
     OR NEW.mapping_revision_id IS NOT OLD.mapping_revision_id
     OR NEW.input_fingerprint IS NOT OLD.input_fingerprint
     OR NEW.result_checksum IS NOT OLD.result_checksum
     OR NEW.error_count IS NOT OLD.error_count
     OR NEW.warning_count IS NOT OLD.warning_count
     OR NEW.idempotency_key IS NOT OLD.idempotency_key
)
BEGIN
    SELECT RAISE(ABORT, 'validation result evidence is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_validation_no_delete
BEFORE DELETE ON validation_runs
WHEN OLD.dataset_version_id IN (
    SELECT dataset_version_id FROM dataset_versions WHERE source_job_id IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'dataset validation evidence is retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_current_validation_consistent
BEFORE UPDATE OF current_validation_run_id ON dataset_versions
WHEN NEW.current_validation_run_id IS NOT NULL
 AND NOT EXISTS (
     SELECT 1 FROM validation_runs AS validation
     WHERE validation.validation_run_id = NEW.current_validation_run_id
       AND validation.dataset_version_id = NEW.dataset_version_id
       AND validation.source_sha256 = (
           SELECT file.sha256 FROM dataset_sources AS source
           JOIN source_files AS file ON file.source_file_id = source.source_file_id
           WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
       )
       AND validation.source_file_id = (
           SELECT source.source_file_id FROM dataset_sources AS source
           WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
       )
       AND validation.source_artifact_id = (
           SELECT source.artifact_id FROM dataset_sources AS source
           WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
       )
       AND validation.parser_id = (
           SELECT artifact.parser_id FROM dataset_sources AS source
           JOIN extraction_artifacts AS artifact ON artifact.artifact_id = source.artifact_id
           WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
       )
       AND validation.parser_version = (
           SELECT artifact.parser_version FROM dataset_sources AS source
           JOIN extraction_artifacts AS artifact ON artifact.artifact_id = source.artifact_id
           WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
       )
       AND validation.processing_result_checksum = (
           SELECT artifact.checksum FROM dataset_sources AS source
           JOIN extraction_artifacts AS artifact ON artifact.artifact_id = source.artifact_id
           WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
       )
       AND validation.result_schema_version = (
           SELECT artifact.schema_version FROM dataset_sources AS source
           JOIN extraction_artifacts AS artifact ON artifact.artifact_id = source.artifact_id
           WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
       )
       AND validation.rule_set_version = 'solarpv.dataset-rules-v1'
       AND validation.mapping_revision_id IS NEW.mapping_revision_id
 )
BEGIN
    SELECT RAISE(ABORT, 'current validation does not match the dataset version inputs');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_approval_validation_current
BEFORE INSERT ON approvals
WHEN EXISTS (
    SELECT 1 FROM dataset_versions AS version
    WHERE version.dataset_version_id = NEW.dataset_version_id
      AND version.source_job_id IS NOT NULL
      AND (
          version.state <> 'valid'
          OR version.current_validation_run_id IS NOT NEW.validation_run_id
          OR NOT EXISTS (
              SELECT 1 FROM validation_runs AS validation
              WHERE validation.validation_run_id = NEW.validation_run_id
                AND validation.dataset_version_id = NEW.dataset_version_id
                AND validation.outcome IN ('passed', 'warnings')
                AND validation.rule_set_version = 'solarpv.dataset-rules-v1'
                AND validation.error_count = 0
                AND validation.source_sha256 = (
                    SELECT file.sha256 FROM dataset_sources AS source
                    JOIN source_files AS file ON file.source_file_id = source.source_file_id
                    WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
                )
                AND validation.source_file_id = (
                    SELECT source.source_file_id FROM dataset_sources AS source
                    WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
                )
                AND validation.source_artifact_id = (
                    SELECT source.artifact_id FROM dataset_sources AS source
                    WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
                )
                AND validation.parser_id = (
                    SELECT artifact.parser_id FROM dataset_sources AS source
                    JOIN extraction_artifacts AS artifact ON artifact.artifact_id = source.artifact_id
                    WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
                )
                AND validation.parser_version = (
                    SELECT artifact.parser_version FROM dataset_sources AS source
                    JOIN extraction_artifacts AS artifact ON artifact.artifact_id = source.artifact_id
                    WHERE source.dataset_version_id = NEW.dataset_version_id LIMIT 1
                )
                AND validation.mapping_revision_id IS version.mapping_revision_id
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'approval requires the current passing validation result');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_approval_update_validation_current
BEFORE UPDATE OF validation_run_id, dataset_version_id ON approvals
WHEN EXISTS (
    SELECT 1 FROM dataset_versions AS version
    WHERE version.dataset_version_id = NEW.dataset_version_id
      AND version.source_job_id IS NOT NULL
      AND (
          version.current_validation_run_id IS NOT NEW.validation_run_id
          OR version.state <> 'valid'
      )
)
BEGIN
    SELECT RAISE(ABORT, 'approval validation identity cannot be changed');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_validation_current_not_stale_pending_review
BEFORE UPDATE OF current_validation_run_id ON dataset_versions
WHEN OLD.current_validation_run_id IS NOT NEW.current_validation_run_id
 AND EXISTS (
     SELECT 1 FROM approvals
     WHERE dataset_version_id = OLD.dataset_version_id
       AND state = 'pending'
       AND validation_run_id = OLD.current_validation_run_id
 )
BEGIN
    SELECT RAISE(ABORT, 'validation cannot be replaced while its review is pending');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_approval_state_current_validation
BEFORE UPDATE OF state ON approvals
WHEN NEW.state = 'approved'
 AND EXISTS (
     SELECT 1 FROM dataset_versions AS version
     WHERE version.dataset_version_id = NEW.dataset_version_id
       AND version.source_job_id IS NOT NULL
       AND (
           version.state <> 'valid'
           OR version.current_validation_run_id IS NOT NEW.validation_run_id
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'approval requires the current passing validation result');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_request_identity_immutable
BEFORE UPDATE ON dataset_version_requests
BEGIN
    SELECT RAISE(ABORT, 'dataset registration request identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_registration_request_matches_version
BEFORE INSERT ON dataset_version_requests
WHEN NOT EXISTS (
    SELECT 1 FROM datasets dataset
    JOIN dataset_versions version ON version.dataset_id = dataset.dataset_id
    JOIN ingestion_jobs job ON job.job_id = NEW.job_id
    WHERE dataset.dataset_id = NEW.dataset_id
      AND dataset.workspace_id = NEW.workspace_id
      AND version.dataset_version_id = NEW.dataset_version_id
      AND version.source_job_id = NEW.job_id
      AND job.workspace_id = NEW.workspace_id
      AND NEW.workspace_id = 'default'
)
BEGIN
    SELECT RAISE(ABORT, 'dataset registration request does not match its version');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_request_no_delete
BEFORE DELETE ON dataset_version_requests
BEGIN
    SELECT RAISE(ABORT, 'dataset registration request evidence is retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_mapping_idempotency_immutable
BEFORE UPDATE OF idempotency_key, fingerprint, dataset_version_id, revision_number
ON dataset_mapping_revisions
BEGIN
    SELECT RAISE(ABORT, 'mapping revision identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_validation_idempotency_immutable
BEFORE UPDATE OF idempotency_key ON validation_runs
WHEN OLD.idempotency_key IS NOT NEW.idempotency_key
BEGIN
    SELECT RAISE(ABORT, 'validation idempotency identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_approval_idempotency_immutable
BEFORE UPDATE OF idempotency_key ON approvals
WHEN OLD.idempotency_key IS NOT NEW.idempotency_key
BEGIN
    SELECT RAISE(ABORT, 'approval idempotency identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_source_registered_immutable_update
BEFORE UPDATE OF dataset_version_id, source_file_id, artifact_id ON dataset_sources
WHEN EXISTS (
    SELECT 1 FROM dataset_versions
    WHERE dataset_version_id = OLD.dataset_version_id AND source_job_id IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'registered dataset source provenance is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_source_registered_immutable_delete
BEFORE DELETE ON dataset_sources
WHEN EXISTS (
    SELECT 1 FROM dataset_versions
    WHERE dataset_version_id = OLD.dataset_version_id AND source_job_id IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'registered dataset source provenance is retained');
END;
