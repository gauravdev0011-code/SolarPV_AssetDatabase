-- Additive Phase 1 integrity controls; migrations 001 and 002 stay immutable.

ALTER TABLE promotion_operations ADD COLUMN dataset_id TEXT
    REFERENCES datasets(dataset_id) ON DELETE RESTRICT;
ALTER TABLE promotion_operations ADD COLUMN dataset_version_id TEXT
    REFERENCES dataset_versions(dataset_version_id) ON DELETE RESTRICT;
ALTER TABLE promotion_operations ADD COLUMN request_fingerprint TEXT;
ALTER TABLE promotion_operations ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0
    CHECK (attempt_count >= 0);

UPDATE promotion_operations
SET dataset_version_id = (
        SELECT dataset_version_id FROM approvals
        WHERE approvals.approval_id = promotion_operations.approval_id
    ),
    dataset_id = (
        SELECT dataset_id FROM dataset_versions
        WHERE dataset_versions.dataset_version_id = (
            SELECT dataset_version_id FROM approvals
            WHERE approvals.approval_id = promotion_operations.approval_id
        )
    );

CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_sources_identity
    ON dataset_sources(dataset_version_id, source_file_id, ifnull(artifact_id, ''));
CREATE INDEX IF NOT EXISTS idx_dataset_sources_file
    ON dataset_sources(source_file_id);
CREATE INDEX IF NOT EXISTS idx_approvals_version
    ON approvals(dataset_version_id, validation_run_id);
CREATE INDEX IF NOT EXISTS idx_promotion_approval
    ON promotion_operations(approval_id, state);
CREATE INDEX IF NOT EXISTS idx_promotion_domain_record
    ON promotion_domain_records(record_type, record_id);

CREATE TRIGGER IF NOT EXISTS trg_dataset_version_transition
BEFORE UPDATE OF state ON dataset_versions
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'draft' AND NEW.state IN ('validating', 'archived'))
    OR (OLD.state = 'validating' AND NEW.state IN ('valid', 'invalid'))
    OR (OLD.state IN ('valid', 'invalid') AND NEW.state = 'archived')
)
BEGIN
    SELECT RAISE(ABORT, 'invalid dataset version state transition');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_version_validation
BEFORE UPDATE OF state ON dataset_versions
WHEN (NEW.state = 'valid' AND NOT EXISTS (
        SELECT 1 FROM validation_runs
        WHERE dataset_version_id = NEW.dataset_version_id
          AND outcome IN ('passed', 'warnings')
     ))
  OR (NEW.state = 'invalid' AND NOT EXISTS (
        SELECT 1 FROM validation_runs
        WHERE dataset_version_id = NEW.dataset_version_id
          AND outcome = 'failed'
     ))
BEGIN
    SELECT RAISE(ABORT, 'dataset version state requires a matching validation run');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_artifact_consistent_insert
BEFORE INSERT ON extraction_artifacts
WHEN NOT EXISTS (
    SELECT 1
    FROM source_files AS file
    JOIN ingestion_jobs AS job
      ON job.job_id = NEW.job_id
     AND job.workspace_id = NEW.workspace_id
     AND job.source_file_id = NEW.source_file_id
    WHERE file.source_file_id = NEW.source_file_id
      AND file.workspace_id = NEW.workspace_id
)
BEGIN
    SELECT RAISE(ABORT, 'artifact source, job, and workspace must agree');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_artifact_consistent_update
BEFORE UPDATE OF workspace_id, source_file_id, job_id ON extraction_artifacts
WHEN NOT EXISTS (
    SELECT 1
    FROM source_files AS file
    JOIN ingestion_jobs AS job
      ON job.job_id = NEW.job_id
     AND job.workspace_id = NEW.workspace_id
     AND job.source_file_id = NEW.source_file_id
    WHERE file.source_file_id = NEW.source_file_id
      AND file.workspace_id = NEW.workspace_id
)
BEGIN
    SELECT RAISE(ABORT, 'artifact source, job, and workspace must agree');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_source_consistent_insert
BEFORE INSERT ON dataset_sources
WHEN NOT EXISTS (
    SELECT 1
    FROM dataset_versions AS version
    JOIN datasets AS dataset ON dataset.dataset_id = version.dataset_id
    JOIN source_files AS file ON file.source_file_id = NEW.source_file_id
    WHERE version.dataset_version_id = NEW.dataset_version_id
      AND dataset.workspace_id = file.workspace_id
      AND (
          NEW.artifact_id IS NULL
          OR EXISTS (
              SELECT 1 FROM extraction_artifacts AS artifact
              WHERE artifact.artifact_id = NEW.artifact_id
                AND artifact.source_file_id = NEW.source_file_id
                AND artifact.workspace_id = dataset.workspace_id
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'dataset source and artifact ownership must agree');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_source_consistent_update
BEFORE UPDATE OF dataset_version_id, source_file_id, artifact_id ON dataset_sources
WHEN NOT EXISTS (
    SELECT 1
    FROM dataset_versions AS version
    JOIN datasets AS dataset ON dataset.dataset_id = version.dataset_id
    JOIN source_files AS file ON file.source_file_id = NEW.source_file_id
    WHERE version.dataset_version_id = NEW.dataset_version_id
      AND dataset.workspace_id = file.workspace_id
      AND (
          NEW.artifact_id IS NULL
          OR EXISTS (
              SELECT 1 FROM extraction_artifacts AS artifact
              WHERE artifact.artifact_id = NEW.artifact_id
                AND artifact.source_file_id = NEW.source_file_id
                AND artifact.workspace_id = dataset.workspace_id
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'dataset source and artifact ownership must agree');
END;

CREATE TRIGGER IF NOT EXISTS trg_interpretation_consistent_insert
BEFORE INSERT ON interpretations
WHEN NOT EXISTS (
    SELECT 1
    FROM source_locations AS location
    JOIN extraction_artifacts AS artifact
      ON artifact.artifact_id = location.artifact_id
    WHERE location.source_location_id = NEW.source_location_id
      AND artifact.workspace_id = NEW.workspace_id
      AND (
          NEW.dataset_version_id IS NULL
          OR EXISTS (
              SELECT 1 FROM dataset_versions AS version
              JOIN datasets AS dataset ON dataset.dataset_id = version.dataset_id
              JOIN dataset_sources AS source
                ON source.dataset_version_id = version.dataset_version_id
               AND source.artifact_id = artifact.artifact_id
              WHERE version.dataset_version_id = NEW.dataset_version_id
                AND dataset.workspace_id = NEW.workspace_id
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'interpretation dataset and source location must agree');
END;

CREATE TRIGGER IF NOT EXISTS trg_interpretation_consistent_update
BEFORE UPDATE OF workspace_id, dataset_version_id, source_location_id ON interpretations
WHEN NOT EXISTS (
    SELECT 1
    FROM source_locations AS location
    JOIN extraction_artifacts AS artifact
      ON artifact.artifact_id = location.artifact_id
    WHERE location.source_location_id = NEW.source_location_id
      AND artifact.workspace_id = NEW.workspace_id
      AND (
          NEW.dataset_version_id IS NULL
          OR EXISTS (
              SELECT 1 FROM dataset_versions AS version
              JOIN datasets AS dataset ON dataset.dataset_id = version.dataset_id
              JOIN dataset_sources AS source
                ON source.dataset_version_id = version.dataset_version_id
               AND source.artifact_id = artifact.artifact_id
              WHERE version.dataset_version_id = NEW.dataset_version_id
                AND dataset.workspace_id = NEW.workspace_id
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'interpretation dataset and source location must agree');
END;

CREATE TRIGGER IF NOT EXISTS trg_approval_valid_before_approval
BEFORE UPDATE OF state ON approvals
WHEN NEW.state = 'approved'
 AND OLD.state <> 'approved'
 AND (
     NEW.decision_by IS NULL OR length(trim(NEW.decision_by)) = 0
     OR NEW.decision_at IS NULL
     OR NOT EXISTS (
         SELECT 1
         FROM validation_runs AS validation
         JOIN dataset_versions AS version
           ON version.dataset_version_id = validation.dataset_version_id
         WHERE validation.validation_run_id = NEW.validation_run_id
           AND validation.dataset_version_id = NEW.dataset_version_id
           AND validation.outcome IN ('passed', 'warnings')
           AND version.state = 'valid'
     )
 )
BEGIN
    SELECT RAISE(ABORT, 'approval requires an operator and a valid dataset validation');
END;

CREATE TRIGGER IF NOT EXISTS trg_approval_revoke_before_promotion
BEFORE UPDATE OF state ON approvals
WHEN OLD.state = 'approved'
 AND NEW.state = 'revoked'
 AND EXISTS (
     SELECT 1 FROM promotion_operations AS operation
     WHERE operation.approval_id = OLD.approval_id
       AND operation.state <> 'cancelled'
 )
BEGIN
    SELECT RAISE(ABORT, 'approval cannot be revoked after promotion has started');
END;

CREATE TRIGGER IF NOT EXISTS trg_approval_transition
BEFORE UPDATE OF state ON approvals
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'pending' AND NEW.state IN ('approved', 'rejected', 'cancelled'))
    OR (OLD.state = 'approved' AND NEW.state = 'revoked')
)
BEGIN
    SELECT RAISE(ABORT, 'invalid approval state transition');
END;

CREATE TRIGGER IF NOT EXISTS trg_approval_identity_immutable
BEFORE UPDATE OF approval_id, dataset_version_id, validation_run_id, destination
ON approvals
WHEN NEW.approval_id IS NOT OLD.approval_id
  OR NEW.dataset_version_id IS NOT OLD.dataset_version_id
  OR NEW.validation_run_id IS NOT OLD.validation_run_id
  OR NEW.destination IS NOT OLD.destination
BEGIN
    SELECT RAISE(ABORT, 'approval identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_request_identity_immutable
BEFORE UPDATE OF promotion_id, approval_id, source_hash, source_name,
                 dataset_id, dataset_version_id, request_fingerprint
ON promotion_operations
WHEN NEW.promotion_id IS NOT OLD.promotion_id
  OR NEW.approval_id IS NOT OLD.approval_id
  OR NEW.source_hash IS NOT OLD.source_hash
  OR NEW.source_name IS NOT OLD.source_name
  OR NEW.dataset_id IS NOT OLD.dataset_id
  OR NEW.dataset_version_id IS NOT OLD.dataset_version_id
  OR NEW.request_fingerprint IS NOT OLD.request_fingerprint
BEGIN
    SELECT RAISE(ABORT, 'promotion request identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_insert_valid
BEFORE INSERT ON promotion_operations
WHEN NEW.state <> 'queued'
 OR NOT EXISTS (
     SELECT 1
     FROM approvals AS approval
     JOIN dataset_versions AS version
       ON version.dataset_version_id = approval.dataset_version_id
     JOIN dataset_sources AS source
       ON source.dataset_version_id = version.dataset_version_id
     JOIN source_files AS file
       ON file.source_file_id = source.source_file_id
     WHERE approval.approval_id = NEW.approval_id
       AND approval.state = 'approved'
       AND approval.destination = 'solarpv_domain'
       AND version.state = 'valid'
       AND version.dataset_id = NEW.dataset_id
       AND version.dataset_version_id = NEW.dataset_version_id
       AND file.sha256 = NEW.source_hash
 )
BEGIN
    SELECT RAISE(ABORT, 'promotion request is not approved for this source and dataset');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_transition
BEFORE UPDATE OF state ON promotion_operations
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'queued' AND NEW.state IN ('running', 'cancelled'))
    OR (OLD.state = 'queued' AND NEW.state IN ('retryable_failure', 'terminal_failure', 'reconciliation_required'))
    OR (OLD.state = 'running' AND NEW.state IN ('succeeded', 'retryable_failure', 'terminal_failure', 'reconciliation_required'))
    OR (OLD.state = 'retryable_failure' AND NEW.state = 'queued')
    OR (OLD.state = 'reconciliation_required' AND NEW.state IN ('succeeded', 'retryable_failure', 'terminal_failure'))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid promotion operation state transition');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_attempt_identity
BEFORE UPDATE OF attempt_count ON promotion_operations
WHEN NEW.attempt_count < OLD.attempt_count
BEGIN
    SELECT RAISE(ABORT, 'promotion attempt count cannot decrease');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_success_consistent
BEFORE UPDATE OF state, outcome, result_json ON promotion_operations
WHEN NEW.state = 'succeeded'
 AND (
     NEW.outcome IS NULL OR NEW.result_json IS NULL
     OR (NEW.outcome = 'imported' AND NOT EXISTS (
         SELECT 1 FROM imported_files WHERE source_hash = NEW.source_hash
     ))
 )
BEGIN
    SELECT RAISE(ABORT, 'successful promotion requires a committed result');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_domain_record_consistent
BEFORE INSERT ON promotion_domain_records
WHEN NOT (
     (NEW.record_type = 'asset' AND EXISTS (SELECT 1 FROM assets WHERE asset_id = NEW.record_id))
  OR (NEW.record_type = 'inspection' AND EXISTS (SELECT 1 FROM inspections WHERE inspection_id = NEW.record_id))
  OR (NEW.record_type = 'observation' AND EXISTS (SELECT 1 FROM observations WHERE observation_id = NEW.record_id))
  OR (NEW.record_type = 'maintenance' AND EXISTS (SELECT 1 FROM maintenance_records WHERE maintenance_id = NEW.record_id))
)
 OR NOT EXISTS (
     SELECT 1 FROM promotion_operations AS operation
     JOIN approvals AS approval ON approval.approval_id = operation.approval_id
     JOIN dataset_sources AS source ON source.dataset_version_id = approval.dataset_version_id
     JOIN source_files AS file ON file.source_file_id = source.source_file_id
     WHERE operation.promotion_id = NEW.promotion_id
       AND operation.state = 'running'
       AND file.sha256 = operation.source_hash
       AND (
           NEW.source_location_id IS NULL
           OR EXISTS (
               SELECT 1 FROM source_locations AS location
               JOIN extraction_artifacts AS artifact ON artifact.artifact_id = location.artifact_id
               WHERE location.source_location_id = NEW.source_location_id
                 AND artifact.source_file_id = file.source_file_id
           )
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'promotion domain record lineage is invalid');
END;
